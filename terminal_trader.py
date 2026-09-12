# -*- coding: utf-8 -*-
"""💼 ТЕРМИНАЛ — ручные сделки юзера из интерфейса (запрос 12.09.26,
после закрытия Finandy).

Paper-движок с прицелом на Binance Futures live: структура позиции
(маржа × плечо, нотионал, ликвидация, комиссия тейкера 0.05%/сторона)
повторяет фьючерсную, чтобы обкатка была честной. Live-исполнение
добавится отдельным слоем через VPS-прокси (fapi с Railway под 451).

Отличия от ПОТОКа/paper_trader: здесь НЕТ авто-входов — только ручные
ордера юзера (маркет / лимит / стоп-вход), SL/TP обязательны, ведение
каждые ~45с, закрытие/БУ/перенос стопа из UI.

Коллекции: terminal_positions (OPEN + PENDING), terminal_trades (история).
Каждый OPEN пишется в журнал (strategy='terminal', state='OPEN') —
маркер на графиках; исход проставляет tick() при закрытии.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

FEE_SIDE_PCT = 0.05          # тейкер Binance Futures, % от нотионала за сторону
LIQ_FRAC = 0.95              # ликвидация при движении против ≈95% от 100/плечо
MAX_LEV = 50
MAX_MARGIN = 100_000.0
DEF_SL_PCT, DEF_TP_PCT = 5.0, 10.0


def _db():
    from database import _get_db
    return _get_db()


def _tg(txt: str) -> None:
    try:
        from config import BOT6_BOT_TOKEN, ADMIN_CHAT_ID
        if BOT6_BOT_TOKEN and ADMIN_CHAT_ID:
            import requests
            requests.post(
                f"https://api.telegram.org/bot{BOT6_BOT_TOKEN}/sendMessage",
                data={"chat_id": ADMIN_CHAT_ID, "text": txt,
                      "parse_mode": "HTML"}, timeout=10)
    except Exception:
        logger.debug("[terminal] tg fail", exc_info=True)


def _fmt(x) -> str:
    if x is None:
        return "?"
    x = float(x)
    if x >= 100:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.4f}"
    return f"{x:.6g}"


def get_price(sym: str) -> float | None:
    """Живая цена одной пары: спот Vision → фьюч fapi → batch-кэш ПОТОКа."""
    import requests
    for url in (f"https://data-api.binance.vision/api/v3/ticker/price?symbol={sym}",
                f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={sym}"):
        try:
            r = requests.get(url, timeout=6)
            if r.status_code == 200:
                return float(r.json()["price"])
        except Exception:
            pass
    try:
        from potok_trader import _prices
        return (_prices() or {}).get(sym)
    except Exception:
        return None


def _validate_levels(direction: str, ref: float, stop: float, tp: float) -> str | None:
    """Гео-проверка уровней (урок трекера будильников: перевёрнутый план
    = мусорные исходы). None = ок, иначе текст ошибки."""
    if direction == "LONG":
        if stop >= ref:
            return f"стоп {_fmt(stop)} должен быть НИЖЕ входа {_fmt(ref)} (LONG)"
        if tp <= ref:
            return f"цель {_fmt(tp)} должна быть ВЫШЕ входа {_fmt(ref)} (LONG)"
    else:
        if stop <= ref:
            return f"стоп {_fmt(stop)} должен быть ВЫШЕ входа {_fmt(ref)} (SHORT)"
        if tp >= ref:
            return f"цель {_fmt(tp)} должна быть НИЖЕ входа {_fmt(ref)} (SHORT)"
    return None


def open_position(payload: dict) -> dict:
    """Открыть позицию (market) или поставить ордер (limit/стоп-вход).

    payload: symbol, direction LONG|SHORT, margin_usdt, leverage,
             order_type market|limit, limit_price?, stop?, tp?, note?
    Стоп/цель не заданы → −5%/+10% от входа в сторону сделки.
    """
    from database import utcnow
    sym = str(payload.get("symbol") or "").upper().replace("/", "").strip()
    if not sym:
        return {"ok": False, "error": "нет тикера"}
    if not sym.endswith("USDT"):
        sym += "USDT"
    d_ = str(payload.get("direction") or "").upper()
    if d_ not in ("LONG", "SHORT"):
        return {"ok": False, "error": "направление LONG или SHORT"}
    want = 1 if d_ == "LONG" else -1
    try:
        margin = float(payload.get("margin_usdt") or 0)
        lev = int(float(payload.get("leverage") or 1))
    except Exception:
        return {"ok": False, "error": "маржа/плечо — числа"}
    if not (1 <= margin <= MAX_MARGIN):
        return {"ok": False, "error": f"маржа 1..{MAX_MARGIN:.0f} $"}
    if not (1 <= lev <= MAX_LEV):
        return {"ok": False, "error": f"плечо 1..{MAX_LEV}"}
    px = get_price(sym)
    if not px:
        return {"ok": False, "error": f"нет цены по {sym}"}
    otype = (payload.get("order_type") or "market").lower()
    if otype == "limit":
        try:
            limit_price = float(payload.get("limit_price") or 0)
        except Exception:
            limit_price = 0
        if limit_price <= 0:
            return {"ok": False, "error": "нужна цена лимит-ордера"}
        ref = limit_price
        # ордер ниже цены ждёт касания сверху (лимит), выше — пробоя (стоп-вход)
        exec_when = "touch_down" if limit_price < px else "touch_up"
    else:
        otype, ref, exec_when = "market", px, None
    try:
        stop = float(payload["stop"]) if payload.get("stop") else ref * (1 - want * DEF_SL_PCT / 100)
        tp = float(payload["tp"]) if payload.get("tp") else ref * (1 + want * DEF_TP_PCT / 100)
    except Exception:
        return {"ok": False, "error": "стоп/цель — числа"}
    err = _validate_levels(d_, ref, stop, tp)
    if err:
        return {"ok": False, "error": err}
    liq = ref * (1 - want * LIQ_FRAC / lev)
    if (want > 0 and stop <= liq) or (want < 0 and stop >= liq):
        return {"ok": False, "error":
                f"стоп {_fmt(stop)} за ценой ликвидации {_fmt(liq)} (плечо ×{lev}) — уменьши плечо или подтяни стоп"}
    now = utcnow()
    doc = {"symbol": sym, "pair": sym[:-4] + "/USDT", "direction": d_,
           "margin_usdt": round(margin, 2), "leverage": lev,
           "notional_usdt": round(margin * lev, 2),
           "stop": stop, "tp": tp, "note": (payload.get("note") or "")[:120],
           "src": (payload.get("src") or "manual")[:40],
           "mode": "paper", "created_at": now, "peak_raw": 0.0}
    risk_usd = abs(ref - stop) / ref * lev * margin
    if otype == "market":
        doc.update({"status": "OPEN", "entry": px, "opened_at": now,
                    "liq": liq})
        try:
            jr = _db().new_strategy_signals.insert_one({
                "strategy": "terminal", "direction": d_, "pair": doc["pair"],
                "symbol": sym, "entry": px, "tp": tp, "sl": stop,
                "created_at": now, "state": "OPEN",
                "pattern": f"💼 ТЕРМИНАЛ · ручной вход ×{lev}",
                "indicators": {"margin_usdt": margin, "leverage": lev,
                               "mode": "paper"}})
            doc["journal_id"] = jr.inserted_id
        except Exception:
            logger.debug("[terminal] journal fail", exc_info=True)
    else:
        doc.update({"status": "PENDING", "entry": ref, "exec_when": exec_when,
                    "liq": liq})
    ins = _db().terminal_positions.insert_one(doc)
    E = "🟢 LONG" if d_ == "LONG" else "🔴 SHORT"
    if otype == "market":
        _tg(f"💼 <b>ТЕРМИНАЛ · ОТКРЫТ {E} · {sym.replace('USDT', '')}</b> (paper)\n"
            f"вход {_fmt(px)} · маржа ${margin:.0f} × {lev} = ${margin * lev:.0f}\n"
            f"SL {_fmt(stop)} ({(stop / px - 1) * 100:+.1f}%) · "
            f"TP {_fmt(tp)} ({(tp / px - 1) * 100:+.1f}%) · ликв. {_fmt(liq)}\n"
            f"риск ${risk_usd:.2f}")
    else:
        arrow = "⬇ касание снизу" if exec_when == "touch_down" else "⬆ пробой вверх"
        _tg(f"💼⏳ <b>ТЕРМИНАЛ · ОРДЕР {E} · {sym.replace('USDT', '')}</b> (paper)\n"
            f"вход по {_fmt(ref)} ({arrow}, сейчас {_fmt(px)})\n"
            f"маржа ${margin:.0f} × {lev} · SL {_fmt(stop)} · TP {_fmt(tp)}")
    return {"ok": True, "id": str(ins.inserted_id), "status": doc["status"],
            "entry": doc["entry"], "risk_usd": round(risk_usd, 2)}


def _close(db, p: dict, exit_price: float, reason: str, now) -> dict:
    want = 1 if p["direction"] == "LONG" else -1
    raw = (exit_price / p["entry"] - 1) * 100 * want
    lev = p.get("leverage") or 1
    margin = p.get("margin_usdt") or 0
    fee = (p.get("notional_usdt") or margin * lev) * FEE_SIDE_PCT / 100 * 2
    pnl_usd = margin * lev * raw / 100 - fee
    pnl_margin_pct = (pnl_usd / margin * 100) if margin else 0
    hold_h = (now - (p.get("opened_at") or p["created_at"])).total_seconds() / 3600
    trade = {**{k: p[k] for k in p if k != "_id"},
             "closed_at": now, "exit_price": exit_price, "reason": reason,
             "raw_pct": round(raw, 2), "pnl_usd": round(pnl_usd, 2),
             "pnl_margin_pct": round(pnl_margin_pct, 2),
             "fee_usd": round(fee, 2), "hold_h": round(hold_h, 1)}
    db.terminal_trades.insert_one(trade)
    db.terminal_positions.delete_one({"_id": p["_id"]})
    if p.get("journal_id") is not None:
        try:
            db.new_strategy_signals.update_one(
                {"_id": p["journal_id"]},
                {"$set": {"state": reason, "pnl_pct": round(raw, 2),
                          "exit_price": exit_price, "exit_at": now}})
        except Exception:
            pass
    emo = "✅" if pnl_usd > 0 else "❌"
    _tg(f"{emo} <b>ТЕРМИНАЛ · ЗАКРЫТ {p['direction']} · "
        f"{p['symbol'].replace('USDT', '')}</b> · {reason} (paper)\n"
        f"PnL <b>${pnl_usd:+.2f}</b> ({pnl_margin_pct:+.1f}% маржи · "
        f"ход {raw:+.2f}%) · в позиции {hold_h:.1f}ч\n"
        f"вход {_fmt(p['entry'])} → выход {_fmt(exit_price)} · комиссия ${fee:.2f}")
    return trade


def close_position(pos_id: str, reason: str = "MANUAL") -> dict:
    from bson import ObjectId
    from database import utcnow
    db = _db()
    p = db.terminal_positions.find_one({"_id": ObjectId(pos_id)})
    if not p:
        return {"ok": False, "error": "позиция не найдена"}
    if p.get("status") == "PENDING":
        db.terminal_positions.delete_one({"_id": p["_id"]})
        _tg(f"💼🚫 ТЕРМИНАЛ · ордер {p['direction']} {p['symbol'].replace('USDT', '')} отменён")
        return {"ok": True, "cancelled": True}
    px = get_price(p["symbol"])
    if not px:
        return {"ok": False, "error": f"нет цены по {p['symbol']}"}
    t = _close(db, p, px, reason, utcnow())
    return {"ok": True, "pnl_usd": t["pnl_usd"], "raw_pct": t["raw_pct"]}


def update_position(pos_id: str, stop=None, tp=None, breakeven: bool = False) -> dict:
    from bson import ObjectId
    db = _db()
    p = db.terminal_positions.find_one({"_id": ObjectId(pos_id)})
    if not p:
        return {"ok": False, "error": "позиция не найдена"}
    ref = p["entry"]
    new_stop = float(stop) if stop else p["stop"]
    new_tp = float(tp) if tp else p["tp"]
    if breakeven:
        want = 1 if p["direction"] == "LONG" else -1
        # БУ с запасом на комиссию круга
        new_stop = ref * (1 + want * 2 * FEE_SIDE_PCT / 100)
    if p.get("status") == "OPEN":
        # для открытой позиции стоп можно двигать за вход (трейлинг/БУ) —
        # проверяем только сторону цели
        want = 1 if p["direction"] == "LONG" else -1
        if (want > 0 and new_tp <= new_stop) or (want < 0 and new_tp >= new_stop):
            return {"ok": False, "error": "цель по ту же сторону, что и стоп"}
    else:
        err = _validate_levels(p["direction"], ref, new_stop, new_tp)
        if err:
            return {"ok": False, "error": err}
    db.terminal_positions.update_one(
        {"_id": p["_id"]}, {"$set": {"stop": new_stop, "tp": new_tp}})
    return {"ok": True, "stop": new_stop, "tp": new_tp,
            "be": bool(breakeven)}


def tick() -> dict:
    """Ведение: PENDING → исполнение при касании; OPEN → LIQ/SL/TP.
    Вызывается из watcher каждые ~45с."""
    from database import utcnow
    db = _db()
    poss = list(db.terminal_positions.find({}))
    if not poss:
        return {"filled": 0, "closed": 0}
    from potok_trader import _live_prices
    prices = _live_prices()
    now = utcnow()
    filled = closed = 0
    for p in poss:
        px = prices.get(p["symbol"])
        if not px:
            continue
        want = 1 if p["direction"] == "LONG" else -1
        if p.get("status") == "PENDING":
            hit = px <= p["entry"] if p["exec_when"] == "touch_down" else px >= p["entry"]
            if hit:
                upd = {"status": "OPEN", "opened_at": now}
                try:
                    jr = db.new_strategy_signals.insert_one({
                        "strategy": "terminal", "direction": p["direction"],
                        "pair": p["pair"], "symbol": p["symbol"],
                        "entry": p["entry"], "tp": p["tp"], "sl": p["stop"],
                        "created_at": now, "state": "OPEN",
                        "pattern": f"💼 ТЕРМИНАЛ · лимит исполнен ×{p.get('leverage') or 1}",
                        "indicators": {"margin_usdt": p.get("margin_usdt"),
                                       "leverage": p.get("leverage"),
                                       "mode": "paper"}})
                    upd["journal_id"] = jr.inserted_id
                except Exception:
                    pass
                db.terminal_positions.update_one({"_id": p["_id"]}, {"$set": upd})
                filled += 1
                _tg(f"💼⚡ <b>ТЕРМИНАЛ · ОРДЕР ИСПОЛНЕН · "
                    f"{'🟢 LONG' if want > 0 else '🔴 SHORT'} "
                    f"{p['symbol'].replace('USDT', '')}</b> по {_fmt(p['entry'])} (paper)")
            continue
        raw = (px / p["entry"] - 1) * 100 * want
        lev = p.get("leverage") or 1
        if raw <= -LIQ_FRAC / lev * 100:
            liq_px = p.get("liq") or p["entry"] * (1 - want * LIQ_FRAC / lev)
            _close(db, p, liq_px, "LIQ", now)
            closed += 1
            continue
        if (want > 0 and px <= p["stop"]) or (want < 0 and px >= p["stop"]):
            _close(db, p, p["stop"], "SL", now)
            closed += 1
            continue
        if (want > 0 and px >= p["tp"]) or (want < 0 and px <= p["tp"]):
            _close(db, p, p["tp"], "TP", now)
            closed += 1
            continue
        peak = max(p.get("peak_raw") or 0, raw)
        if peak != p.get("peak_raw"):
            db.terminal_positions.update_one(
                {"_id": p["_id"]}, {"$set": {"peak_raw": peak}})
    return {"filled": filled, "closed": closed}


def view() -> dict:
    """Срез для UI: позиции с live-PnL, ордера, история, сводка."""
    db = _db()
    poss = list(db.terminal_positions.find({}).sort("created_at", -1))
    prices = {}
    if poss:
        try:
            from potok_trader import _live_prices
            prices = _live_prices()
        except Exception:
            prices = {}
    out_open, out_pend = [], []
    for p in poss:
        px = prices.get(p["symbol"])
        want = 1 if p["direction"] == "LONG" else -1
        row = {"id": str(p["_id"]), "symbol": p["symbol"],
               "direction": p["direction"], "status": p["status"],
               "entry": p["entry"], "stop": p["stop"], "tp": p["tp"],
               "margin_usdt": p.get("margin_usdt"),
               "leverage": p.get("leverage"), "liq": p.get("liq"),
               "note": p.get("note") or "", "price": px,
               "created_at": p["created_at"].isoformat()}
        if p["status"] == "OPEN" and px:
            raw = (px / p["entry"] - 1) * 100 * want
            lev = p.get("leverage") or 1
            margin = p.get("margin_usdt") or 0
            row["raw_pct"] = round(raw, 2)
            row["pnl_usd"] = round(margin * lev * raw / 100, 2)
            row["peak_raw"] = round(p.get("peak_raw") or 0, 2)
            out_open.append(row)
        elif p["status"] == "OPEN":
            out_open.append(row)
        else:
            if px:
                row["dist_pct"] = round((p["entry"] / px - 1) * 100, 2)
            out_pend.append(row)
    trades = []
    for t in db.terminal_trades.find({}).sort("closed_at", -1).limit(60):
        trades.append({"symbol": t["symbol"], "direction": t["direction"],
                       "entry": t["entry"], "exit_price": t["exit_price"],
                       "reason": t["reason"], "raw_pct": t.get("raw_pct"),
                       "pnl_usd": t.get("pnl_usd"),
                       "pnl_margin_pct": t.get("pnl_margin_pct"),
                       "margin_usdt": t.get("margin_usdt"),
                       "leverage": t.get("leverage"),
                       "hold_h": t.get("hold_h"),
                       "closed_at": t["closed_at"].isoformat()})
    agg = list(db.terminal_trades.aggregate([
        {"$group": {"_id": None, "n": {"$sum": 1},
                    "pnl": {"$sum": "$pnl_usd"},
                    "wins": {"$sum": {"$cond": [{"$gt": ["$pnl_usd", 0]}, 1, 0]}}}}]))
    st = agg[0] if agg else {"n": 0, "pnl": 0, "wins": 0}
    return {"open": out_open, "pending": out_pend, "trades": trades,
            "stats": {"n": st["n"], "pnl_usd": round(st["pnl"] or 0, 2),
                      "wr": round(st["wins"] / st["n"] * 100, 1) if st["n"] else None,
                      "open_n": len(out_open), "pending_n": len(out_pend)}}
