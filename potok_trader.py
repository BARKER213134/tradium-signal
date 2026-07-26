# -*- coding: utf-8 -*-
"""🌊 ПОТОК — авто-канал paper-трейдинга: «сигнал, подтверждённый потоком».

Бэктесты 25-26.07 (сетка/гейты выжаты полностью):
  LONG: сигнал (whale/tc/st_mtf/st_vip/floor_buy/confluence) + светофор ДА
        + BUY-аномалия <=12ч + CVD24>0 + funding>=0
        → 90д на реальных сигналах: WR 52%, EV +2.21%/сделку;
          год-реплика: +0.80, 8/13 месяцев в плюсе. Полный размер.
  SHORT: сигнал (shark/tc/second_flip/thin_pump/st_mtf/confluence) + ДА
        + funding > -0.05% (не шортить перепроданное по фандингу)
        → 90д: WR 53%, EV +1.48; год-реплика РЕЖИМНАЯ (2/6 мес) →
          полразмера на старте + circuit-breaker.
  Выходы: один ордер TP +20% / SL -5% · тайм-стоп 48ч при <+2% ·
        smart-выход лонгов (пик >=+4% и sell-климакс на паре) ·
        потолок 96ч · закрытие минусовых при развороте фазы.
        Трейлинг/лестницы/поглощения проверены и отвергнуты.
  Circuit-breaker (на канал): скользящие 30д Σpnl<0 → полразмера;
        Σ30д < -50пп → пауза. Дневной стоп: Σ за день < -15пп → до утра.

Paper-only. Учёт: 1R = риск 1% депо => вклад сделки в депо = pnl%/5 × mult.
"""
from __future__ import annotations
import logging
import time
from datetime import timedelta

logger = logging.getLogger(__name__)

LONG_SRC = {"whale", "triple_confluence", "st_mtf", "st_vip", "floor_buy",
            "confluence_5plus", "confluence_lo"}
SHORT_SRC = {"shark", "triple_confluence", "second_flip", "thin_pump",
             "st_mtf", "st_vip", "confluence_5plus", "confluence_lo"}
SLOTS = 8
COOLDOWN_H = 24
TP_PCT, SL_PCT = 20.0, 5.0
TIME_STOP_H, TIME_STOP_MIN = 48, 2.0
HORIZON_H = 96
SHORT_BASE_MULT = 0.5      # год-реплика: шорт режимный
DAY_STOP_PP = -15.0        # ~-3% депо при 1R=1%
CB_HALF_PP = 0.0           # 30д Σ < 0 → полразмера
CB_PAUSE_PP = -50.0        # 30д Σ < -50пп → пауза канала

_last_funding_fetch = 0.0


def _db():
    from database import _get_db
    return _get_db()


def _tg(txt: str) -> None:
    """Информативный канал ПОТОКА — BOT6 (paper trading bot)."""
    try:
        from config import BOT6_BOT_TOKEN, ADMIN_CHAT_ID
        if BOT6_BOT_TOKEN and ADMIN_CHAT_ID:
            import requests
            requests.post(
                f"https://api.telegram.org/bot{BOT6_BOT_TOKEN}/sendMessage",
                data={"chat_id": ADMIN_CHAT_ID, "text": txt,
                      "parse_mode": "HTML"}, timeout=10)
    except Exception:
        logger.debug("[potok] tg fail", exc_info=True)


def refresh_funding(force: bool = False) -> None:
    """Снапшот funding по ВСЕМ парам одним запросом (premiumIndex),
    раз в 15 мин → market_state.funding_now."""
    global _last_funding_fetch
    if not force and time.time() - _last_funding_fetch < 900:
        return
    try:
        import requests
        r = requests.get("https://fapi.binance.com/fapi/v1/premiumIndex",
                         timeout=12)
        if r.status_code != 200:
            return
        rates = {}
        for x in r.json():
            s = x.get("symbol", "")
            if s.endswith("USDT"):
                try:
                    rates[s] = float(x.get("lastFundingRate") or 0)
                except Exception:
                    pass
        if len(rates) > 100:
            from database import utcnow
            _db().market_state.update_one(
                {"_id": "funding_now"},
                {"$set": {"rates": rates, "updated_at": utcnow()}},
                upsert=True)
            _last_funding_fetch = time.time()
    except Exception:
        logger.debug("[potok] funding fetch fail", exc_info=True)


def _funding(sym: str):
    doc = _db().market_state.find_one({"_id": "funding_now"}) or {}
    return (doc.get("rates") or {}).get(sym)


def _phase_now():
    ms = _db().system.find_one({"_id": "market_side_state"}) or {}
    return ms.get("side")


def _climate_now():
    d = _db().market_state.find_one({"_id": "climate12"}) or {}
    return d.get("state")


def _channel_state(direction: str) -> dict:
    """Circuit-breaker: скользящие 30д и сегодня по закрытым сделкам."""
    from database import utcnow
    db = _db()
    now = utcnow()
    rows30 = list(db.potok_trades.find(
        {"direction": direction, "closed_at": {"$gte": now - timedelta(days=30)}},
        {"pnl_pct": 1, "closed_at": 1}))
    sum30 = sum(r.get("pnl_pct") or 0 for r in rows30)
    day0 = now.replace(hour=0, minute=0, second=0, microsecond=0)
    sum_day = sum((r.get("pnl_pct") or 0) for r in rows30
                  if r.get("closed_at") and r["closed_at"] >= day0)
    base = 1.0 if direction == "LONG" else SHORT_BASE_MULT
    mult, status = base, "полный" if base == 1.0 else "полразмера(база)"
    if sum30 < CB_PAUSE_PP:
        mult, status = 0.0, f"ПАУЗА (30д {sum30:+.0f}пп)"
    elif sum30 < CB_HALF_PP:
        mult, status = base * 0.5, f"полразмера (30д {sum30:+.0f}пп)"
    if sum_day <= DAY_STOP_PP:
        mult, status = 0.0, f"дневной стоп ({sum_day:+.0f}пп)"
    return {"mult": mult, "status": status, "sum30": round(sum30, 1),
            "sum_day": round(sum_day, 1), "n30": len(rows30)}


def _live_prices() -> dict:
    """Свежие цены напрямую (fapi→vision, один запрос все пары) — мимо
    2-минутного batch-кэша: живой PnL на вкладке и точные выходы."""
    import requests
    for url in ("https://fapi.binance.com/fapi/v1/ticker/price",
                "https://data-api.binance.vision/api/v3/ticker/price"):
        try:
            r = requests.get(url, timeout=8)
            if r.status_code == 200:
                out = {}
                for x in r.json():
                    s = x.get("symbol", "")
                    if s.endswith("USDT"):
                        try:
                            out[s] = float(x["price"])
                        except Exception:
                            pass
                if len(out) > 100:
                    return out
        except Exception:
            continue
    return _prices()


def _prices() -> dict:
    """Текущие цены из batch-кэша тикеров (1 запрос на всё)."""
    try:
        import futures_data as fd
        fd._refresh_batch_cache()
        tick = (fd._batch_cache or {}).get("ticker", {})
        return {s: (v.get("price") if isinstance(v, dict) else None)
                for s, v in tick.items()}
    except Exception:
        logger.debug("[potok] prices fail", exc_info=True)
        return {}


def _fmt(v):
    if v is None:
        return "—"
    v = float(v)
    return f"{v:.6g}"


def _open_position(db, sym, pair, d_, src, score, star, ctx, rate, mult, kit,
                   price, phase, clim):
    from database import utcnow
    want = 1 if d_ == "LONG" else -1
    doc = {"symbol": sym, "pair": pair, "direction": d_, "src": src,
           "entry": price, "tp": price * (1 + want * TP_PCT / 100),
           "sl": price * (1 - want * SL_PCT / 100),
           "opened_at": utcnow(), "peak_pnl": 0.0,
           "size_mult": mult, "kit": bool(kit), "score": score,
           "star": bool(star), "funding": rate,
           "cvd24": ctx.get("cvd24"), "phase": phase, "climate": clim,
           "anom_age_h": ctx.get("_anom_age_h")}
    # 🌊 запись входа в журнал (маркер на графиках); state='OPEN' —
    # generic-трекер её не трогает, исход проставит manage() при закрытии
    try:
        jr = db.new_strategy_signals.insert_one({
            "strategy": "potok", "direction": d_, "pair": pair,
            "symbol": sym, "entry": price, "tp": doc["tp"], "sl": doc["sl"],
            "horizon_h": HORIZON_H, "created_at": doc["opened_at"],
            "state": "OPEN",
            "indicators": {"src": src, "score": score, "mult": mult,
                           "kit": bool(kit), "phase": phase,
                           "climate": clim, "funding": rate,
                           "cvd24": ctx.get("cvd24"),
                           "anom_age_h": ctx.get("_anom_age_h")}})
        doc["journal_id"] = jr.inserted_id
    except Exception:
        logger.debug("[potok] journal insert fail", exc_info=True)
    db.potok_positions.insert_one(doc)
    E = {"LONG": "🟢 LONG", "SHORT": "🔴 SHORT"}
    PE = {"LONG": "🟢", "SHORT": "🔴", "NEUTRAL": "⚪", None: "?"}
    anom_txt = (f"BUY-аномалия {doc['anom_age_h']:.0f}ч назад · "
                if d_ == "LONG" and doc.get("anom_age_h") is not None else "")
    _tg(f"🌊 <b>ПОТОК · ОТКРЫТ {E[d_]} · {pair.replace('/USDT', '')}</b>"
        f"{' 🐋КИТ' if kit else ''}{' ⭐' if star else ''}\n"
        f"источник: {src} · светофор ДА (счёт {score})\n"
        f"поток: {anom_txt}CVD24 {'+' if (doc['cvd24'] or 0) >= 0 else ''}"
        f"{_fmt(doc['cvd24'])} · funding {rate * 100:+.3f}%\n"
        f"рынок: фаза {PE.get(phase, '?')}{phase or '?'} · "
        f"климат {PE.get(clim, '?')}{clim or '?'}\n"
        f"вход {_fmt(price)} · TP {_fmt(doc['tp'])} (+20%) · "
        f"SL {_fmt(doc['sl'])} (−5%) · размер {mult:.2f}R\n"
        f"<i>выходы: тайм-стоп 48ч&lt;+2% · "
        f"{'sell-климакс в плюсе ≥4% · ' if d_ == 'LONG' else ''}"
        f"потолок 96ч · разворот фазы</i>")


def try_open() -> int:
    """Свежие ДА-сигналы (40 мин) → гейты каналов → открытия."""
    from database import utcnow
    db = _db()
    now = utcnow()
    since = now - timedelta(minutes=40)
    cands = []   # (src, dir, sym, pair, at, score, star)

    def _n(p):
        s = (p or "").replace("/", "").upper()
        return s if s.endswith("USDT") else s + "USDT"

    for n_ in db.new_strategy_signals.find(
            {"created_at": {"$gte": since}, "svetofor": "ДА"},
            {"strategy": 1, "pair": 1, "symbol": 1, "direction": 1,
             "created_at": 1, "svetofor_score": 1, "svetofor_star": 1}).limit(200):
        if n_.get("direction"):
            cands.append((n_.get("strategy"), n_["direction"],
                          n_.get("symbol") or _n(n_.get("pair")),
                          n_.get("pair") or "", n_["created_at"],
                          n_.get("svetofor_score", 0), n_.get("svetofor_star")))
    for s_ in db.supertrend_signals.find(
            {"flip_at": {"$gte": since}, "svetofor": "ДА"},
            {"tier": 1, "pair": 1, "direction": 1, "flip_at": 1,
             "svetofor_score": 1, "svetofor_star": 1}).limit(200):
        if s_.get("direction"):
            cands.append((f"st_{s_.get('tier') or 'daily'}", s_["direction"],
                          _n(s_.get("pair")), s_.get("pair") or "",
                          s_["flip_at"], s_.get("svetofor_score", 0),
                          s_.get("svetofor_star")))
    for c_ in db.confluence.find(
            {"detected_at": {"$gte": since}, "svetofor": "ДА"},
            {"pair": 1, "symbol": 1, "direction": 1, "detected_at": 1,
             "score": 1, "svetofor_score": 1, "svetofor_star": 1}).limit(200):
        if c_.get("direction"):
            key = ("confluence_5plus" if (c_.get("score") or 0) >= 5
                   else "confluence_lo")
            cands.append((key, c_["direction"],
                          c_.get("symbol") or _n(c_.get("pair")),
                          c_.get("pair") or "", c_["detected_at"],
                          c_.get("svetofor_score", 0), c_.get("svetofor_star")))
    if not cands:
        return 0
    # КИТ-приоритет: сортируем LONG с большим CVD вперёд (v1-упрощение)
    ctx_all = {d["_id"]: d for d in db.pair_context.find({})}
    cands.sort(key=lambda x: -(ctx_all.get(x[2], {}).get("cvd24") or 0)
               if x[1] == "LONG" else 0)
    open_syms = {p["symbol"] for p in db.potok_positions.find({}, {"symbol": 1})}
    n_open = len(open_syms)
    st_long = _channel_state("LONG")
    st_short = _channel_state("SHORT")
    phase, clim = _phase_now(), _climate_now()
    prices = _prices()
    opened = 0
    for src, d_, sym, pair, at, score, star in cands:
        if n_open + opened >= SLOTS:
            break
        if sym in open_syms:
            continue
        st = st_long if d_ == "LONG" else st_short
        if st["mult"] <= 0:
            continue
        if d_ == "LONG" and src not in LONG_SRC:
            continue
        if d_ == "SHORT" and src not in SHORT_SRC:
            continue
        # кулдаун 24ч на пару+направление
        last = db.potok_trades.find_one(
            {"symbol": sym, "direction": d_,
             "opened_at": {"$gte": now - timedelta(hours=COOLDOWN_H)}},
            {"_id": 1})
        if last:
            continue
        rate = _funding(sym)
        if rate is None:
            continue
        ctx = ctx_all.get(sym) or {}
        kit = False
        if d_ == "LONG":
            if rate < 0:
                continue
            cvd = ctx.get("cvd24")
            if cvd is None or cvd <= 0:
                continue
            ab = ctx.get("anom_buy_ts")
            if not ab:
                continue
            age_h = (now.timestamp() - ab / 1000) / 3600
            if age_h > 12:
                continue
            ctx["_anom_age_h"] = age_h
            kit = bool((ctx.get("cvd24") or 0) > 0 and (ctx.get("dz24") or 0) < 0)
        else:
            if rate <= -0.0005:
                continue
        price = prices.get(sym) or ctx.get("price")
        if not price:
            continue
        pair_slash = pair if "/" in (pair or "") else sym[:-4] + "/USDT"
        _open_position(db, sym, pair_slash, d_, src, score, star, ctx, rate,
                       st["mult"], kit, float(price), phase, clim)
        open_syms.add(sym)
        opened += 1
    return opened


def manage() -> int:
    """Ведение позиций: TP/SL/тайм-стоп/smart/потолок/фаза-разворот."""
    from database import utcnow
    db = _db()
    now = utcnow()
    poss = list(db.potok_positions.find({}))
    if not poss:
        return 0
    prices = _live_prices()
    ctx_all = {d["_id"]: d for d in db.pair_context.find(
        {"_id": {"$in": [p["symbol"] for p in poss]}})}
    phase = _phase_now()
    closed = 0
    for p in poss:
        sym, d_ = p["symbol"], p["direction"]
        want = 1 if d_ == "LONG" else -1
        price = prices.get(sym)
        if not price:
            continue
        pnl = (price / p["entry"] - 1) * 100 * want
        peak = max(p.get("peak_pnl") or 0, pnl)
        age_h = (now - p["opened_at"]).total_seconds() / 3600
        reason = None
        if (want == 1 and price <= p["sl"]) or (want == -1 and price >= p["sl"]):
            reason, pnl = "SL", -SL_PCT
        elif (want == 1 and price >= p["tp"]) or (want == -1 and price <= p["tp"]):
            reason, pnl = "TP", TP_PCT
        elif d_ == "LONG" and peak >= 4:
            an = (ctx_all.get(sym) or {}).get("anom_sell_ts")
            if an and (now.timestamp() - an / 1000) / 3600 < 1.5:
                reason = "SMART(sell-климакс)"
        if reason is None and age_h >= TIME_STOP_H and pnl < TIME_STOP_MIN:
            reason = "TIME48"
        if reason is None and age_h >= HORIZON_H:
            reason = "H96"
        if reason is None and pnl < 0 and phase and phase != "NEUTRAL" \
                and phase != d_:
            reason = "PHASE(разворот)"
        if reason is None:
            if peak != p.get("peak_pnl"):
                db.potok_positions.update_one(
                    {"_id": p["_id"]}, {"$set": {"peak_pnl": peak}})
            continue
        db.potok_positions.delete_one({"_id": p["_id"]})
        trade = {**{k: p[k] for k in p if k != "_id"},
                 "closed_at": now, "exit_price": price,
                 "pnl_pct": round(pnl, 2), "reason": reason,
                 "hold_h": round(age_h, 1)}
        db.potok_trades.insert_one(trade)
        if p.get("journal_id") is not None:
            try:
                db.new_strategy_signals.update_one(
                    {"_id": p["journal_id"]},
                    {"$set": {"state": reason.split("(")[0],
                              "pnl_pct": round(pnl, 2),
                              "exit_price": price, "exit_at": now}})
            except Exception:
                pass
        closed += 1
        st = _channel_state(d_)
        depo = round(pnl / 5 * (p.get("size_mult") or 1), 2)
        emo = "✅" if pnl > 0 else "❌"
        _tg(f"{emo} <b>ПОТОК · ЗАКРЫТ {d_} · {p['pair'].replace('/USDT', '')}"
            f"</b> · {reason}\n"
            f"PnL <b>{pnl:+.2f}%</b> (≈{depo:+.2f}% депо) · "
            f"в позиции {age_h:.0f}ч · пик {peak:+.1f}%\n"
            f"канал {d_} 30д: {st['sum30']:+.1f}пп · сегодня {st['sum_day']:+.1f}пп "
            f"· статус: {st['status']}")
    return closed


def tick() -> dict:
    """Один цикл движка (вызывается из watcher каждые ~5 мин)."""
    refresh_funding()
    c = manage()
    o = try_open()
    return {"opened": o, "closed": c}
