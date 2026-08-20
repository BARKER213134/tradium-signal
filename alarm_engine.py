# -*- coding: utf-8 -*-
"""⏰ Будильники (2026-08-03): юзер ставит — платформа сигналит в TG.
Режимы:
  price         — цена пересекла уровень (сторона фиксируется при
                  постановке по текущей цене) → TG, снят.
  price+signal  — уровень достигнут (промежуточный TG «жду сигнал»),
                  затем первый НОВЫЙ сигнал платформы по монете → TG, снят.
  signal        — первый новый сигнал по монете (любой источник из
                  new_strategy_signals) → TG, снят.
Хранение: Mongo alarms {symbol, kind, price, side, state ARMED/FIRED,
price_hit, created_at}. Луп раз в 60с: цены двумя batch-запросами
(спот Vision + fapi premiumIndex), сигналы — одним запросом по символам
взведённых будильников."""
from __future__ import annotations
import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)


def _utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _tg(txt: str) -> None:
    try:
        from config import BOT16_BOT_TOKEN, WHALE_CHAT_ID
        if BOT16_BOT_TOKEN and WHALE_CHAT_ID:
            import requests
            requests.post(
                f"https://api.telegram.org/bot{BOT16_BOT_TOKEN}/sendMessage",
                data={"chat_id": WHALE_CHAT_ID, "text": txt,
                      "parse_mode": "HTML"}, timeout=10)
    except Exception:
        logger.debug("[alarm] tg fail", exc_info=True)


def _prices() -> dict:
    """Все цены двумя запросами: спот поверх фьючерса."""
    out = {}
    import requests
    try:
        r = requests.get("https://fapi.binance.com/fapi/v1/ticker/price",
                         timeout=10)
        if r.status_code == 200:
            for x in r.json():
                try:
                    out[x["symbol"]] = float(x["price"])
                except Exception:
                    pass
    except Exception:
        pass
    try:
        r = requests.get("https://data-api.binance.vision/api/v3/ticker/price",
                         timeout=10)
        if r.status_code == 200:
            for x in r.json():
                try:
                    out[x["symbol"]] = float(x["price"])
                except Exception:
                    pass
    except Exception:
        pass
    return out


def _fmt(v):
    try:
        return f"{float(v):.6g}"
    except Exception:
        return str(v)


def _log_event(db, a, cur, sig=None):
    """Сработавший будильник = отдельное событие-сигнал: пишем в
    alarm_events (журнал подмешивает их источником 'alarm' ⏰,
    значки попадают на графики через by-symbol фид)."""
    try:
        db.alarm_events.insert_one({
            "symbol": a.get("symbol"), "kind": a.get("kind"),
            "level": a.get("price"), "side": a.get("side"),
            "entry": cur,
            "sig_strategy": (sig or {}).get("strategy"),
            "sig_direction": (sig or {}).get("direction"),
            # 18.08: план 🎯-входа (вход/стоп/цель) — попап на графике
            # должен говорить, ЧТО ДЕЛАТЬ, а не только «сработал»
            "auto": a.get("auto"), "sig_src": a.get("sig_src"),
            "sig_dir": a.get("sig_dir"), "note": a.get("note"),
            # 19.08: сработавший ПОСЛЕ перевзвода получает свой значок
            "rearmed": bool(a.get("rearmed_at")),
            "alarm_id": str(a.get("_id")),
            "at": _utcnow()})
    except Exception:
        logger.debug("[alarm] event log fail", exc_info=True)


def _refresh_auto_levels(db, alarms, px):
    """🔁 19.08 (ZK: 🎯 сработал «в воздухе» — зона уехала за время
    ожидания): у авто-входов, к которым цена подошла ближе 2%,
    перепроверяем разметку (get_compact_verdict, кэш 5 мин + лимит раз
    в 10 мин на будильник) ДО проверки срабатывания:
      · край зоны сдвинулся >0.5% → перевзводим уровень на новый край;
      · зоны на стороне сделки нет в пределах 8% → EXPIRED (сетап
        протух), в пустоте не стреляем.
    Ручные ⏰ не трогаем. Возвращает обновлённый список."""
    out = []
    for a in alarms:
        if (a.get("auto") != "entry" or a.get("kind") != "price"
                or not a.get("price")):
            out.append(a)
            continue
        cur = px.get(a["symbol"])
        if not cur or abs(cur / a["price"] - 1) * 100 > 2.0:
            out.append(a)
            continue
        rc = a.get("level_checked_at")
        if rc and (_utcnow() - rc).total_seconds() < 600:
            out.append(a)
            continue
        side = a.get("sig_dir")
        try:
            from setup_checker import get_compact_verdict
            c = get_compact_verdict(a["symbol"]) or {}
        except Exception:
            logger.debug("[alarm] refresh verdict fail", exc_info=True)
            out.append(a)
            continue
        edge = c.get("sup_edge") if side == "LONG" else c.get("res_edge")
        base = a["symbol"].replace("USDT", "")
        if not edge or abs(float(edge) / cur - 1) * 100 > 8:
            db.alarms.update_one(
                {"_id": a["_id"]},
                {"$set": {"state": "EXPIRED", "expired_at": _utcnow(),
                          "note": (a.get("note") or "")
                          + " · ⌛ зона ушла — уровень неактуален, снят"}})
            _tg(f"⌛ <b>АВТО-ВХОД · {base}</b> — снят: цена у уровня "
                f"{_fmt(a['price'])}, но зоны на стороне сделки там больше "
                f"нет (разметка уехала)")
            # событие в журнал/на график (19.08: «чтобы видеть, насколько
            # правильно оно работает»)
            try:
                db.alarm_events.insert_one({
                    "symbol": a["symbol"], "kind": "unarm",
                    "level": a.get("price"), "auto": a.get("auto"),
                    "sig_src": a.get("sig_src"), "sig_dir": a.get("sig_dir"),
                    "at": _utcnow()})
            except Exception:
                pass
            continue
        edge = float(edge)
        upd = {"level_checked_at": _utcnow()}
        if abs(edge / a["price"] - 1) > 0.005:
            # план пересчитываем от новой зоны (иначе трекер исходов
            # посчитает R по старому стопу)
            _sg = 1 if side == "LONG" else -1
            far = c.get("sup_far") if side == "LONG" else c.get("res_far")
            _stop = (float(far) * (1 - _sg * 0.004) if far
                     else edge * (1 - _sg * 0.012))
            _tp = edge + _sg * abs(edge - _stop) * 1.5
            upd.update({"price": edge, "rearmed_at": _utcnow(),
                        "plan_stop": _stop, "plan_tp": _tp,
                        "note": (a.get("note") or "")
                        + f" · 🔁 перевзведён → {edge:.6g} (зона сдвинулась)"})
            _tg(f"🔁 <b>АВТО-ВХОД · {base}</b> — уровень перевзведён к "
                f"актуальной зоне: {_fmt(a['price'])} → <b>{_fmt(edge)}</b>")
            try:
                db.alarm_events.insert_one({
                    "symbol": a["symbol"], "kind": "rearm",
                    "level": edge, "old_level": a.get("price"),
                    "auto": a.get("auto"), "sig_src": a.get("sig_src"),
                    "sig_dir": a.get("sig_dir"), "at": _utcnow()})
            except Exception:
                pass
            a = {**a, "price": edge}
        db.alarms.update_one({"_id": a["_id"]}, {"$set": upd})
        out.append(a)
    return out


_PLAN_STOP_RE = None
_PLAN_TP_RE = None


def track_outcomes(batch: int = 80) -> int:
    """🏁 Исходы сработавших 🎯-авто-входов (19.08, запрос «делай
    трекер»): после FIRED ведём план до конца на 15m барах — вход =
    уровень срабатывания, стоп/цель из плана (поля plan_stop/plan_tp,
    фолбэк — парсинг note), SL первым в баре, потолок 96ч по close.
    Пишем outcome TP/SL/TIMEOUT + outcome_pnl_pct + outcome_r в сам
    будильник — вкладка показывает отработку. N/A = нет плана/данных.
    Вызывается из watcher каждые 30 мин."""
    global _PLAN_STOP_RE, _PLAN_TP_RE
    import re
    if _PLAN_STOP_RE is None:
        _PLAN_STOP_RE = re.compile(r"стоп за зону ([\d.eE+-]+)")
        _PLAN_TP_RE = re.compile(r"цель 1\.5R ([\d.eE+-]+)")
    from database import _get_db
    db = _get_db()
    now = _utcnow()
    fired = list(db.alarms.find(
        {"state": "FIRED", "auto": "entry",
         "outcome": {"$exists": False}, "fired_at": {"$ne": None}})
        .sort("fired_at", 1).limit(batch))
    if not fired:
        return 0
    from exchange import get_klines_any
    done = 0
    for a in fired:
        sym = a.get("symbol") or ""
        side = a.get("sig_dir")
        entry = a.get("price")
        na = {"$set": {"outcome": "N/A", "outcome_at": now}}
        if not entry or side not in ("LONG", "SHORT") or not sym:
            db.alarms.update_one({"_id": a["_id"]}, na)
            continue
        sg = 1 if side == "LONG" else -1
        note = a.get("note") or ""
        stop, tp = a.get("plan_stop"), a.get("plan_tp")
        if not stop:
            m = _PLAN_STOP_RE.search(note)
            stop = float(m.group(1)) if m else None
        if not tp:
            m = _PLAN_TP_RE.search(note)
            tp = float(m.group(1)) if m else None
        if not stop or not tp:
            db.alarms.update_one({"_id": a["_id"]}, na)
            continue
        # 🛡 план из note может относиться к СТАРОМУ уровню (перестановка
        # старым кодом не пересчитывала план): стоп/цель не на своей
        # стороне от входа → восстанавливаем риск-геометрию первого плана
        # относительно фактического уровня (19.08: RUNE/ZK «цель −2.7%»)
        if ((sg > 0 and (stop >= entry or tp <= entry))
                or (sg < 0 and (stop <= entry or tp >= entry))):
            risk_pct = abs(stop / entry - 1)
            m0 = _PLAN_STOP_RE.search(note)
            m1 = re.search(r"лимитка от края зоны ([\d.eE+-]+)", note)
            if m0 and m1:
                try:
                    risk_pct = abs(float(m0.group(1)) / float(m1.group(1)) - 1)
                except Exception:
                    pass
            if not (0.001 <= risk_pct <= 0.09):
                risk_pct = 0.012
            stop = entry * (1 - sg * risk_pct)
            tp = entry + sg * abs(entry - stop) * 1.5
            db.alarms.update_one(
                {"_id": a["_id"]},
                {"$set": {"plan_stop": float(stop), "plan_tp": float(tp)}})
        age_h = (now - a["fired_at"]).total_seconds() / 3600
        need = min(int(age_h * 4) + 8, 1000)
        try:
            kl = get_klines_any(sym[:-4] + "/USDT", "15m", need)
        except Exception:
            kl = None
        if not kl:
            if age_h > 100:
                db.alarms.update_one({"_id": a["_id"]}, na)
            continue
        t_f = a["fired_at"].timestamp() * 1000
        t_end = t_f + 96 * 3600_000
        outcome = pnl = None
        last_c = None
        for b in kl:
            bt = b.get("t") or 0
            if bt + 900_000 <= t_f:
                continue
            if bt > t_end:
                break
            last_c = b["c"]
            if (b["l"] <= stop) if sg > 0 else (b["h"] >= stop):
                outcome = "SL"
                pnl = (stop / entry - 1) * 100 * sg
                break
            if (b["h"] >= tp) if sg > 0 else (b["l"] <= tp):
                outcome = "TP"
                pnl = (tp / entry - 1) * 100 * sg
                break
        if outcome is None:
            if now.timestamp() * 1000 >= t_end and last_c:
                outcome = "TIMEOUT"
                pnl = (last_c / entry - 1) * 100 * sg
            else:
                continue   # ещё в работе — проверим в следующем цикле
        risk = abs(entry - stop) / entry * 100
        db.alarms.update_one({"_id": a["_id"]}, {"$set": {
            "outcome": outcome, "outcome_pnl_pct": round(pnl, 2),
            "outcome_r": round(pnl / risk, 2) if risk else None,
            "outcome_at": now}})
        done += 1
    return done


def _tick_sync(last_sig_ts: dict) -> None:
    from database import _get_db
    db = _get_db()
    # 🎯 18.08: авто-входы живут 48ч — если цена не пришла, сетап протух:
    # тихо гасим (EXPIRED, без TG), освобождая пару для новых авто-входов.
    # Ручные ⏰ не трогаем — воля юзера.
    try:
        db.alarms.update_many(
            {"state": "ARMED", "auto": "entry",
             "created_at": {"$lt": _utcnow() - timedelta(hours=48)}},
            {"$set": {"state": "EXPIRED", "expired_at": _utcnow()}})
    except Exception:
        pass
    alarms = list(db.alarms.find({"state": "ARMED"}))
    try:
        db.heartbeats.update_one({"_id": "alarms"},
                                 {"$set": {"at": _utcnow()}}, upsert=True)
    except Exception:
        pass
    if not alarms:
        return
    px = _prices()
    # 🔁 перевзвод 🎯-уровней к актуальным зонам — ДО проверки
    # срабатывания, чтобы не стрелять по устаревшей разметке
    try:
        alarms = _refresh_auto_levels(db, alarms, px)
    except Exception:
        logger.debug("[alarm] refresh levels fail", exc_info=True)
    # свежие сигналы по символам будильников (для kind signal / price+signal)
    need_sig = {a["symbol"] for a in alarms
                if a["kind"] == "signal"
                or (a["kind"] == "price+signal" and a.get("price_hit"))}
    fresh = {}
    if need_sig:
        since = _utcnow() - timedelta(minutes=5)
        for s in db.new_strategy_signals.find(
                {"symbol": {"$in": list(need_sig)},
                 "created_at": {"$gte": since}},
                {"symbol": 1, "strategy": 1, "direction": 1, "entry": 1,
                 "created_at": 1}).sort("created_at", -1):
            sym = s["symbol"]
            ts = s["created_at"].timestamp()
            # не реагировать на сигнал, который уже видели
            if ts <= last_sig_ts.get(sym, 0):
                continue
            if sym not in fresh:
                fresh[sym] = s
    for a in alarms:
        sym = a["symbol"]
        cur = px.get(sym)
        base = sym.replace("USDT", "")
        if a["kind"] in ("price", "price+signal") and not a.get("price_hit"):
            if cur is None or not a.get("price"):
                continue
            hit = (cur >= a["price"]) if a.get("side") == "above" \
                else (cur <= a["price"])
            if not hit:
                continue
            if a["kind"] == "price":
                if a.get("auto") == "entry":
                    # 🎯 авто-вход: цена пришла в точку, где разбор велел
                    # входить — карточка с планом + свежий 🎰-контекст
                    txt = (f"🎯 <b>АВТО-ВХОД · {base}</b>\n"
                           f"цена дошла до края зоны {_fmt(a['price'])} "
                           f"(сейчас {_fmt(cur)})\n"
                           f"{a.get('note') or ''}")
                    try:
                        from setup_checker import signal_tg_context
                        txt += signal_tg_context(
                            base + "/USDT", a.get("sig_dir"))
                    except Exception:
                        pass
                    _tg(txt)
                else:
                    _tg(f"⏰ <b>БУДИЛЬНИК · {base}</b>\n"
                        f"цена дошла до {_fmt(a['price'])} "
                        f"(сейчас {_fmt(cur)})")
                _log_event(db, a, cur)
                db.alarms.update_one({"_id": a["_id"]},
                                     {"$set": {"state": "FIRED",
                                               "fired_at": _utcnow()}})
            else:
                _tg(f"⏰ <b>БУДИЛЬНИК · {base}</b> — уровень "
                    f"{_fmt(a['price'])} достигнут (сейчас {_fmt(cur)})\n"
                    f"жду первый сигнал платформы по монете…")
                db.alarms.update_one({"_id": a["_id"]},
                                     {"$set": {"price_hit": True}})
            continue
        if a["kind"] == "signal" or (a["kind"] == "price+signal"
                                     and a.get("price_hit")):
            s = fresh.get(sym)
            if s is None:
                continue
            head = ("⏰🚨 <b>БУДИЛЬНИК+СИГНАЛ" if a["kind"] == "price+signal"
                    else "⏰ <b>СИГНАЛ-БУДИЛЬНИК")
            lvl = (f" · уровень {_fmt(a.get('price'))} был достигнут"
                   if a["kind"] == "price+signal" else "")
            _tg(f"{head} · {base}</b>{lvl}\n"
                f"пришёл сигнал: {s.get('strategy')} "
                f"{s.get('direction') or ''} @ {_fmt(s.get('entry'))}\n"
                f"(сейчас {_fmt(cur) if cur else '?'})")
            _log_event(db, a, cur, s)
            db.alarms.update_one({"_id": a["_id"]},
                                 {"$set": {"state": "FIRED",
                                           "fired_at": _utcnow()}})
            last_sig_ts[sym] = s["created_at"].timestamp()


async def alarm_loop():
    """Проверка будильников раз в 60с."""
    await asyncio.sleep(90)
    last_sig_ts: dict = {}
    while True:
        try:
            await asyncio.to_thread(_tick_sync, last_sig_ts)
        except Exception:
            logger.debug("[alarm] tick fail", exc_info=True)
        await asyncio.sleep(60)
