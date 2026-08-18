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
            "at": _utcnow()})
    except Exception:
        logger.debug("[alarm] event log fail", exc_info=True)


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
