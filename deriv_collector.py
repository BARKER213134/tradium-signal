# -*- coding: utf-8 -*-
"""📡 Коллектор деривативных данных (2026-07-31): ликвидации + OI.
ТОЛЬКО сбор в Mongo — сигналов нет. Цель: собственная история для
бэктестов (глубокой истории ликвидаций не купить — пишем сами; через
месяц-два будет датасет: ликвидационные каскады = классика дна,
OI-дивергенции = выдыхающиеся сквизы).

Коллекции:
- liq_5m: 5-мин корзины по паре {_id 'SYM:ts5m', symbol, at,
  long_usd (side SELL = ликвидация лонга), short_usd, n_long, n_short,
  max_usd}. Источник: wss !forceOrder@arr. NB: Binance шлёт максимум
  1 ордер/сек/символ — это СЭМПЛ потока, не полный объём; для
  каскад-детекции достаточно.
- liq_big: единичные ликвидации >= $50k сырыми доками.
- oi_hourly: часовой снапшот OI {_id 'SYM:hour_ts', symbol, at, oi,
  oi_usd} по ликвидным парам (~300/час, fapi_budget tag='oi',
  markPrice одним batch-запросом premiumIndex).
"""
from __future__ import annotations
import asyncio
import json
import logging
import time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

LIQ_WS_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"
BIG_USD = 50_000.0
FLUSH_SEC = 15


def _utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _hb(name: str):
    try:
        from database import _get_db
        _get_db().heartbeats.update_one(
            {"_id": name}, {"$set": {"at": _utcnow()}}, upsert=True)
    except Exception:
        pass


def _flush_liq(buckets: dict, bigs: list):
    """Пакетная запись корзин (в thread — не блокировать event loop)."""
    try:
        from database import _get_db
        from pymongo import UpdateOne
        db = _get_db()
        ops = []
        for (sym, ts5), b in buckets.items():
            ops.append(UpdateOne(
                {"_id": f"{sym}:{ts5}"},
                {"$inc": {"long_usd": b["l"], "short_usd": b["s"],
                          "n_long": b["nl"], "n_short": b["ns"]},
                 "$max": {"max_usd": b["mx"]},
                 "$setOnInsert": {
                     "symbol": sym,
                     "at": datetime.fromtimestamp(ts5, tz=timezone.utc)
                     .replace(tzinfo=None)}},
                upsert=True))
        if ops:
            db.liq_5m.bulk_write(ops, ordered=False)
        if bigs:
            db.liq_big.insert_many(bigs, ordered=False)
        _hb("liq_ws")
    except Exception:
        logger.debug("[liq] flush fail", exc_info=True)


def _wr_status(**kw):
    try:
        from database import _get_db
        _get_db().system.update_one(
            {"_id": "liq_ws_status"},
            {"$set": {"at": _utcnow(), **kw}}, upsert=True)
    except Exception:
        pass


async def run_liq_stream():
    """WS-луп ликвидаций (паттерн delta_websocket: combined /stream URL,
    recv с таймаутом — флаш и ПУЛЬС каждые ~20с ДАЖЕ В ТИШИНЕ; ликвидации
    редкие поштучно — молчание не значит сбой; реконнект при 15 мин без
    единого сообщения; диагностика в system.liq_ws_status)."""
    try:
        import websockets
    except ImportError:
        logger.error("[liq] websockets package not installed")
        _wr_status(state="no_websockets_pkg")
        return
    # !forceOrder@arr молчит (проверено 2026-08-03: канал жив — SUBSCRIBE
    # отвечает, markPrice льётся, а событий нет) → добавлены пер-символьные
    # @forceOrder топ-пар + markPrice-проба как индикатор живости канала
    probe_syms = ["btcusdt", "ethusdt", "solusdt", "dogeusdt", "xrpusdt"]
    streams = ["!forceOrder@arr"] + [f"{s}@forceOrder" for s in probe_syms] \
        + ["btcusdt@markPrice"]
    url = "wss://fstream.binance.com/stream?streams=" + "/".join(streams)
    buckets: dict = {}
    bigs: list = []
    msgs_total = 0
    raw_total = 0        # ВСЕ кадры (в т.ч. ответ на SUBSCRIBE) — диагностика
    marks_total = 0      # кадры markPrice-пробы (доказательство потока)
    seen_keys: set = set()   # дедуп: @arr и пер-символ могут дать один ивент
    while True:
        try:
            async with websockets.connect(
                    url, ping_interval=180, ping_timeout=600,
                    max_size=2 ** 22, close_timeout=10) as ws:
                logger.info("[liq] connected")
                # SUBSCRIBE-проба: Binance обязан ответить кадром
                # {"result":null,"id":1} — если raw_total останется 0,
                # значит канал молчит и на служебные ответы = сбой канала
                try:
                    await ws.send(json.dumps({
                        "method": "SUBSCRIBE",
                        "params": streams, "id": 1}))
                except Exception:
                    pass
                _wr_status(state="connected", last_error=None,
                           raw_total=raw_total)
                silent_s = 0.0
                last_st = time.time()
                while True:
                    raw = None
                    try:
                        raw = await asyncio.wait_for(ws.recv(),
                                                     timeout=FLUSH_SEC)
                        silent_s = 0.0
                        raw_total += 1
                    except asyncio.TimeoutError:
                        silent_s += FLUSH_SEC
                    if time.time() - last_st >= 60:   # счётчики раз в минуту
                        last_st = time.time()
                        _wr_status(state="connected", raw_total=raw_total,
                                   msgs_total=msgs_total,
                                   marks_total=marks_total)
                    # флаш + пульс — по таймеру, независимо от сообщений
                    if buckets or bigs:
                        fb, fbi = buckets, bigs
                        buckets, bigs = {}, []
                        await asyncio.to_thread(_flush_liq, fb, fbi)
                    else:
                        await asyncio.to_thread(_hb, "liq_ws")
                    if raw is None:
                        if silent_s >= 900:
                            logger.warning("[liq] 15 мин тишины — реконнект")
                            _wr_status(state="silent_reconnect",
                                       msgs_total=msgs_total)
                            break
                        continue
                    try:
                        msg = json.loads(raw)
                        data = msg.get("data") or {}
                        if data.get("e") == "markPriceUpdate":
                            marks_total += 1
                            continue
                        o = data.get("o") or {}
                        sym = o.get("s") or ""
                        if not sym.endswith("USDT"):
                            continue
                        qty = float(o.get("z") or o.get("q") or 0)
                        px = float(o.get("ap") or o.get("p") or 0)
                        usd = qty * px
                        if usd <= 0:
                            continue
                        dk = f"{sym}:{o.get('T')}:{qty}"
                        if dk in seen_keys:
                            continue
                        seen_keys.add(dk)
                        if len(seen_keys) > 4000:
                            seen_keys.clear()
                        msgs_total += 1
                        if msgs_total == 1 or msgs_total % 500 == 0:
                            _wr_status(state="flowing",
                                       msgs_total=msgs_total,
                                       last_sym=sym)
                        # SELL = принудительная продажа = ликвидирован ЛОНГ
                        is_long_liq = (o.get("S") == "SELL")
                        ts5 = int(o.get("T", time.time() * 1000)
                                  // 1000 // 300 * 300)
                        b = buckets.setdefault((sym, ts5), {
                            "l": 0.0, "s": 0.0, "nl": 0, "ns": 0, "mx": 0.0})
                        if is_long_liq:
                            b["l"] += usd; b["nl"] += 1
                        else:
                            b["s"] += usd; b["ns"] += 1
                        b["mx"] = max(b["mx"], usd)
                        if usd >= BIG_USD:
                            bigs.append({
                                "symbol": sym, "usd": round(usd, 2),
                                "price": px, "qty": qty,
                                "side": "LONG_LIQ" if is_long_liq else "SHORT_LIQ",
                                "at": _utcnow()})
                    except Exception:
                        continue
        except Exception as e:
            logger.warning(f"[liq] stream error: {type(e).__name__} — reconnect 15s")
            _wr_status(state="error",
                       last_error=f"{type(e).__name__}: {e}"[:200])
            await asyncio.sleep(15)


def _oi_snapshot_sync(pairs: list[str]) -> int:
    """Один проход OI по парам (sync, зовётся в thread). ~300 req,
    пейсинг 0.45с — бюджет fapi не душим."""
    import requests
    from database import _get_db
    db = _get_db()
    # markPrice одним запросом
    marks = {}
    try:
        r = requests.get("https://fapi.binance.com/fapi/v1/premiumIndex",
                         timeout=12)
        if r.status_code == 200:
            for x in r.json():
                try:
                    marks[x["symbol"]] = float(x.get("markPrice") or 0)
                except Exception:
                    pass
    except Exception:
        pass
    hour_ts = int(time.time() // 3600 * 3600)
    at = datetime.fromtimestamp(hour_ts, tz=timezone.utc).replace(tzinfo=None)
    wrote = 0
    for sym in pairs:
        try:
            from fapi_budget import allow
            if not allow(tag="oi"):
                time.sleep(2)
                continue
        except Exception:
            pass
        try:
            r = requests.get("https://fapi.binance.com/fapi/v1/openInterest",
                             params={"symbol": sym}, timeout=10)
            if r.status_code != 200:
                time.sleep(0.45)
                continue
            oi = float((r.json() or {}).get("openInterest") or 0)
            if oi > 0:
                px = marks.get(sym) or 0
                db.oi_hourly.update_one(
                    {"_id": f"{sym}:{hour_ts}"},
                    {"$set": {"symbol": sym, "at": at, "oi": oi,
                              "oi_usd": round(oi * px, 0) if px else None}},
                    upsert=True)
                wrote += 1
        except Exception:
            pass
        time.sleep(0.45)
    _hb("oi_poll")
    return wrote


async def oi_poll_loop():
    """Часовой снапшот OI по ликвидным парам."""
    await asyncio.sleep(240)          # не мешаем стартовым прогревам
    while True:
        try:
            from futures_data import get_liquid_pairs
            pairs = [p.replace("/", "") for p in
                     get_liquid_pairs(min_volume_usd=5_000_000)[:300]]
            if pairs:
                n = await asyncio.to_thread(_oi_snapshot_sync, pairs)
                logger.info(f"[oi] snapshot: {n} пар")
        except Exception:
            logger.debug("[oi] poll fail", exc_info=True)
        await asyncio.sleep(3600)
