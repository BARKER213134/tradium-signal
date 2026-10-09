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


async def ws_probe_once():
    """Одноразовая проба WS-потоков (2026-08-03): фьючерсный WS с Railway
    молчит на ВСЕХ стримах (markPrice, kline, forceOrder) при живом
    SUBSCRIBE-ack. Три варианта по 60с — итог в system.ws_probe:
    A combined fstream markPrice · B raw fstream markPrice ·
    C spot btcusdt@trade. Если C льётся, а A/B нет — политика Binance
    именно по fstream для этого IP."""
    try:
        import websockets
    except ImportError:
        return
    await asyncio.sleep(90)
    probes = [
        ("A_fstream_combined",
         "wss://fstream.binance.com/stream?streams=btcusdt@markPrice"),
        ("B_fstream_raw",
         "wss://fstream.binance.com/ws/btcusdt@markPrice"),
        ("C_spot_raw",
         "wss://stream.binance.com:9443/ws/btcusdt@trade"),
    ]
    out = {}
    for name, url in probes:
        n = 0
        err = None
        try:
            async with websockets.connect(
                    url, ping_interval=20, max_size=2 ** 22,
                    close_timeout=5) as ws:
                t0 = time.time()
                while time.time() - t0 < 60:
                    try:
                        await asyncio.wait_for(ws.recv(), timeout=10)
                        n += 1
                    except asyncio.TimeoutError:
                        pass
        except Exception as e:
            err = f"{type(e).__name__}: {e}"[:120]
        out[name] = {"frames": n, "err": err}
        logger.info(f"[ws-probe] {name}: {n} кадров, err={err}")
    try:
        from database import _get_db
        _get_db().system.update_one(
            {"_id": "ws_probe"},
            {"$set": {"at": _utcnow(), "res": out}}, upsert=True)
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


BINGX = "https://open-api.bingx.com"


def _bx_get(path: str, params: dict | None = None, timeout: int = 15):
    """GET публичного BingX → data или None (fail-open)."""
    import requests
    try:
        r = requests.get(BINGX + path, params=params or {}, timeout=timeout)
        if r.status_code != 200:
            return None
        j = r.json()
        if not isinstance(j, dict) or j.get("code") not in (0, None):
            return None
        return j.get("data")
    except Exception:
        return None


def bingx_premium() -> dict:
    """{SYM: (mark, lastFundingRate)} одним запросом по всем perpetual."""
    out = {}
    for x in _bx_get("/openApi/swap/v2/quote/premiumIndex") or []:
        try:
            out[x["symbol"].replace("-", "")] = (
                float(x.get("markPrice") or 0), float(x.get("lastFundingRate") or 0))
        except Exception:
            pass
    return out


def bingx_liquid_pairs(min_qv: float = 3_000_000.0, top: int = 400) -> list[str]:
    """Пары BingX по 24ч quoteVolume (ticker одним запросом) + BTC/ETH."""
    rows = []
    for x in _bx_get("/openApi/swap/v2/quote/ticker") or []:
        try:
            s = x["symbol"]
            qv = float(x.get("quoteVolume") or 0)
            if s.endswith("-USDT") and qv >= min_qv:
                rows.append((qv, s.replace("-", "")))
        except Exception:
            pass
    rows.sort(reverse=True)
    out = [s for _, s in rows[:top]]
    for s in ("ETHUSDT", "BTCUSDT"):
        if s not in out:
            out.insert(0, s)
    return out


def bingx_oi_usd(sym: str):
    """OI в USDT (BingX отдаёт quote-номинал: BTC 908M при 10.9k BTC)."""
    d = _bx_get("/openApi/swap/v2/quote/openInterest",
                {"symbol": sym[:-4] + "-USDT"}, timeout=10)
    try:
        v = float((d or {}).get("openInterest") or 0)
        return v if v > 0 else None
    except Exception:
        return None


def _oi_snapshot_sync(pairs: list[str], workers: int = 4) -> int:
    """Часовой снапшот OI+funding с BingX (09.10: fapi мёртв с 01.08).
    oi_hourly {_id 'SYM:hour_ts', symbol, at, oi (монет), oi_usd, fr, mark, src}."""
    from concurrent.futures import ThreadPoolExecutor
    from database import _get_db
    from pymongo import UpdateOne
    db = _get_db()
    pm = bingx_premium()
    hour_ts = int(time.time() // 3600 * 3600)
    at = datetime.fromtimestamp(hour_ts, tz=timezone.utc).replace(tzinfo=None)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        ois = dict(zip(pairs, ex.map(bingx_oi_usd, pairs)))
    ops = []
    for sym, usd in ois.items():
        if not usd:
            continue
        mark, fr = pm.get(sym, (0.0, None))
        ops.append(UpdateOne(
            {"_id": f"{sym}:{hour_ts}"},
            {"$set": {"symbol": sym, "at": at,
                      "oi": round(usd / mark, 4) if mark else None,
                      "oi_usd": round(usd, 0), "fr": fr,
                      "mark": mark or None, "src": "bingx"}},
            upsert=True))
    if ops:
        db.oi_hourly.bulk_write(ops, ordered=False)
    _hb("oi_poll")
    _wr_status(oi_last=at.isoformat(), oi_n=len(ops), oi_pairs=len(pairs), oi_src="bingx")
    return len(ops)


def _pulse_sync(top: int = 60) -> dict:
    """15-мин агрегат (deriv_pulse): BTC/ETH OI usd, Σ OI top-N, медиана
    funding, доля отрицательных funding — таймлайн для каскадов."""
    from database import _get_db
    import statistics
    db = _get_db()
    pm = bingx_premium()
    pairs = bingx_liquid_pairs(top=top)
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=4) as ex:
        ois = dict(zip(pairs, ex.map(bingx_oi_usd, pairs)))
    frs = [fr for (_, fr) in pm.values() if fr is not None]
    ts15 = int(time.time() // 900 * 900)
    at = datetime.fromtimestamp(ts15, tz=timezone.utc).replace(tzinfo=None)
    doc = {"at": at, "btc_oi_usd": ois.get("BTCUSDT"), "eth_oi_usd": ois.get("ETHUSDT"),
           "top_oi_usd": round(sum(v for v in ois.values() if v), 0),
           "top_n": sum(1 for v in ois.values() if v),
           "fr_med": round(statistics.median(frs), 6) if frs else None,
           "fr_neg_share": round(sum(1 for f in frs if f < 0) / len(frs), 3) if frs else None,
           "fr_n": len(frs), "src": "bingx"}
    db.deriv_pulse.update_one({"_id": ts15}, {"$set": doc}, upsert=True)
    _hb("deriv_pulse")
    _wr_status(pulse_last=at.isoformat(), pulse_top_n=doc["top_n"])
    return doc


async def oi_poll_loop():
    """Часовой снапшот OI+funding (BingX) по ликвидным парам, в :02."""
    await asyncio.sleep(240)          # не мешаем стартовым прогревам
    while True:
        try:
            pairs = await asyncio.to_thread(bingx_liquid_pairs)
            if pairs:
                n = await asyncio.to_thread(_oi_snapshot_sync, pairs)
                logger.info(f"[oi] bingx snapshot: {n}/{len(pairs)} пар")
        except Exception:
            logger.debug("[oi] poll fail", exc_info=True)
        # до следующего часа + 2 мин
        wait = 3600 - (time.time() % 3600) + 120
        await asyncio.sleep(max(300, min(wait, 3900)))


async def deriv_pulse_loop():
    """15-мин агрегат деривативов (BingX) → deriv_pulse."""
    await asyncio.sleep(180)
    while True:
        try:
            d = await asyncio.to_thread(_pulse_sync)
            logger.debug(f"[deriv-pulse] {d}")
        except Exception:
            logger.debug("[deriv-pulse] fail", exc_info=True)
        wait = 900 - (time.time() % 900) + 20
        await asyncio.sleep(max(60, min(wait, 960)))
