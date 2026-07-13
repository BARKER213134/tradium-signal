"""Mongo-based RSI cache для журнала.

Schema (collection: signal_rsi_cache):
  pair, tf ('15m'|'1h'|'4h'|'1d'), open_ms, rsi, cached_at

TTL: 24h автоматический cleanup через index.

API:
  fill_pair_rsi(pair, tfs=('15m','1h','4h','1d')) — заполняет cache
  get_rsi(pair, at_ts_ms, tf) — читает RSI на свече сигнала
  bulk_get_rsi(pairs_ts_tfs) — bulk read
"""
from __future__ import annotations
import logging
import time
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

# Warmup: достаточно для журнала за last 24h + 28 баров RSI(14)+SMA(14).
# Старые значения (24h) были слишком короткими — fix.
# Слишком большие значения (60 дней для 1d) перегружают CDN — event loop затыкается.
TF_WARMUP_HOURS = {
    '15m': 72,            # 3 дня = 288 баров
    '1h':  120,           # 5 дней = 120 баров
    '4h':  20 * 24,       # 20 дней = 120 баров
    '1d':  45 * 24,       # 45 дней (для RSI+SMA на дневке)
}
TF_BUCKET_MS = {
    '15m': 15 * 60 * 1000,
    '1h':  60 * 60 * 1000,
    '4h':  4 * 60 * 60 * 1000,
    '1d':  24 * 60 * 60 * 1000,
}


def _ensure_indexes():
    try:
        from database import _get_db
        col = _get_db().signal_rsi_cache
        col.create_index([('pair', 1), ('tf', 1), ('open_ms', 1)], unique=True)
        col.create_index('cached_at', expireAfterSeconds=2 * 24 * 3600)  # 48h TTL
    except Exception as e:
        logger.debug(f'[rsi-cache] index ensure: {e}')


_indexes_ready = False


def _fetch_klines_fapi(sym: str, tf: str, limit: int = 1500) -> list:
    """FAPI single-call fetcher. Возвращает list [open_ms, o, h, l, c, vol, close_ms].

    В 10-50x быстрее CDN day-by-day fetch для warmup периодов >5 дней.
    Use as primary в fill_pair_rsi — fallback на CDN если FAPI failed.
    """
    try:
        from fapi_budget import allow
        if not allow():
            return []   # бюджет fapi исчерпан — caller уйдёт на CDN
        import httpx
        r = httpx.get("https://fapi.binance.com/fapi/v1/klines",
                      params={'symbol': sym, 'interval': tf, 'limit': limit},
                      timeout=10.0)
        if r.status_code == 200:
            out = []
            for kr in r.json():
                try:
                    out.append([int(kr[0]), float(kr[1]), float(kr[2]),
                                float(kr[3]), float(kr[4]),
                                float(kr[5]), int(kr[6])])
                except Exception:
                    continue
            return out
    except Exception:
        pass
    return []


def fill_pair_rsi(pair: str, tfs: tuple = ('15m', '1h', '4h', '1d')) -> dict:
    """Фетчит klines + считает RSI + пишет в Mongo. Возвращает {tf: n_written}.

    Использует FAPI single-call (limit=1500) вместо CDN day-by-day fetch.
    Это в 30x быстрее для warmup периодов: 1d 45 дней = 1 HTTP call вместо
    45 zip-загрузок. Для CDN-only fallback использует fetch_klines_cdn.
    """
    from delta_calculator import (fetch_klines_cdn, _normalize_symbol,
                                   _compute_rsi_for_closes)
    from database import _get_db
    from pymongo import UpdateOne
    global _indexes_ready
    if not _indexes_ready:
        _ensure_indexes()
        _indexes_ready = True
    sym = _normalize_symbol(pair)
    if not sym:
        return {}
    col = _get_db().signal_rsi_cache
    now_ms = int(time.time() * 1000)
    now_dt = datetime.now(timezone.utc)
    result = {}
    for tf in tfs:
        warmup_h = TF_WARMUP_HOURS.get(tf, 24)
        # Compute needed bars: warmup_h * 60 / TF_minutes + 50 safety
        tf_min = {'15m': 15, '1h': 60, '4h': 240, '1d': 1440}.get(tf, 60)
        needed_bars = (warmup_h * 60) // tf_min + 50
        limit = min(1500, max(100, needed_bars))
        # Primary: FAPI single-call (fast, не throttled на CDN range)
        kl = _fetch_klines_fapi(sym, tf, limit=limit)
        # Fallback на CDN если FAPI failed (banned / network err)
        if not kl or len(kl) < 16:
            start = now_ms - warmup_h * 3600 * 1000
            end = now_ms + 60 * 1000
            try:
                kl = fetch_klines_cdn(sym, tf, start, end)
            except Exception:
                kl = []
        if not kl or len(kl) < 16:
            result[tf] = 0
            continue
        closes = [float(k[4]) for k in kl]
        rsis = _compute_rsi_for_closes(closes, 14)
        # SMA(14) of RSI values
        sma_rsi = [None] * len(rsis)
        buf = []
        s = 0
        for i, r in enumerate(rsis):
            if r is None:
                continue
            buf.append(r)
            s += r
            if len(buf) > 14:
                s -= buf.pop(0)
            if len(buf) == 14:
                sma_rsi[i] = s / 14
        ops = []
        for i, k in enumerate(kl):
            if rsis[i] is None:
                continue
            try:
                set_doc = {
                    'pair': pair, 'tf': tf, 'open_ms': int(k[0]),
                    'rsi': round(rsis[i], 2),
                    'close': float(k[4]),
                    'cached_at': now_dt,
                }
                if sma_rsi[i] is not None:
                    set_doc['sma_rsi'] = round(sma_rsi[i], 2)
                ops.append(UpdateOne(
                    {'pair': pair, 'tf': tf, 'open_ms': int(k[0])},
                    {'$set': set_doc},
                    upsert=True,
                ))
            except Exception:
                continue
        if ops:
            try:
                col.bulk_write(ops, ordered=False)
                result[tf] = len(ops)
            except Exception as e:
                logger.debug(f'[rsi-cache] bulk_write {pair}/{tf}: {e}')
                result[tf] = 0
        else:
            result[tf] = 0
    return result


async def fill_pair_rsi_async(pair: str, tfs: tuple = ('15m', '1h', '4h', '1d')) -> None:
    """Async-wrapper — fire-and-forget. Используется хуками signal creation
    чтобы RSI был готов сразу к моменту когда пользователь откроет журнал.
    """
    import asyncio
    try:
        await asyncio.to_thread(fill_pair_rsi, pair, tfs)
    except Exception as e:
        logger.debug(f'[rsi-cache] async fill {pair}: {e}')


def bulk_get_rsi_for_items(items: list[dict]) -> None:
    """Bulk-read RSI cache для items, проставляет rsi_15m/rsi_1h/rsi_4h/rsi_1d.

    Один Mongo find $or на все нужные ключи + in-process map lookup.
    """
    if not items:
        return
    try:
        from database import _get_db
        col = _get_db().signal_rsi_cache
    except Exception:
        return
    # Собираем уникальные (pair, tf, open_ms)
    wanted = set()
    item_keys = []  # (it, pair, [(tf, open_ms), ...])
    for it in items:
        ats = it.get('at_ts') or 0
        pair = it.get('pair') or ''
        if not (ats and pair):
            continue
        ats_ms = ats * 1000
        keys_for_it = []
        for tf, bucket_ms in TF_BUCKET_MS.items():
            open_ms = (ats_ms // bucket_ms) * bucket_ms
            wanted.add((pair, tf, open_ms))
            keys_for_it.append((tf, open_ms))
        item_keys.append((it, pair, keys_for_it))
    if not wanted:
        return
    # Bulk find chunked — fetch RSI + SMA(RSI)
    chunks = list(wanted)
    cached_map = {}  # (pair, tf, open_ms) → (rsi, sma_rsi)
    CHUNK = 1000  # увеличил с 500 — Mongo $or поддерживает до ~1500 conds
    for i in range(0, len(chunks), CHUNK):
        conds = [{'pair': p, 'tf': t, 'open_ms': om}
                 for (p, t, om) in chunks[i:i+CHUNK]]
        try:
            for doc in col.find({'$or': conds},
                                {'pair':1,'tf':1,'open_ms':1,
                                 'rsi':1,'sma_rsi':1,'_id':0}):
                cached_map[(doc['pair'], doc['tf'], doc['open_ms'])] = (
                    doc.get('rsi'), doc.get('sma_rsi'))
        except Exception:
            pass
    # Apply to items
    for it, pair, keys in item_keys:
        for tf, open_ms in keys:
            cached = cached_map.get((pair, tf, open_ms))
            if not cached:
                continue
            rsi, sma_rsi = cached
            if rsi is not None:
                it[f'rsi_{tf}'] = rsi
            if sma_rsi is not None:
                it[f'sma_rsi_{tf}'] = sma_rsi
