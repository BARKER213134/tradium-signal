"""Mongo-based Trend cache для журнала.

Trend definition (multi-TF): EMA20 vs EMA50 на close.
  - UP   if EMA20 > EMA50 (uptrend)
  - DOWN if EMA20 < EMA50 (downtrend)
  - FLAT if |EMA20-EMA50|/close < 0.05% (1bp шум)

Schema (collection: signal_trend_cache):
  pair, tf ('15m'|'1h'|'4h'|'1d'), open_ms, ema20, ema50, trend, close, cached_at

TTL: 48h автоматический cleanup через index.

API:
  fill_pair_trend(pair, tfs=...)  — фетчит klines + EMAs + Mongo upsert
  bulk_get_trend_for_items(items) — bulk read для журнала
  fill_pair_trend_async(pair)     — fire-and-forget на новый сигнал
"""
from __future__ import annotations
import logging
import time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Warmup для EMA20/EMA50: достаточно для last 24h сигналов + 50 баров warmup
# Слишком большие значения убивают CDN throughput (120 дней × 1d × 500 пар = blocked).
TF_WARMUP_HOURS = {
    '15m': 96,            # 4 дня = 384 баров
    '1h':  144,           # 6 дней = 144 баров
    '4h':  25 * 24,       # 25 дней = 150 баров (50 EMA + warmup)
    '1d':  90 * 24,       # 90 дней (для EMA50 на дневке)
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
        col = _get_db().signal_trend_cache
        col.create_index([('pair', 1), ('tf', 1), ('open_ms', 1)], unique=True)
        col.create_index('cached_at', expireAfterSeconds=2 * 24 * 3600)  # 48h
    except Exception as e:
        logger.debug(f'[trend-cache] index ensure: {e}')


_indexes_ready = False


def _compute_ema_series(closes: list, period: int) -> list:
    """EMA series — len(closes) длиной, None для warmup (i<period-1)."""
    if not closes or len(closes) < period:
        return [None] * len(closes)
    out = [None] * len(closes)
    # SMA на первые `period` точках как seed
    seed = sum(closes[:period]) / period
    out[period - 1] = seed
    k = 2 / (period + 1)
    for i in range(period, len(closes)):
        out[i] = closes[i] * k + out[i-1] * (1 - k)
    return out


def _trend_from_emas(ema20: float, ema50: float, close: float) -> str:
    if ema20 is None or ema50 is None:
        return 'UNKNOWN'
    diff_pct = abs(ema20 - ema50) / max(close, 1e-9) * 100
    if diff_pct < 0.05:  # < 5bps шум
        return 'FLAT'
    return 'UP' if ema20 > ema50 else 'DOWN'


def fill_pair_trend(pair: str, tfs: tuple = ('15m', '1h', '4h', '1d')) -> dict:
    """Фетчит klines + считает EMA20/EMA50 + Trend + пишет в Mongo."""
    from delta_calculator import fetch_klines_cdn, _normalize_symbol
    from database import _get_db
    from pymongo import UpdateOne
    global _indexes_ready
    if not _indexes_ready:
        _ensure_indexes()
        _indexes_ready = True
    sym = _normalize_symbol(pair)
    if not sym:
        return {}
    col = _get_db().signal_trend_cache
    now_ms = int(time.time() * 1000)
    now_dt = datetime.now(timezone.utc)
    result = {}
    for tf in tfs:
        warmup_h = TF_WARMUP_HOURS.get(tf, 100)
        start = now_ms - warmup_h * 3600 * 1000
        end = now_ms + 60 * 1000
        try:
            kl = fetch_klines_cdn(sym, tf, start, end)
        except Exception:
            kl = []
        if not kl or len(kl) < 55:
            result[tf] = 0
            continue
        closes = [float(k[4]) for k in kl]
        ema20 = _compute_ema_series(closes, 20)
        ema50 = _compute_ema_series(closes, 50)
        ops = []
        for i, k in enumerate(kl):
            if ema20[i] is None or ema50[i] is None:
                continue
            c = closes[i]
            tr = _trend_from_emas(ema20[i], ema50[i], c)
            try:
                ops.append(UpdateOne(
                    {'pair': pair, 'tf': tf, 'open_ms': int(k[0])},
                    {'$set': {
                        'pair': pair, 'tf': tf, 'open_ms': int(k[0]),
                        'ema20': round(ema20[i], 8),
                        'ema50': round(ema50[i], 8),
                        'close': c,
                        'trend': tr,
                        'cached_at': now_dt,
                    }},
                    upsert=True,
                ))
            except Exception:
                continue
        if ops:
            try:
                col.bulk_write(ops, ordered=False)
                result[tf] = len(ops)
            except Exception as e:
                logger.debug(f'[trend-cache] bulk_write {pair}/{tf}: {e}')
                result[tf] = 0
        else:
            result[tf] = 0
    return result


async def fill_pair_trend_async(pair: str, tfs: tuple = ('15m', '1h', '4h', '1d')) -> None:
    """Fire-and-forget. Используется хуком signal creation."""
    import asyncio
    try:
        await asyncio.to_thread(fill_pair_trend, pair, tfs)
    except Exception as e:
        logger.debug(f'[trend-cache] async fill {pair}: {e}')


def bulk_get_trend_for_items(items: list) -> None:
    """Bulk read для journal items. Проставляет trend_15m/1h/4h/1d."""
    if not items:
        return
    try:
        from database import _get_db
        col = _get_db().signal_trend_cache
    except Exception:
        return
    wanted = set()
    item_keys = []
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
    chunks = list(wanted)
    cached_map = {}  # (pair, tf, open_ms) → trend
    CHUNK = 500
    for i in range(0, len(chunks), CHUNK):
        conds = [{'pair': p, 'tf': t, 'open_ms': om}
                 for (p, t, om) in chunks[i:i+CHUNK]]
        try:
            for doc in col.find({'$or': conds},
                                {'pair':1,'tf':1,'open_ms':1,
                                 'trend':1,'_id':0}):
                cached_map[(doc['pair'], doc['tf'], doc['open_ms'])] = \
                    doc.get('trend')
        except Exception:
            pass
    for it, pair, keys in item_keys:
        for tf, open_ms in keys:
            tr = cached_map.get((pair, tf, open_ms))
            if tr:
                it[f'trend_{tf}'] = tr
