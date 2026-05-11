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

# Какие TF и сколько часов истории нужно (warmup ~14 баров + signals window 24h)
TF_WARMUP_HOURS = {
    '15m': 24,    # 96 баров — RSI стабильно для last 1 day
    '1h':  48,    # 48 баров warmup
    '4h':  120,   # 30 баров warmup
    '1d':  35 * 24,  # 35 баров warmup (1 month history)
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


def fill_pair_rsi(pair: str, tfs: tuple = ('15m', '1h', '4h', '1d')) -> dict:
    """Фетчит klines + считает RSI + пишет в Mongo. Возвращает {tf: n_written}."""
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
        ops = []
        for i, k in enumerate(kl):
            if rsis[i] is None:
                continue
            try:
                ops.append(UpdateOne(
                    {'pair': pair, 'tf': tf, 'open_ms': int(k[0])},
                    {'$set': {
                        'pair': pair, 'tf': tf, 'open_ms': int(k[0]),
                        'rsi': round(rsis[i], 2),
                        'close': float(k[4]),
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
                logger.debug(f'[rsi-cache] bulk_write {pair}/{tf}: {e}')
                result[tf] = 0
        else:
            result[tf] = 0
    return result


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
    # Bulk find chunked
    chunks = list(wanted)
    cached_map = {}
    CHUNK = 500
    for i in range(0, len(chunks), CHUNK):
        conds = [{'pair': p, 'tf': t, 'open_ms': om}
                 for (p, t, om) in chunks[i:i+CHUNK]]
        try:
            for doc in col.find({'$or': conds},
                                {'pair':1,'tf':1,'open_ms':1,'rsi':1,'_id':0}):
                cached_map[(doc['pair'], doc['tf'], doc['open_ms'])] = doc.get('rsi')
        except Exception:
            pass
    # Apply to items
    for it, pair, keys in item_keys:
        for tf, open_ms in keys:
            r = cached_map.get((pair, tf, open_ms))
            if r is not None:
                it[f'rsi_{tf}'] = r
