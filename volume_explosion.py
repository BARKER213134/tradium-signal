"""Volume Explosion scanner — детектор всплеска положительного объёма
для mid-cap монет.

Юзер-спецификация (13.05.26):
1. 24h Volume в USDT в окне [MIN, MAX] (mid-cap, default 20M-500M)
2. 1h positive delta volume > MIN_1H_POS_DELTA_USDT (default 1M USDT)
3. 30m positive delta volume вырос на ≥ MIN_30M_GROWTH_PCT% (default 1000% = 10×)

Логика: ищем монеты где недавно (последние 30 мин) резко выросло давление
покупателей, при этом монета достаточно крупная (≥20M в день) но не топ
(<500M — топ редко даёт x10 движения за час).
"""
from __future__ import annotations
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


# ─── Конфигурация (можно менять без рестарта через update_config) ────
CONFIG = {
    'min_24h_vol_usdt':       20_000_000,    # ≥ $20M в день
    'max_24h_vol_usdt':      500_000_000,    # < $500M
    'min_1h_pos_delta_usdt':   1_000_000,    # ≥ $1M positive delta за час
    'min_30m_growth_pct':           1000.0,  # +1000% на 30m (10×)
    'scan_interval_s':              300,     # 5 мин между сканами
    'dedup_window_min':              60,     # не дублировать на одну пару чаще раз в час
}


def update_config(**kwargs):
    """Update CONFIG values on-the-fly. Возвращает новый CONFIG."""
    for k, v in kwargs.items():
        if k in CONFIG:
            CONFIG[k] = v
    return dict(CONFIG)


# ─── Helpers ─────────────────────────────────────────────────────────
def _get_recent_15m_deltas(pair: str, n_bars: int = 8) -> list:
    """Last N 15m candles from cluster_delta. Returns list of docs sorted by open_ms desc."""
    try:
        from database import _get_db
        col = _get_db().cluster_delta
        # Last n_bars hours' worth of 15m candles
        cutoff_ms = int(time.time() * 1000) - n_bars * 15 * 60 * 1000
        docs = list(col.find({
            'pair': pair, 'tf': '15m',
            'open_ms': {'$gte': cutoff_ms},
        }, {'open_ms': 1, 'delta_pct': 1, 'buy_vol': 1, 'sell_vol': 1, '_id': 0})
            .sort('open_ms', -1).limit(n_bars))
        return docs
    except Exception as e:
        logger.debug(f'[vol-explosion] get 15m fail {pair}: {e}')
        return []


def _get_1h_delta(pair: str) -> Optional[dict]:
    """Latest 1h cluster_delta doc."""
    try:
        from database import _get_db
        col = _get_db().cluster_delta
        doc = col.find_one({'pair': pair, 'tf': '1h'},
                           sort=[('open_ms', -1)],
                           projection={'open_ms': 1, 'delta_pct': 1,
                                       'buy_vol': 1, 'sell_vol': 1, '_id': 0})
        return doc
    except Exception:
        return None


def _check_pair(pair: str, vol_24h_usdt: float, current_price: float) -> Optional[dict]:
    """Проверяет одну пару на все условия.
    vol_24h_usdt — quoteVolume из BingX ticker (24h USDT volume).
    current_price — current price (для конвертации buy_vol в USDT).
    Returns: dict с trigger details если matched, иначе None.
    """
    # 1. 24h volume в окне
    if not (CONFIG['min_24h_vol_usdt'] <= vol_24h_usdt < CONFIG['max_24h_vol_usdt']):
        return None

    # 2. 1h positive delta volume > min
    h1 = _get_1h_delta(pair)
    if not h1:
        return None
    buy_1h = float(h1.get('buy_vol', 0) or 0)
    sell_1h = float(h1.get('sell_vol', 0) or 0)
    # cluster_delta stores volumes в base currency (coins), не USDT.
    # Конвертируем через current price: pos_delta_base * price = USDT
    pos_delta_base = buy_1h - sell_1h
    if pos_delta_base <= 0:
        return None  # net продавцы — не интересует
    pos_delta_usdt_1h = pos_delta_base * current_price
    if pos_delta_usdt_1h < CONFIG['min_1h_pos_delta_usdt']:
        return None

    # 3. 30m growth: last 2 15m candles vs prev 2 15m candles
    deltas = _get_recent_15m_deltas(pair, n_bars=4)
    if len(deltas) < 4:
        return None
    # deltas[0] = newest, deltas[3] = oldest
    last_30m_base  = sum(max(0, float(d.get('buy_vol', 0) or 0) - float(d.get('sell_vol', 0) or 0))
                         for d in deltas[:2])
    prev_30m_base  = sum(max(0, float(d.get('buy_vol', 0) or 0) - float(d.get('sell_vol', 0) or 0))
                         for d in deltas[2:4])
    last_30m_usdt = last_30m_base * current_price
    prev_30m_usdt = prev_30m_base * current_price
    # Growth % = (last - prev) / max(prev, 1) * 100
    # При prev=0 — INF; ставим porog $1k чтобы не было div-by-zero noise
    if prev_30m_usdt < 1000:
        growth_pct = float('inf') if last_30m_usdt >= 100_000 else 0
    else:
        growth_pct = (last_30m_usdt - prev_30m_usdt) / prev_30m_usdt * 100
    if growth_pct < CONFIG['min_30m_growth_pct']:
        return None

    return {
        'pair': pair,
        'price': current_price,
        'vol_24h_usdt': round(vol_24h_usdt, 0),
        'pos_delta_usdt_1h': round(pos_delta_usdt_1h, 0),
        'last_30m_pos_usdt': round(last_30m_usdt, 0),
        'prev_30m_pos_usdt': round(prev_30m_usdt, 0),
        'growth_30m_pct': round(growth_pct, 1) if growth_pct != float('inf') else 9999.0,
        'detected_at': datetime.now(timezone.utc),
    }


def scan() -> list[dict]:
    """Полный скан: BingX tickers → фильтр volume → check_pair per pair.
    Returns: list of trigger dicts.
    """
    triggers = []
    try:
        import ccxt
        ex = ccxt.bingx({'options': {'defaultType': 'swap'}, 'enableRateLimit': True})
        tickers = ex.fetch_tickers()
    except Exception as e:
        logger.warning(f'[vol-explosion] tickers fetch fail: {e}')
        return []

    for sym, t in (tickers or {}).items():
        if not sym.endswith(':USDT'):
            continue
        base = sym.split(':')[0]  # 'BTC/USDT:USDT' → 'BTC/USDT'
        if '/USDT' not in base:
            continue
        try:
            qvol = float(t.get('quoteVolume') or 0)
            price = float(t.get('last') or 0)
            if qvol <= 0 or price <= 0:
                continue
            # Quick filter: skip pairs outside volume band BEFORE Mongo queries
            if not (CONFIG['min_24h_vol_usdt'] <= qvol < CONFIG['max_24h_vol_usdt']):
                continue
            result = _check_pair(base, qvol, price)
            if result:
                triggers.append(result)
        except Exception:
            continue

    if triggers:
        logger.info(f'[vol-explosion] {len(triggers)} triggers: '
                    f'{[t["pair"] for t in triggers[:10]]}')
    return triggers


def emit_signal(trigger: dict) -> bool:
    """Запись сигнала в Mongo + dedup check.
    Returns: True если эмитится новый, False если дубликат в окне dedup_window_min.
    """
    try:
        from database import _get_db
        db = _get_db()
        col = db.volume_explosion_signals
        # Indexes
        try:
            col.create_index([('pair', 1), ('detected_at', -1)])
            col.create_index([('detected_at', -1)])
        except Exception:
            pass
        # Dedup
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=CONFIG['dedup_window_min'])
        exists = col.find_one({'pair': trigger['pair'], 'detected_at': {'$gte': cutoff}})
        if exists:
            return False
        doc = {**trigger, 'source': 'vol_explosion'}
        col.insert_one(doc)
        logger.info(
            f"[vol-explosion] EMIT {trigger['pair']} "
            f"24h=${trigger['vol_24h_usdt']/1e6:.1f}M  "
            f"1h_pos=${trigger['pos_delta_usdt_1h']/1e6:.2f}M  "
            f"30m_grow={trigger['growth_30m_pct']:.0f}%"
        )
        return True
    except Exception as e:
        logger.warning(f'[vol-explosion] emit fail: {e}')
        return False
