"""Shared RSI/SMA(RSI) 12h STATE cache.

Используется в:
- admin._compute_journal_sync — для UI колонки "12h"
- paper_trader.on_signal — gate для блокировки сделок против 12h тренда

Backtest 14d показал: +14-17 WR пп для LONG signals если RSI>SMA на 12h,
+15-27 WR для SHORT если RSI<SMA. Universal filter.
"""
from __future__ import annotations
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

# Cache per pair: { pair: {'ts': int, 'state': str, 'rsi': float, 'sma': float} }
_cache: dict = {}
_CACHE_TTL_S = 3600  # 1 час — 12h candle slow


def _compute_rsi(closes: list[float], p: int = 14) -> list:
    n = len(closes)
    if n < p + 1: return [None] * n
    rsi = [None] * n
    gains = [0.0]; losses = [0.0]
    for i in range(1, n):
        ch = closes[i] - closes[i-1]
        gains.append(max(ch, 0.0))
        losses.append(max(-ch, 0.0))
    avg_g = sum(gains[1:p+1]) / p
    avg_l = sum(losses[1:p+1]) / p
    rsi[p] = 100.0 if avg_l == 0 else (100 - 100/(1 + avg_g/avg_l))
    for i in range(p+1, n):
        avg_g = (avg_g*(p-1) + gains[i]) / p
        avg_l = (avg_l*(p-1) + losses[i]) / p
        rsi[i] = 100.0 if avg_l == 0 else (100 - 100/(1 + avg_g/avg_l))
    return rsi


def get_state(pair: str) -> dict:
    """Returns {'state': 'bullish'|'bearish'|'neutral'|None,
                'rsi': float|None, 'sma': float|None,
                'cached_age_s': int}.
    state=None если нет данных или fapi banned.
    """
    now = int(time.time())
    entry = _cache.get(pair)
    if entry and (now - entry.get('ts', 0)) < _CACHE_TTL_S:
        return {**entry, 'cached_age_s': now - entry['ts']}

    # Fresh compute
    try:
        from divergence import _fetch_klines_fapi
        kl = _fetch_klines_fapi(pair, '12h', 50)
        if not kl or len(kl) < 30:
            result = {'state': None, 'rsi': None, 'sma': None}
        else:
            closes = [float(k[4]) for k in kl]
            rsi = _compute_rsi(closes, 14)
            if rsi[-1] is None:
                result = {'state': None, 'rsi': None, 'sma': None}
            else:
                # SMA(14) on last 14 RSI values
                vals = [v for v in rsi[-14:] if v is not None]
                if len(vals) < 14:
                    result = {'state': None, 'rsi': None, 'sma': None}
                else:
                    sma_last = sum(vals) / len(vals)
                    r_last = rsi[-1]
                    if r_last > sma_last:
                        st = 'bullish'
                    elif r_last < sma_last:
                        st = 'bearish'
                    else:
                        st = 'neutral'
                    result = {'state': st, 'rsi': round(r_last, 1),
                              'sma': round(sma_last, 1)}
    except Exception as e:
        logger.debug(f'[rsi12h] compute {pair}: {e}')
        result = {'state': None, 'rsi': None, 'sma': None}

    _cache[pair] = {**result, 'ts': now}
    return {**result, 'cached_age_s': 0}


def check_direction_match(pair: str, direction: str) -> tuple[bool, dict]:
    """Returns (match: bool, state_info: dict).
    match=True если state aligned with direction (или state=None — pass-through).
    """
    info = get_state(pair)
    state = info.get('state')
    d = (direction or '').upper()
    if state is None:
        # No data — не блокируем (avoid false rejections)
        return (True, info)
    if state == 'bullish' and d == 'LONG':
        return (True, info)
    if state == 'bearish' and d == 'SHORT':
        return (True, info)
    if state == 'neutral':
        # Neutral — пропускаем (нет явного против)
        return (True, info)
    return (False, info)


def clear_cache():
    """Clear all cached entries (для тестов)."""
    _cache.clear()
