"""Shared RSI/SMA(RSI) 4h STATE cache. Parallel module to rsi12h_state.

Используется в:
- admin._compute_journal_sync — для UI колонки "4h"
- (опционально) paper_trader.on_signal — second confirmation layer

Backtest analysis показал что 4h state даёт complementary signal к 12h
(faster reaction, shorter swings).
"""
from __future__ import annotations
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

_cache: dict = {}
_CACHE_TTL_S = 1800   # 30 мин — 4h candle меняется быстрее чем 12h
_NEGATIVE_TTL_S = 300  # 5 мин если fapi banned


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
    rsi[p] = 100.0 if avg_l == 0 else (100 - 100 / (1 + avg_g / avg_l))
    for i in range(p+1, n):
        avg_g = (avg_g * (p-1) + gains[i]) / p
        avg_l = (avg_l * (p-1) + losses[i]) / p
        rsi[i] = 100.0 if avg_l == 0 else (100 - 100 / (1 + avg_g / avg_l))
    return rsi


def get_state(pair: str) -> dict:
    """Returns {'state': 'bullish'|'bearish'|'neutral'|None,
                'rsi': float|None, 'sma': float|None, 'cached_age_s': int}
    """
    now = int(time.time())
    entry = _cache.get(pair)
    if entry:
        ttl = _NEGATIVE_TTL_S if entry.get('state') is None else _CACHE_TTL_S
        if (now - entry.get('ts', 0)) < ttl:
            return {**entry, 'cached_age_s': now - entry['ts']}
    try:
        from divergence import _fetch_klines_fapi
        kl = _fetch_klines_fapi(pair, '4h', 50)
        if not kl or len(kl) < 30:
            result = {'state': None, 'rsi': None, 'sma': None}
        else:
            closes = [float(k[4]) for k in kl]
            rsi = _compute_rsi(closes, 14)
            if rsi[-1] is None:
                result = {'state': None, 'rsi': None, 'sma': None}
            else:
                vals = [v for v in rsi[-14:] if v is not None]
                if len(vals) < 14:
                    result = {'state': None, 'rsi': None, 'sma': None}
                else:
                    sma_last = sum(vals) / len(vals)
                    r_last = rsi[-1]
                    if r_last > sma_last: st = 'bullish'
                    elif r_last < sma_last: st = 'bearish'
                    else: st = 'neutral'
                    result = {'state': st, 'rsi': round(r_last, 1),
                              'sma': round(sma_last, 1)}
    except Exception as e:
        logger.debug(f'[rsi4h] compute {pair}: {e}')
        result = {'state': None, 'rsi': None, 'sma': None}
    _cache[pair] = {**result, 'ts': now}
    return {**result, 'cached_age_s': 0}


def check_direction_match(pair: str, direction: str) -> tuple[bool, dict]:
    info = get_state(pair)
    state = info.get('state')
    d = (direction or '').upper()
    if state is None: return (True, info)
    if state == 'bullish' and d == 'LONG': return (True, info)
    if state == 'bearish' and d == 'SHORT': return (True, info)
    if state == 'neutral': return (True, info)
    return (False, info)


def clear_cache():
    _cache.clear()
