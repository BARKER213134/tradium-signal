"""Shared EMA50/EMA200 cross state cache на 1h.

Возвращает последний cross (golden/death) до текущего момента для пары.
Используется в admin._compute_journal_sync для UI колонки "EMA Cross".

По бэктесту 7d (см. _bt_ema50_ema200_1h.py): 80% WR на death cross SHORT
в bear-market неделе → ценный фильтр для определения regime.
"""
from __future__ import annotations
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

_cache: dict = {}
_CACHE_TTL_S = 1800   # 30 мин — 1h candle меняется быстрее чем EMA рассчёт
_NEGATIVE_TTL_S = 300  # 5 мин если fapi banned/error


def _ema(values: list[float], period: int) -> list:
    n = len(values)
    if n < period: return [None] * n
    out: list = [None] * n
    seed = sum(values[:period]) / period
    out[period - 1] = seed
    k = 2 / (period + 1)
    for i in range(period, n):
        out[i] = values[i] * k + out[i - 1] * (1 - k)
    return out


def get_state(pair: str, at_ts: Optional[int] = None) -> dict:
    """Returns {'last_cross': 'GOLDEN'|'DEATH'|None,
                'bars_ago': int|None,
                'cross_t': int|None,  # ms epoch when cross happened
                'ema50': float|None, 'ema200': float|None,
                'cached_age_s': int}

    at_ts: если задан (unix sec) — ищем последний cross на момент at_ts
           (не latest available). Используется для исторических сигналов
           чтобы видеть какой cross был активен ПРИ приходе сигнала.
    """
    now = int(time.time())
    # Cache key учитывает at_ts с гранулярностью 1h (точность достаточная для 1h tf)
    if at_ts:
        cache_key = f"{pair}|{at_ts // 3600}"
    else:
        cache_key = pair
    entry = _cache.get(cache_key)
    if entry:
        ttl = _NEGATIVE_TTL_S if entry.get('last_cross') is None and entry.get('error') else _CACHE_TTL_S
        # Для at_ts кэш можно держать дольше — данные не меняются в прошлом
        if at_ts: ttl = 3600  # 1h
        if (now - entry.get('ts', 0)) < ttl:
            return {**entry, 'cached_age_s': now - entry['ts']}

    try:
        from divergence import _get_klines  # fapi → BingX fallback
        # 500 баров 1h = 20 дней — хватает на EMA200 (warmup) + recent crosses
        kl = _get_klines(pair, '1h', 500)
        if not kl or len(kl) < 220:
            result = {'last_cross': None, 'bars_ago': None, 'cross_t': None,
                      'ema50': None, 'ema200': None, 'error': True}
        else:
            # Отрезаем klines до at_ts если задан
            if at_ts:
                at_ms = at_ts * 1000
                kl = [k for k in kl if int(k[0]) <= at_ms]
                if len(kl) < 220:
                    result = {'last_cross': None, 'bars_ago': None,
                              'cross_t': None, 'ema50': None, 'ema200': None,
                              'error': True}
                    _cache[cache_key] = {**result, 'ts': now}
                    return {**result, 'cached_age_s': 0}

            closes = [float(k[4]) for k in kl]
            ema50 = _ema(closes, 50)
            ema200 = _ema(closes, 200)

            last_cross = None
            cross_idx = None
            cross_t = None
            # Идём с конца назад, ищем последний cross
            for i in range(len(closes) - 1, 200, -1):
                f, s = ema50[i], ema200[i]
                fp, sp = ema50[i-1], ema200[i-1]
                if None in (f, s, fp, sp): continue
                if fp <= sp and f > s:
                    last_cross = 'GOLDEN'
                    cross_idx = i
                    cross_t = int(kl[i][0])
                    break
                if fp >= sp and f < s:
                    last_cross = 'DEATH'
                    cross_idx = i
                    cross_t = int(kl[i][0])
                    break

            bars_ago = (len(closes) - 1 - cross_idx) if cross_idx is not None else None
            result = {
                'last_cross': last_cross,
                'bars_ago': bars_ago,
                'cross_t': cross_t,
                'ema50': round(ema50[-1], 6) if ema50[-1] else None,
                'ema200': round(ema200[-1], 6) if ema200[-1] else None,
            }
    except Exception as e:
        logger.debug(f'[ema-cross] compute {pair}: {e}')
        result = {'last_cross': None, 'bars_ago': None, 'cross_t': None,
                  'ema50': None, 'ema200': None, 'error': True}

    _cache[cache_key] = {**result, 'ts': now}
    return {**result, 'cached_age_s': 0}


def check_direction_match(pair: str, direction: str,
                           at_ts: Optional[int] = None) -> tuple[bool, dict]:
    """True если last cross согласуется с direction:
      GOLDEN → LONG, DEATH → SHORT.
    None state → True (нейтрально, не блокируем).
    """
    info = get_state(pair, at_ts=at_ts)
    cross = info.get('last_cross')
    d = (direction or '').upper()
    if cross is None: return (True, info)
    if cross == 'GOLDEN' and d == 'LONG': return (True, info)
    if cross == 'DEATH' and d == 'SHORT': return (True, info)
    return (False, info)


def clear_cache():
    _cache.clear()
