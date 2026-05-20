"""Open Interest module — leading indicator детект набора позиций.

Идея: OI растёт на flat цене → позиции набираются → готов к движению.
Lead time 6-24h.

API: Binance fapi /futures/data/openInterestHist (period=1h, limit=N)
     Binance fapi /fapi/v1/openInterest (real-time current)

Использование:
  from open_interest import get_oi_metrics
  m = get_oi_metrics('FIDA/USDT')
  # → {'oi_current_usd': float, 'oi_24h_ago_usd': float,
  #    'oi_change_24h_pct': float, 'oi_change_7d_pct': float,
  #    'is_accumulating': bool}
"""
from __future__ import annotations
import logging
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

_cache: dict = {}
_CACHE_TTL_S = 600       # 10 мин (OI меняется быстрее volume)
_NEGATIVE_TTL_S = 300

FAPI = 'https://fapi.binance.com'
_http = httpx.Client(timeout=8.0, limits=httpx.Limits(max_connections=20, max_keepalive_connections=10))


def _to_fapi_symbol(pair: str) -> str:
    s = pair.replace('/', '').upper()
    if not s.endswith('USDT'): s += 'USDT'
    return s


def get_oi_metrics(pair: str) -> dict:
    """Returns {
        'oi_current_usd': float,        # текущий OI в USD
        'oi_24h_ago_usd': float,
        'oi_change_24h_pct': float,     # %
        'oi_change_7d_pct': float,
        'oi_avg_7d_usd': float,
        'is_accumulating': bool,         # OI растёт ≥20% за 24h
        'cached_age_s': int,
    }"""
    now = int(time.time())
    entry = _cache.get(pair)
    if entry:
        ttl = _NEGATIVE_TTL_S if entry.get('error') else _CACHE_TTL_S
        if (now - entry.get('ts', 0)) < ttl:
            return {**entry, 'cached_age_s': now - entry['ts']}

    result = {
        'oi_current_usd': 0.0, 'oi_24h_ago_usd': 0.0,
        'oi_change_24h_pct': 0.0, 'oi_change_7d_pct': 0.0,
        'oi_avg_7d_usd': 0.0, 'is_accumulating': False,
    }

    symbol = _to_fapi_symbol(pair)
    try:
        # 1h × 168 = 7 дней OI history
        r = _http.get(f'{FAPI}/futures/data/openInterestHist',
                      params={'symbol': symbol, 'period': '1h', 'limit': 168})
        if r.status_code != 200:
            result['error'] = True
            _cache[pair] = {**result, 'ts': now}
            return {**result, 'cached_age_s': 0}

        data = r.json()
        if not data or len(data) < 24:
            result['error'] = True
            _cache[pair] = {**result, 'ts': now}
            return {**result, 'cached_age_s': 0}

        # Парсим OI in USD (sumOpenInterestValue)
        oi_values = [float(d.get('sumOpenInterestValue', 0)) for d in data]
        oi_current = oi_values[-1]
        oi_24h_ago = oi_values[-24] if len(oi_values) >= 24 else oi_values[0]
        oi_first = oi_values[0]

        result['oi_current_usd'] = oi_current
        result['oi_24h_ago_usd'] = oi_24h_ago
        if oi_24h_ago > 0:
            result['oi_change_24h_pct'] = round((oi_current - oi_24h_ago) / oi_24h_ago * 100, 2)
        if oi_first > 0:
            result['oi_change_7d_pct'] = round((oi_current - oi_first) / oi_first * 100, 2)
        if oi_values:
            result['oi_avg_7d_usd'] = sum(oi_values) / len(oi_values)

        # Accumulation: OI ≥20% выше чем 24h назад
        result['is_accumulating'] = result['oi_change_24h_pct'] >= 20.0

    except Exception as e:
        logger.debug(f'[open_interest] {pair}: {e}')
        result['error'] = True

    _cache[pair] = {**result, 'ts': now}
    return {**result, 'cached_age_s': 0}


def clear_cache():
    _cache.clear()
