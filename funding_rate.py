"""Funding Rate module — leading indicator детект скоса leverage.

Идея: отрицательный funding rate при flat цене = шорты паркуются →
готов short squeeze. Lead time 6-12h.

API: Binance fapi /fapi/v1/premiumIndex (real-time current rate)
     Binance fapi /fapi/v1/fundingRate (history)

Использование:
  from funding_rate import get_funding_metrics
  m = get_funding_metrics('FIDA/USDT')
  # → {'funding_current_pct': -0.012, 'funding_avg_24h_pct': ...,
  #    'is_short_skew': True, 'is_long_skew': False}
"""
from __future__ import annotations
import logging
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

_cache: dict = {}
_CACHE_TTL_S = 600       # 10 мин
_NEGATIVE_TTL_S = 300

FAPI = 'https://fapi.binance.com'
_http = httpx.Client(timeout=8.0, limits=httpx.Limits(max_connections=20, max_keepalive_connections=10))


def _to_fapi_symbol(pair: str) -> str:
    s = pair.replace('/', '').upper()
    if not s.endswith('USDT'): s += 'USDT'
    return s


def get_funding_metrics(pair: str) -> dict:
    """Returns {
        'funding_current_pct': float,    # текущий rate в %
        'funding_next_at_ms': int,       # время следующей выплаты
        'funding_avg_24h_pct': float,    # средний за 24h (3 funding события)
        'funding_avg_7d_pct': float,
        'is_short_skew': bool,           # funding ≤ -0.02% (shorts паркуются)
        'is_long_skew': bool,            # funding ≥ +0.05% (longs перегружены)
        'is_neutral': bool,
        'cached_age_s': int,
    }"""
    now = int(time.time())
    entry = _cache.get(pair)
    if entry:
        ttl = _NEGATIVE_TTL_S if entry.get('error') else _CACHE_TTL_S
        if (now - entry.get('ts', 0)) < ttl:
            return {**entry, 'cached_age_s': now - entry['ts']}

    result = {
        'funding_current_pct': 0.0, 'funding_next_at_ms': 0,
        'funding_avg_24h_pct': 0.0, 'funding_avg_7d_pct': 0.0,
        'is_short_skew': False, 'is_long_skew': False, 'is_neutral': True,
    }

    symbol = _to_fapi_symbol(pair)
    try:
        # 1. Current rate via premiumIndex
        r = _http.get(f'{FAPI}/fapi/v1/premiumIndex', params={'symbol': symbol})
        if r.status_code == 200:
            d = r.json()
            current = float(d.get('lastFundingRate', 0)) * 100  # to %
            result['funding_current_pct'] = round(current, 4)
            result['funding_next_at_ms'] = int(d.get('nextFundingTime', 0))

        # 2. History (last 21 funding events = ~7 days, 3/day)
        r2 = _http.get(f'{FAPI}/fapi/v1/fundingRate',
                       params={'symbol': symbol, 'limit': 21})
        if r2.status_code == 200:
            hist = r2.json() or []
            rates = [float(h.get('fundingRate', 0)) * 100 for h in hist]
            if rates:
                # last 3 events = ~24h
                last_24h = rates[-3:] if len(rates) >= 3 else rates
                result['funding_avg_24h_pct'] = round(sum(last_24h) / len(last_24h), 4)
                # all 21 events = 7d
                result['funding_avg_7d_pct'] = round(sum(rates) / len(rates), 4)

        # Classification
        cur = result['funding_current_pct']
        avg24 = result['funding_avg_24h_pct']
        # Используем avg24 для шортовой/лонговой skew detection
        # (текущий может быть spike, среднее надёжнее)
        if avg24 <= -0.02:
            result['is_short_skew'] = True
            result['is_neutral'] = False
        elif avg24 >= 0.05:
            result['is_long_skew'] = True
            result['is_neutral'] = False

    except Exception as e:
        logger.debug(f'[funding_rate] {pair}: {e}')
        result['error'] = True

    _cache[pair] = {**result, 'ts': now}
    return {**result, 'cached_age_s': 0}


def clear_cache():
    _cache.clear()
