"""Volume Profile module — leading indicator детект накопления.

Идея: до pump'a объём НАКАПЛИВАЕТСЯ — volume_24h >> avg_7d при flat price.
Это leading indicator (lead time 12-48h).

Использование:
  from volume_profile import get_volume_metrics
  m = get_volume_metrics('FIDA/USDT')
  # → {'volume_24h_usd': 456M, 'volume_avg_7d': 80M,
  #    'volume_score': 5.7, 'volume_accel': 1.4, 'price_volatility_24h': 0.018}

volume_score = volume_24h / volume_avg_7d (растёт ↑ — накопление)
volume_accel = vol_today / vol_yesterday (быстрое ускорение)
price_volatility_24h = std(closes_24h) / mean — низкая = flat = накопление
"""
from __future__ import annotations
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

_cache: dict = {}
_CACHE_TTL_S = 1800      # 30 мин
_NEGATIVE_TTL_S = 300    # 5 мин для пар без данных


def _compute_std(values: list[float]) -> float:
    n = len(values)
    if n < 2: return 0.0
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / n
    return var ** 0.5


def get_volume_metrics(pair: str) -> dict:
    """Returns {
        'volume_24h_usd': float,     # текущий 24h в USD
        'volume_avg_7d_usd': float,  # средний за 7 дней
        'volume_score': float,       # vol_24h / vol_avg_7d (≥2.0 = накопление)
        'volume_accel': float,       # vol_today / vol_yesterday
        'price_24h_change_pct': float,
        'price_volatility_24h': float,  # std/mean closes за 24h (≤3% = flat)
        'is_flat_price': bool,       # volatility < 3% И |price_change| < 4%
        'is_accumulating': bool,     # volume_score ≥ 2.0 И is_flat_price
        'cached_age_s': int,
    }"""
    now = int(time.time())
    entry = _cache.get(pair)
    if entry:
        ttl = _NEGATIVE_TTL_S if entry.get('error') else _CACHE_TTL_S
        if (now - entry.get('ts', 0)) < ttl:
            return {**entry, 'cached_age_s': now - entry['ts']}

    result = {
        'volume_24h_usd': 0.0, 'volume_avg_7d_usd': 0.0,
        'volume_score': 0.0, 'volume_accel': 0.0,
        'price_24h_change_pct': 0.0, 'price_volatility_24h': 0.0,
        'is_flat_price': False, 'is_accumulating': False,
    }

    try:
        from divergence import _fetch_klines_fapi
        # 1h × 168 баров = 7 дней volume для усреднения
        kl = _fetch_klines_fapi(pair, '1h', 200)
        if not kl or len(kl) < 168:
            result['error'] = True
            _cache[pair] = {**result, 'ts': now}
            return {**result, 'cached_age_s': 0}

        # USD volume = price × volume (close × volume per candle)
        # kl format: (t, o, h, l, c, v)
        usd_vols = [k[4] * k[5] for k in kl]  # close × volume in base units → USD value

        # Volume 24h (last 24 candles)
        vol_24h = sum(usd_vols[-24:])
        # Volume yesterday (24-48h)
        vol_yesterday = sum(usd_vols[-48:-24]) if len(usd_vols) >= 48 else vol_24h
        # Volume avg 7d (168 candles, divided into 7 daily blocks)
        vol_7d_total = sum(usd_vols[-168:])
        vol_avg_7d = vol_7d_total / 7.0

        result['volume_24h_usd'] = vol_24h
        result['volume_avg_7d_usd'] = vol_avg_7d
        if vol_avg_7d > 0:
            result['volume_score'] = round(vol_24h / vol_avg_7d, 2)
        if vol_yesterday > 0:
            result['volume_accel'] = round(vol_24h / vol_yesterday, 2)

        # Price metrics last 24h
        closes_24h = [k[4] for k in kl[-24:]]
        if closes_24h:
            mean = sum(closes_24h) / len(closes_24h)
            std = _compute_std(closes_24h)
            result['price_volatility_24h'] = round(std / mean, 4) if mean > 0 else 0
            # 24h change
            first = closes_24h[0]; last = closes_24h[-1]
            if first > 0:
                result['price_24h_change_pct'] = round((last - first) / first * 100, 2)

        # is_flat_price: volatility < 3% AND |change| < 4%
        is_flat = (result['price_volatility_24h'] < 0.03
                   and abs(result['price_24h_change_pct']) < 4.0)
        result['is_flat_price'] = is_flat
        result['is_accumulating'] = (is_flat and result['volume_score'] >= 2.0)

    except Exception as e:
        logger.debug(f'[volume_profile] {pair}: {e}')
        result['error'] = True

    _cache[pair] = {**result, 'ts': now}
    return {**result, 'cached_age_s': 0}


def clear_cache():
    _cache.clear()
