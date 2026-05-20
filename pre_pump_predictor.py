"""Pre-Pump Predictor — composite score 0-100 на основе leading indicators.

Объединяет:
  - Volume Profile (25%): объём аккумулируется на flat цене
  - Open Interest (20%): позиции набираются
  - Funding Rate (15%): shorts паркуются (short skew)
  - BB Squeeze (15%): сжатие готов к взрыву
  - Price Flat (10%): цена стабильна = накопление
  - Sector Rotation (10%): 2+ пары темы активны
  - RSI Compression (5%): низкая волатильность RSI = энергия копится

Tiers:
  🔥 PRIME    score ≥ 75   1-3 в день
  🟡 STRONG   60-74        5-10 в день
  🟢 WATCH    45-59        15-30 в день
"""
from __future__ import annotations
import logging
import time

logger = logging.getLogger(__name__)


def _calc_rsi(closes: list[float], p: int = 14) -> list:
    n = len(closes)
    if n < p + 1: return [None] * n
    rsi: list = [None] * n
    gains = [0.0]; losses = [0.0]
    for i in range(1, n):
        ch = closes[i] - closes[i-1]
        gains.append(max(ch, 0.0)); losses.append(max(-ch, 0.0))
    avg_g = sum(gains[1:p+1]) / p
    avg_l = sum(losses[1:p+1]) / p
    rsi[p] = 100.0 if avg_l == 0 else (100 - 100 / (1 + avg_g / avg_l))
    for i in range(p+1, n):
        avg_g = (avg_g * (p-1) + gains[i]) / p
        avg_l = (avg_l * (p-1) + losses[i]) / p
        rsi[i] = 100.0 if avg_l == 0 else (100 - 100 / (1 + avg_g / avg_l))
    return rsi


def _compute_bb_squeeze_pct(closes: list[float], period: int = 20) -> float:
    """BB width percentile — где текущая ширина в распределении последних 100 баров.
    Returns 0-100 (низкий percentile = squeeze)."""
    n = len(closes)
    if n < period + 100: return 50.0  # neutral

    def _bb_width_at(idx):
        if idx < period - 1: return None
        vals = closes[idx-period+1:idx+1]
        mean = sum(vals) / len(vals)
        var = sum((v - mean) ** 2 for v in vals) / len(vals)
        std = var ** 0.5
        return (4 * std) / mean if mean > 0 else 0

    widths = [_bb_width_at(i) for i in range(n - 100, n)]
    widths = [w for w in widths if w is not None]
    if not widths: return 50.0

    current = widths[-1]
    sorted_w = sorted(widths)
    # Find percentile
    below = sum(1 for w in sorted_w if w < current)
    pct = below / len(sorted_w) * 100
    return pct


def _compute_rsi_compression(closes: list[float]) -> float:
    """RSI volatility за last 30 баров (std). Низкий = compression."""
    rsi = _calc_rsi(closes, 14)
    recent = [r for r in rsi[-30:] if r is not None]
    if len(recent) < 10: return 50.0
    mean = sum(recent) / len(recent)
    var = sum((r - mean) ** 2 for r in recent) / len(recent)
    std = var ** 0.5
    # Низкая std (≤8) = compression. Высокая (≥20) = volatile.
    # Normalize: std=5 → 80 pts, std=20 → 20 pts
    return max(0, min(100, 100 - std * 4))


def predict_pair(pair: str, sector_active: bool = False) -> dict:
    """Returns composite predictive score 0-100 with breakdown.

    Score weights:
      volume_score   25%  (vol_24h vs avg_7d ≥ 2.0)
      oi_growth      20%  (OI ≥ +20% за 24h)
      funding_skew   15%  (short_skew or strongly long_skew)
      bb_squeeze     15%  (BB width ≤ 20 percentile)
      price_flat     10%  (volatility ≤ 3%, change ≤ 4%)
      sector_active  10%  (передаётся внешне после batch sector detection)
      rsi_comp        5%  (RSI std ≤ 8)
    """
    score = 0.0
    components = {}
    direction = 'LONG'  # default — большинство pre-pump = LONG accumulation

    try:
        # 1. Volume Profile (25 pts)
        from volume_profile import get_volume_metrics
        vm = get_volume_metrics(pair)
        vol_score = vm.get('volume_score', 0)
        is_flat = vm.get('is_flat_price', False)
        # Volume score: 1.0=neutral, 2.0=accumulating, 3.0=hot, 4+=extreme
        if vol_score >= 4.0:
            components['volume'] = 25
        elif vol_score >= 3.0:
            components['volume'] = 20
        elif vol_score >= 2.0:
            components['volume'] = 15
        elif vol_score >= 1.5:
            components['volume'] = 8
        else:
            components['volume'] = 0
        score += components['volume']
    except Exception as e:
        logger.debug(f'[predictor] volume {pair}: {e}')
        components['volume'] = 0
        vm = {}

    try:
        # 2. Open Interest (20 pts)
        from open_interest import get_oi_metrics
        oi = get_oi_metrics(pair)
        oi_growth = oi.get('oi_change_24h_pct', 0)
        if oi_growth >= 50:
            components['oi'] = 20
        elif oi_growth >= 30:
            components['oi'] = 15
        elif oi_growth >= 20:
            components['oi'] = 10
        elif oi_growth >= 10:
            components['oi'] = 5
        else:
            components['oi'] = 0
        score += components['oi']
    except Exception as e:
        logger.debug(f'[predictor] oi {pair}: {e}')
        components['oi'] = 0
        oi = {}

    try:
        # 3. Funding Rate Skew (15 pts)
        from funding_rate import get_funding_metrics
        fr = get_funding_metrics(pair)
        funding_avg = fr.get('funding_avg_24h_pct', 0)
        # Short skew (negative) — fuel for LONG squeeze
        if funding_avg <= -0.05:
            components['funding'] = 15
            direction = 'LONG'  # short squeeze setup
        elif funding_avg <= -0.02:
            components['funding'] = 10
            direction = 'LONG'
        # Long skew (very positive) — fuel for SHORT squeeze
        elif funding_avg >= 0.10:
            components['funding'] = 12
            direction = 'SHORT'
        elif funding_avg >= 0.05:
            components['funding'] = 7
            direction = 'SHORT'
        else:
            components['funding'] = 0
        score += components['funding']
    except Exception as e:
        logger.debug(f'[predictor] funding {pair}: {e}')
        components['funding'] = 0
        fr = {}

    try:
        # 4. BB Squeeze + RSI Compression (15 + 5 = 20 pts)
        from divergence import _fetch_klines_fapi
        kl = _fetch_klines_fapi(pair, '1h', 150)
        if kl and len(kl) >= 130:
            closes = [k[4] for k in kl]
            bb_pct = _compute_bb_squeeze_pct(closes)
            # Низкий percentile = squeeze = высокий score
            if bb_pct <= 10:
                components['bb_squeeze'] = 15
            elif bb_pct <= 20:
                components['bb_squeeze'] = 10
            elif bb_pct <= 30:
                components['bb_squeeze'] = 5
            else:
                components['bb_squeeze'] = 0
            score += components['bb_squeeze']

            rsi_comp = _compute_rsi_compression(closes)
            components['rsi_comp'] = round(rsi_comp / 100 * 5, 1)
            score += components['rsi_comp']
        else:
            components['bb_squeeze'] = 0
            components['rsi_comp'] = 0
    except Exception as e:
        logger.debug(f'[predictor] bb/rsi {pair}: {e}')
        components['bb_squeeze'] = 0
        components['rsi_comp'] = 0

    # 5. Price Flat bonus (10 pts) — только если volume также накапливается
    if vm.get('is_flat_price'):
        components['price_flat'] = 10
    elif vm.get('price_volatility_24h', 1) < 0.05:
        components['price_flat'] = 5
    else:
        components['price_flat'] = 0
    score += components['price_flat']

    # 6. Sector Active (10 pts) — передаётся внешне
    components['sector'] = 10 if sector_active else 0
    score += components['sector']

    # Determine tier
    score = round(score, 1)
    if score >= 75: tier = 'PRIME'
    elif score >= 60: tier = 'STRONG'
    elif score >= 45: tier = 'WATCH'
    else: tier = 'norm'

    return {
        'pair': pair,
        'composite_score': score,
        'tier': tier,
        'direction': direction,
        'components': components,
        'volume_data': vm,
        'oi_data': oi if oi else {},
        'funding_data': fr if fr else {},
        'at_ts': int(time.time()),
    }


def predict_bulk(pairs: list, sector_active_pairs: set = None) -> list:
    """Batch predict для списка пар. Returns list of results."""
    sector_set = sector_active_pairs or set()
    out = []
    for p in pairs:
        try:
            res = predict_pair(p, sector_active=(p in sector_set))
            out.append(res)
        except Exception as e:
            logger.debug(f'[predictor] {p}: {e}')
    return out
