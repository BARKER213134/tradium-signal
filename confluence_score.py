"""Confluence Score 0-100 — агрегированный сигнал силы setup'а.

Веса (sum=100):
  RSI divergence:        25 (bullish/bearish strength × 0.25)
  V-pattern:             15 (drop_pct + reversal_pct)
  Multi-TF SMA align:    15 (1d+4h+1h trend confluence)
  KL bounce (price-near-level): 15
  Stack count ≥ 4:       15
  Anomaly z-score:       10
  Volume above avg:       5

direction = LONG если bullish components доминируют, иначе SHORT.
"""
from __future__ import annotations
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def compute_score(item: dict) -> dict:
    """Compute confluence score for a journal item.

    item должен содержать поля enriched:
      - rsi_1h, sma_rsi_1h, rsi_4h, sma_rsi_4h, rsi_1d, sma_rsi_1d
      - trend_1h, trend_4h, trend_1d (CV format e.g. 'UP')
      - delta_zscore_15m, delta_zscore_1h
      - stack_distinct, stack_long, stack_short
      - _kl (KL bounce object) — optional
      - divergence (computed externally) — optional
      - v_pattern (computed externally) — optional

    Returns: {'score': int 0-100, 'direction': 'LONG'|'SHORT'|None,
              'components': dict (детали скоринга)}
    """
    bull_score = 0.0
    bear_score = 0.0
    components: dict = {}

    # 1. RSI Divergence (25 max)
    div = item.get('divergence') or {}
    if div:
        s = float(div.get('strength', 0))
        if div.get('type') == 'bullish':
            v = min(25, s / 100 * 25)
            bull_score += v
            components['divergence_bullish'] = round(v, 1)
        elif div.get('type') == 'bearish':
            v = min(25, s / 100 * 25)
            bear_score += v
            components['divergence_bearish'] = round(v, 1)

    # 2. V-pattern (15 max)
    vp = item.get('v_pattern') or {}
    if vp:
        drop = float(vp.get('drop_pct') or vp.get('rise_pct') or 0)
        rev = float(vp.get('reversal_pct', 0))
        s = min(15, drop * 1.5 + rev * 2)
        if vp.get('type') == 'v_bottom':
            bull_score += s
            components['v_bottom'] = round(s, 1)
        elif vp.get('type') == 'v_top':
            bear_score += s
            components['v_top'] = round(s, 1)

    # 3. Multi-TF SMA align (15 max)
    def _sma_pos(tf):
        r = item.get(f'rsi_{tf}')
        m = item.get(f'sma_rsi_{tf}')
        if r is None or m is None:
            return None
        return 'bull' if r > m else 'bear'
    p1 = _sma_pos('1h'); p4 = _sma_pos('4h'); pd = _sma_pos('1d')
    sma_align = sum(1 for x in [p1, p4, pd] if x == 'bull')
    sma_anti  = sum(1 for x in [p1, p4, pd] if x == 'bear')
    if sma_align >= 2:
        v = 5 * sma_align
        bull_score += v
        components['sma_align_bull'] = round(v, 1)
    if sma_anti >= 2:
        v = 5 * sma_anti
        bear_score += v
        components['sma_align_bear'] = round(v, 1)

    # 4. Trend EMA per TF (component of sma_align actually but use separately too)
    def _trend(tf):
        t = item.get(f'trend_{tf}')
        if not t: return None
        t = t.upper()
        if 'UP' in t: return 'up'
        if 'DOWN' in t: return 'down'
        return None
    trend_align = 0
    trend_anti = 0
    for tf in ['1h','4h','1d']:
        t = _trend(tf)
        if t == 'up': trend_align += 1
        elif t == 'down': trend_anti += 1
    if trend_align >= 2:
        # already counted in sma — minor bonus
        components['trend_align_bull'] = trend_align
    if trend_anti >= 2:
        components['trend_align_bear'] = trend_anti

    # 5. KL bounce (15 max)
    kl = item.get('_kl') or {}
    if kl and kl.get('near_level'):
        strength = kl.get('strength', 'neutral')
        v = {'strong': 15, 'warning': 8, 'confirming': 6, 'neutral': 3}.get(strength, 3)
        ev = (kl.get('event') or '').lower()
        if 'support' in ev or 'bounce' in ev or 'lift' in ev:
            bull_score += v
            components['kl_support'] = v
        elif 'resistance' in ev or 'rejection' in ev or 'breakdown' in ev:
            bear_score += v
            components['kl_resistance'] = v
        else:
            # Generic level proximity = small bonus to dominant direction
            bull_score += v / 2
            bear_score += v / 2
            components['kl_neutral'] = v / 2

    # 6. Stack count ≥ 4 (15 max)
    sd = int(item.get('stack_distinct') or 0)
    sl = int(item.get('stack_long') or 0)
    ss = int(item.get('stack_short') or 0)
    if sd >= 4:
        v = min(15, sd * 2)
        if sl > ss:
            bull_score += v
            components['stack_long'] = v
        elif ss > sl:
            bear_score += v
            components['stack_short'] = v
        else:
            bull_score += v / 2
            bear_score += v / 2

    # 7. Anomaly z-score (10 max)
    z15 = item.get('delta_zscore_15m')
    z1  = item.get('delta_zscore_1h')
    if z1 is not None:
        if z1 > 1.5:
            v = min(10, z1 * 3)
            bull_score += v
            components['anomaly_pos'] = round(v, 1)
        elif z1 < -1.5:
            v = min(10, abs(z1) * 3)
            bear_score += v
            components['anomaly_neg'] = round(v, 1)
    elif z15 is not None:
        if z15 > 1.5: bull_score += min(5, z15 * 1.5)
        elif z15 < -1.5: bear_score += min(5, abs(z15) * 1.5)

    # 8. Direction & final score
    if bull_score > bear_score:
        direction = 'LONG'
        score = int(round(bull_score - bear_score / 2))
    elif bear_score > bull_score:
        direction = 'SHORT'
        score = int(round(bear_score - bull_score / 2))
    else:
        direction = None
        score = 0

    score = max(0, min(100, score))
    return {
        'score': score,
        'direction': direction,
        'bull_score': round(bull_score, 1),
        'bear_score': round(bear_score, 1),
        'components': components,
    }
