"""V-Bottom / V-Top pattern detector.

V-Bottom: резкий обвал ≥3% за 1-3 свечи, потом reversal candle
  (hammer/engulfing/pin-bar) на close. Wyckoff "spring" — manipulation
  перед разворотом.

V-Top: mirror.
"""
from __future__ import annotations
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def _is_hammer(o: float, h: float, l: float, c: float, bull: bool = True) -> bool:
    """Hammer (bull) or shooting star (bear) candle.
    Long shadow on opposite side, small body."""
    body = abs(c - o)
    full = h - l
    if full <= 0:
        return False
    body_ratio = body / full
    if body_ratio > 0.4:  # body too big
        return False
    if bull:
        # Long lower shadow, body in upper third
        lower_shadow = min(o, c) - l
        return lower_shadow > body * 1.5 and (h - max(o, c)) < body
    else:
        # Long upper shadow, body in lower third
        upper_shadow = h - max(o, c)
        return upper_shadow > body * 1.5 and (min(o, c) - l) < body


def _is_engulfing(prev_o: float, prev_c: float,
                   cur_o: float, cur_c: float, bull: bool = True) -> bool:
    """Bullish/bearish engulfing pattern."""
    if bull:
        # Prev red, cur green, cur body engulfs prev body
        return (prev_c < prev_o and cur_c > cur_o
                and cur_o <= prev_c and cur_c >= prev_o)
    else:
        return (prev_c > prev_o and cur_c < cur_o
                and cur_o >= prev_c and cur_c <= prev_o)


def detect_v_pattern_at_bar(klines: list, idx: int,
                            min_drop_pct: float = 3.0) -> Optional[dict]:
    """Detect V-pattern ending AT klines[idx].

    Logic:
      - Look at last 3 bars [idx-2, idx-1, idx]
      - Compute drop from high(idx-2..idx-1) to low(idx-1..idx)
      - If drop >= min_drop_pct AND current bar is reversal candle:
        → V-Bottom signal

    Returns: {'type', 'drop_pct', 'reversal_strength', 'detected_at'} or None.
    """
    if idx < 3 or idx >= len(klines):
        return None
    # 3-bar window
    window = klines[max(0, idx - 2):idx + 1]
    if len(window) < 3:
        return None

    # V-Bottom check
    pre_high = max(k[2] for k in window[:-1])  # high of bars before current
    cur_low = window[-1][3]
    drop_pct = (pre_high - cur_low) / pre_high * 100 if pre_high > 0 else 0

    if drop_pct >= min_drop_pct:
        # Check reversal candle on current bar
        o, h, l, c = window[-1][1], window[-1][2], window[-1][3], window[-1][4]
        po, pc = window[-2][1], window[-2][4]
        bull_reversal = False
        if c > o:
            bull_reversal = True  # green close = reversal
        if _is_hammer(o, h, l, c, bull=True):
            bull_reversal = True
        if _is_engulfing(po, pc, o, c, bull=True):
            bull_reversal = True
        if bull_reversal:
            close_above_open = (c - o) / o * 100 if o > 0 else 0
            return {
                'type': 'v_bottom',
                'drop_pct': round(drop_pct, 2),
                'reversal_pct': round(close_above_open, 2),
                'pre_high': pre_high,
                'cur_low': cur_low,
                'close': c,
                'detected_at_ms': window[-1][0],
            }

    # V-Top mirror
    pre_low = min(k[3] for k in window[:-1])
    cur_high = window[-1][2]
    rise_pct = (cur_high - pre_low) / pre_low * 100 if pre_low > 0 else 0
    if rise_pct >= min_drop_pct:
        o, h, l, c = window[-1][1], window[-1][2], window[-1][3], window[-1][4]
        po, pc = window[-2][1], window[-2][4]
        bear_reversal = False
        if c < o:
            bear_reversal = True
        if _is_hammer(o, h, l, c, bull=False):
            bear_reversal = True
        if _is_engulfing(po, pc, o, c, bull=False):
            bear_reversal = True
        if bear_reversal:
            close_below_open = (o - c) / o * 100 if o > 0 else 0
            return {
                'type': 'v_top',
                'rise_pct': round(rise_pct, 2),
                'reversal_pct': round(close_below_open, 2),
                'pre_low': pre_low,
                'cur_high': cur_high,
                'close': c,
                'detected_at_ms': window[-1][0],
            }
    return None


def detect_v_pattern(pair: str, tf: str = '15m', limit: int = 50,
                     min_drop_pct: float = 3.0) -> Optional[dict]:
    """Real-time check: V-pattern ending at last bar."""
    try:
        from divergence import _get_klines
        kl = _get_klines(pair, tf, limit)
        if not kl or len(kl) < 4:
            return None
        return detect_v_pattern_at_bar(kl, len(kl) - 1, min_drop_pct)
    except Exception as e:
        logger.debug(f'[v-pattern] {pair}: {e}')
        return None
