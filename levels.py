"""Поиск ближайших pivot support/resistance уровней из klines.

Pivot-high: свеча, у которой high выше N свечей слева и справа.
Pivot-low: свеча, у которой low ниже N свечей слева и справа.

Возвращаем ближайшие к текущей цене уровни снизу (S1) и сверху (R1).
"""
from typing import Optional


def find_pivots(candles: list[dict], left: int = 3, right: int = 3) -> tuple[list[float], list[float]]:
    """Возвращает (list_of_highs, list_of_lows) — локальные экстремумы."""
    highs = []
    lows = []
    n = len(candles)
    for i in range(left, n - right):
        h = candles[i]["h"]
        l = candles[i]["l"]
        is_pivot_high = all(candles[i - k]["h"] <= h for k in range(1, left + 1)) and \
                        all(candles[i + k]["h"] <= h for k in range(1, right + 1))
        is_pivot_low = all(candles[i - k]["l"] >= l for k in range(1, left + 1)) and \
                       all(candles[i + k]["l"] >= l for k in range(1, right + 1))
        if is_pivot_high:
            highs.append(h)
        if is_pivot_low:
            lows.append(l)
    return highs, lows


def nearest_levels(candles: list[dict], current_price: float,
                   left: int = 3, right: int = 3) -> tuple[Optional[float], Optional[float]]:
    """Возвращает (S1, R1) — ближайший support ниже и resistance выше текущей цены."""
    if not candles or current_price is None:
        return None, None
    highs, lows = find_pivots(candles, left, right)
    # Resistance: минимальный high выше текущей цены
    above = [h for h in highs if h > current_price]
    s1_r1_r = min(above) if above else None
    # Support: максимальный low ниже текущей цены
    below = [l for l in lows if l < current_price]
    s1_r1_s = max(below) if below else None
    return s1_r1_s, s1_r1_r
