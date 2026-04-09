from levels import find_pivots, nearest_levels


def _c(o, h, l, c):
    return {"o": o, "h": h, "l": l, "c": c, "t": 0, "v": 100}


def test_find_pivots_simple():
    # Цепочка с явными экстремумами
    candles = [
        _c(10, 12, 9, 11),
        _c(11, 13, 10, 12),
        _c(12, 15, 11, 14),  # pivot high
        _c(14, 14, 12, 13),
        _c(13, 13, 10, 11),
        _c(11, 12, 8, 9),    # pivot low
        _c(9, 11, 8, 10),
        _c(10, 13, 9, 12),
        _c(12, 14, 11, 13),
    ]
    highs, lows = find_pivots(candles, left=2, right=2)
    assert 15.0 in highs
    assert 8.0 in lows


def test_nearest_levels_returns_support_and_resistance():
    candles = [
        _c(10, 12, 9, 11),
        _c(11, 13, 10, 12),
        _c(12, 15, 11, 14),
        _c(14, 14, 12, 13),
        _c(13, 13, 10, 11),
        _c(11, 12, 8, 9),
        _c(9, 11, 8, 10),
        _c(10, 13, 9, 12),
        _c(12, 14, 11, 13),
    ]
    s1, r1 = nearest_levels(candles, current_price=11.5, left=2, right=2)
    assert s1 == 8.0  # ближайший low ниже 11.5
    assert r1 == 15.0  # ближайший high выше 11.5


def test_empty_candles():
    s1, r1 = nearest_levels([], 100.0)
    assert s1 is None
    assert r1 is None
