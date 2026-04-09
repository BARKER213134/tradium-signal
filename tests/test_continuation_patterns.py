from continuation_patterns import (
    rising_three_methods, falling_three_methods,
    bull_flag, bear_flag,
    ascending_triangle, descending_triangle,
    detect_continuation,
)


def _c(o, h, l, c):
    return {"o": o, "h": h, "l": l, "c": c}


def test_rising_three_methods_detected():
    candles = [
        _c(100, 115, 99, 114),   # big green
        _c(113, 114, 108, 110),  # small red inside
        _c(110, 112, 106, 108),
        _c(108, 110, 105, 107),
        _c(107, 120, 106, 118),  # big green closing above first
    ]
    assert rising_three_methods(candles) is True


def test_rising_three_methods_false():
    candles = [
        _c(100, 105, 99, 101),
        _c(101, 103, 100, 102),
        _c(102, 104, 101, 103),
        _c(103, 105, 102, 104),
        _c(104, 106, 103, 105),
    ]
    assert rising_three_methods(candles) is False


def test_falling_three_methods_detected():
    candles = [
        _c(120, 121, 105, 106),  # big red
        _c(108, 112, 107, 110),  # small bull inside
        _c(110, 113, 108, 111),
        _c(111, 114, 110, 112),
        _c(112, 113, 100, 102),  # big red closing below first
    ]
    assert falling_three_methods(candles) is True


def test_bull_flag_detection():
    # Импульс + плавный откат
    impulse = [
        _c(100, 105, 99, 104),
        _c(104, 108, 103, 107),
        _c(107, 111, 106, 110),
        _c(110, 114, 109, 113),
        _c(113, 117, 112, 116),
    ]
    flag = [
        _c(116, 116, 114, 115),
        _c(115, 115, 113, 114),
        _c(114, 114, 112, 113),
        _c(113, 113, 111, 112),
        _c(112, 113, 111, 112),
    ]
    assert bull_flag(impulse + flag) is True


def test_bear_flag_detection():
    impulse = [
        _c(100, 101, 95, 96),
        _c(96, 97, 92, 93),
        _c(93, 94, 89, 90),
        _c(90, 91, 86, 87),
        _c(87, 88, 83, 84),
    ]
    flag = [
        _c(84, 86, 84, 85),
        _c(85, 87, 85, 86),
        _c(86, 87, 85, 86),
        _c(86, 87, 85, 86),
        _c(86, 87, 85, 86),
    ]
    assert bear_flag(impulse + flag) is True


def test_detect_continuation_returns_bullish_for_long():
    candles = [
        _c(100, 115, 99, 114),
        _c(113, 114, 108, 110),
        _c(110, 112, 106, 108),
        _c(108, 110, 105, 107),
        _c(107, 120, 106, 118),
    ]
    result = detect_continuation(candles, "LONG")
    assert "Rising Three Methods" in result


def test_detect_continuation_empty():
    assert detect_continuation([], "LONG") == []
    assert detect_continuation([_c(1, 2, 0, 1)], "LONG") == []
