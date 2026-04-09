from patterns import detect_patterns


def c(o, h, l, cl):
    return {"o": o, "h": h, "l": l, "c": cl}


def test_empty_list():
    assert detect_patterns([], "LONG") == []


def test_one_candle_bull_hammer():
    # Hammer: small body near top, long lower wick
    hammer = c(10.0, 10.15, 9.0, 10.1)
    res = detect_patterns([hammer], "LONG")
    assert "Hammer" in res


def test_hammer_not_in_bearish():
    hammer = c(10.0, 10.15, 9.0, 10.1)
    res = detect_patterns([hammer], "SHORT")
    assert "Hammer" not in res


def test_bullish_engulfing():
    prev = c(10.0, 10.1, 9.5, 9.6)   # bear
    cur = c(9.5, 10.5, 9.4, 10.4)    # bull engulfing
    res = detect_patterns([prev, cur], "LONG")
    assert "Bullish Engulfing" in res
    res2 = detect_patterns([prev, cur], "SHORT")
    assert "Bullish Engulfing" not in res2


def test_bearish_engulfing():
    prev = c(10.0, 10.5, 9.9, 10.4)   # bull
    cur = c(10.5, 10.6, 9.6, 9.7)     # bear engulfing
    res = detect_patterns([prev, cur], "SHORT")
    assert "Bearish Engulfing" in res
    assert "Bearish Engulfing" not in detect_patterns([prev, cur], "LONG")


def test_morning_star():
    p2 = c(10.0, 10.1, 9.0, 9.1)   # big bear
    p1 = c(9.0, 9.2, 8.9, 9.05)    # small body (doji-ish)
    cu = c(9.1, 10.2, 9.05, 10.1)  # big bull, closes above midpoint of p2
    res = detect_patterns([p2, p1, cu], "LONG")
    assert "Morning Star" in res


def test_shooting_star():
    # inverted hammer that is bearish
    star = c(10.0, 11.0, 9.95, 9.9)
    res = detect_patterns([star], "SHORT")
    assert "Shooting Star" in res
    assert "Shooting Star" not in detect_patterns([star], "LONG")


def test_three_black_crows():
    p2 = c(10.0, 10.1, 9.5, 9.6)
    p1 = c(9.8, 9.9, 9.2, 9.3)
    cu = c(9.5, 9.6, 8.9, 9.0)
    res = detect_patterns([p2, p1, cu], "SHORT")
    assert "Three Black Crows" in res


def test_noise_no_patterns():
    # flat-ish candles with no extreme features
    flat = [c(10.0, 10.05, 9.95, 10.02)] * 3
    res_l = detect_patterns(flat, "LONG")
    res_s = detect_patterns(flat, "SHORT")
    # Accept doji-ish may fire nothing; main requirement: no engulfing/stars
    assert "Bullish Engulfing" not in res_l
    assert "Bearish Engulfing" not in res_s
    assert "Morning Star" not in res_l
    assert "Evening Star" not in res_s


def test_two_candles_ok():
    prev = c(10.0, 10.1, 9.5, 9.6)
    cur = c(9.5, 10.5, 9.4, 10.4)
    assert isinstance(detect_patterns([prev, cur], "LONG"), list)
