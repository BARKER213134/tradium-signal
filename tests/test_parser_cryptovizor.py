from parser_cryptovizor import parse_cryptovizor_message


def test_short_signal():
    text = "👀 Perfectly fit 09.04\n$LA.P     🟢🔴🔴🔴🔴  17:31"
    result = parse_cryptovizor_message(text)
    assert len(result) == 1
    s = result[0]
    assert s["pair"] == "LA/USDT"
    assert s["direction"] == "SHORT"
    assert s["trend"] == "GRRRR"
    assert s["time_str"] == "17:31"


def test_long_signal():
    text = "👀 Perfectly fit 09.04\n$KAITO.P  🔴🟢🟢🟢🟢  10:31"
    result = parse_cryptovizor_message(text)
    assert len(result) == 1
    assert result[0]["pair"] == "KAITO/USDT"
    assert result[0]["direction"] == "LONG"
    assert result[0]["trend"] == "RGGGG"


def test_multi_signal_message():
    text = """👀 Perfectly fit 09.04
$LA.P     🟢🔴🔴🔴🔴  17:31
$SONIC.P  🟢🔴🔴🔴🔴  17:31
$QNT.P    🟢🔴🔴🔴🔴  17:31"""
    result = parse_cryptovizor_message(text)
    assert len(result) == 3
    assert all(s["direction"] == "SHORT" for s in result)
    assert {s["ticker"] for s in result} == {"LA", "SONIC", "QNT"}


def test_mixed_directions():
    text = """👀 Perfectly fit 09.04
$LUMIA.P     🔴🟢🟢🟢🟢  17:01
$1000LUNC.P  🟢🔴🔴🔴🔴  17:01"""
    result = parse_cryptovizor_message(text)
    assert len(result) == 2
    d = {s["ticker"]: s["direction"] for s in result}
    assert d["LUMIA"] == "LONG"
    assert d["1000LUNC"] == "SHORT"


def test_numeric_ticker():
    text = "👀 Perfectly fit 09.04\n$1000LUNC.P  🟢🔴🔴🔴🔴  17:01"
    result = parse_cryptovizor_message(text)
    assert len(result) == 1
    assert result[0]["pair"] == "1000LUNC/USDT"


def test_empty_text():
    assert parse_cryptovizor_message("") == []


def test_non_cryptovizor_text():
    # Tradium-сетап, без заголовка 👀 Perfectly fit — пропускаем
    text = "$KAITO 1h Binance\nTREND 🔴🟢🟢🟢🟢\nEntry: 0.4262"
    assert parse_cryptovizor_message(text) == []
