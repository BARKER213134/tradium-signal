from parser import parse_signal


KAITO_LONG = """Tradium Setups [TERMINAL]
Автор: Setup Screener 🏆 №30
#сетап

$KAITO 1h Binance #Futures
TREND 🔴🟢🟢🟢🟢
MA 🟢 RSI    00:00 09.04.2026
Volume 1D      25.91 M
CD Week       -720.37 K

Long 📈
LIMIT
🎯 Entry: 0.4262
✅ TP: 0.4661 9.36%
❌ SL: 0.4063 4.67%
⚖ Risk-reward: 2.01
🥇 Risk: 1.0%
💰 Amount: 889.01

From Cryptovizor Screener

Comment: Получен сигнал о начале коррекции, сгенерирован торговый сетап и отправлен в терминал.
 | Key levels: SUPPORT 0.415 - 0.4198 | Current: 0.4512 (-7.49%)

⏳ Waiting

09.04.26 02:07
"""


BTC_SHORT = """Tradium Setups [TERMINAL]
Автор: Setup Screener 🏆 №7
#сетап

$BTC 4h Binance #Futures
TREND 🟢🔴🔴🔴🔴

Short 📉
LIMIT
🎯 Entry: 65000
✅ TP: 62000 4.6%
❌ SL: 66500 2.3%
⚖ Risk-reward: 2.00
🥇 Risk: 0.5%
💰 Amount: 1500

Comment: Reversal setup on 4h.
"""


MALFORMED_NO_TP = """$ETH 1h Binance
Long
🎯 Entry: 3200
❌ SL: 3100
⚖ Risk-reward: 1.5
"""


def test_kaito_long_full():
    r = parse_signal(KAITO_LONG)
    assert r["pair"] == "KAITO/USDT"
    assert r["timeframe"] == "1h"
    assert r["direction"] == "LONG"
    assert r["entry"] == 0.4262
    assert r["tp1"] == 0.4661
    assert r["tp_percent"] == 9.36
    assert r["sl"] == 0.4063
    assert r["sl_percent"] == 4.67
    assert r["risk_reward"] == 2.01
    assert r["risk_percent"] == 1.0
    assert r["amount"] == 889.01
    assert r["trend"] == "RGGGG"
    assert r["setup_number"] == 30
    assert r["comment"].startswith("Получен сигнал")


def test_btc_short_4h():
    r = parse_signal(BTC_SHORT)
    assert r["pair"] == "BTC/USDT"
    assert r["timeframe"] == "4h"
    assert r["direction"] == "SHORT"
    assert r["entry"] == 65000
    assert r["tp1"] == 62000
    assert r["sl"] == 66500
    assert r["risk_reward"] == 2.00
    assert r["setup_number"] == 7
    assert r["trend"] == "GRRRR"


def test_malformed_missing_tp():
    r = parse_signal(MALFORMED_NO_TP)
    assert r["pair"] == "ETH/USDT"
    assert r["direction"] == "LONG"
    assert r["entry"] == 3200
    assert r["sl"] == 3100
    assert r["tp1"] is None
    assert r["tp_percent"] is None


def test_empty_string():
    r = parse_signal("")
    assert r["pair"] is None
    assert r["direction"] is None
    assert r["entry"] is None


def test_none_input():
    r = parse_signal(None)
    assert r["pair"] is None


def test_quote_currency_fallback_btc():
    # Pair only expressed via fallback with non-USDT quote — no TF token on
    # the same line, so the primary regex misses, and fallback catches it.
    text = "Signal on SOL-BTC pair\nLong\nEntry: 0.0031"
    r = parse_signal(text)
    assert r["pair"] == "SOL/BTC"
    assert r["direction"] == "LONG"
    assert r["entry"] == 0.0031
