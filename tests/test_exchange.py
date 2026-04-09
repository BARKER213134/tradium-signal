from unittest.mock import patch, MagicMock
import exchange


def test_normalize():
    assert exchange._normalize("BTC/USDT") == "BTCUSDT"
    assert exchange._normalize("btc-usdt") == "BTCUSDT"
    assert exchange._normalize("") == ""
    assert exchange._normalize(None) == ""
    assert exchange._normalize(" eth/usdt ") == "ETHUSDT"


def _mock_resp(status=200, data=None):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = data if data is not None else {}
    r.raise_for_status = MagicMock()
    return r


def test_get_prices_batch_happy(monkeypatch):
    # Reset caches for deterministic behavior
    exchange._symbols_cache = {"BTCUSDT", "ETHUSDT"}
    exchange._symbols_cache_ts = 9e18
    exchange._price_cache.clear()

    calls = {}

    def fake_get(url, params=None, timeout=None):
        calls["url"] = url
        calls["params"] = params
        return _mock_resp(200, [
            {"symbol": "BTCUSDT", "price": "60000.1"},
            {"symbol": "ETHUSDT", "price": "3000.5"},
        ])

    monkeypatch.setattr(exchange.httpx, "get", fake_get)
    out = exchange.get_prices(["BTC/USDT", "ETH/USDT"])
    assert out["BTCUSDT"] == 60000.1
    assert out["ETHUSDT"] == 3000.5
    assert "ticker/price" in calls["url"]


def test_get_prices_filters_unknown(monkeypatch):
    exchange._symbols_cache = {"BTCUSDT"}
    exchange._symbols_cache_ts = 9e18
    exchange._price_cache.clear()

    def fake_get(url, params=None, timeout=None):
        # Only BTCUSDT should be requested
        assert "FAKECOINUSDT" not in (params or {}).get("symbols", "")
        return _mock_resp(200, [{"symbol": "BTCUSDT", "price": "1"}])

    monkeypatch.setattr(exchange.httpx, "get", fake_get)
    out = exchange.get_prices(["BTC/USDT", "FAKECOIN/USDT"])
    assert "BTCUSDT" in out
    assert "FAKECOINUSDT" not in out


def test_get_prices_400(monkeypatch):
    exchange._symbols_cache = {"BTCUSDT"}
    exchange._symbols_cache_ts = 9e18
    exchange._price_cache.clear()
    monkeypatch.setattr(exchange.httpx, "get", lambda *a, **k: _mock_resp(400))
    out = exchange.get_prices(["BTC/USDT"])
    assert out == {}


def test_get_klines(monkeypatch):
    def fake_get(url, params=None, timeout=None):
        return _mock_resp(200, [
            [1, "10", "11", "9", "10.5", "100"],
            [2, "10.5", "11.5", "10", "11", "200"],
        ])
    monkeypatch.setattr(exchange.httpx, "get", fake_get)
    k = exchange.get_klines("BTC/USDT", "1h", limit=2)
    assert len(k) == 2
    assert k[0]["o"] == 10.0
    assert k[0]["c"] == 10.5
    assert k[1]["h"] == 11.5


def test_get_klines_bad_status(monkeypatch):
    monkeypatch.setattr(exchange.httpx, "get", lambda *a, **k: _mock_resp(451))
    assert exchange.get_klines("BTC/USDT", "1h") == []


def test_get_prices_empty_input():
    assert exchange.get_prices([]) == {}
