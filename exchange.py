"""Интеграция с Binance public API — список пар и текущие цены."""
import logging
import time
import httpx

logger = logging.getLogger(__name__)

BINANCE_BASE = "https://data-api.binance.vision"
BINANCE_FUTURES = "https://fapi.binance.com"

# Persistent HTTP client с keep-alive — без этого каждый запрос делал новый
# TLS-handshake (+200-500мс к каждому klines). pool_maxsize=20 хватает для
# параллельных запросов графиков + фонового прогрева.
_http_limits = httpx.Limits(max_connections=30, max_keepalive_connections=20,
                            keepalive_expiry=30.0)
_http_client = httpx.Client(timeout=8.0, limits=_http_limits,
                            headers={"Accept-Encoding": "gzip"})


def _http_get(url: str, **kw):
    """Единая точка входа для sync HTTP — использует общий keep-alive клиент."""
    return _http_client.get(url, **kw)

# кэш
_symbols_cache: set[str] = set()
_symbols_cache_ts: float = 0
_SYMBOLS_TTL = 3600  # 1 час

_price_cache: dict[str, tuple[float, float]] = {}  # symbol -> (price, ts)
_PRICE_TTL = 5  # 5 секунд


def _normalize(pair: str) -> str:
    """BTC/USDT -> BTCUSDT"""
    if not pair:
        return ""
    return pair.replace("/", "").replace("-", "").replace(" ", "").upper()


def get_all_usdt_symbols() -> set[str]:
    """Возвращает все активные торговые пары *USDT на Binance."""
    global _symbols_cache, _symbols_cache_ts
    now = time.time()
    if _symbols_cache and (now - _symbols_cache_ts) < _SYMBOLS_TTL:
        return _symbols_cache
    try:
        r = _http_get(f"{BINANCE_BASE}/api/v3/exchangeInfo", timeout=10)
        r.raise_for_status()
        data = r.json()
        symbols = {
            s["symbol"]
            for s in data.get("symbols", [])
            if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT"
        }
        _symbols_cache = symbols
        _symbols_cache_ts = now
        logger.info(f"Binance: загружено {len(symbols)} USDT-пар")
        return symbols
    except Exception as e:
        logger.error(f"Ошибка загрузки пар Binance: {e}")
        return _symbols_cache


def get_price(pair: str) -> float | None:
    """Возвращает текущую цену пары (BTC/USDT или BTCUSDT)."""
    symbol = _normalize(pair)
    if not symbol:
        return None
    now = time.time()
    cached = _price_cache.get(symbol)
    if cached and (now - cached[1]) < _PRICE_TTL:
        return cached[0]
    try:
        r = _http_get(
            f"{BINANCE_BASE}/api/v3/ticker/price",
            params={"symbol": symbol},
            timeout=5,
        )
        if r.status_code != 200:
            return None
        price = float(r.json().get("price", 0))
        _price_cache[symbol] = (price, now)
        return price
    except Exception as e:
        logger.debug(f"Ошибка цены {symbol}: {e}")
        return None


# Маппинг ТФ из формата сигнала в формат Binance
TF_MAP = {
    "1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m", "30m": "30m",
    "1h": "1h", "2h": "2h", "4h": "4h", "6h": "6h", "8h": "8h", "12h": "12h",
    "1d": "1d", "1D": "1d", "3d": "3d", "1w": "1w", "1W": "1w", "1M": "1M",
}


def get_klines(pair: str, timeframe: str, limit: int = 50) -> list[dict]:
    """Получает свечи с Binance. Возвращает [{'t','o','h','l','c','v'}, ...] старые→новые."""
    symbol = _normalize(pair)
    interval = TF_MAP.get(timeframe, timeframe.lower() if timeframe else "1h")
    if not symbol:
        return []
    try:
        r = _http_get(
            f"{BINANCE_BASE}/api/v3/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
        )
        if r.status_code != 200:
            return []
        return [
            {
                "t": int(k[0]),
                "o": float(k[1]),
                "h": float(k[2]),
                "l": float(k[3]),
                "c": float(k[4]),
                "v": float(k[5]),
            }
            for k in r.json()
        ]
    except Exception as e:
        logger.debug(f"klines {symbol}: {e}")
        return []


def _fetch_batch(symbols: list[str]) -> dict[str, float] | None:
    """Внутренняя batch-функция. Возвращает None при 400 (плохой символ в наборе)."""
    if not symbols:
        return {}
    try:
        import json as _json
        r = _http_get(
            f"{BINANCE_BASE}/api/v3/ticker/price",
            params={"symbols": _json.dumps(symbols, separators=(",", ":"))},
            timeout=8,
        )
        if r.status_code == 400:
            return None  # один из символов не торгуется — вернём None для fallback
        if r.status_code != 200:
            return {}
        now = time.time()
        out = {}
        for item in r.json():
            sym = item["symbol"]
            price = float(item["price"])
            _price_cache[sym] = (price, now)
            out[sym] = price
        return out
    except Exception as e:
        logger.debug(f"Batch prices error: {e}")
        return {}


def _fetch_single(symbol: str) -> float | None:
    try:
        r = _http_get(
            f"{BINANCE_BASE}/api/v3/ticker/price",
            params={"symbol": symbol},
            timeout=5,
        )
        if r.status_code != 200:
            return None
        price = float(r.json().get("price", 0))
        _price_cache[symbol] = (price, time.time())
        return price
    except Exception:
        return None


# ─── Futures API ───────────────────────────────────────────────────────

_futures_cache: dict[str, tuple[float, float]] = {}
_FUTURES_TTL = 5


def get_futures_price(pair: str) -> float | None:
    """Цена с USDT-M Futures."""
    symbol = _normalize(pair)
    if not symbol:
        return None
    now = time.time()
    cached = _futures_cache.get(symbol)
    if cached and (now - cached[1]) < _FUTURES_TTL:
        return cached[0]
    try:
        r = _http_get(
            f"{BINANCE_FUTURES}/fapi/v1/ticker/price",
            params={"symbol": symbol},
            timeout=5,
        )
        if r.status_code != 200:
            return None
        price = float(r.json().get("price", 0))
        _futures_cache[symbol] = (price, now)
        return price
    except Exception:
        return None


def get_futures_prices(pairs: list[str]) -> dict[str, float]:
    """Batch цены с Binance Futures. Per-symbol (futures batch endpoint ненадёжный)."""
    out: dict[str, float] = {}
    for p in pairs:
        norm = _normalize(p)
        if not norm:
            continue
        price = get_futures_price(p)
        if price is not None:
            out[norm] = price
    return out


def get_futures_klines(pair: str, timeframe: str, limit: int = 50) -> list[dict]:
    """Свечи с Binance USDT-M Futures."""
    symbol = _normalize(pair)
    interval = TF_MAP.get(timeframe, timeframe.lower() if timeframe else "30m")
    if not symbol:
        return []
    try:
        r = _http_get(
            f"{BINANCE_FUTURES}/fapi/v1/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
            timeout=8,
        )
        if r.status_code != 200:
            return []
        return [
            {
                "t": int(k[0]),
                "o": float(k[1]),
                "h": float(k[2]),
                "l": float(k[3]),
                "c": float(k[4]),
                "v": float(k[5]),
            }
            for k in r.json()
        ]
    except Exception as e:
        logger.debug(f"futures klines {symbol}: {e}")
        return []


def get_price_any(pair: str) -> float | None:
    """Пробует spot → если нет → futures."""
    price = get_price(pair)
    if price is not None:
        return price
    return get_futures_price(pair)


def get_prices_any(pairs: list[str]) -> dict[str, float]:
    """Batch: сначала spot, пропущенные — через futures."""
    result = get_prices(pairs)
    # Все пары без цены на spot — пробуем через futures
    missing = [p for p in pairs if _normalize(p) not in result]
    if missing:
        futures = get_futures_prices(missing)
        result.update(futures)
    return result


def get_futures_prices_only(pairs: list[str]) -> dict[str, float]:
    """Только futures, без spot. Для Cryptovizor перпетуалов."""
    return get_futures_prices(pairs)


# ── Keltner Channel ETH (кешированный) ────────────────────────────────
_kc_cache: dict = {}
_kc_cache_ts: float = 0
_KC_TTL = 300  # 5 мин — снижаем нагрузку на рендер /signals


def _calc_keltner(candles, period=20, multiplier=2.0):
    """Keltner Channel: EMA(period) ± multiplier × ATR.
    Возвращает direction: LONG/SHORT/NEUTRAL."""
    if not candles or len(candles) < period + 1:
        return "NEUTRAL"

    # EMA
    closes = [c["c"] for c in candles]
    ema = [closes[0]]
    m = 2 / (period + 1)
    for v in closes[1:]:
        ema.append(v * m + ema[-1] * (1 - m))

    # ATR
    trs = []
    for i in range(1, len(candles)):
        c = candles[i]
        prev_c = candles[i - 1]["c"]
        trs.append(max(c["h"] - c["l"], abs(c["h"] - prev_c), abs(c["l"] - prev_c)))

    atr_values = []
    for i in range(len(trs)):
        if i < period - 1:
            atr_values.append(None)
        elif i == period - 1:
            atr_values.append(sum(trs[:period]) / period)
        else:
            atr_values.append((atr_values[-1] * (period - 1) + trs[i]) / period)

    # Последняя свеча
    i = len(candles) - 1
    atr = atr_values[i - 1] if i - 1 < len(atr_values) else None
    if atr is None:
        return "NEUTRAL"

    upper = ema[i] + multiplier * atr
    lower = ema[i] - multiplier * atr
    price = candles[i]["c"]

    if price > upper:
        return "LONG"
    elif price < lower:
        return "SHORT"
    return "NEUTRAL"


def get_keltner_eth() -> dict:
    """Keltner Channel ETH 1h: direction (LONG/SHORT/NEUTRAL), confirmed. Кеш 5 мин."""
    global _kc_cache, _kc_cache_ts
    now = time.time()
    if _kc_cache and (now - _kc_cache_ts) < _KC_TTL:
        return _kc_cache

    try:
        candles = get_klines_any("ETH/USDT", "1h", limit=50)
        direction = _calc_keltner(candles)
        _kc_cache = {
            "direction": direction,
            "confirmed": direction != "NEUTRAL",
        }
    except Exception:
        _kc_cache = {"direction": "NEUTRAL", "confirmed": False}

    _kc_cache_ts = now
    return _kc_cache


# Обратная совместимость
get_supertrend_eth = get_keltner_eth


# ── Pump Check (Volume + OI + Funding) ────────────────────────────────

def check_pump_potential(symbol: str) -> dict:
    """Проверяет Volume Spike + OI change + Funding для одной монеты.
    Возвращает {volume_spike, oi_change, funding, score, label}."""
    import httpx
    FAPI = "https://fapi.binance.com"
    result = {"volume_spike": 0, "oi_change": 0, "funding": 0, "score": 0, "factors": []}

    try:
        # 1. Volume Spike — текущий vs средний за 5 свечей 1h
        r = _http_get(f"{FAPI}/fapi/v1/klines",
                      params={"symbol": symbol, "interval": "1h", "limit": 6}, timeout=5)
        if r.status_code == 200:
            k = r.json()
            if len(k) >= 6:
                curr_vol = float(k[-1][5])
                avg_vol = sum(float(k[i][5]) for i in range(-6, -1)) / 5
                if avg_vol > 0:
                    ratio = curr_vol / avg_vol
                    result["volume_spike"] = round(ratio, 1)
                    if ratio >= 2.0:
                        result["score"] += 1
                        result["factors"].append(f"📊 Объём ×{ratio:.1f} от среднего")

        # 2. OI Change — за последний час
        r = _http_get(f"{FAPI}/futures/data/openInterestHist",
                      params={"symbol": symbol, "period": "1h", "limit": 2}, timeout=5)
        if r.status_code == 200:
            data = r.json()
            if len(data) >= 2:
                prev = float(data[-2].get("sumOpenInterestValue", 0))
                curr = float(data[-1].get("sumOpenInterestValue", 0))
                if prev > 0:
                    change = ((curr - prev) / prev) * 100
                    result["oi_change"] = round(change, 2)
                    if abs(change) >= 3.0:
                        result["score"] += 1
                        result["factors"].append(f"📈 OI {change:+.1f}%")

        # 3. Funding Rate
        from anomaly_scanner import _batch_cache
        funding = _batch_cache.get("funding", {}).get(symbol)
        if funding is not None:
            result["funding"] = round(funding, 4)
            if abs(funding) >= 0.03:
                result["score"] += 1
                side = "лонги платят" if funding > 0 else "шорты платят"
                result["factors"].append(f"💰 Funding {funding:.3f}% ({side})")
        else:
            # Fallback
            r = _http_get(f"{FAPI}/fapi/v1/premiumIndex",
                          params={"symbol": symbol}, timeout=5)
            if r.status_code == 200:
                d = r.json()
                fr = float(d.get("lastFundingRate", 0)) * 100
                result["funding"] = round(fr, 4)
                if abs(fr) >= 0.03:
                    result["score"] += 1
                    side = "лонги платят" if fr > 0 else "шорты платят"
                    result["factors"].append(f"💰 Funding {fr:.3f}% ({side})")

    except Exception as e:
        logger.debug(f"Pump check {symbol}: {e}")

    # Всегда добавляем данные даже если нормальные
    if not any(f.startswith("📊") for f in result["factors"]):
        result["factors"].insert(0, f"📊 Объём ×{result['volume_spike']} от среднего")
    if not any(f.startswith("📈") for f in result["factors"]):
        oi = result["oi_change"]
        result["factors"].append(f"📈 OI {oi:+.1f}%")
    if not any(f.startswith("💰") for f in result["factors"]):
        fr = result["funding"]
        if fr != 0:
            result["factors"].append(f"💰 Funding {fr:.3f}%")

    # Label
    if result["score"] >= 2:
        result["label"] = "🚀 HIGH POTENTIAL"
    else:
        result["label"] = ""

    return result


# ── ETH/BTC market context (кешированный) ───────────────────────────
_eth_ctx_cache: dict = {}
_eth_ctx_ts: float = 0
_ETH_CTX_TTL = 300  # 5 мин — снижаем нагрузку на рендер /signals


def get_eth_market_context() -> dict:
    """Возвращает ETH 1h%, BTC 1h%, ETH/BTC тренд. Кеш 5 мин."""
    global _eth_ctx_cache, _eth_ctx_ts
    now = time.time()
    if _eth_ctx_cache and (now - _eth_ctx_ts) < _ETH_CTX_TTL:
        return _eth_ctx_cache

    ctx = {"eth_1h": 0, "btc_1h": 0, "eth_btc": "—", "eth_price": 0, "btc_price": 0}
    try:
        # ETH 1h
        r = _http_get(f"{BINANCE_FUTURES}/fapi/v1/klines",
                      params={"symbol": "ETHUSDT", "interval": "1h", "limit": 2}, timeout=5)
        if r.status_code == 200:
            k = r.json()
            if len(k) >= 2:
                prev_c = float(k[-2][4])
                curr_c = float(k[-1][4])
                ctx["eth_1h"] = round(((curr_c - prev_c) / prev_c) * 100, 2)
                ctx["eth_price"] = curr_c

        # BTC 1h
        r = _http_get(f"{BINANCE_FUTURES}/fapi/v1/klines",
                      params={"symbol": "BTCUSDT", "interval": "1h", "limit": 2}, timeout=5)
        if r.status_code == 200:
            k = r.json()
            if len(k) >= 2:
                prev_c = float(k[-2][4])
                curr_c = float(k[-1][4])
                ctx["btc_1h"] = round(((curr_c - prev_c) / prev_c) * 100, 2)
                ctx["btc_price"] = curr_c

        # ETH/BTC тренд (последние 4 свечи 1h)
        r = _http_get(f"{BINANCE_BASE}/api/v3/klines",
                      params={"symbol": "ETHBTC", "interval": "1h", "limit": 4}, timeout=5)
        if r.status_code == 200:
            k = r.json()
            if len(k) >= 4:
                first = float(k[0][4])
                last = float(k[-1][4])
                pct = ((last - first) / first) * 100
                ctx["eth_btc"] = f"{'↑' if pct > 0.1 else '↓' if pct < -0.1 else '→'} {pct:+.2f}%"
                ctx["eth_btc_trend"] = "up" if pct > 0.1 else "down" if pct < -0.1 else "flat"
    except Exception as e:
        logger.debug(f"ETH context: {e}")

    _eth_ctx_cache = ctx
    _eth_ctx_ts = now
    return ctx


def get_klines_any(pair: str, timeframe: str, limit: int = 50) -> list[dict]:
    """Сначала spot klines, если пусто → futures."""
    candles = get_klines(pair, timeframe, limit)
    if candles:
        return candles
    return get_futures_klines(pair, timeframe, limit)


def get_prices(pairs: list[str]) -> dict[str, float]:
    """Batch: цены для списка пар. При 400 падает на per-symbol fallback,
    чтобы один мусорный символ не ломал все остальные."""
    known = get_all_usdt_symbols()
    symbols = [_normalize(p) for p in pairs if p]
    symbols = [s for s in symbols if s and (not known or s in known)]
    if not symbols:
        return {}

    batch = _fetch_batch(symbols)
    if batch is not None:
        return batch

    # Batch вернул 400 — ищем плохой символ поштучно
    logger.warning("Binance 400 on batch, fallback to per-symbol fetch")
    out: dict[str, float] = {}
    for s in symbols:
        p = _fetch_single(s)
        if p is not None:
            out[s] = p
    return out
