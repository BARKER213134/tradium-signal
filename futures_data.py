"""Futures market data — общие утилиты (бывший верх anomaly_scanner.py).

Batch-кэш funding/ticker/OI для всех фьючерсных пар (3 запроса вместо 400+)
+ списки пар. Используется: exchange, setup_checker, market_phase,
verified_entry, watcher (confluence scan).

Anomaly detection удалён (2026-07-02) — этот модуль остался как data-хаб.
"""
import logging
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

FAPI = "https://fapi.binance.com"

# Persistent HTTP client с keep-alive — без этого каждый запрос делал
# новый TLS-handshake (~200мс). Сканер гоняет ~500 пар × 5-8 типов проверок
# каждые 5 минут — старый код тратил десятки секунд только на handshake'ах.
_http_limits = httpx.Limits(max_connections=30, max_keepalive_connections=20,
                            keepalive_expiry=30.0)
_http_client = httpx.Client(timeout=10.0, limits=_http_limits,
                            headers={"Accept-Encoding": "gzip"})


def _http_get(url: str, **kw):
    return _http_client.get(url, **kw)

# Кеш списка пар
_pairs_cache: list[str] = []
_pairs_cache_ts: float = 0
_PAIRS_TTL = 3600

# ── Batch-кеши (один запрос вместо 400+) ─────────────────────────────
_batch_cache: dict = {}
_batch_cache_ts: float = 0
_BATCH_TTL = 120  # 2 мин


def _refresh_batch_cache():
    """Один раз загружает funding, OI, ticker для ВСЕХ пар."""
    global _batch_cache, _batch_cache_ts
    now = time.time()
    if _batch_cache and (now - _batch_cache_ts) < _BATCH_TTL:
        return

    cache = {"funding": {}, "ticker": {}, "oi": {}}

    # 1. premiumIndex — funding rate + mark price для всех пар (1 запрос)
    try:
        r = _http_get(f"{FAPI}/fapi/v1/premiumIndex", timeout=10)
        if r.status_code == 200:
            for item in r.json():
                s = item.get("symbol", "")
                cache["funding"][s] = float(item.get("lastFundingRate", 0)) * 100
    except Exception as e:
        logger.debug(f"Batch premiumIndex: {e}")

    # 2. ticker/24hr — объём, цена для всех пар (1 запрос)
    try:
        r = _http_get(f"{FAPI}/fapi/v1/ticker/24hr", timeout=10)
        if r.status_code == 200:
            for item in r.json():
                s = item.get("symbol", "")
                cache["ticker"][s] = {
                    "price": float(item.get("lastPrice", 0)),
                    "volume_usd": float(item.get("quoteVolume", 0)),
                }
    except Exception as e:
        logger.debug(f"Batch ticker: {e}")

    # 3. openInterest для всех пар (1 запрос)
    try:
        r = _http_get(f"{FAPI}/fapi/v1/openInterest", timeout=10)
        # Этот endpoint принимает только один символ, используем ticker вместо
    except Exception:
        pass

    _batch_cache = cache
    _batch_cache_ts = now
    logger.info(f"Batch cache refreshed: {len(cache['funding'])} funding, {len(cache['ticker'])} tickers")


def get_liquid_pairs(min_volume_usd: float = 5_000_000) -> list[str]:
    """Возвращает только пары с дневным объёмом >= min_volume_usd."""
    _refresh_batch_cache()
    pairs = get_all_futures_pairs()
    liquid = []
    for p in pairs:
        vol = _batch_cache.get("ticker", {}).get(p, {}).get("volume_usd", 0)
        if vol >= min_volume_usd:
            liquid.append(p)
    logger.info(f"Liquid pairs (>${min_volume_usd/1e6:.0f}M): {len(liquid)}/{len(pairs)}")
    return liquid


def get_all_futures_pairs() -> list[str]:
    """Все USDT perpetual пары на Binance Futures."""
    global _pairs_cache, _pairs_cache_ts
    now = time.time()
    if _pairs_cache and (now - _pairs_cache_ts) < _PAIRS_TTL:
        return _pairs_cache
    try:
        r = _http_get(f"{FAPI}/fapi/v1/exchangeInfo", timeout=15)
        if r.status_code != 200:
            return _pairs_cache
        import re
        _bad_sym = re.compile(r'[\u4e00-\u9fff]|^\d+USDT$')
        symbols = [
            s["symbol"] for s in r.json().get("symbols", [])
            if s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING"
            and not _bad_sym.search(s["symbol"])
        ]
        _pairs_cache = symbols
        _pairs_cache_ts = now
        logger.info(f"Futures pairs: {len(symbols)}")
        return symbols
    except Exception as e:
        logger.error(f"Futures exchangeInfo: {e}")
        return _pairs_cache

