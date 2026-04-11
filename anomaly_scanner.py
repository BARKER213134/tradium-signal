"""Сканер аномалий по всем фьючерсным парам Binance.

Batch REST подход — каждые 5 минут сканирует OI, Funding, L/S ratio, Order book walls.
400+ пар, разбитые на батчи чтобы не превысить rate limit.

Типы аномалий:
1. OI Spike — Open Interest change > 5% за период
2. Funding Extreme — funding rate > 0.05% или < -0.05%
3. L/S Ratio Extreme — > 2.5 или < 0.5
4. Order Book Wall — крупный ордер (> 10× среднего) рядом с ценой
5. Taker Imbalance — buy/sell ratio > 1.5 или < 0.5
"""
import logging
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

FAPI = "https://fapi.binance.com"

# Кеш списка пар
_pairs_cache: list[str] = []
_pairs_cache_ts: float = 0
_PAIRS_TTL = 3600


def get_all_futures_pairs() -> list[str]:
    """Все USDT perpetual пары на Binance Futures."""
    global _pairs_cache, _pairs_cache_ts
    now = time.time()
    if _pairs_cache and (now - _pairs_cache_ts) < _PAIRS_TTL:
        return _pairs_cache
    try:
        r = httpx.get(f"{FAPI}/fapi/v1/exchangeInfo", timeout=15)
        if r.status_code != 200:
            return _pairs_cache
        symbols = [
            s["symbol"] for s in r.json().get("symbols", [])
            if s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING"
        ]
        _pairs_cache = symbols
        _pairs_cache_ts = now
        logger.info(f"Futures pairs: {len(symbols)}")
        return symbols
    except Exception as e:
        logger.error(f"Futures exchangeInfo: {e}")
        return _pairs_cache


def check_oi_spike(symbol: str, threshold: float = 5.0) -> Optional[dict]:
    """OI change за последний час. Returns None если нет аномалии."""
    try:
        r = httpx.get(f"{FAPI}/futures/data/openInterestHist",
                      params={"symbol": symbol, "period": "1h", "limit": 2}, timeout=5)
        if r.status_code != 200 or not r.json():
            return None
        data = r.json()
        if len(data) < 2:
            return None
        prev = float(data[-2].get("sumOpenInterestValue", 0))
        curr = float(data[-1].get("sumOpenInterestValue", 0))
        if prev <= 0:
            return None
        change_pct = ((curr - prev) / prev) * 100
        if abs(change_pct) >= threshold:
            return {"type": "oi_spike", "value": round(change_pct, 2), "oi_usd": round(curr, 0)}
    except Exception:
        pass
    return None


def check_funding(symbol: str, threshold: float = 0.05) -> Optional[dict]:
    """Funding rate extreme."""
    try:
        r = httpx.get(f"{FAPI}/fapi/v1/fundingRate",
                      params={"symbol": symbol, "limit": 1}, timeout=5)
        if r.status_code != 200 or not r.json():
            return None
        rate = float(r.json()[0].get("fundingRate", 0)) * 100  # в процентах
        if abs(rate) >= threshold:
            return {"type": "funding_extreme", "value": round(rate, 4)}
    except Exception:
        pass
    return None


def check_ls_ratio(symbol: str, high: float = 2.5, low: float = 0.5) -> Optional[dict]:
    """Top trader Long/Short ratio extreme."""
    try:
        r = httpx.get(f"{FAPI}/futures/data/topLongShortPositionRatio",
                      params={"symbol": symbol, "period": "1h", "limit": 1}, timeout=5)
        if r.status_code != 200 or not r.json():
            return None
        ratio = float(r.json()[0].get("longShortRatio", 1))
        if ratio >= high or ratio <= low:
            return {"type": "ls_extreme", "value": round(ratio, 2)}
    except Exception:
        pass
    return None


def check_taker_ratio(symbol: str, high: float = 1.5, low: float = 0.5) -> Optional[dict]:
    """Taker buy/sell ratio extreme."""
    try:
        r = httpx.get(f"{FAPI}/futures/data/takerlongshortRatio",
                      params={"symbol": symbol, "period": "1h", "limit": 1}, timeout=5)
        if r.status_code != 200 or not r.json():
            return None
        ratio = float(r.json()[0].get("buySellRatio", 1))
        if ratio >= high or ratio <= low:
            return {"type": "taker_imbalance", "value": round(ratio, 2)}
    except Exception:
        pass
    return None


def check_orderbook_wall(symbol: str, multiplier: float = 10.0) -> Optional[dict]:
    """Ищет стены в order book (объём > multiplier × среднего)."""
    try:
        r = httpx.get(f"{FAPI}/fapi/v1/depth",
                      params={"symbol": symbol, "limit": 100}, timeout=5)
        if r.status_code != 200:
            return None
        data = r.json()
        bids = [(float(p), float(q)) for p, q in data.get("bids", [])[:50]]
        asks = [(float(p), float(q)) for p, q in data.get("asks", [])[:50]]

        if not bids or not asks:
            return None

        avg_bid = sum(q for _, q in bids) / len(bids)
        avg_ask = sum(q for _, q in asks) / len(asks)

        # Ищем стену
        wall = None
        for price, qty in bids:
            if qty > avg_bid * multiplier:
                wall = {"side": "bid", "price": price, "qty": round(qty, 2)}
                break
        if not wall:
            for price, qty in asks:
                if qty > avg_ask * multiplier:
                    wall = {"side": "ask", "price": price, "qty": round(qty, 2)}
                    break

        if wall:
            return {"type": "wall", "value": wall}
    except Exception:
        pass
    return None


def scan_symbol(symbol: str) -> dict:
    """Полный скан одной пары. Возвращает найденные аномалии + score."""
    anomalies = []

    oi = check_oi_spike(symbol)
    if oi:
        anomalies.append(oi)

    funding = check_funding(symbol)
    if funding:
        anomalies.append(funding)

    ls = check_ls_ratio(symbol)
    if ls:
        anomalies.append(ls)

    taker = check_taker_ratio(symbol)
    if taker:
        anomalies.append(taker)

    wall = check_orderbook_wall(symbol)
    if wall:
        anomalies.append(wall)

    if not anomalies:
        return None

    # Текущая цена
    price = None
    try:
        r = httpx.get(f"{FAPI}/fapi/v1/ticker/price",
                      params={"symbol": symbol}, timeout=3)
        if r.status_code == 200:
            price = float(r.json().get("price", 0))
    except Exception:
        pass

    # Определяем направление
    direction = "NEUTRAL"
    long_signals = 0
    short_signals = 0
    for a in anomalies:
        if a["type"] == "funding_extreme" and a["value"] > 0:
            short_signals += 1  # перегрев лонгов → contrarian SHORT
        elif a["type"] == "funding_extreme" and a["value"] < 0:
            long_signals += 1
        elif a["type"] == "ls_extreme" and a["value"] > 2:
            short_signals += 1
        elif a["type"] == "ls_extreme" and a["value"] < 0.5:
            long_signals += 1
        elif a["type"] == "oi_spike" and a["value"] > 0:
            long_signals += 1
        elif a["type"] == "oi_spike" and a["value"] < 0:
            short_signals += 1
        elif a["type"] == "taker_imbalance" and a["value"] > 1.3:
            long_signals += 1
        elif a["type"] == "taker_imbalance" and a["value"] < 0.7:
            short_signals += 1

    if long_signals > short_signals:
        direction = "LONG"
    elif short_signals > long_signals:
        direction = "SHORT"

    return {
        "symbol": symbol,
        "pair": symbol.replace("USDT", "/USDT"),
        "price": price,
        "score": len(anomalies),
        "direction": direction,
        "anomalies": anomalies,
    }


def scan_batch(symbols: list[str], min_score: int = 2) -> list[dict]:
    """Сканирует батч пар. Возвращает только с score >= min_score."""
    results = []
    for s in symbols:
        try:
            r = scan_symbol(s)
            if r and r["score"] >= min_score:
                results.append(r)
        except Exception as e:
            logger.debug(f"Scan {s}: {e}")
    return sorted(results, key=lambda x: -x["score"])
