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


def check_oi_spike(symbol: str, threshold: float = 5.0) -> Optional[dict]:
    """OI change за последний час. Returns None если нет аномалии."""
    try:
        r = _http_get(f"{FAPI}/futures/data/openInterestHist",
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
    """Funding rate extreme. Использует batch-кеш."""
    rate = _batch_cache.get("funding", {}).get(symbol)
    if rate is not None and abs(rate) >= threshold:
        return {"type": "funding_extreme", "value": round(rate, 4)}
    return None


def check_ls_ratio(symbol: str, high: float = 2.5, low: float = 0.5) -> Optional[dict]:
    """Top trader Long/Short ratio extreme."""
    try:
        r = _http_get(f"{FAPI}/futures/data/topLongShortPositionRatio",
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
        r = _http_get(f"{FAPI}/futures/data/takerlongshortRatio",
                      params={"symbol": symbol, "period": "1h", "limit": 1}, timeout=5)
        if r.status_code != 200 or not r.json():
            return None
        ratio = float(r.json()[0].get("buySellRatio", 1))
        if ratio >= high or ratio <= low:
            return {"type": "taker_imbalance", "value": round(ratio, 2)}
    except Exception:
        pass
    return None


def check_trade_speed(symbol: str, multiplier: float = 3.0) -> Optional[dict]:
    """Speed Print аналог: количество сделок за последнюю минуту vs среднее.
    Если > multiplier× → алгоритм или кит активен."""
    try:
        now_ms = int(time.time() * 1000)
        one_min = 60 * 1000
        # Последняя минута
        r1 = _http_get(f"{FAPI}/fapi/v1/aggTrades",
                       params={"symbol": symbol, "startTime": now_ms - one_min, "endTime": now_ms, "limit": 1000},
                       timeout=5)
        if r1.status_code != 200:
            return None
        recent_count = len(r1.json())

        # 10 минут назад (для среднего)
        r2 = _http_get(f"{FAPI}/fapi/v1/aggTrades",
                       params={"symbol": symbol, "startTime": now_ms - 10 * one_min, "endTime": now_ms - 9 * one_min, "limit": 1000},
                       timeout=5)
        if r2.status_code != 200:
            return None
        avg_count = len(r2.json())

        if avg_count <= 0:
            return None

        ratio = recent_count / avg_count
        if ratio >= multiplier:
            return {"type": "trade_speed", "value": round(ratio, 1), "trades_per_min": recent_count}
    except Exception:
        pass
    return None


def check_delta_clusters(symbol: str) -> Optional[dict]:
    """Delta по ценовым уровням из aggTrades.
    Группирует сделки по цене, считает buy-sell delta на каждом уровне.
    Возвращает аномалию если на экстремуме сильный дисбаланс."""
    try:
        now_ms = int(time.time() * 1000)
        r = _http_get(f"{FAPI}/fapi/v1/aggTrades",
                      params={"symbol": symbol, "startTime": now_ms - 5 * 60 * 1000, "limit": 1000},
                      timeout=5)
        if r.status_code != 200:
            return None
        trades = r.json()
        if len(trades) < 50:
            return None

        # Определяем шаг группировки по цене
        prices = [float(t["p"]) for t in trades]
        price_range = max(prices) - min(prices)
        if price_range <= 0:
            return None
        step = price_range / 20  # 20 уровней
        if step <= 0:
            step = 0.01

        # Группируем
        clusters = {}
        for t in trades:
            p = float(t["p"])
            q = float(t["q"])
            level = round(p / step) * step
            if level not in clusters:
                clusters[level] = {"buy": 0, "sell": 0}
            if t.get("m"):  # maker = sell
                clusters[level]["sell"] += q
            else:
                clusters[level]["buy"] += q

        # Считаем delta на каждом уровне
        deltas = []
        for level, v in clusters.items():
            delta = v["buy"] - v["sell"]
            total = v["buy"] + v["sell"]
            deltas.append({"level": round(level, 6), "delta": round(delta, 2), "total": round(total, 2)})

        if not deltas:
            return None

        # Ищем экстремальный delta
        max_delta = max(deltas, key=lambda d: abs(d["delta"]))
        avg_total = sum(d["total"] for d in deltas) / len(deltas)

        # Аномалия: delta > 60% от total на этом уровне И total > 2× среднего
        if abs(max_delta["delta"]) > max_delta["total"] * 0.6 and max_delta["total"] > avg_total * 2:
            return {
                "type": "delta_cluster",
                "value": max_delta["delta"],
                "level": max_delta["level"],
                "total": max_delta["total"],
            }
    except Exception:
        pass
    return None


def _check_ftt_tf(symbol: str, interval: str = "1h") -> Optional[dict]:
    """FTT на одном таймфрейме. Возвращает dict с ftt_score или None."""
    try:
        r = _http_get(f"{FAPI}/fapi/v1/klines",
                      params={"symbol": symbol, "interval": interval, "limit": 5}, timeout=5)
        if r.status_code != 200:
            return None
        candles = r.json()
        if len(candles) < 5:
            return None

        c = candles[-1]
        o, h, l, cl, vol = float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])
        body = abs(cl - o)
        full_range = h - l
        if full_range <= 0 or body <= 0:
            return None

        upper_wick = h - max(o, cl)
        lower_wick = min(o, cl) - l

        # SMA объёма за 4 предыдущие свечи
        prev_vols = [float(candles[i][5]) for i in range(-5, -1)]
        avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else vol
        if avg_vol <= 0:
            return None
        vol_ratio = vol / avg_vol

        # Определяем направление FTT
        is_upper_ftt = upper_wick > body * 1.5 and upper_wick > full_range * 0.45
        is_lower_ftt = lower_wick > body * 1.5 and lower_wick > full_range * 0.45

        if not is_upper_ftt and not is_lower_ftt:
            return None

        ftt_dir = "SHORT" if is_upper_ftt else "LONG"
        wick = upper_wick if is_upper_ftt else lower_wick

        # ── Scoring FTT quality (1-5) ──────────────────────────

        ftt_score = 0

        # 1. Wick длина (чем длиннее тень, тем сильнее отвержение)
        wick_pct = wick / full_range
        if wick_pct > 0.7:
            ftt_score += 2
        elif wick_pct > 0.55:
            ftt_score += 1

        # 2. Объём выше среднего
        if vol_ratio > 2.0:
            ftt_score += 2
        elif vol_ratio > 1.3:
            ftt_score += 1

        # 3. Закрытие далеко от экстремума (сильное отвержение)
        if is_upper_ftt:
            close_position = (h - cl) / full_range  # 1 = закрылся на лоу
            if close_position > 0.7:
                ftt_score += 1
        else:
            close_position = (cl - l) / full_range  # 1 = закрылся на хае
            if close_position > 0.7:
                ftt_score += 1

        # 4. Предыдущие свечи шли в направлении тени (тренд → разворот)
        prev_direction = 0
        for i in range(-4, -1):
            pc = candles[i]
            if float(pc[4]) > float(pc[1]):
                prev_direction += 1  # bullish
            else:
                prev_direction -= 1  # bearish

        if is_upper_ftt and prev_direction >= 2:  # был рост → теперь SHORT
            ftt_score += 1
        elif is_lower_ftt and prev_direction <= -2:  # было падение → теперь LONG
            ftt_score += 1

        # 5. aggTrades delta на экстремуме (кто торговал на тени)
        try:
            now_ms = int(time.time() * 1000)
            tr = _http_get(f"{FAPI}/fapi/v1/aggTrades",
                           params={"symbol": symbol, "startTime": now_ms - 60 * 60 * 1000, "limit": 500},
                           timeout=5)
            if tr.status_code == 200:
                trades = tr.json()
                # Делим сделки на "в зоне тени" и "в зоне тела"
                if is_upper_ftt:
                    wick_zone = max(o, cl)  # тень выше тела
                    wick_trades_sell = sum(float(t["q"]) for t in trades if float(t["p"]) > wick_zone and t.get("m"))
                    wick_trades_buy = sum(float(t["q"]) for t in trades if float(t["p"]) > wick_zone and not t.get("m"))
                    # SHORT FTT: на верхней тени должны доминировать продажи
                    if wick_trades_sell > wick_trades_buy * 1.5 and wick_trades_sell > 0:
                        ftt_score += 1
                else:
                    wick_zone = min(o, cl)
                    wick_trades_buy = sum(float(t["q"]) for t in trades if float(t["p"]) < wick_zone and not t.get("m"))
                    wick_trades_sell = sum(float(t["q"]) for t in trades if float(t["p"]) < wick_zone and t.get("m"))
                    # LONG FTT: на нижней тени должны доминировать покупки
                    if wick_trades_buy > wick_trades_sell * 1.5 and wick_trades_buy > 0:
                        ftt_score += 1
        except Exception:
            pass

        # Минимум 2 балла для сигнала
        if ftt_score < 2:
            return None

        return {
            "type": "ftt",
            "value": ftt_dir,
            "ftt_score": min(ftt_score, 5),
            "wick_ratio": round(wick_pct, 2),
            "vol_ratio": round(vol_ratio, 1),
            "close_position": round(close_position, 2),
            "tf": interval,
        }
    except Exception:
        pass
    return None


def check_ftt(symbol: str) -> Optional[dict]:
    """Multi-TF FTT: проверяет 15m, 1h, 4h. Берёт лучший с бонусом за совпадения."""
    results = []
    tf_weights = {"15m": 0.7, "1h": 1.0, "4h": 1.5}
    for tf, weight in tf_weights.items():
        r = _check_ftt_tf(symbol, tf)
        if r:
            r["_weight"] = weight
            results.append(r)

    if not results:
        return None

    # Берём лучший по score × weight
    best = max(results, key=lambda x: x["ftt_score"] * x["_weight"])
    # Бонус за подтверждение на нескольких TF
    if len(results) >= 2:
        best["ftt_score"] = min(best["ftt_score"] + 1, 5)
    if len(results) >= 3:
        best["ftt_score"] = 5  # все 3 TF = идеальный FTT

    tfs = "+".join(r["tf"] for r in results)
    best["value"] = best["value"]  # LONG/SHORT
    best.pop("_weight", None)
    best["tf"] = tfs
    return best


def check_orderbook_wall(symbol: str, multiplier: float = 10.0) -> Optional[dict]:
    """Ищет стены в order book (объём > multiplier × среднего)."""
    try:
        r = _http_get(f"{FAPI}/fapi/v1/depth",
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

    speed = check_trade_speed(symbol)
    if speed:
        anomalies.append(speed)

    delta = check_delta_clusters(symbol)
    if delta:
        anomalies.append(delta)

    ftt = check_ftt(symbol)
    if ftt:
        anomalies.append(ftt)

    if not anomalies:
        return None

    # Текущая цена (из batch-кеша, без HTTP)
    price = _batch_cache.get("ticker", {}).get(symbol, {}).get("price")

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
        elif a["type"] == "ftt":
            if a["value"] == "LONG":
                long_signals += 2  # FTT = сильный сигнал
            else:
                short_signals += 2
        elif a["type"] == "delta_cluster":
            if a["value"] > 0:
                long_signals += 1
            else:
                short_signals += 1

    if long_signals > short_signals:
        direction = "LONG"
    elif short_signals > long_signals:
        direction = "SHORT"

    # ── Weighted Score (не просто количество, а качество) ──
    types = [a["type"] for a in anomalies]
    has_ftt = "ftt" in types
    has_delta = "delta_cluster" in types
    has_speed = "trade_speed" in types
    has_oi = "oi_spike" in types

    # Весовая система: FTT и Delta — ключевые
    wscore = 0
    for a in anomalies:
        if a["type"] == "ftt":
            ftt_pts = a.get("ftt_score", 3)
            wscore += 2 + (1 if ftt_pts >= 4 else 0)  # 2-3 очка
        elif a["type"] == "delta_cluster":
            wscore += 2
        elif a["type"] == "trade_speed":
            wscore += 1.5
        elif a["type"] == "oi_spike":
            wscore += 1.5
        else:
            wscore += 1  # funding, ls, taker, wall

    # Бонус за комбинации
    if has_ftt and has_delta:
        wscore += 2  # FTT + Delta = сильная связка
    if has_ftt and has_speed:
        wscore += 1  # FTT + Speed = подтверждение

    return {
        "symbol": symbol,
        "pair": symbol.replace("USDT", "/USDT"),
        "price": price,
        "score": round(wscore, 1),
        "raw_count": len(anomalies),
        "direction": direction,
        "anomalies": anomalies,
        "has_ftt": has_ftt,
        "has_delta": has_delta,
    }


def scan_batch(symbols: list[str], min_score: float = 10.0) -> list[dict]:
    """Сканирует батч пар. Жёсткий фильтр качества.

    Требования для прохождения:
    - wscore >= min_score (по умолчанию 5)
    - Обязательно FTT или Delta Cluster (ключевые разворотные паттерны)
    - Минимум 3 разных типа аномалий
    """
    results = []
    for s in symbols:
        try:
            r = scan_symbol(s)
            if not r:
                continue
            # Жёсткие фильтры
            if r["score"] < min_score:
                continue
            if not r["has_ftt"] and not r["has_delta"]:
                continue  # без FTT/Delta — слабый сигнал
            if r["raw_count"] < 3:
                continue  # минимум 3 типа аномалий
            results.append(r)
        except Exception as e:
            logger.debug(f"Scan {s}: {e}")
    return sorted(results, key=lambda x: -x["score"])
