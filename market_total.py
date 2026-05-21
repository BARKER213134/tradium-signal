"""Market TOTAL2 bias indicator.

CoinGecko's /global/market_cap_chart is Pro-only. Free workaround:
  1. Fetch current circulating supply of top-20 altcoins from CoinGecko (1 call)
  2. Fetch 4H klines of each coin from Binance (free, fast)
  3. Compute TOTAL2 ≈ sum(price × supply) per 4H candle for these alts
  4. Compute SuperTrend(10, 3) on 4H TOTAL2 series
  5. Direction → bias

Top-20 alts cover ~85% of TOTAL2 (excl. stablecoins). Supply changes are slow,
so using current supplies for historical mcap is accurate enough for trend detection.

Bias output:
  - 🟢 LONG    if 1d-aggregated ST UP + 4h ST UP
  - 🟡 LONG?   if 1d UP + 4h DOWN (rolling over)
  - 🔴 SHORT   if 1d DOWN + 4h DOWN
  - 🟡 SHORT?  if 1d DOWN + 4h UP (bouncing)
  - ⚪ WAIT    on conflict or no data
"""
from __future__ import annotations
import logging
import time
import httpx
import statistics
from datetime import datetime, timezone, timedelta
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

# Top-20 altcoins by mcap (excl. BTC + stablecoins).
# Each entry: (binance_symbol, coingecko_id)
TOP_ALTS = [
    ("ETHUSDT",  "ethereum"),
    ("BNBUSDT",  "binancecoin"),
    ("SOLUSDT",  "solana"),
    ("XRPUSDT",  "ripple"),
    ("DOGEUSDT", "dogecoin"),
    ("ADAUSDT",  "cardano"),
    ("AVAXUSDT", "avalanche-2"),
    ("TRXUSDT",  "tron"),
    ("LINKUSDT", "chainlink"),
    ("DOTUSDT",  "polkadot"),
    ("LTCUSDT",  "litecoin"),
    ("NEARUSDT", "near"),
    ("ATOMUSDT", "cosmos"),
    ("ARBUSDT",  "arbitrum"),
    ("OPUSDT",   "optimism"),
    ("TONUSDT",  "the-open-network"),
    ("SHIBUSDT", "shiba-inu"),
    ("PEPEUSDT", "pepe"),
    ("FETUSDT",  "fetch-ai"),
    ("HBARUSDT", "hedera-hashgraph"),
]

# Hardcoded fallback supplies (circulating supply, рефреш ~раз в квартал).
# Используется если CoinGecko API недоступен/rate-limited.
# Snapshot от мая 2026:
FALLBACK_SUPPLIES = {
    "ethereum":          120_400_000,
    "binancecoin":       140_000_000,
    "solana":            540_000_000,
    "ripple":            59_300_000_000,
    "dogecoin":          150_500_000_000,
    "cardano":           36_000_000_000,
    "avalanche-2":       418_000_000,
    "tron":              94_900_000_000,
    "chainlink":         657_000_000,
    "polkadot":          1_500_000_000,
    "litecoin":          76_000_000,
    "near":              1_200_000_000,
    "cosmos":            400_000_000,
    "arbitrum":          5_000_000_000,
    "optimism":          1_700_000_000,
    "the-open-network":  3_400_000_000,
    "shiba-inu":         589_000_000_000_000,
    "pepe":              420_690_000_000_000,
    "fetch-ai":          2_700_000_000,
    "hedera-hashgraph":  42_000_000_000,
}

_cache: dict = {}
_CACHE_TTL = 300.0  # 5 min
_SUPPLY_CACHE_TTL = 6 * 3600.0  # supplies change slowly, cache 6h

_http = httpx.Client(timeout=15.0)


def _fetch_supplies() -> dict[str, float]:
    """Returns {coingecko_id: circulating_supply}. Cached 6h.
    Fallback: hardcoded FALLBACK_SUPPLIES если CoinGecko rate-limited/down.
    """
    now = time.time()
    cached = _cache.get("supplies")
    if cached and (now - cached[0]) < _SUPPLY_CACHE_TTL:
        return cached[1]
    ids = ",".join(cg_id for _, cg_id in TOP_ALTS)
    try:
        r = _http.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={"vs_currency": "usd", "ids": ids, "per_page": 100, "page": 1},
            timeout=10.0,
        )
        if r.status_code == 200:
            data = r.json()
            out = {}
            for item in data:
                cg_id = item.get("id")
                supply = item.get("circulating_supply") or 0
                if cg_id and supply > 0:
                    out[cg_id] = float(supply)
            if len(out) >= 15:  # at least 15 of 20 must succeed
                _cache["supplies"] = (now, out)
                logger.info(f"[total2] fetched supplies from CoinGecko: {len(out)} coins")
                return out
        logger.warning(f"[total2] CoinGecko HTTP {r.status_code} — using fallback supplies")
    except Exception as e:
        logger.warning(f"[total2] CoinGecko fail ({e}) — using fallback supplies")
    # Fallback to hardcoded supplies
    if cached:
        return cached[1]
    out = dict(FALLBACK_SUPPLIES)
    _cache["supplies"] = (now, out)
    logger.info(f"[total2] using FALLBACK_SUPPLIES: {len(out)} coins")
    return out


def _fetch_klines_binance(symbol: str, interval: str = "4h",
                          limit: int = 360) -> list[dict]:
    """Fetch klines. Primary: Vision CDN (всегда работает).
    Если хочется свежее данные (today), будет fallback на fapi — но Vision
    хватает для bias (1-2h lag некритично)."""
    # Vision CDN — primary path (works on any IP, no rate limits)
    out = _fetch_klines_vision(symbol, interval, days=60)
    if out and len(out) >= 50:
        return out
    # Fallback: try fapi (might work on some IPs)
    try:
        r = _http.get(
            "https://fapi.binance.com/fapi/v1/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
            timeout=5.0,  # don't hang on blocked IPs
        )
        if r.status_code == 200:
            rows = r.json()
            return [{
                "t": int(row[0]), "o": float(row[1]), "h": float(row[2]),
                "l": float(row[3]), "c": float(row[4]), "v": float(row[5]),
            } for row in rows]
    except Exception:
        pass
    return out  # may be empty


def _fetch_klines_vision(symbol: str, interval: str = "4h",
                         days: int = 60) -> list[dict]:
    """Fetch via Binance Vision CDN — static daily zips, no rate limit."""
    import io, zipfile, csv
    now = datetime.now(timezone.utc)
    dates = [(now - timedelta(days=d)).strftime("%Y-%m-%d") for d in range(1, days + 1)]
    out = []
    def _fetch_day(ds):
        url = (f"https://data.binance.vision/data/futures/um/daily/klines/"
               f"{symbol}/{interval}/{symbol}-{interval}-{ds}.zip")
        try:
            r = _http.get(url)
            if r.status_code != 200:
                return []
            zf = zipfile.ZipFile(io.BytesIO(r.content))
            rows = csv.reader(zf.read(zf.namelist()[0]).decode().splitlines())
            return [{
                "t": int(row[0]), "o": float(row[1]), "h": float(row[2]),
                "l": float(row[3]), "c": float(row[4]), "v": float(row[5]),
            } for row in rows if row and row[0].isdigit()]
        except Exception:
            return []
    with ThreadPoolExecutor(max_workers=10) as tp:
        for r in tp.map(_fetch_day, dates):
            out.extend(r)
    # Dedupe + sort
    seen = set()
    uniq = []
    for k in out:
        if k["t"] in seen:
            continue
        seen.add(k["t"])
        uniq.append(k)
    uniq.sort(key=lambda x: x["t"])
    return uniq


def _compute_total2_series(interval: str = "4h") -> list[dict]:
    """Aggregates mcap (close_price × supply) for top-20 alts per candle.
    Returns sorted [{t, c}, ...] — TOTAL2 close-price series.
    """
    supplies = _fetch_supplies()
    if not supplies:
        logger.error("[total2] no supplies — abort series build")
        return []

    # Fetch klines in parallel
    klines_by_symbol: dict[str, list] = {}
    def _fetch(item):
        sym, cg_id = item
        return (sym, cg_id, _fetch_klines_binance(sym, interval, 360))

    with ThreadPoolExecutor(max_workers=10) as tp:
        for sym, cg_id, klines in tp.map(_fetch, TOP_ALTS):
            klines_by_symbol[(sym, cg_id)] = klines

    # Diagnostic — count which alts gave us klines
    coins_with_data = sum(1 for kl in klines_by_symbol.values() if kl)
    coins_empty = [s for (s, _), kl in klines_by_symbol.items() if not kl]
    logger.info(f"[total2] coins with klines: {coins_with_data}/{len(TOP_ALTS)}")
    if coins_empty:
        logger.warning(f"[total2] coins with NO klines: {coins_empty}")

    # Aggregate per timestamp
    by_ts: dict[int, float] = {}
    for (sym, cg_id), klines in klines_by_symbol.items():
        supply = supplies.get(cg_id, 0)
        if supply <= 0 or not klines:
            continue
        for k in klines:
            mcap = k["c"] * supply
            by_ts[k["t"]] = by_ts.get(k["t"], 0) + mcap

    series = [{"t": t, "c": v} for t, v in sorted(by_ts.items())]
    logger.info(f"[total2] series built: {len(series)} 4H bars")
    return series


def _compute_supertrend(candles: list[dict], period: int = 10,
                        mult: float = 3.0) -> Optional[dict]:
    """Standard SuperTrend on synthetic candles (only close prices).
    Since we don't have OHLC for TOTAL2, we use close as proxy for h/l too.
    Returns: {state, value, prev_state, last_flip_idx, trend_array}
    """
    n = len(candles)
    if n < period + 5:
        return None
    # Synthetic OHLC: o=c=h=l (no intraday data for TOTAL2 aggregate)
    closes = [c["c"] for c in candles]
    # For ATR we use close-to-close as TR approximation
    trs = [0.0]
    for i in range(1, n):
        trs.append(abs(closes[i] - closes[i-1]))
    # RMA ATR
    atr = [None] * n
    atr[period - 1] = sum(trs[:period]) / period
    for i in range(period, n):
        atr[i] = (atr[i-1] * (period - 1) + trs[i]) / period

    final_upper = [0.0] * n
    final_lower = [0.0] * n
    trend = [0] * n
    for i in range(n):
        if atr[i] is None:
            continue
        # hl2 ≈ close for our synthetic series
        bu = closes[i] + mult * atr[i]
        bl = closes[i] - mult * atr[i]
        if i == 0 or atr[i-1] is None:
            final_upper[i] = bu
            final_lower[i] = bl
            trend[i] = 1
            continue
        final_upper[i] = bu if (bu < final_upper[i-1] or
                                 closes[i-1] > final_upper[i-1]) else final_upper[i-1]
        final_lower[i] = bl if (bl > final_lower[i-1] or
                                 closes[i-1] < final_lower[i-1]) else final_lower[i-1]
        if trend[i-1] == 1 and closes[i] < final_lower[i-1]:
            trend[i] = -1
        elif trend[i-1] == -1 and closes[i] > final_upper[i-1]:
            trend[i] = 1
        else:
            trend[i] = trend[i-1] if trend[i-1] != 0 else 1

    # Find last flip
    last_flip_idx = None
    for i in range(n-1, 0, -1):
        if trend[i] != 0 and trend[i-1] != 0 and trend[i] != trend[i-1]:
            last_flip_idx = i
            break

    cur = trend[-1]
    state = "UP" if cur == 1 else ("DOWN" if cur == -1 else None)
    return {
        "state": state,
        "trend": trend,
        "last_flip_idx": last_flip_idx,
        "value": final_lower[-1] if cur == 1 else final_upper[-1],
        "atr": atr[-1],
    }


def get_market_bias(force_refresh: bool = False) -> dict:
    """Returns {bias, label, color, st_4h, st_1d, history, ...}.

    bias ∈ {LONG, SHORT, WAIT, LONG_CAUTION, SHORT_CAUTION}
    """
    now = time.time()
    cached = _cache.get("bias")
    if cached and not force_refresh and (now - cached[0]) < _CACHE_TTL:
        return cached[1]

    # Compute TOTAL2 on 4h
    series_4h = _compute_total2_series("4h")
    if not series_4h:
        result = {
            "bias": "WAIT", "label": "WAIT", "color": "#888",
            "reason": "no_data", "history": [], "st_4h": None, "st_1d": None,
        }
        _cache["bias"] = (now, result)
        return result

    st_4h = _compute_supertrend(series_4h, period=10, mult=3.0)

    # Build daily aggregation from 4h: take every 6th bar's close as 1d close.
    # 360 4h bars = 60d → 60 daily points.
    daily = []
    for i in range(5, len(series_4h), 6):
        daily.append({"t": series_4h[i]["t"], "c": series_4h[i]["c"]})
    st_1d = _compute_supertrend(daily, period=10, mult=3.0) if len(daily) >= 15 else None

    # Duration в текущем состоянии (с момента последнего flip)
    def _duration_str(secs: float) -> str:
        if secs < 0: return "—"
        if secs < 3600:
            return f"{int(secs/60)}m"
        h = int(secs / 3600)
        if h < 48:
            return f"{h}h"
        d = h // 24
        rem_h = h % 24
        return f"{d}d {rem_h}h" if rem_h else f"{d}d"

    now_ms = int(time.time() * 1000)
    dur_4h_str = "?"
    dur_1d_str = "?"
    if st_4h and st_4h.get("last_flip_idx") is not None and series_4h:
        flip_idx = st_4h["last_flip_idx"]
        if 0 <= flip_idx < len(series_4h):
            dur_4h_str = _duration_str((now_ms - series_4h[flip_idx]["t"]) / 1000)
    if st_1d and st_1d.get("last_flip_idx") is not None and daily:
        flip_idx = st_1d["last_flip_idx"]
        if 0 <= flip_idx < len(daily):
            dur_1d_str = _duration_str((now_ms - daily[flip_idx]["t"]) / 1000)

    # Compose bias
    s4 = (st_4h or {}).get("state")
    s1 = (st_1d or {}).get("state")
    # Bias ориентируется по 4H ST (реактивнее, ловит regime shift раньше).
    # 1D остаётся для контекста в reason но НЕ блокирует/не модифицирует label.
    if s4 == "UP":
        bias, label, color = "LONG", "LONG", "#00e5a0"
        reason = f"4H UP ({dur_4h_str}) · 1D {s1 or '?'} ({dur_1d_str})"
    elif s4 == "DOWN":
        bias, label, color = "SHORT", "SHORT", "#ff4d6d"
        reason = f"4H DOWN ({dur_4h_str}) · 1D {s1 or '?'} ({dur_1d_str})"
    else:
        bias, label, color = "WAIT", "WAIT", "#7a8ba6"
        reason = "нет данных по 4H"

    # Current TOTAL2 value (last close)
    cur_total2 = series_4h[-1]["c"] if series_4h else 0
    # Δ за 24h (last 6 4h bars back)
    if len(series_4h) >= 7:
        prev_24h = series_4h[-7]["c"]
        chg_24h_pct = (cur_total2 - prev_24h) / prev_24h * 100 if prev_24h else 0
    else:
        chg_24h_pct = 0

    # Return last 90 4h candles for mini-chart (15d)
    chart_history = series_4h[-90:] if len(series_4h) > 90 else series_4h

    result = {
        "bias": bias,
        "label": label,
        "color": color,
        "reason": reason,
        "total2_usd": cur_total2,
        "chg_24h_pct": round(chg_24h_pct, 2),
        "st_4h": {
            "state": s4,
            "value": (st_4h or {}).get("value"),
            "duration": dur_4h_str,
        } if st_4h else None,
        "st_1d": {
            "state": s1,
            "value": (st_1d or {}).get("value"),
            "duration": dur_1d_str,
        } if st_1d else None,
        "history": [{"t": c["t"], "c": c["c"]} for c in chart_history],
        "computed_at": now,
        "coins_count": len(TOP_ALTS),
    }
    _cache["bias"] = (now, result)
    logger.info(f"[total2] bias={bias} reason='{reason}' "
                 f"st4h={s4} st1d={s1} total2=${cur_total2/1e9:.1f}B")
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    import json
    r = get_market_bias(force_refresh=True)
    print(json.dumps({k: v for k, v in r.items() if k != "history"},
                     indent=2, default=str))
    print(f"\nHistory points: {len(r.get('history', []))}")
