"""Smart Levels — автоматические уровни для графиков:
- Historical Cluster Levels (из нашей БД clusters)
- Auto S/R (из свингов свечей)
- PDH/PDL/PWH/PWL (вчерашние/недельные экстремумы)
- Round Numbers (psychological levels)

Все уровни возвращаются с метаданными для визуализации на LWC.
"""
from __future__ import annotations
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from database import _clusters, utcnow

logger = logging.getLogger(__name__)


# ─── 1. Historical Cluster Levels ────────────────────────────────────────
def get_cluster_levels(symbol: str, days: int = 30, limit: int = 20) -> list[dict]:
    """Все прошлые trigger_price по паре за период.
    Returns: [{price, outcome: 'TP'|'SL'|'OPEN', direction, at, pnl, strength}, ...]
    """
    norm_pair = symbol.replace("USDT", "/USDT") if "/" not in symbol else symbol
    norm_sym = norm_pair.replace("/", "")
    since = utcnow() - timedelta(days=days)

    docs = list(_clusters().find({
        "$or": [{"symbol": norm_sym}, {"pair": norm_pair}],
        "trigger_at": {"$gte": since},
    }).sort("trigger_at", -1).limit(limit))

    out = []
    for c in docs:
        price = c.get("trigger_price")
        if not price:
            continue
        status = c.get("status", "OPEN")
        outcome = status if status in ("TP", "SL") else "OPEN"
        at = c.get("trigger_at")
        at_iso = at.isoformat() if hasattr(at, "isoformat") else str(at)
        out.append({
            "price": float(price),
            "outcome": outcome,
            "direction": c.get("direction", ""),
            "at": at_iso,
            "at_ts": int(at.timestamp()) if hasattr(at, "timestamp") else 0,
            "pnl": round(c.get("pnl_percent") or 0, 2),
            "strength": c.get("strength", "NORMAL"),
            "sources": c.get("sources_count", 0),
        })
    return out


# ─── 2. Auto S/R (swing highs/lows + clustering) ─────────────────────────
def compute_sr_levels(candles: list[dict], pivot_window: int = 5, cluster_tolerance: float = 0.003, top_n: int = 5) -> list[dict]:
    """Находит свинг high/low и кластеризует близкие.

    pivot_window = бары слева/справа от пивота (чем больше → меньше уровней, но сильнее)
    cluster_tolerance = 0.3% — уровни ближе этого склеиваются
    top_n = возвращаем top-N сильнейших (по количеству касаний)
    """
    if len(candles) < pivot_window * 2 + 1:
        return []

    # 1. Пивоты
    swing_highs = []
    swing_lows = []
    for i in range(pivot_window, len(candles) - pivot_window):
        h = candles[i]["high"] if "high" in candles[i] else candles[i]["h"]
        l = candles[i]["low"]  if "low"  in candles[i] else candles[i]["l"]
        left = candles[i - pivot_window:i]
        right = candles[i + 1:i + 1 + pivot_window]
        is_high = all((c.get("high") or c.get("h", 0)) < h for c in left + right)
        is_low  = all((c.get("low")  or c.get("l", 10**12)) > l for c in left + right)
        if is_high:
            swing_highs.append({"price": h, "idx": i, "ts": candles[i].get("time") or candles[i].get("t")})
        if is_low:
            swing_lows.append({"price": l, "idx": i, "ts": candles[i].get("time") or candles[i].get("t")})

    all_pivots = [(p["price"], "R") for p in swing_highs] + [(p["price"], "S") for p in swing_lows]
    if not all_pivots:
        return []

    # 2. Кластеризация — близкие уровни склеиваем
    all_pivots.sort(key=lambda x: x[0])
    clusters: list[dict] = []
    for price, kind in all_pivots:
        if clusters and abs(price - clusters[-1]["avg"]) / clusters[-1]["avg"] < cluster_tolerance:
            clusters[-1]["touches"].append(price)
            clusters[-1]["kinds"].append(kind)
            clusters[-1]["avg"] = sum(clusters[-1]["touches"]) / len(clusters[-1]["touches"])
        else:
            clusters.append({"avg": price, "touches": [price], "kinds": [kind]})

    # 3. Ранжируем по силе (кол-во touches)
    clusters.sort(key=lambda c: -len(c["touches"]))

    # 4. Формат вывода
    out = []
    for c in clusters[:top_n]:
        r_count = c["kinds"].count("R")
        s_count = c["kinds"].count("S")
        level_type = "resistance" if r_count > s_count else "support"
        if r_count == s_count:
            level_type = "mixed"
        out.append({
            "price": round(c["avg"], 8),
            "touches": len(c["touches"]),
            "type": level_type,
            "strength": min(5, max(1, len(c["touches"]))),
        })
    return out


# ─── 3. PDH/PDL/PWH/PWL ──────────────────────────────────────────────────
def compute_period_extremes(candles: list[dict]) -> dict:
    """Считает PDH/PDL (previous day) и PWH/PWL (previous week) из свечей.

    candles — это 1h/15m/etc свечи. Группируем по дате.
    Вчерашний день = current UTC day - 1. Прошлая неделя = последние 7 дней до вчера.
    """
    if not candles:
        return {}

    now_ts = utcnow().timestamp()
    today_start = datetime.fromtimestamp(now_ts, tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    yesterday_start = today_start - 86400
    week_start = today_start - 7 * 86400

    prev_day  = [c for c in candles if yesterday_start <= (c.get("time") or c.get("t", 0)) < today_start]
    prev_week = [c for c in candles if week_start      <= (c.get("time") or c.get("t", 0)) < today_start]

    def _max_high(arr):
        return max((c.get("high") or c.get("h", 0)) for c in arr) if arr else None
    def _min_low(arr):
        return min((c.get("low")  or c.get("l", 10**12)) for c in arr) if arr else None

    return {
        "pdh": _max_high(prev_day),
        "pdl": _min_low(prev_day),
        "pwh": _max_high(prev_week),
        "pwl": _min_low(prev_week),
    }


# ─── 4. Round Numbers ────────────────────────────────────────────────────
def compute_round_levels(current_price: float, pct_range: float = 0.05) -> list[dict]:
    """Круглые уровни в диапазоне ± pct_range вокруг текущей цены.

    Шаг определяется по магнитуде цены:
      price >= 10000 → step 1000 (BTC)
      price >= 1000  → step 100  (ETH)
      price >= 100   → step 10   (SOL/BNB)
      price >= 10    → step 1    (ADA тип)
      price >= 1     → step 0.1
      price >= 0.1   → step 0.01
      price >= 0.01  → step 0.001
      else           → step 0.0001
    """
    if current_price is None or current_price <= 0:
        return []

    if current_price >= 10000:
        step = 1000
    elif current_price >= 1000:
        step = 100
    elif current_price >= 100:
        step = 10
    elif current_price >= 10:
        step = 1
    elif current_price >= 1:
        step = 0.1
    elif current_price >= 0.1:
        step = 0.01
    elif current_price >= 0.01:
        step = 0.001
    else:
        step = 0.0001

    lo = current_price * (1 - pct_range)
    hi = current_price * (1 + pct_range)

    import math
    start = math.floor(lo / step) * step
    out = []
    price = start
    # ограничиваем 20 уровней чтоб не захламлять график
    count = 0
    while price <= hi and count < 20:
        if price >= lo and price != 0:
            # Major = кратен 10× step (например, круглое 10000 из step=1000)
            magnitude = "major" if abs(price / (step * 10) - round(price / (step * 10))) < 1e-9 else "minor"
            out.append({"price": round(price, 8), "magnitude": magnitude, "step": step})
            count += 1
        price += step
    return out


# ─── 5. Pivot Points (classical) ─────────────────────────────────────────
def compute_pivot_points(prev_high: Optional[float], prev_low: Optional[float], prev_close: Optional[float]) -> dict:
    """Классические pivot points от предыдущего дня.
    P = (H+L+C)/3
    R1 = 2P - L,  S1 = 2P - H
    R2 = P + (H-L),  S2 = P - (H-L)
    R3 = H + 2(P-L),  S3 = L - 2(H-P)
    """
    if prev_high is None or prev_low is None or prev_close is None:
        return {}
    P = (prev_high + prev_low + prev_close) / 3
    R1 = 2 * P - prev_low
    S1 = 2 * P - prev_high
    R2 = P + (prev_high - prev_low)
    S2 = P - (prev_high - prev_low)
    return {
        "pivot": round(P, 8),
        "r1": round(R1, 8), "s1": round(S1, 8),
        "r2": round(R2, 8), "s2": round(S2, 8),
    }


# ─── Главный API ─────────────────────────────────────────────────────────
def get_smart_levels(symbol: str, candles: list[dict], enable: Optional[list[str]] = None) -> dict:
    """Главная функция: возвращает все smart levels для символа.

    symbol — например "BTCUSDT" или "BTC/USDT"
    candles — список словарей с ключами time/high/low/open/close (или t/h/l/o/c)
    enable — список включённых слоёв: ['clusters','sr','period','rounds','pivots']. None = все.
    """
    enable = enable or ["clusters", "sr", "period", "rounds", "pivots"]
    out = {}

    if "clusters" in enable:
        try:
            out["cluster_levels"] = get_cluster_levels(symbol, days=30, limit=20)
        except Exception as e:
            logger.warning(f"cluster levels fail: {e}")
            out["cluster_levels"] = []

    if "sr" in enable and candles:
        try:
            out["sr_levels"] = compute_sr_levels(candles, pivot_window=5, top_n=5)
        except Exception as e:
            logger.warning(f"sr levels fail: {e}")
            out["sr_levels"] = []

    if "period" in enable and candles:
        try:
            out.update(compute_period_extremes(candles))
        except Exception as e:
            logger.warning(f"period extremes fail: {e}")

    if "rounds" in enable and candles:
        try:
            last = candles[-1]
            last_close = last.get("close") or last.get("c")
            out["round_levels"] = compute_round_levels(last_close, pct_range=0.05)
        except Exception as e:
            logger.warning(f"rounds fail: {e}")
            out["round_levels"] = []

    if "pivots" in enable and candles:
        # Pivots из PDH/PDL/prev close
        extremes = compute_period_extremes(candles) if "period" not in enable else out
        # prev close = последняя свеча предыдущего дня
        now_ts = utcnow().timestamp()
        today_start = datetime.fromtimestamp(now_ts, tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        yesterday_start = today_start - 86400
        prev_day_candles = [c for c in candles if yesterday_start <= (c.get("time") or c.get("t", 0)) < today_start]
        prev_close = None
        if prev_day_candles:
            last_prev = max(prev_day_candles, key=lambda c: c.get("time") or c.get("t", 0))
            prev_close = last_prev.get("close") or last_prev.get("c")
        try:
            out["pivots"] = compute_pivot_points(extremes.get("pdh"), extremes.get("pdl"), prev_close)
        except Exception as e:
            logger.warning(f"pivots fail: {e}")
            out["pivots"] = {}

    return out
