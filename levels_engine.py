"""📐 Levels Engine — собственные S/R уровни, считаются из свечей.

Заменяет удалённые Key Levels (парсились из Telegram — источник отключён).
Никаких внешних источников: только klines (Binance с BingX fallback).

Алгоритм (v1, свинги + кластеризация):
  1. Берём N свечей для TF (см. TF_CONFIG).
  2. Находим свинг-хаи/лоу: экстремум с `wing` барами слева и справа.
  3. ATR(14) — допуск кластеризации: свинги ближе cluster_atr_mult×ATR
     склеиваются в одну ЗОНУ (low..high).
  4. Сила зоны = касания + свежесть последнего касания.
  5. Классификация: зона выше текущей цены = resistance, ниже = support.
     Зона, внутри которой цена сейчас, помечается 'inside' (пробой/ретест).
  6. Отдаём top-N зон каждой стороны (ближайшие к цене — приоритет).

Выход compute_levels():
  [{low, high, mid, kind: 'support'|'resistance'|'inside',
    touches, strength: 0-100, last_touch_ts, first_touch_ts, tf}]

Каждый TF считается из СВОИХ свечей — 15m уровни ≠ ресемпл 1h.
"""
from __future__ import annotations
import logging
import math
from typing import Optional

logger = logging.getLogger(__name__)

# Конфиг per-TF: сколько свечей брать, размер "крыла" свинга,
# множитель ATR для склейки свингов в зону.
TF_CONFIG = {
    "15m": {"bars": 400, "wing": 3, "cluster_atr_mult": 0.45},
    "30m": {"bars": 400, "wing": 3, "cluster_atr_mult": 0.45},
    "1h":  {"bars": 500, "wing": 4, "cluster_atr_mult": 0.50},
    "4h":  {"bars": 400, "wing": 4, "cluster_atr_mult": 0.55},
    "12h": {"bars": 300, "wing": 3, "cluster_atr_mult": 0.55},
    "1d":  {"bars": 365, "wing": 3, "cluster_atr_mult": 0.60},
}
DEFAULT_TFS = ("15m", "1h", "4h", "1d")
MAX_ZONES_PER_SIDE = 4   # top-N поддержек и top-N сопротивлений
MIN_TOUCHES = 2          # зона с 1 касанием = шум, не показываем


def _atr(candles: list[dict], period: int = 14) -> Optional[float]:
    """Классический ATR (Wilder). candles: [{'h','l','c'},...] старые→новые."""
    n = len(candles)
    if n < period + 1:
        return None
    trs = []
    for i in range(1, n):
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i - 1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


def _find_swings(candles: list[dict], wing: int) -> tuple[list, list]:
    """Свинг-хаи и свинг-лоу. Возвращает ([(idx, price, ts)], [(idx, price, ts)])."""
    highs, lows = [], []
    n = len(candles)
    for i in range(wing, n - wing):
        h = candles[i]["h"]
        l = candles[i]["l"]
        window = candles[i - wing:i + wing + 1]
        if h == max(c["h"] for c in window):
            highs.append((i, h, candles[i]["t"]))
        if l == min(c["l"] for c in window):
            lows.append((i, l, candles[i]["t"]))
    return highs, lows


def _cluster_swings(swings: list[tuple], tolerance: float) -> list[dict]:
    """Склеивает свинги по цене (в пределах tolerance) в зоны.
    swings: [(idx, price, ts)] — любой стороны."""
    if not swings:
        return []
    by_price = sorted(swings, key=lambda s: s[1])
    clusters: list[list[tuple]] = [[by_price[0]]]
    for s in by_price[1:]:
        # Сравниваем с центром текущего кластера
        cur = clusters[-1]
        center = sum(x[1] for x in cur) / len(cur)
        if abs(s[1] - center) <= tolerance:
            cur.append(s)
        else:
            clusters.append([s])
    zones = []
    for cl in clusters:
        prices = [x[1] for x in cl]
        tss = [x[2] for x in cl]
        zones.append({
            "low": min(prices),
            "high": max(prices),
            "mid": sum(prices) / len(prices),
            "touches": len(cl),
            "first_touch_ts": min(tss),
            "last_touch_ts": max(tss),
        })
    return zones


def _score_zone(z: dict, now_ts: int, span_s: float) -> int:
    """Сила зоны 0-100: касания (до 60) + свежесть последнего касания (до 40)."""
    touch_score = min(60, z["touches"] * 15)  # 2 кас=30, 4 кас=60
    age = max(0.0, now_ts - z["last_touch_ts"] / (1000 if z["last_touch_ts"] > 1e12 else 1))
    recency = max(0.0, 1.0 - (age / span_s)) if span_s > 0 else 0.0
    return int(round(touch_score + recency * 40))


def compute_levels(pair: str, tf: str, candles: Optional[list[dict]] = None) -> list[dict]:
    """Главная функция: S/R зоны для пары на TF.

    candles (опционально) — [{'t','o','h','l','c','v'},...] старые→новые;
    если не переданы — тянем через exchange.get_klines_any (BingX fallback).
    't' в миллисекундах (как отдаёт get_klines).
    """
    cfg = TF_CONFIG.get(tf)
    if not cfg:
        return []
    if candles is None:
        try:
            from exchange import get_klines_any
            candles = get_klines_any(pair, tf, cfg["bars"])
        except Exception:
            logger.debug(f"[levels] klines fail {pair} {tf}", exc_info=True)
            return []
    if not candles or len(candles) < cfg["wing"] * 2 + 20:
        return []

    atr = _atr(candles)
    if not atr or atr <= 0:
        return []
    tolerance = atr * cfg["cluster_atr_mult"]

    sw_highs, sw_lows = _find_swings(candles, cfg["wing"])
    # Кластеризуем ХАИ и ЛОУ вместе: уровень, который был и сопротивлением
    # и поддержкой (flip) — самый интересный. Касания суммируются.
    zones = _cluster_swings(sw_highs + sw_lows, tolerance)
    zones = [z for z in zones if z["touches"] >= MIN_TOUCHES]
    if not zones:
        return []

    price_now = candles[-1]["c"]
    t_first = candles[0]["t"]
    t_last = candles[-1]["t"]
    # ts в мс → сек для скоринга
    span_s = max(1.0, (t_last - t_first) / 1000.0)
    now_s = t_last / 1000.0

    out = []
    for z in zones:
        # Зона не должна быть тоньше 0.15×ATR (визуально — полоска)
        if z["high"] - z["low"] < atr * 0.15:
            pad = (atr * 0.15 - (z["high"] - z["low"])) / 2
            z["low"] -= pad
            z["high"] += pad
        if z["low"] <= price_now <= z["high"]:
            kind = "inside"
        elif z["mid"] > price_now:
            kind = "resistance"
        else:
            kind = "support"
        last_ts_s = z["last_touch_ts"] / (1000.0 if z["last_touch_ts"] > 1e12 else 1.0)
        age = max(0.0, now_s - last_ts_s)
        recency = max(0.0, 1.0 - age / span_s)
        strength = int(round(min(60, z["touches"] * 15) + recency * 40))
        out.append({
            "low": z["low"], "high": z["high"], "mid": z["mid"],
            "kind": kind, "touches": z["touches"], "strength": strength,
            "first_touch_ts": int(z["first_touch_ts"]),
            "last_touch_ts": int(z["last_touch_ts"]),
            "dist_pct": round((z["mid"] - price_now) / price_now * 100, 2),
            "tf": tf,
        })

    # top-N с каждой стороны по силе + ВСЕГДА крайние зоны (самое верхнее
    # сопротивление и самая нижняя поддержка) — иначе края диапазона
    # оставались без уровней (фидбек 2026-07-02).
    res = [z for z in out if z["kind"] == "resistance"]
    sup = [z for z in out if z["kind"] == "support"]
    ins = [z for z in out if z["kind"] == "inside"]
    res.sort(key=lambda z: (-z["strength"], abs(z["dist_pct"])))
    sup.sort(key=lambda z: (-z["strength"], abs(z["dist_pct"])))
    final = res[:MAX_ZONES_PER_SIDE] + sup[:MAX_ZONES_PER_SIDE] + ins[:2]
    if res:
        top_res = max(res, key=lambda z: z["mid"])
        if top_res not in final:
            final.append(top_res)
    if sup:
        bot_sup = min(sup, key=lambda z: z["mid"])
        if bot_sup not in final:
            final.append(bot_sup)
    final.sort(key=lambda z: z["mid"], reverse=True)
    return final


def compute_all_tfs(pair: str, tfs: tuple = DEFAULT_TFS) -> dict:
    """Уровни по всем TF для пары. Возвращает {tf: [zones]}."""
    return {tf: compute_levels(pair, tf) for tf in tfs}


def refresh_pair_levels(pair: str, tfs: tuple = DEFAULT_TFS) -> int:
    """Пересчитывает и сохраняет уровни пары в Mongo computed_levels.
    Возвращает число сохранённых зон. Вызывается из watcher-лупа (thread)."""
    try:
        from database import _get_db, utcnow
        db = _get_db()
        col = db.computed_levels
        total = 0
        for tf in tfs:
            zones = compute_levels(pair, tf)
            col.update_one(
                {"pair": pair, "tf": tf},
                {"$set": {"zones": zones, "updated_at": utcnow(),
                          "price_checked": True}},
                upsert=True,
            )
            total += len(zones)
        return total
    except Exception:
        logger.debug(f"[levels] refresh fail {pair}", exc_info=True)
        return 0


def get_levels_cached(pair: str, tf: str, max_age_min: int = 30) -> Optional[dict]:
    """Читает уровни из Mongo. None если нет или старше max_age_min
    (тогда caller решает — пересчитать on-demand)."""
    try:
        from database import _get_db, utcnow
        from datetime import timedelta
        doc = _get_db().computed_levels.find_one({"pair": pair, "tf": tf})
        if not doc:
            return None
        upd = doc.get("updated_at")
        if upd and (utcnow() - upd) > timedelta(minutes=max_age_min):
            return None
        return {"zones": doc.get("zones", []),
                "updated_at": upd.isoformat() if upd else None}
    except Exception:
        return None


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)
    for tf in ("1h", "4h"):
        zones = compute_levels("BTC/USDT", tf)
        print(f"\n=== BTC/USDT {tf}: {len(zones)} zones ===")
        for z in zones:
            print(f"  {z['kind']:<11} {z['low']:.2f}-{z['high']:.2f} "
                  f"touches={z['touches']} strength={z['strength']} dist={z['dist_pct']}%")
