"""🧊 ACCUMULATION watchlist — детектор фаз накопления (research 2026-07-10).

НЕ торговый сигнал. Исследование (90д × 30m × 206 пар, 3 итерации:
пробой/объём/OBV/spring/ретест/базы 2-7 дней) показало: фаза накопления
распознаётся надёжно, но направленно-нейтральна — сторона разрешения не
предсказывается ни одним фильтром устойчиво (train/test разваливается).

Честное применение: watchlist. Монета стоит в базе -> наблюдаем, вход
берём по сигналам разрешения (💥 IGNITION / 💰 TEN стреляют на выходе).

Определение базы (1h-свечи, окно 48ч):
  • весь ход (hi-lo) за 48ч < 5% от цены
  • |ход за 24ч| < 2.5% (нет дрейфа)
  • состояние держится >= 12ч подряд
"""
from __future__ import annotations
import logging
from typing import Optional

logger = logging.getLogger(__name__)

W = 48            # окно базы, часов
RNG_MAX = 5.0     # ширина базы, %
ROC24_MAX = 2.5   # дрейф за сутки, %
MIN_HOURS = 12    # сколько часов состояние должно держаться
MAX_BACK = 240    # глубина поиска начала базы


def _state_at(candles: list[dict], idx: int) -> bool:
    """Накопление ли на баре idx (окно W заканчивается на idx включительно)."""
    if idx < W + 25:
        return False
    win = candles[idx - W + 1: idx + 1]
    c = candles[idx]["c"]
    if not c:
        return False
    hi = max(x["h"] for x in win)
    lo = min(x["l"] for x in win)
    if (hi - lo) / c * 100 >= RNG_MAX:
        return False
    c24 = candles[idx - 24]["c"]
    if not c24 or abs(c / c24 - 1) * 100 >= ROC24_MAX:
        return False
    return True


def check_pair(pair: str, candles_1h: Optional[list[dict]] = None) -> Optional[dict]:
    try:
        if candles_1h is None:
            from exchange import get_klines_any
            candles_1h = get_klines_any(pair, "1h", 320)
        if not candles_1h or len(candles_1h) < W + 30:
            return None
        last = len(candles_1h) - 1
        if not _state_at(candles_1h, last):
            return None
        hours = 1
        while hours < MAX_BACK and last - hours > W + 25 and _state_at(candles_1h, last - hours):
            hours += 1
        if hours < MIN_HOURS:
            return None
        win = candles_1h[last - W + 1:]
        base_hi = max(x["h"] for x in win)
        base_lo = min(x["l"] for x in win)
        price = candles_1h[last]["c"]
        return {
            "pair": pair,
            "symbol": pair.replace("/", "").upper(),
            "price": price,
            "base_hi": base_hi,
            "base_lo": base_lo,
            "rng_pct": round((base_hi - base_lo) / price * 100, 2),
            "hours": hours,
            "dist_up_pct": round((base_hi - price) / price * 100, 2),
            "dist_dn_pct": round((price - base_lo) / price * 100, 2),
        }
    except Exception:
        logger.debug(f"[accum] check fail {pair}", exc_info=True)
        return None


def scan_universe(max_pairs: int = 300) -> list[dict]:
    """Скан ликвидных пар. Вызывается из watcher-лупа (thread)."""
    from futures_data import get_liquid_pairs
    out = []
    try:
        pairs = get_liquid_pairs(min_volume_usd=5_000_000)[:max_pairs]
    except Exception:
        return out
    for sym in pairs:
        pair = sym.replace("USDT", "/USDT") if "/" not in sym else sym
        try:
            st = check_pair(pair)
            if st:
                out.append(st)
        except Exception:
            continue
    out.sort(key=lambda x: -x["hours"])
    return out


def track_resolutions(new_items: list[dict]) -> list[dict]:
    """Пары, вышедшие из базы с пробоем границы, пишутся в accum_events
    (source='accum' в журнале: 🧊 база → ВВЕРХ/ВНИЗ). Вызывать ДО
    store_snapshot — сравнивает с прошлым снапшотом."""
    events = []
    try:
        from database import _get_db, utcnow
        from exchange import get_klines_any
        db = _get_db()
        prev = {d["pair"]: d for d in db.accum_state.find()}
        cur = {it["pair"] for it in new_items}
        for pair, d in prev.items():
            if pair in cur:
                continue
            try:
                kl = get_klines_any(pair, "1h", 3)
                price = kl[-1]["c"] if kl else None
            except Exception:
                price = None
            if not price or not d.get("base_hi") or not d.get("base_lo"):
                continue
            if price > d["base_hi"]:
                resolution, direction = "UP", "LONG"
            elif price < d["base_lo"]:
                resolution, direction = "DOWN", "SHORT"
            else:
                continue    # растворилась без пробоя — не событие
            events.append({
                "pair": pair, "symbol": d.get("symbol"),
                "direction": direction, "resolution": resolution,
                "base_hi": d["base_hi"], "base_lo": d["base_lo"],
                "rng_pct": d.get("rng_pct"), "hours": d.get("hours"),
                "res_price": price, "resolved_at": utcnow(),
            })
        if events:
            db.accum_events.insert_many(events)
            logger.info(f"[accum] разрешений: "
                        f"{[(e['pair'], e['resolution']) for e in events]}")
    except Exception:
        logger.exception("[accum] resolutions fail")
    return events


def store_snapshot(items: list[dict]) -> None:
    """Полная замена снапшота (актуальный список баз)."""
    try:
        from database import _get_db, utcnow
        col = _get_db().accum_state
        col.delete_many({})
        now = utcnow()
        if items:
            col.insert_many([{**it, "updated_at": now} for it in items])
    except Exception:
        logger.exception("[accum] store fail")
