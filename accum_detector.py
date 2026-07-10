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
import time
from typing import Optional

logger = logging.getLogger(__name__)

W = 48            # окно базы, часов
RNG_MAX = 5.0     # ширина базы, %
ROC24_MAX = 2.5   # дрейф за сутки, %
MIN_HOURS = 12    # сколько часов состояние должно держаться
MAX_BACK = 240    # глубина поиска начала базы


def _fetch_klines_delta(symbol: str, limit: int = 320) -> Optional[list]:
    """1h свечи С тайкер-дельтой (поле 9 = taker buy volume). get_klines_any
    его отбрасывает, поэтому прямой REST: fapi (prod) -> Vision (локально)."""
    import requests
    sym = symbol.replace("/", "").upper()
    for url in ("https://fapi.binance.com/fapi/v1/klines",
                "https://data-api.binance.vision/api/v3/klines"):
        try:
            r = requests.get(url, params=dict(symbol=sym, interval="1h",
                                              limit=limit), timeout=10)
            if r.status_code != 200:
                continue
            rows = r.json()
            if rows and len(rows) > 60:
                return [dict(t=int(x[0]), o=float(x[1]), h=float(x[2]),
                             l=float(x[3]), c=float(x[4]), v=float(x[5]),
                             tb=float(x[9])) for x in rows]
        except Exception:
            continue
    return None


def _delta_z(candles: list[dict], start_idx: int) -> Optional[float]:
    """Кумулятивная тайкер-дельта с start_idx, нормированная random-walk:
    sum(delta) / (std(delta) * sqrt(n)). >2 = явный покупатель, <-2 продавец.
    Research 2026-07-10: базы с dz>2 выходят вверх в 77.5%, с dz<-2 — вниз
    в 77.3% (n=1264). Торговой обёртки нет (ход после разрешения мал) —
    используется как ИНФО в watchlist."""
    try:
        deltas = [2*c["tb"] - c["v"] for c in candles]
        hist = deltas[max(0, start_idx-200):start_idx]
        if len(hist) < 50:
            return None
        mean = sum(hist)/len(hist)
        var = sum((d-mean)**2 for d in hist)/len(hist)
        std = var ** 0.5
        if std <= 0:
            return None
        seg = deltas[start_idx:]
        if not seg:
            return None
        return round(sum(seg) / (std * len(seg) ** 0.5), 2)
    except Exception:
        return None


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
        # 🟢/🔴 кто внутри базы — тайкер-дельта (info, research 07-10).
        # Если свечи пришли уже с полем tb (из scan_universe) — без дозапроса.
        dz = None
        try:
            kd = (candles_1h if candles_1h and "tb" in candles_1h[0]
                  else _fetch_klines_delta(pair))
            if kd and len(kd) > hours + 60:
                dz = _delta_z(kd, len(kd) - hours)
        except Exception:
            pass
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
            "delta_dz": dz,
        }
    except Exception:
        logger.debug(f"[accum] check fail {pair}", exc_info=True)
        return None


_last_scan: dict = {}   # диагностика последнего скана (виден в /api/accumulation)


def scan_universe(max_pairs: int = 300) -> list[dict]:
    """Скан ликвидных пар. Вызывается из watcher-лупа (thread)."""
    from futures_data import get_liquid_pairs
    from database import utcnow
    out = []
    _last_scan.clear()
    _last_scan["started"] = utcnow().isoformat()
    try:
        pairs = get_liquid_pairs(min_volume_usd=5_000_000)[:max_pairs]
        # На 6-й минуте после рестарта ticker-кэш бывает пуст -> 0 пар и
        # пустой снапшот на 30 мин (2026-07-10). Ретрай + фолбэк на полный
        # список фьючерсных пар.
        if not pairs:
            time.sleep(30)
            pairs = get_liquid_pairs(min_volume_usd=5_000_000)[:max_pairs]
        if not pairs:
            from futures_data import get_all_futures_pairs
            pairs = get_all_futures_pairs()[:max_pairs]
            _last_scan["fallback"] = "all_futures_pairs"
    except Exception as e:
        _last_scan["error"] = f"pairs: {e}"
        return out
    _last_scan["pairs"] = len(pairs)
    if not pairs:
        _last_scan["error"] = "universe empty"
        return out
    checked = errors = 0
    first_err = None
    delta_map = []
    for sym in pairs:
        pair = sym.replace("USDT", "/USDT") if "/" not in sym else sym
        try:
            # один фьючерсный запрос на пару: и база, и текущая 24ч-дельта
            kd = _fetch_klines_delta(pair, 320)
            checked += 1
            if kd and len(kd) >= 260:
                dz24 = _delta_z(kd, len(kd) - 24)
                if dz24 is not None:
                    delta_map.append({"pair": pair,
                                      "symbol": pair.replace("/", "").upper(),
                                      "dz24": dz24})
                st = check_pair(pair, candles_1h=kd)
            else:
                st = check_pair(pair)
            if st:
                out.append(st)
        except Exception as e:
            errors += 1
            if first_err is None:
                first_err = f"{pair}: {e}"
    try:
        store_delta_map(delta_map)
        _last_scan["delta_map"] = len(delta_map)
    except Exception:
        pass
    # sample: сколько пар вообще дали свечи (диагностика get_klines_any)
    try:
        if pairs:
            from exchange import get_klines_any
            p0 = pairs[0].replace("USDT", "/USDT") if "/" not in pairs[0] else pairs[0]
            kl = get_klines_any(p0, "1h", 320)
            _last_scan["sample_klines"] = len(kl) if kl else 0
    except Exception as e:
        _last_scan["sample_klines"] = f"err: {e}"
    _last_scan.update(checked=checked, errors=errors, found=len(out),
                      first_err=first_err, finished=utcnow().isoformat())
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
                "delta_dz": d.get("delta_dz"),
            })
        if events:
            db.accum_events.insert_many(events)
            logger.info(f"[accum] разрешений: "
                        f"{[(e['pair'], e['resolution']) for e in events]}")
    except Exception:
        logger.exception("[accum] resolutions fail")
    return events


def store_delta_map(rows: list[dict]) -> None:
    """Текущая фьючерсная 24ч-дельта по всем сканируемым парам (для колонки
    Δ в скринере). Полная замена снапшота."""
    try:
        from database import _get_db, utcnow
        col = _get_db().delta_state
        col.delete_many({})
        now = utcnow()
        if rows:
            col.insert_many([{**r, "updated_at": now} for r in rows])
    except Exception:
        logger.exception("[accum] delta_map store fail")


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
