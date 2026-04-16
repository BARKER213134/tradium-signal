"""FVG Top Picks — Confluence Score + is_top_pick для Forex FVG сигналов.

Аналог top_picks.py для крипты, но адаптирован под один источник (TV webhook).
Вместо «подтверждения от 2+ источников» используем взвешенный скор из 5 критериев.

Score breakdown:
  base FVG (любой)            +1
  Multi-TF (1H + 4H agree)    +2
  Smart Level proximity       +2
  Multi-Pair correlation      +1
  ATR size ≥ 1.5×             +1

STRONG (≥5) → is_top_pick = True

Применяется ретроактивно: когда новый FVG приходит и становится top pick,
пересчитываем score связанных сигналов (±12h), т.к. они могли получить
бонусы multi_tf/multi_pair из-за нового сигнала.
"""
from __future__ import annotations
import logging
from datetime import datetime, timedelta
from typing import Optional

from database import _fvg_signals, utcnow

logger = logging.getLogger(__name__)

TOP_PICK_MIN_SCORE = 5
MULTI_TF_WINDOW_H = 12      # окно поиска 1H ↔ 4H подтверждения
SMART_LEVEL_TOL_REL = 0.0015  # 0.15% — близость entry к уровню
MULTI_PAIR_WINDOW_H = 6     # окно поиска коррелированных пар
MULTI_PAIR_MIN = 2          # сколько других пар должны подтвердить
ATR_STRONG_MULT = 1.5       # size >= 1.5x ATR


# ── Correlation groups (normalized to USD direction) ─────────
# Когда пара bullish и она в USD_WEAK_BULLISH → USD слабеет
# Когда пара bullish и она в USD_WEAK_BEARISH (USDXXX) → USD укрепляется
USD_WEAK_BULLISH = {
    "EURUSD", "GBPUSD", "AUDUSD", "NZDUSD", "XAUUSD", "XAGUSD",
    # Кроссы без USD — для simplicity считаем нейтральными (не засчитываем)
}
USD_WEAK_BEARISH = {
    "USDJPY", "USDCHF", "USDCAD", "USDMXN", "USDNOK",
}


def _theme_direction(instrument: str, direction: str) -> Optional[str]:
    """Возвращает 'USD_WEAK' / 'USD_STRONG' или None (нейтрально).

    USD_WEAK  = доллар слабеет (anti-USD)
    USD_STRONG = доллар укрепляется
    """
    if direction not in ("bullish", "bearish"):
        return None
    if instrument in USD_WEAK_BULLISH:
        return "USD_WEAK" if direction == "bullish" else "USD_STRONG"
    if instrument in USD_WEAK_BEARISH:
        return "USD_STRONG" if direction == "bullish" else "USD_WEAK"
    return None


def _norm_tf(tf) -> str:
    """Нормализация TF: '60'/'1h'/'1H' → '1H'; '240'/'4h'/'4H' → '4H'."""
    s = str(tf or "").upper().strip()
    if s in ("60", "1H", "1"):
        return "1H"
    if s in ("240", "4H", "4"):
        return "4H"
    if s in ("D", "1D"):
        return "1D"
    return s


def _check_multi_tf(instrument: str, direction: str,
                     formed_at: datetime, current_tf: str,
                     window_h: int = MULTI_TF_WINDOW_H) -> bool:
    """Есть ли FVG на противоположном TF (1H↔4H) в том же направлении ±12h."""
    cur = _norm_tf(current_tf)
    if cur == "1H":
        target_tfs = ["4H", "240"]
    elif cur == "4H":
        target_tfs = ["1H", "60"]
    else:
        return False
    start = formed_at - timedelta(hours=window_h)
    end = formed_at + timedelta(hours=window_h)
    return _fvg_signals().find_one({
        "instrument": instrument,
        "direction": direction,
        "timeframe": {"$in": target_tfs},
        "formed_at": {"$gte": start, "$lte": end},
    }) is not None


def _round_numbers_near(price: float, tol_abs: float) -> bool:
    """Близко ли к психологическому круглому уровню."""
    if price <= 0:
        return False
    if price < 10:
        # Форекс мажоры / кроссы: 1.0500, 1.1000, 0.6500 etc.
        r50 = round(price * 200) / 200   # шаг 0.005
        r100 = round(price * 100) / 100  # шаг 0.01
        return abs(price - r50) <= tol_abs or abs(price - r100) <= tol_abs
    elif price < 1000:
        # JPY pairs (110-160), XAUUSD (1900-2400)
        r1 = round(price)
        r5 = round(price * 2) / 2
        return abs(price - r1) <= tol_abs or abs(price - r5) <= tol_abs
    else:
        # Indices: SPX500 5000, NAS100 20000, US30 40000
        r100 = round(price / 100) * 100
        r50 = round(price / 50) * 50
        return abs(price - r100) <= tol_abs or abs(price - r50) <= tol_abs


def _swing_levels_near(entry: float, candles: list[dict], tol_abs: float,
                        pivot_window: int = 5, lookback: int = 200) -> bool:
    """Entry близко к значимому swing high/low в последних lookback барах."""
    if not candles or entry <= 0:
        return False
    cs = candles[-lookback:] if len(candles) > lookback else candles
    if len(cs) < pivot_window * 2 + 1:
        return False
    for i in range(pivot_window, len(cs) - pivot_window):
        c = cs[i]
        h = c.get("h", c.get("high"))
        l = c.get("l", c.get("low"))
        if h is None or l is None:
            continue
        left = cs[i - pivot_window:i]
        right = cs[i + 1:i + 1 + pivot_window]
        try:
            is_high = all((cx.get("h", cx.get("high", 0)) or 0) < h for cx in left + right)
            is_low = all((cx.get("l", cx.get("low", 1e18)) or 1e18) > l for cx in left + right)
        except (TypeError, ValueError):
            continue
        if is_high and abs(entry - h) <= tol_abs:
            return True
        if is_low and abs(entry - l) <= tol_abs:
            return True
    return False


def _check_smart_level_proximity(instrument: str, entry_price: float,
                                   candles: Optional[list[dict]] = None) -> dict:
    """Proximity к важному уровню: round number, swing high/low, PDH/PDL."""
    if entry_price <= 0:
        return {"match": False}
    tol_abs = entry_price * SMART_LEVEL_TOL_REL
    # 1. Round number
    if _round_numbers_near(entry_price, tol_abs):
        return {"match": True, "kind": "round_number"}
    # 2. Swing high/low в истории свечей
    if candles and _swing_levels_near(entry_price, candles, tol_abs):
        return {"match": True, "kind": "swing"}
    # 3. PDH/PDL (previous day high/low) из свечей
    if candles and len(candles) >= 24:
        # Берём свечи последних 48h, разделяем на сутки
        now_ts = candles[-1].get("t", 0)
        day_ago = now_ts - 86400
        two_days_ago = now_ts - 172800
        prev_day = [c for c in candles if two_days_ago <= c.get("t", 0) < day_ago]
        if prev_day:
            pdh = max((c.get("h", 0) for c in prev_day), default=0)
            pdl = min((c.get("l", 1e18) for c in prev_day), default=1e18)
            if pdh and abs(entry_price - pdh) <= tol_abs:
                return {"match": True, "kind": "PDH"}
            if pdl < 1e18 and abs(entry_price - pdl) <= tol_abs:
                return {"match": True, "kind": "PDL"}
    return {"match": False}


def _check_multi_pair_correlation(instrument: str, direction: str,
                                    formed_at: datetime,
                                    window_h: int = MULTI_PAIR_WINDOW_H,
                                    min_pairs: int = MULTI_PAIR_MIN) -> int:
    """Сколько ДРУГИХ пар дали FVG в том же USD-theme direction за ±6h.
    Возвращает count (0 если theme undefined или мало совпадений)."""
    theme = _theme_direction(instrument, direction)
    if not theme:
        return 0
    start = formed_at - timedelta(hours=window_h)
    end = formed_at + timedelta(hours=window_h)
    count = 0
    seen_instruments = set()
    for s in _fvg_signals().find({
        "formed_at": {"$gte": start, "$lte": end},
        "instrument": {"$ne": instrument},
    }, {"instrument": 1, "direction": 1}):
        inst = s.get("instrument", "")
        if inst in seen_instruments:
            continue
        other_theme = _theme_direction(inst, s.get("direction", ""))
        if other_theme == theme:
            seen_instruments.add(inst)
            count += 1
    return count


def _check_atr_strength(fvg_size_rel: float, atr: Optional[float],
                         price: float) -> bool:
    """FVG size >= 1.5× ATR (оба в рел. единицах)."""
    if not atr or price <= 0 or fvg_size_rel <= 0:
        return False
    atr_rel = atr / price
    return fvg_size_rel >= ATR_STRONG_MULT * atr_rel


def compute_score(fvg_doc: dict, candles: Optional[list[dict]] = None,
                   atr: Optional[float] = None) -> dict:
    """Расчёт confluence score.

    Returns: {score, breakdown, is_top_pick, theme}
    """
    score = 1
    breakdown = [{"source": "base_fvg", "points": 1}]

    instrument = fvg_doc.get("instrument", "")
    direction = fvg_doc.get("direction", "")
    formed_at = fvg_doc.get("formed_at")
    entry_price = fvg_doc.get("entry_price", 0) or fvg_doc.get("formed_price", 0)
    fvg_size_rel = fvg_doc.get("fvg_size_rel", 0) or 0
    tf = fvg_doc.get("timeframe", "1H")
    price_ref = fvg_doc.get("formed_price", entry_price)

    # 1. Multi-TF (1H↔4H)
    if formed_at and _check_multi_tf(instrument, direction, formed_at, tf):
        score += 2
        breakdown.append({"source": "multi_tf", "points": 2})

    # 2. Smart Level proximity
    level = _check_smart_level_proximity(instrument, entry_price, candles)
    if level.get("match"):
        score += 2
        breakdown.append({
            "source": "smart_level",
            "points": 2,
            "kind": level.get("kind"),
        })

    # 3. Multi-pair correlation
    if formed_at:
        corr_count = _check_multi_pair_correlation(instrument, direction, formed_at)
        if corr_count >= MULTI_PAIR_MIN:
            score += 1
            breakdown.append({
                "source": "multi_pair_correlation",
                "points": 1,
                "correlated_pairs": corr_count,
                "theme": _theme_direction(instrument, direction),
            })

    # 4. ATR strength
    if _check_atr_strength(fvg_size_rel, atr, price_ref):
        score += 1
        breakdown.append({
            "source": "atr_strong",
            "points": 1,
            "size_rel": round(fvg_size_rel, 6),
        })

    return {
        "score": score,
        "breakdown": breakdown,
        "is_top_pick": score >= TOP_PICK_MIN_SCORE,
        "theme": _theme_direction(instrument, direction),
    }


def tag_fvg(fvg_id, candles: Optional[list[dict]] = None,
             atr: Optional[float] = None, retroact: bool = True) -> dict:
    """Считает score + обновляет документ. Если стал top pick — ретроактивно
    пересчитывает связанные ±12h на других инструментах/TF."""
    from bson import ObjectId
    if isinstance(fvg_id, str):
        fvg_id = ObjectId(fvg_id)
    doc = _fvg_signals().find_one({"_id": fvg_id})
    if not doc:
        return {"ok": False, "error": "not found"}
    result = compute_score(doc, candles=candles, atr=atr)
    _fvg_signals().update_one({"_id": fvg_id}, {"$set": {
        "confluence_score": result["score"],
        "score_breakdown": result["breakdown"],
        "is_top_pick": result["is_top_pick"],
        "theme": result.get("theme"),
        "top_pick_tagged_at": utcnow(),
    }})
    retag_count = 0
    if retroact:
        retag_count = retag_related(doc)
    return {"ok": True, "retagged": retag_count, **result}


def retag_related(trigger_doc: dict, window_h: int = MULTI_TF_WINDOW_H) -> int:
    """Пересчитать score для ближних активных сигналов (±12h).
    Без candles/atr — ATR/Smart Level не пересчитываются, только Multi-TF и Multi-Pair."""
    instrument = trigger_doc.get("instrument", "")
    direction = trigger_doc.get("direction", "")
    formed_at = trigger_doc.get("formed_at")
    if not formed_at:
        return 0
    start = formed_at - timedelta(hours=window_h)
    end = formed_at + timedelta(hours=window_h)
    updated = 0
    for s in _fvg_signals().find({
        "_id": {"$ne": trigger_doc["_id"]},
        "formed_at": {"$gte": start, "$lte": end},
        "status": {"$in": ["WAITING_RETEST", "ENTERED"]},
    }):
        old_score = s.get("confluence_score") or 0
        # Пересчитываем без candles (легковесно) — только multi_tf/multi_pair бонусы
        # изменятся от появления нового сигнала; ATR/SmartLevel не зависят от других FVG.
        # Сохраняем существующие atr/smart_level бонусы:
        old_breakdown = s.get("score_breakdown") or []
        preserved = [b for b in old_breakdown
                      if b.get("source") in ("smart_level", "atr_strong")]

        new_res = compute_score(s)
        # Вручную добавим сохранённые статические бонусы к новому пересчёту
        dyn_sources = {b["source"] for b in new_res["breakdown"]}
        for p in preserved:
            if p["source"] not in dyn_sources:
                new_res["breakdown"].append(p)
                new_res["score"] += p.get("points", 0)
        new_res["is_top_pick"] = new_res["score"] >= TOP_PICK_MIN_SCORE

        if new_res["score"] != old_score:
            _fvg_signals().update_one({"_id": s["_id"]}, {"$set": {
                "confluence_score": new_res["score"],
                "score_breakdown": new_res["breakdown"],
                "is_top_pick": new_res["is_top_pick"],
                "top_pick_tagged_at": utcnow(),
            }})
            updated += 1
    return updated


# ── API helpers ────────────────────────────────────────────
def get_top_picks(hours: int = 168, limit: int = 200,
                   min_score: int = TOP_PICK_MIN_SCORE) -> list[dict]:
    """Список FVG Top Picks за N часов."""
    since = utcnow() - timedelta(hours=hours)
    docs = list(_fvg_signals().find({
        "is_top_pick": True,
        "confluence_score": {"$gte": min_score},
        "formed_at": {"$gte": since},
    }).sort("formed_at", -1).limit(limit))
    out = []
    for d in docs:
        d.pop("_id", None)
        for k in ("formed_at", "expire_at", "entered_at", "closed_at",
                   "created_at", "updated_at", "top_pick_tagged_at"):
            v = d.get(k)
            if hasattr(v, "isoformat"):
                d[k] = v.isoformat()
        out.append(d)
    return out


def get_top_picks_stats(hours: int = 720) -> dict:
    """Статистика по FVG Top Picks за N часов."""
    since = utcnow() - timedelta(hours=hours)
    col = _fvg_signals()
    total = col.count_documents({"is_top_pick": True, "formed_at": {"$gte": since}})
    tp = col.count_documents({"is_top_pick": True, "status": "TP", "formed_at": {"$gte": since}})
    sl = col.count_documents({"is_top_pick": True, "status": "SL", "formed_at": {"$gte": since}})
    active = col.count_documents({
        "is_top_pick": True,
        "status": {"$in": ["WAITING_RETEST", "ENTERED"]},
        "formed_at": {"$gte": since},
    })
    wr = tp / (tp + sl) * 100 if (tp + sl) > 0 else None
    # Avg R
    try:
        pipeline = [
            {"$match": {"is_top_pick": True,
                         "status": {"$in": ["TP", "SL"]},
                         "formed_at": {"$gte": since}}},
            {"$group": {"_id": None,
                         "sum": {"$sum": "$outcome_R"},
                         "cnt": {"$sum": 1}}},
        ]
        agg = list(col.aggregate(pipeline))
        if agg:
            sum_R = agg[0].get("sum") or 0
            cnt = agg[0].get("cnt") or 0
            avg_R = sum_R / cnt if cnt else 0
        else:
            sum_R, avg_R = 0, 0
    except Exception:
        sum_R, avg_R = 0, 0
    return {
        "total": total, "active": active,
        "tp": tp, "sl": sl,
        "wr": round(wr, 1) if wr is not None else None,
        "sum_R": round(sum_R, 2),
        "avg_R": round(avg_R, 3),
    }


def rescore_all(hours: int = 168) -> dict:
    """Пересчёт confluence score для всех FVG за N часов.
    Используется для бэкфилла после изменения логики/добавления TF."""
    from fvg_scanner import get_cached_candles, _compute_atr as _scanner_atr  # type: ignore[attr-defined]
    since = utcnow() - timedelta(hours=hours)
    stats = {"total": 0, "tagged_top_pick": 0, "changed": 0}
    # Кешируем candles per instrument чтобы не дёргать БД 100 раз
    candles_cache: dict = {}
    atr_cache: dict = {}
    for s in _fvg_signals().find({"formed_at": {"$gte": since}}):
        stats["total"] += 1
        inst = s.get("instrument", "")
        if inst not in candles_cache:
            try:
                candles_cache[inst] = get_cached_candles(inst, "1h", max_age_min=4320) or []
            except Exception:
                candles_cache[inst] = []
        cs = candles_cache[inst]
        if inst not in atr_cache:
            atr_cache[inst] = _compute_atr_local(cs) if cs else None
        old_score = s.get("confluence_score") or 0
        new_res = compute_score(s, candles=cs, atr=atr_cache[inst])
        if new_res["score"] != old_score or new_res["is_top_pick"] != s.get("is_top_pick", False):
            _fvg_signals().update_one({"_id": s["_id"]}, {"$set": {
                "confluence_score": new_res["score"],
                "score_breakdown": new_res["breakdown"],
                "is_top_pick": new_res["is_top_pick"],
                "theme": new_res.get("theme"),
                "top_pick_tagged_at": utcnow(),
            }})
            stats["changed"] += 1
        if new_res["is_top_pick"]:
            stats["tagged_top_pick"] += 1
    return stats


def _compute_atr_local(candles: list[dict], period: int = 14) -> Optional[float]:
    """Локальная копия ATR (чтоб не импортировать из tv_webhook и избежать циклов)."""
    if not candles or len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        c = candles[i]
        p = candles[i - 1]
        h, l, pc = c.get("h", 0), c.get("l", 0), p.get("c", 0)
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period
