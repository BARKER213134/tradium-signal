"""Anti-cluster / Divergence Detector.

Ищет ПРОТИВОРЕЧИЯ между сигналами на одной паре в окне времени.
Когда LONG и SHORT сигналы от разных источников сосуществуют — это конфликт,
рынок не определился → блокируем формирование кластера.

Веса источников (реалтайм vs post-technical):
- anomaly    = 2.0  (видит происходящее прямо сейчас)
- confluence = 1.3  (композит ТА, средняя свежесть)
- tradium    = 1.0  (паттерн уже сформирован, может быть отложен)
- cryptovizor= 1.0  (то же)
- paper      = 0.5  (копия, не учитываем сильно)

Severity:
- nuclear — anomaly veto: последний сигнал в окне — от Anomaly против большинства
- strong  — ratio >= 0.5 (голоса поделены пополам)
- weak    — ratio >= 0.3 (одна сторона явно доминирует, но есть оппозиция)
- none    — ratio < 0.3 или одна сторона полностью пустая

Интеграция:
- cluster_detector.should_trigger_cluster() вызывает detect_conflict()
  перед формированием — блокирует если severity in (strong, nuclear).
- paper_trader.on_signal() аналогично — блокирует открытие позиции.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import Optional
import logging

from database import _conflicts, utcnow
from cluster_detector import collect_signals_for, _norm_pair

logger = logging.getLogger(__name__)


SOURCE_WEIGHTS = {
    "anomaly": 2.0,
    "confluence": 1.3,
    "tradium": 1.0,
    "cryptovizor": 1.0,
    "paper": 0.5,
}


def detect_conflict(pair: str, at: Optional[datetime] = None, window_h: int = 4) -> dict:
    """Ищет противоречия LONG vs SHORT сигналов на паре за окно.

    Возвращает dict:
      has_conflict: bool
      severity: 'none' | 'weak' | 'strong' | 'nuclear'
      long_weight, short_weight: float
      ratio: float (min/max)
      longs, shorts: list[dict] (источники)
      anomaly_veto: bool
      recommendation: str (что показать пользователю)
    """
    if at is None:
        at = utcnow()

    longs = collect_signals_for(pair, "LONG", at, window_h)
    shorts = collect_signals_for(pair, "SHORT", at, window_h)

    if not longs or not shorts:
        return {
            "has_conflict": False, "severity": "none",
            "long_weight": sum(SOURCE_WEIGHTS.get(s["source"], 1.0) for s in longs),
            "short_weight": sum(SOURCE_WEIGHTS.get(s["source"], 1.0) for s in shorts),
            "ratio": 0.0,
            "longs": longs, "shorts": shorts,
            "anomaly_veto": False,
            "recommendation": "✅ OK",
        }

    lw = sum(SOURCE_WEIGHTS.get(s["source"], 1.0) for s in longs)
    sw = sum(SOURCE_WEIGHTS.get(s["source"], 1.0) for s in shorts)
    ratio = min(lw, sw) / max(lw, sw) if max(lw, sw) > 0 else 0.0

    # Anomaly autoveto: последний сигнал противоречит большинству
    all_sigs = sorted(longs + shorts, key=lambda x: x["at"], reverse=True)
    last_sig = all_sigs[0] if all_sigs else None
    anomaly_veto = False
    if last_sig and last_sig["source"] == "anomaly":
        majority_long = lw > sw
        last_is_short = last_sig in shorts
        last_is_long = last_sig in longs
        if (majority_long and last_is_short) or ((not majority_long) and last_is_long):
            anomaly_veto = True

    if anomaly_veto:
        severity = "nuclear"
        rec = "🚨 STAY OUT — anomaly detected fresh reversal"
    elif ratio >= 0.5:
        severity = "strong"
        rec = "🚫 STAY OUT — sources equally divided"
    elif ratio >= 0.3:
        severity = "weak"
        rec = "⚠️ Reduce size — one source dissents"
    else:
        severity = "none"
        rec = "✅ OK"

    return {
        "has_conflict": severity != "none",
        "severity": severity,
        "long_weight": round(lw, 2),
        "short_weight": round(sw, 2),
        "ratio": round(ratio, 2),
        "longs": longs,
        "shorts": shorts,
        "anomaly_veto": anomaly_veto,
        "recommendation": rec,
    }


def log_conflict_block(pair: str, direction: str, conflict: dict, at: Optional[datetime] = None) -> dict:
    """Сохраняет заблокированный кластер в БД для истории/бектеста.

    direction — направление кластера который ХОТЕЛ сформироваться (LONG или SHORT)
    conflict — результат detect_conflict()
    """
    if at is None:
        at = utcnow()
    norm = _norm_pair(pair)

    # Дедуп: не создаём дубли за последний час на той же паре
    dedup_since = at - timedelta(hours=1)
    existing = _conflicts().find_one({
        "pair": norm,
        "blocked_direction": direction,
        "detected_at": {"$gte": dedup_since, "$lte": at},
    })
    if existing:
        return existing

    doc = {
        "pair": norm,
        "symbol": norm.replace("/", ""),
        "blocked_direction": direction,
        "severity": conflict["severity"],
        "long_weight": conflict["long_weight"],
        "short_weight": conflict["short_weight"],
        "ratio": conflict["ratio"],
        "anomaly_veto": conflict["anomaly_veto"],
        "longs": [
            {"source": s["source"], "at": s["at"], "price": s.get("price"), "meta": s.get("meta", {})}
            for s in conflict["longs"]
        ],
        "shorts": [
            {"source": s["source"], "at": s["at"], "price": s.get("price"), "meta": s.get("meta", {})}
            for s in conflict["shorts"]
        ],
        "recommendation": conflict["recommendation"],
        "detected_at": at,
        "status": "active",
        "resolved_at": None,
        "outcome": None,  # какое направление победило (через N часов)
    }
    _conflicts().insert_one(doc)
    logger.info(f"[anti-cluster] BLOCK {norm} {direction} severity={conflict['severity']} "
                f"L={conflict['long_weight']} S={conflict['short_weight']}")
    return doc


def get_active_conflicts(hours: int = 4, limit: int = 50) -> list[dict]:
    """Все активные конфликты за окно (для UI вкладки Conflicts)."""
    since = utcnow() - timedelta(hours=hours)
    docs = list(_conflicts().find({
        "detected_at": {"$gte": since},
    }).sort("detected_at", -1).limit(limit))
    out = []
    for d in docs:
        d.pop("_id", None)
        for k in ("detected_at", "resolved_at"):
            v = d.get(k)
            if hasattr(v, "isoformat"):
                d[k] = v.isoformat()
        # ISO для sub-dates
        for key in ("longs", "shorts"):
            for item in d.get(key, []):
                at = item.get("at")
                if hasattr(at, "isoformat"):
                    item["at"] = at.isoformat()
        out.append(d)
    return out


def get_conflict_stats(hours: int = 168) -> dict:
    """Статистика за окно: сколько конфликтов обнаружено, по severity."""
    since = utcnow() - timedelta(hours=hours)
    total = _conflicts().count_documents({"detected_at": {"$gte": since}})
    by_sev = {}
    for sev in ("weak", "strong", "nuclear"):
        by_sev[sev] = _conflicts().count_documents({
            "detected_at": {"$gte": since}, "severity": sev,
        })
    # outcome статистика (если resolved)
    correct = _conflicts().count_documents({
        "detected_at": {"$gte": since}, "status": "resolved",
        "outcome_matched_block": True,
    })
    wrong = _conflicts().count_documents({
        "detected_at": {"$gte": since}, "status": "resolved",
        "outcome_matched_block": False,
    })
    return {
        "total": total,
        "by_severity": by_sev,
        "resolved_correct": correct,  # заблокировали правильно (цена пошла против исходного направления)
        "resolved_wrong": wrong,      # заблокировали зря (цена всё-таки пошла в исходную сторону)
        "save_rate": round(correct / (correct + wrong) * 100, 1) if (correct + wrong) > 0 else None,
    }
