"""Top Picks — сигналы подтверждённые STRONG Confluence (score>=5) за 48ч.

По бектесту (48 кластеров): Top Picks дают WR 75% vs baseline 56%.

Поддерживаемые типы сигналов:
- Cluster (_clusters collection)
- Tradium triggered (_signals source=tradium, pattern_triggered=True)
- Cryptovizor triggered (_signals source=cryptovizor, pattern_triggered=True)

Флаг is_top_pick проставляется:
- Автоматически при создании (в cluster_detector.create_cluster и watcher)
- Batch backfill для истории (backfill_top_picks)
"""
from __future__ import annotations
import logging
from datetime import datetime, timedelta
from typing import Optional

from database import _clusters, _signals, _confluence, utcnow
from cluster_detector import _norm_pair

logger = logging.getLogger(__name__)

TOP_PICK_WINDOW_H = 48
TOP_PICK_MIN_SCORE = 5  # STRONG Confluence


def is_top_pick(pair: str, direction: str, at: Optional[datetime] = None,
                window_h: int = TOP_PICK_WINDOW_H) -> dict:
    """Проверяет: есть ли STRONG Confluence (score>=5) в том же направлении
    за последние window_h часов до at.

    Returns:
      {
        is_top_pick: bool,
        confirmations: [{source, score, at, direction, ...}],
        confirmations_count: int,
      }
    """
    if at is None:
        at = utcnow()
    norm = _norm_pair(pair)
    norm_sym = norm.replace("/", "")
    start = at - timedelta(hours=window_h)

    confs = list(_confluence().find({
        "$or": [{"pair": norm}, {"symbol": norm_sym}],
        "direction": direction,
        "score": {"$gte": TOP_PICK_MIN_SCORE},
        "detected_at": {"$gte": start, "$lte": at},
    }))

    confirmations = []
    for c in confs:
        confirmations.append({
            "source": "confluence",
            "score": c.get("score"),
            "at": c.get("detected_at").isoformat() if hasattr(c.get("detected_at"), "isoformat") else None,
            "direction": c.get("direction"),
            "pattern": c.get("pattern"),
            "strength": c.get("strength"),
        })

    return {
        "is_top_pick": len(confirmations) > 0,
        "confirmations": confirmations,
        "confirmations_count": len(confirmations),
    }


def tag_cluster(cluster_id: int) -> bool:
    """Проверяет кластер и проставляет is_top_pick если подходит."""
    c = _clusters().find_one({"id": cluster_id})
    if not c:
        return False
    res = is_top_pick(c.get("pair", ""), c.get("direction", ""), c.get("trigger_at"))
    if res["is_top_pick"]:
        _clusters().update_one({"_id": c["_id"]}, {"$set": {
            "is_top_pick": True,
            "top_pick_tagged_at": utcnow(),
            "top_pick_confirmations": res["confirmations"],
            "top_pick_confirmations_count": res["confirmations_count"],
        }})
        return True
    return False


def tag_signal(signal_db_id: int, source: str) -> bool:
    """Для Tradium/CV сигналов (_signals collection) — проверка и флаг."""
    s = _signals().find_one({"id": signal_db_id, "source": source})
    if not s:
        return False
    # Для этих сигналов time of interest = pattern_triggered_at (когда стал активен)
    at = s.get("pattern_triggered_at") or s.get("received_at")
    res = is_top_pick(s.get("pair", ""), s.get("direction", ""), at)
    if res["is_top_pick"]:
        _signals().update_one({"_id": s["_id"]}, {"$set": {
            "is_top_pick": True,
            "top_pick_tagged_at": utcnow(),
            "top_pick_confirmations": res["confirmations"],
            "top_pick_confirmations_count": res["confirmations_count"],
        }})
        return True
    return False


def backfill_top_picks(days: int = 30) -> dict:
    """Пробегает по истории, проставляет is_top_pick на всех подходящих сигналах.

    Returns: {clusters_tagged, tradium_tagged, cv_tagged, total}
    """
    since = utcnow() - timedelta(days=days)
    stats = {"clusters_tagged": 0, "tradium_tagged": 0, "cv_tagged": 0}

    # Clusters
    for c in _clusters().find({"trigger_at": {"$gte": since}}):
        # Уже обработан?
        if c.get("is_top_pick") is not None:  # уже есть флаг
            continue
        res = is_top_pick(c.get("pair", ""), c.get("direction", ""), c.get("trigger_at"))
        update_doc = {"top_pick_checked_at": utcnow()}
        if res["is_top_pick"]:
            update_doc.update({
                "is_top_pick": True,
                "top_pick_tagged_at": utcnow(),
                "top_pick_confirmations": res["confirmations"],
                "top_pick_confirmations_count": res["confirmations_count"],
            })
            stats["clusters_tagged"] += 1
        else:
            update_doc["is_top_pick"] = False
        _clusters().update_one({"_id": c["_id"]}, {"$set": update_doc})

    # Tradium
    for s in _signals().find({
        "source": "tradium",
        "pattern_triggered": True,
        "received_at": {"$gte": since},
    }):
        if s.get("is_top_pick") is not None:
            continue
        at = s.get("pattern_triggered_at") or s.get("received_at")
        res = is_top_pick(s.get("pair", ""), s.get("direction", ""), at)
        update_doc = {"top_pick_checked_at": utcnow()}
        if res["is_top_pick"]:
            update_doc.update({
                "is_top_pick": True,
                "top_pick_tagged_at": utcnow(),
                "top_pick_confirmations": res["confirmations"],
                "top_pick_confirmations_count": res["confirmations_count"],
            })
            stats["tradium_tagged"] += 1
        else:
            update_doc["is_top_pick"] = False
        _signals().update_one({"_id": s["_id"]}, {"$set": update_doc})

    # Cryptovizor
    for s in _signals().find({
        "source": "cryptovizor",
        "pattern_triggered": True,
        "received_at": {"$gte": since},
    }):
        if s.get("is_top_pick") is not None:
            continue
        at = s.get("pattern_triggered_at") or s.get("received_at")
        res = is_top_pick(s.get("pair", ""), s.get("direction", ""), at)
        update_doc = {"top_pick_checked_at": utcnow()}
        if res["is_top_pick"]:
            update_doc.update({
                "is_top_pick": True,
                "top_pick_tagged_at": utcnow(),
                "top_pick_confirmations": res["confirmations"],
                "top_pick_confirmations_count": res["confirmations_count"],
            })
            stats["cv_tagged"] += 1
        else:
            update_doc["is_top_pick"] = False
        _signals().update_one({"_id": s["_id"]}, {"$set": update_doc})

    stats["total"] = stats["clusters_tagged"] + stats["tradium_tagged"] + stats["cv_tagged"]
    return stats


def get_all_top_picks(hours: int = 168, limit: int = 200) -> list[dict]:
    """Возвращает список Top Picks (cluster + tradium + cv) за N часов.
    Сортирует по времени (новые сверху). Unified формат для UI."""
    since = utcnow() - timedelta(hours=hours)
    out = []

    # Clusters
    for c in _clusters().find({"is_top_pick": True, "trigger_at": {"$gte": since}}):
        at = c.get("trigger_at")
        out.append({
            "type": "cluster",
            "pair": c.get("pair", ""),
            "symbol": (c.get("pair") or "").replace("/", ""),
            "direction": c.get("direction", ""),
            "entry": c.get("trigger_price"),
            "tp": c.get("tp_price"),
            "sl": c.get("sl_price"),
            "at": at.isoformat() if hasattr(at, "isoformat") else str(at),
            "at_ts": int(at.timestamp()) if hasattr(at, "timestamp") else 0,
            "status": c.get("status", "OPEN"),
            "pnl_pct": c.get("pnl_percent"),
            "strength": c.get("strength"),
            "sources_count": c.get("sources_count"),
            "signals_count": c.get("signals_count"),
            "confirmations_count": c.get("top_pick_confirmations_count", 0),
            "confirmations": c.get("top_pick_confirmations", []),
            "cluster_id": c.get("id"),
        })

    # Tradium triggered
    for s in _signals().find({
        "is_top_pick": True, "source": "tradium",
        "pattern_triggered": True,
        "pattern_triggered_at": {"$gte": since},
    }):
        at = s.get("pattern_triggered_at") or s.get("received_at")
        out.append({
            "type": "tradium",
            "pair": s.get("pair", ""),
            "symbol": (s.get("pair") or "").replace("/", ""),
            "direction": s.get("direction", ""),
            "entry": s.get("entry"),
            "tp": s.get("tp1"),
            "sl": s.get("sl"),
            "at": at.isoformat() if hasattr(at, "isoformat") else str(at),
            "at_ts": int(at.timestamp()) if hasattr(at, "timestamp") else 0,
            "status": s.get("status", "СЛЕЖУ"),
            "pnl_pct": None,
            "confirmations_count": s.get("top_pick_confirmations_count", 0),
            "confirmations": s.get("top_pick_confirmations", []),
            "signal_id": s.get("id"),
            "ai_score": s.get("ai_score"),
        })

    # CV triggered
    for s in _signals().find({
        "is_top_pick": True, "source": "cryptovizor",
        "pattern_triggered": True,
        "pattern_triggered_at": {"$gte": since},
    }):
        at = s.get("pattern_triggered_at") or s.get("received_at")
        out.append({
            "type": "cryptovizor",
            "pair": s.get("pair", ""),
            "symbol": (s.get("pair") or "").replace("/", ""),
            "direction": s.get("direction", ""),
            "entry": s.get("pattern_price") or s.get("entry"),
            "tp": s.get("dca2"),
            "sl": s.get("dca1"),
            "at": at.isoformat() if hasattr(at, "isoformat") else str(at),
            "at_ts": int(at.timestamp()) if hasattr(at, "timestamp") else 0,
            "status": s.get("status", "СЛЕЖУ"),
            "pnl_pct": None,
            "pattern_name": s.get("pattern_name"),
            "confirmations_count": s.get("top_pick_confirmations_count", 0),
            "confirmations": s.get("top_pick_confirmations", []),
            "signal_id": s.get("id"),
            "ai_score": s.get("ai_score"),
        })

    out.sort(key=lambda x: -(x.get("at_ts") or 0))
    return out[:limit]


def get_top_picks_stats(hours: int = 720) -> dict:
    """Статистика по Top Picks за период (по умолчанию 30 дней)."""
    items = get_all_top_picks(hours=hours, limit=1000)
    total = len(items)
    # Только кластеры имеют outcome пока
    closed = [i for i in items if i.get("type") == "cluster" and i.get("status") in ("TP", "SL")]
    tp = sum(1 for i in closed if i.get("status") == "TP")
    sl = sum(1 for i in closed if i.get("status") == "SL")
    open_count = sum(1 for i in items if i.get("status") in ("OPEN", "СЛЕЖУ"))
    pnls = [i.get("pnl_pct") or 0 for i in closed]
    sum_pnl = sum(pnls)
    avg_pnl = sum_pnl / len(pnls) if pnls else 0
    wr = tp / (tp + sl) * 100 if (tp + sl) > 0 else None
    by_type = {}
    for i in items:
        t = i.get("type", "?")
        by_type[t] = by_type.get(t, 0) + 1
    return {
        "total": total,
        "active": open_count,
        "tp": tp,
        "sl": sl,
        "wr": round(wr, 1) if wr is not None else None,
        "sum_pnl": round(sum_pnl, 2),
        "avg_pnl": round(avg_pnl, 3),
        "by_type": by_type,
    }
