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
                window_h: int = TOP_PICK_WINDOW_H,
                for_source: str = "") -> dict:
    """Проверяет попадает ли сигнал в Top Pick.

    Логика:
      - Для cluster/tradium/cryptovizor: достаточно 1+ STRONG Confluence (score>=5)
        в том же направлении за window_h часов → Top Pick
      - Для confluence (score>=5): требуется хотя бы 1 другой сигнал (cluster/
        tradium triggered/CV pattern) на той же паре в том же направлении
        за window_h часов → Top Pick (симметрия)

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
    # Для Confluence окно двухстороннее (±48h) — confluence может появиться ДО
    # другого сигнала, и надо считать его top pick если сигнал пришёл позже.
    # Для cluster/tradium/CV — ищем Confluence в прошлом (или ±48h для симметрии).
    start = at - timedelta(hours=window_h)
    end = at + timedelta(hours=window_h)

    confirmations = []

    if for_source == "confluence":
        # Для Confluence подтверждающие = cluster/Tradium/CV в окне ±48h
        from database import _clusters
        # Cluster
        for cl in _clusters().find({
            "$or": [{"pair": norm}, {"symbol": norm_sym}],
            "direction": direction,
            "trigger_at": {"$gte": start, "$lte": end},
        }):
            confirmations.append({
                "source": "cluster",
                "at": cl.get("trigger_at").isoformat() if hasattr(cl.get("trigger_at"), "isoformat") else None,
                "direction": cl.get("direction"),
                "strength": cl.get("strength"),
                "signals_count": cl.get("signals_count"),
            })
        # Tradium triggered (окно ±48h)
        for s in _signals().find({
            "source": "tradium", "pattern_triggered": True,
            "pair": {"$in": [norm, norm_sym]},
            "direction": direction,
            "$or": [
                {"pattern_triggered_at": {"$gte": start, "$lte": end}},
                {"received_at": {"$gte": start, "$lte": end}},
            ],
        }):
            t = s.get("pattern_triggered_at") or s.get("received_at")
            confirmations.append({
                "source": "tradium",
                "at": t.isoformat() if hasattr(t, "isoformat") else None,
                "direction": s.get("direction"),
                "ai_score": s.get("ai_score"),
            })
        # CV triggered (окно ±48h)
        for s in _signals().find({
            "source": "cryptovizor", "pattern_triggered": True,
            "pair": {"$in": [norm, norm_sym]},
            "direction": direction,
            "pattern_triggered_at": {"$gte": start, "$lte": end},
        }):
            confirmations.append({
                "source": "cryptovizor",
                "at": s.get("pattern_triggered_at").isoformat() if hasattr(s.get("pattern_triggered_at"), "isoformat") else None,
                "direction": s.get("direction"),
                "pattern": s.get("pattern_name"),
                "ai_score": s.get("ai_score"),
            })
    else:
        # Для cluster/tradium/CV подтверждающие = STRONG Confluence
        confs = list(_confluence().find({
            "$or": [{"pair": norm}, {"symbol": norm_sym}],
            "direction": direction,
            "score": {"$gte": TOP_PICK_MIN_SCORE},
            "detected_at": {"$gte": start, "$lte": at},
        }))
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
    """Проверяет кластер и проставляет is_top_pick если подходит.
    При положительном — ретроспективно отмечает связанные Confluence."""
    c = _clusters().find_one({"id": cluster_id})
    if not c:
        return False
    pair = c.get("pair", "")
    direction = c.get("direction", "")
    at = c.get("trigger_at")
    res = is_top_pick(pair, direction, at, for_source="cluster")
    if res["is_top_pick"]:
        _clusters().update_one({"_id": c["_id"]}, {"$set": {
            "is_top_pick": True,
            "top_pick_tagged_at": utcnow(),
            "top_pick_confirmations": res["confirmations"],
            "top_pick_confirmations_count": res["confirmations_count"],
        }})
        _propagate_to_related_confluence(pair, direction, at)
        return True
    return False


def tag_signal(signal_db_id: int, source: str) -> bool:
    """Для Tradium/CV сигналов (_signals collection) — проверка и флаг.
    При положительном результате также ретроспективно помечает
    связанные Confluence STRONG на той же паре+направлении (propagation).
    """
    s = _signals().find_one({"id": signal_db_id, "source": source})
    if not s:
        return False
    at = s.get("pattern_triggered_at") or s.get("received_at")
    pair = s.get("pair", "")
    direction = s.get("direction", "")
    res = is_top_pick(pair, direction, at, for_source=source)
    if res["is_top_pick"]:
        _signals().update_one({"_id": s["_id"]}, {"$set": {
            "is_top_pick": True,
            "top_pick_tagged_at": utcnow(),
            "top_pick_confirmations": res["confirmations"],
            "top_pick_confirmations_count": res["confirmations_count"],
        }})
        # Retroactive: помечаем связанные Confluence
        _propagate_to_related_confluence(pair, direction, at)
        return True
    return False


def _propagate_to_related_confluence(pair: str, direction: str,
                                       around_at: Optional[datetime],
                                       window_h: int = TOP_PICK_WINDOW_H) -> int:
    """Ретроспективно помечает Confluence STRONG рядом как Top Pick.
    Вызывается когда другой сигнал стал top pick → подтверждающий
    Confluence тоже должен стать top pick (симметрия)."""
    if around_at is None:
        return 0
    norm = _norm_pair(pair)
    norm_sym = norm.replace("/", "")
    start = around_at - timedelta(hours=window_h)
    end = around_at + timedelta(hours=window_h)
    updated = 0
    for c in _confluence().find({
        "$or": [{"pair": norm}, {"symbol": norm_sym}],
        "direction": direction,
        "score": {"$gte": TOP_PICK_MIN_SCORE},
        "detected_at": {"$gte": start, "$lte": end},
        "is_top_pick": {"$ne": True},  # только те что ещё не помечены
    }):
        c_at = c.get("detected_at")
        res = is_top_pick(pair, direction, c_at, for_source="confluence")
        if res["is_top_pick"]:
            _confluence().update_one({"_id": c["_id"]}, {"$set": {
                "is_top_pick": True,
                "top_pick_tagged_at": utcnow(),
                "top_pick_confirmations": res["confirmations"],
                "top_pick_confirmations_count": res["confirmations_count"],
            }})
            updated += 1
    return updated


def tag_confluence(conf_id) -> bool:
    """Для Confluence STRONG — проверка и флаг (подтверждение от cluster/Tradium/CV)."""
    c = _confluence().find_one({"_id": conf_id})
    if not c or (c.get("score") or 0) < TOP_PICK_MIN_SCORE:
        return False
    at = c.get("detected_at")
    res = is_top_pick(c.get("pair") or c.get("symbol", ""), c.get("direction", ""), at, for_source="confluence")
    if res["is_top_pick"]:
        _confluence().update_one({"_id": c["_id"]}, {"$set": {
            "is_top_pick": True,
            "top_pick_tagged_at": utcnow(),
            "top_pick_confirmations": res["confirmations"],
            "top_pick_confirmations_count": res["confirmations_count"],
        }})
        return True
    return False


def backfill_top_picks(days: int = 30) -> dict:
    """Пробегает по истории, проставляет is_top_pick на всех подходящих сигналах.

    Returns: {clusters_tagged, tradium_tagged, cv_tagged, confluence_tagged, total}
    """
    since = utcnow() - timedelta(days=days)
    stats = {"clusters_tagged": 0, "tradium_tagged": 0, "cv_tagged": 0, "confluence_tagged": 0}

    # Clusters
    for c in _clusters().find({"trigger_at": {"$gte": since}}):
        if c.get("is_top_pick") is not None:
            continue
        res = is_top_pick(c.get("pair", ""), c.get("direction", ""), c.get("trigger_at"), for_source="cluster")
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
        res = is_top_pick(s.get("pair", ""), s.get("direction", ""), at, for_source="tradium")
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
        res = is_top_pick(s.get("pair", ""), s.get("direction", ""), at, for_source="cryptovizor")
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

    # Confluence STRONG (score>=5) — подтверждение от cluster/Tradium/CV
    for c in _confluence().find({
        "score": {"$gte": TOP_PICK_MIN_SCORE},
        "detected_at": {"$gte": since},
    }):
        if c.get("is_top_pick") is not None:
            continue
        at = c.get("detected_at")
        pair = c.get("pair") or c.get("symbol", "")
        res = is_top_pick(pair, c.get("direction", ""), at, for_source="confluence")
        update_doc = {"top_pick_checked_at": utcnow()}
        if res["is_top_pick"]:
            update_doc.update({
                "is_top_pick": True,
                "top_pick_tagged_at": utcnow(),
                "top_pick_confirmations": res["confirmations"],
                "top_pick_confirmations_count": res["confirmations_count"],
            })
            stats["confluence_tagged"] += 1
        else:
            update_doc["is_top_pick"] = False
        _confluence().update_one({"_id": c["_id"]}, {"$set": update_doc})

    stats["total"] = sum(v for k, v in stats.items() if k.endswith("_tagged"))
    return stats


def get_all_top_picks(hours: int = 168, limit: int = 200) -> list[dict]:
    """Возвращает список Top Picks (cluster + tradium + cv) за N часов.
    Сортирует по времени (новые сверху). Unified формат для UI."""
    since = utcnow() - timedelta(hours=hours)
    out = []

    # Projection — только нужные для UI. top_pick_confirmations не включаем
    # в общем списке (грузится в модалке при клике, если понадобится).
    CL_FIELDS = {"pair":1,"symbol":1,"direction":1,"trigger_price":1,"tp_price":1,
                 "sl_price":1,"trigger_at":1,"status":1,"pnl_percent":1,"strength":1,
                 "sources_count":1,"signals_count":1,"top_pick_confirmations_count":1,"id":1}
    SIG_FIELDS = {"pair":1,"direction":1,"entry":1,"pattern_price":1,"tp1":1,"sl":1,
                  "dca1":1,"dca2":1,"pattern_triggered_at":1,"received_at":1,"status":1,
                  "pattern_name":1,"source":1,"ai_score":1,
                  "top_pick_confirmations_count":1,"id":1}
    CF_FIELDS = {"pair":1,"symbol":1,"direction":1,"price":1,"r1":1,"s1":1,"score":1,
                 "pattern":1,"detected_at":1,"top_pick_confirmations_count":1}

    # Clusters
    for c in _clusters().find({"is_top_pick": True, "trigger_at": {"$gte": since}}, CL_FIELDS).sort("trigger_at", -1).limit(limit):
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
    }, SIG_FIELDS).sort("pattern_triggered_at", -1).limit(limit):
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
            "signal_id": s.get("id"),
            "ai_score": s.get("ai_score"),
        })

    # CV triggered
    for s in _signals().find({
        "is_top_pick": True, "source": "cryptovizor",
        "pattern_triggered": True,
        "pattern_triggered_at": {"$gte": since},
    }, SIG_FIELDS).sort("pattern_triggered_at", -1).limit(limit):
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
            "signal_id": s.get("id"),
            "ai_score": s.get("ai_score"),
        })

    # Confluence STRONG с подтверждением
    for c in _confluence().find({
        "is_top_pick": True,
        "detected_at": {"$gte": since},
    }, CF_FIELDS).sort("detected_at", -1).limit(limit):
        at = c.get("detected_at")
        pair = c.get("pair") or (c.get("symbol") or "").replace("USDT", "/USDT")
        out.append({
            "type": "confluence",
            "pair": pair,
            "symbol": (pair or "").replace("/", ""),
            "direction": c.get("direction", ""),
            "entry": c.get("price"),
            "tp": c.get("r1"),
            "sl": c.get("s1"),
            "at": at.isoformat() if hasattr(at, "isoformat") else str(at),
            "at_ts": int(at.timestamp()) if hasattr(at, "timestamp") else 0,
            "status": "ACTIVE",  # confluence не имеет lifecycle
            "pnl_pct": None,
            "score": c.get("score"),
            "pattern": c.get("pattern"),
            "confirmations_count": c.get("top_pick_confirmations_count", 0),
            "conf_id": str(c.get("_id")),
        })

    out.sort(key=lambda x: -(x.get("at_ts") or 0))
    return out[:limit]


def get_top_picks_stats(hours: int = 720) -> dict:
    """Статистика по Top Picks (count-based, без полной выборки документов)."""
    since = utcnow() - timedelta(hours=hours)

    # Количества через count_documents — быстро
    n_clusters = _clusters().count_documents({"is_top_pick": True, "trigger_at": {"$gte": since}})
    n_tradium = _signals().count_documents({
        "is_top_pick": True, "source": "tradium", "pattern_triggered": True,
        "pattern_triggered_at": {"$gte": since},
    })
    n_cv = _signals().count_documents({
        "is_top_pick": True, "source": "cryptovizor", "pattern_triggered": True,
        "pattern_triggered_at": {"$gte": since},
    })
    n_conf = _confluence().count_documents({"is_top_pick": True, "detected_at": {"$gte": since}})

    # Cluster outcomes — нужны PnL и статусы, но только по cluster TP/SL
    tp = _clusters().count_documents({"is_top_pick": True, "status": "TP", "trigger_at": {"$gte": since}})
    sl = _clusters().count_documents({"is_top_pick": True, "status": "SL", "trigger_at": {"$gte": since}})
    open_count = _clusters().count_documents({"is_top_pick": True, "status": "OPEN", "trigger_at": {"$gte": since}})

    # Sum/Avg PnL через aggregation (только cluster closed)
    try:
        pipeline = [
            {"$match": {"is_top_pick": True, "status": {"$in": ["TP", "SL"]},
                        "trigger_at": {"$gte": since}}},
            {"$group": {"_id": None, "sum": {"$sum": "$pnl_percent"}, "cnt": {"$sum": 1}}},
        ]
        agg = list(_clusters().aggregate(pipeline))
        if agg:
            sum_pnl = agg[0].get("sum") or 0
            cnt = agg[0].get("cnt") or 0
            avg_pnl = sum_pnl / cnt if cnt else 0
        else:
            sum_pnl, avg_pnl = 0, 0
    except Exception:
        sum_pnl, avg_pnl = 0, 0

    wr = tp / (tp + sl) * 100 if (tp + sl) > 0 else None
    return {
        "total": n_clusters + n_tradium + n_cv + n_conf,
        "active": open_count,
        "tp": tp,
        "sl": sl,
        "wr": round(wr, 1) if wr is not None else None,
        "sum_pnl": round(sum_pnl, 2),
        "avg_pnl": round(avg_pnl, 3),
        "by_type": {
            "cluster": n_clusters, "tradium": n_tradium,
            "cryptovizor": n_cv, "confluence": n_conf,
        },
    }
