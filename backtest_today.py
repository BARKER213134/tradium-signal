"""Backtest сегодняшних (или за N часов) сигналов по всем источникам.

Используется endpoint /api/backtest/today. Проходит по каждому сигналу,
проверяет какая цена была достигнута после entry (TP/SL/HOLD) по свечам
Binance 1H. Группирует по категориям: CV/Cluster/Confluence + подгруппы
(ai≥50, ai≥70, STRONG+, top_pick), финальная группа ALL TOP PICKS vs
NON-TOP для сравнения.
"""
from __future__ import annotations
import logging
from datetime import datetime, timedelta
from typing import Optional

from database import _signals, _confluence, _clusters, utcnow
from exchange import get_klines_any

logger = logging.getLogger(__name__)


def _fetch_candles_since(symbol: str, entry_ts: int, limit: int = 48) -> list[dict]:
    """1h candles after entry_ts."""
    try:
        candles = get_klines_any(symbol, "1h", limit=100)
        out = []
        for c in candles or []:
            ts = c.get("t") or c.get("time")
            if not ts:
                continue
            if ts > 10**12:
                ts = ts // 1000
            if ts >= entry_ts:
                out.append({"t": ts, "h": c["h"], "l": c["l"], "c": c["c"]})
        return out[:limit]
    except Exception as e:
        logger.debug(f"fetch {symbol}: {e}")
        return []


def _simulate(pair: str, direction: str, entry_price: float, entry_at: datetime,
               tp_pct: float = 3.0, sl_pct: float = 2.0, hold_h: int = 48) -> dict:
    """Простая симуляция: когда hit TP/SL или HOLD до последней свечи."""
    if not pair or not direction or not entry_price or not entry_at:
        return {"result": "NO_DATA", "pnl_pct": None, "bars": 0}
    sym = pair.replace("/", "").upper()
    if not sym.endswith("USDT"):
        sym = sym + "USDT"
    entry_ts = int(entry_at.timestamp()) if hasattr(entry_at, "timestamp") else 0
    candles = _fetch_candles_since(sym, entry_ts, hold_h)
    if not candles:
        return {"result": "NO_DATA", "pnl_pct": None, "bars": 0}

    is_long = direction.upper() in ("LONG", "BUY", "BULLISH")
    if is_long:
        tp_price = entry_price * (1 + tp_pct / 100)
        sl_price = entry_price * (1 - sl_pct / 100)
    else:
        tp_price = entry_price * (1 - tp_pct / 100)
        sl_price = entry_price * (1 + sl_pct / 100)

    for i, c in enumerate(candles):
        h, l = c["h"], c["l"]
        if is_long:
            # Консервативно: если и TP и SL в одной свече — SL первее
            if l <= sl_price:
                return {"result": "SL", "pnl_pct": -sl_pct, "bars": i + 1}
            if h >= tp_price:
                return {"result": "TP", "pnl_pct": tp_pct, "bars": i + 1}
        else:
            if h >= sl_price:
                return {"result": "SL", "pnl_pct": -sl_pct, "bars": i + 1}
            if l <= tp_price:
                return {"result": "TP", "pnl_pct": tp_pct, "bars": i + 1}

    # HOLD — считаем PnL по последней цене
    last = candles[-1]["c"]
    pnl = (last - entry_price) / entry_price * 100
    if not is_long:
        pnl = -pnl
    return {"result": "HOLD", "pnl_pct": round(pnl, 2), "bars": len(candles), "current": last}


def _group_stats(label: str, signals: list[dict], tp_pct: float, sl_pct: float, hold_h: int) -> dict:
    """Запуск бэктеста на группе сигналов + агрегация."""
    tp = sl = hold = nd = 0
    pnls = []
    items = []
    for s in signals:
        r = _simulate(s["pair"], s["direction"], s["entry"], s["at"], tp_pct, sl_pct, hold_h)
        if r["result"] == "TP":
            tp += 1
        elif r["result"] == "SL":
            sl += 1
        elif r["result"] == "HOLD":
            hold += 1
        else:
            nd += 1
        if r["pnl_pct"] is not None:
            pnls.append(r["pnl_pct"])
        items.append({
            "pair": s["pair"],
            "direction": s["direction"],
            "entry": s["entry"],
            "at": s["at"].isoformat() if hasattr(s["at"], "isoformat") else str(s["at"]),
            "source": s.get("source", ""),
            "result": r["result"],
            "pnl_pct": r.get("pnl_pct"),
            "bars": r.get("bars", 0),
        })
    closed = tp + sl
    wr = round(tp / closed * 100, 1) if closed else 0.0
    total_pnl = round(sum(pnls), 2)
    avg_pnl = round(total_pnl / len(pnls), 2) if pnls else 0.0
    return {
        "label": label,
        "total": len(signals),
        "tp": tp, "sl": sl, "hold": hold, "no_data": nd,
        "wr": wr,
        "sum_pnl": total_pnl,
        "avg_pnl": avg_pnl,
        "items": items,
    }


def run(hours: int = 24, tp_pct: float = 3.0, sl_pct: float = 2.0, hold_h: int = 48) -> dict:
    """Главная функция бэктеста.

    hours: окно (default 24 — с начала UTC суток, если >24 — просто период)
    tp_pct, sl_pct, hold_h: параметры сделок
    """
    now = utcnow()
    if hours == 24:
        # С начала UTC суток
        since = datetime(now.year, now.month, now.day)
    else:
        since = now - timedelta(hours=hours)

    # ── Собираем сигналы из трёх источников ──
    cv_sigs = []
    for s in _signals().find({
        "source": "cryptovizor", "pattern_triggered": True,
        "pattern_triggered_at": {"$gte": since},
    }, {"pair": 1, "direction": 1, "pattern_price": 1, "entry": 1,
        "pattern_triggered_at": 1, "ai_score": 1, "is_top_pick": 1, "_id": 0}):
        entry = s.get("pattern_price") or s.get("entry")
        if not entry:
            continue
        cv_sigs.append({
            "pair": s.get("pair", ""),
            "direction": s.get("direction", ""),
            "entry": float(entry),
            "at": s.get("pattern_triggered_at"),
            "source": "cryptovizor",
            "ai_score": s.get("ai_score") or 0,
            "is_top_pick": bool(s.get("is_top_pick")),
        })

    cl_sigs = []
    for c in _clusters().find({"trigger_at": {"$gte": since}},
        {"pair": 1, "direction": 1, "trigger_price": 1, "trigger_at": 1,
         "strength": 1, "is_top_pick": 1, "_id": 0}):
        entry = c.get("trigger_price")
        if not entry:
            continue
        cl_sigs.append({
            "pair": c.get("pair", ""),
            "direction": c.get("direction", ""),
            "entry": float(entry),
            "at": c.get("trigger_at"),
            "source": "cluster",
            "strength": c.get("strength", "NORMAL"),
            "is_top_pick": bool(c.get("is_top_pick")),
        })

    cf_sigs = []
    for c in _confluence().find({"detected_at": {"$gte": since}},
        {"pair": 1, "symbol": 1, "direction": 1, "price": 1, "detected_at": 1,
         "score": 1, "is_top_pick": 1, "_id": 0}):
        entry = c.get("price")
        if not entry:
            continue
        cf_sigs.append({
            "pair": c.get("pair") or c.get("symbol", ""),
            "direction": c.get("direction", ""),
            "entry": float(entry),
            "at": c.get("detected_at"),
            "source": "confluence",
            "score": c.get("score") or 0,
            "is_top_pick": bool(c.get("is_top_pick")),
        })

    # ── Группировка и прогон ──
    groups = [
        _group_stats("🚀 CV все", cv_sigs, tp_pct, sl_pct, hold_h),
        _group_stats("🚀 CV ai≥50", [s for s in cv_sigs if s.get("ai_score", 0) >= 50], tp_pct, sl_pct, hold_h),
        _group_stats("🚀 CV ai≥70", [s for s in cv_sigs if s.get("ai_score", 0) >= 70], tp_pct, sl_pct, hold_h),
        _group_stats("🚀 CV top_pick", [s for s in cv_sigs if s["is_top_pick"]], tp_pct, sl_pct, hold_h),
        _group_stats("💠 Cluster все", cl_sigs, tp_pct, sl_pct, hold_h),
        _group_stats("💠 Cluster STRONG+MEGA", [s for s in cl_sigs if s.get("strength") in ("STRONG", "MEGA")], tp_pct, sl_pct, hold_h),
        _group_stats("💠 Cluster top_pick", [s for s in cl_sigs if s["is_top_pick"]], tp_pct, sl_pct, hold_h),
        _group_stats("🎯 Confluence все", cf_sigs, tp_pct, sl_pct, hold_h),
        _group_stats("🎯 Confluence score≥5", [s for s in cf_sigs if (s.get("score") or 0) >= 5], tp_pct, sl_pct, hold_h),
        _group_stats("🎯 Confluence top_pick", [s for s in cf_sigs if s["is_top_pick"]], tp_pct, sl_pct, hold_h),
        _group_stats("👑 ALL TOP PICKS", [s for s in cv_sigs + cl_sigs + cf_sigs if s["is_top_pick"]], tp_pct, sl_pct, hold_h),
        _group_stats("· ALL NON-TOP", [s for s in cv_sigs + cl_sigs + cf_sigs if not s["is_top_pick"]], tp_pct, sl_pct, hold_h),
    ]

    return {
        "since": since.isoformat(),
        "until": now.isoformat(),
        "hours": hours,
        "params": {"tp_pct": tp_pct, "sl_pct": sl_pct, "hold_h": hold_h},
        "groups": groups,
        "totals": {
            "cv": len(cv_sigs),
            "cluster": len(cl_sigs),
            "confluence": len(cf_sigs),
            "total": len(cv_sigs) + len(cl_sigs) + len(cf_sigs),
        },
    }
