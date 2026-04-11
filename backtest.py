"""Бектест Cryptovizor сигналов.

Для каждого сигнала в статусе ПАТТЕРН/VOLUME:
- Берёт цену на момент сигнала (entry)
- Берёт текущую цену (или цену через N часов)
- Считает PnL%
- Агрегирует статистику по паттернам, направлениям, парам, времени

Результат — JSON summary который Claude использует как контекст
для оценки новых сигналов.
"""
import logging
from datetime import datetime, timezone

from database import _signals, utcnow
from exchange import get_futures_prices_only, get_prices

logger = logging.getLogger(__name__)


def run_backtest(source: str = "cryptovizor") -> dict:
    """Прогоняет бектест по всем завершённым сигналам.

    Returns:
        {
            "total": 229,
            "with_result": 180,
            "overall_win_rate": 62.3,
            "overall_avg_pnl": 1.45,
            "by_pattern": {"Hammer": {"count": 25, "win_rate": 72, "avg_pnl": 2.1}, ...},
            "by_direction": {"LONG": {...}, "SHORT": {...}},
            "by_hour": {9: {"count": 15, "win_rate": 60}, ...},
            "top_patterns": ["Hammer", "Bullish Engulfing", ...],
            "worst_patterns": ["Shooting Star", ...],
            "signals": [{"pair": "X", "pnl": 2.3, "pattern": "Y", ...}, ...]
        }
    """
    signals = list(_signals().find({
        "source": source,
        "status": {"$in": ["ПАТТЕРН", "VOLUME"]},
        "entry": {"$ne": None},
        "pair": {"$ne": None},
    }))

    if not signals:
        return {"total": 0, "error": "no signals"}

    # Пробуем получить текущие цены (spot + futures)
    # Если API недоступен — используем pattern_price из БД
    pairs = list({s.get("pair") for s in signals if s.get("pair")})
    all_prices = {}
    try:
        prices_spot = get_prices(pairs)
        all_prices.update(prices_spot)
    except Exception:
        pass

    # Futures только если spot не покрыл всё (и только с таймаутом 3с)
    missing_pairs = [p for p in pairs if p.replace("/", "").upper() not in all_prices]
    if missing_pairs:
        try:
            prices_fut = get_futures_prices_only(missing_pairs[:20])  # лимит чтобы не зависнуть
            all_prices.update(prices_fut)
        except Exception:
            pass

    results = []
    for s in signals:
        pair = s.get("pair", "")
        norm = pair.replace("/", "").upper()
        current = all_prices.get(norm)
        entry = s.get("entry")
        direction = s.get("direction", "LONG")
        pattern = s.get("pattern_name", "unknown")

        # Fallback 1: pattern_price (цена на момент срабатывания паттерна)
        if current is None:
            current = s.get("pattern_price")
        # Fallback 2: entry (цена при сигнале — PnL будет 0%, но сигнал не пропадёт)
        if current is None:
            current = entry
        if entry is None or current is None or entry <= 0:
            continue

        raw_pnl = ((current - entry) / entry) * 100
        pnl = -raw_pnl if direction in ("SHORT", "SELL") else raw_pnl
        win = pnl > 0

        received = s.get("received_at")
        hour = received.hour if received else 0

        results.append({
            "pair": pair,
            "direction": direction,
            "pattern": pattern,
            "entry": entry,
            "current": current,
            "pnl": round(pnl, 2),
            "win": win,
            "hour": hour,
            "trend": s.get("trend", ""),
            "ai_score": s.get("ai_score"),
        })

    if not results:
        return {"total": len(signals), "with_result": 0, "error": "no prices"}

    # Агрегация
    wins = [r for r in results if r["win"]]
    losses = [r for r in results if not r["win"]]

    # По паттерну
    by_pattern = {}
    for r in results:
        p = r["pattern"]
        if p not in by_pattern:
            by_pattern[p] = {"count": 0, "wins": 0, "pnls": []}
        by_pattern[p]["count"] += 1
        if r["win"]:
            by_pattern[p]["wins"] += 1
        by_pattern[p]["pnls"].append(r["pnl"])
    for k, v in by_pattern.items():
        v["win_rate"] = round(v["wins"] / v["count"] * 100, 1) if v["count"] else 0
        v["avg_pnl"] = round(sum(v["pnls"]) / len(v["pnls"]), 2) if v["pnls"] else 0
        del v["pnls"]

    # По направлению
    by_direction = {}
    for d in ("LONG", "SHORT"):
        subset = [r for r in results if r["direction"] == d]
        if subset:
            w = sum(1 for r in subset if r["win"])
            by_direction[d] = {
                "count": len(subset),
                "win_rate": round(w / len(subset) * 100, 1),
                "avg_pnl": round(sum(r["pnl"] for r in subset) / len(subset), 2),
            }

    # По часу
    by_hour = {}
    for r in results:
        h = r["hour"]
        if h not in by_hour:
            by_hour[h] = {"count": 0, "wins": 0}
        by_hour[h]["count"] += 1
        if r["win"]:
            by_hour[h]["wins"] += 1
    for h, v in by_hour.items():
        v["win_rate"] = round(v["wins"] / v["count"] * 100, 1) if v["count"] else 0

    # Топ и худшие паттерны
    sorted_patterns = sorted(by_pattern.items(), key=lambda x: x[1]["win_rate"], reverse=True)
    top_patterns = [p for p, v in sorted_patterns if v["count"] >= 3 and v["win_rate"] >= 60][:5]
    worst_patterns = [p for p, v in sorted_patterns if v["count"] >= 3 and v["win_rate"] < 40][:5]

    return {
        "total": len(signals),
        "with_result": len(results),
        "overall_win_rate": round(len(wins) / len(results) * 100, 1),
        "overall_avg_pnl": round(sum(r["pnl"] for r in results) / len(results), 2),
        "total_pnl": round(sum(r["pnl"] for r in results), 2),
        "best_pnl": round(max(r["pnl"] for r in results), 2),
        "worst_pnl": round(min(r["pnl"] for r in results), 2),
        "by_pattern": by_pattern,
        "by_direction": by_direction,
        "by_hour": by_hour,
        "top_patterns": top_patterns,
        "worst_patterns": worst_patterns,
        "signals": sorted(results, key=lambda r: r["pnl"], reverse=True),
    }


def backtest_summary_for_ai(source: str = "cryptovizor") -> str:
    """Генерирует текстовый summary для Claude — используется как контекст
    при фильтрации новых сигналов."""
    bt = run_backtest(source)
    if bt.get("error"):
        return f"Недостаточно данных для бектеста: {bt.get('error')}"

    lines = [
        f"BACKTEST SUMMARY ({bt['with_result']} signals analyzed):",
        f"Overall: win_rate={bt['overall_win_rate']}%, avg_pnl={bt['overall_avg_pnl']}%, total_pnl={bt['total_pnl']}%",
        f"Best: {bt['best_pnl']}%, Worst: {bt['worst_pnl']}%",
        "",
        "BY PATTERN:",
    ]
    for p, v in sorted(bt["by_pattern"].items(), key=lambda x: -x[1]["count"]):
        lines.append(f"  {p}: count={v['count']}, win_rate={v['win_rate']}%, avg_pnl={v['avg_pnl']}%")

    lines.append("\nBY DIRECTION:")
    for d, v in bt["by_direction"].items():
        lines.append(f"  {d}: count={v['count']}, win_rate={v['win_rate']}%, avg_pnl={v['avg_pnl']}%")

    lines.append("\nTOP PATTERNS (win_rate >= 60%, count >= 3):")
    lines.append(f"  {', '.join(bt['top_patterns']) or 'none yet'}")

    lines.append("\nWORST PATTERNS (win_rate < 40%, count >= 3):")
    lines.append(f"  {', '.join(bt['worst_patterns']) or 'none yet'}")

    lines.append(f"\nBY HOUR (UTC):")
    for h in sorted(bt["by_hour"]):
        v = bt["by_hour"][h]
        lines.append(f"  {h:02d}:00 — count={v['count']}, win_rate={v['win_rate']}%")

    return "\n".join(lines)
