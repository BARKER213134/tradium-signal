"""Backtest: какая стратегия фильтрации даёт лучшие результаты на наших кластерах?

Тестируем:
- Baseline: все кластеры (текущая фильтрация)
- By strength: только MEGA / STRONG / NORMAL
- By conflict: только без anti-cluster
- Combined: strength + no-conflict
- By our proposed score: top 20% / 50% / 80%
- By sources count: 3+ sources / 2 sources
- By reversal alignment: aligned / against

Метрики:
- N (сколько сделок)
- WR %
- Avg PnL %
- Sum PnL %
- Profit Factor
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")

from database import _clusters, _conflicts, _signals
from datetime import timedelta
from collections import defaultdict


def load_clusters_closed():
    """Закрытые кластеры (TP/SL) с известным исходом."""
    docs = list(_clusters().find({"status": {"$in": ["TP", "SL"]}}))
    # Обогащаем каждый контекстом:
    # - был ли конфликт на паре в тот момент?
    # - сколько Confluence STRONG за последние 48ч до
    for c in docs:
        pair = c.get("pair", "")
        trig = c.get("trigger_at")
        if not trig:
            continue
        # Конфликты в ±1ч от trigger
        cfl = _conflicts().count_documents({
            "$or": [{"pair": pair}, {"symbol": c.get("symbol")}],
            "detected_at": {
                "$gte": trig - timedelta(hours=1),
                "$lte": trig + timedelta(hours=1),
            },
        })
        c["_conflict_count"] = cfl
        # Сколько STRONG Confluence за 48ч до
        from database import _confluence
        sc = _confluence().count_documents({
            "$or": [{"pair": pair}, {"symbol": c.get("symbol")}],
            "direction": c.get("direction"),
            "score": {"$gte": 5},
            "detected_at": {
                "$gte": trig - timedelta(hours=48),
                "$lt": trig,
            },
        })
        c["_strong_conf_48h"] = sc
    return docs


def cluster_score(c):
    """Наша формула Opportunity Score для backtested cluster."""
    score = 0
    strength = c.get("strength", "NORMAL")
    # Качество источника
    score += {"MEGA": 30, "STRONG": 25, "NORMAL": 18, "RISKY": 10}.get(strength, 15)
    # Разнообразие источников
    score += min(15, (c.get("sources_count", 1)) * 6)  # 2=12, 3=18→15, 4=24→15
    # Количество голосов
    score += min(10, (c.get("signals_count", 1)) * 2)  # 2=4, 3=6, 4=8, 5=10
    # Reversal Meter alignment
    rev = c.get("reversal_score", 0)
    rev_dir = c.get("reversal_direction", "NEUTRAL")
    dir_long = c.get("direction") in ("LONG", "BUY")
    aligned = (dir_long and rev_dir == "BULLISH") or (not dir_long and rev_dir == "BEARISH")
    if aligned and abs(rev) >= 50:
        score += 15
    elif aligned:
        score += 8
    elif rev_dir != "NEUTRAL":  # против reversal — штраф
        score -= 10
    # Anti-cluster: штраф за конфликт
    if c.get("_conflict_count", 0) > 0:
        score -= 50  # большой штраф
    # Бонус за STRONG confluence рядом
    sc = c.get("_strong_conf_48h", 0)
    if sc >= 2:
        score += 10
    elif sc == 1:
        score += 5
    return max(0, min(100, score))


def stats(docs, name):
    if not docs:
        return {"name": name, "n": 0, "tp": 0, "sl": 0, "wr": 0, "avg_pnl": 0, "sum_pnl": 0, "pf": 0}
    tp = sum(1 for c in docs if c.get("status") == "TP")
    sl = sum(1 for c in docs if c.get("status") == "SL")
    pnls = [c.get("pnl_percent") or 0 for c in docs]
    sum_pnl = sum(pnls)
    avg_pnl = sum_pnl / len(pnls)
    wins = [p for p in pnls if p > 0]
    losses = [abs(p) for p in pnls if p < 0]
    pf = (sum(wins) / sum(losses)) if losses and sum(losses) > 0 else 999.9
    wr = tp / (tp + sl) * 100 if (tp + sl) > 0 else 0
    return {
        "name": name, "n": len(docs),
        "tp": tp, "sl": sl,
        "wr": round(wr, 1),
        "avg_pnl": round(avg_pnl, 3),
        "sum_pnl": round(sum_pnl, 2),
        "pf": round(pf, 2),
    }


def main():
    docs = load_clusters_closed()
    print(f"Total closed clusters in DB: {len(docs)}")
    if not docs:
        print("No data to backtest")
        return

    # Считаем score для каждого
    for c in docs:
        c["_score"] = cluster_score(c)
    docs.sort(key=lambda x: -x["_score"])

    # Распределение score
    print(f"\nScore distribution:")
    buckets = defaultdict(int)
    for c in docs:
        b = (c["_score"] // 10) * 10
        buckets[b] += 1
    for b in sorted(buckets.keys()):
        print(f"  {b}-{b+9}: {'█' * buckets[b]} ({buckets[b]})")

    # Тесты разных фильтров
    results = []
    results.append(stats(docs, "Baseline (all closed)"))

    # By strength
    for s in ["MEGA", "STRONG", "NORMAL", "RISKY"]:
        results.append(stats([c for c in docs if c.get("strength") == s], f"Strength == {s}"))

    # By conflict
    results.append(stats([c for c in docs if c.get("_conflict_count", 0) == 0], "No anti-cluster conflict"))
    results.append(stats([c for c in docs if c.get("_conflict_count", 0) > 0], "With conflict (yikes!)"))

    # By reversal alignment
    def aligned(c):
        dir_long = c.get("direction") in ("LONG", "BUY")
        return (dir_long and c.get("reversal_direction") == "BULLISH") or \
               (not dir_long and c.get("reversal_direction") == "BEARISH")
    results.append(stats([c for c in docs if aligned(c)], "Reversal Meter aligned"))
    results.append(stats([c for c in docs if c.get("reversal_direction") == "NEUTRAL"], "Reversal Neutral"))

    # By sources count
    for n in [2, 3]:
        results.append(stats([c for c in docs if c.get("sources_count", 0) >= n], f"Sources >= {n}"))

    # By STRONG confluence nearby
    results.append(stats([c for c in docs if c.get("_strong_conf_48h", 0) >= 1],
                         "With STRONG Confluence ≤ 48h"))

    # By score thresholds
    total = len(docs)
    for pct in [20, 30, 50]:
        cut = sorted([c["_score"] for c in docs], reverse=True)
        if len(cut) >= int(total * pct / 100):
            threshold = cut[int(total * pct / 100) - 1]
            subset = [c for c in docs if c["_score"] >= threshold]
            results.append(stats(subset, f"Top {pct}% by Score (≥{threshold})"))

    # Combined: STRONG/MEGA + no conflict + aligned
    combo = [c for c in docs
             if c.get("strength") in ("MEGA", "STRONG")
             and c.get("_conflict_count", 0) == 0
             and aligned(c)]
    results.append(stats(combo, "★ Combined: MEGA/STRONG + no-conflict + aligned"))

    # Combined simpler: strong + no conflict
    combo2 = [c for c in docs
              if c.get("strength") in ("MEGA", "STRONG")
              and c.get("_conflict_count", 0) == 0]
    results.append(stats(combo2, "★ Combined: MEGA/STRONG + no-conflict"))

    # Print
    print(f"\n{'='*100}")
    print(f"{'Filter':<55} {'N':>4} {'TP':>4} {'SL':>4} {'WR%':>6} {'AvgPnL':>8} {'SumPnL':>8} {'PF':>6}")
    print("="*100)
    # Baseline first
    r0 = results[0]
    print(f"{r0['name']:<55} {r0['n']:>4} {r0['tp']:>4} {r0['sl']:>4} {r0['wr']:>5.1f}% {r0['avg_pnl']:>7.3f}% {r0['sum_pnl']:>7.2f}% {r0['pf']:>6.2f}")
    print("-" * 100)
    # Rest sorted by WR
    rest = results[1:]
    rest.sort(key=lambda x: (-x["wr"], -x["avg_pnl"]))
    for r in rest:
        if r["n"] < 3:
            # маркер маленькой выборки
            marker = " (small N!)"
        else:
            marker = ""
        print(f"{r['name']:<55} {r['n']:>4} {r['tp']:>4} {r['sl']:>4} {r['wr']:>5.1f}% {r['avg_pnl']:>7.3f}% {r['sum_pnl']:>7.2f}% {r['pf']:>6.2f}{marker}")

    # TOP-5 individual clusters by score
    print(f"\n{'='*100}")
    print("TOP-10 CLUSTERS BY SCORE (и их реальный исход):")
    print("="*100)
    for c in docs[:10]:
        emoji = "✅" if c.get("status") == "TP" else "❌"
        print(f"  score={c['_score']:>3}  {emoji} {c.get('pair'):<12} {c.get('direction'):<5} {c.get('strength'):<8} "
              f"sources={c.get('sources_count')} sigs={c.get('signals_count')} "
              f"conf={c['_conflict_count']} strong_cf={c['_strong_conf_48h']} PnL={c.get('pnl_percent') or 0:+.2f}%")


if __name__ == "__main__":
    main()
