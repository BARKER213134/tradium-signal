"""Backtest Top Picks validation.

Для каждого Top Pick сигнала (Tradium/CV/Confluence) симулируем исход
walk-forward по 1h свечам:
  - Price touches TP → status=TP, R=+1
  - Price touches SL → status=SL, R=-1
  - N bars (default 48) passed → TIMEOUT, close at last close

Сравниваем Top Picks vs non-Top-Picks той же природы.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")

from database import _signals, _confluence, _clusters
from exchange import get_klines_any
from datetime import datetime, timedelta
from collections import defaultdict


def simulate_outcome(pair, direction, entry, tp, sl, at_time, max_bars=48):
    """Walk-forward simulation. Returns dict or None."""
    if not (entry and tp and sl and at_time):
        return None
    try:
        candles = get_klines_any(pair, "1h", max_bars + 50)
    except Exception:
        return None
    if not candles or len(candles) < 5:
        return None

    # Find candle index after `at_time`
    if hasattr(at_time, "timestamp"):
        entry_ts = int(at_time.timestamp())
    else:
        return None

    start_idx = None
    for i, c in enumerate(candles):
        c_t = c["t"] // 1000 if c["t"] > 10**12 else c["t"]
        if c_t >= entry_ts:
            start_idx = i
            break
    if start_idx is None or start_idx + 2 >= len(candles):
        return None

    is_long = direction in ("LONG", "BUY")
    risk = abs(entry - sl)
    if risk == 0:
        return None

    # Walk forward
    for i in range(start_idx + 1, min(start_idx + 1 + max_bars, len(candles))):
        c = candles[i]
        hi, lo = c["h"], c["l"]
        if is_long:
            if lo <= sl:
                return {"status": "SL", "R": -1.0, "bars": i - start_idx, "exit": sl}
            if hi >= tp:
                R = (tp - entry) / risk
                return {"status": "TP", "R": round(R, 2), "bars": i - start_idx, "exit": tp}
        else:
            if hi >= sl:
                return {"status": "SL", "R": -1.0, "bars": i - start_idx, "exit": sl}
            if lo <= tp:
                R = (entry - tp) / risk
                return {"status": "TP", "R": round(R, 2), "bars": i - start_idx, "exit": tp}

    # Timeout — close at last available close
    last = candles[min(start_idx + max_bars, len(candles) - 1)]
    exit_p = last["c"]
    R = (exit_p - entry) / risk if is_long else (entry - exit_p) / risk
    return {"status": "TIMEOUT", "R": round(R, 2), "bars": max_bars, "exit": exit_p}


def bucket_stats(name, results):
    if not results:
        return None
    n = len(results)
    tp = sum(1 for r in results if r["status"] == "TP")
    sl = sum(1 for r in results if r["status"] == "SL")
    timeout = sum(1 for r in results if r["status"] == "TIMEOUT")
    sum_r = sum(r["R"] for r in results)
    avg_r = sum_r / n
    wr = tp / (tp + sl) * 100 if (tp + sl) > 0 else 0
    wins = [r["R"] for r in results if r["R"] > 0]
    losses = [abs(r["R"]) for r in results if r["R"] < 0]
    pf = sum(wins) / sum(losses) if losses and sum(losses) > 0 else 999.9
    return {
        "name": name, "n": n, "tp": tp, "sl": sl, "timeout": timeout,
        "wr": round(wr, 1), "sum_r": round(sum_r, 2),
        "avg_r": round(avg_r, 3), "pf": round(pf, 2),
    }


def backtest_tradium():
    """Tradium Top Picks vs non-top (с pattern_triggered=True, есть entry/tp1/sl)."""
    top = []
    regular = []
    for s in _signals().find({"source": "tradium", "pattern_triggered": True, "pair": {"$ne": None}}):
        at = s.get("pattern_triggered_at") or s.get("received_at")
        res = simulate_outcome(s["pair"], s["direction"], s.get("entry"), s.get("tp1"), s.get("sl"), at)
        if res is None:
            continue
        (top if s.get("is_top_pick") else regular).append(res)
    return top, regular


def backtest_cv():
    """CV Top Picks vs non-top. TP=dca2 (R1), SL=dca1 (S1)."""
    top, regular = [], []
    for s in _signals().find({"source": "cryptovizor", "pattern_triggered": True, "pair": {"$ne": None}}):
        at = s.get("pattern_triggered_at") or s.get("received_at")
        entry = s.get("pattern_price") or s.get("entry")
        tp = s.get("dca2")  # R1 level as TP
        sl = s.get("dca1")  # S1 level as SL
        if not (entry and tp and sl):
            continue
        res = simulate_outcome(s["pair"], s["direction"], entry, tp, sl, at)
        if res is None:
            continue
        (top if s.get("is_top_pick") else regular).append(res)
    return top, regular


def backtest_confluence():
    """Confluence Top Picks vs non-top. TP=r1, SL=s1."""
    top, regular = [], []
    for c in _confluence().find({"score": {"$gte": 5}, "pair": {"$ne": None}}):
        pair = c.get("pair") or (c.get("symbol") or "").replace("USDT", "/USDT")
        if not pair:
            continue
        at = c.get("detected_at")
        entry = c.get("price")
        tp = c.get("r1")
        sl = c.get("s1")
        if not (entry and tp and sl):
            continue
        res = simulate_outcome(pair, c["direction"], entry, tp, sl, at)
        if res is None:
            continue
        (top if c.get("is_top_pick") else regular).append(res)
    return top, regular


def backtest_cluster():
    """Cluster Top Picks vs non-top. Из DB (уже есть outcome)."""
    top, regular = [], []
    for c in _clusters().find({"status": {"$in": ["TP", "SL"]}}):
        pnl = c.get("pnl_percent") or 0
        risk_pct = 1.5  # sl_pct in config
        R = pnl / risk_pct  # приблизительный R
        res = {"status": c["status"], "R": round(R, 2), "bars": 0, "exit": c.get("exit_price")}
        (top if c.get("is_top_pick") else regular).append(res)
    return top, regular


def main():
    print("=" * 80)
    print("TOP PICKS VALIDATION — Tradium/CV/Confluence/Cluster")
    print("=" * 80)

    all_results = []

    # Cluster (уже имеет outcomes)
    cl_top, cl_reg = backtest_cluster()
    print(f"\n[CLUSTER] from DB actual outcomes")
    print(f"  Top Picks: {len(cl_top)} closed")
    print(f"  Regular:   {len(cl_reg)} closed")
    r_top = bucket_stats("Cluster TOP", cl_top)
    r_reg = bucket_stats("Cluster REG", cl_reg)
    if r_top: all_results.append(r_top)
    if r_reg: all_results.append(r_reg)

    # Tradium
    print(f"\n[TRADIUM] simulating walk-forward 48 bars (1h)...")
    tr_top, tr_reg = backtest_tradium()
    print(f"  Top Picks: {len(tr_top)}, Regular: {len(tr_reg)}")
    r_top = bucket_stats("Tradium TOP", tr_top)
    r_reg = bucket_stats("Tradium REG", tr_reg)
    if r_top: all_results.append(r_top)
    if r_reg: all_results.append(r_reg)

    # CV
    print(f"\n[CRYPTOVIZOR] simulating walk-forward 48 bars...")
    cv_top, cv_reg = backtest_cv()
    print(f"  Top Picks: {len(cv_top)}, Regular: {len(cv_reg)}")
    r_top = bucket_stats("CV TOP", cv_top)
    r_reg = bucket_stats("CV REG", cv_reg)
    if r_top: all_results.append(r_top)
    if r_reg: all_results.append(r_reg)

    # Confluence
    print(f"\n[CONFLUENCE score>=5] simulating walk-forward 48 bars...")
    cf_top, cf_reg = backtest_confluence()
    print(f"  Top Picks: {len(cf_top)}, Regular STRONG (non-top): {len(cf_reg)}")
    r_top = bucket_stats("Confluence TOP", cf_top)
    r_reg = bucket_stats("Confluence REG", cf_reg)
    if r_top: all_results.append(r_top)
    if r_reg: all_results.append(r_reg)

    # Report
    print("\n" + "=" * 80)
    print(f"{'Bucket':<22} {'N':>4} {'TP':>3} {'SL':>3} {'TMO':>4} {'WR%':>5} {'SumR':>8} {'AvgR':>7} {'PF':>6}")
    print("=" * 80)
    for r in all_results:
        print(f"{r['name']:<22} {r['n']:>4} {r['tp']:>3} {r['sl']:>3} {r['timeout']:>4} {r['wr']:>5.1f} {r['sum_r']:>8.2f} {r['avg_r']:>7.3f} {r['pf']:>6.2f}")

    # Сравнение TOP vs REG
    print("\n" + "=" * 80)
    print("TOP vs REGULAR — разница в качестве:")
    print("=" * 80)
    for base in ["Cluster", "Tradium", "CV", "Confluence"]:
        top = next((r for r in all_results if r["name"] == f"{base} TOP"), None)
        reg = next((r for r in all_results if r["name"] == f"{base} REG"), None)
        if top and reg and top["n"] >= 3 and reg["n"] >= 3:
            d_wr = top["wr"] - reg["wr"]
            d_avg = top["avg_r"] - reg["avg_r"]
            d_pf = top["pf"] - reg["pf"]
            sign_wr = "+" if d_wr >= 0 else ""
            sign_avg = "+" if d_avg >= 0 else ""
            better = "✅" if d_wr > 0 and d_avg > 0 else "⚠" if d_wr > 0 or d_avg > 0 else "❌"
            print(f"  {base:<12} TOP={top['wr']}% / REG={reg['wr']}%  Δ={sign_wr}{d_wr:.1f} п.п.  "
                  f"AvgR Δ={sign_avg}{d_avg:.3f}  PF Δ={d_pf:+.2f}  {better}")
        elif top:
            print(f"  {base:<12} TOP n={top['n']} (REG n<3, недостаточно для сравнения)")

    # Save
    import json
    with open("backtest_top_picks_validate_results.json", "w") as f:
        json.dump({
            "generated_at": datetime.utcnow().isoformat(),
            "results": all_results,
        }, f, indent=2, default=str)
    print(f"\nSaved: backtest_top_picks_validate_results.json")


if __name__ == "__main__":
    main()
