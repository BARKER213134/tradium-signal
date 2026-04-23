"""Бэктест FVG на 3 месяца — 4 стратегии (B baseline + Hybrid v2/v3/v4).

Отличия от backtest_fvg.py:
- 90 дней для 1H, 180 дней для 1D
- Убрали crypto из основного прогона (по прошлому бэктесту провал)
- 4H убрали (хуже 1H и 1D)
- Focus: 1H + 1D
- Entry: Conservative (B) — лучшая
- Exit: Trailing
- 4 варианта фильтров: baseline B, Hybrid v2, v3, v4
"""
import logging
logging.basicConfig(level=logging.WARNING, force=True)
import sys
try: sys.stdout.reconfigure(encoding='utf-8')
except: pass

import concurrent.futures
import warnings
from collections import defaultdict

warnings.filterwarnings("ignore")

import yfinance as yf
from fvg_detector import detect_fvg, evaluate_conservative
from fvg_hybrid_v2 import evaluate_hybrid_v2, evaluate_hybrid_v3, evaluate_hybrid_v4


INSTRUMENTS = {
    # Forex (22)
    "EURUSD":  ("EURUSD=X", "forex"),
    "GBPUSD":  ("GBPUSD=X", "forex"),
    "USDJPY":  ("USDJPY=X", "forex"),
    "USDCHF":  ("USDCHF=X", "forex"),
    "AUDUSD":  ("AUDUSD=X", "forex"),
    "NZDUSD":  ("NZDUSD=X", "forex"),
    "USDCAD":  ("USDCAD=X", "forex"),
    "EURGBP":  ("EURGBP=X", "forex"),
    "EURJPY":  ("EURJPY=X", "forex"),
    "EURCHF":  ("EURCHF=X", "forex"),
    "EURAUD":  ("EURAUD=X", "forex"),
    "EURCAD":  ("EURCAD=X", "forex"),
    "GBPJPY":  ("GBPJPY=X", "forex"),
    "GBPAUD":  ("GBPAUD=X", "forex"),
    "GBPCAD":  ("GBPCAD=X", "forex"),
    "AUDJPY":  ("AUDJPY=X", "forex"),
    "AUDCAD":  ("AUDCAD=X", "forex"),
    "AUDNZD":  ("AUDNZD=X", "forex"),
    "CADJPY":  ("CADJPY=X", "forex"),
    "CHFJPY":  ("CHFJPY=X", "forex"),
    "USDMXN":  ("USDMXN=X", "forex"),
    "USDNOK":  ("USDNOK=X", "forex"),
    # Metals (2)
    "XAUUSD":  ("GC=F", "metal"),
    "XAGUSD":  ("SI=F", "metal"),
    # Indices (6)
    "SPX500":  ("^GSPC", "index"),
    "NAS100":  ("^IXIC", "index"),
    "US30":    ("^DJI",  "index"),
    "GER40":   ("^GDAXI", "index"),
    "UK100":   ("^FTSE", "index"),
    "JPN225":  ("^N225", "index"),
    # Energy (2)
    "USOIL":   ("CL=F",  "energy"),
    "UKOIL":   ("BZ=F",  "energy"),
}

TIMEFRAMES = {
    "1H": ("90d", "1h"),
    "1D": ("180d", "1d"),
}

STRATEGIES = {
    "B_base": lambda fvg, candles, cls: evaluate_conservative(fvg, candles, exit_mode="trailing"),
    "Hybrid_v2": lambda fvg, candles, cls: evaluate_hybrid_v2(fvg, candles, cls, "trailing"),
    "Hybrid_v3": lambda fvg, candles, cls: evaluate_hybrid_v3(fvg, candles, cls, "trailing"),
    "Hybrid_v4": lambda fvg, candles, cls: evaluate_hybrid_v4(fvg, candles, cls, "trailing"),
}


def fetch_candles(ticker, period, interval):
    try:
        df = yf.download(ticker, period=period, interval=interval, progress=False,
                         auto_adjust=True, threads=False)
        if df.empty: return []
        candles = []
        for ts, row in df.iterrows():
            try:
                def val(k):
                    v = row[k]
                    return float(v.iloc[0] if hasattr(v, 'iloc') else v)
                o, h, l, c = val("Open"), val("High"), val("Low"), val("Close")
                if not (o > 0 and h > 0 and l > 0 and c > 0): continue
                candles.append({"t": int(ts.timestamp()), "o": o, "h": h, "l": l, "c": c, "v": 0})
            except: continue
        return candles
    except: return []


def run_one(name, ticker, asset_class):
    results = []
    for tf_name, (period, interval) in TIMEFRAMES.items():
        candles = fetch_candles(ticker, period, interval)
        if len(candles) < 30: continue
        fvgs = detect_fvg(candles, min_size_rel=0.0002)
        for strat_name, strat_fn in STRATEGIES.items():
            trades = []
            for fvg in fvgs:
                r = strat_fn(fvg, candles, asset_class)
                if r.status == "SKIPPED": continue
                trades.append(r)
            if not trades: continue
            wins = sum(1 for t in trades if t.R > 0)
            total = len(trades)
            sum_r = sum(t.R for t in trades)
            results.append({
                "instrument": name, "asset_class": asset_class, "tf": tf_name,
                "strategy": strat_name,
                "n_fvg": len(fvgs), "n_trades": total,
                "wins": wins, "losses": total - wins,
                "wr": round(wins / max(total, 1) * 100, 1),
                "sum_r": round(sum_r, 2),
                "avg_r": round(sum_r / max(total, 1), 3),
            })
    return results


def main():
    print(f"Instruments: {len(INSTRUMENTS)} (no crypto)")
    print(f"Timeframes: {list(TIMEFRAMES.keys())}")
    print(f"Strategies: {list(STRATEGIES.keys())}")
    print(f"Период: 90 дней (1H), 180 дней (1D)\n", flush=True)

    all_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(run_one, n, t, c): n for n, (t, c) in INSTRUMENTS.items()}
        done = 0
        for fut in concurrent.futures.as_completed(futures):
            name = futures[fut]
            done += 1
            try:
                res = fut.result()
                all_results.extend(res)
                print(f"[{done}/{len(INSTRUMENTS)}] {name}: {len(res)} runs", flush=True)
            except Exception as e:
                print(f"[{done}/{len(INSTRUMENTS)}] {name}: ERROR {e}", flush=True)

    print(f"\nВсего результатов: {len(all_results)}\n")

    # ── По стратегиям ──
    print("=" * 90)
    print("ПО СТРАТЕГИЯМ (Conservative+Trailing базовая + Hybrid фильтры)")
    print("=" * 90)
    by_s = defaultdict(list)
    for r in all_results: by_s[r["strategy"]].append(r)
    print(f"{'Стратегия':<12} {'Runs':>5} {'Trades':>7} {'Wins':>5} {'Loss':>5} {'WR%':>6} {'Avg R':>8} {'Sum R':>8} {'Expect':>8}")
    print("-" * 90)
    for s, runs in sorted(by_s.items()):
        tr = sum(r["n_trades"] for r in runs)
        w = sum(r["wins"] for r in runs); l = sum(r["losses"] for r in runs)
        sum_r = sum(r["sum_r"] for r in runs)
        avg = sum_r / max(tr, 1)
        wr = w / max(w + l, 1) * 100
        marker = "✅" if sum_r > 0 else "❌"
        print(f"{marker}{s:<10} {len(runs):>5} {tr:>7} {w:>5} {l:>5} {wr:>5.1f}% {avg:>+7.3f}R {sum_r:>+7.1f}R {avg:>+7.3f}")

    # ── По TF для каждой стратегии ──
    print("\n" + "=" * 90)
    print("ПО СТРАТЕГИИ × TF")
    print("=" * 90)
    by_st = defaultdict(list)
    for r in all_results: by_st[(r["strategy"], r["tf"])].append(r)
    print(f"{'Strategy':<12} {'TF':<4} {'Runs':>5} {'Trades':>7} {'WR%':>6} {'Avg R':>8} {'Sum R':>8}")
    print("-" * 64)
    for (s, tf), runs in sorted(by_st.items()):
        tr = sum(r["n_trades"] for r in runs)
        w = sum(r["wins"] for r in runs); l = sum(r["losses"] for r in runs)
        sum_r = sum(r["sum_r"] for r in runs)
        wr = w / max(w + l, 1) * 100
        avg = sum_r / max(tr, 1)
        print(f"{s:<12} {tf:<4} {len(runs):>5} {tr:>7} {wr:>5.1f}% {avg:>+7.3f}R {sum_r:>+7.1f}R")

    # ── По классам ──
    print("\n" + "=" * 90)
    print("ПО СТРАТЕГИИ × КЛАСС")
    print("=" * 90)
    by_cls = defaultdict(list)
    for r in all_results: by_cls[(r["strategy"], r["asset_class"])].append(r)
    print(f"{'Strategy':<12} {'Class':<10} {'Trades':>7} {'WR%':>6} {'Avg R':>8} {'Sum R':>8}")
    print("-" * 64)
    for (s, c), runs in sorted(by_cls.items()):
        tr = sum(r["n_trades"] for r in runs)
        w = sum(r["wins"] for r in runs); ll = sum(r["losses"] for r in runs)
        sum_r = sum(r["sum_r"] for r in runs)
        wr = w / max(w + ll, 1) * 100
        avg = sum_r / max(tr, 1)
        marker = "✅" if sum_r > 0 else "❌"
        print(f"{marker}{s:<10} {c:<10} {tr:>7} {wr:>5.1f}% {avg:>+7.3f}R {sum_r:>+7.1f}R")

    # ── ТОП 15 combo по Sum R ──
    print("\n" + "=" * 90)
    print("ТОП 15 COMBO (min 5 trades)")
    print("=" * 90)
    qualified = [r for r in all_results if r["n_trades"] >= 5]
    qualified.sort(key=lambda r: -r["sum_r"])
    print(f"{'Instr':<8} {'TF':<4} {'Strategy':<12} {'N':>4} {'WR%':>6} {'Avg R':>8} {'Sum R':>8}")
    print("-" * 60)
    for r in qualified[:15]:
        print(f"{r['instrument']:<8} {r['tf']:<4} {r['strategy']:<12} {r['n_trades']:>4} "
              f"{r['wr']:>5.1f}% {r['avg_r']:>+7.3f}R {r['sum_r']:>+7.1f}R")

    # ── ТОП инструменты для лучшей стратегии ──
    # Найдём лучшую стратегию по Sum R
    best_s = max(by_s.items(), key=lambda x: sum(r["sum_r"] for r in x[1]))[0]
    print(f"\n" + "=" * 90)
    print(f"ТОП 15 ИНСТРУМЕНТОВ для стратегии {best_s}")
    print("=" * 90)
    best_runs = [r for r in all_results if r["strategy"] == best_s]
    by_inst = defaultdict(list)
    for r in best_runs: by_inst[r["instrument"]].append(r)
    sums = [(n, sum(r["sum_r"] for r in rr), sum(r["n_trades"] for r in rr)) for n, rr in by_inst.items()]
    sums.sort(key=lambda x: -x[1])
    print(f"{'Instrument':<10} {'Sum R':>8} {'Trades':>8}")
    for n, sr, nt in sums[:15]:
        marker = "✅" if sr > 0 else "❌"
        print(f"{marker} {n:<10} {sr:>+7.1f}R {nt:>7}")

    # Save
    import json
    with open("backtest_fvg_3m.json", "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
    print(f"\nРезультаты: backtest_fvg_3m.json")


if __name__ == "__main__":
    main()
