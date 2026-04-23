"""Бэктест FVG стратегий на 42 инструментах FundingPips × 3 TF × 9 стратегий.

Стратегии:
  Entry:  A (Aggressive), B (Conservative), C (Hybrid)
  Exit:   rr2, rr3, trailing (3 варианта TP)
  Итого 9 комбо × 3 TF × 42 инструмента = 1134 backtest runs.

Данные: Yahoo Finance (besplatno, без ключей).
Период: 14 дней для 1H, 60 дней для 4H и 1D.
"""
import logging
logging.basicConfig(level=logging.WARNING, force=True)
import sys
try: sys.stdout.reconfigure(encoding='utf-8')
except: pass

import concurrent.futures
import warnings
from collections import defaultdict
from typing import Optional

warnings.filterwarnings("ignore")

import yfinance as yf
from fvg_detector import detect_fvg, evaluate_aggressive, evaluate_conservative, evaluate_hybrid


# ── Инструменты FundingPips → тикеры Yahoo ────────────────────
INSTRUMENTS = {
    # Forex Majors
    "EURUSD":  ("EURUSD=X", "forex"),
    "GBPUSD":  ("GBPUSD=X", "forex"),
    "USDJPY":  ("USDJPY=X", "forex"),
    "USDCHF":  ("USDCHF=X", "forex"),
    "AUDUSD":  ("AUDUSD=X", "forex"),
    "NZDUSD":  ("NZDUSD=X", "forex"),
    "USDCAD":  ("USDCAD=X", "forex"),
    # Crosses
    "EURGBP":  ("EURGBP=X", "forex"),
    "EURJPY":  ("EURJPY=X", "forex"),
    "EURCHF":  ("EURCHF=X", "forex"),
    "EURAUD":  ("EURAUD=X", "forex"),
    "EURCAD":  ("EURCAD=X", "forex"),
    "GBPJPY":  ("GBPJPY=X", "forex"),
    "GBPCHF":  ("GBPCHF=X", "forex"),
    "GBPAUD":  ("GBPAUD=X", "forex"),
    "GBPCAD":  ("GBPCAD=X", "forex"),
    "AUDJPY":  ("AUDJPY=X", "forex"),
    "AUDCAD":  ("AUDCAD=X", "forex"),
    "AUDCHF":  ("AUDCHF=X", "forex"),
    "AUDNZD":  ("AUDNZD=X", "forex"),
    "CADJPY":  ("CADJPY=X", "forex"),
    "CHFJPY":  ("CHFJPY=X", "forex"),
    # Exotics
    "USDMXN":  ("USDMXN=X", "forex"),
    "USDZAR":  ("USDZAR=X", "forex"),
    "USDSEK":  ("USDSEK=X", "forex"),
    "USDNOK":  ("USDNOK=X", "forex"),
    "USDSGD":  ("USDSGD=X", "forex"),
    # Metals
    "XAUUSD":  ("GC=F", "metal"),    # Gold futures
    "XAGUSD":  ("SI=F", "metal"),    # Silver futures
    # Indices
    "SPX500":  ("^GSPC", "index"),
    "NAS100":  ("^IXIC", "index"),
    "US30":    ("^DJI",  "index"),
    "GER40":   ("^GDAXI", "index"),
    "UK100":   ("^FTSE", "index"),
    "JPN225":  ("^N225", "index"),
    # Energies
    "USOIL":   ("CL=F",  "energy"),   # WTI
    "UKOIL":   ("BZ=F",  "energy"),   # Brent
    # Crypto
    "BTCUSD":  ("BTC-USD", "crypto"),
    "ETHUSD":  ("ETH-USD", "crypto"),
    "XRPUSD":  ("XRP-USD", "crypto"),
    "LTCUSD":  ("LTC-USD", "crypto"),
    "BCHUSD":  ("BCH-USD", "crypto"),
}

TIMEFRAMES = {
    "1H": ("14d", "1h"),
    "4H": ("60d", "1h"),   # yfinance не даёт 4h, аггрегируем из 1h
    "1D": ("60d", "1d"),
}

ENTRY_STRATEGIES = ["A", "B", "C"]
EXIT_MODES = ["rr2", "rr3", "trailing"]


def fetch_candles(ticker: str, period: str, interval: str) -> list[dict]:
    """Скачивает данные через yfinance, конвертит в candles."""
    try:
        df = yf.download(ticker, period=period, interval=interval, progress=False,
                         auto_adjust=True, threads=False)
        if df.empty:
            return []
        candles = []
        for ts, row in df.iterrows():
            try:
                def val(k):
                    v = row[k]
                    return float(v.iloc[0] if hasattr(v, 'iloc') else v)
                o, h, l, c = val("Open"), val("High"), val("Low"), val("Close")
                if not (o > 0 and h > 0 and l > 0 and c > 0):
                    continue
                candles.append({
                    "t": int(ts.timestamp()),
                    "o": o, "h": h, "l": l, "c": c,
                    "v": 0,
                })
            except Exception:
                continue
        return candles
    except Exception as e:
        return []


def aggregate_to_4h(candles_1h: list[dict]) -> list[dict]:
    """Агрегирует 1H свечи в 4H."""
    if not candles_1h:
        return []
    out = []
    bucket = []
    for c in candles_1h:
        bucket.append(c)
        if len(bucket) == 4:
            out.append({
                "t": bucket[0]["t"],
                "o": bucket[0]["o"],
                "h": max(b["h"] for b in bucket),
                "l": min(b["l"] for b in bucket),
                "c": bucket[-1]["c"],
                "v": sum(b["v"] for b in bucket),
            })
            bucket = []
    return out


def run_backtest_one(name: str, ticker: str, asset_class: str):
    """Для одного инструмента: прогоняет все 9 стратегий × 3 TF."""
    results = []

    for tf_name, (period, interval) in TIMEFRAMES.items():
        if tf_name == "4H":
            # 4H = агрегация из 1h
            c1 = fetch_candles(ticker, "60d", "1h")
            candles = aggregate_to_4h(c1)
        else:
            candles = fetch_candles(ticker, period, interval)

        if len(candles) < 20:
            continue
        fvgs = detect_fvg(candles, min_size_rel=0.0003)

        for entry_strat in ENTRY_STRATEGIES:
            for exit_mode in EXIT_MODES:
                trades = []
                for fvg in fvgs:
                    if entry_strat == "A":
                        r = evaluate_aggressive(fvg, candles, exit_mode=exit_mode)
                    elif entry_strat == "B":
                        r = evaluate_conservative(fvg, candles, exit_mode=exit_mode)
                    else:
                        r = evaluate_hybrid(fvg, candles, exit_mode=exit_mode)
                    if r.status == "SKIPPED":
                        continue
                    trades.append(r)

                if not trades:
                    continue

                # Metrics
                wins = sum(1 for t in trades if t.R > 0)
                losses = sum(1 for t in trades if t.R <= 0)
                total = len(trades)
                wr = wins / max(total, 1) * 100
                sum_r = sum(t.R for t in trades)
                avg_r = sum_r / max(total, 1)
                max_r = max((t.R for t in trades), default=0)
                min_r = min((t.R for t in trades), default=0)
                avg_bars = sum(t.bars_held for t in trades) / max(total, 1)

                results.append({
                    "instrument": name,
                    "asset_class": asset_class,
                    "tf": tf_name,
                    "entry": entry_strat,
                    "exit": exit_mode,
                    "n_fvg": len(fvgs),
                    "n_trades": total,
                    "wins": wins, "losses": losses,
                    "wr": round(wr, 1),
                    "sum_r": round(sum_r, 2),
                    "avg_r": round(avg_r, 3),
                    "max_r": round(max_r, 2),
                    "min_r": round(min_r, 2),
                    "avg_bars": round(avg_bars, 1),
                })
    return results


def main():
    print(f"Instruments: {len(INSTRUMENTS)}")
    print(f"Timeframes: {list(TIMEFRAMES.keys())}")
    print(f"Strategy combos: {len(ENTRY_STRATEGIES)} entry × {len(EXIT_MODES)} exit = {len(ENTRY_STRATEGIES) * len(EXIT_MODES)}")
    print(f"Total backtest runs: {len(INSTRUMENTS) * len(TIMEFRAMES) * len(ENTRY_STRATEGIES) * len(EXIT_MODES)}")
    print("\nЗапуск...\n", flush=True)

    all_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(run_backtest_one, name, t, cls): name
                   for name, (t, cls) in INSTRUMENTS.items()}
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

    if not all_results:
        print("Нет данных")
        return

    print(f"\n=== ВСЕГО РЕЗУЛЬТАТОВ: {len(all_results)} ===\n")

    # ── 1) Overall по стратегиям (entry × exit) ──
    print("=" * 100)
    print("ПО СТРАТЕГИЯМ (entry × exit)")
    print("=" * 100)
    by_strat = defaultdict(list)
    for r in all_results:
        by_strat[(r["entry"], r["exit"])].append(r)
    print(f"{'Entry':<7} {'Exit':<10} {'N Runs':>7} {'Trades':>7} {'Wins':>6} {'Loss':>6} {'WR%':>6} {'Avg R':>8} {'Sum R':>8}")
    print("-" * 100)
    for (e, x), runs in sorted(by_strat.items()):
        total_trades = sum(r["n_trades"] for r in runs)
        wins = sum(r["wins"] for r in runs)
        losses = sum(r["losses"] for r in runs)
        sum_r = sum(r["sum_r"] for r in runs)
        avg_r = sum_r / max(total_trades, 1)
        wr = wins / max(wins + losses, 1) * 100
        marker = "✅" if sum_r > 0 else "  "
        print(f"{marker}{e:<5} {x:<10} {len(runs):>7} {total_trades:>7} {wins:>6} {losses:>6} {wr:>5.1f}% {avg_r:>+7.3f}R {sum_r:>+7.1f}R")

    # ── 2) По TF ──
    print("\n" + "=" * 100)
    print("ПО ТАЙМФРЕЙМАМ")
    print("=" * 100)
    by_tf = defaultdict(list)
    for r in all_results:
        by_tf[r["tf"]].append(r)
    print(f"{'TF':<4} {'N Runs':>7} {'Trades':>7} {'WR%':>6} {'Avg R':>8} {'Sum R':>8}")
    print("-" * 50)
    for tf, runs in sorted(by_tf.items()):
        trades = sum(r["n_trades"] for r in runs)
        w = sum(r["wins"] for r in runs); l = sum(r["losses"] for r in runs)
        sum_r = sum(r["sum_r"] for r in runs)
        wr = w / max(w + l, 1) * 100
        print(f"{tf:<4} {len(runs):>7} {trades:>7} {wr:>5.1f}% {sum_r/max(trades,1):>+7.3f}R {sum_r:>+7.1f}R")

    # ── 3) По классам ──
    print("\n" + "=" * 100)
    print("ПО КЛАССАМ АКТИВОВ")
    print("=" * 100)
    by_cls = defaultdict(list)
    for r in all_results:
        by_cls[r["asset_class"]].append(r)
    print(f"{'Class':<10} {'N Runs':>7} {'Trades':>7} {'WR%':>6} {'Avg R':>8} {'Sum R':>8}")
    print("-" * 56)
    for cls, runs in sorted(by_cls.items()):
        trades = sum(r["n_trades"] for r in runs)
        w = sum(r["wins"] for r in runs); l = sum(r["losses"] for r in runs)
        sum_r = sum(r["sum_r"] for r in runs)
        wr = w / max(w + l, 1) * 100
        print(f"{cls:<10} {len(runs):>7} {trades:>7} {wr:>5.1f}% {sum_r/max(trades,1):>+7.3f}R {sum_r:>+7.1f}R")

    # ── 4) ТОП 20 комбо (instrument + TF + entry + exit) ──
    print("\n" + "=" * 100)
    print("ТОП 20 КОМБО ПО SUM R (min 5 trades)")
    print("=" * 100)
    print(f"{'Inst':<10} {'TF':<4} {'E':<2} {'Exit':<10} {'N':>4} {'WR%':>6} {'Avg R':>8} {'Sum R':>8}")
    print("-" * 64)
    qualified = [r for r in all_results if r["n_trades"] >= 5]
    qualified.sort(key=lambda r: -r["sum_r"])
    for r in qualified[:20]:
        print(f"{r['instrument']:<10} {r['tf']:<4} {r['entry']:<2} {r['exit']:<10} "
              f"{r['n_trades']:>4} {r['wr']:>5.1f}% {r['avg_r']:>+7.3f}R {r['sum_r']:>+7.1f}R")

    # ── 5) АНТИ-ТОП 10 (худшие убыточные) ──
    print("\n" + "=" * 100)
    print("АНТИ-ТОП 10 (самые убыточные, min 5 trades)")
    print("=" * 100)
    print(f"{'Inst':<10} {'TF':<4} {'E':<2} {'Exit':<10} {'N':>4} {'WR%':>6} {'Avg R':>8} {'Sum R':>8}")
    print("-" * 64)
    qualified.sort(key=lambda r: r["sum_r"])
    for r in qualified[:10]:
        print(f"{r['instrument']:<10} {r['tf']:<4} {r['entry']:<2} {r['exit']:<10} "
              f"{r['n_trades']:>4} {r['wr']:>5.1f}% {r['avg_r']:>+7.3f}R {r['sum_r']:>+7.1f}R")

    # ── 6) Лучшие инструменты по сумме R (по всем стратегиям) ──
    print("\n" + "=" * 100)
    print("ТОП 15 ИНСТРУМЕНТОВ ПО СУММЕ R (все стратегии)")
    print("=" * 100)
    by_inst = defaultdict(list)
    for r in all_results:
        by_inst[r["instrument"]].append(r)
    inst_sums = [(name, sum(r["sum_r"] for r in runs),
                  sum(r["n_trades"] for r in runs))
                 for name, runs in by_inst.items()]
    inst_sums.sort(key=lambda x: -x[1])
    print(f"{'Инструмент':<10} {'Sum R':>8} {'Trades':>8}")
    for name, sr, nt in inst_sums[:15]:
        print(f"{name:<10} {sr:>+7.1f}R {nt:>8}")

    # Save JSON
    import json
    out_path = "backtest_fvg_results.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
    print(f"\nРезультаты сохранены в {out_path}")


if __name__ == "__main__":
    main()
