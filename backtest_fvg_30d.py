"""Backtest Forex FVG стратегии на 30-дневных данных yfinance.
Перебирает параметры (size, body, sl_buffer), показывает оптимум.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")

from datetime import datetime, timezone, timedelta
from fvg_scanner import INSTRUMENTS, fetch_candles
from fvg_detector import detect_fvg
import itertools


def simulate_trade(fvg, candles_after, sl_buffer_rel=0.05, max_wait_bars=30,
                   max_hold_bars=50, tp_target_R=2.0, trailing_at_R=1.0,
                   trailing_distance_R=1.0):
    """Симулирует сделку: ждём ретест, входим, следим за TP/SL/trailing.

    Возвращает (status, R_outcome, peak_R):
      TP    → закрыт в плюс
      SL    → закрыт в минус
      NO_ENTRY → ретест не случился за max_wait_bars → skip
      TIMEOUT  → вошли, но не TP/SL за max_hold_bars
    """
    if fvg.direction == "bullish":
        entry = fvg.top
        sl = fvg.bottom - (fvg.top - fvg.bottom) * sl_buffer_rel
        risk = entry - sl
    else:
        entry = fvg.bottom
        sl = fvg.top + (fvg.top - fvg.bottom) * sl_buffer_rel
        risk = sl - entry

    if risk <= 0:
        return ("NO_ENTRY", None, 0.0)

    # Фаза 1: ждём retest
    entered_idx = None
    wait_cap = min(max_wait_bars, len(candles_after))
    for i in range(wait_cap):
        c = candles_after[i]
        if fvg.direction == "bullish":
            if c["l"] <= entry:
                entered_idx = i
                break
        else:
            if c["h"] >= entry:
                entered_idx = i
                break
    if entered_idx is None:
        return ("NO_ENTRY", None, 0.0)

    # Фаза 2: мониторим после входа
    hold = candles_after[entered_idx:entered_idx + max_hold_bars]
    peak_R = 0.0
    trailing_sl = sl
    trailing_active = False

    for c in hold:
        hi, lo = c["h"], c["l"]
        if fvg.direction == "bullish":
            # Max прибыль в R
            max_mfe = (hi - entry) / risk
            if max_mfe > peak_R:
                peak_R = max_mfe
            # Активируем trailing?
            if not trailing_active and peak_R >= trailing_at_R:
                trailing_active = True
                trailing_sl = entry + (peak_R - trailing_distance_R) * risk
            elif trailing_active:
                new_trail = entry + (peak_R - trailing_distance_R) * risk
                if new_trail > trailing_sl:
                    trailing_sl = new_trail
            # Проверяем SL (включая trailing)
            cur_sl = trailing_sl if trailing_active else sl
            if lo <= cur_sl:
                r = (cur_sl - entry) / risk
                return ("TP" if r > 0 else "SL", r, peak_R)
            # TP?
            if (hi - entry) / risk >= tp_target_R:
                return ("TP", tp_target_R, peak_R)
        else:
            max_mfe = (entry - lo) / risk
            if max_mfe > peak_R:
                peak_R = max_mfe
            if not trailing_active and peak_R >= trailing_at_R:
                trailing_active = True
                trailing_sl = entry - (peak_R - trailing_distance_R) * risk
            elif trailing_active:
                new_trail = entry - (peak_R - trailing_distance_R) * risk
                if new_trail < trailing_sl:
                    trailing_sl = new_trail
            cur_sl = trailing_sl if trailing_active else sl
            if hi >= cur_sl:
                r = (entry - cur_sl) / risk
                return ("TP" if r > 0 else "SL", r, peak_R)
            if (entry - lo) / risk >= tp_target_R:
                return ("TP", tp_target_R, peak_R)

    # Timeout — закрываем по цене последней свечи
    last_c = hold[-1]["c"]
    r = (last_c - entry) / risk if fvg.direction == "bullish" else (entry - last_c) / risk
    return ("TIMEOUT", r, peak_R)


def run_backtest(candles_by_inst, size_rel, body_ratio, sl_buffer_rel):
    """Гонит один вариант параметров по всем инструментам."""
    trades = []
    for name, candles in candles_by_inst.items():
        if len(candles) < 40:
            continue
        fvgs = detect_fvg(candles, min_size_rel=size_rel)
        for fvg in fvgs:
            if fvg.impulse_body_ratio < body_ratio:
                continue
            # Найти candles ПОСЛЕ FVG (i.e. после свечи i)
            idx = None
            for i, c in enumerate(candles):
                if c["t"] == fvg.time:
                    idx = i
                    break
            if idx is None or idx + 2 >= len(candles):
                continue
            # Candles после c[i+1] (c[i+1] это impulse bar, следующие — наша зона мониторинга)
            candles_after = candles[idx + 2:]
            status, R, peak = simulate_trade(fvg, candles_after, sl_buffer_rel=sl_buffer_rel)
            if status == "NO_ENTRY":
                continue
            trades.append({"instrument": name, "direction": fvg.direction, "status": status, "R": R, "peak": peak})
    return trades


def summarize(trades):
    n = len(trades)
    if n == 0:
        return {"n": 0}
    tp = sum(1 for t in trades if t["status"] == "TP")
    sl = sum(1 for t in trades if t["status"] == "SL")
    timeout = sum(1 for t in trades if t["status"] == "TIMEOUT")
    sum_r = sum(t["R"] for t in trades)
    avg_r = sum_r / n
    wr = tp / (tp + sl) * 100 if (tp + sl) > 0 else 0
    # Profit factor
    wins = [t["R"] for t in trades if t["R"] > 0]
    losses = [abs(t["R"]) for t in trades if t["R"] < 0]
    pf = (sum(wins) / sum(losses)) if losses and sum(losses) > 0 else 999.9
    return {"n": n, "tp": tp, "sl": sl, "timeout": timeout,
            "wr": round(wr, 1), "sum_r": round(sum_r, 2), "avg_r": round(avg_r, 3), "pf": round(pf, 2)}


def main():
    print("[BACKTEST] Starting 30-day FVG backtest on 22 forex pairs...")
    forex = [(name, entry) for name, entry in INSTRUMENTS.items() if entry[1] == "forex"]
    print(f"Instruments: {len(forex)}")

    # Scarify candles (30d of 1h = 720 max, yfinance gives ~165 on 7d)
    # Use 30d period
    candles_by_inst = {}
    for name, (ticker, _, _) in forex:
        c = fetch_candles(ticker, period="30d", interval="1h")
        if c:
            candles_by_inst[name] = c
            print(f"  {name}: {len(c)} candles")
    print(f"\nTotal candles loaded: {sum(len(c) for c in candles_by_inst.values())}")

    # Parameter sweep
    size_vals = [0.00015, 0.00025, 0.0003, 0.0005]
    body_vals = [0.3, 0.4, 0.5, 0.6]
    sl_vals   = [0.05, 0.10, 0.20]

    results = []
    total = len(size_vals) * len(body_vals) * len(sl_vals)
    i = 0
    for s, b, sl_buf in itertools.product(size_vals, body_vals, sl_vals):
        i += 1
        print(f"[{i}/{total}] size={s*100:.3f}% body={b} sl_buf={sl_buf} ...", end=" ", flush=True)
        trades = run_backtest(candles_by_inst, s, b, sl_buf)
        r = summarize(trades)
        r.update({"size_rel": s, "body_ratio": b, "sl_buffer_rel": sl_buf})
        results.append(r)
        print(f"trades={r['n']} WR={r['wr']}% sumR={r['sum_r']} PF={r['pf']}")

    # Сортируем по Profit Factor (trades >= 30)
    valid = [r for r in results if r["n"] >= 30]
    valid.sort(key=lambda x: (-x["pf"], -x["sum_r"]))

    print("\n" + "=" * 100)
    print("TOP 10 by Profit Factor (min 30 trades):")
    print("=" * 100)
    print(f"{'#':>2} {'size%':>7} {'body':>5} {'sl_buf':>7} {'N':>4} {'TP':>4} {'SL':>4} {'TMO':>4} {'WR%':>5} {'SumR':>7} {'AvgR':>7} {'PF':>6}")
    for i, r in enumerate(valid[:10], 1):
        print(f"{i:>2} {r['size_rel']*100:>7.3f} {r['body_ratio']:>5} {r['sl_buffer_rel']:>7.2f} {r['n']:>4} {r['tp']:>4} {r['sl']:>4} {r['timeout']:>4} {r['wr']:>5.1f} {r['sum_r']:>7.2f} {r['avg_r']:>7.3f} {r['pf']:>6.2f}")

    print("\n" + "=" * 100)
    print("BY VOLUME (top 10 by N trades):")
    print("=" * 100)
    by_vol = sorted(results, key=lambda x: -x["n"])[:10]
    for i, r in enumerate(by_vol, 1):
        print(f"{i:>2} {r['size_rel']*100:>7.3f} {r['body_ratio']:>5} {r['sl_buffer_rel']:>7.2f} {r['n']:>4} {r['tp']:>4} {r['sl']:>4} {r['timeout']:>4} {r['wr']:>5.1f} {r['sum_r']:>7.2f} {r['avg_r']:>7.3f} {r['pf']:>6.2f}")

    # Current config для сравнения
    cur = next((r for r in results if r["size_rel"] == 0.00015 and r["body_ratio"] == 0.4 and r["sl_buffer_rel"] == 0.05), None)
    if cur:
        print(f"\n[CURRENT CONFIG] size=0.015% body=0.4 sl_buf=0.05")
        print(f"  N={cur['n']} TP={cur['tp']} SL={cur['sl']} WR={cur['wr']}% SumR={cur['sum_r']} AvgR={cur['avg_r']} PF={cur['pf']}")

    # Save to JSON
    import json
    with open("backtest_fvg_30d_results.json", "w") as f:
        json.dump({
            "generated_at": datetime.utcnow().isoformat(),
            "period": "30d",
            "instruments": list(candles_by_inst.keys()),
            "results": results,
        }, f, indent=2)
    print(f"\nSaved: backtest_fvg_30d_results.json ({len(results)} combos)")


if __name__ == "__main__":
    main()
