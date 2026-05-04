"""ZigZag + Fib backtest V2 — с фильтрами.

V1 показал что голый fib bounce убыточен (E = -0.08...-0.18R).
Тестируем фильтры:
1. ST trend filter — entry только если 1h ST trend совпадает с trade_dir
2. Volume confirmation — на rejection candle volume > MA20 * 1.5 (echo Volume Surge)
3. Combined: ST + Volume

Если хоть один фильтр поднимает E в плюс — есть рабочая стратегия.
"""
import json
import sys
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')

# Reuse ZigZag from v1
sys.path.insert(0, '.')
from _bt_zigzag_fib import (
    detect_zigzag, fib_zone, simulate_trade, find_fib_entry,
    FIB_LEVELS, FIB_EXT_TP,
)


# ── ATR-based SuperTrend (simplified, period=10, mult=3) ──────────────
def compute_atr(candles, period=14):
    """Returns list of ATR values, len = len(candles)."""
    if len(candles) < period + 1:
        return [None] * len(candles)
    trs = [candles[0]['h'] - candles[0]['l']]
    for i in range(1, len(candles)):
        prev_c = candles[i - 1]['c']
        h, l = candles[i]['h'], candles[i]['l']
        trs.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
    atrs = [None] * (period - 1)
    cur = sum(trs[:period]) / period
    atrs.append(cur)
    for i in range(period, len(candles)):
        cur = (cur * (period - 1) + trs[i]) / period
        atrs.append(cur)
    return atrs


def compute_st_trend(candles, period=10, mult=3.0):
    """Simplified SuperTrend: returns list of -1/+1 trend per candle.
    +1 = up trend, -1 = down."""
    if len(candles) < period + 1:
        return [0] * len(candles)
    atrs = compute_atr(candles, period)
    trends = [0]
    upper_band = [None]
    lower_band = [None]
    for i in range(1, len(candles)):
        if atrs[i] is None:
            trends.append(0)
            upper_band.append(None)
            lower_band.append(None)
            continue
        c = candles[i]
        hl2 = (c['h'] + c['l']) / 2
        ub = hl2 + mult * atrs[i]
        lb = hl2 - mult * atrs[i]
        # Adjust bands relative to previous
        prev_close = candles[i - 1]['c']
        if upper_band[-1] is not None:
            if ub < upper_band[-1] or prev_close > upper_band[-1]:
                pass
            else:
                ub = upper_band[-1]
            if lb > lower_band[-1] or prev_close < lower_band[-1]:
                pass
            else:
                lb = lower_band[-1]
        # Determine trend
        prev_trend = trends[-1] or 1
        if prev_trend == 1 and c['c'] < lb:
            trend = -1
        elif prev_trend == -1 and c['c'] > ub:
            trend = 1
        else:
            trend = prev_trend
        trends.append(trend)
        upper_band.append(ub)
        lower_band.append(lb)
    return trends


# ── Volume MA helper ──────────────────────────────────────────────────
def vol_ratio_at(candles, idx, period=20):
    """Returns vol[idx] / mean(vol[idx-period:idx])."""
    if idx < period:
        return None
    window = candles[idx - period:idx]
    avg = sum(c['v'] for c in window) / period
    if avg <= 0:
        return None
    return candles[idx]['v'] / avg


# ── Main backtest with filters ────────────────────────────────────────
def run_filtered_backtest(klines_cache, min_swing_pct=5.0,
                          fib_levels=FIB_LEVELS,
                          require_st=False, require_vol=False, vol_min=1.5):
    """Same as v1 but applies filters at entry."""
    results = defaultdict(lambda: defaultdict(list))

    for pair, candles in klines_cache.items():
        if len(candles) < 50:
            continue
        swings = detect_zigzag(candles, min_pct=min_swing_pct)
        if len(swings) < 2:
            continue
        # Pre-compute ST trend if needed
        st_trend = compute_st_trend(candles) if require_st else None

        for i in range(1, len(swings)):
            a_idx, _, a_price, _ = swings[i - 1]
            b_idx, _, b_price, b_dir = swings[i]
            leg_pct = abs(b_price - a_price) / a_price * 100
            if leg_pct < min_swing_pct:
                continue

            if b_dir == 'H':
                trade_dir = 'LONG'
                sl_price = a_price
            else:
                trade_dir = 'SHORT'
                sl_price = a_price

            for fib in fib_levels:
                entry = find_fib_entry(candles, a_idx, a_price, b_idx,
                                        b_price, b_dir, fib)
                if entry is None:
                    continue
                entry_idx, entry_price = entry

                # ── ST filter ──
                if require_st and st_trend is not None:
                    cur_trend = st_trend[entry_idx]
                    if trade_dir == 'LONG' and cur_trend != 1:
                        continue
                    if trade_dir == 'SHORT' and cur_trend != -1:
                        continue

                # ── Volume filter ──
                if require_vol:
                    vr = vol_ratio_at(candles, entry_idx, period=20)
                    if vr is None or vr < vol_min:
                        continue

                # TP/SL/RR
                if trade_dir == 'LONG':
                    leg_size = b_price - a_price
                    tp_price = b_price + leg_size * (FIB_EXT_TP - 1.0)
                    if not (tp_price > entry_price > sl_price):
                        continue
                else:
                    leg_size = a_price - b_price
                    tp_price = b_price - leg_size * (FIB_EXT_TP - 1.0)
                    if not (sl_price > entry_price > tp_price):
                        continue

                if trade_dir == 'LONG':
                    risk = entry_price - sl_price
                    reward = tp_price - entry_price
                else:
                    risk = sl_price - entry_price
                    reward = entry_price - tp_price
                if risk <= 0 or reward <= 0:
                    continue
                rr = reward / risk
                if rr < 0.3 or rr > 10:
                    continue

                outcome, exit_price, bars = simulate_trade(
                    candles, entry_idx, entry_price, sl_price, tp_price,
                    trade_dir, max_bars=72,
                )
                if outcome == 'WIN':
                    r = rr
                elif outcome == 'LOSS':
                    r = -1.0
                else:
                    if trade_dir == 'LONG':
                        r = (exit_price - entry_price) / risk
                    else:
                        r = (entry_price - exit_price) / risk

                results[fib][trade_dir].append({
                    'pair': pair, 'leg_pct': leg_pct, 'rr': rr,
                    'outcome': outcome, 'r': r, 'bars': bars,
                })
    return results


def print_stats(results, label):
    print(f'\n{"="*70}\n{label}\n{"="*70}')
    print(f'{"Fib":>7} {"Dir":>5} {"N":>5} {"WR":>7} {"E":>8} {"AvgRR":>7}')
    print('-' * 50)
    for fib in sorted(results.keys()):
        for direction in ('LONG', 'SHORT'):
            trades = results[fib][direction]
            if not trades:
                continue
            n = len(trades)
            wins = sum(1 for t in trades if t['outcome'] == 'WIN')
            wr = wins / n * 100
            avg_r = sum(t['r'] for t in trades) / n
            avg_rr = sum(t['rr'] for t in trades) / n
            print(f'{fib:>7.3f} {direction:>5} {n:>5} '
                  f'{wr:>6.1f}% {avg_r:>+6.2f}R {avg_rr:>6.2f}')

    print('\nCombined LONG+SHORT:')
    print(f'{"Fib":>7} {"N":>5} {"WR":>7} {"E":>8}')
    print('-' * 35)
    for fib in sorted(results.keys()):
        all_trades = results[fib]['LONG'] + results[fib]['SHORT']
        if not all_trades:
            continue
        n = len(all_trades)
        wins = sum(1 for t in all_trades if t['outcome'] == 'WIN')
        wr = wins / n * 100
        avg_r = sum(t['r'] for t in all_trades) / n
        print(f'{fib:>7.3f} {n:>5} {wr:>6.1f}% {avg_r:>+6.2f}R')


# ── MAIN ──────────────────────────────────────────────────────────────
print('Loading klines cache...')
with open('_bt_klines_cache.json') as f:
    klines_cache = json.load(f)
print(f'Loaded {len(klines_cache)} pairs\n')

SWING_PCT = 5.0  # фокус на 5% — оптимальное значение по объёму данных

# Test 1: ST trend filter only
print('\n' + '#' * 70)
print(f'# TEST 1: ST trend filter (min_swing={SWING_PCT}%)')
print('#' * 70)
r1 = run_filtered_backtest(klines_cache, min_swing_pct=SWING_PCT,
                            require_st=True)
print_stats(r1, f'ST filter only')

# Test 2: Volume filter only (vol_ratio > 1.5)
print('\n' + '#' * 70)
print(f'# TEST 2: Volume filter (>=1.5x MA20)')
print('#' * 70)
r2 = run_filtered_backtest(klines_cache, min_swing_pct=SWING_PCT,
                            require_vol=True, vol_min=1.5)
print_stats(r2, 'Vol >= 1.5x')

# Test 3: ST + Volume combined
print('\n' + '#' * 70)
print(f'# TEST 3: ST + Volume (vol >= 1.5x)')
print('#' * 70)
r3 = run_filtered_backtest(klines_cache, min_swing_pct=SWING_PCT,
                            require_st=True, require_vol=True, vol_min=1.5)
print_stats(r3, 'ST + Vol >= 1.5x')

# Test 4: ST + Volume HIGH (vol_ratio > 3.0 — same as Volume Surge edge)
print('\n' + '#' * 70)
print(f'# TEST 4: ST + Volume Surge (>=3.0x MA20)')
print('#' * 70)
r4 = run_filtered_backtest(klines_cache, min_swing_pct=SWING_PCT,
                            require_st=True, require_vol=True, vol_min=3.0)
print_stats(r4, 'ST + Vol >= 3.0x')

# Test 5: deep retracement only (fib 0.618 + 0.786) with ST filter — best risk-reward zone
print('\n' + '#' * 70)
print(f'# TEST 5: Deep fib (0.618/0.786) + ST + Vol >= 2.0x')
print('#' * 70)
r5 = run_filtered_backtest(klines_cache, min_swing_pct=SWING_PCT,
                            fib_levels=[0.618, 0.786],
                            require_st=True, require_vol=True, vol_min=2.0)
print_stats(r5, 'Deep+ST+Vol2x')

print('\n=== DONE ===')
