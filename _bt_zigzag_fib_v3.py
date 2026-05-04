"""V3 — variable TP at fixed R-multiple (1R, 1.5R, 2R)."""
import json
import sys
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
from _bt_zigzag_fib import detect_zigzag, find_fib_entry, simulate_trade
from _bt_zigzag_fib_v2 import compute_st_trend, vol_ratio_at


def run_fixed_r_backtest(klines_cache, min_swing_pct=5.0,
                          fib_levels=[0.618, 0.786],
                          tp_r=1.5,
                          require_st=True, require_vol=True, vol_min=2.0):
    """Variable TP at fixed R."""
    results = defaultdict(lambda: defaultdict(list))

    for pair, candles in klines_cache.items():
        if len(candles) < 50:
            continue
        swings = detect_zigzag(candles, min_pct=min_swing_pct)
        if len(swings) < 2:
            continue
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

                if require_st and st_trend:
                    cur_trend = st_trend[entry_idx]
                    if trade_dir == 'LONG' and cur_trend != 1:
                        continue
                    if trade_dir == 'SHORT' and cur_trend != -1:
                        continue

                if require_vol:
                    vr = vol_ratio_at(candles, entry_idx, period=20)
                    if vr is None or vr < vol_min:
                        continue

                if trade_dir == 'LONG':
                    risk = entry_price - sl_price
                    if risk <= 0:
                        continue
                    tp_price = entry_price + risk * tp_r
                else:
                    risk = sl_price - entry_price
                    if risk <= 0:
                        continue
                    tp_price = entry_price - risk * tp_r

                outcome, exit_price, bars = simulate_trade(
                    candles, entry_idx, entry_price, sl_price, tp_price,
                    trade_dir, max_bars=72,
                )
                if outcome == 'WIN':
                    r = tp_r
                elif outcome == 'LOSS':
                    r = -1.0
                else:
                    if trade_dir == 'LONG':
                        r = (exit_price - entry_price) / risk
                    else:
                        r = (entry_price - exit_price) / risk

                results[fib][trade_dir].append({
                    'pair': pair, 'leg_pct': leg_pct, 'rr': tp_r,
                    'outcome': outcome, 'r': r, 'bars': bars,
                })
    return results


def print_stats(results, label, tp_r):
    print(f'\n=== {label} (TP={tp_r}R) ===')
    print(f'{"Fib":>7} {"Dir":>5} {"N":>5} {"WR":>7} {"E":>8}')
    for fib in sorted(results.keys()):
        for direction in ('LONG', 'SHORT'):
            trades = results[fib][direction]
            if not trades:
                continue
            n = len(trades)
            wins = sum(1 for t in trades if t['outcome'] == 'WIN')
            wr = wins / n * 100
            avg_r = sum(t['r'] for t in trades) / n
            print(f'{fib:>7.3f} {direction:>5} {n:>5} {wr:>6.1f}% {avg_r:>+6.2f}R')
    print('Combined:')
    for fib in sorted(results.keys()):
        all_t = results[fib]['LONG'] + results[fib]['SHORT']
        if not all_t:
            continue
        n = len(all_t)
        wins = sum(1 for t in all_t if t['outcome'] == 'WIN')
        wr = wins / n * 100
        avg_r = sum(t['r'] for t in all_t) / n
        # Breakeven WR: WR_BE = 1/(1+tp_r) for fixed loss=-1R
        be_wr = 1 / (1 + tp_r) * 100
        edge = wr - be_wr
        print(f'  fib={fib:.3f} N={n:>4} WR={wr:.1f}% (BE={be_wr:.1f}%, '
              f'edge={edge:+.1f}%) E={avg_r:+.2f}R')


print('Loading klines cache...')
with open('_bt_klines_cache.json') as f:
    klines_cache = json.load(f)
print(f'Loaded {len(klines_cache)} pairs\n')

# Try multiple TP R-multiples
for tp_r in [0.7, 1.0, 1.5, 2.0]:
    print('\n' + '#' * 70)
    print(f'# Fixed TP = {tp_r}R, fib=0.618/0.786, ST+Vol 2x')
    print('#' * 70)
    res = run_fixed_r_backtest(
        klines_cache, min_swing_pct=5.0,
        fib_levels=[0.618, 0.786],
        tp_r=tp_r, require_st=True, require_vol=True, vol_min=2.0,
    )
    print_stats(res, f'Fixed TP={tp_r}R', tp_r)

print('\n\n=== Without ST filter (just Vol 3x + fib 0.5/0.618) ===')
for tp_r in [0.7, 1.0, 1.5]:
    print(f'\n--- TP = {tp_r}R ---')
    res = run_fixed_r_backtest(
        klines_cache, min_swing_pct=5.0,
        fib_levels=[0.5, 0.618],
        tp_r=tp_r, require_st=False, require_vol=True, vol_min=3.0,
    )
    print_stats(res, f'Vol3x TP={tp_r}R', tp_r)

print('\n=== DONE ===')
