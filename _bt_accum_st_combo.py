"""Accumulation + ST trend filter — комбо тест.

Гипотеза: accumulation breakout + ST trend в ту же сторону = более чистый
сигнал. ST даст подтверждение что breakout реальный, не false squeeze.
"""
import json
import sys
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')

sys.path.insert(0, '.')
from _bt_accumulation_breakout import (
    detect_accumulation, simulate_trade, compute_atr,
)
from _bt_zigzag_fib_v2 import compute_st_trend


def run_combo(klines_cache, lookback=12, max_bb_pct=4.0,
               vol_mult=8.0, body_atr=1.0, tp_r=2.0):
    results = defaultdict(list)
    setups = 0
    setups_st_filtered = 0

    for pair, candles in klines_cache.items():
        if len(candles) < 60:
            continue
        atrs = compute_atr(candles, 14)
        st_trend = compute_st_trend(candles)

        for i in range(30, len(candles) - 5):
            accum = detect_accumulation(
                candles, i - 1, lookback=lookback,
                max_bb_pct=max_bb_pct, max_range_pct=5.0,
            )
            if not accum:
                continue

            c = candles[i]
            atr = atrs[i] if atrs[i] else 0
            if atr <= 0 or c['v'] <= 0 or accum['avg_vol'] <= 0:
                continue
            vol_ratio = c['v'] / accum['avg_vol']
            if vol_ratio < vol_mult:
                continue
            body = abs(c['c'] - c['o'])
            if body < body_atr * atr:
                continue

            if c['c'] > accum['range_high']:
                direction = 'LONG'
                sl_price = accum['range_low']
            elif c['c'] < accum['range_low']:
                direction = 'SHORT'
                sl_price = accum['range_high']
            else:
                continue
            setups += 1

            # ST FILTER: только в направлении trend
            cur_st = st_trend[i]
            if direction == 'LONG' and cur_st != 1:
                continue
            if direction == 'SHORT' and cur_st != -1:
                continue
            setups_st_filtered += 1

            if i + 1 >= len(candles):
                continue
            entry_price = candles[i + 1]['o']
            if direction == 'LONG':
                risk = entry_price - sl_price
                if risk <= 0:
                    continue
                tp_price = entry_price + risk * tp_r
            else:
                risk = sl_price - entry_price
                if risk <= 0:
                    continue
                tp_price = entry_price - risk * tp_r

            risk_pct = risk / entry_price * 100
            if risk_pct > 5.0:
                continue

            outcome, exit_price = simulate_trade(
                candles, i + 1, entry_price, sl_price, tp_price,
                direction, max_bars=72,
            )
            if outcome == 'WIN':
                r = tp_r
            elif outcome == 'LOSS':
                r = -1.0
            else:
                if direction == 'LONG':
                    r = (exit_price - entry_price) / risk
                else:
                    r = (entry_price - exit_price) / risk

            results[direction].append({
                'pair': pair, 'vol_ratio': vol_ratio,
                'outcome': outcome, 'r': r,
                'risk_pct': risk_pct,
            })
    return results, setups, setups_st_filtered


def print_stats(results, label, tp_r):
    print(f'\n=== {label} (TP={tp_r}R) ===')
    be_wr = 1 / (1 + tp_r) * 100
    print(f'{"Dir":>6} {"N":>5} {"WR":>7} {"E":>8} {"Edge":>7}')
    print('-' * 42)
    for direction in ('LONG', 'SHORT'):
        trades = results[direction]
        if not trades:
            continue
        n = len(trades)
        wins = sum(1 for t in trades if t['outcome'] == 'WIN')
        wr = wins / n * 100
        avg_r = sum(t['r'] for t in trades) / n
        edge = wr - be_wr
        print(f'{direction:>6} {n:>5} {wr:>6.1f}% {avg_r:>+6.2f}R '
              f'{edge:>+5.1f}%')

    all_t = results['LONG'] + results['SHORT']
    if all_t:
        n = len(all_t)
        wins = sum(1 for t in all_t if t['outcome'] == 'WIN')
        wr = wins / n * 100
        avg_r = sum(t['r'] for t in all_t) / n
        edge = wr - be_wr
        print(f'{"COMB":>6} {n:>5} {wr:>6.1f}% {avg_r:>+6.2f}R '
              f'{edge:>+5.1f}% (BE={be_wr:.0f}%)')


print('Loading klines cache...')
with open('_bt_klines_cache.json') as f:
    klines_cache = json.load(f)
print(f'Loaded {len(klines_cache)} pairs')

configs = [
    (12, 4.0, 5.0,  1.0, 2.0, 'ST+Accum vol≥5x TP=2R'),
    (12, 4.0, 8.0,  1.0, 2.0, 'ST+Accum vol≥8x TP=2R'),
    (12, 4.0, 10.0, 1.5, 2.0, 'ST+Accum vol≥10x body≥1.5 TP=2R'),
    (12, 4.0, 8.0,  1.0, 1.5, 'ST+Accum vol≥8x TP=1.5R'),
    (12, 4.0, 8.0,  1.0, 2.5, 'ST+Accum vol≥8x TP=2.5R'),
    (12, 5.0, 3.0,  0.7, 2.0, 'ST+Accum LOOSE'),
]
for cfg in configs:
    lb, bb, vm, ba, tp, label = cfg
    print('\n' + '#' * 70)
    print(f'# {label}')
    print('#' * 70)
    res, total_setups, filtered_setups = run_combo(
        klines_cache, lookback=lb, max_bb_pct=bb,
        vol_mult=vm, body_atr=ba, tp_r=tp,
    )
    drop_pct = (1 - filtered_setups / max(total_setups, 1)) * 100
    print(f'  Total: {total_setups}, after ST filter: {filtered_setups} '
          f'(dropped {drop_pct:.1f}%)')
    print_stats(res, label, tp)

print('\n=== DONE ===')
