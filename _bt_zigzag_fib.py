"""ZigZag + Fibonacci retracement backtest.

Hypothesis: после major swing (>=N%) откат к Fib level (0.382/0.5/0.618/0.786)
с rejection candle = entry в направлении продолжения тренда.

Setup:
1. ZigZag detection — swing points where alternating high/low with min swing pct
2. After completed leg → wait for retracement
3. Entry: when price tags fib level AND closes back in trend direction
4. SL: 100% retracement (swing extreme)
5. TP: 1.272 extension (or 1R/2R alternatives)

Output: WR, expectancy R per fib level, sliced by direction & swing size.
"""
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone

sys.stdout.reconfigure(encoding='utf-8')


# ── ZigZag detection ──────────────────────────────────────────────────
def detect_zigzag(candles, min_pct=3.0):
    """Returns list of swing points: [(idx, ts, price, direction), ...]
    direction: 'H' for high pivot, 'L' for low pivot.
    Uses streaming algo: track current extreme until reversal of min_pct."""
    if len(candles) < 5:
        return []
    swings = []
    # Initial direction guess: compare first 2 candles
    cur_dir = 'UP' if candles[1]['c'] > candles[0]['c'] else 'DOWN'
    cur_extreme_idx = 0
    cur_extreme_price = candles[0]['h'] if cur_dir == 'UP' else candles[0]['l']

    for i in range(1, len(candles)):
        c = candles[i]
        if cur_dir == 'UP':
            # update high if new max
            if c['h'] > cur_extreme_price:
                cur_extreme_price = c['h']
                cur_extreme_idx = i
            else:
                # check reversal: low has dropped >=min_pct from cur_extreme
                drop_pct = (cur_extreme_price - c['l']) / cur_extreme_price * 100
                if drop_pct >= min_pct:
                    # confirm UP swing (high pivot) at cur_extreme_idx
                    swings.append((cur_extreme_idx,
                                   candles[cur_extreme_idx]['t'],
                                   cur_extreme_price, 'H'))
                    # switch to DOWN, current low is new extreme
                    cur_dir = 'DOWN'
                    cur_extreme_idx = i
                    cur_extreme_price = c['l']
        else:  # DOWN
            if c['l'] < cur_extreme_price:
                cur_extreme_price = c['l']
                cur_extreme_idx = i
            else:
                rise_pct = (c['h'] - cur_extreme_price) / cur_extreme_price * 100
                if rise_pct >= min_pct:
                    swings.append((cur_extreme_idx,
                                   candles[cur_extreme_idx]['t'],
                                   cur_extreme_price, 'L'))
                    cur_dir = 'UP'
                    cur_extreme_idx = i
                    cur_extreme_price = c['h']
    return swings


# ── Fibonacci levels for a leg ────────────────────────────────────────
FIB_LEVELS = [0.382, 0.5, 0.618, 0.786]
FIB_EXT_TP = 1.272


def fib_zone(swing_low, swing_high, pct):
    """Returns retracement price for given fib pct from a swing range."""
    return swing_high - (swing_high - swing_low) * pct


# ── Backtest single trade ─────────────────────────────────────────────
def simulate_trade(candles, entry_idx, entry_price, sl_price, tp_price,
                   direction, max_bars=72):
    """Simulate forward bar-by-bar. Returns (outcome, exit_price, bars_held).
    outcome: 'WIN' / 'LOSS' / 'OPEN'."""
    end = min(len(candles), entry_idx + max_bars + 1)
    for j in range(entry_idx + 1, end):
        c = candles[j]
        if direction == 'LONG':
            if c['l'] <= sl_price:
                return ('LOSS', sl_price, j - entry_idx)
            if c['h'] >= tp_price:
                return ('WIN', tp_price, j - entry_idx)
        else:  # SHORT
            if c['h'] >= sl_price:
                return ('LOSS', sl_price, j - entry_idx)
            if c['l'] <= tp_price:
                return ('WIN', tp_price, j - entry_idx)
    last = candles[end - 1]
    return ('OPEN', last['c'], end - 1 - entry_idx)


# ── Find entry after swing complete ───────────────────────────────────
def find_fib_entry(candles, swing_a_idx, swing_a_price, swing_b_idx,
                   swing_b_price, b_dir, fib_pct, max_wait=48):
    """After leg from A (swing_a) → B (swing_b) is complete,
    wait up to max_wait bars for price to enter fib retracement zone.
    Entry trigger: price reaches fib level AND closes opposite (rejection).

    b_dir: 'H' means UP leg (LONG continuation expected after retracement)
           'L' means DOWN leg (SHORT continuation expected)

    Returns (entry_idx, entry_price) or None."""
    if b_dir == 'H':
        # UP leg: A=low, B=high. Retracement DOWN to fib.
        # Entry when low touches fib AND close > fib (bounce up)
        fib_price = fib_zone(swing_a_price, swing_b_price, fib_pct)
        end = min(len(candles), swing_b_idx + max_wait + 1)
        for j in range(swing_b_idx + 1, end):
            c = candles[j]
            if c['l'] <= fib_price and c['c'] > fib_price:
                return (j, c['c'])
        return None
    else:
        # DOWN leg: A=high, B=low. Retracement UP to fib.
        # fib_price interpreted as price after climbing pct of (A-B) from B
        fib_price = swing_b_price + (swing_a_price - swing_b_price) * fib_pct
        end = min(len(candles), swing_b_idx + max_wait + 1)
        for j in range(swing_b_idx + 1, end):
            c = candles[j]
            if c['h'] >= fib_price and c['c'] < fib_price:
                return (j, c['c'])
        return None


# ── Main backtest ─────────────────────────────────────────────────────
def run_backtest(klines_cache, min_swing_pct=3.0, fib_levels=FIB_LEVELS):
    """Per pair: detect zigzags, for each completed leg + each fib level,
    simulate trade. Returns dict of stats."""
    # results[fib_pct][direction] = list of trade dicts
    results = defaultdict(lambda: defaultdict(list))

    for pair, candles in klines_cache.items():
        if len(candles) < 50:
            continue
        swings = detect_zigzag(candles, min_pct=min_swing_pct)
        if len(swings) < 2:
            continue

        # Iterate through pairs of consecutive swings (leg = swing[i-1] → swing[i])
        for i in range(1, len(swings)):
            a_idx, _, a_price, _ = swings[i - 1]
            b_idx, _, b_price, b_dir = swings[i]

            # Skip if leg too small (probably noise)
            leg_pct = abs(b_price - a_price) / a_price * 100
            if leg_pct < min_swing_pct:
                continue

            # b_dir tells us where the leg ended:
            #   'H' = UP leg complete (low → high), expect LONG continuation
            #   'L' = DOWN leg complete (high → low), expect SHORT continuation
            if b_dir == 'H':
                trade_dir = 'LONG'
                # SL: below swing low (100% retracement)
                sl_price = a_price
            else:
                trade_dir = 'SHORT'
                sl_price = a_price  # = swing high

            for fib in fib_levels:
                entry = find_fib_entry(candles, a_idx, a_price, b_idx,
                                        b_price, b_dir, fib)
                if entry is None:
                    continue
                entry_idx, entry_price = entry

                # Compute TP: 1.272 extension from swing range
                if trade_dir == 'LONG':
                    leg_size = b_price - a_price
                    tp_price = b_price + leg_size * (FIB_EXT_TP - 1.0)
                    # Sanity: TP > entry, SL < entry
                    if not (tp_price > entry_price > sl_price):
                        continue
                else:
                    leg_size = a_price - b_price
                    tp_price = b_price - leg_size * (FIB_EXT_TP - 1.0)
                    if not (sl_price > entry_price > tp_price):
                        continue

                # R-multiple
                if trade_dir == 'LONG':
                    risk = entry_price - sl_price
                    reward = tp_price - entry_price
                else:
                    risk = sl_price - entry_price
                    reward = entry_price - tp_price
                if risk <= 0 or reward <= 0:
                    continue
                rr = reward / risk

                # Skip if RR is silly (too low or too high)
                if rr < 0.3 or rr > 10:
                    continue

                outcome, exit_price, bars = simulate_trade(
                    candles, entry_idx, entry_price, sl_price, tp_price,
                    trade_dir, max_bars=72,
                )

                # Realized R
                if outcome == 'WIN':
                    r = rr
                elif outcome == 'LOSS':
                    r = -1.0
                else:  # OPEN
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
    print(f'{"Fib":>7} {"Dir":>5} {"N":>5} {"WR":>7} {"AvgR":>7} {"E":>7} {"AvgRR":>7}')
    print('-' * 56)
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
            # Expectancy = WR * avg_win_R + LR * (-1)
            expectancy = avg_r  # already weighted by outcome distribution
            print(f'{fib:>7.3f} {direction:>5} {n:>5} '
                  f'{wr:>6.1f}% {avg_r:>+6.2f}R {expectancy:>+6.2f}R '
                  f'{avg_rr:>6.2f}')

    # Combined per fib (LONG+SHORT)
    print('\nCombined LONG+SHORT:')
    print(f'{"Fib":>7} {"N":>5} {"WR":>7} {"AvgR":>7} {"E":>7}')
    print('-' * 40)
    for fib in sorted(results.keys()):
        all_trades = results[fib]['LONG'] + results[fib]['SHORT']
        if not all_trades:
            continue
        n = len(all_trades)
        wins = sum(1 for t in all_trades if t['outcome'] == 'WIN')
        wr = wins / n * 100
        avg_r = sum(t['r'] for t in all_trades) / n
        print(f'{fib:>7.3f} {n:>5} {wr:>6.1f}% {avg_r:>+6.2f}R {avg_r:>+6.2f}R')


# ── MAIN ──────────────────────────────────────────────────────────────
print('Loading klines cache...')
with open('_bt_klines_cache.json') as f:
    klines_cache = json.load(f)
print(f'Loaded {len(klines_cache)} pairs')

# Test multiple swing thresholds
for swing_pct in [3.0, 5.0, 7.0]:
    print(f'\n\n{"#"*70}')
    print(f'# SWING THRESHOLD: {swing_pct}%')
    print(f'{"#"*70}')
    results = run_backtest(klines_cache, min_swing_pct=swing_pct)
    print_stats(results, f'min_swing={swing_pct}%')

print('\n\n=== DONE ===')
