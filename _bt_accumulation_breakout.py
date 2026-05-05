"""ACCUMULATION → ANOMALOUS CANDLE breakout detection.

Hypothesis: после периода низкой волатильности и сжатого объёма
(consolidation/accumulation) первая аномальная свеча (vol spike + большое
тело) сигнализирует начало markup phase = institutions finished positioning.

Algorithm:
1. ACCUMULATION detect (look-back N=12 bars):
   - BB width / price < SQUEEZE_BB% (squeeze condition)
   - Avg volume в окне < MA20 (cooled volume)
   - Range high-low < ACCUM_RANGE% (tight range)
2. BREAKOUT candle (current):
   - Volume >= K × accumulation_avg_volume (5x, 8x, 12x)
   - Body >= BODY_ATR × ATR (large body)
   - Close beyond accumulation range edge
3. Entry next candle open
4. SL: opposite side of accum range
5. TP: 1.5R / 2R / 3R fixed

Slice by:
- Squeeze tightness
- Volume multiplier
- Body size threshold
- Direction (LONG/SHORT)
"""
import json
import sys
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')


def compute_atr(candles, period=14):
    """Returns list of ATR values aligned to candles[0..n-1]."""
    if len(candles) < period + 1:
        return [None] * len(candles)
    trs = [candles[0]['h'] - candles[0]['l']]
    for i in range(1, len(candles)):
        prev_c = candles[i - 1]['c']
        h, l = candles[i]['h'], candles[i]['l']
        trs.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
    out = [None] * (period - 1)
    cur = sum(trs[:period]) / period
    out.append(cur)
    for i in range(period, len(candles)):
        cur = (cur * (period - 1) + trs[i]) / period
        out.append(cur)
    return out


def detect_accumulation(candles, end_idx, lookback=12,
                         max_bb_pct=4.0, max_range_pct=5.0):
    """Returns dict with accumulation stats или None если нет accumulation.
    end_idx = индекс ПОСЛЕДНЕЙ свечи накопления (breakout = end_idx+1)."""
    if end_idx < lookback + 20:
        return None
    window = candles[end_idx - lookback + 1:end_idx + 1]
    if len(window) < lookback:
        return None

    closes = [c['c'] for c in window]
    highs = [c['h'] for c in window]
    lows = [c['l'] for c in window]
    vols = [c['v'] for c in window]

    # 1) Range check: high-low diff
    range_max = max(highs)
    range_min = min(lows)
    if range_min <= 0:
        return None
    range_pct = (range_max - range_min) / range_min * 100
    if range_pct > max_range_pct:
        return None  # range слишком широкий

    # 2) BB width: stddev of close / mean close
    mean_close = sum(closes) / lookback
    var = sum((c - mean_close) ** 2 for c in closes) / lookback
    std = var ** 0.5
    bb_pct = std / mean_close * 100 * 4  # 2 std * 2 (upper + lower)
    if bb_pct > max_bb_pct:
        return None  # not enough squeeze

    # 3) Volume check: avg в окне меньше чем MA20 предыдущих
    if end_idx < 20:
        avg_vol_window = sum(vols) / lookback
    else:
        prev20 = candles[end_idx - 20:end_idx]
        avg_prev = sum(c['v'] for c in prev20) / 20
        avg_vol_window = sum(vols) / lookback
        if avg_vol_window > avg_prev * 1.0:
            return None  # объём не сжат

    return {
        'range_high': range_max,
        'range_low': range_min,
        'range_pct': range_pct,
        'bb_pct': bb_pct,
        'avg_vol': avg_vol_window,
        'mean_close': mean_close,
    }


def simulate_trade(candles, entry_idx, entry_price, sl_price, tp_price,
                    direction, max_bars=72):
    end = min(len(candles), entry_idx + max_bars + 1)
    for j in range(entry_idx + 1, end):
        c = candles[j]
        if direction == 'LONG':
            if c['l'] <= sl_price:
                return ('LOSS', sl_price)
            if c['h'] >= tp_price:
                return ('WIN', tp_price)
        else:
            if c['h'] >= sl_price:
                return ('LOSS', sl_price)
            if c['l'] <= tp_price:
                return ('WIN', tp_price)
    last = candles[end - 1]
    return ('OPEN', last['c'])


def run_backtest(klines_cache,
                  lookback=12, max_bb_pct=4.0, max_range_pct=5.0,
                  vol_mult=5.0, body_atr=1.0, tp_r=2.0):
    """Find accumulation→breakout setups, simulate trades."""
    results = defaultdict(list)
    accum_found = 0
    setups_found = 0

    for pair, candles in klines_cache.items():
        if len(candles) < 60:
            continue
        atrs = compute_atr(candles, 14)

        for i in range(30, len(candles) - 5):
            # Detect accumulation ending at i-1 (i = potential breakout)
            accum = detect_accumulation(
                candles, i - 1,
                lookback=lookback,
                max_bb_pct=max_bb_pct,
                max_range_pct=max_range_pct,
            )
            if not accum:
                continue
            accum_found += 1

            # Check breakout candle at i
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

            # Direction: breakout side
            if c['c'] > accum['range_high']:
                direction = 'LONG'
                sl_price = accum['range_low']
            elif c['c'] < accum['range_low']:
                direction = 'SHORT'
                sl_price = accum['range_high']
            else:
                continue  # close inside range, no breakout

            setups_found += 1

            # Entry next candle open
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

            # Sanity: risk не должен превышать 5%
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
                'body_atr': body / atr, 'risk_pct': risk_pct,
                'outcome': outcome, 'r': r,
                'range_pct': accum['range_pct'],
                'bb_pct': accum['bb_pct'],
            })

    return results, accum_found, setups_found


def print_stats(results, label, tp_r):
    print(f'\n=== {label} (TP={tp_r}R) ===')
    print(f'{"Dir":>6} {"N":>5} {"WR":>7} {"E":>8} {"BE":>6}')
    print('-' * 38)
    be_wr = 1 / (1 + tp_r) * 100
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


# ── MAIN ──────────────────────────────────────────────────────────────
print('Loading klines cache...')
with open('_bt_klines_cache.json') as f:
    klines_cache = json.load(f)
print(f'Loaded {len(klines_cache)} pairs\n')

# Test multiple configurations
configs = [
    # (lookback, max_bb_pct, vol_mult, body_atr, tp_r, label)
    (12, 4.0, 5.0,  1.0, 2.0, 'BASE: 12bar lookback, BB<4%, vol≥5x, body≥1ATR, TP=2R'),
    (12, 3.0, 5.0,  1.0, 2.0, 'TIGHTER squeeze BB<3%'),
    (12, 4.0, 8.0,  1.0, 2.0, 'STRONGER vol≥8x'),
    (12, 4.0, 10.0, 1.5, 2.0, 'BIG candle: vol≥10x + body≥1.5ATR'),
    (12, 4.0, 5.0,  1.0, 1.5, 'BASE TP=1.5R'),
    (12, 4.0, 5.0,  1.0, 3.0, 'BASE TP=3R'),
    (8,  4.0, 5.0,  1.0, 2.0, 'SHORTER lookback 8'),
    (24, 4.0, 5.0,  1.0, 2.0, 'LONGER lookback 24'),
    (12, 5.0, 3.0,  0.7, 2.0, 'LOOSER all (more setups)'),
]

for cfg in configs:
    lb, bb, vm, ba, tp, label = cfg
    print('\n' + '#' * 70)
    print(f'# {label}')
    print('#' * 70)
    res, accum_n, setup_n = run_backtest(
        klines_cache,
        lookback=lb, max_bb_pct=bb,
        vol_mult=vm, body_atr=ba, tp_r=tp,
    )
    print(f'  Accum periods found: {accum_n} | Breakout setups: {setup_n}')
    print_stats(res, label, tp)

print('\n=== DONE ===')
