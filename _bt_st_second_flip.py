"""SECOND ST FLIP backtest:

Pattern: pair had ST LONG flip → ST flipped DOWN (pullback) → ST flips
LONG AGAIN within window. Test if second LONG flip is more profitable.

Hypothesis: 1st flip = "may or may not work" (just trend change).
2nd flip after pullback = trend confirmed (higher low) = better edge.

Variations to test:
  - window between flips: 6h, 12h, 24h, 48h
  - position in sequence: 2nd, 3rd, 4th flip
  - tier filter: any / mtf / daily
  - target: +5%, +7%, +10%
"""
import json
import sys
from datetime import datetime, timedelta
from collections import defaultdict
from pymongo import MongoClient

sys.stdout.reconfigure(encoding='utf-8')

uri = 'mongodb+srv://jameswood_db_user:P4bJfNu63sMVnRPG@cluster0.rawzyzc.mongodb.net/?appName=Cluster0'
client = MongoClient(uri, serverSelectionTimeoutMS=15000)
db = client['tradium']

print('Loading klines cache...')
with open('_bt_klines_cache.json') as f:
    KLINES = json.load(f)
print(f'Pairs cached: {len(KLINES)}')


def find_idx(candles, ts_ms):
    for i, c in enumerate(candles):
        if c['t'] >= ts_ms: return i
    return None


def simulate_pct(candles, entry_idx, entry_price, direction,
                  target_pct, stop_pct, max_bars=72):
    if direction == 'LONG':
        tp = entry_price * (1 + target_pct/100)
        sl = entry_price * (1 - stop_pct/100)
    else:
        tp = entry_price * (1 - target_pct/100)
        sl = entry_price * (1 + stop_pct/100)
    end = min(len(candles), entry_idx + max_bars + 1)
    for j in range(entry_idx + 1, end):
        c = candles[j]
        if direction == 'LONG':
            if c['l'] <= sl: return 'LOSS', sl
            if c['h'] >= tp: return 'WIN', tp
        else:
            if c['h'] >= sl: return 'LOSS', sl
            if c['l'] <= tp: return 'WIN', tp
    return 'OPEN', candles[end-1]['c']


def main():
    since = datetime.utcnow() - timedelta(days=14)
    print(f'Loading ST flips since {since}...')

    # ВСЕ ST flips (не только LONG MTF/Daily — нужны и SHORT для определения
    # pullback'ов между LONG flips)
    flips = list(db.supertrend_signals.find({
        'flip_at': {'$gte': since}
    }).sort([('pair_norm', 1), ('flip_at', 1)]))
    print(f'Total ST flips: {len(flips)}')

    # Group by pair, dedup (same pair same flip_at = duplicates by tier)
    by_pair = defaultdict(list)
    for f in flips:
        pair = f.get('pair_norm')
        # Дедуп: один флип может быть в нескольких tier'ах. Берём один с
        # самым высоким tier (vip > mtf > daily) на данный flip_at.
        by_pair[pair].append(f)

    # Для каждой пары: получить уникальные flip_at + direction
    pair_sequences = {}
    for pair, fs in by_pair.items():
        seq = []
        seen_keys = set()
        for f in fs:
            k = (f['flip_at'], f['direction'])
            if k in seen_keys: continue
            seen_keys.add(k)
            seq.append({
                'flip_at': f['flip_at'],
                'direction': f['direction'],
                'tier': f.get('tier', 'mtf'),
                'entry': f.get('entry_price'),
                'sl': f.get('sl_price'),
            })
        seq.sort(key=lambda x: x['flip_at'])
        pair_sequences[pair] = seq

    # Найти все 2nd LONG flips: позиция #2+ в LONG-серии после reset
    # Pattern: ... LONG_1 ... SHORT_1 ... LONG_2 (within window) ...
    # Окно — макс. время от LONG_1 до LONG_2

    WINDOWS_HOURS = [12, 24, 48, 72, 168]  # max gap between two LONG flips
    TARGETS = [5, 7, 10]
    STOP_PCT = 5.0

    print()
    print('=' * 90)
    print('BASELINE: Все ST LONG flips (1st = любой)')
    print('=' * 90)
    print(f'{"Target":>7} {"N":>5} {"WR":>7} {"AvgRet":>9} {"BE":>5}')
    print('-' * 50)

    # Baseline: just count any LONG flip outcome
    all_first = []
    for pair, seq in pair_sequences.items():
        candles = KLINES.get(pair)
        if not candles: continue
        for f in seq:
            if f['direction'] != 'LONG': continue
            ts_ms = int(f['flip_at'].timestamp() * 1000)
            idx = find_idx(candles, ts_ms)
            if idx is None or idx >= len(candles) - 5: continue
            all_first.append({'pair': pair, 'idx': idx, 'candles': candles,
                              'tier': f['tier']})

    for tp in TARGETS:
        be_wr = STOP_PCT / (tp + STOP_PCT) * 100
        wins = losses = opens = 0
        rs = []
        for s in all_first:
            outcome, exit_price = simulate_pct(
                s['candles'], s['idx'], s['candles'][s['idx']]['c'],
                'LONG', tp, STOP_PCT, max_bars=72,
            )
            if outcome == 'WIN':
                wins += 1; rs.append(tp)
            elif outcome == 'LOSS':
                losses += 1; rs.append(-STOP_PCT)
            else:
                opens += 1
                ep = s['candles'][s['idx']]['c']
                rs.append((exit_price - ep) / ep * 100)
        n = len(rs)
        wr = wins/n*100 if n else 0
        avg = sum(rs)/n if rs else 0
        print(f'+{tp}%   {n:>5} {wr:>6.1f}% {avg:>+6.2f}% {be_wr:>4.0f}%')

    print()
    print('=' * 90)
    print('SECOND ST LONG flip (после первого LONG → SHORT → LONG)')
    print('=' * 90)
    print(f'  Pattern: LONG_1 → SHORT (any) → LONG_2 (entry)')
    print(f'  Variation: max gap (часов) от LONG_1 до LONG_2')
    print()

    # Find second flip occurrences
    for max_window_h in WINDOWS_HOURS:
        seconds = []
        for pair, seq in pair_sequences.items():
            candles = KLINES.get(pair)
            if not candles: continue
            # Iterate sequence — find LONG_2 patterns
            for i in range(2, len(seq)):
                cur = seq[i]
                if cur['direction'] != 'LONG': continue
                # Find prev SHORT (must exist)
                prev_short = None
                for j in range(i-1, -1, -1):
                    if seq[j]['direction'] == 'SHORT':
                        prev_short = j
                        break
                if prev_short is None: continue
                # Find LONG before that SHORT
                prev_long = None
                for j in range(prev_short-1, -1, -1):
                    if seq[j]['direction'] == 'LONG':
                        prev_long = j
                        break
                if prev_long is None: continue
                # Check window
                gap_h = (cur['flip_at'] - seq[prev_long]['flip_at']).total_seconds() / 3600
                if gap_h > max_window_h: continue
                # Entry = current 2nd LONG flip candle close
                ts_ms = int(cur['flip_at'].timestamp() * 1000)
                idx = find_idx(candles, ts_ms)
                if idx is None or idx >= len(candles) - 5: continue
                seconds.append({
                    'pair': pair, 'idx': idx, 'candles': candles,
                    'tier': cur['tier'], 'gap_h': gap_h,
                })

        # Stats per target
        print(f'--- Window <={max_window_h}h ---')
        if not seconds:
            print(f'  N=0')
            continue
        for tp in TARGETS:
            be_wr = STOP_PCT / (tp + STOP_PCT) * 100
            wins = losses = opens = 0
            rs = []
            for s in seconds:
                outcome, exit_price = simulate_pct(
                    s['candles'], s['idx'], s['candles'][s['idx']]['c'],
                    'LONG', tp, STOP_PCT, max_bars=72,
                )
                if outcome == 'WIN':
                    wins += 1; rs.append(tp)
                elif outcome == 'LOSS':
                    losses += 1; rs.append(-STOP_PCT)
                else:
                    opens += 1
                    ep = s['candles'][s['idx']]['c']
                    rs.append((exit_price - ep) / ep * 100)
            n = len(rs)
            wr = wins/n*100 if n else 0
            avg = sum(rs)/n if rs else 0
            edge = wr - be_wr
            marker = '🚀' if avg > 1 else ('✓' if avg > 0 else ' ')
            print(f'  +{tp}%   N={n:>4} WR={wr:>5.1f}% AvgRet={avg:>+5.2f}% '
                  f'Edge={edge:>+5.1f}% {marker}')

    # ── Also test 3rd flip (LONG → SHORT → LONG → SHORT → LONG) ────
    print()
    print('=' * 90)
    print('THIRD ST LONG flip (LONG-SHORT-LONG-SHORT-LONG, window <=48h to LONG_1)')
    print('=' * 90)
    thirds = []
    for pair, seq in pair_sequences.items():
        candles = KLINES.get(pair)
        if not candles: continue
        # Find pattern: L S L S L (current = last L)
        for i in range(4, len(seq)):
            cur = seq[i]
            if cur['direction'] != 'LONG': continue
            # Walk back: prev pattern L S L S
            if seq[i-1]['direction'] != 'SHORT': continue
            if seq[i-2]['direction'] != 'LONG': continue
            if seq[i-3]['direction'] != 'SHORT': continue
            if seq[i-4]['direction'] != 'LONG': continue
            gap_h = (cur['flip_at'] - seq[i-4]['flip_at']).total_seconds() / 3600
            if gap_h > 48: continue
            ts_ms = int(cur['flip_at'].timestamp() * 1000)
            idx = find_idx(candles, ts_ms)
            if idx is None or idx >= len(candles) - 5: continue
            thirds.append({
                'pair': pair, 'idx': idx, 'candles': candles,
                'tier': cur['tier'], 'gap_h': gap_h,
            })

    print(f'  N (LSLSL pattern, gap<=48h): {len(thirds)}')
    if thirds:
        for tp in TARGETS:
            be_wr = STOP_PCT / (tp + STOP_PCT) * 100
            wins = losses = 0
            rs = []
            for s in thirds:
                outcome, exit_price = simulate_pct(
                    s['candles'], s['idx'], s['candles'][s['idx']]['c'],
                    'LONG', tp, STOP_PCT, max_bars=72,
                )
                if outcome == 'WIN':
                    wins += 1; rs.append(tp)
                elif outcome == 'LOSS':
                    rs.append(-STOP_PCT)
                else:
                    ep = s['candles'][s['idx']]['c']
                    rs.append((exit_price - ep) / ep * 100)
            n = len(rs)
            wr = wins/n*100 if n else 0
            avg = sum(rs)/n if rs else 0
            edge = wr - be_wr
            print(f'  +{tp}%   N={n:>4} WR={wr:>5.1f}% AvgRet={avg:>+5.2f}% '
                  f'Edge={edge:>+5.1f}%')

    # ── Summary table comparing 1st vs 2nd ────
    print()
    print('=' * 90)
    print('COMPARISON: 1st flip vs 2nd flip vs 3rd flip (same target +10%, stop -5%)')
    print('=' * 90)

    def stats_for(setups, target=10):
        wins = 0
        rs = []
        for s in setups:
            outcome, exit_price = simulate_pct(
                s['candles'], s['idx'], s['candles'][s['idx']]['c'],
                'LONG', target, STOP_PCT, max_bars=72,
            )
            if outcome == 'WIN':
                wins += 1; rs.append(target)
            elif outcome == 'LOSS':
                rs.append(-STOP_PCT)
            else:
                ep = s['candles'][s['idx']]['c']
                rs.append((exit_price - ep) / ep * 100)
        n = len(rs)
        return n, (wins/n*100 if n else 0), (sum(rs)/n if rs else 0)

    print(f'{"Pattern":<30} {"N":>5} {"WR":>7} {"AvgRet%":>9}')
    print('-' * 60)
    n1, wr1, avg1 = stats_for(all_first)
    print(f'{"1st flip (any LONG)":<30} {n1:>5} {wr1:>6.1f}% {avg1:>+6.2f}%')

    # 2nd flip with 24h window (best statistic balance)
    seconds_24 = []
    for pair, seq in pair_sequences.items():
        candles = KLINES.get(pair)
        if not candles: continue
        for i in range(2, len(seq)):
            cur = seq[i]
            if cur['direction'] != 'LONG': continue
            prev_short = next((j for j in range(i-1, -1, -1)
                                if seq[j]['direction']=='SHORT'), None)
            if prev_short is None: continue
            prev_long = next((j for j in range(prev_short-1, -1, -1)
                               if seq[j]['direction']=='LONG'), None)
            if prev_long is None: continue
            gap_h = (cur['flip_at'] - seq[prev_long]['flip_at']).total_seconds() / 3600
            if gap_h > 24: continue
            ts_ms = int(cur['flip_at'].timestamp() * 1000)
            idx = find_idx(candles, ts_ms)
            if idx is None or idx >= len(candles) - 5: continue
            seconds_24.append({'pair': pair, 'idx': idx, 'candles': candles})

    n2, wr2, avg2 = stats_for(seconds_24)
    print(f'{"2nd flip (gap<=24h)":<30} {n2:>5} {wr2:>6.1f}% {avg2:>+6.2f}%')

    n3, wr3, avg3 = stats_for(thirds)
    print(f'{"3rd flip (LSLSL gap<=48h)":<30} {n3:>5} {wr3:>6.1f}% {avg3:>+6.2f}%')

    print()
    print(f'Improvement 2nd vs 1st: WR {wr2-wr1:+.1f}%, AvgRet {avg2-avg1:+.2f}%')
    print(f'Improvement 3rd vs 1st: WR {wr3-wr1:+.1f}%, AvgRet {avg3-avg1:+.2f}%')

    print('\n=== DONE ===')


if __name__ == '__main__':
    main()
