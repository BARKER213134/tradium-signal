"""V2 — более широкая интерпретация "второй ST":

Любой ST LONG flip на паре, после которого УЖЕ был хотя бы один LONG flip
за последние N часов. Без обязательного SHORT между ними.

Это включает:
1. Ratcheting trailing flips (ST trail вверх → новый flip-up на более высоком level)
2. После короткого pullback что не дотянулся до SHORT flip
3. Multi-tier flips (1h flipped, потом MTF/Daily подтянулись)
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


def find_idx(candles, ts_ms):
    for i, c in enumerate(candles):
        if c['t'] >= ts_ms: return i
    return None


def simulate_pct(candles, idx, entry, target, stop, max_bars=72):
    tp = entry * (1 + target/100)
    sl = entry * (1 - stop/100)
    end = min(len(candles), idx + max_bars + 1)
    for j in range(idx + 1, end):
        c = candles[j]
        if c['l'] <= sl: return 'LOSS', sl
        if c['h'] >= tp: return 'WIN', tp
    return 'OPEN', candles[end-1]['c']


def main():
    since = datetime.utcnow() - timedelta(days=14)
    flips = list(db.supertrend_signals.find({
        'flip_at': {'$gte': since}
    }).sort([('flip_at', 1)]))
    print(f'Total ST flips: {len(flips)}')

    # Group by pair, dedup by (pair, flip_at, direction)
    by_pair = defaultdict(list)
    for f in flips:
        by_pair[f.get('pair_norm')].append({
            'flip_at': f['flip_at'],
            'direction': f['direction'],
            'tier': f.get('tier', 'mtf'),
            'entry': f.get('entry_price'),
        })

    # Dedup
    for pair in list(by_pair):
        seen = set()
        unique = []
        for f in sorted(by_pair[pair], key=lambda x: x['flip_at']):
            k = (f['flip_at'], f['direction'])
            if k in seen: continue
            seen.add(k)
            unique.append(f)
        by_pair[pair] = unique

    # Categorize each LONG flip:
    # - "1st_only": pair has ONLY this flip (or first LONG without prior LONG in 48h)
    # - "2nd_LONG": at least one PRIOR LONG flip within 48h (regardless of SHORT between)
    # - "2nd_LONG_strict": LONG → SHORT → LONG (strict pattern)
    BASELINE_HOURS = 48

    cat_first = []  # 1st LONG (no prior LONG in 48h)
    cat_2nd = []    # 2nd+ LONG (had prior LONG in 48h, any pattern)
    cat_2nd_after_short = []  # 2nd LONG after SHORT explicitly

    for pair, seq in by_pair.items():
        candles = KLINES.get(pair)
        if not candles: continue
        for i, f in enumerate(seq):
            if f['direction'] != 'LONG': continue
            # Look back in seq for any LONG within 48h
            had_long_recently = False
            had_short_between = False
            for j in range(i-1, -1, -1):
                gap_h = (f['flip_at'] - seq[j]['flip_at']).total_seconds() / 3600
                if gap_h > BASELINE_HOURS: break
                if seq[j]['direction'] == 'LONG':
                    had_long_recently = True
                    # Check if any SHORT between this LONG and current
                    for k in range(j+1, i):
                        if seq[k]['direction'] == 'SHORT':
                            had_short_between = True
                            break
                    break
            ts_ms = int(f['flip_at'].timestamp() * 1000)
            idx = find_idx(candles, ts_ms)
            if idx is None or idx >= len(candles) - 5: continue
            entry = f.get('entry') or candles[idx]['c']
            setup = {'pair': pair, 'idx': idx, 'candles': candles,
                     'entry': entry, 'tier': f['tier']}
            if not had_long_recently:
                cat_first.append(setup)
            else:
                cat_2nd.append(setup)
                if had_short_between:
                    cat_2nd_after_short.append(setup)

    print(f'\n  1st LONG (no prior LONG in 48h): N={len(cat_first)}')
    print(f'  2nd+ LONG (any prior LONG ≤48h): N={len(cat_2nd)}')
    print(f'  2nd LONG with SHORT between (strict): N={len(cat_2nd_after_short)}')

    print()
    print('=' * 90)
    print('Comparing categories — TP +5/+7/+10%, stop -5%')
    print('=' * 90)

    def stats(cat, target, stop=5):
        wins = 0
        rs = []
        for s in cat:
            outcome, exit_price = simulate_pct(
                s['candles'], s['idx'], s['entry'],
                target, stop, max_bars=72,
            )
            if outcome == 'WIN':
                wins += 1; rs.append(target)
            elif outcome == 'LOSS':
                rs.append(-stop)
            else:
                rs.append((exit_price - s['entry']) / s['entry'] * 100)
        n = len(rs)
        return n, (wins/n*100 if n else 0), (sum(rs)/n if rs else 0)

    print(f'{"Category":<35} {"Target":>7} {"N":>5} {"WR":>7} {"AvgRet":>9}')
    print('-' * 70)
    for tp in [5, 7, 10]:
        n1, w1, a1 = stats(cat_first, tp)
        n2, w2, a2 = stats(cat_2nd, tp)
        n3, w3, a3 = stats(cat_2nd_after_short, tp)
        print(f'{"1st LONG":<35} +{tp}%   {n1:>5} {w1:>6.1f}% {a1:>+6.2f}%')
        print(f'{"2nd+ LONG (any pattern, ≤48h)":<35} +{tp}%   {n2:>5} {w2:>6.1f}% {a2:>+6.2f}%')
        print(f'{"2nd LONG strict (L→S→L)":<35} +{tp}%   {n3:>5} {w3:>6.1f}% {a3:>+6.2f}%')
        print(f'  Improvement 2nd vs 1st: WR {w2-w1:+.1f}%, AvgRet {a2-a1:+.2f}%')
        print()

    # Try with different windows
    print('=' * 90)
    print('2nd LONG (any pattern) by WINDOW size — TP +7%, stop -5%')
    print('=' * 90)
    print(f'{"Window":<10} {"N":>5} {"WR":>7} {"AvgRet":>9}')
    print('-' * 40)

    for hours in [6, 12, 24, 48, 72, 168]:
        cat = []
        for pair, seq in by_pair.items():
            candles = KLINES.get(pair)
            if not candles: continue
            for i, f in enumerate(seq):
                if f['direction'] != 'LONG': continue
                had_long = any(seq[j]['direction']=='LONG'
                               and (f['flip_at']-seq[j]['flip_at']).total_seconds()/3600 <= hours
                               for j in range(i-1,-1,-1))
                if not had_long: continue
                ts_ms = int(f['flip_at'].timestamp() * 1000)
                idx = find_idx(candles, ts_ms)
                if idx is None or idx >= len(candles) - 5: continue
                cat.append({'idx': idx, 'candles': candles,
                            'entry': f.get('entry') or candles[idx]['c']})
        n, w, a = stats(cat, target=7)
        print(f'≤{hours}h    {n:>5} {w:>6.1f}% {a:>+6.2f}%')

    # ── Test by tier on 2nd LONG ────
    print()
    print('=' * 90)
    print('2nd LONG (≤48h) by TIER — TP +7%, stop -5%')
    print('=' * 90)
    for tier_name in ['vip', 'mtf', 'daily']:
        cat = []
        for pair, seq in by_pair.items():
            candles = KLINES.get(pair)
            if not candles: continue
            for i, f in enumerate(seq):
                if f['direction'] != 'LONG': continue
                if f['tier'] != tier_name: continue
                had_long = any(seq[j]['direction']=='LONG'
                               and (f['flip_at']-seq[j]['flip_at']).total_seconds()/3600 <= 48
                               for j in range(i-1,-1,-1))
                if not had_long: continue
                ts_ms = int(f['flip_at'].timestamp() * 1000)
                idx = find_idx(candles, ts_ms)
                if idx is None or idx >= len(candles) - 5: continue
                cat.append({'idx': idx, 'candles': candles,
                            'entry': f.get('entry') or candles[idx]['c']})
        n, w, a = stats(cat, target=7)
        print(f'  tier={tier_name:<6} N={n:>5} WR={w:>5.1f}% AvgRet={a:>+5.2f}%')

    print('\n=== DONE ===')


if __name__ == '__main__':
    main()
