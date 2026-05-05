"""CONFLUENCE STACK detector backtest:

Гипотеза: 3+ Confluence MEDIUM (score>=4) одного направления на одной паре
в окне 24h = signal стека = вход после 3-го такого сигнала.

GIGGLE показал: 4 MEDIUM LONG за 22h → 32% pump.
Может это не случайность.
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
print(f'Cached pairs: {len(KLINES)}')


def find_candle_at(candles, ts_ms):
    for i, c in enumerate(candles):
        if c['t'] >= ts_ms:
            return i, c
    return None, None


def simulate_pct(candles, entry_idx, entry_price, direction,
                  target_pct, stop_pct, max_bars=72):
    if direction == 'LONG':
        tp = entry_price * (1 + target_pct / 100)
        sl = entry_price * (1 - stop_pct / 100)
    else:
        tp = entry_price * (1 - target_pct / 100)
        sl = entry_price * (1 + stop_pct / 100)
    end = min(len(candles), entry_idx + max_bars + 1)
    for j in range(entry_idx + 1, end):
        c = candles[j]
        if direction == 'LONG':
            if c['l'] <= sl:
                return 'LOSS', sl
            if c['h'] >= tp:
                return 'WIN', tp
        else:
            if c['h'] >= sl:
                return 'LOSS', sl
            if c['l'] <= tp:
                return 'WIN', tp
    last = candles[end - 1]
    return 'OPEN', last['c']


def main():
    since = datetime.utcnow() - timedelta(days=14)

    # Все confluence сигналы (MEDIUM и выше)
    print('Loading confluence signals...')
    cf = list(db.confluence.find({
        'detected_at': {'$gte': since},
        'score': {'$gte': 4},
    }).sort([('detected_at', 1)]))
    print(f'Total Confluence signals (score>=4): {len(cf)}')

    # Group by (pair, direction): list of detected_at
    by_pair_dir = defaultdict(list)
    for s in cf:
        key = (s.get('pair'), s.get('direction'))
        by_pair_dir[key].append(s)

    # Для каждой пары/direction искать stacks: 3+ сигналов в окне 24h
    WINDOW_HOURS = 24
    STACK_MIN = 3

    stacks = []  # list of (pair, direction, last_signal_ts, count, signals)
    for (pair, direction), sigs in by_pair_dir.items():
        if len(sigs) < STACK_MIN:
            continue
        # Sliding window
        for i in range(STACK_MIN - 1, len(sigs)):
            window_start_ts = sigs[i]['detected_at'] - timedelta(hours=WINDOW_HOURS)
            count = 1
            included = [sigs[i]]
            for j in range(i - 1, -1, -1):
                if sigs[j]['detected_at'] >= window_start_ts:
                    count += 1
                    included.append(sigs[j])
                else:
                    break
            if count >= STACK_MIN:
                stacks.append({
                    'pair': pair, 'direction': direction,
                    'trigger_at': sigs[i]['detected_at'],
                    'count': count,
                    'signals': included,
                })

    print(f'Stacks found (3+ signals in 24h, same dir): {len(stacks)}')
    print()

    # Симулируем каждый stack: входим на trigger candle, target +10%, stop -5%
    targets = [5.0, 7.0, 10.0, 15.0]
    stop_pct = 5.0

    for target_pct in targets:
        print(f'\n{"=" * 60}')
        print(f'TARGET +{target_pct}%, STOP -{stop_pct}%')
        print('=' * 60)
        print(f'{"Stack":>6} {"N":>4} {"WR":>7} {"AvgRet":>8} {"BE":>5}')
        be_wr = stop_pct / (target_pct + stop_pct) * 100

        # Slice by stack count: 3+, 4+, 5+
        for min_count in (3, 4, 5):
            results = defaultdict(list)
            seen_keys = set()  # Dedup: одна (pair, direction, day) max 1 trade

            for st in stacks:
                if st['count'] < min_count:
                    continue
                pair_norm = (st['pair'] or '').replace('/', '').upper()
                if pair_norm not in KLINES:
                    continue
                # Dedup: same pair+direction same day → 1 trade
                day_key = (pair_norm, st['direction'],
                            st['trigger_at'].strftime('%Y-%m-%d'))
                if day_key in seen_keys:
                    continue
                seen_keys.add(day_key)

                candles = KLINES[pair_norm]
                if len(candles) < 30:
                    continue
                trigger_ts = int(st['trigger_at'].timestamp() * 1000)
                ei, ec = find_candle_at(candles, trigger_ts)
                if ei is None or ei >= len(candles) - 5:
                    continue
                entry_price = ec['o']
                outcome, exit_price = simulate_pct(
                    candles, ei, entry_price, st['direction'],
                    target_pct=target_pct, stop_pct=stop_pct, max_bars=72,
                )
                if outcome == 'WIN':
                    r_pct = target_pct
                elif outcome == 'LOSS':
                    r_pct = -stop_pct
                else:
                    if st['direction'] == 'LONG':
                        r_pct = (exit_price - entry_price) / entry_price * 100
                    else:
                        r_pct = (entry_price - exit_price) / entry_price * 100
                results[st['direction']].append({
                    'pair': pair_norm, 'outcome': outcome, 'r_pct': r_pct,
                    'count': st['count'],
                })

            for direction in ('LONG', 'SHORT', 'BOTH'):
                if direction == 'BOTH':
                    trades = results['LONG'] + results['SHORT']
                else:
                    trades = results[direction]
                if not trades:
                    continue
                n = len(trades)
                wins = sum(1 for t in trades if t['outcome'] == 'WIN')
                wr = wins / n * 100
                avg = sum(t['r_pct'] for t in trades) / n
                tag = f'{min_count}+_{direction[:4]}'
                print(f'  {tag:>10} {n:>4} {wr:>6.1f}% {avg:>+6.2f}% '
                      f'{be_wr:>4.0f}%')

    # ── Best stacks examples ────
    print()
    print('=' * 60)
    print('TOP 10 stacks by signal count (recent)')
    print('=' * 60)
    stacks.sort(key=lambda s: (-s['count'], -s['trigger_at'].timestamp()))
    for st in stacks[:15]:
        sigs_str = ', '.join(s['detected_at'].strftime('%m-%d %H:%M')
                              for s in st['signals'][:5])
        trig = st['trigger_at'].strftime('%m-%d %H:%M')
        print(f'  {trig} | {st["pair"]:>15} {st["direction"]:>5} | '
              f'count={st["count"]} | last 5: {sigs_str}')

    print('\n=== DONE ===')


if __name__ == '__main__':
    main()
