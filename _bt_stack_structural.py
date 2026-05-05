"""STRUCTURAL FILTER backtest для Confluence Stack:

Базовый сигнал: 3+ Confluence MEDIUM same direction за 24h.
Дополнительные структурные фильтры:
  1. NEAR LEVEL — entry в пределах 1.5% от support (LONG) или resistance (SHORT)
     (берём из factors confluence сигнала type=level)
  2. VOLUME RISING — vol(last 3 bars) > vol(prev 12 bars) × 1.2
  3. ETH ALIGNED — ETH price change за последний час (LONG: +0.3%+, SHORT: -0.3%+)
  4. PATTERN STRONG — последний паттерн в стеке = bullish/bearish reversal
     (Tweezer, Three Soldiers, Engulfing, Morning/Evening Star)

Тестируем стратегии:
- Base: stack alone (контроль)
- +Level: stack + near support/resistance
- +Vol: stack + rising volume
- +ETH: stack + ETH correlated
- +Pattern: stack + strong reversal pattern
- ALL: всё одновременно
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

ETH_CANDLES = KLINES.get('ETHUSDT', [])
print(f'ETH candles: {len(ETH_CANDLES)}')


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


def parse_level_from_factors(factors, direction):
    """Find level value (e.g. 'S1 @ 31.53') in factors. Return level price."""
    for f in factors or []:
        if f.get('type') != 'level':
            continue
        val = (f.get('value') or '').upper()
        # Format: "S1 @ 31.53" or "R1 @ 31.91"
        if '@' not in val:
            continue
        try:
            level_price = float(val.split('@')[1].strip())
            level_type = 'S' if val.startswith('S') else 'R'
            return level_price, level_type
        except Exception:
            continue
    return None, None


def check_volume_rising(candles, entry_idx):
    """Last 3 bars avg vol > prev 12 bars avg vol × 1.2."""
    if entry_idx < 16:
        return False
    last3 = candles[entry_idx - 2:entry_idx + 1]
    prev12 = candles[entry_idx - 14:entry_idx - 2]
    avg_last = sum(c.get('v', 0) for c in last3) / 3
    avg_prev = sum(c.get('v', 0) for c in prev12) / 12
    if avg_prev <= 0:
        return False
    return avg_last >= avg_prev * 1.2


def check_eth_aligned(direction, ts_ms):
    """ETH change last hour matches direction (>= 0.3% absolute)."""
    if not ETH_CANDLES:
        return None  # no data, skip filter
    eth_idx, eth_c = find_candle_at(ETH_CANDLES, ts_ms)
    if eth_idx is None or eth_idx < 1:
        return None
    prev = ETH_CANDLES[eth_idx - 1]
    change_pct = (eth_c['c'] - prev['c']) / prev['c'] * 100
    if direction == 'LONG':
        return change_pct >= 0.3
    else:
        return change_pct <= -0.3


STRONG_PATTERNS_LONG = {
    'Пинцет на дне', 'Три белых солдата', 'Бычье поглощение',
    'Утренняя звезда', 'Молот'
}
STRONG_PATTERNS_SHORT = {
    'Пинцет на верху', 'Три чёрные вороны', 'Медвежье поглощение',
    'Вечерняя звезда', 'Падающая звезда', 'Повешенный'
}


def check_pattern_strong(stack, direction):
    """Хотя бы один сигнал в стеке имел strong pattern."""
    target = STRONG_PATTERNS_LONG if direction == 'LONG' else STRONG_PATTERNS_SHORT
    for sig in stack['signals']:
        for f in sig.get('factors') or []:
            if f.get('type') == 'pattern':
                v = f.get('value', '')
                if v in target:
                    return True
    return False


def check_near_level(stack, entry_price, direction):
    """Entry price в пределах 1.5% от level из factors (S для LONG, R для SHORT)."""
    for sig in stack['signals']:
        level_price, level_type = parse_level_from_factors(
            sig.get('factors'), direction)
        if level_price is None:
            continue
        # LONG ожидает support BELOW or equal. SHORT — resistance ABOVE.
        if direction == 'LONG' and level_type == 'S':
            dist_pct = abs(entry_price - level_price) / level_price * 100
            if dist_pct <= 1.5:
                return True
        elif direction == 'SHORT' and level_type == 'R':
            dist_pct = abs(entry_price - level_price) / level_price * 100
            if dist_pct <= 1.5:
                return True
    return False


def main():
    since = datetime.utcnow() - timedelta(days=14)

    # Fetch confluence WITH factors
    print('Loading confluence with factors...')
    cf = list(db.confluence.find({
        'detected_at': {'$gte': since},
        'score': {'$gte': 4},
    }, {
        'pair': 1, 'direction': 1, 'detected_at': 1, 'score': 1,
        'strength': 1, 'factors': 1,
    }).sort([('detected_at', 1)]))
    print(f'Total Confluence (score>=4): {len(cf)}')

    # Build stacks (3+ same direction in 24h)
    by_pair_dir = defaultdict(list)
    for s in cf:
        key = (s.get('pair'), s.get('direction'))
        by_pair_dir[key].append(s)

    WINDOW_HOURS = 24
    STACK_MIN = 3
    stacks = []
    for (pair, direction), sigs in by_pair_dir.items():
        if len(sigs) < STACK_MIN:
            continue
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

    print(f'Stacks (3+ in 24h): {len(stacks)}')
    print()

    # Simulate each stack with each filter combo
    target_pct = 10.0
    stop_pct = 5.0

    # Filter combinations to test
    filter_combos = [
        ('Base (no filters)',     {'level': False, 'vol': False, 'eth': False, 'pattern': False}),
        ('+Level only',           {'level': True,  'vol': False, 'eth': False, 'pattern': False}),
        ('+Volume rising only',   {'level': False, 'vol': True,  'eth': False, 'pattern': False}),
        ('+ETH aligned only',     {'level': False, 'vol': False, 'eth': True,  'pattern': False}),
        ('+Pattern strong only',  {'level': False, 'vol': False, 'eth': False, 'pattern': True}),
        ('Level + Volume',        {'level': True,  'vol': True,  'eth': False, 'pattern': False}),
        ('Level + ETH',           {'level': True,  'vol': False, 'eth': True,  'pattern': False}),
        ('Volume + ETH',          {'level': False, 'vol': True,  'eth': True,  'pattern': False}),
        ('Level + Volume + ETH',  {'level': True,  'vol': True,  'eth': True,  'pattern': False}),
        ('ALL 4 filters',         {'level': True,  'vol': True,  'eth': True,  'pattern': True}),
    ]

    print('=' * 80)
    print(f'TARGET +{target_pct}%, STOP -{stop_pct}%, BE_WR={int(stop_pct/(target_pct+stop_pct)*100)}%')
    print('=' * 80)
    print(f'{"Strategy":<28} {"N":>5} {"WR":>7} {"AvgRet":>8} {"Edge":>6}')
    print('-' * 70)

    BE_WR = stop_pct / (target_pct + stop_pct) * 100

    # Pre-evaluate filters for each stack
    stack_data = []
    for st in stacks:
        pair_norm = (st['pair'] or '').replace('/', '').upper()
        if pair_norm not in KLINES:
            continue
        candles = KLINES[pair_norm]
        if len(candles) < 30:
            continue
        trigger_ts = int(st['trigger_at'].timestamp() * 1000)
        ei, ec = find_candle_at(candles, trigger_ts)
        if ei is None or ei >= len(candles) - 5:
            continue
        entry_price = ec['o']

        # Run trade
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

        # Compute filters
        f_level = check_near_level(st, entry_price, st['direction'])
        f_vol = check_volume_rising(candles, ei)
        f_eth = check_eth_aligned(st['direction'], trigger_ts)
        f_pattern = check_pattern_strong(st, st['direction'])

        stack_data.append({
            'pair': pair_norm, 'direction': st['direction'],
            'count': st['count'], 'r_pct': r_pct, 'outcome': outcome,
            'level': f_level, 'vol': f_vol, 'eth': f_eth,
            'pattern': f_pattern,
        })

    print(f'Total stacks evaluated: {len(stack_data)}')
    print()

    for label, filters in filter_combos:
        # Filter stacks
        passing = []
        for sd in stack_data:
            if filters['level'] and not sd['level']:
                continue
            if filters['vol'] and not sd['vol']:
                continue
            if filters['eth']:
                if sd['eth'] is None or sd['eth'] is False:
                    continue
            if filters['pattern'] and not sd['pattern']:
                continue
            passing.append(sd)
        n = len(passing)
        if n == 0:
            print(f'{label:<28} {"--":>5} -- -- --')
            continue
        wins = sum(1 for s in passing if s['outcome'] == 'WIN')
        wr = wins / n * 100
        avg = sum(s['r_pct'] for s in passing) / n
        edge = wr - BE_WR
        marker = '✓' if avg > 0 else ''
        print(f'{label:<28} {n:>5} {wr:>6.1f}% {avg:>+6.2f}% '
              f'{edge:>+5.1f}% {marker}')

    # ── Detailed by direction for top filter combo ──
    print()
    print('=' * 80)
    print('LEVEL + VOLUME + ETH — by direction')
    print('=' * 80)
    print(f'{"Dir":>6} {"N":>5} {"WR":>7} {"AvgRet":>8}')
    for direction in ('LONG', 'SHORT'):
        passing = [sd for sd in stack_data
                    if sd['direction'] == direction
                    and sd['level']
                    and sd['vol']
                    and sd['eth'] is True]
        n = len(passing)
        if n == 0:
            continue
        wins = sum(1 for s in passing if s['outcome'] == 'WIN')
        wr = wins / n * 100
        avg = sum(s['r_pct'] for s in passing) / n
        print(f'{direction:>6} {n:>5} {wr:>6.1f}% {avg:>+6.2f}%')

    # ── Per-filter contribution ──
    print()
    print('=' * 80)
    print('Per-filter ON/OFF comparison (for stacks where filter applicable)')
    print('=' * 80)
    print(f'{"Filter":>10} {"State":>5} {"N":>5} {"WR":>7} {"AvgRet":>8}')
    for fname in ['level', 'vol', 'eth', 'pattern']:
        for state in (True, False):
            applicable = [sd for sd in stack_data
                          if sd[fname] is not None and sd[fname] == state]
            n = len(applicable)
            if n == 0:
                continue
            wins = sum(1 for s in applicable if s['outcome'] == 'WIN')
            wr = wins / n * 100
            avg = sum(s['r_pct'] for s in applicable) / n
            print(f'{fname:>10} {str(state):>5} {n:>5} {wr:>6.1f}% '
                  f'{avg:>+6.2f}%')

    # ── Lower target test (5% / 7%) ──
    print()
    print('=' * 80)
    print('Same filters but with TARGET +5% (easier hit)')
    print('=' * 80)

    target_pct_lo = 5.0
    BE_WR_lo = stop_pct / (target_pct_lo + stop_pct) * 100

    # Re-simulate for target=5%
    stack_data_5 = []
    for st in stacks:
        pair_norm = (st['pair'] or '').replace('/', '').upper()
        if pair_norm not in KLINES:
            continue
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
            target_pct=target_pct_lo, stop_pct=stop_pct, max_bars=72,
        )
        if outcome == 'WIN':
            r_pct = target_pct_lo
        elif outcome == 'LOSS':
            r_pct = -stop_pct
        else:
            if st['direction'] == 'LONG':
                r_pct = (exit_price - entry_price) / entry_price * 100
            else:
                r_pct = (entry_price - exit_price) / entry_price * 100
        f_level = check_near_level(st, entry_price, st['direction'])
        f_vol = check_volume_rising(candles, ei)
        f_eth = check_eth_aligned(st['direction'], trigger_ts)
        f_pattern = check_pattern_strong(st, st['direction'])
        stack_data_5.append({
            'pair': pair_norm, 'direction': st['direction'],
            'count': st['count'], 'r_pct': r_pct, 'outcome': outcome,
            'level': f_level, 'vol': f_vol, 'eth': f_eth,
            'pattern': f_pattern,
        })

    print(f'{"Strategy":<28} {"N":>5} {"WR":>7} {"AvgRet":>8} {"Edge":>6}')
    print('-' * 70)
    for label, filters in filter_combos[5:]:  # skip single-filter, show combos
        passing = []
        for sd in stack_data_5:
            if filters['level'] and not sd['level']:
                continue
            if filters['vol'] and not sd['vol']:
                continue
            if filters['eth']:
                if sd['eth'] is None or sd['eth'] is False:
                    continue
            if filters['pattern'] and not sd['pattern']:
                continue
            passing.append(sd)
        n = len(passing)
        if n == 0:
            print(f'{label:<28} {"--":>5}')
            continue
        wins = sum(1 for s in passing if s['outcome'] == 'WIN')
        wr = wins / n * 100
        avg = sum(s['r_pct'] for s in passing) / n
        edge = wr - BE_WR_lo
        marker = '✓' if avg > 0 else ''
        print(f'{label:<28} {n:>5} {wr:>6.1f}% {avg:>+6.2f}% '
              f'{edge:>+5.1f}% {marker}')

    print('\n=== DONE ===')


if __name__ == '__main__':
    main()
