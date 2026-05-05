"""SIGNAL FRESHNESS backtest:
Для каждого сигнала пробуем войти ЧЕРЕЗ N часов после fire'а.
Цель: найти оптимальное delay чтобы взять +10% (или иной target).

Гипотеза:
- Delay=0 (сразу): иногда whipsaw, false breakout
- Delay=2-4h (ждём подтверждения тренда): better filter
- Delay=12+h: уже поздно, momentum exhausted

Тестируем:
1. Multiple delays: 0, 1, 2, 4, 6, 8, 12, 24h
2. Multiple targets: 5%, 7%, 10%, 15%
3. Stops: -3%, -5%, -7% от entry
4. Max hold: 72h

Slice by:
- Signal source (ST VIP/MTF/Daily, CV, Tradium, new strategies)
- Direction LONG/SHORT
- BTC trend phase
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

# Load klines cache
print('Loading klines cache...')
with open('_bt_klines_cache.json') as f:
    KLINES = json.load(f)
print(f'Cached pairs: {len(KLINES)}')


def find_candle_at(candles, ts_ms):
    """Find first candle with t >= ts_ms."""
    for i, c in enumerate(candles):
        if c['t'] >= ts_ms:
            return i, c
    return None, None


def simulate_with_target(candles, entry_idx, entry_price, direction,
                          target_pct, stop_pct, max_bars=72):
    """Simulate: target +X% / stop -Y% / time-stop max_bars.
    Returns ('WIN', exit_price), ('LOSS', exit_price), ('OPEN', exit_price)."""
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


def collect_signals():
    """Collect all signals as flat list: (pair_norm, direction, ts_ms, source, tier)"""
    since = datetime.utcnow() - timedelta(days=14)
    signals = []

    # SuperTrend
    st = list(db.supertrend_signals.find(
        {'flip_at': {'$gte': since}},
        {'pair_norm': 1, 'direction': 1, 'flip_at': 1, 'tier': 1}
    ))
    for s in st:
        signals.append({
            'pair_norm': s['pair_norm'],
            'direction': s['direction'],
            'ts_ms': int(s['flip_at'].timestamp() * 1000),
            'source': 'st',
            'tier': s.get('tier', 'mtf'),
        })

    # CV pattern triggered
    cv = list(db.signals.find(
        {'source': 'cryptovizor', 'pattern_triggered': True,
         'pattern_triggered_at': {'$gte': since}},
        {'pair': 1, 'direction': 1, 'pattern_triggered_at': 1}
    ))
    for s in cv:
        pair_norm = (s.get('pair', '') or '').replace('/', '').upper()
        signals.append({
            'pair_norm': pair_norm,
            'direction': s['direction'],
            'ts_ms': int(s['pattern_triggered_at'].timestamp() * 1000),
            'source': 'cv',
            'tier': '-',
        })

    # New strategies
    ns = list(db.new_strategy_signals.find(
        {'created_at': {'$gte': since}},
        {'pair': 1, 'direction': 1, 'created_at': 1, 'strategy': 1}
    ))
    for s in ns:
        pair_norm = (s.get('pair', '') or '').replace('/', '').upper()
        signals.append({
            'pair_norm': pair_norm,
            'direction': s['direction'],
            'ts_ms': int(s['created_at'].timestamp() * 1000),
            'source': s['strategy'],
            'tier': '-',
        })

    return signals


def main():
    signals = collect_signals()
    print(f'Total signals collected: {len(signals)}')
    by_src = defaultdict(int)
    for s in signals:
        by_src[s['source']] += 1
    for k, v in sorted(by_src.items(), key=lambda x: -x[1]):
        print(f'  {k}: {v}')
    print()

    DELAYS_HOURS = [0, 1, 2, 4, 6, 8, 12, 24]
    TARGET_PCT = 10.0  # user requested 10%
    STOP_PCT = 5.0     # 2:1 RR

    # Group: source × delay → trades
    results = defaultdict(lambda: defaultdict(list))

    skipped_no_klines = 0
    skipped_short_data = 0

    for sig in signals:
        pair_norm = sig['pair_norm']
        candles = KLINES.get(pair_norm)
        if not candles:
            skipped_no_klines += 1
            continue
        if len(candles) < 30:
            skipped_short_data += 1
            continue

        sig_ts = sig['ts_ms']
        direction = sig['direction']

        # Find signal candle index
        sig_idx, sig_candle = find_candle_at(candles, sig_ts)
        if sig_idx is None:
            continue
        signal_price = sig_candle['o']  # opening price as reference

        for delay_h in DELAYS_HOURS:
            entry_ts = sig_ts + delay_h * 3600 * 1000
            entry_idx, entry_candle = find_candle_at(candles, entry_ts)
            if entry_idx is None or entry_idx >= len(candles) - 5:
                continue
            entry_price = entry_candle['o']

            # CONFIRMATION: продолжается ли тренд после delay?
            # Если delay=0 — confirmation вообще нет, входим на сигнал
            # Если delay>0 — требуем что цена не ушла ПРОТИВ нас
            if delay_h > 0:
                if direction == 'LONG':
                    if entry_price <= signal_price:
                        # Тренд РАЗВЕРНУЛСЯ за delay часов — пропускаем
                        results[sig['source']][delay_h].append({
                            'pair': pair_norm,
                            'outcome': 'SKIP_NO_CONFIRM',
                            'r_pct': 0.0,
                        })
                        continue
                else:  # SHORT
                    if entry_price >= signal_price:
                        results[sig['source']][delay_h].append({
                            'pair': pair_norm,
                            'outcome': 'SKIP_NO_CONFIRM',
                            'r_pct': 0.0,
                        })
                        continue

            # Simulate trade
            outcome, exit_price = simulate_with_target(
                candles, entry_idx, entry_price, direction,
                target_pct=TARGET_PCT, stop_pct=STOP_PCT, max_bars=72,
            )

            if outcome == 'WIN':
                r_pct = TARGET_PCT
            elif outcome == 'LOSS':
                r_pct = -STOP_PCT
            else:
                if direction == 'LONG':
                    r_pct = (exit_price - entry_price) / entry_price * 100
                else:
                    r_pct = (entry_price - exit_price) / entry_price * 100

            results[sig['source']][delay_h].append({
                'pair': pair_norm,
                'outcome': outcome,
                'r_pct': r_pct,
            })

    print(f'Skipped no_klines: {skipped_no_klines}, short_data: {skipped_short_data}')
    print()

    # Print stats by source × delay
    sources_to_show = ['st', 'cv', 'volume_surge', 'triple_confluence',
                        'vol_accum']
    for src in sources_to_show:
        if src not in results:
            continue
        print('=' * 75)
        print(f'SOURCE: {src.upper()}  (target +{TARGET_PCT}%, stop -{STOP_PCT}%)')
        print('=' * 75)
        print(f'{"Delay":>6} {"N":>5} {"Skip%":>6} {"WR":>7} {"Avg%":>7} '
              f'{"Win%":>5} {"Loss%":>5} {"Open%":>5}')
        print('-' * 70)

        for d in DELAYS_HOURS:
            trades = results[src].get(d, [])
            if not trades:
                continue
            n_total = len(trades)
            n_skipped = sum(1 for t in trades
                             if t['outcome'] == 'SKIP_NO_CONFIRM')
            n_active = n_total - n_skipped
            if n_active == 0:
                continue
            active = [t for t in trades if t['outcome'] != 'SKIP_NO_CONFIRM']
            wins = sum(1 for t in active if t['outcome'] == 'WIN')
            losses = sum(1 for t in active if t['outcome'] == 'LOSS')
            opens = sum(1 for t in active if t['outcome'] == 'OPEN')
            wr = wins / n_active * 100
            avg_pct = sum(t['r_pct'] for t in active) / n_active
            skip_pct = n_skipped / n_total * 100
            win_share = wins / n_active * 100
            loss_share = losses / n_active * 100
            open_share = opens / n_active * 100
            print(f'{d:>5}h {n_active:>5} {skip_pct:>5.1f}% '
                  f'{wr:>6.1f}% {avg_pct:>+6.2f}% '
                  f'{win_share:>4.0f}% {loss_share:>4.0f}% '
                  f'{open_share:>4.0f}%')
        print()

    # ── Specifically by ST tier ───
    print('=' * 75)
    print('SUPERTREND by TIER × DELAY (target +10%, stop -5%)')
    print('=' * 75)
    by_tier_delay = defaultdict(lambda: defaultdict(list))
    for sig in signals:
        if sig['source'] != 'st':
            continue
        candles = KLINES.get(sig['pair_norm'])
        if not candles:
            continue
        sig_ts = sig['ts_ms']
        direction = sig['direction']
        sig_idx, sig_candle = find_candle_at(candles, sig_ts)
        if sig_idx is None:
            continue
        signal_price = sig_candle['o']

        for d in DELAYS_HOURS:
            ets = sig_ts + d * 3600 * 1000
            ei, ec = find_candle_at(candles, ets)
            if ei is None or ei >= len(candles) - 5:
                continue
            ep = ec['o']
            if d > 0:
                if direction == 'LONG' and ep <= signal_price:
                    by_tier_delay[sig['tier']][d].append(
                        {'outcome': 'SKIP_NO_CONFIRM', 'r_pct': 0.0})
                    continue
                if direction == 'SHORT' and ep >= signal_price:
                    by_tier_delay[sig['tier']][d].append(
                        {'outcome': 'SKIP_NO_CONFIRM', 'r_pct': 0.0})
                    continue
            outcome, exit_price = simulate_with_target(
                candles, ei, ep, direction,
                target_pct=TARGET_PCT, stop_pct=STOP_PCT, max_bars=72,
            )
            if outcome == 'WIN':
                r_pct = TARGET_PCT
            elif outcome == 'LOSS':
                r_pct = -STOP_PCT
            else:
                if direction == 'LONG':
                    r_pct = (exit_price - ep) / ep * 100
                else:
                    r_pct = (ep - exit_price) / ep * 100
            by_tier_delay[sig['tier']][d].append(
                {'outcome': outcome, 'r_pct': r_pct})

    for tier in ('vip', 'mtf', 'daily'):
        if tier not in by_tier_delay:
            continue
        print(f'\nTIER = {tier.upper()}:')
        print(f'{"Delay":>6} {"N":>5} {"Skip%":>6} {"WR":>7} {"Avg%":>7}')
        for d in DELAYS_HOURS:
            trades = by_tier_delay[tier].get(d, [])
            if not trades:
                continue
            n_total = len(trades)
            n_skipped = sum(1 for t in trades
                             if t['outcome'] == 'SKIP_NO_CONFIRM')
            active = [t for t in trades if t['outcome'] != 'SKIP_NO_CONFIRM']
            n_active = len(active)
            if n_active == 0:
                continue
            wins = sum(1 for t in active if t['outcome'] == 'WIN')
            wr = wins / n_active * 100
            avg = sum(t['r_pct'] for t in active) / n_active
            skip_pct = n_skipped / n_total * 100
            print(f'{d:>5}h {n_active:>5} {skip_pct:>5.1f}% '
                  f'{wr:>6.1f}% {avg:>+6.2f}%')

    print('\n=== DONE ===')


if __name__ == '__main__':
    main()
