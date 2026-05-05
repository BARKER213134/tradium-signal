"""HOLD TIME analysis для Volume Surge:
Если зашёл на Volume Surge — сколько держать?

Тест: для каждого VS entry смотрим P&L на 1h, 2h, 4h, 8h, 12h, 24h, 48h, 72h
без TP/SL — просто закрытие по времени. Найти оптимальный hold.

Также с trailing stop варианты:
- Hold N hours, then exit at close
- Hold until ST band breached (ST trailing)
- Hold until +X% then exit (target only, no stop)
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


def find_candle_at(candles, ts_ms):
    for i, c in enumerate(candles):
        if c['t'] >= ts_ms:
            return i
    return None


def vol_ratio(candles, idx, period=20):
    if idx < period: return None
    avg = sum(c.get('v', 0) for c in candles[idx-period:idx]) / period
    if avg <= 0: return None
    return candles[idx]['v'] / avg


def main():
    since = datetime.utcnow() - timedelta(days=14)
    st_signals = list(db.supertrend_signals.find({'flip_at': {'$gte': since}}))
    print(f'ST flips: {len(st_signals)}')

    # Build VS setups (vol >= 3x)
    setups = []
    for st in st_signals:
        pair_norm = st.get('pair_norm')
        candles = KLINES.get(pair_norm)
        if not candles or len(candles) < 30:
            continue
        flip_ts = int(st['flip_at'].timestamp() * 1000)
        idx = find_candle_at(candles, flip_ts)
        if idx is None or idx < 21 or idx >= len(candles) - 5:
            continue
        vr = vol_ratio(candles, idx, 20)
        if vr is None or vr < 3.0:
            continue
        setups.append({
            'pair': pair_norm, 'direction': st['direction'],
            'tier': st.get('tier', 'mtf'),
            'idx': idx, 'candles': candles,
            'entry_price': candles[idx]['c'],
            'sl_price': st.get('sl_price'),
            'vol_ratio': vr,
        })
    print(f'Volume Surge setups (vol>=3x): {len(setups)}')
    print()

    # ── 1) Hold N hours, exit at close (no stop, no target) ────
    print('=' * 80)
    print('SIMPLE HOLD: вход на VS, закрытие через N часов (без TP/SL)')
    print('=' * 80)
    print(f'{"Hold":>5} {"N":>5} {"WR":>6} {"AvgRet%":>9} {"MedRet%":>9} '
          f'{"Best%":>7} {"Worst%":>7}')
    print('-' * 60)

    holds = [1, 2, 4, 6, 8, 12, 16, 24, 36, 48, 72]
    for h in holds:
        rs = []
        for s in setups:
            exit_idx = s['idx'] + h
            if exit_idx >= len(s['candles']):
                continue
            exit_price = s['candles'][exit_idx]['c']
            if s['direction'] == 'LONG':
                ret = (exit_price - s['entry_price']) / s['entry_price'] * 100
            else:
                ret = (s['entry_price'] - exit_price) / s['entry_price'] * 100
            rs.append(ret)
        if not rs: continue
        n = len(rs)
        wins = sum(1 for r in rs if r > 0)
        wr = wins/n*100
        avg = sum(rs)/n
        med = sorted(rs)[len(rs)//2]
        best = max(rs)
        worst = min(rs)
        print(f'{h:>4}h {n:>5} {wr:>5.1f}% {avg:>+7.2f}%  {med:>+7.2f}%  '
              f'{best:>+6.2f}% {worst:>+6.2f}%')

    # ── 2) Hold N hours WITH trailing -5% stop ────
    print()
    print('=' * 80)
    print('HOLD + TRAILING STOP -5% — закрытие через N часов или -5%')
    print('=' * 80)
    print(f'{"Hold":>5} {"N":>5} {"WR":>6} {"AvgRet%":>9} {"StopHits":>9}')
    print('-' * 50)

    for h in holds:
        rs = []
        stops = 0
        for s in setups:
            entry = s['entry_price']
            direction = s['direction']
            sl_price = entry * (0.95 if direction == 'LONG' else 1.05)
            end = min(len(s['candles']), s['idx'] + h + 1)
            outcome = None
            exit_price = None
            for j in range(s['idx'] + 1, end):
                c = s['candles'][j]
                if direction == 'LONG':
                    if c['l'] <= sl_price:
                        outcome = 'STOP'; exit_price = sl_price; break
                else:
                    if c['h'] >= sl_price:
                        outcome = 'STOP'; exit_price = sl_price; break
            if outcome is None:
                if end >= len(s['candles']):
                    continue
                exit_price = s['candles'][end - 1]['c']
                outcome = 'TIME'
            if direction == 'LONG':
                r = (exit_price - entry) / entry * 100
            else:
                r = (entry - exit_price) / entry * 100
            rs.append(r)
            if outcome == 'STOP':
                stops += 1
        if not rs: continue
        n = len(rs)
        wins = sum(1 for r in rs if r > 0)
        print(f'{h:>4}h {n:>5} {wins/n*100:>5.1f}% {sum(rs)/n:>+7.2f}%   '
              f'{stops}/{n}={stops/n*100:.0f}%')

    # ── 3) Target ladder ────
    print()
    print('=' * 80)
    print('TARGET LADDER: hit any TP first or stop -5%')
    print('=' * 80)
    print(f'{"Target":>7} {"AvgHold":>9} {"WR":>6} {"AvgRet%":>9}')
    print('-' * 45)

    for tp_pct in [3, 5, 7, 10, 15]:
        rs, hits, holds_h = [], 0, []
        for s in setups:
            entry = s['entry_price']
            direction = s['direction']
            tp = entry * ((100+tp_pct)/100 if direction == 'LONG' else (100-tp_pct)/100)
            sl = entry * (0.95 if direction == 'LONG' else 1.05)
            end = min(len(s['candles']), s['idx'] + 73)
            outcome = None; exit_price = None; bars = None
            for j in range(s['idx'] + 1, end):
                c = s['candles'][j]
                if direction == 'LONG':
                    if c['l'] <= sl:
                        outcome = 'STOP'; exit_price = sl; bars = j - s['idx']; break
                    if c['h'] >= tp:
                        outcome = 'WIN'; exit_price = tp; bars = j - s['idx']; break
                else:
                    if c['h'] >= sl:
                        outcome = 'STOP'; exit_price = sl; bars = j - s['idx']; break
                    if c['l'] <= tp:
                        outcome = 'WIN'; exit_price = tp; bars = j - s['idx']; break
            if outcome is None:
                exit_price = s['candles'][end-1]['c']
                bars = end-1 - s['idx']
                outcome = 'OPEN'
            if direction == 'LONG':
                r = (exit_price - entry) / entry * 100
            else:
                r = (entry - exit_price) / entry * 100
            rs.append(r)
            holds_h.append(bars)
            if outcome == 'WIN':
                hits += 1
        if not rs: continue
        n = len(rs)
        avg_hold = sum(holds_h) / n
        print(f'+{tp_pct}%   {avg_hold:>5.1f}h    {hits/n*100:>5.1f}% '
              f'{sum(rs)/n:>+7.2f}%')

    # ── 4) Per-tier hold optimum ────
    print()
    print('=' * 80)
    print('OPTIMAL HOLD by TIER (no stop, just hold N hours)')
    print('=' * 80)
    for tier in ['vip', 'mtf', 'daily']:
        tier_setups = [s for s in setups if s['tier'] == tier]
        if not tier_setups: continue
        print(f'\n{tier.upper()} (N base={len(tier_setups)}):')
        print(f'{"Hold":>5} {"N":>4} {"WR":>6} {"AvgRet%":>9}')
        for h in [1, 2, 4, 8, 12, 24, 48, 72]:
            rs = []
            for s in tier_setups:
                ei = s['idx'] + h
                if ei >= len(s['candles']): continue
                ep = s['candles'][ei]['c']
                if s['direction'] == 'LONG':
                    ret = (ep - s['entry_price']) / s['entry_price'] * 100
                else:
                    ret = (s['entry_price'] - ep) / s['entry_price'] * 100
                rs.append(ret)
            if not rs: continue
            n = len(rs)
            wins = sum(1 for r in rs if r > 0)
            print(f'{h:>4}h {n:>4} {wins/n*100:>5.1f}% {sum(rs)/n:>+7.2f}%')

    print('\n=== DONE ===')


if __name__ == '__main__':
    main()
