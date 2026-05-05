"""🌋 VOLCANO 5-DAY BACKTEST — production filter точно как в new_strategies.py

User: 'нет сигналов сейчас волкано сделай бектест за 5 дней добавь'

Цель: проверить
1. Сколько Volcano setups прошли все 6 фильтров за 5 дней
2. WR / AvgRet — подтверждает ли winners analysis (38% / +2.15%)
3. Если N=0 за 5d — фильтры слишком строгие, нужно мягчить
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

# === Production Volcano constants (точная копия из new_strategies.py) ===
VOLCANO_TIERS = ('mtf', 'daily')
VOLCANO_VOL_MIN = 3.0
VOLCANO_BODY_ATR_MIN = 1.0
VOLCANO_RSI_MAX = 70
VOLCANO_BAD_HOURS = {0, 1, 5, 6, 9, 21, 23}
VOLCANO_ACCUM_LOOKBACK = 12
VOLCANO_ACCUM_MULT = 0.7


def find_idx(candles, ts_ms):
    for i, c in enumerate(candles):
        if c['t'] >= ts_ms: return i
    return None


def compute_atr_last(candles, period=14):
    if len(candles) < period + 1: return 0.0
    trs = [candles[0]['h'] - candles[0]['l']]
    for i in range(1, len(candles)):
        prev = candles[i-1]['c']
        h, l = candles[i]['h'], candles[i]['l']
        trs.append(max(h-l, abs(h-prev), abs(l-prev)))
    cur = sum(trs[:period]) / period
    for i in range(period, len(candles)):
        cur = (cur * (period-1) + trs[i]) / period
    return cur


def compute_rsi_last(candles, period=14):
    if len(candles) < period + 1: return 50.0
    closes = [c['c'] for c in candles]
    g, l = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        g.append(max(d, 0)); l.append(max(-d, 0))
    if len(g) < period: return 50.0
    ag = sum(g[:period]) / period
    al = sum(l[:period]) / period
    for i in range(period, len(g)):
        ag = (ag * (period-1) + g[i]) / period
        al = (al * (period-1) + l[i]) / period
    if al == 0: return 100.0
    return 100 - 100 / (1 + ag/al)


def vol_ratio(candles_window):
    """Last candle vol / MA20 of preceding."""
    if len(candles_window) < 21: return 0
    last = candles_window[-1]
    prev = candles_window[-21:-1]
    avg = sum(c.get('v', 0) for c in prev) / 20
    if avg <= 0: return 0
    return last.get('v', 0) / avg


def check_volcano(candles_segment, tier, hour_utc):
    """Returns dict с проверкой каждого фильтра + final pass/fail."""
    result = {
        'tier_ok': tier in VOLCANO_TIERS,
        'hour_ok': hour_utc not in VOLCANO_BAD_HOURS,
        'enough_bars': len(candles_segment) >= 25,
        'vol_ok': False, 'accum_ok': False,
        'body_ok': False, 'rsi_ok': False,
        'vol_ratio': 0, 'body_atr': 0, 'rsi': 0,
        'pass': False,
    }
    if not result['enough_bars']: return result

    vr = vol_ratio(candles_segment)
    result['vol_ratio'] = round(vr, 2)
    result['vol_ok'] = vr >= VOLCANO_VOL_MIN

    # was_accum: avg vol bars [-15:-3] < MA20 [-23:-3] × 0.7
    if len(candles_segment) >= 23:
        accum_window = candles_segment[-15:-3]
        ma20_window = candles_segment[-23:-3]
        if len(accum_window) == 12 and len(ma20_window) == 20:
            avg_a = sum(c.get('v', 0) for c in accum_window) / 12
            avg_m = sum(c.get('v', 0) for c in ma20_window) / 20
            result['accum_ok'] = avg_m > 0 and avg_a < avg_m * VOLCANO_ACCUM_MULT

    atr = compute_atr_last(candles_segment, 14)
    last = candles_segment[-1]
    body = abs(last['c'] - last['o'])
    body_atr = body / atr if atr > 0 else 0
    result['body_atr'] = round(body_atr, 2)
    result['body_ok'] = body_atr >= VOLCANO_BODY_ATR_MIN

    rsi = compute_rsi_last(candles_segment, 14)
    result['rsi'] = round(rsi, 1)
    result['rsi_ok'] = rsi < VOLCANO_RSI_MAX

    result['pass'] = (result['tier_ok'] and result['hour_ok']
                     and result['vol_ok'] and result['accum_ok']
                     and result['body_ok'] and result['rsi_ok'])
    return result


def simulate_pct(candles, entry_idx, entry_price, target_pct=10, stop_pct=5,
                  max_bars=72):
    tp = entry_price * (1 + target_pct/100)
    sl = entry_price * (1 - stop_pct/100)
    end = min(len(candles), entry_idx + max_bars + 1)
    for j in range(entry_idx + 1, end):
        c = candles[j]
        if c['l'] <= sl: return 'LOSS', sl, j - entry_idx
        if c['h'] >= tp: return 'WIN', tp, j - entry_idx
    last = candles[end-1]
    return 'OPEN', last['c'], end - 1 - entry_idx


def main():
    # 5-day window
    since = datetime.utcnow() - timedelta(days=5)
    print(f'Window: last 5 days (since {since})')

    # Load ST flips за 5 дней
    st_signals = list(db.supertrend_signals.find({
        'flip_at': {'$gte': since},
        'direction': 'LONG',  # только LONG (Volcano LONG only)
        'tier': {'$in': list(VOLCANO_TIERS)},
    }))
    print(f'ST LONG MTF/Daily flips за 5d: {len(st_signals)}')

    # Process each
    total_processed = 0
    pass_all = []
    filter_failures = defaultdict(int)
    skipped_no_klines = 0

    for st in st_signals:
        pair = st.get('pair_norm')
        candles = KLINES.get(pair)
        if not candles:
            skipped_no_klines += 1
            continue

        flip_ts = int(st['flip_at'].timestamp() * 1000)
        idx = find_idx(candles, flip_ts)
        if idx is None or idx < 25 or idx >= len(candles) - 5:
            continue

        # Slice candles up to and including flip candle (idx)
        segment = candles[:idx+1]
        hour = st['flip_at'].hour
        tier = st.get('tier', 'mtf')

        result = check_volcano(segment, tier, hour)
        total_processed += 1

        # Track which filter failed
        for k in ('tier_ok', 'hour_ok', 'vol_ok', 'accum_ok', 'body_ok', 'rsi_ok'):
            if not result[k]:
                filter_failures[k] += 1

        if result['pass']:
            entry = candles[idx]['c']
            outcome, exit_price, bars = simulate_pct(
                candles, idx, entry, target_pct=10, stop_pct=5, max_bars=72,
            )
            r_pct = 10 if outcome == 'WIN' else (-5 if outcome == 'LOSS'
                else (exit_price - entry) / entry * 100)
            pass_all.append({
                'pair': pair, 'tier': tier,
                'flip_at': st['flip_at'],
                'entry': entry,
                'vol_ratio': result['vol_ratio'],
                'body_atr': result['body_atr'],
                'rsi': result['rsi'],
                'outcome': outcome, 'r_pct': r_pct, 'bars': bars,
            })

    print()
    print(f'Total processed: {total_processed} (no_klines: {skipped_no_klines})')
    print()
    print('=' * 70)
    print('FILTER FAILURE counts (сколько ST flips отвалилось на каждом фильтре)')
    print('=' * 70)
    print(f'  tier ∈ (mtf,daily) FAIL: {filter_failures["tier_ok"]:>4} '
          f'({filter_failures["tier_ok"]/total_processed*100:.1f}%)')
    print(f'  hour NOT bad      FAIL: {filter_failures["hour_ok"]:>4} '
          f'({filter_failures["hour_ok"]/total_processed*100:.1f}%)')
    print(f'  vol >= 3x         FAIL: {filter_failures["vol_ok"]:>4} '
          f'({filter_failures["vol_ok"]/total_processed*100:.1f}%)')
    print(f'  was_accum         FAIL: {filter_failures["accum_ok"]:>4} '
          f'({filter_failures["accum_ok"]/total_processed*100:.1f}%)')
    print(f'  body >= 1*ATR     FAIL: {filter_failures["body_ok"]:>4} '
          f'({filter_failures["body_ok"]/total_processed*100:.1f}%)')
    print(f'  RSI < 70          FAIL: {filter_failures["rsi_ok"]:>4} '
          f'({filter_failures["rsi_ok"]/total_processed*100:.1f}%)')

    print()
    print('=' * 70)
    print(f'🌋 VOLCANO setups passing ALL filters: {len(pass_all)}')
    print('=' * 70)

    if not pass_all:
        print('\n⚠ ZERO setups in 5 days! Filters too strict.')
        print('Самый restrictive фильтр выше укажет что мягчить.')
    else:
        wins = sum(1 for p in pass_all if p['outcome'] == 'WIN')
        losses = sum(1 for p in pass_all if p['outcome'] == 'LOSS')
        opens = sum(1 for p in pass_all if p['outcome'] == 'OPEN')
        wr = wins/len(pass_all)*100
        avg = sum(p['r_pct'] for p in pass_all)/len(pass_all)
        print(f'\n  WIN  (+10%): {wins:>3} ({wins/len(pass_all)*100:.1f}%)')
        print(f'  LOSS (-5%):  {losses:>3} ({losses/len(pass_all)*100:.1f}%)')
        print(f'  OPEN:        {opens:>3} ({opens/len(pass_all)*100:.1f}%)')
        print(f'  WR: {wr:.1f}%, AvgRet: {avg:+.2f}%')
        print()
        print('All Volcano setups (5d):')
        print(f'{"flip_at":<20} {"pair":<14} {"tier":<6} {"vol":>5} '
              f'{"body":>6} {"rsi":>5} {"outcome":>7} {"R%":>6}')
        print('-' * 75)
        for p in sorted(pass_all, key=lambda x: x['flip_at'], reverse=True):
            print(f'{str(p["flip_at"])[:19]:<20} {p["pair"]:<14} '
                  f'{p["tier"]:<6} {p["vol_ratio"]:>4.1f}× '
                  f'{p["body_atr"]:>4.1f}× {p["rsi"]:>4.1f} '
                  f'{p["outcome"]:>7} {p["r_pct"]:>+5.1f}%')

    # ── Test relaxed filters ────
    print()
    print('=' * 70)
    print('РЕЛАКСАЦИЯ ФИЛЬТРОВ — что если мягчить?')
    print('=' * 70)

    # Try variations
    variations = [
        ('Production (current)', dict()),
        ('accum_mult 0.7→0.85 (мягче accum)',
         {'accum_mult': 0.85}),
        ('accum_mult → 1.0 (любой vol < MA20)',
         {'accum_mult': 1.0}),
        ('body 1ATR → 0.7ATR',
         {'body_min': 0.7}),
        ('body 1ATR → 0.5ATR',
         {'body_min': 0.5}),
        ('vol 3x → 2x (мягче vol surge)',
         {'vol_min': 2.0}),
        ('lookback 12→8 (короче accum)',
         {'lookback': 8, 'ma_period': 16}),
        ('NO bad hours filter',
         {'allow_bad_hours': True}),
        ('NO RSI filter',
         {'no_rsi': True}),
        ('ALL relaxed: accum=0.85, body=0.7, vol=2x',
         {'accum_mult': 0.85, 'body_min': 0.7, 'vol_min': 2.0}),
    ]

    for label, params in variations:
        accum_mult = params.get('accum_mult', VOLCANO_ACCUM_MULT)
        body_min = params.get('body_min', VOLCANO_BODY_ATR_MIN)
        vol_min = params.get('vol_min', VOLCANO_VOL_MIN)
        lookback = params.get('lookback', VOLCANO_ACCUM_LOOKBACK)
        ma_period = params.get('ma_period', 20)
        allow_bad = params.get('allow_bad_hours', False)
        no_rsi = params.get('no_rsi', False)

        passed = []
        for st in st_signals:
            pair = st.get('pair_norm')
            candles = KLINES.get(pair)
            if not candles: continue
            flip_ts = int(st['flip_at'].timestamp() * 1000)
            idx = find_idx(candles, flip_ts)
            if idx is None or idx < ma_period + lookback + 5: continue
            if idx >= len(candles) - 5: continue

            segment = candles[:idx+1]
            hour = st['flip_at'].hour
            tier = st.get('tier', 'mtf')

            if tier not in VOLCANO_TIERS: continue
            if not allow_bad and hour in VOLCANO_BAD_HOURS: continue

            # vol ratio
            if len(segment) < 21: continue
            avg_v = sum(c.get('v', 0) for c in segment[-21:-1]) / 20
            if avg_v <= 0: continue
            vr = segment[-1].get('v', 0) / avg_v
            if vr < vol_min: continue

            # accum: avg [-3-lookback:-3] < MA(ma_period) [-3-lookback-ma_period:-3-lookback] * mult
            accum_start = -3 - lookback
            ma_start = accum_start - ma_period
            if abs(ma_start) > len(segment): continue
            accum_window = segment[accum_start:-3]
            ma_window = segment[ma_start:accum_start]
            if len(accum_window) != lookback or len(ma_window) != ma_period:
                continue
            avg_a = sum(c.get('v', 0) for c in accum_window) / lookback
            avg_m = sum(c.get('v', 0) for c in ma_window) / ma_period
            if avg_m <= 0 or avg_a >= avg_m * accum_mult:
                continue

            # body
            atr = compute_atr_last(segment, 14)
            if atr <= 0: continue
            last = segment[-1]
            body = abs(last['c'] - last['o'])
            if body / atr < body_min: continue

            # rsi
            if not no_rsi:
                rsi = compute_rsi_last(segment, 14)
                if rsi >= VOLCANO_RSI_MAX: continue

            entry = last['c']
            outcome, exit_price, bars = simulate_pct(
                candles, idx, entry, target_pct=10, stop_pct=5, max_bars=72,
            )
            r_pct = 10 if outcome == 'WIN' else (-5 if outcome == 'LOSS'
                else (exit_price - entry) / entry * 100)
            passed.append({'outcome': outcome, 'r_pct': r_pct})

        n = len(passed)
        if n == 0:
            print(f'  {label:<40} N=0')
            continue
        wins = sum(1 for p in passed if p['outcome'] == 'WIN')
        wr = wins/n*100
        avg = sum(p['r_pct'] for p in passed)/n
        marker = '🚀' if avg > 0.5 else ('✓' if avg > 0 else ' ')
        print(f'  {label:<40} N={n:>3} WR={wr:>5.1f}% AvgRet={avg:>+5.2f}% {marker}')

    print('\n=== DONE ===')


if __name__ == '__main__':
    main()
