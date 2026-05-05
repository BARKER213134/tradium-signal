"""WINNERS ANALYSIS: что общее у Volume Surge trades которые дошли до +10%?

Подход:
1. Берём все VS LONG (vol>=3x, MTF/Daily) за 14 дней
2. Симулируем с target +10% / stop -5%
3. Для каждого setup считаем 20+ features:
   - Volume features: ratio, surge intensity, vol trend last 5 bars
   - Price action: body/ATR, wick ratios, range position
   - Trend: ATR/price, distance from MA20, slope last 10 bars
   - Time: hour, weekday, days since pair started moving
   - Multi-TF: 4h trend (synthetic from 1h candles)
   - Position: distance to recent high/low
   - Volatility: ATR pct, range last 24h
4. Compare distributions WIN vs LOSS
5. Find features with biggest WIN/LOSS divergence
6. Test new filters based on findings
"""
import json
import sys
from datetime import datetime, timedelta
from collections import defaultdict
from statistics import mean, stdev, median
from pymongo import MongoClient

sys.stdout.reconfigure(encoding='utf-8')

uri = 'mongodb+srv://jameswood_db_user:P4bJfNu63sMVnRPG@cluster0.rawzyzc.mongodb.net/?appName=Cluster0'
client = MongoClient(uri, serverSelectionTimeoutMS=15000)
db = client['tradium']

print('Loading klines cache...')
with open('_bt_klines_cache.json') as f:
    KLINES = json.load(f)
ETH = KLINES.get('ETHUSDT', [])
BTC = KLINES.get('BTCUSDT', [])
print(f'Pairs: {len(KLINES)}, ETH: {len(ETH)}, BTC: {len(BTC)}')


def find_idx(candles, ts):
    for i, c in enumerate(candles):
        if c['t'] >= ts:
            return i
    return None


def compute_atr(candles, period=14):
    if len(candles) < period+1:
        return [None]*len(candles)
    trs = [candles[0]['h']-candles[0]['l']]
    for i in range(1, len(candles)):
        p = candles[i-1]['c']
        h, l = candles[i]['h'], candles[i]['l']
        trs.append(max(h-l, abs(h-p), abs(l-p)))
    out = [None]*(period-1)
    cur = sum(trs[:period])/period
    out.append(cur)
    for i in range(period, len(candles)):
        cur = (cur*(period-1)+trs[i])/period
        out.append(cur)
    return out


def compute_rsi(candles, period=14):
    if len(candles) < period+1:
        return [None]*len(candles)
    closes = [c['c'] for c in candles]
    g, l = [], []
    for i in range(1, len(closes)):
        d = closes[i]-closes[i-1]
        g.append(max(d,0)); l.append(max(-d,0))
    rsi = [None]*period
    ag = sum(g[:period])/period
    al = sum(l[:period])/period
    rs = ag/al if al>0 else 100
    rsi.append(100-100/(1+rs))
    for i in range(period, len(g)):
        ag = (ag*(period-1)+g[i])/period
        al = (al*(period-1)+l[i])/period
        rs = ag/al if al>0 else 100
        rsi.append(100-100/(1+rs))
    return rsi


def extract_features(candles, idx, st_data, atrs, rsis):
    """Извлекаем 20+ features для свечи idx."""
    if idx < 25 or idx >= len(candles)-1:
        return None
    c = candles[idx]
    atr = atrs[idx]
    rsi = rsis[idx]
    if not atr or atr <= 0 or rsi is None:
        return None

    # Vol features
    vol_window = [cc.get('v', 0) for cc in candles[idx-20:idx]]
    avg_vol = sum(vol_window)/20
    if avg_vol <= 0: return None
    vol_ratio = c['v']/avg_vol

    # Vol trend last 5 bars (растёт ли объём)
    last5_vols = [cc.get('v', 0) for cc in candles[idx-5:idx]]
    vol_trend_inc = sum(1 for i in range(1,5) if last5_vols[i] > last5_vols[i-1])

    # Price action
    body = abs(c['c']-c['o'])
    body_atr = body/atr
    high_wick = c['h'] - max(c['o'], c['c'])
    low_wick = min(c['o'], c['c']) - c['l']
    wick_ratio_top = high_wick/body if body > 0 else 0
    wick_ratio_bot = low_wick/body if body > 0 else 0

    # Range position в last 24h
    last24 = candles[idx-23:idx+1]
    h24 = max(cc['h'] for cc in last24)
    l24 = min(cc['l'] for cc in last24)
    if h24 == l24:
        range_pos = 0.5
    else:
        range_pos = (c['c'] - l24) / (h24 - l24)

    # Trend strength: slope of close last 10 bars
    closes10 = [cc['c'] for cc in candles[idx-9:idx+1]]
    n = len(closes10)
    sx = sum(range(n))
    sy = sum(closes10)
    sxy = sum(i*closes10[i] for i in range(n))
    sxx = sum(i*i for i in range(n))
    slope = (n*sxy - sx*sy) / (n*sxx - sx*sx) if (n*sxx - sx*sx) != 0 else 0
    slope_pct = slope / closes10[0] * 100 if closes10[0] > 0 else 0

    # Volatility: ATR % of price
    atr_pct = atr/c['c']*100

    # Distance from MA20
    ma20 = sum(cc['c'] for cc in candles[idx-19:idx+1])/20
    dist_ma20_pct = (c['c']-ma20)/ma20*100

    # Time
    dt = datetime.utcfromtimestamp(c['t']/1000)
    hour = dt.hour
    weekday = dt.weekday()

    # ETH change 1h before this signal
    eth_chg = None
    if ETH:
        eth_idx = find_idx(ETH, c['t'])
        if eth_idx and eth_idx >= 1:
            prev_eth = ETH[eth_idx-1]['c']
            eth_chg = (ETH[eth_idx]['c']-prev_eth)/prev_eth*100

    # BTC change 1h
    btc_chg = None
    if BTC:
        btc_idx = find_idx(BTC, c['t'])
        if btc_idx and btc_idx >= 1:
            prev_btc = BTC[btc_idx-1]['c']
            btc_chg = (BTC[btc_idx]['c']-prev_btc)/prev_btc*100

    # 24h change
    if idx >= 24:
        change_24h = (c['c']-candles[idx-24]['c'])/candles[idx-24]['c']*100
    else:
        change_24h = 0

    # Distance from local low last 24h (LONG enters preference)
    low_dist_pct = (c['c']-l24)/l24*100 if l24 > 0 else 0

    # 4h synthetic trend (last 4 candles all green = +1, all red = -1)
    last4 = candles[idx-3:idx+1]
    greens4 = sum(1 for cc in last4 if cc['c'] > cc['o'])
    trend_4bar = greens4 - 2  # -2..+2 (from 0..4 greens)

    # Volume spike standalone vs preceded by accumulation
    # accumulation = avg vol last 12 bars BEFORE last 3 < MA20 * 0.7
    if idx >= 35:
        accum_window = candles[idx-15:idx-3]  # 12 bars
        accum_avg = sum(cc.get('v', 0) for cc in accum_window) / 12
        ma20_back = sum(c2.get('v', 0) for c2 in candles[idx-20-3:idx-3]) / 20
        was_accum = accum_avg < ma20_back * 0.7
    else:
        was_accum = False

    return {
        'pair': st_data.get('pair_norm'),
        'tier': st_data.get('tier', 'mtf'),
        'direction': st_data['direction'],
        'idx': idx, 'candles': candles,
        'entry_price': c['c'], 'sl_price': st_data.get('sl_price'),
        # Features
        'vol_ratio': vol_ratio,
        'vol_trend_inc': vol_trend_inc,  # 0-4 bars increasing
        'body_atr': body_atr,
        'wick_top': wick_ratio_top,
        'wick_bot': wick_ratio_bot,
        'range_pos_24h': range_pos,
        'slope_10': slope_pct,
        'atr_pct': atr_pct,
        'dist_ma20_pct': dist_ma20_pct,
        'rsi': rsi,
        'hour': hour,
        'weekday': weekday,
        'eth_chg': eth_chg,
        'btc_chg': btc_chg,
        'change_24h': change_24h,
        'low_dist_pct': low_dist_pct,
        'trend_4bar': trend_4bar,
        'was_accum': was_accum,
    }


def simulate(s, target_pct=10, stop_pct=5, max_bars=72):
    candles = s['candles']
    idx = s['idx']
    entry = s['entry_price']
    direction = s['direction']
    if direction == 'LONG':
        tp = entry*(1+target_pct/100)
        sl = entry*(1-stop_pct/100)
    else:
        tp = entry*(1-target_pct/100)
        sl = entry*(1+stop_pct/100)
    end = min(len(candles), idx+max_bars+1)
    for j in range(idx+1, end):
        c = candles[j]
        if direction == 'LONG':
            if c['l'] <= sl: return 'LOSS', j-idx
            if c['h'] >= tp: return 'WIN', j-idx
        else:
            if c['h'] >= sl: return 'LOSS', j-idx
            if c['l'] <= tp: return 'WIN', j-idx
    return 'OPEN', max_bars


def main():
    since = datetime.utcnow() - timedelta(days=14)
    print('Loading ST signals...')
    st_signals = list(db.supertrend_signals.find({'flip_at': {'$gte': since}}))
    print(f'ST flips: {len(st_signals)}')

    setups = []
    cache_atr, cache_rsi = {}, {}

    for st in st_signals:
        pair = st.get('pair_norm')
        candles = KLINES.get(pair)
        if not candles or len(candles) < 50:
            continue
        if pair not in cache_atr:
            cache_atr[pair] = compute_atr(candles, 14)
            cache_rsi[pair] = compute_rsi(candles, 14)
        atrs = cache_atr[pair]
        rsis = cache_rsi[pair]

        flip_ts = int(st['flip_at'].timestamp()*1000)
        idx = find_idx(candles, flip_ts)
        if idx is None: continue

        feat = extract_features(candles, idx, st, atrs, rsis)
        if not feat: continue

        # Filter base: vol>=3x, LONG, MTF/Daily (skip VIP per earlier finding)
        if feat['vol_ratio'] < 3.0: continue
        if feat['direction'] != 'LONG': continue
        if feat['tier'] not in ('mtf', 'daily'): continue

        outcome, bars = simulate(feat, target_pct=10, stop_pct=5, max_bars=72)
        feat['outcome'] = outcome
        feat['bars_held'] = bars
        setups.append(feat)

    n_total = len(setups)
    wins = [s for s in setups if s['outcome'] == 'WIN']
    losses = [s for s in setups if s['outcome'] == 'LOSS']
    opens = [s for s in setups if s['outcome'] == 'OPEN']
    n_w, n_l, n_o = len(wins), len(losses), len(opens)
    print()
    print(f'Total VS LONG MTF/Daily setups: {n_total}')
    print(f'  WIN (+10% hit):  {n_w} ({n_w/n_total*100:.1f}%)')
    print(f'  LOSS (-5% stop): {n_l} ({n_l/n_total*100:.1f}%)')
    print(f'  OPEN (timeout):  {n_o} ({n_o/n_total*100:.1f}%)')

    print()
    print('=' * 90)
    print('FEATURE COMPARISON: Winners vs Losers vs Opens (median)')
    print('=' * 90)
    print(f'{"Feature":<22} {"WIN":>10} {"LOSS":>10} {"OPEN":>10} {"W-L diff":>10}')
    print('-' * 75)

    feature_names = [
        'vol_ratio', 'vol_trend_inc', 'body_atr', 'wick_top', 'wick_bot',
        'range_pos_24h', 'slope_10', 'atr_pct', 'dist_ma20_pct',
        'rsi', 'hour', 'weekday', 'eth_chg', 'btc_chg', 'change_24h',
        'low_dist_pct', 'trend_4bar',
    ]

    feature_stats = {}
    for f in feature_names:
        w_vals = [s[f] for s in wins if s.get(f) is not None]
        l_vals = [s[f] for s in losses if s.get(f) is not None]
        o_vals = [s[f] for s in opens if s.get(f) is not None]
        if not w_vals or not l_vals: continue
        w_med = median(w_vals)
        l_med = median(l_vals)
        o_med = median(o_vals) if o_vals else None
        diff = w_med - l_med
        feature_stats[f] = (w_med, l_med, diff)
        sig = '⭐' if abs(diff) > 0.1 * (abs(w_med) + abs(l_med) + 0.01) / 2 else ''
        print(f'{f:<22} {w_med:>+10.3f} {l_med:>+10.3f} '
              f'{(o_med if o_med is not None else 0):>+10.3f} {diff:>+10.3f} {sig}')

    # Boolean was_accum — отдельный анализ
    w_accum = sum(1 for s in wins if s.get('was_accum'))
    l_accum = sum(1 for s in losses if s.get('was_accum'))
    print(f'\n{"was_accum True %":<22} {w_accum/n_w*100:>9.1f}% {l_accum/n_l*100:>9.1f}%')

    # ── Hour distribution ────
    print()
    print('=' * 90)
    print('HOUR DISTRIBUTION (LONG MTF/Daily, vol>=3x)')
    print('=' * 90)
    print(f'{"Hour":>4} {"WIN%":>7} {"WR":>7} {"N":>5}')
    by_hour = defaultdict(lambda: {'w': 0, 'l': 0, 'o': 0})
    for s in setups:
        by_hour[s['hour']][s['outcome'][0].lower()] += 1
    for h in range(24):
        d = by_hour.get(h)
        if not d: continue
        n = d['w'] + d['l'] + d['o']
        if n < 5: continue
        wr = d['w']/n*100
        marker = '🔥' if wr >= 30 else ('💀' if wr <= 15 else '')
        print(f'{h:>3}h {wr:>6.1f}% {wr:>6.1f}% {n:>5} {marker}')

    # ── Range position deciles ────
    print()
    print('=' * 90)
    print('RANGE POSITION 24h × WR (где в 24h диапазоне находится цена)')
    print('=' * 90)
    print(f'{"Decile":>8} {"Range":>15} {"N":>5} {"WIN%":>7}')
    by_dec = defaultdict(list)
    for s in setups:
        d = min(9, int(s['range_pos_24h']*10))
        by_dec[d].append(s)
    for d in range(10):
        items = by_dec.get(d, [])
        if len(items) < 10: continue
        n = len(items)
        wins_d = sum(1 for x in items if x['outcome']=='WIN')
        wr = wins_d/n*100
        rng = f'{d*10}-{(d+1)*10}%'
        print(f'  dec{d:>2} {rng:>10} {n:>5} {wr:>6.1f}%')

    # ── RSI buckets ────
    print()
    print('=' * 90)
    print('RSI bucket × WR')
    print('=' * 90)
    rsi_buckets = [(0,30,'oversold'), (30,40,'low'), (40,50,'mid-low'),
                    (50,60,'mid-high'), (60,70,'elevated'), (70,80,'overbought'),
                    (80,100,'extreme')]
    for lo, hi, label in rsi_buckets:
        bucket = [s for s in setups if lo <= s['rsi'] < hi]
        if len(bucket) < 5: continue
        n = len(bucket)
        wins_b = sum(1 for x in bucket if x['outcome']=='WIN')
        wr = wins_b/n*100
        print(f'  RSI {lo}-{hi} ({label:<11}) N={n:>4} WIN%={wr:>5.1f}%')

    # ── 24h change buckets ────
    print()
    print('=' * 90)
    print('24h CHANGE x WR (LONG — should be rising or falling already?)')
    print('=' * 90)
    chg_buckets = [(-100,-10,'crashed'), (-10,-5,'down strong'), (-5,-2,'down'),
                    (-2,0,'flat-down'), (0,2,'flat-up'), (2,5,'up'),
                    (5,10,'up strong'), (10,100,'pumping')]
    for lo, hi, label in chg_buckets:
        bucket = [s for s in setups if lo <= s['change_24h'] < hi]
        if len(bucket) < 5: continue
        n = len(bucket)
        wins_b = sum(1 for x in bucket if x['outcome']=='WIN')
        wr = wins_b/n*100
        print(f'  chg {lo:>+4}..{hi:>+4}% ({label:<12}) N={n:>4} WIN%={wr:>5.1f}%')

    # ── BTC change при сигнале ────
    print()
    print('=' * 90)
    print('BTC 1h change at signal × WR')
    print('=' * 90)
    btc_buckets = [(-5,-1,'down strong'), (-1,-0.3,'down mild'),
                    (-0.3,0.3,'flat'), (0.3,1,'up mild'), (1,5,'up strong')]
    for lo, hi, label in btc_buckets:
        bucket = [s for s in setups if s.get('btc_chg') is not None
                  and lo <= s['btc_chg'] < hi]
        if len(bucket) < 5: continue
        n = len(bucket)
        wins_b = sum(1 for x in bucket if x['outcome']=='WIN')
        wr = wins_b/n*100
        print(f'  BTC {lo:>+4}..{hi:>+4}% ({label:<12}) N={n:>4} WIN%={wr:>5.1f}%')

    # ── Body/ATR buckets ────
    print()
    print('=' * 90)
    print('BODY/ATR ratio × WR')
    print('=' * 90)
    for lo, hi in [(0,0.5), (0.5,1.0), (1.0,1.5), (1.5,2.0), (2.0,5.0)]:
        bucket = [s for s in setups if lo <= s['body_atr'] < hi]
        if len(bucket) < 5: continue
        n = len(bucket)
        wins_b = sum(1 for x in bucket if x['outcome']=='WIN')
        wr = wins_b/n*100
        print(f'  body/ATR {lo:.1f}-{hi:.1f}  N={n:>4} WIN%={wr:>5.1f}%')

    # ── Combined: best filters discovered ────
    print()
    print('=' * 90)
    print('TEST NEW FILTERS based on findings')
    print('=' * 90)

    BE_WR = 5/(10+5)*100  # for TP+10/SL-5

    def filter_test(label, predicate):
        passing = [s for s in setups if predicate(s)]
        n = len(passing)
        if n == 0:
            print(f'{label:<55} N=---')
            return
        wins_p = sum(1 for s in passing if s['outcome'] == 'WIN')
        wr = wins_p/n*100
        rs = []
        for s in passing:
            if s['outcome'] == 'WIN': rs.append(10)
            elif s['outcome'] == 'LOSS': rs.append(-5)
            else:
                # estimate from current position via simulate at hold=72h close
                rs.append(0)
        avg = sum(rs)/n
        edge = wr - BE_WR
        marker = '🚀' if avg > 0.5 else ('✓' if avg > 0 else '')
        print(f'{label:<55} N={n:>5} WR={wr:>5.1f}% Edge={edge:>+5.1f}% '
              f'AvgRet={avg:>+5.2f}% {marker}')

    print(f'BE WR (target+10/stop-5): {BE_WR:.0f}%\n')
    filter_test('Base: VS LONG MTF/Daily', lambda s: True)
    filter_test('+ RSI < 70', lambda s: s['rsi'] < 70)
    filter_test('+ RSI < 60', lambda s: s['rsi'] < 60)
    filter_test('+ range_pos > 0.7 (top of 24h)', lambda s: s['range_pos_24h'] > 0.7)
    filter_test('+ range_pos < 0.3 (bottom of 24h)', lambda s: s['range_pos_24h'] < 0.3)
    filter_test('+ change_24h > 0', lambda s: s['change_24h'] > 0)
    filter_test('+ change_24h > 5%', lambda s: s['change_24h'] > 5)
    filter_test('+ change_24h < -5% (oversold)', lambda s: s['change_24h'] < -5)
    filter_test('+ btc_chg > 0', lambda s: s.get('btc_chg') is not None and s['btc_chg'] > 0)
    filter_test('+ btc_chg > 0.3', lambda s: s.get('btc_chg') is not None and s['btc_chg'] > 0.3)
    filter_test('+ slope_10 > 0', lambda s: s['slope_10'] > 0)
    filter_test('+ body_atr >= 1.0', lambda s: s['body_atr'] >= 1.0)
    filter_test('+ body_atr >= 1.5', lambda s: s['body_atr'] >= 1.5)
    filter_test('+ trend_4bar >= 1', lambda s: s['trend_4bar'] >= 1)
    filter_test('+ trend_4bar >= 2', lambda s: s['trend_4bar'] >= 2)
    filter_test('+ vol_trend_inc >= 3', lambda s: s['vol_trend_inc'] >= 3)
    filter_test('+ atr_pct >= 1.5%', lambda s: s['atr_pct'] >= 1.5)
    filter_test('+ atr_pct < 3%', lambda s: s['atr_pct'] < 3)
    filter_test('+ dist_ma20_pct < 5%', lambda s: abs(s['dist_ma20_pct']) < 5)
    filter_test('+ wick_top < 0.5', lambda s: s['wick_top'] < 0.5)
    filter_test('+ was_accum True', lambda s: s.get('was_accum'))
    filter_test('+ was_accum False', lambda s: not s.get('was_accum'))
    filter_test('+ vol_ratio >= 5x', lambda s: s['vol_ratio'] >= 5)
    filter_test('+ vol_ratio >= 8x', lambda s: s['vol_ratio'] >= 8)

    print()
    print('STACKED filters (combinations):')
    filter_test('RSI<70 + body>=1ATR + trend_4bar>=1',
                lambda s: s['rsi'] < 70 and s['body_atr'] >= 1.0 and s['trend_4bar'] >= 1)
    filter_test('RSI<70 + body>=1.5 + slope>0',
                lambda s: s['rsi'] < 70 and s['body_atr'] >= 1.5 and s['slope_10'] > 0)
    filter_test('change_24h>0 + body>=1.5 + RSI<70',
                lambda s: s['change_24h'] > 0 and s['body_atr'] >= 1.5 and s['rsi'] < 70)
    filter_test('btc_chg>0 + RSI<70 + body>=1',
                lambda s: s.get('btc_chg') and s['btc_chg'] > 0 and s['rsi'] < 70 and s['body_atr'] >= 1)
    filter_test('btc_chg>0.3 + body>=1.5 + RSI<70',
                lambda s: s.get('btc_chg') and s['btc_chg'] > 0.3 and s['body_atr'] >= 1.5 and s['rsi'] < 70)
    filter_test('SUPER: BTC up + change>0 + body>=1 + RSI<70 + trend_4bar>=1',
                lambda s: (s.get('btc_chg') and s['btc_chg'] > 0
                            and s['change_24h'] > 0
                            and s['body_atr'] >= 1.0
                            and s['rsi'] < 70
                            and s['trend_4bar'] >= 1))
    filter_test('was_accum + body>=1 + RSI<70',
                lambda s: s.get('was_accum') and s['body_atr'] >= 1 and s['rsi'] < 70)
    filter_test('vol>=8x + body>=1.5 + RSI<70',
                lambda s: s['vol_ratio'] >= 8 and s['body_atr'] >= 1.5 and s['rsi'] < 70)

    print('\n=== DONE ===')


if __name__ == '__main__':
    main()
