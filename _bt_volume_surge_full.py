"""COMPREHENSIVE Volume Surge backtest — все фильтры.

Базовый сигнал: ST flip 1h + volume(на flip candle) >= K × MA20.
Тестируем добавление фильтров:
  1. Volume threshold: 2x, 3x, 5x, 8x, 10x
  2. ST tier: VIP / MTF / Daily / All
  3. Body size: candle body >= K × ATR
  4. RSI filter: skip если RSI > 75 (LONG) или RSI < 25 (SHORT)
  5. Hour filter: skip "плохих" часов (1, 6 UTC)
  6. Weekday filter: skip среды
  7. ETH correlation: same direction
  8. Confluence boost: было ли Confluence сигнал same direction за 24h
  9. CV boost: был ли CV pattern same direction за 24h
  10. Near level: confluence сигнал имел level factor
TP variants: 5%, 7%, 10%, 15%, 2R, 3R
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
ETH_CANDLES = KLINES.get('ETHUSDT', [])
print(f'Pairs cached: {len(KLINES)}, ETH candles: {len(ETH_CANDLES)}')


# ── Helpers ──────────────────────────────────────────────────────────
def find_candle_at(candles, ts_ms):
    for i, c in enumerate(candles):
        if c['t'] >= ts_ms:
            return i
    return None


def compute_atr(candles, period=14):
    if len(candles) < period + 1:
        return [None] * len(candles)
    trs = [candles[0]['h'] - candles[0]['l']]
    for i in range(1, len(candles)):
        prev = candles[i-1]['c']
        h, l = candles[i]['h'], candles[i]['l']
        trs.append(max(h - l, abs(h - prev), abs(l - prev)))
    out = [None] * (period - 1)
    cur = sum(trs[:period]) / period
    out.append(cur)
    for i in range(period, len(candles)):
        cur = (cur * (period - 1) + trs[i]) / period
        out.append(cur)
    return out


def compute_rsi(candles, period=14):
    if len(candles) < period + 1:
        return [None] * len(candles)
    closes = [c['c'] for c in candles]
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    rsi = [None] * period
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    rs = avg_g / avg_l if avg_l > 0 else 100
    rsi.append(100 - 100/(1+rs))
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period-1) + gains[i]) / period
        avg_l = (avg_l * (period-1) + losses[i]) / period
        rs = avg_g / avg_l if avg_l > 0 else 100
        rsi.append(100 - 100/(1+rs))
    return rsi


def vol_ratio_at(candles, idx, period=20):
    if idx < period:
        return None
    vols = [c.get('v', 0) for c in candles[idx-period:idx]]
    avg = sum(vols) / period
    if avg <= 0:
        return None
    return candles[idx]['v'] / avg


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
    last = candles[end-1]
    return 'OPEN', last['c']


def simulate_r(candles, entry_idx, entry_price, sl_price, direction,
                tp_r, max_bars=72):
    if direction == 'LONG':
        risk = entry_price - sl_price
        if risk <= 0: return None, None
        tp = entry_price + risk * tp_r
    else:
        risk = sl_price - entry_price
        if risk <= 0: return None, None
        tp = entry_price - risk * tp_r
    end = min(len(candles), entry_idx + max_bars + 1)
    for j in range(entry_idx + 1, end):
        c = candles[j]
        if direction == 'LONG':
            if c['l'] <= sl_price: return 'LOSS', -1.0
            if c['h'] >= tp: return 'WIN', tp_r
        else:
            if c['h'] >= sl_price: return 'LOSS', -1.0
            if c['l'] <= tp: return 'WIN', tp_r
    last = candles[end-1]
    if direction == 'LONG':
        r = (last['c'] - entry_price) / risk
    else:
        r = (entry_price - last['c']) / risk
    return 'OPEN', r


def eth_change_at(ts_ms):
    """ETH 1h change at ts_ms."""
    if not ETH_CANDLES: return None
    for i, c in enumerate(ETH_CANDLES):
        if c['t'] >= ts_ms:
            if i < 1: return None
            prev = ETH_CANDLES[i-1]
            return (c['c'] - prev['c']) / prev['c'] * 100
    return None


# ── Main: build all setups ───────────────────────────────────────────
def build_setups():
    """Для каждого ST flip собираем full feature vector."""
    since = datetime.utcnow() - timedelta(days=14)

    # ST flips
    st_signals = list(db.supertrend_signals.find({
        'flip_at': {'$gte': since},
    }))
    print(f'ST flips: {len(st_signals)}')

    # Confluence by (pair, direction, ts) for boost lookup
    conf_signals = list(db.confluence.find({
        'detected_at': {'$gte': since},
        'score': {'$gte': 4},
    }, {'pair': 1, 'direction': 1, 'detected_at': 1, 'factors': 1}))
    conf_by_pair = defaultdict(list)
    for c in conf_signals:
        key = (c.get('pair'), c.get('direction'))
        conf_by_pair[key].append(c)
    print(f'Confluence signals (score>=4): {len(conf_signals)}')

    # CV pattern triggered
    cv_signals = list(db.signals.find({
        'source': 'cryptovizor',
        'pattern_triggered': True,
        'pattern_triggered_at': {'$gte': since},
    }, {'pair': 1, 'direction': 1, 'pattern_triggered_at': 1}))
    cv_by_pair = defaultdict(list)
    for s in cv_signals:
        key = (s.get('pair'), s.get('direction'))
        cv_by_pair[key].append(s)
    print(f'CV patterns: {len(cv_signals)}')

    setups = []
    skipped_no_klines = 0

    # Pre-compute ATR/RSI for each pair
    cache_atr = {}
    cache_rsi = {}

    for st in st_signals:
        pair_norm = st.get('pair_norm')
        candles = KLINES.get(pair_norm)
        if not candles:
            skipped_no_klines += 1
            continue
        if len(candles) < 30:
            continue

        if pair_norm not in cache_atr:
            cache_atr[pair_norm] = compute_atr(candles, 14)
            cache_rsi[pair_norm] = compute_rsi(candles, 14)
        atrs = cache_atr[pair_norm]
        rsis = cache_rsi[pair_norm]

        flip_ts = int(st['flip_at'].timestamp() * 1000)
        idx = find_candle_at(candles, flip_ts)
        if idx is None or idx < 21 or idx >= len(candles) - 5:
            continue

        c = candles[idx]
        atr = atrs[idx]
        rsi = rsis[idx]
        if not atr or atr <= 0 or rsi is None:
            continue

        # Volume ratio
        vr = vol_ratio_at(candles, idx, period=20)
        if vr is None:
            continue

        # Body
        body = abs(c['c'] - c['o'])
        body_atr = body / atr

        # Time features
        flip_dt = st['flip_at']
        hour = flip_dt.hour
        weekday = flip_dt.weekday()  # 0=Mon, 6=Sun

        direction = st['direction']
        tier = st.get('tier', 'mtf')

        # ETH correlation
        eth_chg = eth_change_at(flip_ts)
        if direction == 'LONG':
            eth_aligned = (eth_chg is not None and eth_chg >= 0.2)
        else:
            eth_aligned = (eth_chg is not None and eth_chg <= -0.2)

        # Confluence boost (any conf signal same dir within 24h before flip)
        pair_slash = pair_norm[:-4] + '/USDT'
        flip_dt_naive = flip_dt
        conf_24h_ago = flip_dt_naive - timedelta(hours=24)
        has_conf = False
        for cf in conf_by_pair.get((pair_slash, direction), []):
            if conf_24h_ago <= cf['detected_at'] <= flip_dt_naive:
                has_conf = True
                break

        # CV boost
        has_cv = False
        for cv in cv_by_pair.get((pair_slash, direction), []):
            if conf_24h_ago <= cv['pattern_triggered_at'] <= flip_dt_naive:
                has_cv = True
                break

        # Near level (confluence had level factor recently)
        has_level = False
        for cf in conf_by_pair.get((pair_slash, direction), []):
            if conf_24h_ago <= cf['detected_at'] <= flip_dt_naive:
                for f in cf.get('factors') or []:
                    if f.get('type') == 'level':
                        has_level = True
                        break
                if has_level:
                    break

        # Entry/SL
        entry_price = c['c']
        sl_price = st.get('sl_price')
        if not sl_price:
            continue

        setup = {
            'pair': pair_norm, 'direction': direction, 'tier': tier,
            'idx': idx, 'candles': candles,
            'entry_price': entry_price, 'sl_price': sl_price,
            'vol_ratio': vr, 'body_atr': body_atr,
            'rsi': rsi, 'hour': hour, 'weekday': weekday,
            'eth_aligned': eth_aligned, 'has_conf': has_conf,
            'has_cv': has_cv, 'has_level': has_level,
        }
        setups.append(setup)

    print(f'Setups built: {len(setups)} (no_klines: {skipped_no_klines})')
    return setups


def evaluate_strategy(setups, **filters):
    """Apply filters and return stats for both target_pct and tp_r modes."""
    # Filters: vol_min, tier, body_min, rsi_max_long, rsi_min_short,
    # skip_hours, skip_weekday, eth_required, conf_required, cv_required, level_required
    passing = []
    for s in setups:
        if 'vol_min' in filters and s['vol_ratio'] < filters['vol_min']:
            continue
        if 'tier' in filters and filters['tier'] != 'all':
            if s['tier'] != filters['tier']:
                continue
        if 'body_min' in filters and s['body_atr'] < filters['body_min']:
            continue
        if 'rsi_max_long' in filters and s['direction'] == 'LONG':
            if s['rsi'] > filters['rsi_max_long']:
                continue
        if 'rsi_min_short' in filters and s['direction'] == 'SHORT':
            if s['rsi'] < filters['rsi_min_short']:
                continue
        if filters.get('skip_hours') and s['hour'] in filters['skip_hours']:
            continue
        if filters.get('skip_weekday') is not None and s['weekday'] == filters['skip_weekday']:
            continue
        if filters.get('eth_required') and not s['eth_aligned']:
            continue
        if filters.get('conf_required') and not s['has_conf']:
            continue
        if filters.get('cv_required') and not s['has_cv']:
            continue
        if filters.get('level_required') and not s['has_level']:
            continue
        passing.append(s)
    return passing


def stats_pct(passing, target_pct, stop_pct, max_bars=72):
    wins = losses = opens = 0
    rs = []
    for s in passing:
        outcome, exit_price = simulate_pct(
            s['candles'], s['idx'], s['entry_price'], s['direction'],
            target_pct=target_pct, stop_pct=stop_pct, max_bars=max_bars,
        )
        if outcome == 'WIN':
            wins += 1
            r_pct = target_pct
        elif outcome == 'LOSS':
            losses += 1
            r_pct = -stop_pct
        else:
            opens += 1
            if s['direction'] == 'LONG':
                r_pct = (exit_price - s['entry_price']) / s['entry_price'] * 100
            else:
                r_pct = (s['entry_price'] - exit_price) / s['entry_price'] * 100
        rs.append(r_pct)
    n = len(rs)
    if n == 0:
        return None
    wr = wins/n*100
    avg = sum(rs)/n
    return {'n': n, 'wr': wr, 'avg': avg, 'wins': wins, 'losses': losses, 'opens': opens}


def stats_r(passing, tp_r, max_bars=72):
    wins = losses = opens = 0
    rs = []
    for s in passing:
        result = simulate_r(
            s['candles'], s['idx'], s['entry_price'], s['sl_price'],
            s['direction'], tp_r=tp_r, max_bars=max_bars,
        )
        if result is None or result[0] is None:
            continue
        outcome, r = result
        if outcome == 'WIN':
            wins += 1
        elif outcome == 'LOSS':
            losses += 1
        else:
            opens += 1
        rs.append(r)
    n = len(rs)
    if n == 0:
        return None
    return {'n': n, 'wr': wins/n*100, 'avg': sum(rs)/n,
            'wins': wins, 'losses': losses, 'opens': opens}


def print_row(label, st, BE_WR=None):
    if st is None:
        print(f'{label:<40} N=---')
        return
    edge = (st['wr'] - BE_WR) if BE_WR else 0
    marker = '✓' if st['avg'] > 0 else ' '
    print(f'{label:<40} N={st["n"]:>5} WR={st["wr"]:>5.1f}% '
          f'AvgRet={st["avg"]:>+6.2f} Edge={edge:>+5.1f}% {marker}')


def main():
    setups = build_setups()
    print()
    print('=' * 90)
    print('VOLUME THRESHOLD scan (TP=2R fixed, ST band SL)')
    print('=' * 90)
    for vmin in [0, 2, 3, 5, 8, 10, 15]:
        passing = evaluate_strategy(setups, vol_min=vmin)
        st = stats_r(passing, tp_r=2.0)
        print_row(f'vol >= {vmin}x', st)

    print()
    print('=' * 90)
    print('TIER × VOL scan (TP=2R)')
    print('=' * 90)
    for tier in ['vip', 'mtf', 'daily']:
        for vmin in [0, 3, 5, 8]:
            passing = evaluate_strategy(setups, tier=tier, vol_min=vmin)
            st = stats_r(passing, tp_r=2.0)
            print_row(f'{tier:>5} + vol>={vmin}x', st)

    print()
    print('=' * 90)
    print('VOLUME SURGE BASE: vol >= 3x (TP в %, stop -5%)')
    print('=' * 90)
    base = evaluate_strategy(setups, vol_min=3.0)
    print(f'Base N: {len(base)}')
    for tp in [5, 7, 10, 15]:
        BE = 5/(tp+5)*100
        st = stats_pct(base, target_pct=tp, stop_pct=5)
        print_row(f'TP +{tp}% / SL -5%', st, BE_WR=BE)

    print()
    print('=' * 90)
    print('VOLUME SURGE BASE: vol >= 3x with R-multiples (ST band SL)')
    print('=' * 90)
    for tp_r in [1.0, 1.5, 2.0, 2.5, 3.0]:
        BE = 1/(1+tp_r)*100
        st = stats_r(base, tp_r=tp_r)
        print_row(f'TP={tp_r}R', st, BE_WR=BE)

    print()
    print('=' * 90)
    print('FILTER ABLATION (base = vol>=3x, TP=2R)')
    print('=' * 90)
    BE2 = 100/3
    base_st = stats_r(base, tp_r=2.0)
    print_row('Base: vol>=3x', base_st, BE_WR=BE2)

    # Add each filter individually
    filter_tests = [
        ('+ tier=vip',           {'vol_min': 3, 'tier': 'vip'}),
        ('+ tier=mtf',           {'vol_min': 3, 'tier': 'mtf'}),
        ('+ tier=daily',         {'vol_min': 3, 'tier': 'daily'}),
        ('+ body>=1ATR',         {'vol_min': 3, 'body_min': 1.0}),
        ('+ body>=1.5ATR',       {'vol_min': 3, 'body_min': 1.5}),
        ('+ skip RSI>75 LONG',   {'vol_min': 3, 'rsi_max_long': 75, 'rsi_min_short': 25}),
        ('+ skip RSI>70 LONG',   {'vol_min': 3, 'rsi_max_long': 70, 'rsi_min_short': 30}),
        ('+ skip hours 1,6 UTC', {'vol_min': 3, 'skip_hours': {1, 6}}),
        ('+ skip Wednesday',     {'vol_min': 3, 'skip_weekday': 2}),
        ('+ ETH aligned',        {'vol_min': 3, 'eth_required': True}),
        ('+ has Confluence 24h', {'vol_min': 3, 'conf_required': True}),
        ('+ has CV 24h',         {'vol_min': 3, 'cv_required': True}),
        ('+ has level',          {'vol_min': 3, 'level_required': True}),
    ]
    for label, flt in filter_tests:
        passing = evaluate_strategy(setups, **flt)
        st = stats_r(passing, tp_r=2.0)
        print_row(label, st, BE_WR=BE2)

    print()
    print('=' * 90)
    print('STACKED FILTERS (vol>=3x base) — combinations')
    print('=' * 90)

    combos = [
        ('vol>=3x + skip 1,6 UTC + skip Wed',
         {'vol_min': 3, 'skip_hours': {1, 6}, 'skip_weekday': 2}),
        ('vol>=3x + RSI<70 LONG + skip 1,6,Wed',
         {'vol_min': 3, 'rsi_max_long': 70, 'rsi_min_short': 30,
          'skip_hours': {1, 6}, 'skip_weekday': 2}),
        ('vol>=5x + skip 1,6,Wed',
         {'vol_min': 5, 'skip_hours': {1, 6}, 'skip_weekday': 2}),
        ('vol>=5x + tier=mtf + skip 1,6,Wed',
         {'vol_min': 5, 'tier': 'mtf', 'skip_hours': {1, 6}, 'skip_weekday': 2}),
        ('vol>=3x + body>=1ATR + skip 1,6,Wed',
         {'vol_min': 3, 'body_min': 1.0, 'skip_hours': {1, 6}, 'skip_weekday': 2}),
        ('vol>=3x + ETH + skip Wed',
         {'vol_min': 3, 'eth_required': True, 'skip_weekday': 2}),
        ('vol>=3x + Confluence boost + skip 1,6,Wed',
         {'vol_min': 3, 'conf_required': True, 'skip_hours': {1, 6}, 'skip_weekday': 2}),
        ('vol>=5x + tier=mtf + RSI<70 + skip 1,6,Wed',
         {'vol_min': 5, 'tier': 'mtf', 'rsi_max_long': 70, 'rsi_min_short': 30,
          'skip_hours': {1, 6}, 'skip_weekday': 2}),
        ('OPTIMAL? vol>=3x + body>=1ATR + RSI<70 + skip 1,6,Wed',
         {'vol_min': 3, 'body_min': 1.0, 'rsi_max_long': 70, 'rsi_min_short': 30,
          'skip_hours': {1, 6}, 'skip_weekday': 2}),
    ]
    for label, flt in combos:
        passing = evaluate_strategy(setups, **flt)
        st = stats_r(passing, tp_r=2.0)
        print_row(label, st, BE_WR=BE2)

    print()
    print('=' * 90)
    print('LONG vs SHORT — base vol>=3x, TP=2R')
    print('=' * 90)
    for direction in ['LONG', 'SHORT']:
        passing = [s for s in evaluate_strategy(setups, vol_min=3) if s['direction'] == direction]
        st = stats_r(passing, tp_r=2.0)
        print_row(f'  {direction}', st, BE_WR=BE2)

    # Volume threshold by direction
    print()
    print('VOL threshold × direction (TP=2R)')
    for vmin in [3, 5, 8]:
        for direction in ['LONG', 'SHORT']:
            passing = [s for s in evaluate_strategy(setups, vol_min=vmin)
                        if s['direction'] == direction]
            st = stats_r(passing, tp_r=2.0)
            print_row(f'  vol>={vmin}x {direction}', st, BE_WR=BE2)

    print('\n=== DONE ===')


if __name__ == '__main__':
    main()
