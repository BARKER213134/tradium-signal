"""Backtest: влияние EMA50/EMA200 cross (regime align) на success всех сигналов.

Гипотеза юзера: если входить в направлении последнего EMA50/200 cross на 1h
(GOLDEN→LONG, DEATH→SHORT) — насколько улучшается WR и MFE?

Метод:
1. Load all signals last 14d из cryptovizor + new_strategy + supertrend
2. Для каждого сигнала: fetch 1h klines до at_ts, find last cross BEFORE at_ts
3. Сlassify: match=True если cross direction == signal direction
4. Sim forward 72h (через 15m klines)
5. Compare: BASE vs +EMA align vs ALL crosses
"""
from __future__ import annotations
import sys, time, json, statistics
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass
sys.path.insert(0, '.')
from dotenv import load_dotenv
load_dotenv(override=True)
import logging
logging.basicConfig(level=logging.WARNING)

import httpx, io, zipfile, csv
from database import _get_db

db = _get_db()
http = httpx.Client(timeout=15.0, limits=httpx.Limits(max_connections=30, max_keepalive_connections=15))

EMA_FAST = 50
EMA_SLOW = 200
FORWARD_BARS = 288  # 72h × 4 (15m bars)
LOOKBACK_DAYS = 14
KLINES_DAYS = 30  # Need EMA200 warmup (~8d on 1h) + LOOKBACK + forward 3d


def fetch_vision(symbol, tf, date_str):
    url = f'https://data.binance.vision/data/futures/um/daily/klines/{symbol}/{tf}/{symbol}-{tf}-{date_str}.zip'
    try:
        r = http.get(url)
        if r.status_code != 200: return []
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        rows = csv.reader(zf.read(zf.namelist()[0]).decode().splitlines())
        out = []
        for row in rows:
            try:
                out.append({'t': int(row[0]), 'o': float(row[1]),
                            'h': float(row[2]), 'l': float(row[3]),
                            'c': float(row[4]), 'v': float(row[5])})
            except: continue
        return out
    except Exception:
        return []


def fetch_klines_cdn(pair, tf, days):
    """Fetch klines из Binance Vision (static CDN, no rate limits)."""
    sym = pair.replace('/', '').upper()
    if not sym.endswith('USDT'): sym += 'USDT'
    now = datetime.now(timezone.utc)
    out = []
    for d in range(1, days + 1):
        ds = (now - timedelta(days=d)).strftime('%Y-%m-%d')
        out.extend(fetch_vision(sym, tf, ds))
    # Dedupe + sort
    seen = set(); uniq = []
    for k in out:
        if k['t'] in seen: continue
        seen.add(k['t']); uniq.append(k)
    uniq.sort(key=lambda k: k['t'])
    return uniq


def calc_ema(values, period):
    if not values or len(values) < period: return []
    out = [None] * len(values)
    seed = sum(values[:period]) / period
    out[period - 1] = seed
    k = 2 / (period + 1)
    for i in range(period, len(values)):
        out[i] = values[i] * k + out[i - 1] * (1 - k)
    return out


def find_last_cross(k1h, ema_f, ema_s, at_ms):
    """Last EMA fast×slow cross BEFORE at_ms. Returns (direction, idx, t) or (None, None, None)."""
    # Find index of last bar with t <= at_ms
    end_idx = -1
    for i in range(len(k1h) - 1, -1, -1):
        if k1h[i]['t'] <= at_ms:
            end_idx = i
            break
    if end_idx < EMA_SLOW: return (None, None, None)
    for i in range(end_idx, EMA_SLOW, -1):
        f, s = ema_f[i], ema_s[i]
        fp, sp = ema_f[i-1], ema_s[i-1]
        if None in (f, s, fp, sp): continue
        if fp <= sp and f > s:
            return ('LONG', i, k1h[i]['t'])
        if fp >= sp and f < s:
            return ('SHORT', i, k1h[i]['t'])
    return (None, None, None)


def find_15m_idx(k15, t_ms):
    lo, hi = 0, len(k15) - 1
    while lo <= hi:
        m = (lo + hi) // 2
        if k15[m]['t'] <= t_ms: lo = m + 1
        else: hi = m - 1
    return hi


def sim_forward(k15, idx, entry, direction):
    fwd = k15[idx+1:idx+1+FORWARD_BARS]
    if len(fwd) < 4: return None
    is_l = direction == 'LONG'
    highs = [k['h'] for k in fwd]
    lows = [k['l'] for k in fwd]
    closes = [k['c'] for k in fwd]
    if is_l:
        mf = max(highs); ma = min(lows)
        mfe = (mf - entry) / entry * 100
        mae = (entry - ma) / entry * 100
    else:
        mf = min(lows); ma = max(highs)
        mfe = (entry - mf) / entry * 100
        mae = (ma - entry) / entry * 100
    final = (closes[-1] - entry) / entry * 100 if is_l else (entry - closes[-1]) / entry * 100
    return {'mfe_pct': mfe, 'mae_pct': mae, 'final_pct': final}


def main():
    t0 = time.time()
    since = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    print(f'Backtest: EMA{EMA_FAST}/EMA{EMA_SLOW} cross alignment vs all signals ({LOOKBACK_DAYS}d)\n')

    sigs = []
    for s in db.signals.find({'source': 'cryptovizor', 'pattern_triggered': True,
                              'pattern_triggered_at': {'$gte': since}},
                             {'pair':1,'direction':1,'pattern_price':1,'pattern_triggered_at':1}):
        e = s.get('pattern_price') or 0
        if not (e and s.get('pair')): continue
        sigs.append({'source':'cryptovizor', 'pair':s['pair'],
                     'direction':(s.get('direction','') or '').upper(),
                     'entry':float(e),'at_ts':int(s['pattern_triggered_at'].timestamp())})
    for s in db.new_strategy_signals.find({'created_at':{'$gte':since}},
                                          {'pair':1,'direction':1,'entry':1,'created_at':1,'strategy':1}):
        if not (s.get('entry') and s.get('pair')): continue
        sigs.append({'source':s.get('strategy','?'), 'pair':s['pair'],
                     'direction':(s.get('direction','') or '').upper(),
                     'entry':float(s['entry']),'at_ts':int(s['created_at'].timestamp())})
    for s in db.supertrend_signals.find({'flip_at':{'$gte':since}},
                                        {'pair':1,'direction':1,'entry_price':1,'flip_at':1}):
        if not (s.get('entry_price') and s.get('pair')): continue
        sigs.append({'source':'supertrend', 'pair':s['pair'],
                     'direction':(s.get('direction','') or '').upper(),
                     'entry':float(s['entry_price']),'at_ts':int(s['flip_at'].timestamp())})
    print(f'Loaded {len(sigs)} signals')

    by_pair = defaultdict(list)
    for s in sigs: by_pair[s['pair']].append(s)
    pairs = list(by_pair.keys())
    print(f'Unique pairs: {len(pairs)}')

    print(f'\nFetching 1h + 15m klines per pair via Vision CDN ({KLINES_DAYS}d)...')
    kl_1h = {}; kl_15m = {}
    done = [0]
    def _f(pair):
        k1h = fetch_klines_cdn(pair, '1h', KLINES_DAYS)
        k15 = fetch_klines_cdn(pair, '15m', LOOKBACK_DAYS + 4)  # signal + 72h forward
        done[0] += 1
        if done[0] % 30 == 0:
            print(f'  {done[0]}/{len(pairs)} ({time.time()-t0:.0f}s)')
        return (pair, k1h, k15)
    with ThreadPoolExecutor(max_workers=20) as tp:
        for p, k1h, k15 in tp.map(_f, pairs):
            kl_1h[p] = k1h
            kl_15m[p] = k15
    print(f'Fetched in {time.time()-t0:.0f}s')

    print('\nClassifying signals + simulating outcomes...')
    results = []
    for sig in sigs:
        pair = sig['pair']
        k1h = kl_1h.get(pair, [])
        k15 = kl_15m.get(pair, [])
        if len(k1h) < EMA_SLOW + 20 or len(k15) < 50: continue
        ts_ms = sig['at_ts'] * 1000

        closes = [c['c'] for c in k1h]
        ema_f = calc_ema(closes, EMA_FAST)
        ema_s = calc_ema(closes, EMA_SLOW)
        if not ema_f or not ema_s: continue

        cross_dir, cross_idx, cross_t = find_last_cross(k1h, ema_f, ema_s, ts_ms)
        # Bars between cross and signal (1h bars)
        bars_ago = None
        if cross_idx is not None:
            # End idx (signal bar)
            end_idx = None
            for i in range(len(k1h)-1, -1, -1):
                if k1h[i]['t'] <= ts_ms:
                    end_idx = i; break
            bars_ago = (end_idx - cross_idx) if end_idx is not None else None

        idx_15 = find_15m_idx(k15, ts_ms)
        if idx_15 < 0 or idx_15 >= len(k15) - 10: continue
        outcome = sim_forward(k15, idx_15, sig['entry'], sig['direction'])
        if not outcome: continue

        match = (cross_dir == sig['direction']) if cross_dir else None
        results.append({
            **sig,
            'cross_dir': cross_dir,
            'bars_ago': bars_ago,
            'match': match,
            **outcome,
        })

    print(f'\nAnalyzed {len(results)} signals\n')
    if not results: return

    def pct(arr, p): s = sorted(arr); return s[int(len(s)*p/100)] if s else 0
    def stats(items, label):
        if not items: return f'{label:<45s} N=0'
        mfes = [r['mfe_pct'] for r in items]
        maes = [r['mae_pct'] for r in items]
        finals = [r['final_pct'] for r in items]
        wins = sum(1 for r in items if r['final_pct'] > 0)
        ge5 = sum(1 for r in items if r['mfe_pct'] >= 5.0)
        ge10 = sum(1 for r in items if r['mfe_pct'] >= 10.0)
        return (f'{label:<45s} N={len(items):>4d} '
                f'WR={wins/len(items)*100:>5.1f}% '
                f'MFE_med={statistics.median(mfes):>+5.2f}% '
                f'MFE_avg={sum(mfes)/len(mfes):>+5.2f}% '
                f'p75={pct(mfes,75):>+5.2f}% '
                f'Fin_med={statistics.median(finals):>+5.2f}% '
                f'≥5%={ge5/len(items)*100:>5.1f}% '
                f'≥10%={ge10/len(items)*100:>5.1f}%')

    print('=' * 200)
    print('SIGNAL × EMA50/200 cross alignment (1h)')
    print('=' * 200)
    print(stats(results, 'ALL signals (base)'))
    matched = [r for r in results if r['match'] is True]
    against = [r for r in results if r['match'] is False]
    no_cross = [r for r in results if r['match'] is None]
    print(stats(matched, 'EMA align (match=True, regime=signal)'))
    print(stats(against, 'EMA against (match=False)'))
    print(stats(no_cross, 'No cross found'))

    print('\nBy source × match:')
    by_src = defaultdict(list)
    for r in results: by_src[r['source']].append(r)
    for src in sorted(by_src.keys(), key=lambda x: -len(by_src[x])):
        items = by_src[src]
        m = [r for r in items if r['match'] is True]
        a = [r for r in items if r['match'] is False]
        print(f'\n--- {src} (N={len(items)}) ---')
        print(stats(items, f'{src} ALL'))
        if m: print(stats(m, f'{src} EMA match'))
        if a: print(stats(a, f'{src} EMA against'))

    print('\nBy direction × match:')
    for direction in ['LONG', 'SHORT']:
        base = [r for r in results if r['direction'] == direction]
        if not base: continue
        m = [r for r in base if r['match'] is True]
        a = [r for r in base if r['match'] is False]
        print(f'\n--- {direction} (N={len(base)}) ---')
        print(stats(base, f'{direction} ALL'))
        if m: print(stats(m, f'{direction} EMA match'))
        if a: print(stats(a, f'{direction} EMA against'))

    # Summary delta
    print('\n' + '=' * 200)
    print('SUMMARY: насколько EMA align улучшает результат')
    print('=' * 200)
    base_wr = sum(1 for r in results if r['final_pct'] > 0) / len(results) * 100
    base_mfe = statistics.median([r['mfe_pct'] for r in results])
    if matched:
        m_wr = sum(1 for r in matched if r['final_pct'] > 0) / len(matched) * 100
        m_mfe = statistics.median([r['mfe_pct'] for r in matched])
        m_fin = statistics.median([r['final_pct'] for r in matched])
        print(f'  +EMA align:   N={len(matched):>4d} ({len(matched)/len(results)*100:>5.1f}% survival)  '
              f'WR Δ{m_wr-base_wr:>+5.1f}пп → {m_wr:>5.1f}%  '
              f'MFE Δ{m_mfe-base_mfe:>+5.2f}% → {m_mfe:>+5.2f}%  '
              f'Fin_med={m_fin:>+5.2f}%')
    if against:
        a_wr = sum(1 for r in against if r['final_pct'] > 0) / len(against) * 100
        a_mfe = statistics.median([r['mfe_pct'] for r in against])
        a_fin = statistics.median([r['final_pct'] for r in against])
        print(f'  +EMA against: N={len(against):>4d} ({len(against)/len(results)*100:>5.1f}% survival)  '
              f'WR Δ{a_wr-base_wr:>+5.1f}пп → {a_wr:>5.1f}%  '
              f'MFE Δ{a_mfe-base_mfe:>+5.2f}% → {a_mfe:>+5.2f}%  '
              f'Fin_med={a_fin:>+5.2f}%')

    # EV calc для align only
    if matched:
        print('\n' + '=' * 100)
        print('EV если торговать ТОЛЬКО EMA-aligned signals (TP/SL grid)')
        print('=' * 100)
        for tp_pct, sl_pct in [(3,2), (5,3), (7,4), (10,5)]:
            tp_hit = sum(1 for r in matched if r['mfe_pct'] >= tp_pct
                          and r['mae_pct'] < sl_pct)
            sl_hit = sum(1 for r in matched if r['mae_pct'] >= sl_pct)
            ev = (tp_hit/len(matched)) * tp_pct - (sl_hit/len(matched)) * sl_pct
            print(f'  TP=+{tp_pct}% SL=-{sl_pct}%: TP_hit={tp_hit/len(matched)*100:>5.1f}% '
                  f'SL_hit={sl_hit/len(matched)*100:>5.1f}% EV≈{ev:>+5.2f}%/trade')

    with open('_bt_signals_ema_cross_results.json', 'w', encoding='utf-8') as f:
        json.dump(results, f)
    print(f'\nTotal time: {time.time()-t0:.0f}s')


if __name__ == '__main__':
    main()
