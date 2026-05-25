"""Quick scan: signals last 7d, forward MFE ≥30% → top 10 winners.
Использует Vision CDN (no API key, works on any IP).
"""
from __future__ import annotations
import io
import zipfile
import csv
import logging
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor

import httpx

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger('httpx').setLevel(logging.WARNING)
http = httpx.Client(timeout=15.0,
                    limits=httpx.Limits(max_connections=100, max_keepalive_connections=60))

FORWARD_BARS_15M = 480  # 5 days × 96 bars/day
MFE_THRESHOLD = 30.0
LOOKBACK_DAYS = 7


def _vision_klines(symbol: str, tf: str, days: int) -> list[dict]:
    now = datetime.now(timezone.utc)
    dates = [(now - timedelta(days=d)).strftime('%Y-%m-%d')
             for d in range(1, days + 1)]
    out = []
    def _f(ds):
        url = (f'https://data.binance.vision/data/futures/um/daily/klines/'
               f'{symbol}/{tf}/{symbol}-{tf}-{ds}.zip')
        try:
            r = http.get(url)
            if r.status_code != 200:
                return []
            zf = zipfile.ZipFile(io.BytesIO(r.content))
            rows = csv.reader(zf.read(zf.namelist()[0]).decode().splitlines())
            return [{'t': int(row[0]), 'o': float(row[1]), 'h': float(row[2]),
                     'l': float(row[3]), 'c': float(row[4]), 'v': float(row[5])}
                    for row in rows if row and row[0].isdigit()]
        except Exception:
            return []
    with ThreadPoolExecutor(max_workers=10) as tp:
        for r in tp.map(_f, dates):
            out.extend(r)
    seen = set(); uniq = []
    for k in out:
        if k['t'] in seen: continue
        seen.add(k['t']); uniq.append(k)
    uniq.sort(key=lambda x: x['t'])
    return uniq


def _find_bar_idx(kl, ts_ms):
    lo, hi = 0, len(kl) - 1
    while lo <= hi:
        m = (lo + hi) // 2
        if kl[m]['t'] <= ts_ms: lo = m + 1
        else: hi = m - 1
    return hi


def _sim_mfe(k15, idx, entry, direction='LONG'):
    fwd = k15[idx+1:idx+1+FORWARD_BARS_15M]
    if len(fwd) < 10: return None
    if direction == 'LONG':
        peak = max(k['h'] for k in fwd)
        mfe = (peak - entry) / entry * 100
        # Time to peak
        peak_idx = next(i for i, k in enumerate(fwd) if k['h'] == peak)
        peak_t = fwd[peak_idx]['t']
        peak_price = peak
    else:
        peak = min(k['l'] for k in fwd)
        mfe = (entry - peak) / entry * 100
        peak_idx = next(i for i, k in enumerate(fwd) if k['l'] == peak)
        peak_t = fwd[peak_idx]['t']
        peak_price = peak
    bars_to_peak = peak_idx + 1
    return {'mfe_pct': mfe, 'peak_t': peak_t, 'peak_price': peak_price,
            'bars_to_peak': bars_to_peak}


def main():
    from database import _get_db
    db = _get_db()
    since = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    print(f'Loading signals last {LOOKBACK_DAYS}d...', flush=True)

    # Load top-7 sources
    sigs: list = []
    for s in db.new_strategy_signals.find(
        {'created_at': {'$gte': since},
         'strategy': {'$in': ['whale', 'shark', 'combo', 'triple_confluence']}},
        {'pair': 1, 'direction': 1, 'entry': 1, 'created_at': 1, 'strategy': 1,
         'whale_tier': 1, 'shark_tier': 1, 'whale_score': 1, 'shark_score': 1}
    ):
        if not (s.get('entry') and s.get('pair')): continue
        sigs.append({
            'pair': s['pair'], 'direction': (s.get('direction','') or '').upper(),
            'entry': float(s['entry']),
            'at_ts': int(s['created_at'].timestamp()),
            'source': s.get('strategy'),
            'tier': s.get('whale_tier') or s.get('shark_tier'),
            'score': s.get('whale_score') or s.get('shark_score'),
        })
    # ST VIP
    for s in db.supertrend_signals.find(
        {'flip_at': {'$gte': since}, 'tier': 'vip'},
        {'pair': 1, 'direction': 1, 'entry_price': 1, 'flip_at': 1}
    ):
        if not (s.get('entry_price') and s.get('pair')): continue
        sigs.append({
            'pair': s['pair'], 'direction': (s.get('direction','') or '').upper(),
            'entry': float(s['entry_price']),
            'at_ts': int(s['flip_at'].timestamp()),
            'source': 'st_vip', 'tier': 'VIP', 'score': None,
        })
    # Confluence STRONG
    for s in db.confluence.find(
        {'detected_at': {'$gte': since}},
        {'pair': 1, 'direction': 1, 'price': 1, 'detected_at': 1, 'score': 1}
    ):
        if not (s.get('price') and s.get('pair')): continue
        if (s.get('score') or 0) < 5: continue
        sigs.append({
            'pair': s['pair'], 'direction': (s.get('direction','') or '').upper(),
            'entry': float(s['price']),
            'at_ts': int(s['detected_at'].timestamp()),
            'source': 'confluence', 'tier': 'STRONG', 'score': s['score'],
        })
    print(f'Loaded {len(sigs)} signals from top-7 sources', flush=True)

    # Fetch 15m klines per unique pair (parallel)
    unique_pairs = list(set(s['pair'] for s in sigs))
    print(f'Fetching 15m klines for {len(unique_pairs)} pairs...', flush=True)
    klines_cache: dict = {}
    def _fetch_pair(pair):
        sym = pair.replace('/', '').upper()
        if not sym.endswith('USDT'): sym += 'USDT'
        return (pair, _vision_klines(sym, '15m', LOOKBACK_DAYS + 5))
    with ThreadPoolExecutor(max_workers=20) as tp:
        for pair, k15 in tp.map(_fetch_pair, unique_pairs):
            klines_cache[pair] = k15

    # Simulate per signal
    print('Computing MFE per signal...', flush=True)
    winners = []
    for s in sigs:
        k15 = klines_cache.get(s['pair'], [])
        if len(k15) < 50: continue
        idx = _find_bar_idx(k15, s['at_ts'] * 1000)
        if idx < 0 or idx >= len(k15) - 10: continue
        out = _sim_mfe(k15, idx, s['entry'], s['direction'])
        if not out: continue
        if out['mfe_pct'] >= MFE_THRESHOLD:
            winners.append({**s, **out})

    winners.sort(key=lambda x: -x['mfe_pct'])
    print(f'\n=== WINNERS (MFE >= {MFE_THRESHOLD}%) — top 20 ===', flush=True)
    print(f'{"Time":<18} {"Pair":<18} {"Source":<22} {"Dir":<5} {"Tier":<10} '
          f'{"Entry":<12} {"Peak":<12} {"MFE":<8} {"Hours":<6}')
    for i, w in enumerate(winners[:20]):
        dt = datetime.fromtimestamp(w['at_ts'], tz=timezone.utc)
        hrs = round(w['bars_to_peak'] * 15 / 60, 1)
        print(f'{dt.strftime("%m-%d %H:%M UTC"):<18} {w["pair"]:<18} '
              f'{w["source"]:<22} {w["direction"]:<5} {w.get("tier","-") or "-":<10} '
              f'{w["entry"]:<12} {round(w["peak_price"],8):<12} '
              f'+{w["mfe_pct"]:.1f}%   {hrs}h')

    print(f'\nTotal winners: {len(winners)}/{len(sigs)} signals')

    # Source breakdown
    from collections import Counter
    src_cnt = Counter(w['source'] for w in winners)
    print('By source:')
    for src, cnt in src_cnt.most_common():
        print(f'  {src}: {cnt}')


if __name__ == '__main__':
    main()
