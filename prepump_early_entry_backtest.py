"""Backtest: EARLY_ENTRY logic — был бы профит если входить ПРИ accumulation+signal?

Концепт: на каждый исторический CV/ST/anomaly signal за last 14d:
1. Был ли pair в pre_pump WATCH state в этот момент (≥2 days accumulation)?
   Используем simplified pre-pump: ≥3 signals same direction в окне 7d до at_ts.
2. Если да → EARLY_ENTRY trigger. Симулируем forward 72h через Vision CDN
   (только 15m × 10 days — много faster чем full backtest).
3. Stats: WR / MFE / EV per source.

Runs faster because:
- Не нужен fapi /openInterestHist /fundingRate (не используется в EARLY_ENTRY)
- Не нужны 1h klines × 40 days × все пары (composite score не считается)
- Только 15m klines для forward sim — кэшируем per pair

Logic совпадает с live EARLY_ENTRY в watcher.py — проверяет ту же гипотезу.
"""
from __future__ import annotations
import logging
import time
import io
import zipfile
import csv
import statistics
from datetime import datetime, timezone, timedelta
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor

import httpx

logger = logging.getLogger(__name__)

http_client = httpx.Client(timeout=15.0, limits=httpx.Limits(max_connections=30, max_keepalive_connections=20))

LOOKBACK_DAYS = 14         # окно исторических signals
KLINES_15M_DAYS = 17       # 14d signals + 3d forward
FORWARD_BARS_15M = 288     # 72h × 4

# WATCH criteria
WATCH_LOOKBACK_S = 7 * 86400
WATCH_MIN_SIGNALS = 3      # минимум 3 signals same direction
WATCH_MIN_DAYS = 2         # минимум 2 разных дня
WATCH_PRICE_BAND = 0.15    # ≤15% диапазон (мягче для backtest, в live 10%)

# In-memory state
_state = {
    'running': False, 'started_at': None, 'signals_total': 0,
    'signals_done': 0, 'matched': 0, 'last_pair': '',
    'error': None, 'finished_at': None,
}


def _get_state():
    return dict(_state)


def _vision_klines(symbol: str, tf: str, days: int = 17) -> list:
    """Binance Vision CDN — static. Parallel per day."""
    now = datetime.now(timezone.utc)
    dates = [(now - timedelta(days=d)).strftime('%Y-%m-%d') for d in range(1, days + 1)]
    out = []
    def _fetch_day(ds):
        url = f'https://data.binance.vision/data/futures/um/daily/klines/{symbol}/{tf}/{symbol}-{tf}-{ds}.zip'
        try:
            r = http_client.get(url)
            if r.status_code != 200: return []
            zf = zipfile.ZipFile(io.BytesIO(r.content))
            rows = csv.reader(zf.read(zf.namelist()[0]).decode().splitlines())
            return [{'t': int(row[0]), 'o': float(row[1]), 'h': float(row[2]),
                     'l': float(row[3]), 'c': float(row[4]), 'v': float(row[5])}
                    for row in rows if row and row[0].isdigit()]
        except Exception:
            return []
    with ThreadPoolExecutor(max_workers=10) as tp:
        for r in tp.map(_fetch_day, dates):
            out.extend(r)
    seen = set(); uniq = []
    for k in out:
        if k['t'] in seen: continue
        seen.add(k['t']); uniq.append(k)
    uniq.sort(key=lambda x: x['t'])
    return uniq


def _to_fapi_symbol(pair):
    s = pair.replace('/', '').upper()
    if not s.endswith('USDT'): s += 'USDT'
    return s


def _find_bar_idx(kl, ts_ms):
    lo, hi = 0, len(kl) - 1
    while lo <= hi:
        m = (lo + hi) // 2
        if kl[m]['t'] <= ts_ms: lo = m + 1
        else: hi = m - 1
    return hi


def _sim_forward(k15, idx, entry, direction, fwd_bars=FORWARD_BARS_15M):
    fwd = k15[idx+1:idx+1+fwd_bars]
    if len(fwd) < 4: return None
    is_l = direction == 'LONG'
    highs = [k['h'] for k in fwd]; lows = [k['l'] for k in fwd]
    closes = [k['c'] for k in fwd]
    if is_l:
        mf = max(highs); ma = min(lows)
        mfe = (mf - entry) / entry * 100; mae = (entry - ma) / entry * 100
    else:
        mf = min(lows); ma = max(highs)
        mfe = (entry - mf) / entry * 100; mae = (ma - entry) / entry * 100
    final = (closes[-1] - entry) / entry * 100 if is_l else (entry - closes[-1]) / entry * 100
    return {'mfe_pct': mfe, 'mae_pct': mae, 'final_pct': final}


def _check_watch_state(pair_sigs_sorted: list, current_ts: int, current_dir: str) -> dict:
    """pair_sigs_sorted: [(ts, src, dir, entry_price), ...] sorted by ts.
    Returns dict if pair was в WATCH state at current_ts, else None."""
    cutoff = current_ts - WATCH_LOOKBACK_S
    # only signals before current_ts (no look-ahead) same direction
    relevant = [(ts, src, d, pr) for ts, src, d, pr in pair_sigs_sorted
                if cutoff <= ts < current_ts and d == current_dir]
    if len(relevant) < WATCH_MIN_SIGNALS: return None
    days = set()
    for ts, _, _, _ in relevant:
        days.add(datetime.fromtimestamp(ts, tz=timezone.utc).date())
    if len(days) < WATCH_MIN_DAYS: return None
    prices = [pr for _, _, _, pr in relevant if pr]
    if not prices: return None
    mean_pr = sum(prices) / len(prices)
    if mean_pr <= 0: return None
    band_pct = (max(prices) - min(prices)) / mean_pr
    if band_pct > WATCH_PRICE_BAND: return None
    return {
        'days': len(days), 'total': len(relevant),
        'sources': list(set(src for _, src, _, _ in relevant)),
        'band_pct': band_pct, 'mean_price': mean_pr,
    }


def run_early_entry_backtest() -> dict:
    """Synchronous backtest entry."""
    global _state
    _state = {
        'running': True, 'started_at': datetime.now(timezone.utc).isoformat(),
        'signals_total': 0, 'signals_done': 0, 'matched': 0,
        'last_pair': '', 'error': None, 'finished_at': None,
    }
    try:
        t0 = time.time()
        from database import _get_db
        db = _get_db()
        since = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

        # Step 1: загружаем ВСЕ исторические signals (для WATCH state computation)
        all_sigs = []
        # cryptovizor
        for s in db.signals.find({'source': 'cryptovizor', 'pattern_triggered': True,
                                   'pattern_triggered_at': {'$gte': since}},
                                  {'pair':1,'direction':1,'pattern_price':1,'pattern_triggered_at':1,'pattern_name':1}):
            e = s.get('pattern_price') or 0
            if not (e and s.get('pair')): continue
            all_sigs.append({
                'source': 'cryptovizor', 'pair': s['pair'],
                'direction': (s.get('direction','') or '').upper(),
                'entry': float(e),
                'at_ts': int(s['pattern_triggered_at'].timestamp()),
                'pattern': s.get('pattern_name', ''),
            })
        # supertrend
        for s in db.supertrend_signals.find({'flip_at': {'$gte': since}},
                                              {'pair':1,'direction':1,'entry_price':1,'flip_at':1,'tier':1}):
            if not (s.get('entry_price') and s.get('pair')): continue
            all_sigs.append({
                'source': 'supertrend', 'pair': s['pair'],
                'direction': (s.get('direction','') or '').upper(),
                'entry': float(s['entry_price']),
                'at_ts': int(s['flip_at'].timestamp()),
                'pattern': f"flip {s.get('tier','?')}",
            })
        # anomaly
        for s in db.anomalies.find({'detected_at': {'$gte': since}},
                                     {'pair':1,'direction':1,'price':1,'detected_at':1,'anomalies':1}):
            if not (s.get('price') and s.get('pair')): continue
            types = [a['type'] for a in s.get('anomalies', [])][:2]
            all_sigs.append({
                'source': 'anomaly', 'pair': s['pair'],
                'direction': (s.get('direction','') or '').upper(),
                'entry': float(s['price']),
                'at_ts': int(s['detected_at'].timestamp()),
                'pattern': ', '.join(types),
            })
        # new_strategies
        for s in db.new_strategy_signals.find({'created_at': {'$gte': since}},
                                                {'pair':1,'direction':1,'entry':1,'created_at':1,'strategy':1}):
            if not (s.get('entry') and s.get('pair')): continue
            all_sigs.append({
                'source': s.get('strategy', '?'), 'pair': s['pair'],
                'direction': (s.get('direction','') or '').upper(),
                'entry': float(s['entry']),
                'at_ts': int(s['created_at'].timestamp()),
                'pattern': s.get('strategy', ''),
            })

        _state['signals_total'] = len(all_sigs)
        logger.info(f'[early-bt] loaded {len(all_sigs)} signals')

        # Step 2: Group by pair, sort by ts
        by_pair: dict = defaultdict(list)
        for s in all_sigs:
            by_pair[s['pair']].append((s['at_ts'], s['source'], s['direction'], s['entry']))
        for p in by_pair:
            by_pair[p].sort()

        # Step 3: Для каждого signal — check WATCH state, fetch 15m forward
        # Pre-fetch 15m klines для каждой пары (один раз)
        unique_pairs = list(by_pair.keys())
        logger.info(f'[early-bt] {len(unique_pairs)} unique pairs — fetching 15m klines...')

        klines_15m: dict = {}
        def _fetch_pair(pair):
            sym = _to_fapi_symbol(pair)
            return (pair, _vision_klines(sym, '15m', KLINES_15M_DAYS))
        with ThreadPoolExecutor(max_workers=15) as tp:
            i = 0
            for pair, k15 in tp.map(_fetch_pair, unique_pairs):
                klines_15m[pair] = k15
                i += 1
                if i % 25 == 0:
                    logger.info(f'[early-bt] fetched 15m for {i}/{len(unique_pairs)} pairs')

        # Step 4: Iterate signals, check WATCH, sim forward
        triggers = []
        for sidx, sig in enumerate(all_sigs):
            _state['signals_done'] = sidx + 1
            _state['last_pair'] = sig['pair']
            try:
                pair_sigs = by_pair[sig['pair']]
                watch = _check_watch_state(pair_sigs, sig['at_ts'], sig['direction'])
                if not watch: continue
                # EARLY_ENTRY triggers!
                k15 = klines_15m.get(sig['pair'], [])
                if len(k15) < 50: continue
                idx_15 = _find_bar_idx(k15, sig['at_ts'] * 1000)
                if idx_15 < 0 or idx_15 >= len(k15) - 10: continue
                outcome = _sim_forward(k15, idx_15, sig['entry'], sig['direction'])
                if not outcome: continue
                triggers.append({
                    **sig,
                    'watch_days': watch['days'],
                    'watch_total': watch['total'],
                    'watch_band': watch['band_pct'],
                    'watch_sources': watch['sources'],
                    **outcome,
                })
                _state['matched'] = len(triggers)
            except Exception as e:
                logger.debug(f'[early-bt] {sig.get("pair")}: {e}')

        logger.info(f'[early-bt] DONE: {len(triggers)} EARLY_ENTRY triggers from {len(all_sigs)} signals')

        # Step 5: Aggregate
        if not triggers:
            _state['running'] = False
            _state['finished_at'] = datetime.now(timezone.utc).isoformat()
            return {'triggers': 0, 'note': 'no early entries qualified'}

        def _stats(items, label):
            if not items: return {'label': label, 'n': 0}
            mfes = [r['mfe_pct'] for r in items]
            finals = [r['final_pct'] for r in items]
            maes = [r['mae_pct'] for r in items]
            wins = sum(1 for r in items if r['final_pct'] > 0)
            ge5 = sum(1 for r in items if r['mfe_pct'] >= 5.0)
            ge10 = sum(1 for r in items if r['mfe_pct'] >= 10.0)
            return {
                'label': label, 'n': len(items),
                'wr_pct': round(wins/len(items)*100, 1),
                'mfe_median': round(statistics.median(mfes), 2),
                'mfe_avg': round(sum(mfes)/len(mfes), 2),
                'mae_median': round(statistics.median(maes), 2),
                'final_median': round(statistics.median(finals), 2),
                'pct_ge5': round(ge5/len(items)*100, 1),
                'pct_ge10': round(ge10/len(items)*100, 1),
            }

        def _ev_grid(items):
            out = {}
            for tp, sl in [(5,3), (7,4), (10,5), (15,7)]:
                tp_hit = sum(1 for r in items if r['mfe_pct'] >= tp and r['mae_pct'] < sl)
                sl_hit = sum(1 for r in items if r['mae_pct'] >= sl)
                ev = (tp_hit/max(len(items),1))*tp - (sl_hit/max(len(items),1))*sl
                out[f'tp{tp}_sl{sl}'] = {
                    'tp_hit_pct': round(tp_hit/max(len(items),1)*100, 1),
                    'sl_hit_pct': round(sl_hit/max(len(items),1)*100, 1),
                    'ev_pct': round(ev, 2),
                }
            return out

        # Per source
        by_src = defaultdict(list)
        for r in triggers: by_src[r['source']].append(r)

        report = {
            'lookback_days': LOOKBACK_DAYS,
            'signals_scanned': len(all_sigs),
            'pairs_scanned': len(unique_pairs),
            'early_triggers_total': len(triggers),
            'overall': {**_stats(triggers, 'ALL'), 'ev_grid': _ev_grid(triggers)},
            'by_source': {src: {**_stats(items, src), 'ev_grid': _ev_grid(items)}
                          for src, items in by_src.items() if len(items) >= 5},
            'frequency_per_day': round(len(triggers) / LOOKBACK_DAYS, 2),
            'started_at': _state['started_at'],
            'finished_at': datetime.now(timezone.utc).isoformat(),
            'elapsed_s': round(time.time() - t0, 1),
        }

        # Top wins for manual review
        sorted_by_mfe = sorted(triggers, key=lambda r: -r['mfe_pct'])
        report['top_winners'] = [
            {'pair': r['pair'], 'direction': r['direction'], 'source': r['source'],
              'at_ts': r['at_ts'], 'entry': r['entry'],
              'mfe_pct': round(r['mfe_pct'], 2), 'mae_pct': round(r['mae_pct'], 2),
              'final_pct': round(r['final_pct'], 2),
              'watch_days': r['watch_days'], 'watch_total': r['watch_total']}
            for r in sorted_by_mfe[:25]
        ]

        # Save to Mongo
        try:
            db.prepump_early_backtest_report.delete_many({})
            db.prepump_early_backtest_report.insert_one(report)
        except Exception as e:
            logger.warning(f'[early-bt] mongo write fail: {e}')

        _state['running'] = False
        _state['finished_at'] = datetime.now(timezone.utc).isoformat()
        return report
    except Exception as e:
        logger.exception('[early-bt] fatal')
        _state['error'] = str(e)
        _state['running'] = False
        _state['finished_at'] = datetime.now(timezone.utc).isoformat()
        return {'error': str(e)}
