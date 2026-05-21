"""Backtest 14d: WR/MFE каждого источника сигналов БЕЗ filter wrapper'ов.

Цель: чистая иерархия — какой источник реально лучший?

Источники (10+):
- cryptovizor (с pattern_triggered)
- tradium
- supertrend × tiers (vip/mtf/daily)
- anomaly
- confluence
- new_strategy_signals (vol_accum/volume_surge/triple_confluence/second_flip/volcano)
- rsi_cross_12h
- cv_flip (когда state=FLIPPED)
- cluster
- verified

Метод:
1. Load all signals last 14d из Mongo per source
2. Fetch 15m klines (Vision CDN parallel)
3. Forward sim 72h per signal
4. Stats: N, WR, MFE_med/avg, MAE_med, Final_med, ≥5%/10%/20%, EV grid

Output: ranked table by WR + MFE — топ-3 источников для приоритезации.
"""
from __future__ import annotations
import logging
import time
import io
import zipfile
import csv
import statistics
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import httpx

logger = logging.getLogger(__name__)
http_client = httpx.Client(timeout=15.0, limits=httpx.Limits(max_connections=30, max_keepalive_connections=20))

LOOKBACK_DAYS = 14
KLINES_15M_DAYS = 17  # 14d signals + 3d forward
FORWARD_BARS_15M = 288

_state = {
    'running': False, 'started_at': None,
    'sources_total': 0, 'pairs_to_fetch': 0, 'pairs_fetched': 0,
    'signals_total': 0, 'signals_done': 0,
    'last_pair': '', 'error': None, 'finished_at': None,
}


def _get_state():
    return dict(_state)


def _to_sym(pair):
    s = pair.replace('/', '').upper()
    if not s.endswith('USDT'): s += 'USDT'
    return s


def _vision_klines(symbol: str, tf: str, days: int) -> list:
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


def _find_bar_idx(kl, ts_ms):
    lo, hi = 0, len(kl) - 1
    while lo <= hi:
        m = (lo + hi) // 2
        if kl[m]['t'] <= ts_ms: lo = m + 1
        else: hi = m - 1
    return hi


def _sim_forward(k15, idx, entry, direction):
    fwd = k15[idx+1:idx+1+FORWARD_BARS_15M]
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


def run_signal_quality_backtest() -> dict:
    """Synchronous backtest каждого источника."""
    global _state
    _state = {
        'running': True, 'started_at': datetime.now(timezone.utc).isoformat(),
        'sources_total': 0, 'pairs_to_fetch': 0, 'pairs_fetched': 0,
        'signals_total': 0, 'signals_done': 0,
        'last_pair': '', 'error': None, 'finished_at': None,
    }
    try:
        t0 = time.time()
        from database import _get_db
        db = _get_db()
        since = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

        # Step 1: Load all signals per source
        all_sigs: list = []
        # cryptovizor (with pattern_triggered = реальные срабатывания)
        for s in db.signals.find({'source': 'cryptovizor', 'pattern_triggered': True,
                                   'pattern_triggered_at': {'$gte': since}},
                                  {'pair':1,'direction':1,'pattern_price':1,'pattern_triggered_at':1}):
            e = s.get('pattern_price') or 0
            if not (e and s.get('pair')): continue
            all_sigs.append({'src':'cryptovizor', 'pair':s['pair'],
                              'direction':(s.get('direction','') or '').upper(),
                              'entry':float(e),
                              'at_ts':int(s['pattern_triggered_at'].timestamp())})
        # tradium (received_at — приходит)
        for s in db.signals.find({'source': 'tradium', 'received_at': {'$gte': since}},
                                  {'pair':1,'direction':1,'entry':1,'received_at':1,'pattern_triggered':1}):
            e = s.get('entry') or 0
            if not (e and s.get('pair')): continue
            all_sigs.append({'src':'tradium', 'pair':s['pair'],
                              'direction':(s.get('direction','') or '').upper(),
                              'entry':float(e),
                              'at_ts':int(s['received_at'].timestamp())})
        # supertrend per tier
        for s in db.supertrend_signals.find({'flip_at':{'$gte':since}},
                                              {'pair':1,'direction':1,'entry_price':1,'flip_at':1,'tier':1}):
            if not (s.get('entry_price') and s.get('pair')): continue
            tier = s.get('tier', 'mtf')
            all_sigs.append({'src':f'st_{tier}', 'pair':s['pair'],
                              'direction':(s.get('direction','') or '').upper(),
                              'entry':float(s['entry_price']),
                              'at_ts':int(s['flip_at'].timestamp())})
        # anomaly
        for s in db.anomalies.find({'detected_at':{'$gte':since}},
                                     {'pair':1,'direction':1,'price':1,'detected_at':1}):
            if not (s.get('price') and s.get('pair')): continue
            all_sigs.append({'src':'anomaly', 'pair':s['pair'],
                              'direction':(s.get('direction','') or '').upper(),
                              'entry':float(s['price']),
                              'at_ts':int(s['detected_at'].timestamp())})
        # confluence
        for s in db.confluence.find({'detected_at':{'$gte':since}},
                                      {'pair':1,'direction':1,'price':1,'detected_at':1,'score':1}):
            if not (s.get('price') and s.get('pair')): continue
            all_sigs.append({'src':'confluence', 'pair':s['pair'],
                              'direction':(s.get('direction','') or '').upper(),
                              'entry':float(s['price']),
                              'at_ts':int(s['detected_at'].timestamp())})
        # new_strategies per strategy (vol_accum/triple_confluence/etc)
        for s in db.new_strategy_signals.find({'created_at':{'$gte':since}},
                                                {'pair':1,'direction':1,'entry':1,'created_at':1,'strategy':1}):
            if not (s.get('entry') and s.get('pair')): continue
            strat = s.get('strategy', '?')
            all_sigs.append({'src':strat, 'pair':s['pair'],
                              'direction':(s.get('direction','') or '').upper(),
                              'entry':float(s['entry']),
                              'at_ts':int(s['created_at'].timestamp())})
        # rsi_cross_12h
        try:
            for s in db.rsi_sma_cross_signals.find({'detected_at':{'$gte':since}},
                                                     {'pair':1,'direction':1,'entry':1,'detected_at':1}):
                if not (s.get('entry') and s.get('pair')): continue
                all_sigs.append({'src':'rsi_cross_12h', 'pair':s['pair'],
                                  'direction':(s.get('direction','') or '').upper(),
                                  'entry':float(s['entry']),
                                  'at_ts':int(s['detected_at'].timestamp())})
        except Exception: pass
        # cv_flip — только FLIPPED
        try:
            for s in db.cv_flip_signals.find({'cv_triggered_at':{'$gte':since},
                                                'state':'FLIPPED'},
                                               {'pair':1,'direction':1,'entry':1,'flip_price':1,
                                                 'cv_triggered_at':1,'flip_at':1}):
                e = s.get('entry') or s.get('flip_price') or 0
                if not (e and s.get('pair')): continue
                # use flip_at as entry time (когда подтвердился flip)
                ts = s.get('flip_at') or s.get('cv_triggered_at')
                if not ts: continue
                all_sigs.append({'src':'cv_flip', 'pair':s['pair'],
                                  'direction':(s.get('direction','') or '').upper(),
                                  'entry':float(e),
                                  'at_ts':int(ts.timestamp())})
        except Exception: pass
        # cluster
        try:
            for s in db.clusters.find({'trigger_at':{'$gte':since}},
                                        {'pair':1,'direction':1,'trigger_price':1,'trigger_at':1,'strength':1}):
                if not (s.get('trigger_price') and s.get('pair')): continue
                all_sigs.append({'src':f"cluster_{s.get('strength','?').lower()}", 'pair':s['pair'],
                                  'direction':(s.get('direction','') or '').upper(),
                                  'entry':float(s['trigger_price']),
                                  'at_ts':int(s['trigger_at'].timestamp())})
        except Exception: pass
        # verified
        try:
            for s in db.verified_signals.find({'created_at':{'$gte':since}},
                                                {'pair':1,'direction':1,'entry':1,'created_at':1,'verdict':1}):
                if not (s.get('entry') and s.get('pair')): continue
                all_sigs.append({'src':f"verified_{s.get('verdict','?')}", 'pair':s['pair'],
                                  'direction':(s.get('direction','') or '').upper(),
                                  'entry':float(s['entry']),
                                  'at_ts':int(s['created_at'].timestamp())})
        except Exception: pass

        _state['signals_total'] = len(all_sigs)
        logger.info(f'[sq-bt] loaded {len(all_sigs)} signals from sources')

        # Step 2: Get unique pairs, fetch 15m klines parallel
        unique_pairs = list(set(s['pair'] for s in all_sigs))
        _state['pairs_to_fetch'] = len(unique_pairs)
        logger.info(f'[sq-bt] {len(unique_pairs)} unique pairs — fetching 15m klines')

        klines_15m: dict = {}
        def _fetch_pair(pair):
            sym = _to_sym(pair)
            return (pair, _vision_klines(sym, '15m', KLINES_15M_DAYS))
        with ThreadPoolExecutor(max_workers=20) as tp:
            i = 0
            for pair, k15 in tp.map(_fetch_pair, unique_pairs):
                klines_15m[pair] = k15
                _state['pairs_fetched'] = i + 1
                _state['last_pair'] = pair
                i += 1

        # Step 3: Sim forward per signal
        results: list = []
        for sidx, sig in enumerate(all_sigs):
            _state['signals_done'] = sidx + 1
            try:
                k15 = klines_15m.get(sig['pair'], [])
                if len(k15) < 50: continue
                idx_15 = _find_bar_idx(k15, sig['at_ts'] * 1000)
                if idx_15 < 0 or idx_15 >= len(k15) - 10: continue
                outcome = _sim_forward(k15, idx_15, sig['entry'], sig['direction'])
                if not outcome: continue
                results.append({**sig, **outcome})
            except Exception:
                pass

        # Step 4: Aggregate per source
        by_src = defaultdict(list)
        for r in results: by_src[r['src']].append(r)

        def _stats(items):
            if not items: return None
            mfes = [r['mfe_pct'] for r in items]
            maes = [r['mae_pct'] for r in items]
            finals = [r['final_pct'] for r in items]
            wins = sum(1 for r in items if r['final_pct'] > 0)
            ge5 = sum(1 for r in items if r['mfe_pct'] >= 5.0)
            ge10 = sum(1 for r in items if r['mfe_pct'] >= 10.0)
            ge20 = sum(1 for r in items if r['mfe_pct'] >= 20.0)
            return {
                'n': len(items),
                'wr_pct': round(wins/len(items)*100, 1),
                'mfe_median': round(statistics.median(mfes), 2),
                'mfe_avg': round(sum(mfes)/len(mfes), 2),
                'mae_median': round(statistics.median(maes), 2),
                'final_median': round(statistics.median(finals), 2),
                'pct_ge5': round(ge5/len(items)*100, 1),
                'pct_ge10': round(ge10/len(items)*100, 1),
                'pct_ge20': round(ge20/len(items)*100, 1),
            }

        def _ev_grid(items):
            out = {}
            for tp, sl in [(3,2), (5,3), (7,4), (10,5)]:
                tp_hit = sum(1 for r in items if r['mfe_pct'] >= tp and r['mae_pct'] < sl)
                sl_hit = sum(1 for r in items if r['mae_pct'] >= sl)
                ev = (tp_hit/max(len(items),1))*tp - (sl_hit/max(len(items),1))*sl
                out[f'tp{tp}_sl{sl}'] = {
                    'tp_hit_pct': round(tp_hit/max(len(items),1)*100, 1),
                    'sl_hit_pct': round(sl_hit/max(len(items),1)*100, 1),
                    'ev_pct': round(ev, 2),
                }
            return out

        sources_report = {}
        for src in by_src:
            items = by_src[src]
            if len(items) < 5: continue  # skip small samples
            sources_report[src] = {**_stats(items), 'ev_grid': _ev_grid(items)}

        # Rank by score (WR weighted by sample size)
        ranked = sorted(
            sources_report.items(),
            key=lambda kv: -(kv[1]['wr_pct'] * (kv[1]['n'] ** 0.5) / 100),
        )

        report = {
            'lookback_days': LOOKBACK_DAYS,
            'signals_total': len(all_sigs),
            'signals_analyzed': len(results),
            'pairs_total': len(unique_pairs),
            'sources_with_5plus': len(sources_report),
            'sources_report': sources_report,
            'ranked_by_score': [
                {'src': src, **stats} for src, stats in ranked
            ],
            'started_at': _state['started_at'],
            'finished_at': datetime.now(timezone.utc).isoformat(),
            'elapsed_s': round(time.time() - t0, 1),
        }

        try:
            db.signal_quality_backtest_report.delete_many({})
            db.signal_quality_backtest_report.insert_one(report)
        except Exception as e:
            logger.warning(f'[sq-bt] mongo write fail: {e}')

        _state['running'] = False
        _state['finished_at'] = datetime.now(timezone.utc).isoformat()
        return report
    except Exception as e:
        logger.exception('[sq-bt] fatal')
        _state['error'] = str(e)
        _state['running'] = False
        _state['finished_at'] = datetime.now(timezone.utc).isoformat()
        return {'error': str(e)}
