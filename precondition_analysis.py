"""Анализ: какие сигналы предшествовали WINNING моментам triple_confluence /
st_vip / confluence (топ-3 источники с WR ≥ 55%).

Цель: найти "stack pattern" — какие комбинации источников ПРИВОДЯТ к pump.

Метод:
1. Загружаем все winners из топ-3 источников (Fin>0 ИЛИ MFE≥10%) за 14d.
2. Для каждого winner: look back 24h на той же паре same direction,
   собираем все источники signals.
3. Aggregate: какие источники чаще всего были ДО winner.
4. Comparison: то же для LOSER (Fin<-3% AND MFE<5%) — что отличается.

Output:
- Per winner source: какие preceding sources чаще встречаются
- Pattern: top combinations 2-3-4 source stack
- WIN vs LOSER preconditions difference
"""
from __future__ import annotations
import logging
import time
import io
import zipfile
import csv
import statistics
import bisect
from datetime import datetime, timezone, timedelta
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor

import httpx

logger = logging.getLogger(__name__)
http_client = httpx.Client(timeout=15.0, limits=httpx.Limits(max_connections=30, max_keepalive_connections=20))

LOOKBACK_DAYS = 14
KLINES_15M_DAYS = 17
FORWARD_BARS = 288     # 72h
PRECEDING_WINDOW_S = 24 * 3600  # 24h before signal
TARGET_SOURCES = ('triple_confluence', 'st_vip', 'confluence')
WINNER_MFE_TH = 10.0   # MFE >= 10% = winner
LOSER_FIN_TH = -3.0    # Final <= -3% = loser

_state = {'running': False, 'progress': '', 'error': None,
          'started_at': None, 'finished_at': None}


def _get_state():
    return dict(_state)


def _to_sym(pair):
    s = pair.replace('/', '').upper()
    if not s.endswith('USDT'): s += 'USDT'
    return s


def _vision_klines(symbol, tf, days):
    now = datetime.now(timezone.utc)
    dates = [(now - timedelta(days=d)).strftime('%Y-%m-%d') for d in range(1, days + 1)]
    out = []
    def _fd(ds):
        url = f'https://data.binance.vision/data/futures/um/daily/klines/{symbol}/{tf}/{symbol}-{tf}-{ds}.zip'
        try:
            r = http_client.get(url)
            if r.status_code != 200: return []
            zf = zipfile.ZipFile(io.BytesIO(r.content))
            rows = csv.reader(zf.read(zf.namelist()[0]).decode().splitlines())
            return [{'t': int(row[0]), 'h': float(row[2]), 'l': float(row[3]),
                     'c': float(row[4])} for row in rows if row and row[0].isdigit()]
        except Exception:
            return []
    with ThreadPoolExecutor(max_workers=10) as tp:
        for r in tp.map(_fd, dates): out.extend(r)
    seen = set(); uniq = []
    for k in out:
        if k['t'] in seen: continue
        seen.add(k['t']); uniq.append(k)
    uniq.sort(key=lambda x: x['t'])
    return uniq


def _find_idx(kl, ts_ms):
    lo, hi = 0, len(kl) - 1
    while lo <= hi:
        m = (lo + hi) // 2
        if kl[m]['t'] <= ts_ms: lo = m + 1
        else: hi = m - 1
    return hi


def _sim_forward(k15, idx, entry, direction):
    fwd = k15[idx+1:idx+1+FORWARD_BARS]
    if len(fwd) < 4: return None
    is_l = direction == 'LONG'
    h = [k['h'] for k in fwd]; l = [k['l'] for k in fwd]; c = [k['c'] for k in fwd]
    if is_l:
        mfe = (max(h)-entry)/entry*100; mae = (entry-min(l))/entry*100
    else:
        mfe = (entry-min(l))/entry*100; mae = (max(h)-entry)/entry*100
    final = (c[-1]-entry)/entry*100 if is_l else (entry-c[-1])/entry*100
    return {'mfe_pct': mfe, 'mae_pct': mae, 'final_pct': final}


def run_precondition_analysis() -> dict:
    """Main entry."""
    global _state
    _state = {'running': True, 'progress': 'starting',
              'started_at': datetime.now(timezone.utc).isoformat(),
              'error': None, 'finished_at': None}
    try:
        t0 = time.time()
        from database import _get_db
        db = _get_db()
        since = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

        # === Load ALL signals (any source) — для look-back lookup ===
        _state['progress'] = 'loading_all_signals'
        all_sigs: list = []  # каждый (pair, ts, src, direction)

        # cryptovizor
        for s in db.signals.find({'source': 'cryptovizor', 'pattern_triggered': True,
                                   'pattern_triggered_at': {'$gte': since}},
                                  {'pair':1,'direction':1,'pattern_triggered_at':1}):
            if s.get('pair'): all_sigs.append((s['pair'],
                int(s['pattern_triggered_at'].timestamp()),
                'cryptovizor',
                (s.get('direction','') or '').upper()))
        # tradium
        for s in db.signals.find({'source': 'tradium', 'received_at': {'$gte': since}},
                                  {'pair':1,'direction':1,'received_at':1}):
            if s.get('pair'): all_sigs.append((s['pair'], int(s['received_at'].timestamp()),
                'tradium', (s.get('direction','') or '').upper()))
        # supertrend per tier
        for s in db.supertrend_signals.find({'flip_at':{'$gte':since}},
                                              {'pair':1,'direction':1,'flip_at':1,'tier':1}):
            if s.get('pair'): all_sigs.append((s['pair'], int(s['flip_at'].timestamp()),
                f"st_{s.get('tier','mtf')}", (s.get('direction','') or '').upper()))
        # anomaly
        for s in db.anomalies.find({'detected_at':{'$gte':since}},
                                     {'pair':1,'direction':1,'detected_at':1}):
            if s.get('pair'): all_sigs.append((s['pair'], int(s['detected_at'].timestamp()),
                'anomaly', (s.get('direction','') or '').upper()))
        # confluence
        for s in db.confluence.find({'detected_at':{'$gte':since}},
                                      {'pair':1,'direction':1,'detected_at':1}):
            if s.get('pair'): all_sigs.append((s['pair'], int(s['detected_at'].timestamp()),
                'confluence', (s.get('direction','') or '').upper()))
        # new_strategies
        for s in db.new_strategy_signals.find({'created_at':{'$gte':since}},
                                                {'pair':1,'direction':1,'created_at':1,'strategy':1}):
            if s.get('pair'): all_sigs.append((s['pair'], int(s['created_at'].timestamp()),
                s.get('strategy','?'), (s.get('direction','') or '').upper()))
        # cv_flip FLIPPED
        try:
            for s in db.cv_flip_signals.find({'cv_triggered_at':{'$gte':since}},
                                              {'pair':1,'direction':1,'cv_triggered_at':1,'state':1}):
                if s.get('pair'): all_sigs.append((s['pair'], int(s['cv_triggered_at'].timestamp()),
                    'cv_flip', (s.get('direction','') or '').upper()))
        except Exception: pass
        # cluster
        try:
            for s in db.clusters.find({'trigger_at':{'$gte':since}},
                                        {'pair':1,'direction':1,'trigger_at':1}):
                if s.get('pair'): all_sigs.append((s['pair'], int(s['trigger_at'].timestamp()),
                    'cluster', (s.get('direction','') or '').upper()))
        except Exception: pass
        # rsi_cross
        try:
            for s in db.rsi_sma_cross_signals.find({'detected_at':{'$gte':since}},
                                                     {'pair':1,'direction':1,'detected_at':1}):
                if s.get('pair'): all_sigs.append((s['pair'], int(s['detected_at'].timestamp()),
                    'rsi_cross_12h', (s.get('direction','') or '').upper()))
        except Exception: pass

        logger.info(f'[pre-an] loaded {len(all_sigs)} total signals')

        # Index: pair → sorted list of (ts, src, direction)
        by_pair: dict = defaultdict(list)
        for pair, ts, src, d in all_sigs:
            by_pair[pair].append((ts, src, d))
        for p in by_pair:
            by_pair[p].sort()

        # === Load TARGET signals (TC / st_vip / confluence) с entry price ===
        _state['progress'] = 'loading_target_signals'
        target_sigs: list = []
        # triple_confluence
        for s in db.new_strategy_signals.find({'created_at':{'$gte':since},
                                                 'strategy':'triple_confluence'},
                                                {'pair':1,'direction':1,'entry':1,'created_at':1}):
            if s.get('entry') and s.get('pair'):
                target_sigs.append({'src':'triple_confluence', 'pair':s['pair'],
                    'direction':(s.get('direction','') or '').upper(),
                    'entry':float(s['entry']), 'at_ts':int(s['created_at'].timestamp())})
        # st_vip
        for s in db.supertrend_signals.find({'flip_at':{'$gte':since}, 'tier':'vip'},
                                              {'pair':1,'direction':1,'entry_price':1,'flip_at':1}):
            if s.get('entry_price') and s.get('pair'):
                target_sigs.append({'src':'st_vip', 'pair':s['pair'],
                    'direction':(s.get('direction','') or '').upper(),
                    'entry':float(s['entry_price']), 'at_ts':int(s['flip_at'].timestamp())})
        # confluence
        for s in db.confluence.find({'detected_at':{'$gte':since}},
                                      {'pair':1,'direction':1,'price':1,'detected_at':1}):
            if s.get('price') and s.get('pair'):
                target_sigs.append({'src':'confluence', 'pair':s['pair'],
                    'direction':(s.get('direction','') or '').upper(),
                    'entry':float(s['price']), 'at_ts':int(s['detected_at'].timestamp())})
        logger.info(f'[pre-an] target signals: {len(target_sigs)}')

        # === Fetch 15m klines per unique pair ===
        _state['progress'] = 'fetching_klines'
        unique_pairs = list(set(s['pair'] for s in target_sigs))
        klines_15m: dict = {}
        def _fp(pair):
            return (pair, _vision_klines(_to_sym(pair), '15m', KLINES_15M_DAYS))
        with ThreadPoolExecutor(max_workers=20) as tp:
            for pair, k15 in tp.map(_fp, unique_pairs):
                klines_15m[pair] = k15

        # === Per target: sim forward + collect preceding signals ===
        _state['progress'] = 'simulating_and_collecting'
        winners: list = []
        losers: list = []
        for sig in target_sigs:
            k15 = klines_15m.get(sig['pair'], [])
            if len(k15) < 50: continue
            idx = _find_idx(k15, sig['at_ts'] * 1000)
            if idx < 0 or idx >= len(k15) - 10: continue
            outcome = _sim_forward(k15, idx, sig['entry'], sig['direction'])
            if not outcome: continue

            # Collect preceding signals (24h before, same direction, same pair)
            pair_sigs = by_pair.get(sig['pair'], [])
            ts = sig['at_ts']
            cutoff = ts - PRECEDING_WINDOW_S
            ts_list = [x[0] for x in pair_sigs]
            lo = bisect.bisect_left(ts_list, cutoff)
            hi = bisect.bisect_right(ts_list, ts)
            preceding_window = pair_sigs[lo:hi]
            # Filter same direction (allow neutral too?). Strict same.
            preceding = [(x[0], x[1]) for x in preceding_window if x[2] == sig['direction']]
            preceding_sources = [s[1] for s in preceding]

            record = {
                **sig,
                **outcome,
                'preceding_count': len(preceding),
                'preceding_sources': preceding_sources,
                'preceding_unique_sources': list(set(preceding_sources)),
            }
            if outcome['mfe_pct'] >= WINNER_MFE_TH:
                winners.append(record)
            elif outcome['final_pct'] <= LOSER_FIN_TH:
                losers.append(record)

        logger.info(f'[pre-an] winners: {len(winners)}, losers: {len(losers)}')

        # === Aggregate stats ===
        def _agg(items):
            n = len(items)
            if n == 0: return {}
            # Counts per preceding source
            src_freq = Counter()
            for r in items:
                for src in r['preceding_unique_sources']:
                    src_freq[src] += 1
            # Преобразуем в %
            src_pct = {src: round(cnt/n*100, 1) for src, cnt in src_freq.items()}
            # 2-combo frequency
            combo2 = Counter()
            for r in items:
                srcs = sorted(set(r['preceding_unique_sources']))
                for i in range(len(srcs)):
                    for j in range(i+1, len(srcs)):
                        combo2[f'{srcs[i]}+{srcs[j]}'] += 1
            top_combos = dict(combo2.most_common(15))
            # Stats on preceding count
            counts = [r['preceding_count'] for r in items]
            unique_counts = [len(r['preceding_unique_sources']) for r in items]
            return {
                'n': n,
                'avg_preceding_count': round(sum(counts)/n, 1),
                'median_preceding_count': statistics.median(counts) if counts else 0,
                'avg_unique_sources': round(sum(unique_counts)/n, 2),
                'src_freq_pct': dict(sorted(src_pct.items(), key=lambda kv: -kv[1])),
                'top_2_combos': top_combos,
            }

        # Per target source
        per_target = {}
        for tsrc in TARGET_SOURCES:
            tw = [w for w in winners if w['src'] == tsrc]
            tl = [l for l in losers if l['src'] == tsrc]
            per_target[tsrc] = {
                'winners': _agg(tw),
                'losers': _agg(tl),
                'win_rate': round(len(tw) / max(len(tw) + len(tl), 1) * 100, 1),
            }

        # Aggregate (all targets)
        overall = {
            'all_winners': _agg(winners),
            'all_losers': _agg(losers),
            'total_winners': len(winners),
            'total_losers': len(losers),
            'overall_win_rate': round(len(winners) / max(len(winners) + len(losers), 1) * 100, 1),
        }

        # Top 20 winners detail (for manual review)
        top_winners_detail = sorted(winners, key=lambda r: -r['mfe_pct'])[:20]
        top_detail = [
            {'pair': w['pair'], 'src': w['src'], 'direction': w['direction'],
              'at_ts': w['at_ts'], 'mfe_pct': round(w['mfe_pct'], 2),
              'final_pct': round(w['final_pct'], 2),
              'preceding_count': w['preceding_count'],
              'preceding_sources': w['preceding_sources']}
            for w in top_winners_detail
        ]

        report = {
            'lookback_days': LOOKBACK_DAYS,
            'preceding_window_h': PRECEDING_WINDOW_S // 3600,
            'winner_mfe_th': WINNER_MFE_TH,
            'loser_fin_th': LOSER_FIN_TH,
            'overall': overall,
            'per_target_source': per_target,
            'top_winners_detail': top_detail,
            'started_at': _state['started_at'],
            'finished_at': datetime.now(timezone.utc).isoformat(),
            'elapsed_s': round(time.time() - t0, 1),
        }

        try:
            db.precondition_analysis_report.delete_many({})
            db.precondition_analysis_report.insert_one(report)
        except Exception as e:
            logger.warning(f'[pre-an] mongo save fail: {e}')

        _state['running'] = False
        _state['finished_at'] = datetime.now(timezone.utc).isoformat()
        _state['progress'] = 'done'
        return report
    except Exception as e:
        logger.exception('[pre-an] fatal')
        _state['error'] = str(e)
        _state['running'] = False
        _state['finished_at'] = datetime.now(timezone.utc).isoformat()
        return {'error': str(e)}
