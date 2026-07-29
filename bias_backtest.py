"""Backtest TOP-7 signals filtered by historical TOTAL2 bias (30d).

Цель: проверить даёт ли TOTAL2 direction filter улучшение WR/MFE.

Алгоритм:
  1. Build TOTAL2 4H series (60+ days) for full ST history
  2. Compute SuperTrend(10, 3) на 4H + на 1D (each 6th 4H bar)
  3. Per-timestamp bias map:
       LONG    if 1D UP + 4H UP
       SHORT   if 1D DOWN + 4H DOWN
       WAIT    otherwise (conflict)
  4. Для каждого из 7 источников: load signals last 30d
  5. Для каждого signal: lookup bias at signal_ts
  6. Forward-sim 72h via Binance Vision 15m
  7. Aggregate per source × per direction × per bias_alignment:
       - ALIGNED: bias matches direction
       - COUNTER: bias opposes direction
       - WAIT:    bias unclear

Output: side-by-side stats — показать насколько фильтр улучшает.
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
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import httpx

logger = logging.getLogger(__name__)
http_client = httpx.Client(
    timeout=15.0,
    limits=httpx.Limits(max_connections=100, max_keepalive_connections=60),
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

LOOKBACK_DAYS = 30
KLINES_15M_DAYS = 33  # 30d signals + 3d forward sim
FORWARD_BARS_15M = 288  # 72h

_state = {
    'running': False, 'started_at': None,
    'phase': '',  # 'total2' / 'signals' / 'sim' / 'agg'
    'progress_pct': 0,
    'pairs_fetched': 0, 'pairs_total': 0,
    'signals_total': 0, 'signals_done': 0,
    'error': None, 'finished_at': None,
}


def get_state():
    return dict(_state)


# ───────── TOTAL2 historical series ─────────────────────────────

def _to_sym(pair: str) -> str:
    s = pair.replace('/', '').upper()
    if not s.endswith('USDT'):
        s += 'USDT'
    return s


def _vision_klines(symbol: str, tf: str, days: int) -> list[dict]:
    """Primary: Binance Vision CDN. Fallback: BingX (для not-on-Binance пар).
    """
    now = datetime.now(timezone.utc)
    dates = [(now - timedelta(days=d)).strftime('%Y-%m-%d')
             for d in range(1, days + 1)]
    out = []
    def _fetch_day(ds):
        url = (f'https://data.binance.vision/data/futures/um/daily/klines/'
               f'{symbol}/{tf}/{symbol}-{tf}-{ds}.zip')
        try:
            r = http_client.get(url)
            if r.status_code != 200:
                return []
            zf = zipfile.ZipFile(io.BytesIO(r.content))
            rows = csv.reader(zf.read(zf.namelist()[0]).decode().splitlines())
            return [{'t': int(row[0]), 'o': float(row[1]), 'h': float(row[2]),
                     'l': float(row[3]), 'c': float(row[4]), 'v': float(row[5])}
                    for row in rows if row and row[0].isdigit()]
        except Exception:
            return []
    with ThreadPoolExecutor(max_workers=10, thread_name_prefix='bias-bt') as tp:
        for r in tp.map(_fetch_day, dates):
            out.extend(r)
    seen = set()
    uniq = []
    for k in out:
        if k['t'] in seen:
            continue
        seen.add(k['t'])
        uniq.append(k)
    uniq.sort(key=lambda x: x['t'])

    # ── Fallback: BingX для пар отсутствующих на Binance ──
    # Vision вернул пусто или мало (<50 баров) → пробуем BingX swap
    if len(uniq) < 50:
        try:
            from exchange import get_bingx_klines
            # symbol = "TSLAUSDT" → pair = "TSLA/USDT"
            pair = (symbol[:-4] + '/USDT') if symbol.endswith('USDT') else symbol
            # BingX limit обычно 1000 — для 15m 33d нужно 3168 баров.
            # Берём с запасом, BingX вернёт сколько может.
            cap = 1000 if tf in ('15m', '5m', '1m') else 500
            bx = get_bingx_klines(pair, tf, cap)
            if bx and len(bx) >= 50:
                return bx
        except Exception:
            pass
    return uniq


def _build_total2_history() -> list[dict]:
    """Returns sorted list [{t, c}] — TOTAL2 close per 4H bar over 60+ days.
    Uses top-20 alts × current supplies (proxy ~85% of TradingView TOTAL2).
    """
    from market_total import TOP_ALTS, _fetch_supplies

    supplies = _fetch_supplies()
    if not supplies:
        logger.error('[bias-bt] no supplies — abort')
        return []

    # Fetch 4H klines for each alt via Vision CDN (no IP block)
    by_ts: dict[int, float] = {}
    _state['pairs_total'] = len(TOP_ALTS)

    def _fetch(item):
        sym, cg_id = item
        return (sym, cg_id, _vision_klines(sym, '4h', 65))

    with ThreadPoolExecutor(max_workers=10, thread_name_prefix='bias-bt') as tp:
        for i, (sym, cg_id, klines) in enumerate(tp.map(_fetch, TOP_ALTS)):
            _state['pairs_fetched'] = i + 1
            supply = supplies.get(cg_id, 0)
            if supply <= 0 or not klines:
                continue
            for k in klines:
                mcap = k['c'] * supply
                by_ts[k['t']] = by_ts.get(k['t'], 0) + mcap

    series = [{'t': t, 'c': v} for t, v in sorted(by_ts.items())]
    logger.info(f'[bias-bt] TOTAL2 series: {len(series)} 4H bars')
    return series


def _compute_supertrend_full(closes: list[float], period: int = 10,
                              mult: float = 3.0) -> list[int]:
    """Returns trend[] array (1=UP, -1=DOWN) of length len(closes)."""
    n = len(closes)
    if n < period + 5:
        return [0] * n
    trs = [0.0]
    for i in range(1, n):
        trs.append(abs(closes[i] - closes[i-1]))
    atr = [None] * n
    atr[period - 1] = sum(trs[:period]) / period
    for i in range(period, n):
        atr[i] = (atr[i-1] * (period - 1) + trs[i]) / period

    final_upper = [0.0] * n
    final_lower = [0.0] * n
    trend = [0] * n
    for i in range(n):
        if atr[i] is None:
            continue
        bu = closes[i] + mult * atr[i]
        bl = closes[i] - mult * atr[i]
        if i == 0 or atr[i-1] is None:
            final_upper[i] = bu
            final_lower[i] = bl
            trend[i] = 1
            continue
        final_upper[i] = bu if (bu < final_upper[i-1] or
                                 closes[i-1] > final_upper[i-1]) else final_upper[i-1]
        final_lower[i] = bl if (bl > final_lower[i-1] or
                                 closes[i-1] < final_lower[i-1]) else final_lower[i-1]
        if trend[i-1] == 1 and closes[i] < final_lower[i-1]:
            trend[i] = -1
        elif trend[i-1] == -1 and closes[i] > final_upper[i-1]:
            trend[i] = 1
        else:
            trend[i] = trend[i-1] if trend[i-1] != 0 else 1
    return trend


def _build_bias_map(series_4h: list[dict]) -> tuple[list[int], list[str]]:
    """Returns (timestamps_ms, biases) — parallel lists where bias[i] is
    'LONG'/'SHORT'/'WAIT' at timestamps_ms[i]. Uses combined 4H + 1D ST.
    """
    if not series_4h:
        return [], []
    closes = [c['c'] for c in series_4h]
    trend_4h = _compute_supertrend_full(closes, 10, 3.0)

    # Daily aggregation: every 6th 4H bar
    daily_indices = list(range(5, len(series_4h), 6))
    daily_closes = [closes[i] for i in daily_indices]
    trend_1d_compact = _compute_supertrend_full(daily_closes, 10, 3.0)
    # Expand trend_1d back to 4H granularity (each daily trend covers next 6 4H bars)
    trend_1d_full = [0] * len(series_4h)
    for di, fi in enumerate(daily_indices):
        # Apply trend_1d_compact[di] to bars [fi, fi+6)
        end = daily_indices[di + 1] if di + 1 < len(daily_indices) else len(series_4h)
        for j in range(fi, end):
            trend_1d_full[j] = trend_1d_compact[di]
        # Also fill from previous daily index forward
        if di == 0:
            for j in range(0, fi):
                trend_1d_full[j] = trend_1d_compact[di]

    biases = []
    for i in range(len(series_4h)):
        t4 = trend_4h[i]
        t1 = trend_1d_full[i]
        if t1 == 1 and t4 == 1:
            biases.append('LONG')
        elif t1 == -1 and t4 == -1:
            biases.append('SHORT')
        elif t1 == 1 and t4 == -1:
            biases.append('LONG_CAUTION')
        elif t1 == -1 and t4 == 1:
            biases.append('SHORT_CAUTION')
        else:
            biases.append('WAIT')

    ts_ms = [c['t'] for c in series_4h]
    return ts_ms, biases


def _bias_at(ts_ms: int, bias_ts: list[int], bias_arr: list[str]) -> str:
    """Lookup bias at signal_ts via bisect."""
    if not bias_ts:
        return 'WAIT'
    idx = bisect.bisect_right(bias_ts, ts_ms) - 1
    if idx < 0:
        return 'WAIT'
    return bias_arr[idx]


# ───────── Signal loaders (7 sources) ─────────────────────────

def _load_7_sources(db) -> dict[str, list[dict]]:
    """Returns {source_key: [{pair, direction, entry, at_ts}, ...]}."""
    since = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    out: dict[str, list[dict]] = defaultdict(list)

    # 1. triple_confluence
    for s in db.new_strategy_signals.find(
        {'created_at': {'$gte': since}, 'strategy': 'triple_confluence'},
        {'pair':1,'direction':1,'entry':1,'created_at':1}
    ):
        if not (s.get('entry') and s.get('pair')): continue
        out['triple_confluence'].append({
            'pair': s['pair'],
            'direction': (s.get('direction','') or '').upper(),
            'entry': float(s['entry']),
            'at_ts': int(s['created_at'].timestamp()),
        })

    # 2. st_vip
    for s in db.supertrend_signals.find(
        {'flip_at': {'$gte': since}, 'tier': 'vip'},
        {'pair':1,'direction':1,'entry_price':1,'flip_at':1}
    ):
        if not (s.get('entry_price') and s.get('pair')): continue
        out['st_vip'].append({
            'pair': s['pair'],
            'direction': (s.get('direction','') or '').upper(),
            'entry': float(s['entry_price']),
            'at_ts': int(s['flip_at'].timestamp()),
        })

    # 3. confluence
    for s in db.confluence.find(
        {'detected_at': {'$gte': since}},
        {'pair':1,'direction':1,'price':1,'detected_at':1,'score':1}
    ):
        if not (s.get('price') and s.get('pair')): continue
        out['confluence'].append({
            'pair': s['pair'],
            'direction': (s.get('direction','') or '').upper(),
            'entry': float(s['price']),
            'at_ts': int(s['detected_at'].timestamp()),
            'score': s.get('score'),
        })

    # 4. WHALE STANDARD + PREMIUM (LONG only)
    for s in db.new_strategy_signals.find(
        {'created_at': {'$gte': since}, 'strategy': 'whale',
         'whale_tier': {'$in': ['STANDARD', 'PREMIUM']}},
        {'pair':1,'direction':1,'entry':1,'created_at':1,'whale_tier':1}
    ):
        if not (s.get('entry') and s.get('pair')): continue
        key = 'whale_premium' if s.get('whale_tier') == 'PREMIUM' else 'whale_standard'
        out[key].append({
            'pair': s['pair'],
            'direction': (s.get('direction','') or '').upper(),
            'entry': float(s['entry']),
            'at_ts': int(s['created_at'].timestamp()),
        })

    # 5. combo
    for s in db.new_strategy_signals.find(
        {'created_at': {'$gte': since}, 'strategy': 'combo'},
        {'pair':1,'direction':1,'entry':1,'created_at':1}
    ):
        if not (s.get('entry') and s.get('pair')): continue
        out['combo'].append({
            'pair': s['pair'],
            'direction': (s.get('direction','') or '').upper(),
            'entry': float(s['entry']),
            'at_ts': int(s['created_at'].timestamp()),
        })

    # 6. cv_flip (only FLIPPED state)
    for s in db.cv_flip_signals.find(
        {'cv_triggered_at': {'$gte': since}, 'state': 'FLIPPED'},
        {'pair':1,'direction':1,'entry':1,'flip_price':1,'cv_triggered_at':1,'flip_at':1}
    ):
        e = s.get('entry') or s.get('flip_price') or 0
        if not (e and s.get('pair')): continue
        ts = s.get('flip_at') or s.get('cv_triggered_at')
        if not ts: continue
        out['cv_flip'].append({
            'pair': s['pair'],
            'direction': (s.get('direction','') or '').upper(),
            'entry': float(e),
            'at_ts': int(ts.timestamp()),
        })

    for k, v in out.items():
        logger.info(f'[bias-bt] loaded {k}: {len(v)} signals')
    return out


# ───────── Forward sim ──────────────────────────────────────

def _find_bar_idx(kl, ts_ms):
    lo, hi = 0, len(kl) - 1
    while lo <= hi:
        m = (lo + hi) // 2
        if kl[m]['t'] <= ts_ms:
            lo = m + 1
        else:
            hi = m - 1
    return hi


def _sim_forward(k15, idx, entry, direction):
    fwd = k15[idx+1:idx+1+FORWARD_BARS_15M]
    if len(fwd) < 4:
        return None
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
    final = ((closes[-1] - entry) / entry * 100 if is_l
             else (entry - closes[-1]) / entry * 100)
    return {'mfe_pct': mfe, 'mae_pct': mae, 'final_pct': final}


def _stats(items: list[dict]) -> dict:
    if not items:
        return {'n': 0}
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


# ───────── Main backtest ──────────────────────────────────

def run_bias_backtest() -> dict:
    global _state
    _state = {
        'running': True, 'started_at': datetime.now(timezone.utc).isoformat(),
        'phase': 'total2', 'progress_pct': 0,
        'pairs_fetched': 0, 'pairs_total': 0,
        'signals_total': 0, 'signals_done': 0,
        'error': None, 'finished_at': None,
    }
    try:
        t0 = time.time()
        from database import _get_db
        db = _get_db()

        # Step 1: Build TOTAL2 history + bias map
        _state['phase'] = 'total2'
        series_4h = _build_total2_history()
        if not series_4h:
            raise RuntimeError('failed to build TOTAL2 history')
        bias_ts, bias_arr = _build_bias_map(series_4h)
        bias_distribution = defaultdict(int)
        for b in bias_arr:
            bias_distribution[b] += 1
        logger.info(f'[bias-bt] bias distribution: {dict(bias_distribution)}')

        # Step 2: Load signals from 7 sources
        _state['phase'] = 'signals'
        sources = _load_7_sources(db)
        _state['signals_total'] = sum(len(v) for v in sources.values())
        logger.info(f'[bias-bt] total signals across 7 sources: {_state["signals_total"]}')

        # Step 3: Fetch 15m klines for all unique pairs
        all_pairs = set()
        for sigs in sources.values():
            for s in sigs:
                all_pairs.add(s['pair'])
        logger.info(f'[bias-bt] unique pairs: {len(all_pairs)}')

        klines_15m: dict[str, list] = {}
        def _fetch_pair(pair):
            sym = _to_sym(pair)
            return (pair, _vision_klines(sym, '15m', KLINES_15M_DAYS))

        _state['phase'] = 'klines'
        _state['pairs_total'] = len(all_pairs)
        _state['pairs_fetched'] = 0
        with ThreadPoolExecutor(max_workers=20, thread_name_prefix='bias-bt') as tp:
            for i, (pair, k15) in enumerate(tp.map(_fetch_pair, all_pairs)):
                klines_15m[pair] = k15
                _state['pairs_fetched'] = i + 1
                if (i + 1) % 50 == 0:
                    logger.info(f'[bias-bt] klines {i+1}/{len(all_pairs)}')

        # Step 4: Per-source × per-bias-alignment simulation
        _state['phase'] = 'sim'
        # Bias alignment: ALIGNED / COUNTER / WAIT
        # Lots of buckets: {source: {alignment: [outcomes]}}
        results: dict = {src: {'ALIGNED': [], 'COUNTER': [], 'WAIT': [], 'ALL': []}
                         for src in sources}

        total_done = 0
        for src, sigs in sources.items():
            for sig in sigs:
                total_done += 1
                _state['signals_done'] = total_done
                k15 = klines_15m.get(sig['pair'], [])
                if len(k15) < 50:
                    continue
                idx = _find_bar_idx(k15, sig['at_ts'] * 1000)
                if idx < 0 or idx >= len(k15) - 10:
                    continue
                if sig['direction'] not in ('LONG', 'SHORT'):
                    continue
                outcome = _sim_forward(k15, idx, sig['entry'], sig['direction'])
                if not outcome:
                    continue

                # Bias at signal time
                bias = _bias_at(sig['at_ts'] * 1000, bias_ts, bias_arr)

                # Classification
                if bias in ('LONG', 'LONG_CAUTION') and sig['direction'] == 'LONG':
                    alignment = 'ALIGNED'
                elif bias in ('SHORT', 'SHORT_CAUTION') and sig['direction'] == 'SHORT':
                    alignment = 'ALIGNED'
                elif bias in ('LONG', 'LONG_CAUTION') and sig['direction'] == 'SHORT':
                    alignment = 'COUNTER'
                elif bias in ('SHORT', 'SHORT_CAUTION') and sig['direction'] == 'LONG':
                    alignment = 'COUNTER'
                else:
                    alignment = 'WAIT'  # bias = WAIT or unknown

                results[src][alignment].append(outcome)
                results[src]['ALL'].append(outcome)

        # Step 5: Aggregate stats
        _state['phase'] = 'agg'
        report = {
            'lookback_days': LOOKBACK_DAYS,
            'bias_distribution': dict(bias_distribution),
            'unique_pairs': len(all_pairs),
            'total_signals': _state['signals_total'],
            'sources': {},
            'started_at': _state['started_at'],
            'finished_at': datetime.now(timezone.utc).isoformat(),
            'elapsed_s': round(time.time() - t0, 1),
        }
        for src in results:
            report['sources'][src] = {
                'ALIGNED': _stats(results[src]['ALIGNED']),
                'COUNTER': _stats(results[src]['COUNTER']),
                'WAIT':    _stats(results[src]['WAIT']),
                'ALL':     _stats(results[src]['ALL']),
            }

        # Save to Mongo
        try:
            db.bias_backtest_report.delete_many({})
            db.bias_backtest_report.insert_one(report)
        except Exception as e:
            logger.warning(f'[bias-bt] mongo write fail: {e}')

        _state['running'] = False
        _state['finished_at'] = report['finished_at']
        return report
    except Exception as e:
        logger.exception('[bias-bt] fatal')
        _state['error'] = str(e)
        _state['running'] = False
        _state['finished_at'] = datetime.now(timezone.utc).isoformat()
        return {'error': str(e)}


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    rep = run_bias_backtest()
    import json
    # Pretty print
    print('\n' + '=' * 80)
    print(f'BIAS BACKTEST 30d — TOP-7 sources filtered by TOTAL2 bias')
    print('=' * 80)
    print(f"Bias distribution (4H bars): {rep.get('bias_distribution', {})}")
    print(f"Unique pairs: {rep.get('unique_pairs')}, Total signals: {rep.get('total_signals')}")
    print(f"Elapsed: {rep.get('elapsed_s')}s")
    print('=' * 80)
    for src, stats in (rep.get('sources') or {}).items():
        print(f'\n{src.upper()}:')
        for align in ('ALL', 'ALIGNED', 'COUNTER', 'WAIT'):
            s = stats.get(align, {})
            if s.get('n', 0) == 0:
                print(f'  {align:8s}  N={s.get("n",0):4d}  (no data)')
            else:
                print(f'  {align:8s}  N={s["n"]:4d}  WR={s["wr_pct"]:5.1f}%  '
                      f'MFE_med={s["mfe_median"]:6.2f}%  MAE_med={s["mae_median"]:5.2f}%  '
                      f'≥10%={s["pct_ge10"]:5.1f}%')
