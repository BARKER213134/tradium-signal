"""🐋 WHALE 30-day backtest + comparison vs COMBO / ST_VIP / Triple Confluence.

Strategy:
  1. Load top-volume USDT pairs from BingX universe.
  2. For each pair: fetch 30d of 2H + 15m klines (Vision CDN, parallel).
  3. Compute SuperTrend(10, 3) on 2H, find all UP-flips.
  4. At each flip → compute WHALE score (with anti-markers from Mongo).
  5. If score ≥ 40 (MARGINAL+), forward-sim 72h on 15m bars.
  6. Aggregate stats per tier (PREMIUM/STANDARD/MARGINAL).
  7. Parallel: load existing st_vip / triple_confluence / combo signals
     same period and compute same stats for direct comparison.

Output: comparison table to console + Mongo for UI.
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
from typing import Optional

import httpx

from whale_detector import compute_whale_score, check_anti_markers
from supertrend import _calc_supertrend

logger = logging.getLogger(__name__)
http_client = httpx.Client(
    timeout=15.0,
    limits=httpx.Limits(max_connections=100, max_keepalive_connections=60),
)
# Silence httpx INFO spam (one log line per request × 50k requests = bad)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

LOOKBACK_DAYS = 30
KLINES_2H_DAYS = 60   # need history for indicators (base/DT/cap)
KLINES_15M_DAYS = 33  # 30d signals + 3d forward sim
FORWARD_BARS_15M = 288  # 72h

# Per-pair cooldown — 12h
WHALE_COOLDOWN_S = 12 * 3600

_state = {
    'running': False, 'started_at': None,
    'pairs_total': 0, 'pairs_done': 0, 'last_pair': '',
    'flips_total': 0, 'whale_fired': 0,
    'whale_premium': 0, 'whale_standard': 0, 'whale_marginal': 0,
    'comparison_ready': False, 'error': None, 'finished_at': None,
}


def get_state():
    return dict(_state)


def _to_sym(pair: str) -> str:
    s = pair.replace('/', '').upper()
    if not s.endswith('USDT'):
        s += 'USDT'
    return s


def _vision_klines(symbol: str, tf: str, days: int) -> list[dict]:
    """Fetch klines from Binance Vision CDN (no rate limit)."""
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

    with ThreadPoolExecutor(max_workers=10) as tp:
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
    return uniq


def _find_st_flips_up(candles_2h: list[dict]) -> list[int]:
    """Returns list of indices in candles_2h where ST(10,3) flipped DOWN→UP.
    Computes ST incrementally — for each bar, run _calc_supertrend on prefix
    and detect trend change vs prev bar. Optimised: do single full computation.
    """
    if len(candles_2h) < 50:
        return []
    # Run _calc_supertrend manually inline (we need per-bar trend, not just last)
    n = len(candles_2h)
    closes = [c['c'] for c in candles_2h]
    highs = [c['h'] for c in candles_2h]
    lows = [c['l'] for c in candles_2h]
    period, mult = 10, 3.0
    # True Range
    tr = [highs[0] - lows[0]]
    for i in range(1, n):
        tr.append(max(highs[i] - lows[i],
                      abs(highs[i] - closes[i-1]),
                      abs(lows[i] - closes[i-1])))
    # Wilder RMA ATR
    atr = [None] * n
    atr[period - 1] = sum(tr[:period]) / period
    for i in range(period, n):
        atr[i] = (atr[i-1] * (period - 1) + tr[i]) / period
    # Bands + trend
    hl2 = [(highs[i] + lows[i]) / 2 for i in range(n)]
    final_upper = [0.0] * n
    final_lower = [0.0] * n
    trend = [0] * n  # 1 UP, -1 DOWN
    for i in range(n):
        if atr[i] is None:
            continue
        bu = hl2[i] + mult * atr[i]
        bl = hl2[i] - mult * atr[i]
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
    # Detect flips DOWN→UP
    flips = []
    for i in range(1, n):
        if trend[i] == 1 and trend[i-1] == -1:
            flips.append(i)
    return flips


def _find_bar_idx(kl: list[dict], ts_ms: int) -> int:
    lo, hi = 0, len(kl) - 1
    while lo <= hi:
        m = (lo + hi) // 2
        if kl[m]['t'] <= ts_ms:
            lo = m + 1
        else:
            hi = m - 1
    return hi


def _sim_forward(k15: list[dict], idx: int, entry: float,
                 direction: str = 'LONG') -> Optional[dict]:
    fwd = k15[idx + 1: idx + 1 + FORWARD_BARS_15M]
    if len(fwd) < 4:
        return None
    is_l = direction == 'LONG'
    highs = [k['h'] for k in fwd]
    lows = [k['l'] for k in fwd]
    closes = [k['c'] for k in fwd]
    if is_l:
        mf = max(highs)
        ma = min(lows)
        mfe = (mf - entry) / entry * 100
        mae = (entry - ma) / entry * 100
    else:
        mf = min(lows)
        ma = max(highs)
        mfe = (entry - mf) / entry * 100
        mae = (ma - entry) / entry * 100
    final = ((closes[-1] - entry) / entry * 100 if is_l
             else (entry - closes[-1]) / entry * 100)
    return {'mfe_pct': mfe, 'mae_pct': mae, 'final_pct': final}


def _load_universe(db, limit: int = 500) -> list[str]:
    """Load top-volume USDT pairs that have signals in DB last 30d
    (proxy for actively traded pairs).
    """
    since = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    pairs: set = set()
    try:
        for s in db.supertrend_signals.find(
            {'flip_at': {'$gte': since}, 'tier': {'$in': ['vip', 'mtf']}},
            {'pair': 1}
        ):
            if s.get('pair'):
                pairs.add(s['pair'])
    except Exception:
        pass
    try:
        for s in db.signals.find(
            {'source': 'cryptovizor',
             'pattern_triggered_at': {'$gte': since}},
            {'pair': 1}
        ):
            if s.get('pair'):
                pairs.add(s['pair'])
    except Exception:
        pass
    # Hard cap for runtime
    return sorted(pairs)[:limit]


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
        'wr_pct': round(wins / len(items) * 100, 1),
        'mfe_median': round(statistics.median(mfes), 2),
        'mfe_avg': round(sum(mfes) / len(mfes), 2),
        'mae_median': round(statistics.median(maes), 2),
        'final_median': round(statistics.median(finals), 2),
        'pct_ge5': round(ge5 / len(items) * 100, 1),
        'pct_ge10': round(ge10 / len(items) * 100, 1),
        'pct_ge20': round(ge20 / len(items) * 100, 1),
    }


def _ev_grid(items: list[dict]) -> dict:
    if not items:
        return {}
    out = {}
    n = len(items)
    for tp, sl in [(3, 2), (5, 3), (7, 4), (10, 5), (15, 7)]:
        tp_hit = sum(1 for r in items if r['mfe_pct'] >= tp and r['mae_pct'] < sl)
        sl_hit = sum(1 for r in items if r['mae_pct'] >= sl)
        ev = (tp_hit / n) * tp - (sl_hit / n) * sl
        out[f'tp{tp}_sl{sl}'] = {
            'tp_hit_pct': round(tp_hit / n * 100, 1),
            'sl_hit_pct': round(sl_hit / n * 100, 1),
            'ev_pct': round(ev, 2),
        }
    return out


# ── Load comparison signals (COMBO / ST_VIP / TC) ──────────────

def _load_comparison_signals(db) -> dict[str, list[dict]]:
    """Load LONG signals from the 3 sources for direct comparison."""
    since = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    out: dict[str, list[dict]] = {
        'st_vip_LONG': [],
        'triple_confluence_LONG': [],
        'combo_LONG': [],
    }
    # ST VIP
    for s in db.supertrend_signals.find(
        {'flip_at': {'$gte': since}, 'tier': 'vip'},
        {'pair': 1, 'direction': 1, 'entry_price': 1, 'flip_at': 1}
    ):
        if (s.get('direction', '') or '').upper() != 'LONG':
            continue
        if not (s.get('entry_price') and s.get('pair')):
            continue
        out['st_vip_LONG'].append({
            'pair': s['pair'],
            'entry': float(s['entry_price']),
            'at_ts': int(s['flip_at'].timestamp()),
        })
    # Triple Confluence
    for s in db.new_strategy_signals.find(
        {'created_at': {'$gte': since}, 'strategy': 'triple_confluence'},
        {'pair': 1, 'direction': 1, 'entry': 1, 'created_at': 1}
    ):
        if (s.get('direction', '') or '').upper() != 'LONG':
            continue
        if not (s.get('entry') and s.get('pair')):
            continue
        out['triple_confluence_LONG'].append({
            'pair': s['pair'],
            'entry': float(s['entry']),
            'at_ts': int(s['created_at'].timestamp()),
        })
    # COMBO
    for s in db.new_strategy_signals.find(
        {'created_at': {'$gte': since}, 'strategy': 'combo'},
        {'pair': 1, 'direction': 1, 'entry': 1, 'created_at': 1}
    ):
        if (s.get('direction', '') or '').upper() != 'LONG':
            continue
        if not (s.get('entry') and s.get('pair')):
            continue
        out['combo_LONG'].append({
            'pair': s['pair'],
            'entry': float(s['entry']),
            'at_ts': int(s['created_at'].timestamp()),
        })
    return out


# ── Main backtest entry point ─────────────────────────────────

def run_whale_backtest(pair_limit: int = 150) -> dict:
    """Synchronous WHALE backtest + comparison."""
    global _state
    _state = {
        'running': True,
        'started_at': datetime.now(timezone.utc).isoformat(),
        'pairs_total': 0, 'pairs_done': 0, 'last_pair': '',
        'flips_total': 0, 'whale_fired': 0,
        'whale_premium': 0, 'whale_standard': 0, 'whale_marginal': 0,
        'comparison_ready': False, 'error': None, 'finished_at': None,
    }
    try:
        t0 = time.time()
        from database import _get_db
        db = _get_db()

        # ── Step 1: universe ──
        pairs = _load_universe(db, limit=pair_limit)
        _state['pairs_total'] = len(pairs)
        logger.info(f'[whale-bt] universe: {len(pairs)} pairs')

        # ── Step 2: per-pair processing (OUTER parallelism) ──
        whale_results: list[dict] = []
        whale_skipped_cooldown = 0
        flips_total = 0
        cutoff_ms = int((datetime.now(timezone.utc) -
                          timedelta(days=LOOKBACK_DAYS)).timestamp()) * 1000

        def _process_pair(pair: str) -> dict:
            """Returns dict with per-pair results."""
            local_results = []
            local_skipped = 0
            local_flips = 0
            try:
                sym = _to_sym(pair)
                k2h = _vision_klines(sym, '2h', KLINES_2H_DAYS)
                if len(k2h) < 100:
                    return {'results': [], 'skipped': 0, 'flips': 0, 'pair': pair}
                k15 = _vision_klines(sym, '15m', KLINES_15M_DAYS)
                if len(k15) < 200:
                    return {'results': [], 'skipped': 0, 'flips': 0, 'pair': pair}

                flip_indices = _find_st_flips_up(k2h)
                flip_indices = [i for i in flip_indices
                                if k2h[i]['t'] >= cutoff_ms]
                local_flips = len(flip_indices)

                last_fire_ts = 0
                for fi in flip_indices:
                    flip_ts_s = k2h[fi]['t'] // 1000
                    if flip_ts_s - last_fire_ts < WHALE_COOLDOWN_S:
                        local_skipped += 1
                        continue
                    anti_flags = check_anti_markers(db, pair, flip_ts_s, 'LONG')
                    score_res = compute_whale_score(k2h, fi, anti_flags)
                    if not score_res['passes_core']:
                        continue
                    if not score_res['tier']:
                        continue
                    idx15 = _find_bar_idx(k15, k2h[fi]['t'])
                    if idx15 < 0 or idx15 >= len(k15) - 10:
                        continue
                    entry = k2h[fi]['c']
                    outcome = _sim_forward(k15, idx15, entry, 'LONG')
                    if not outcome:
                        continue
                    local_results.append({
                        'pair': pair,
                        'flip_ts': k2h[fi]['t'],
                        'entry': entry,
                        'score': score_res['score'],
                        'tier': score_res['tier'],
                        'indicators': score_res['indicators'],
                        'breakdown': score_res['breakdown'],
                        **outcome,
                    })
                    last_fire_ts = flip_ts_s
            except Exception as e:
                logger.debug(f'[whale-bt] {pair}: {e}')
            return {'results': local_results, 'skipped': local_skipped,
                    'flips': local_flips, 'pair': pair}

        # 12 pairs in parallel × 10 inner workers = 120 concurrent fetches
        # (httpx limit raised to 100 connections — actual cap ~100)
        with ThreadPoolExecutor(max_workers=12) as tp:
            for pidx, res in enumerate(tp.map(_process_pair, pairs)):
                _state['pairs_done'] = pidx + 1
                _state['last_pair'] = res['pair']
                whale_results.extend(res['results'])
                whale_skipped_cooldown += res['skipped']
                flips_total += res['flips']
                _state['flips_total'] = flips_total
                _state['whale_fired'] = len(whale_results)
                for r in res['results']:
                    if r['tier'] == 'PREMIUM':
                        _state['whale_premium'] += 1
                    elif r['tier'] == 'STANDARD':
                        _state['whale_standard'] += 1
                    elif r['tier'] == 'MARGINAL':
                        _state['whale_marginal'] += 1
                if (pidx + 1) % 25 == 0:
                    logger.info(
                        f'[whale-bt] progress {pidx+1}/{len(pairs)} '
                        f'flips={flips_total} fired={len(whale_results)} '
                        f'(P/S/M={_state["whale_premium"]}/'
                        f'{_state["whale_standard"]}/{_state["whale_marginal"]})'
                    )

        # ── Step 3: WHALE stats per tier ──
        whale_by_tier: dict = defaultdict(list)
        for r in whale_results:
            whale_by_tier[r['tier']].append(r)

        whale_report: dict = {}
        for tier in ('PREMIUM', 'STANDARD', 'MARGINAL'):
            items = whale_by_tier.get(tier, [])
            whale_report[tier] = {
                **_stats(items),
                'ev_grid': _ev_grid(items),
            }
        # Also "ALL WHALE" combined
        whale_report['ALL'] = {
            **_stats(whale_results),
            'ev_grid': _ev_grid(whale_results),
        }

        # ── Step 4: Comparison signals (st_vip / TC / combo) ──
        cmp_sigs = _load_comparison_signals(db)
        # We need to sim each via 15m klines — reuse fetched
        # (only have 15m for whale-universe pairs; for others skip)
        # Build pair→k15 cache from earlier fetches isn't preserved across loops.
        # Easier: re-fetch per unique pair.

        cmp_pair_set: set = set()
        for src, sigs in cmp_sigs.items():
            for s in sigs:
                cmp_pair_set.add(s['pair'])
        cmp_pair_set -= set(pairs)  # already fetched? no — we discarded.
        # Just fetch all cmp pairs fresh (smaller universe usually).
        cmp_k15: dict[str, list] = {}
        cmp_pairs_list = list(set(s['pair']
                                  for sigs in cmp_sigs.values()
                                  for s in sigs))

        def _fetch_one(pair):
            sym = _to_sym(pair)
            return (pair, _vision_klines(sym, '15m', KLINES_15M_DAYS))

        with ThreadPoolExecutor(max_workers=20) as tp:
            for pair, k15 in tp.map(_fetch_one, cmp_pairs_list):
                cmp_k15[pair] = k15

        cmp_report: dict = {}
        for src, sigs in cmp_sigs.items():
            items = []
            for s in sigs:
                k15 = cmp_k15.get(s['pair'], [])
                if len(k15) < 50:
                    continue
                idx = _find_bar_idx(k15, s['at_ts'] * 1000)
                if idx < 0 or idx >= len(k15) - 10:
                    continue
                outcome = _sim_forward(k15, idx, s['entry'], 'LONG')
                if not outcome:
                    continue
                items.append({**s, **outcome})
            cmp_report[src] = {
                **_stats(items),
                'ev_grid': _ev_grid(items),
            }

        _state['comparison_ready'] = True

        # ── Step 5: Final report ──
        report = {
            'lookback_days': LOOKBACK_DAYS,
            'pairs_scanned': len(pairs),
            'st_flips_total': flips_total,
            'whale_fired_total': len(whale_results),
            'whale_skipped_cooldown': whale_skipped_cooldown,
            'whale_report': whale_report,
            'comparison_report': cmp_report,
            'started_at': _state['started_at'],
            'finished_at': datetime.now(timezone.utc).isoformat(),
            'elapsed_s': round(time.time() - t0, 1),
        }

        try:
            db.whale_backtest_report.delete_many({})
            db.whale_backtest_report.insert_one(report)
        except Exception as e:
            logger.warning(f'[whale-bt] mongo write: {e}')

        # Also persist top WHALE hits as new_strategy_signals (strategy='whale')
        # so they appear in the journal alongside COMBO. Only PREMIUM+STANDARD.
        try:
            from database import utcnow
            inserted = 0
            for r in whale_results:
                if r['tier'] not in ('PREMIUM', 'STANDARD'):
                    continue
                flip_dt = datetime.fromtimestamp(r['flip_ts'] / 1000,
                                                   tz=timezone.utc)
                exists = db.new_strategy_signals.find_one({
                    'pair': r['pair'], 'strategy': 'whale',
                    'created_at': flip_dt,
                })
                if exists:
                    continue
                db.new_strategy_signals.insert_one({
                    'pair': r['pair'],
                    'symbol': r['pair'].replace('/', '').upper(),
                    'direction': 'LONG',
                    'entry': r['entry'],
                    'strategy': 'whale',
                    'whale_score': r['score'],
                    'whale_tier': r['tier'],
                    'whale_breakdown': r['breakdown'],
                    'whale_indicators': r['indicators'],
                    'created_at': flip_dt,
                    'state': 'BACKFILLED',
                    'tp_R': 2.0,
                })
                inserted += 1
            report['journal_inserted'] = inserted
            logger.info(f'[whale-bt] journal inserts: {inserted}')
        except Exception as e:
            logger.warning(f'[whale-bt] journal insert fail: {e}')

        _state['running'] = False
        _state['finished_at'] = report['finished_at']
        return report
    except Exception as e:
        logger.exception('[whale-bt] fatal')
        _state['error'] = str(e)
        _state['running'] = False
        _state['finished_at'] = datetime.now(timezone.utc).isoformat()
        return {'error': str(e)}


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')
    rep = run_whale_backtest(pair_limit=150)
    import json
    print(json.dumps(rep, indent=2, default=str))
