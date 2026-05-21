"""Pre-Pump Predictor — 30-day backtest на Railway (где fapi доступен).

Запускается через admin endpoint POST /api/prepump/backtest/start.
Результаты пишутся в Mongo collection prepump_backtest_results.
Status через GET /api/prepump/backtest/status.

Метод:
1. Top 150 пар на BingX (filter)
2. Для каждой пары fetch:
   - 1h klines × 40 days (Vision CDN или fapi)
   - OI history 1h × 30 days (fapi /openInterestHist)
   - Funding rate × 30 days (fapi /fundingRate)
   - 15m klines × 33 days (для forward sim)
3. На каждом часовом баре (last 30d × 24h = 720):
   - Compute predictive_score retroactively (используя historical data
     до этого момента, БЕЗ look-ahead bias)
   - Если score ≥ 75 → PRIME trigger
4. Forward sim 72h через 15m klines
5. Output: WR/MFE/EV per tier + reverse audit top-20 gainers
"""
from __future__ import annotations
import asyncio
import logging
import time
import statistics
from datetime import datetime, timezone, timedelta
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor

import httpx

logger = logging.getLogger(__name__)

FAPI = 'https://fapi.binance.com'
http_client = httpx.Client(timeout=15.0, limits=httpx.Limits(max_connections=30, max_keepalive_connections=15))

LOOKBACK_DAYS = 30
KLINES_1H_DAYS = 40        # +10d warmup для EMA200
KLINES_15M_DAYS = 33       # 30d signals + 3d forward
FORWARD_BARS_15M = 288     # 72h × 4
SCAN_PAIRS_CAP = 150       # топ N пар для backtest
SCAN_HOUR_STEP = 4         # сканируем каждый 4-й час (180 ticks/pair = 720 hours / 4)
MIN_SCORE_FOR_RECORD = 45  # WATCH+

# In-memory progress state (опрашивается через GET endpoint)
_state = {
    'running': False,
    'started_at': None,
    'pairs_total': 0,
    'pairs_done': 0,
    'triggers_found': 0,
    'last_pair': '',
    'error': None,
    'finished_at': None,
}


def _get_state():
    return dict(_state)


def fapi_klines(symbol: str, tf: str, limit: int = 500, end_ms=None) -> list:
    params = {'symbol': symbol, 'interval': tf, 'limit': limit}
    if end_ms: params['endTime'] = end_ms
    try:
        r = http_client.get(f'{FAPI}/fapi/v1/klines', params=params)
        if r.status_code != 200: return []
        return [{'t': int(k[0]), 'o': float(k[1]), 'h': float(k[2]),
                 'l': float(k[3]), 'c': float(k[4]), 'v': float(k[5])}
                for k in r.json()]
    except Exception:
        return []


def fapi_oi_hist(symbol: str, period: str = '1h', limit: int = 500) -> list:
    """OI history: [{timestamp, sumOpenInterest, sumOpenInterestValue}]"""
    try:
        r = http_client.get(f'{FAPI}/futures/data/openInterestHist',
                            params={'symbol': symbol, 'period': period, 'limit': limit})
        if r.status_code != 200: return []
        data = r.json() or []
        return [{'t': int(d.get('timestamp', 0)),
                 'oi_usd': float(d.get('sumOpenInterestValue', 0))} for d in data]
    except Exception:
        return []


def fapi_funding_hist(symbol: str, limit: int = 100) -> list:
    """Funding rate history: [{fundingTime, fundingRate}]"""
    try:
        r = http_client.get(f'{FAPI}/fapi/v1/fundingRate',
                            params={'symbol': symbol, 'limit': limit})
        if r.status_code != 200: return []
        return [{'t': int(d.get('fundingTime', 0)),
                 'rate_pct': float(d.get('fundingRate', 0)) * 100}
                for d in r.json() or []]
    except Exception:
        return []


def _calc_ema(values, period):
    if not values or len(values) < period: return []
    out = [None] * len(values)
    seed = sum(values[:period]) / period
    out[period - 1] = seed
    k = 2 / (period + 1)
    for i in range(period, len(values)):
        out[i] = values[i] * k + out[i - 1] * (1 - k)
    return out


def _calc_rsi(closes, p=14):
    n = len(closes)
    if n < p + 1: return [None] * n
    rsi = [None] * n
    gains = [0.0]; losses = [0.0]
    for i in range(1, n):
        ch = closes[i] - closes[i-1]
        gains.append(max(ch, 0.0)); losses.append(max(-ch, 0.0))
    avg_g = sum(gains[1:p+1]) / p
    avg_l = sum(losses[1:p+1]) / p
    rsi[p] = 100.0 if avg_l == 0 else (100 - 100 / (1 + avg_g / avg_l))
    for i in range(p+1, n):
        avg_g = (avg_g * (p-1) + gains[i]) / p
        avg_l = (avg_l * (p-1) + losses[i]) / p
        rsi[i] = 100.0 if avg_l == 0 else (100 - 100 / (1 + avg_g / avg_l))
    return rsi


def _bb_width_at(closes, idx, period=20):
    if idx < period - 1: return None
    vals = closes[idx-period+1:idx+1]
    mean = sum(vals) / len(vals)
    var = sum((v - mean) ** 2 for v in vals) / len(vals)
    std = var ** 0.5
    return (4 * std) / mean if mean > 0 else 0


def _bb_squeeze_pct(closes, idx):
    """BB width percentile based на последних 100 барах до idx."""
    if idx < 120: return 50.0
    widths = []
    for i in range(idx - 100, idx + 1):
        w = _bb_width_at(closes, i)
        if w is not None: widths.append(w)
    if not widths: return 50.0
    current = widths[-1]
    sorted_w = sorted(widths)
    below = sum(1 for w in sorted_w if w < current)
    return below / len(sorted_w) * 100


def _rsi_compression(closes, idx, window=30):
    rsi = _calc_rsi(closes[:idx+1])
    recent = [r for r in rsi[-window:] if r is not None]
    if len(recent) < 10: return 50.0
    mean = sum(recent) / len(recent)
    std = (sum((r - mean) ** 2 for r in recent) / len(recent)) ** 0.5
    return max(0, min(100, 100 - std * 4))


def find_bar_idx(kl, ts_ms):
    lo, hi = 0, len(kl) - 1
    while lo <= hi:
        m = (lo + hi) // 2
        if kl[m]['t'] <= ts_ms: lo = m + 1
        else: hi = m - 1
    return hi


def compute_score_at(k1h: list, oi_hist: list, funding_hist: list,
                      bar_idx: int, sector_active: bool = False) -> dict:
    """Composite score retroactively на бар bar_idx.
    Uses ТОЛЬКО data до bar_idx (no look-ahead).
    """
    score = 0.0
    components = {}
    direction = 'LONG'

    if bar_idx < 168:
        return {'composite_score': 0, 'tier': 'norm', 'direction': 'LONG',
                'components': {}}

    # Slice klines до bar_idx
    kl_window = k1h[:bar_idx+1]
    bar_ts_ms = k1h[bar_idx]['t']
    closes = [k['c'] for k in kl_window]
    usd_vols = [k['c'] * k['v'] for k in kl_window]

    # 1. Volume profile (25 pts)
    vol_24h = sum(usd_vols[-24:])
    vol_7d_total = sum(usd_vols[-168:])
    vol_avg_7d = vol_7d_total / 7.0
    vol_score = (vol_24h / vol_avg_7d) if vol_avg_7d > 0 else 0
    if vol_score >= 4.0: components['volume'] = 25
    elif vol_score >= 3.0: components['volume'] = 20
    elif vol_score >= 2.0: components['volume'] = 15
    elif vol_score >= 1.5: components['volume'] = 8
    else: components['volume'] = 0
    score += components['volume']

    # 2. OI growth (20 pts)
    oi_at_bar = [d for d in oi_hist if d['t'] <= bar_ts_ms]
    oi_growth = 0
    if len(oi_at_bar) >= 24:
        oi_current = oi_at_bar[-1]['oi_usd']
        oi_24h_ago = oi_at_bar[-24]['oi_usd']
        if oi_24h_ago > 0:
            oi_growth = (oi_current - oi_24h_ago) / oi_24h_ago * 100
    if oi_growth >= 50: components['oi'] = 20
    elif oi_growth >= 30: components['oi'] = 15
    elif oi_growth >= 20: components['oi'] = 10
    elif oi_growth >= 10: components['oi'] = 5
    else: components['oi'] = 0
    score += components['oi']

    # 3. Funding (15 pts)
    funding_avg_24h = 0
    funding_at_bar = [d for d in funding_hist if d['t'] <= bar_ts_ms]
    if len(funding_at_bar) >= 3:
        last_3 = funding_at_bar[-3:]
        funding_avg_24h = sum(d['rate_pct'] for d in last_3) / 3
    if funding_avg_24h <= -0.05:
        components['funding'] = 15; direction = 'LONG'
    elif funding_avg_24h <= -0.02:
        components['funding'] = 10; direction = 'LONG'
    elif funding_avg_24h >= 0.10:
        components['funding'] = 12; direction = 'SHORT'
    elif funding_avg_24h >= 0.05:
        components['funding'] = 7; direction = 'SHORT'
    else: components['funding'] = 0
    score += components['funding']

    # 4. BB Squeeze (15 pts)
    bb_pct = _bb_squeeze_pct(closes, bar_idx)
    if bb_pct <= 10: components['bb_squeeze'] = 15
    elif bb_pct <= 20: components['bb_squeeze'] = 10
    elif bb_pct <= 30: components['bb_squeeze'] = 5
    else: components['bb_squeeze'] = 0
    score += components['bb_squeeze']

    # 5. Price flat (10 pts)
    if len(closes) >= 24:
        last_24 = closes[-24:]
        mean = sum(last_24) / len(last_24)
        std = (sum((v - mean) ** 2 for v in last_24) / len(last_24)) ** 0.5
        volatility = std / mean if mean > 0 else 1
        first, last = last_24[0], last_24[-1]
        change_pct = (last - first) / first * 100 if first > 0 else 0
        is_flat = volatility < 0.03 and abs(change_pct) < 4.0
        components['price_flat'] = 10 if is_flat else (5 if volatility < 0.05 else 0)
    else: components['price_flat'] = 0
    score += components['price_flat']

    # 6. Sector (10 pts) — внешний параметр
    components['sector'] = 10 if sector_active else 0
    score += components['sector']

    # 7. RSI compression (5 pts)
    rsi_comp = _rsi_compression(closes, bar_idx)
    components['rsi_comp'] = round(rsi_comp / 100 * 5, 1)
    score += components['rsi_comp']

    score = round(score, 1)
    if score >= 75: tier = 'PRIME'
    elif score >= 60: tier = 'STRONG'
    elif score >= 45: tier = 'WATCH'
    else: tier = 'norm'

    return {'composite_score': score, 'tier': tier,
            'direction': direction, 'components': components,
            'vol_score': round(vol_score, 2), 'oi_growth': round(oi_growth, 2),
            'funding_avg': round(funding_avg_24h, 4)}


def sim_forward_15m(k15: list, entry_ts_ms: int, entry_price: float,
                     direction: str) -> dict:
    idx = find_bar_idx(k15, entry_ts_ms)
    if idx < 0 or idx >= len(k15) - 4: return None
    fwd = k15[idx+1:idx+1+FORWARD_BARS_15M]
    if len(fwd) < 4: return None
    is_l = direction == 'LONG'
    highs = [k['h'] for k in fwd]; lows = [k['l'] for k in fwd]
    closes = [k['c'] for k in fwd]
    if is_l:
        mf = max(highs); ma = min(lows)
        mfe = (mf - entry_price) / entry_price * 100
        mae = (entry_price - ma) / entry_price * 100
    else:
        mf = min(lows); ma = max(highs)
        mfe = (entry_price - mf) / entry_price * 100
        mae = (ma - entry_price) / entry_price * 100
    final = (closes[-1] - entry_price) / entry_price * 100 if is_l else \
            (entry_price - closes[-1]) / entry_price * 100
    return {'mfe_pct': mfe, 'mae_pct': mae, 'final_pct': final}


def _to_fapi_symbol(pair):
    s = pair.replace('/', '').upper()
    if not s.endswith('USDT'): s += 'USDT'
    return s


def run_backtest_sync() -> dict:
    """Synchronous backtest entry. Writes progress to _state. Returns final results."""
    global _state
    _state = {
        'running': True, 'started_at': datetime.now(timezone.utc).isoformat(),
        'pairs_total': 0, 'pairs_done': 0, 'triggers_found': 0,
        'last_pair': '', 'error': None, 'finished_at': None,
    }

    try:
        from bingx_pairs import get_bingx_usdt_perp_pairs
        from database import _get_db
        db = _get_db()
        col_results = db.prepump_backtest_results

        # Step 1: Get pairs (BingX filter)
        pairs = list(get_bingx_usdt_perp_pairs())[:SCAN_PAIRS_CAP]
        _state['pairs_total'] = len(pairs)
        logger.info(f'[bt-prepump] starting backtest, {len(pairs)} pairs')

        # Step 2: For each pair fetch full historical data
        all_triggers = []
        for pi, pair in enumerate(pairs, 1):
            _state['last_pair'] = pair
            _state['pairs_done'] = pi - 1
            sym = _to_fapi_symbol(pair)
            try:
                # 1h klines: 40 дней = 960 bars (max fapi limit 1500)
                k1h_raw = fapi_klines(sym, '1h', 1000)
                if not k1h_raw or len(k1h_raw) < 200: continue

                # OI history: 1h × 30d = 720 bars (но limit fapi 500, fetch последние 500)
                oi_hist = fapi_oi_hist(sym, '1h', 500)
                # Funding rate: limit 100 (~33 дня при 3/day)
                fr_hist = fapi_funding_hist(sym, 100)

                # 15m klines for forward sim: 2 chunks
                k15_a = fapi_klines(sym, '15m', 1500)
                k15_b = []
                if k15_a:
                    k15_b = fapi_klines(sym, '15m', 1500, end_ms=k15_a[0]['t'] - 1)
                k15 = k15_b + k15_a
                seen = set(); uniq15 = []
                for k in k15:
                    if k['t'] in seen: continue
                    seen.add(k['t']); uniq15.append(k)
                uniq15.sort(key=lambda k: k['t'])

                if len(uniq15) < 100: continue

                # Step 3: Scan hourly bars от last 30d backward
                now_ms = int(time.time() * 1000)
                cutoff_ms = now_ms - LOOKBACK_DAYS * 86400 * 1000
                # last_trigger_ts чтобы не спамить (rate limit 4h per pair)
                last_trigger_ts = 0

                for idx in range(200, len(k1h_raw), SCAN_HOUR_STEP):
                    bar_ts_ms = k1h_raw[idx]['t']
                    if bar_ts_ms < cutoff_ms: continue
                    # Skip если less than 72h forward данных
                    if bar_ts_ms + 72 * 3600 * 1000 > now_ms - 60000: break

                    res = compute_score_at(k1h_raw, oi_hist, fr_hist, idx, sector_active=False)
                    if res['composite_score'] < MIN_SCORE_FOR_RECORD: continue

                    # Rate limit per pair (4h between same-pair triggers)
                    if bar_ts_ms - last_trigger_ts < 4 * 3600 * 1000:
                        continue
                    last_trigger_ts = bar_ts_ms

                    # Forward sim
                    entry_price = k1h_raw[idx]['c']
                    outcome = sim_forward_15m(uniq15, bar_ts_ms, entry_price, res['direction'])
                    if not outcome: continue

                    trig = {
                        'pair': pair, 'at_ts': bar_ts_ms // 1000,
                        'at': datetime.fromtimestamp(bar_ts_ms / 1000, tz=timezone.utc),
                        'entry': entry_price,
                        'direction': res['direction'],
                        'composite_score': res['composite_score'],
                        'tier': res['tier'],
                        'components': res['components'],
                        'vol_score': res.get('vol_score', 0),
                        'oi_growth': res.get('oi_growth', 0),
                        'funding_avg': res.get('funding_avg', 0),
                        **outcome,
                    }
                    all_triggers.append(trig)
                    _state['triggers_found'] = len(all_triggers)
            except Exception as e:
                logger.debug(f'[bt-prepump] pair {pair}: {e}')

        _state['pairs_done'] = len(pairs)
        logger.info(f'[bt-prepump] total triggers={len(all_triggers)}, analyzing...')

        # Step 4: Aggregate stats
        by_tier = defaultdict(list)
        for t in all_triggers: by_tier[t['tier']].append(t)

        def _stats(items):
            if not items: return {}
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

        def _ev_grid(items):
            out = {}
            for tp, sl in [(5,3), (7,4), (10,5), (15,7), (20,10)]:
                tp_hit = sum(1 for r in items if r['mfe_pct'] >= tp and r['mae_pct'] < sl)
                sl_hit = sum(1 for r in items if r['mae_pct'] >= sl)
                ev = (tp_hit / max(len(items),1)) * tp - (sl_hit / max(len(items),1)) * sl
                out[f'tp{tp}_sl{sl}'] = {'tp_hit_pct': round(tp_hit/max(len(items),1)*100,1),
                                          'sl_hit_pct': round(sl_hit/max(len(items),1)*100,1),
                                          'ev_pct': round(ev, 2)}
            return out

        report = {
            'lookback_days': LOOKBACK_DAYS,
            'pairs_scanned': len(pairs),
            'total_triggers': len(all_triggers),
            'started_at': _state['started_at'],
            'finished_at': datetime.now(timezone.utc).isoformat(),
            'by_tier': {
                'PRIME': {**_stats(by_tier['PRIME']), 'ev_grid': _ev_grid(by_tier['PRIME'])},
                'STRONG': {**_stats(by_tier['STRONG']), 'ev_grid': _ev_grid(by_tier['STRONG'])},
                'WATCH': {**_stats(by_tier['WATCH']), 'ev_grid': _ev_grid(by_tier['WATCH'])},
            },
            'frequency_per_day': {
                'PRIME': round(len(by_tier['PRIME']) / LOOKBACK_DAYS, 2),
                'STRONG': round(len(by_tier['STRONG']) / LOOKBACK_DAYS, 2),
                'WATCH': round(len(by_tier['WATCH']) / LOOKBACK_DAYS, 2),
            },
        }

        # Step 5: Top PRIME triggers (для manual review)
        primes_sorted = sorted(by_tier['PRIME'], key=lambda r: -r['mfe_pct'])[:30]
        report['top_primes'] = [
            {'pair': r['pair'], 'at': r['at'].isoformat() if hasattr(r['at'], 'isoformat') else str(r['at']),
              'score': r['composite_score'], 'direction': r['direction'],
              'mfe_pct': round(r['mfe_pct'], 2), 'mae_pct': round(r['mae_pct'], 2),
              'final_pct': round(r['final_pct'], 2)}
            for r in primes_sorted
        ]

        # Save individual triggers + report
        try:
            col_results.delete_many({})  # clear old
            for t in all_triggers:
                col_results.insert_one(t)
            db.prepump_backtest_report.delete_many({})
            db.prepump_backtest_report.insert_one(report)
        except Exception as e:
            logger.warning(f'[bt-prepump] mongo write fail: {e}')

        _state['running'] = False
        _state['finished_at'] = datetime.now(timezone.utc).isoformat()
        logger.info(f'[bt-prepump] DONE: {len(all_triggers)} triggers')
        return report
    except Exception as e:
        logger.exception('[bt-prepump] fatal error')
        _state['error'] = str(e)
        _state['running'] = False
        _state['finished_at'] = datetime.now(timezone.utc).isoformat()
        return {'error': str(e)}
