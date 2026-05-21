"""🦈 SHARK signal detector — SHORT mirror of WHALE.

Pattern hierarchy from 20-chart manual SHORT analysis (25 entries):
  Distribution Top Breakdown  68% (17/25)
    - Multi-Top variant (48% of all)  ← TOP premium amplifier
    - Extreme pump prior (≥50%)
    - Strong pump (30-49%)
    - Long distribution (≥10d)
    - Hybrid w/ blow-off candle
  Blow-Off Climax              24% (6/25)
    - Pure extreme (no distribution)
    - Blow-off + brief distribution
  Lower-High Continuation       8% (2/25)

Universal markers (100% hit rate on 25 entries):
  - ST flip 2H DOWN (CORE trigger)
  - Volume spike ≥2× in 6h window
  - RSI cross below SMA(RSI, 14)

Mirror symmetry with WHALE (LONG):
  WHALE Range Breakout       ↔ SHARK Distribution Top
  WHALE Multi-Bottom         ↔ SHARK Multi-Top (DOMINANT)
  WHALE Capitulation V-bottom ↔ SHARK Blow-Off Climax
  WHALE Long Base post-DT    ↔ SHARK Long Distribution
  WHALE Higher-Low base      ↔ SHARK Lower-High Continuation
  WHALE Capitulation wick    ↔ SHARK Failed-breakout wick (upper)
"""
from __future__ import annotations
import logging
import statistics
from typing import Optional

logger = logging.getLogger(__name__)

# ── Scoring weights ─────────────────────────────────────────────
CORE_SCORE = 30  # base for ST_flip_2H_DOWN + vol_spike_2x

AMPLIFIERS = {
    # PATH A — Distribution Top (68%)
    'distribution_3d':       15,
    'distribution_10d':      25,
    'multi_top':             20,   # ★ Top premium feature (48% of cases)
    'prior_uptrend_15':      10,
    'prior_uptrend_30':      20,
    'prior_uptrend_50':      30,   # extreme (USELESS/TRUTH-style)
    'failed_breakout_wick':  15,   # upper wick > 2× body on peak candle
    # PATH B — Blow-Off Climax (24%)
    'blow_off_20':           25,   # single candle move ≥20%
    'blow_off_30':           30,   # ≥30% (extreme)
    'upper_wick_ratio_2':    15,   # wick_up / body ≥ 2
    'rsi_extreme_overbought': 15,  # RSI was >85 + now crossing
    'vol_ratio_5x':          10,
    'vol_ratio_10x':         15,
    # PATH C — Lower-High Continuation (8%)
    'lower_high':            15,
    'established_downtrend': 10,   # ST 1D = DOWN
    # Universal
    'rsi_cross_below_sma':   10,
}

ANTI_MARKERS = {
    # TBD — recalibrate after precondition analysis on SHORT signals
    'had_cv_flip_long_24h':  -10,  # bullish marker = anti-SHORT
    'had_st_mtf_long_24h':   -10,
}

TIER_THRESHOLDS = {
    'PREMIUM':   80,
    'STANDARD':  60,
    'MARGINAL':  40,
}


# ── Indicator helpers ──────────────────────────────────────────

def _rsi_14(closes: list[float]) -> list[float]:
    """Wilder's RSI(14)."""
    n = len(closes)
    if n < 15:
        return [0.0] * n
    gains, losses = [], []
    for i in range(1, n):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    rsi = [0.0] * n
    avg_g = sum(gains[:14]) / 14
    avg_l = sum(losses[:14]) / 14
    rs = avg_g / avg_l if avg_l > 0 else 1e9
    rsi[14] = 100 - (100 / (1 + rs))
    for i in range(15, n):
        avg_g = (avg_g * 13 + gains[i-1]) / 14
        avg_l = (avg_l * 13 + losses[i-1]) / 14
        rs = avg_g / avg_l if avg_l > 0 else 1e9
        rsi[i] = 100 - (100 / (1 + rs))
    return rsi


def _sma(vals: list[float], period: int) -> list[float]:
    n = len(vals)
    out = [0.0] * n
    if n < period:
        return out
    for i in range(period - 1, n):
        out[i] = sum(vals[i - period + 1: i + 1]) / period
    return out


# ── CORE: Volume spike (same as WHALE — direction-agnostic) ───

def check_vol_spike(candles_2h: list[dict], flip_idx: int,
                    window_bars: int = 3) -> tuple[bool, float]:
    """Volume spike ≥2× in window [flip_idx-1 .. flip_idx+window_bars].
    Real-time-friendly: window clamped к границам массива.
    """
    if flip_idx < 30 or flip_idx >= len(candles_2h):
        return (False, 0.0)
    baseline = [c['v'] for c in candles_2h[flip_idx - 30: flip_idx]]
    sma9 = statistics.mean(baseline) if baseline else 0
    if sma9 <= 0:
        return (False, 0.0)
    start = max(0, flip_idx - 1)
    end = min(len(candles_2h), flip_idx + window_bars + 1)
    max_ratio = 0.0
    for i in range(start, end):
        r = candles_2h[i]['v'] / sma9
        if r > max_ratio:
            max_ratio = r
    return (max_ratio >= 2.0, round(max_ratio, 2))


# ── PATH A: Distribution Top ──────────────────────────────────

def check_distribution_top(candles_2h: list[dict], flip_idx: int,
                            amplitude_pct: float = 6.0) -> int:
    """Подсчёт дней в которые price оставался в верхней band ≤ amplitude_pct
    от entry-price, looking backwards from flip_idx.
    Returns days (int).
    """
    if flip_idx < 30:
        return 0
    entry_price = candles_2h[flip_idx]['c']
    if entry_price <= 0:
        return 0
    max_lookback = min(flip_idx, 240)
    bars_in_range = 0
    # Для distribution top "band" — price может быть в [entry*(1-amplitude), entry*(1+amplitude)]
    band_lo = entry_price * (1 - amplitude_pct / 100)
    band_hi = entry_price * (1 + amplitude_pct / 100)
    for i in range(flip_idx - 1, flip_idx - max_lookback - 1, -1):
        c = candles_2h[i]
        if band_lo <= c['c'] <= band_hi:
            bars_in_range += 1
        else:
            break
    return bars_in_range // 12  # 12 баров 2H = 1 день


def check_multi_top(candles_2h: list[dict], flip_idx: int,
                     lookback_days: int = 10,
                     tolerance_pct: float = 0.7) -> int:
    """Подсчёт количества wick rejections на одном уровне (top wicks).
    Multi-top = ≥3 пика на одинаковой высоте.

    Algorithm:
      - Find max high in lookback window
      - Count candles with high >= max_high * (1 - tolerance_pct/100)
      - Each rejection wick (long upper wick + close lower) = 1 top
    Returns count (≥3 = multi-top trigger).
    """
    if flip_idx < 30:
        return 0
    bars = min(lookback_days * 12, flip_idx)
    window = candles_2h[flip_idx - bars: flip_idx]
    if not window:
        return 0
    max_high = max(c['h'] for c in window)
    if max_high <= 0:
        return 0
    threshold = max_high * (1 - tolerance_pct / 100)
    # Count separate "tops" — must be spaced ≥4 bars apart to count as distinct
    tops_indices = []
    for i, c in enumerate(window):
        if c['h'] >= threshold:
            # Verify it's a rejection (close significantly below high)
            body = abs(c['c'] - c['o'])
            upper_wick = c['h'] - max(c['o'], c['c'])
            # Either long upper wick OR red close near top
            if upper_wick > 0.5 * body or c['c'] < c['o']:
                # Spacing check (skip if too close to last counted top)
                if not tops_indices or i - tops_indices[-1] >= 4:
                    tops_indices.append(i)
    return len(tops_indices)


def check_prior_uptrend(candles_2h: list[dict], flip_idx: int,
                          lookback_days: int = 14) -> float:
    """Returns max rise % from low to peak (within lookback window).
    Mirror of check_prior_downtrend in WHALE.
    """
    if flip_idx < 30:
        return 0.0
    bars = min(lookback_days * 12, flip_idx)
    window = candles_2h[flip_idx - bars: flip_idx]
    if not window:
        return 0.0
    low = min(c['l'] for c in window)
    entry = candles_2h[flip_idx]['c']
    if low <= 0 or entry <= 0:
        return 0.0
    rise_pct = (entry - low) / low * 100
    return max(0.0, rise_pct)


def check_failed_breakout_wick(candles_2h: list[dict], flip_idx: int,
                                 lookback_days: int = 5) -> bool:
    """Detects long upper wick (failed breakout up) in recent peak candles.
    Mirror of capitulation_wick (which checks long lower wick).
    Criteria: upper_wick > 2× body AND wick depth > 1.5× ATR.
    """
    if flip_idx < 30:
        return False
    bars = min(lookback_days * 12, flip_idx)
    window = candles_2h[flip_idx - bars: flip_idx]
    if len(window) < 5:
        return False
    trs = []
    for i in range(1, len(window)):
        h, l, pc = window[i]['h'], window[i]['l'], window[i-1]['c']
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = statistics.mean(trs) if trs else 0
    if atr <= 0:
        return False
    for c in window:
        body = abs(c['c'] - c['o'])
        upper_wick = c['h'] - max(c['o'], c['c'])
        if upper_wick <= 0:
            continue
        if body > 0 and upper_wick > 2 * body and upper_wick > 1.5 * atr:
            return True
        # Edge case: tiny body, huge wick
        if body == 0 and upper_wick > 2 * atr:
            return True
    return False


# ── PATH B: Blow-Off Climax ───────────────────────────────────

def check_blow_off(candles_2h: list[dict], flip_idx: int,
                    lookback_bars: int = 6) -> dict:
    """Detects single-candle blow-off (climax pump) in recent bars.
    Returns dict with:
      - max_single_move_pct: largest open-to-high % move in window
      - upper_wick_ratio: max ratio of upper_wick/body in window
      - blow_off_candle_idx: index of candle if detected
    """
    if flip_idx < 30:
        return {'max_single_move_pct': 0.0, 'upper_wick_ratio': 0.0,
                'blow_off_candle_idx': None}
    start = max(0, flip_idx - lookback_bars)
    window = candles_2h[start: flip_idx + 1]
    max_move = 0.0
    max_ratio = 0.0
    boi = None
    for i, c in enumerate(window):
        # Single-candle move from open to high
        if c['o'] > 0:
            move = (c['h'] - c['o']) / c['o'] * 100
            if move > max_move:
                max_move = move
                boi = start + i
        # Upper wick / body
        body = abs(c['c'] - c['o'])
        upper_wick = c['h'] - max(c['o'], c['c'])
        if body > 0:
            ratio = upper_wick / body
            if ratio > max_ratio:
                max_ratio = ratio
    return {
        'max_single_move_pct': round(max_move, 2),
        'upper_wick_ratio': round(max_ratio, 2),
        'blow_off_candle_idx': boi,
    }


def check_rsi_extreme(candles_2h: list[dict], flip_idx: int,
                       lookback_bars: int = 6) -> bool:
    """Returns True если RSI был >85 в lookback window AND сейчас crossing
    below SMA(RSI,14). Mirror of "RSI extreme oversold" в WHALE.
    """
    if flip_idx < 30:
        return False
    closes = [c['c'] for c in candles_2h[: flip_idx + 1]]
    rsi = _rsi_14(closes)
    rsi_sma = _sma(rsi, 14)
    start = max(20, flip_idx - lookback_bars)
    was_extreme = any(rsi[i] >= 85 for i in range(start, flip_idx + 1))
    if not was_extreme:
        return False
    # Now check if crossing below SMA recently
    for i in range(max(20, flip_idx - 3), flip_idx + 1):
        if i > 0 and rsi[i-1] >= rsi_sma[i-1] and rsi[i] < rsi_sma[i]:
            return True
    return False


# ── PATH C: Lower-High Continuation ───────────────────────────

def check_lower_high(candles_2h: list[dict], flip_idx: int,
                      lookback_days: int = 7) -> bool:
    """Detects lower-high pattern: current peak is below a previous peak
    within lookback window (by ≥5%). Mirror of higher_low.
    """
    if flip_idx < 30:
        return False
    bars = min(lookback_days * 12, flip_idx)
    window = candles_2h[flip_idx - bars: flip_idx + 1]
    if len(window) < 12:
        return False
    # Current local peak: max of last 6 bars
    recent_peak = max(c['h'] for c in window[-6:])
    # Prior peak: max of earlier portion (excluding last 6 bars)
    if len(window) - 6 < 6:
        return False
    prior_peak = max(c['h'] for c in window[:-6])
    # Lower high = recent peak is at least 5% below prior peak
    if prior_peak > 0 and recent_peak < prior_peak * 0.95:
        return True
    return False


# ── Universal: RSI cross BELOW SMA ─────────────────────────────

def check_rsi_cross_below(candles_2h: list[dict], flip_idx: int,
                            lookback_bars: int = 4) -> bool:
    """True if RSI crossed BELOW SMA(RSI,14) within lookback_bars before/at
    flip_idx. Mirror of check_rsi_cross в WHALE.
    """
    if flip_idx < 30:
        return False
    closes = [c['c'] for c in candles_2h[: flip_idx + 1]]
    rsi = _rsi_14(closes)
    rsi_sma = _sma(rsi, 14)
    start = max(20, flip_idx - lookback_bars)
    for i in range(start + 1, flip_idx + 1):
        if rsi[i-1] >= rsi_sma[i-1] and rsi[i] < rsi_sma[i]:
            return True
    return False


# ── Anti-marker checks ──────────────────────────────────────

def check_anti_markers(db, pair: str, flip_ts: int,
                        direction: str = 'SHORT') -> dict[str, bool]:
    """Anti-markers для SHORT: bullish signals in last 24h hurt SHORT setups.
    Returns flags for LONG-direction cv_flip / st_mtf as anti-markers.
    """
    from datetime import datetime, timezone, timedelta
    flags = {'had_cv_flip_long_24h': False, 'had_st_mtf_long_24h': False}
    try:
        cutoff = datetime.fromtimestamp(flip_ts - 24 * 3600, tz=timezone.utc)
        end = datetime.fromtimestamp(flip_ts, tz=timezone.utc)
        # cv_flip LONG в окне = anti-SHORT
        cv = db.cv_flip_signals.find_one({
            'pair': pair,
            'cv_triggered_at': {'$gte': cutoff, '$lt': end},
        })
        if cv and (cv.get('direction', '') or '').upper() == 'LONG':
            flags['had_cv_flip_long_24h'] = True
        # st_mtf LONG в окне = anti-SHORT
        st = db.supertrend_signals.find_one({
            'pair': pair, 'tier': 'mtf',
            'flip_at': {'$gte': cutoff, '$lt': end},
        })
        if st and (st.get('direction', '') or '').upper() == 'LONG':
            flags['had_st_mtf_long_24h'] = True
    except Exception:
        pass
    return flags


# ── SHARK scoring (main entry) ─────────────────────────────────

def compute_shark_score(candles_2h: list[dict], flip_idx: int,
                         anti_flags: Optional[dict] = None,
                         st_1d_state: Optional[str] = None) -> dict:
    """Computes SHARK score for a 2H ST flip DOWN at flip_idx.

    Args:
      candles_2h: list of {t,o,h,l,c,v}
      flip_idx: index of the flip-bar (last closed bar)
      anti_flags: dict from check_anti_markers (optional)
      st_1d_state: 'UP'/'DOWN'/None — for Path C amplifier

    Returns: dict with score, tier, breakdown, indicators, passes_core
    """
    if flip_idx < 30 or flip_idx >= len(candles_2h):
        return {'score': 0, 'tier': None, 'passes_core': False,
                'breakdown': {}, 'indicators': {}}

    breakdown: dict = {}
    indicators: dict = {}

    # CORE: vol spike (ST flip given by caller)
    vol_ok, vol_ratio = check_vol_spike(candles_2h, flip_idx)
    indicators['vol_ratio_max'] = vol_ratio
    if not vol_ok:
        return {'score': 0, 'tier': None, 'passes_core': False,
                'breakdown': {'vol_spike_failed': True},
                'indicators': indicators}

    score = CORE_SCORE
    breakdown['core_st_flip_vol_spike'] = CORE_SCORE

    # PATH A — Distribution Top
    distribution_days = check_distribution_top(candles_2h, flip_idx)
    indicators['distribution_days'] = distribution_days
    if distribution_days >= 10:
        score += AMPLIFIERS['distribution_10d']
        breakdown['distribution_10d'] = AMPLIFIERS['distribution_10d']
    elif distribution_days >= 3:
        score += AMPLIFIERS['distribution_3d']
        breakdown['distribution_3d'] = AMPLIFIERS['distribution_3d']

    multi_top_count = check_multi_top(candles_2h, flip_idx)
    indicators['multi_top_count'] = multi_top_count
    if multi_top_count >= 3:
        score += AMPLIFIERS['multi_top']
        breakdown['multi_top'] = AMPLIFIERS['multi_top']

    prior_uptrend = check_prior_uptrend(candles_2h, flip_idx)
    indicators['prior_uptrend_pct'] = round(prior_uptrend, 1)
    if prior_uptrend >= 50:
        score += AMPLIFIERS['prior_uptrend_50']
        breakdown['prior_uptrend_50'] = AMPLIFIERS['prior_uptrend_50']
    elif prior_uptrend >= 30:
        score += AMPLIFIERS['prior_uptrend_30']
        breakdown['prior_uptrend_30'] = AMPLIFIERS['prior_uptrend_30']
    elif prior_uptrend >= 15:
        score += AMPLIFIERS['prior_uptrend_15']
        breakdown['prior_uptrend_15'] = AMPLIFIERS['prior_uptrend_15']

    failed_wick = check_failed_breakout_wick(candles_2h, flip_idx)
    indicators['failed_breakout_wick'] = failed_wick
    if failed_wick:
        score += AMPLIFIERS['failed_breakout_wick']
        breakdown['failed_breakout_wick'] = AMPLIFIERS['failed_breakout_wick']

    # PATH B — Blow-Off Climax
    blow_off = check_blow_off(candles_2h, flip_idx)
    indicators['blow_off_max_move_pct'] = blow_off['max_single_move_pct']
    indicators['upper_wick_ratio'] = blow_off['upper_wick_ratio']
    if blow_off['max_single_move_pct'] >= 30:
        score += AMPLIFIERS['blow_off_30']
        breakdown['blow_off_30'] = AMPLIFIERS['blow_off_30']
    elif blow_off['max_single_move_pct'] >= 20:
        score += AMPLIFIERS['blow_off_20']
        breakdown['blow_off_20'] = AMPLIFIERS['blow_off_20']

    if blow_off['upper_wick_ratio'] >= 2:
        score += AMPLIFIERS['upper_wick_ratio_2']
        breakdown['upper_wick_ratio_2'] = AMPLIFIERS['upper_wick_ratio_2']

    rsi_extreme = check_rsi_extreme(candles_2h, flip_idx)
    indicators['rsi_extreme_overbought'] = rsi_extreme
    if rsi_extreme:
        score += AMPLIFIERS['rsi_extreme_overbought']
        breakdown['rsi_extreme_overbought'] = AMPLIFIERS['rsi_extreme_overbought']

    if vol_ratio >= 10:
        score += AMPLIFIERS['vol_ratio_10x']
        breakdown['vol_ratio_10x'] = AMPLIFIERS['vol_ratio_10x']
    elif vol_ratio >= 5:
        score += AMPLIFIERS['vol_ratio_5x']
        breakdown['vol_ratio_5x'] = AMPLIFIERS['vol_ratio_5x']

    # PATH C — Lower-High Continuation
    lower_high = check_lower_high(candles_2h, flip_idx)
    indicators['lower_high'] = lower_high
    if lower_high:
        score += AMPLIFIERS['lower_high']
        breakdown['lower_high'] = AMPLIFIERS['lower_high']

    if st_1d_state == 'DOWN':
        score += AMPLIFIERS['established_downtrend']
        breakdown['established_downtrend'] = AMPLIFIERS['established_downtrend']

    # Universal: RSI cross below SMA
    rsi_x = check_rsi_cross_below(candles_2h, flip_idx)
    indicators['rsi_cross_below_sma'] = rsi_x
    if rsi_x:
        score += AMPLIFIERS['rsi_cross_below_sma']
        breakdown['rsi_cross_below_sma'] = AMPLIFIERS['rsi_cross_below_sma']

    # Anti-markers
    if anti_flags:
        for k, v in anti_flags.items():
            if v and k in ANTI_MARKERS:
                score += ANTI_MARKERS[k]
                breakdown[k] = ANTI_MARKERS[k]

    # Tier classification
    tier = None
    if score >= TIER_THRESHOLDS['PREMIUM']:
        tier = 'PREMIUM'
    elif score >= TIER_THRESHOLDS['STANDARD']:
        tier = 'STANDARD'
    elif score >= TIER_THRESHOLDS['MARGINAL']:
        tier = 'MARGINAL'

    return {
        'score': score,
        'tier': tier,
        'passes_core': True,
        'breakdown': breakdown,
        'indicators': indicators,
    }


# ── Real-time SHARK trigger (called from watcher.py) ──────────

SHARK_COOLDOWN_S_LIVE = 12 * 3600  # 12h per pair


def maybe_fire_shark(signal_data: dict) -> dict | None:
    """Triggered when supertrend signal arrives in watcher.py.
    Real-time: считаем SHARK score на последнем закрытом 2H баре.
    Fires только SHORT, только STANDARD+PREMIUM tier.
    """
    pair = signal_data.get('pair', '?')
    try:
        source = signal_data.get('source', '')
        if source != 'supertrend':
            return None
        tier = signal_data.get('st_tier', '')
        if tier not in ('vip', 'mtf'):
            logger.debug(f'[shark-live] {pair} skip — tier={tier}')
            return None
        direction = (signal_data.get('direction', '') or '').upper()
        if direction != 'SHORT':
            logger.debug(f'[shark-live] {pair} skip — direction={direction}')
            return None

        if not pair or pair == '?':
            return None

        # Cooldown
        from database import _get_db, utcnow
        from datetime import datetime, timezone, timedelta
        db = _get_db()
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=SHARK_COOLDOWN_S_LIVE)
        recent = db.new_strategy_signals.find_one({
            'pair': pair, 'strategy': 'shark',
            'created_at': {'$gte': cutoff},
        })
        if recent:
            logger.info(f'[shark-live] {pair} cooldown')
            return None

        # Fetch 2H klines
        from exchange import get_klines_any
        candles = get_klines_any(pair, '2h', 350)
        if not candles or len(candles) < 100:
            logger.warning(f'[shark-live] {pair} few 2H klines: {len(candles) if candles else 0}')
            return None

        flip_idx = len(candles) - 2  # last closed bar
        flip_bar_ts = candles[flip_idx]['t']

        # Anti-markers
        anti_flags = check_anti_markers(db, pair, int(flip_bar_ts / 1000), 'SHORT')

        # Optional: 1D ST state (try cache_only — не блокируем event loop)
        st_1d_state = None
        try:
            from supertrend import supertrend_state
            st1d = supertrend_state(pair, '1d', cache_only=True)
            if st1d and st1d.get('state'):
                st_1d_state = st1d['state']
        except Exception:
            pass

        # Score
        score_res = compute_shark_score(candles, flip_idx, anti_flags, st_1d_state)
        ind = score_res.get('indicators', {})
        if not score_res['passes_core']:
            logger.info(f'[shark-live] {pair} no_core — vol={ind.get("vol_ratio_max")}')
            return None
        tier = score_res.get('tier')
        if not tier:
            logger.info(f'[shark-live] {pair} no_tier — score={score_res["score"]}')
            return None
        if tier == 'MARGINAL':
            logger.info(f'[shark-live] {pair} MARGINAL skip — score={score_res["score"]}')
            return None

        entry = signal_data.get('entry') or candles[flip_idx]['c']
        try: entry = float(entry)
        except Exception:
            return None

        doc = {
            'pair': pair,
            'symbol': pair.replace('/', '').upper(),
            'direction': 'SHORT',
            'entry': entry,
            'strategy': 'shark',
            'shark_score': score_res['score'],
            'shark_tier': tier,
            'shark_breakdown': score_res['breakdown'],
            'shark_indicators': ind,
            'created_at': utcnow(),
            'state': 'NEW',
            'tp_R': 2.0,
        }
        try:
            db.new_strategy_signals.insert_one(doc)
            logger.info(f'[shark-live] 🦈 FIRED {pair} {tier} '
                         f'score={score_res["score"]} '
                         f'multi_top={ind.get("multi_top_count")} '
                         f'amp={list(score_res["breakdown"].keys())}')
        except Exception as e:
            logger.warning(f'[shark-live] {pair} insert fail: {e}')
        return doc
    except Exception:
        logger.exception(f'[shark-live] {pair} maybe_fire fail')
        return None


# ── Safety-net scanner (mirror WHALE) ─────────────────────────

def scan_recent_flips_for_shark(pairs: list[str] | None = None,
                                  lookback_hours: int = 6) -> dict:
    """Сканит топ пары на пропущенные SHARK flips DOWN (mirror WHALE scanner)."""
    from database import _get_db, utcnow
    from datetime import datetime, timezone, timedelta
    from exchange import get_klines_any

    db = _get_db()
    if pairs is None:
        since = datetime.now(timezone.utc) - timedelta(days=7)
        pairs = sorted({s['pair'] for s in db.supertrend_signals.find(
            {'flip_at': {'$gte': since}, 'tier': {'$in': ['vip', 'mtf']}},
            {'pair': 1}) if s.get('pair')})
    if not pairs:
        return {'scanned': 0, 'flips_found': 0, 'fired': 0,
                'by_tier': {}, 'errors': 0}

    stats = {'scanned': 0, 'flips_found': 0, 'fired': 0,
             'by_tier': {'PREMIUM': 0, 'STANDARD': 0, 'MARGINAL': 0},
             'errors': 0, 'examples': [],
             'fired_docs': []}
    now_ts = int(datetime.now(timezone.utc).timestamp())
    cutoff_ms = (now_ts - lookback_hours * 3600) * 1000
    cooldown_dt = datetime.now(timezone.utc) - timedelta(seconds=SHARK_COOLDOWN_S_LIVE)

    for pair in pairs:
        stats['scanned'] += 1
        try:
            candles = get_klines_any(pair, '2h', 350)
            if not candles or len(candles) < 100:
                continue

            # Inline SuperTrend trend[] computation
            n = len(candles)
            closes = [c['c'] for c in candles]
            highs = [c['h'] for c in candles]
            lows = [c['l'] for c in candles]
            period, mult = 10, 3.0
            tr = [highs[0] - lows[0]]
            for i in range(1, n):
                tr.append(max(highs[i] - lows[i],
                              abs(highs[i] - closes[i-1]),
                              abs(lows[i] - closes[i-1])))
            atr = [None] * n
            atr[period - 1] = sum(tr[:period]) / period
            for i in range(period, n):
                atr[i] = (atr[i-1] * (period - 1) + tr[i]) / period
            hl2 = [(highs[i] + lows[i]) / 2 for i in range(n)]
            final_upper = [0.0] * n
            final_lower = [0.0] * n
            trend = [0] * n
            for i in range(n):
                if atr[i] is None: continue
                bu = hl2[i] + mult * atr[i]
                bl = hl2[i] - mult * atr[i]
                if i == 0 or atr[i-1] is None:
                    final_upper[i] = bu; final_lower[i] = bl; trend[i] = 1
                    continue
                final_upper[i] = bu if (bu < final_upper[i-1] or
                                         closes[i-1] > final_upper[i-1]) else final_upper[i-1]
                final_lower[i] = bl if (bl > final_lower[i-1] or
                                         closes[i-1] < final_lower[i-1]) else final_lower[i-1]
                if trend[i-1] == 1 and closes[i] < final_lower[i-1]: trend[i] = -1
                elif trend[i-1] == -1 and closes[i] > final_upper[i-1]: trend[i] = 1
                else: trend[i] = trend[i-1] if trend[i-1] != 0 else 1

            # Найти последний UP→DOWN flip в окне lookback (для SHARK)
            flip_idx = None
            for i in range(n - 1, 0, -1):
                if candles[i]['t'] < cutoff_ms:
                    break
                if trend[i] == -1 and trend[i-1] == 1:
                    flip_idx = i
                    break
            if flip_idx is None:
                continue
            stats['flips_found'] += 1

            # Cooldown check
            recent = db.new_strategy_signals.find_one({
                'pair': pair, 'strategy': 'shark',
                'created_at': {'$gte': cooldown_dt},
            })
            if recent:
                continue

            # Score
            anti_flags = check_anti_markers(db, pair, int(candles[flip_idx]['t'] / 1000),
                                              'SHORT')
            score_res = compute_shark_score(candles, flip_idx, anti_flags)
            if not score_res['passes_core']:
                continue
            tier = score_res.get('tier')
            if not tier or tier == 'MARGINAL':
                continue

            entry = candles[flip_idx]['c']
            flip_dt = datetime.fromtimestamp(candles[flip_idx]['t'] / 1000,
                                              tz=timezone.utc)
            doc = {
                'pair': pair,
                'symbol': pair.replace('/', '').upper(),
                'direction': 'SHORT',
                'entry': entry,
                'strategy': 'shark',
                'shark_score': score_res['score'],
                'shark_tier': tier,
                'shark_breakdown': score_res['breakdown'],
                'shark_indicators': score_res['indicators'],
                'created_at': flip_dt,
                'state': 'SCANNED',
                'tp_R': 2.0,
            }
            try:
                if db.new_strategy_signals.find_one({
                    'pair': pair, 'strategy': 'shark',
                    'created_at': flip_dt,
                }):
                    continue
                db.new_strategy_signals.insert_one(doc)
                stats['fired'] += 1
                stats['by_tier'][tier] += 1
                stats['examples'].append({
                    'pair': pair, 'tier': tier,
                    'score': score_res['score'],
                    'flip_at': flip_dt.isoformat(),
                })
                stats['fired_docs'].append(dict(doc))
                logger.info(f'[shark-scan] 🦈 SCANNED {pair} {tier} '
                             f'score={score_res["score"]}')
            except Exception as e:
                logger.warning(f'[shark-scan] insert {pair}: {e}')
                stats['errors'] += 1
        except Exception:
            stats['errors'] += 1
            logger.debug(f'[shark-scan] {pair} error', exc_info=True)

    logger.info(f'[shark-scan] DONE scanned={stats["scanned"]} '
                 f'flips={stats["flips_found"]} fired={stats["fired"]} '
                 f'by_tier={stats["by_tier"]}')
    return stats
