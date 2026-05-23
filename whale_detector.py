"""🐋 WHALE signal detector — based on 20-chart manual analysis.

Pattern hierarchy from 20 winning long entries:
  Range Breakout         50% (10/20)
    - Premium (≥10d compression + DT/capitulation)
    - Standard (5-10d base)
    - P&D risk (no amplifier)
  Reversal               20% (4/20)
  Long Base post-DT      10% (2/20)
  Continuation           10% (2/20)
  Trending Stair-step     5% (1/20)
  Liquidity Spring        5% (1/20)

Universal markers (100% hit rate on 20 cases):
  - ST flip 2H UP (CORE trigger)
  - Volume spike ≥2x in 6h window of flip

Amplifiers (modifier scoring):
  +25 base_days ≥ 10        (Premium compression)
  +15 base_days ≥ 5         (Standard base)
  +20 prior_downtrend ≥ 30% (short squeeze fuel)
  +10 prior_downtrend ≥ 15%
  +20 capitulation_wick     (top-tier amplifier)
  +15 vol_ratio ≥ 10x       (extreme)
  +10 vol_ratio ≥ 5x        (high)
  +10 rsi_cross > sma       (momentum confirmation)
  -10 had_cv_flip_24h       (anti-marker from precondition analysis)
  -10 had_st_mtf_24h        (anti-marker)

Tiers (post-amplifier classification):
  PREMIUM   (score ≥ 80) → expect +50-100% move, hold strategy
  STANDARD  (60-79)      → expect +20-40%, TP grid 25/50/100
  MARGINAL  (40-59)      → expect +15-25%, aggressive one-shot TP
"""
from __future__ import annotations
import logging
import statistics
from typing import Optional

logger = logging.getLogger(__name__)

# ── Scoring weights ─────────────────────────────────────────────
CORE_SCORE = 30  # base for ST_flip_2H + vol_spike_2x

AMPLIFIERS = {
    'base_10d':         25,
    'base_5d':          15,
    'prior_dt_30':      20,
    'prior_dt_15':      10,
    'capitulation':     20,
    'vol_ratio_10x':    15,
    'vol_ratio_5x':     10,
    'rsi_cross':        10,
}

ANTI_MARKERS = {
    'had_cv_flip_24h':  -10,
    'had_st_mtf_24h':   -10,
}

TIER_THRESHOLDS = {
    'PREMIUM':   80,
    'STANDARD':  60,
    'MARGINAL':  40,
}


# ── Indicator helpers (operate on 2H candles list[dict{t,o,h,l,c,v}]) ──

def _rsi_14(closes: list[float]) -> list[float]:
    """Wilder's RSI(14). Returns list aligned to closes (first 14 = NaN/0)."""
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


# ── WHALE amplifier checks ──────────────────────────────────────

def check_vol_spike(candles_2h: list[dict], flip_idx: int,
                    window_bars: int = 3) -> tuple[bool, float]:
    """Vol spike ≥2x в окне [flip_idx-1 .. flip_idx+window_bars].
    SMA9 baseline = 30 баров ДО flip.
    Real-time-friendly: если window выходит за конец массива — берём
    то что доступно (минимум flip_idx сам).
    Returns (passed, max_ratio).
    """
    if flip_idx < 30 or flip_idx >= len(candles_2h):
        return (False, 0.0)
    baseline = [c['v'] for c in candles_2h[flip_idx - 30: flip_idx]]
    sma9 = statistics.mean(baseline) if baseline else 0
    if sma9 <= 0:
        return (False, 0.0)
    # Window: [flip_idx-1, flip_idx+window_bars] clamped к границам массива
    start = max(0, flip_idx - 1)
    end = min(len(candles_2h), flip_idx + window_bars + 1)
    max_ratio = 0.0
    for i in range(start, end):
        r = candles_2h[i]['v'] / sma9
        if r > max_ratio:
            max_ratio = r
    return (max_ratio >= 2.0, round(max_ratio, 2))


def check_base_duration(candles_2h: list[dict], flip_idx: int,
                        amplitude_pct: float = 8.0) -> int:
    """Count consecutive days where price stayed within `amplitude_pct` band
    looking backwards from flip_idx. Returns days (int).
    """
    if flip_idx < 30:
        return 0
    entry_price = candles_2h[flip_idx]['c']
    if entry_price <= 0:
        return 0
    # Look back up to 20 days (240 bars of 2h)
    max_lookback = min(flip_idx, 240)
    bars_in_range = 0
    band_lo = entry_price * (1 - amplitude_pct / 100)
    band_hi = entry_price * (1 + amplitude_pct / 100)
    for i in range(flip_idx - 1, flip_idx - max_lookback - 1, -1):
        c = candles_2h[i]
        if band_lo <= c['c'] <= band_hi:
            bars_in_range += 1
        else:
            break
    # 12 bars of 2h = 1 day
    return bars_in_range // 12


def check_prior_downtrend(candles_2h: list[dict], flip_idx: int,
                          lookback_days: int = 14) -> float:
    """Returns max drop % from high to flip_idx within lookback window."""
    if flip_idx < 30:
        return 0.0
    bars = min(lookback_days * 12, flip_idx)
    window = candles_2h[flip_idx - bars: flip_idx]
    if not window:
        return 0.0
    high = max(c['h'] for c in window)
    entry = candles_2h[flip_idx]['c']
    if high <= 0 or entry <= 0:
        return 0.0
    drop_pct = (high - entry) / high * 100
    return max(0.0, drop_pct)


def check_capitulation_wick(candles_2h: list[dict], flip_idx: int,
                            lookback_days: int = 10) -> bool:
    """Detects long lower wick candle in recent base (lookback_days).
    Criteria: lower wick > 2x abs(body) AND wick depth > 1.5x ATR.
    """
    if flip_idx < 30:
        return False
    bars = min(lookback_days * 12, flip_idx)
    # Compute crude ATR (mean true-range) over the window
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
    # Scan for capitulation candle
    for c in window:
        body = abs(c['c'] - c['o'])
        lower_wick = min(c['o'], c['c']) - c['l']
        if lower_wick <= 0:
            continue
        if body > 0 and lower_wick > 2 * body and lower_wick > 1.5 * atr:
            return True
        # Edge case: tiny body, huge wick
        if body == 0 and lower_wick > 2 * atr:
            return True
    return False


def check_rsi_cross(candles_2h: list[dict], flip_idx: int,
                    lookback_bars: int = 4) -> bool:
    """True if RSI(14) crossed above SMA(RSI,14) within lookback_bars
    before/at flip_idx.
    """
    if flip_idx < 30:
        return False
    closes = [c['c'] for c in candles_2h[: flip_idx + 1]]
    rsi = _rsi_14(closes)
    rsi_sma = _sma(rsi, 14)
    start = max(20, flip_idx - lookback_bars)
    for i in range(start + 1, flip_idx + 1):
        if rsi[i-1] <= rsi_sma[i-1] and rsi[i] > rsi_sma[i]:
            return True
    return False


# ── Anti-marker checks (against Mongo) ──────────────────────────

def check_anti_markers(db, pair: str, flip_ts: int,
                       direction: str = 'LONG') -> dict[str, bool]:
    """Returns flags for anti-marker signals in the 24h preceding flip_ts.
    flip_ts is Unix seconds.
    """
    from datetime import datetime, timezone, timedelta
    flags = {'had_cv_flip_24h': False, 'had_st_mtf_24h': False}
    try:
        cutoff = datetime.fromtimestamp(flip_ts - 24 * 3600, tz=timezone.utc)
        end = datetime.fromtimestamp(flip_ts, tz=timezone.utc)
        cv = db.cv_flip_signals.find_one({
            'pair': pair,
            'cv_triggered_at': {'$gte': cutoff, '$lt': end},
        })
        if cv and (cv.get('direction', '') or '').upper() == direction:
            flags['had_cv_flip_24h'] = True
        st = db.supertrend_signals.find_one({
            'pair': pair, 'tier': 'mtf',
            'flip_at': {'$gte': cutoff, '$lt': end},
        })
        if st and (st.get('direction', '') or '').upper() == direction:
            flags['had_st_mtf_24h'] = True
    except Exception:
        pass
    return flags


# ── WHALE scoring (main entry) ─────────────────────────────────

def compute_whale_score(candles_2h: list[dict], flip_idx: int,
                        anti_flags: Optional[dict] = None) -> dict:
    """Computes WHALE score for a 2H ST flip at flip_idx.

    Returns dict with:
      score, tier, breakdown, indicators
      passes_core (bool): True if ST_flip + vol_spike both present
    """
    if flip_idx < 30 or flip_idx >= len(candles_2h):
        return {'score': 0, 'tier': None, 'passes_core': False,
                'breakdown': {}, 'indicators': {}}

    breakdown: dict = {}
    indicators: dict = {}

    # CORE: vol spike check (ST flip already given by caller)
    vol_ok, vol_ratio = check_vol_spike(candles_2h, flip_idx)
    indicators['vol_ratio_max'] = vol_ratio
    passes_core = bool(vol_ok)
    # Считаем amplifiers ВСЕГДА (даже без CORE) — score покажет "потенциал".
    # passes_core flag отдельно указывает фактически ли setup готов.
    score = CORE_SCORE if passes_core else 0
    if passes_core:
        breakdown['core_st_flip_vol_spike'] = CORE_SCORE
    else:
        breakdown['vol_spike_failed'] = True

    # Base duration
    base_days = check_base_duration(candles_2h, flip_idx)
    indicators['base_days'] = base_days
    if base_days >= 10:
        score += AMPLIFIERS['base_10d']
        breakdown['base_10d'] = AMPLIFIERS['base_10d']
    elif base_days >= 5:
        score += AMPLIFIERS['base_5d']
        breakdown['base_5d'] = AMPLIFIERS['base_5d']

    # Prior downtrend
    prior_dt = check_prior_downtrend(candles_2h, flip_idx)
    indicators['prior_downtrend_pct'] = round(prior_dt, 1)
    if prior_dt >= 30:
        score += AMPLIFIERS['prior_dt_30']
        breakdown['prior_dt_30'] = AMPLIFIERS['prior_dt_30']
    elif prior_dt >= 15:
        score += AMPLIFIERS['prior_dt_15']
        breakdown['prior_dt_15'] = AMPLIFIERS['prior_dt_15']

    # Capitulation wick
    cap = check_capitulation_wick(candles_2h, flip_idx)
    indicators['capitulation_wick'] = cap
    if cap:
        score += AMPLIFIERS['capitulation']
        breakdown['capitulation'] = AMPLIFIERS['capitulation']

    # Volume amplifier (extreme)
    if vol_ratio >= 10:
        score += AMPLIFIERS['vol_ratio_10x']
        breakdown['vol_ratio_10x'] = AMPLIFIERS['vol_ratio_10x']
    elif vol_ratio >= 5:
        score += AMPLIFIERS['vol_ratio_5x']
        breakdown['vol_ratio_5x'] = AMPLIFIERS['vol_ratio_5x']

    # RSI cross
    rsi_x = check_rsi_cross(candles_2h, flip_idx)
    indicators['rsi_cross'] = rsi_x
    if rsi_x:
        score += AMPLIFIERS['rsi_cross']
        breakdown['rsi_cross'] = AMPLIFIERS['rsi_cross']

    # Anti-markers
    if anti_flags:
        for k, v in anti_flags.items():
            if v and k in ANTI_MARKERS:
                score += ANTI_MARKERS[k]
                breakdown[k] = ANTI_MARKERS[k]

    # Tier classification — требует passes_core (vol spike fired).
    # Без CORE score показывает "потенциал", но tier остаётся None →
    # signal не fires в real-time (maybe_fire_whale skip'ает).
    tier = None
    if passes_core:
        if score >= TIER_THRESHOLDS['PREMIUM']:
            tier = 'PREMIUM'
        elif score >= TIER_THRESHOLDS['STANDARD']:
            tier = 'STANDARD'
        elif score >= TIER_THRESHOLDS['MARGINAL']:
            tier = 'MARGINAL'

    return {
        'score': score,
        'tier': tier,
        'passes_core': passes_core,
        'breakdown': breakdown,
        'indicators': indicators,
    }


# ── Real-time WHALE trigger (called from watcher.py) ────────────

WHALE_COOLDOWN_S_LIVE = 12 * 3600  # 12h per pair


def scan_recent_flips_for_whale(pairs: list[str] | None = None,
                                  lookback_hours: int = 6) -> dict:
    """Safety net: периодически сканирует топ пары на ПРОПУЩЕННЫЕ WHALE
    сигналы (real-time hook может пропустить если watcher был down /
    реcтартилcя / Binance API завис).

    Для каждой пары:
      1. Fetch 2H klines (~350 баров через get_klines_any)
      2. Найти последний flip UP within `lookback_hours`
      3. Если найден — посчитать WHALE score
      4. Insert если STANDARD+/PREMIUM и нет cooldown

    Returns dict с stats {scanned, flips_found, fired, by_tier, errors}.
    """
    from database import _get_db, utcnow
    from datetime import datetime, timezone, timedelta
    from exchange import get_klines_any

    db = _get_db()
    # Default: top-volume USDT пары из supertrend signals last 7d
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
             'fired_docs': []}  # full docs for TG dispatch by caller
    now_ts = int(datetime.now(timezone.utc).timestamp())
    cutoff_ms = (now_ts - lookback_hours * 3600) * 1000
    cooldown_dt = datetime.now(timezone.utc) - timedelta(seconds=WHALE_COOLDOWN_S_LIVE)

    for pair in pairs:
        stats['scanned'] += 1
        try:
            candles = get_klines_any(pair, '2h', 350)
            if not candles or len(candles) < 100:
                continue

            # Compute SuperTrend trend array to find recent UP flips
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

            # Найти последний DOWN→UP flip в окне lookback
            flip_idx = None
            for i in range(n - 1, 0, -1):
                if candles[i]['t'] < cutoff_ms:
                    break
                if trend[i] == 1 and trend[i-1] == -1:
                    flip_idx = i
                    break
            if flip_idx is None:
                continue
            stats['flips_found'] += 1

            # Cooldown check
            recent = db.new_strategy_signals.find_one({
                'pair': pair, 'strategy': 'whale',
                'created_at': {'$gte': cooldown_dt},
            })
            if recent:
                continue

            # Score
            anti_flags = check_anti_markers(db, pair, int(candles[flip_idx]['t'] / 1000),
                                              'LONG')
            score_res = compute_whale_score(candles, flip_idx, anti_flags)
            if not score_res['passes_core']:
                continue
            tier = score_res.get('tier')
            if not tier or tier == 'MARGINAL':
                continue

            # Fire!
            entry = candles[flip_idx]['c']
            flip_dt = datetime.fromtimestamp(candles[flip_idx]['t'] / 1000,
                                              tz=timezone.utc)
            doc = {
                'pair': pair,
                'symbol': pair.replace('/', '').upper(),
                'direction': 'LONG',
                'entry': entry,
                'strategy': 'whale',
                'whale_score': score_res['score'],
                'whale_tier': tier,
                'whale_breakdown': score_res['breakdown'],
                'whale_indicators': score_res['indicators'],
                'created_at': flip_dt,
                'state': 'SCANNED',  # SCANNED = safety-net pickup
                'tp_R': 2.0,
            }
            try:
                # Avoid duplicate insert (same flip_dt)
                if db.new_strategy_signals.find_one({
                    'pair': pair, 'strategy': 'whale',
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
                # Полный doc для TG dispatch (caller хочет передать в _whale_send_telegram)
                stats['fired_docs'].append(dict(doc))
                logger.info(f'[whale-scan] 🐋 SCANNED {pair} {tier} '
                             f'score={score_res["score"]} '
                             f'flip_at={flip_dt.strftime("%H:%M")}')
            except Exception as e:
                logger.warning(f'[whale-scan] insert {pair}: {e}')
                stats['errors'] += 1
        except Exception:
            stats['errors'] += 1
            logger.debug(f'[whale-scan] {pair} error', exc_info=True)

    logger.info(f'[whale-scan] DONE scanned={stats["scanned"]} '
                 f'flips={stats["flips_found"]} fired={stats["fired"]} '
                 f'by_tier={stats["by_tier"]}')
    return stats


def maybe_fire_whale(signal_data: dict) -> dict | None:
    """Triggered when supertrend signal arrives in watcher.py.
    Real-time: считаем WHALE score на ПОСЛЕДНЕМ ЗАКРЫТОМ 2H баре (свече флипа).
    Fires только LONG, только STANDARD+PREMIUM tier.
    """
    pair = signal_data.get('pair', '?')
    try:
        source = signal_data.get('source', '')
        if source != 'supertrend':
            return None  # only ST events trigger WHALE
        tier = signal_data.get('st_tier', '')
        if tier not in ('vip', 'mtf'):
            logger.debug(f'[whale-live] {pair} skip — tier={tier}')
            return None
        direction = (signal_data.get('direction', '') or '').upper()
        if direction != 'LONG':
            logger.debug(f'[whale-live] {pair} skip — direction={direction}')
            return None

        if not pair or pair == '?':
            return None

        # Per-pair cooldown via Mongo
        from database import _get_db, utcnow
        from datetime import datetime, timezone, timedelta
        db = _get_db()
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=WHALE_COOLDOWN_S_LIVE)
        recent = db.new_strategy_signals.find_one({
            'pair': pair, 'strategy': 'whale',
            'created_at': {'$gte': cutoff},
        })
        if recent:
            logger.info(f'[whale-live] {pair} cooldown — last whale {recent.get("created_at")}')
            return None

        # Fetch 2H klines — нужно ≥ 100 баров для индикаторов
        from exchange import get_klines_any
        candles = get_klines_any(pair, '2h', 350)
        if not candles or len(candles) < 100:
            logger.warning(f'[whale-live] {pair} no/few 2H klines: {len(candles) if candles else 0}')
            return None

        # Используем ПРЕДпоследний бар как flip-reference (последний может быть
        # ещё формирующимся — его volume неполный).
        # candles[-1] = текущий формирующийся, candles[-2] = последний закрытый.
        flip_idx = len(candles) - 2
        flip_bar_ts = candles[flip_idx]['t']

        # Anti-markers из Mongo
        anti_flags = check_anti_markers(db, pair, int(flip_bar_ts / 1000), 'LONG')

        # Compute score
        score_res = compute_whale_score(candles, flip_idx, anti_flags)
        ind = score_res.get('indicators', {})
        if not score_res['passes_core']:
            logger.info(f'[whale-live] {pair} no_core — vol_ratio={ind.get("vol_ratio_max", 0)} (need ≥2.0)')
            return None
        tier = score_res.get('tier')
        if not tier:
            logger.info(f'[whale-live] {pair} no_tier — score={score_res["score"]} (need ≥40)')
            return None
        if tier == 'MARGINAL':
            logger.info(f'[whale-live] {pair} MARGINAL skip — score={score_res["score"]}')
            return None

        entry = signal_data.get('entry') or candles[flip_idx]['c']
        try: entry = float(entry)
        except Exception:
            logger.warning(f'[whale-live] {pair} bad entry: {entry}')
            return None

        doc = {
            'pair': pair,
            'symbol': pair.replace('/', '').upper(),
            'direction': 'LONG',
            'entry': entry,
            'strategy': 'whale',
            'whale_score': score_res['score'],
            'whale_tier': tier,
            'whale_breakdown': score_res['breakdown'],
            'whale_indicators': ind,
            'created_at': utcnow(),
            'state': 'NEW',
            'tp_R': 2.0,
        }
        try:
            db.new_strategy_signals.insert_one(doc)
            logger.info(f'[whale-live] 🐋 FIRED {pair} {tier} '
                         f'score={score_res["score"]} '
                         f'vol={ind.get("vol_ratio_max")}× '
                         f'base={ind.get("base_days")}d '
                         f'amp={list(score_res["breakdown"].keys())}')
        except Exception as e:
            logger.warning(f'[whale-live] {pair} insert fail: {e}')
        return doc
    except Exception:
        logger.exception(f'[whale-live] {pair} maybe_fire fail')
        return None


