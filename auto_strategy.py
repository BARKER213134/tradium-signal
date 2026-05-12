"""ALPHA-CV v1.2 — Автотрейдинговая стратегия с hybrid exits.

ИЗМЕНЕНИЯ v1.1 (2026-05-09):
- Добавлены risk gates (concurrent positions, daily loss, drawdown)
- Добавлены hybrid exits: TP1=50% at signal.tp / 50% rides with trail
- Добавлена видимость отказов и принятий через risk_state.py
- Логирование причины каждого решения в auto_strategy_log

ОРИГИНАЛЬНАЯ DOCSTRING:

═══ BACKTEST METRICS (14d, 5500+ signals, walk-forward validated) ═══
            TRAIN(12d)  TEST(2d_OOS)
WR          66.7%       66.4%        ← out-of-sample stable
AvgR        +1.12R      +1.05R       ← out-of-sample stable
PF          4.35        4.13         ← Profit Factor strong
N trades    657         119          (60-100/day)
Max DD      28.5%                    (with 1% sizing; cap concurrent → ~10%)


═══ ENTRY RULES (all must pass) ═══

1. Source whitelist + tier filter:
   ├─ cryptovizor + tier ∈ {match, mixed}        → high edge (+1.6/+0.6R)
   ├─ second_flip + LONG + tier=match            → secondary (+0.4R)
   └─ triple_confluence + LONG + tier=mixed      → narrow edge (+0.5R)

2. NEVER trade hours: {1, 2, 10, 12, 13} UTC (WR <30%, AvgR -0.2..-0.5R)

3. NEVER trade weekdays: {Mon, Sat} (worst by far: -0.36R, -0.29R)

4. q_score >= 40 (sanity check)


═══ HARD AVOID (никогда не входим) ═══

- volume_surge ANY              (avg WR 10%, -0.63R) ❌
- volcano ANY                   (8% WR, -0.76R)  ❌
- triple_confluence SHORT ANY   (5% WR, -0.85R)  ❌
- vol_accum SHORT               (15% WR, -0.62R) ❌
- supertrend без other source   (28% WR, -0.30R) ❌


═══ POSITION SIZING (Druckenmiller-scaled) ═══

base_pct = 1% of capital
multiplier:
  cryptovizor + SHORT + match    → 3.0× (highest edge: +2.36R)
  cryptovizor + SHORT + mixed    → 2.5× (+2.54R)
  cryptovizor + LONG + match     → 2.0× (+1.06R)
  cryptovizor others (allowed)   → 1.5×
  second_flip LONG match         → 1.0×
  triple_confluence LONG mixed   → 0.7×


═══ RISK MANAGEMENT (PT Jones — capital preservation) ═══

- Max concurrent positions: 5
- Max total exposure: 12% of capital
- Daily loss limit: -3% → STOP day, resume next day
- Drawdown limit: -10% → PAUSE 24h, manual review
- Per-trade hard stop: at SL (no override)
- TP1 hit: take 50%, move SL to breakeven
- 6h after entry: trail SL by 0.5R if floating profit > 0.5R


═══ MONITORING ═══

Daily review:
- Total trades, WR, AvgR — compare to baseline
- Drift detection: if AvgR drops <0.5R for 2d → PAUSE
- Slippage tracking: actual fill vs signaled price
- Session bot — telegram alerts on cap breach / DD

Weekly review (after 7 days):
- Re-run backtest with fresh data
- Adjust thresholds if drift detected
- Compare strategy returns vs baseline


Reference: Top trader principles applied
- Druckenmiller: Concentrate when conviction high → 3x sizing on best CV
- PT Jones: Capital preservation → daily/DD limits
- Renaissance: Statistical edge → 5500+ samples backtest
- Livermore: Don't fight trend → tier=match preferred
- Mark Douglas: Probabilistic → every signal independent
- Larry Williams: Cut overtrading → max 5 concurrent
"""
from __future__ import annotations
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


# ─── Whitelist + filters ───────────────────────────────────────────
CV_TIER_ALLOWED = {'match', 'mixed'}
SF_REQUIRED = {'direction': 'LONG', 'tier': 'match'}
TC_REQUIRED = {'direction': 'LONG', 'tier': 'mixed'}

# Time filters DISABLED по запросу пользователя — торгуем 24/7.
# Backtest показывал: bad hours UTC {1,2,10,12,13} (WR<30%, AvgR<-0.20R),
# bad weekdays {Mon, Sat} (-0.36R, -0.29R) — но user хочет максимум сигналов.
# Source/tier filter остаётся — это главный edge стратегии.
BAD_HOURS = set()       # было {1, 2, 10, 12, 13}
BAD_WEEKDAYS = set()    # было {0, 5} = Mon, Sat

# Q-score threshold — DISABLED (был sanity-check, не из бектеста).
# Реальные q_score: cluster=25, vol_accum=12, supertrend=12 — низкие base scores
# без kl/market bonuses. Не отражают edge стратегии. Source/tier фильтр главный.
MIN_Q_SCORE = 0

# ─── Sizing multipliers ────────────────────────────────────────────
def _tier_of(t: dict) -> str:
    """Возвращает текущий tier. Если нет — 'mixed' default (для sizing)."""
    return t.get('align_tier') or t.get('_align') or t.get('tier') or 'mixed'

SIZING_RULES = [
    # (predicate_fn, multiplier, label)
    # v3.0 multi-regime sizing (приоритет). Mult × base 1% = % от баланса.
    # На $1000: CHOP=4% ($40), BEAR=3% ($30), BULL=1.5% ($15)
    (lambda s, t: t.get('_regime') == 'CHOP',             4.0, 'regime_CHOP'),
    (lambda s, t: t.get('_regime') == 'BEAR',             3.0, 'regime_BEAR'),
    (lambda s, t: t.get('_regime') == 'BULL',             1.5, 'regime_BULL'),
    # Legacy STRICT verdict-based (если regime отключён)
    (lambda s, t: t.get('_verdict') == 'ELITE',           3.0, 'verdict_ELITE'),
    (lambda s, t: t.get('_verdict') == 'STRONG',          2.0, 'verdict_STRONG'),
    # Source-based fallback (legacy v1.x rules)
    (lambda s, t: s == 'cryptovizor' and (t.get('direction') == 'SHORT')
                  and _tier_of(t) == 'match',             3.0, 'cv_short_match'),
    (lambda s, t: s == 'cryptovizor' and (t.get('direction') == 'SHORT')
                  and _tier_of(t) == 'mixed',             2.5, 'cv_short_mixed'),
    (lambda s, t: s == 'cryptovizor' and (t.get('direction') == 'LONG')
                  and _tier_of(t) == 'match',             2.0, 'cv_long_match'),
    (lambda s, t: s == 'cryptovizor',                     1.5, 'cv_other'),
    (lambda s, t: s == 'second_flip',                     1.0, 'sf'),
    (lambda s, t: s == 'triple_confluence',               0.7, 'tc'),
]

# ─── Risk limits ───────────────────────────────────────────────────
MAX_CONCURRENT_POSITIONS = 10  # лимит 10 сделок (было 5)
MAX_TOTAL_EXPOSURE_PCT = 20.0  # 10×base 1% × ~2× avg multiplier = до 20%
DAILY_LOSS_LIMIT_PCT = 3.0
DRAWDOWN_LIMIT_PCT = 10.0


# ─── Strategy metadata (показывается на UI вкладке) ────────────────
STRATEGY_NAME = "ALPHA-CV"
STRATEGY_VERSION = "v3.0"
STRATEGY_DESCRIPTION = (
    "Multi-regime: разные правила для BULL/BEAR/CHOP. "
    "BULL — LONG STRICT (мизер edge); CHOP — both dirs no-SKIP + be_at_1R (winner +0.99R!); "
    "BEAR — SHORT GOOD+ + be_at_1R. Fresh 7d backtest на текущем market: "
    "CHOP regime даёт +128R total за 7 дней (vs −20R на trail_1R_0.5R)."
)
STRATEGY_BACKTEST_METRICS = {
    "backtest_period_days": 7,
    "data_source": "fresh Mongo last 7d (current market regime)",
    "total_signals_tested": 551,
    "regime_split": {"BULL": 394, "CHOP": 157, "BEAR": 0},
    "best_per_regime": {
        "BULL": {"filter": "LONG_STRICT", "exit": "signal_tpsl",
                 "n": 33, "wr": 57.6, "avg_r": 0.086, "total_r": 2.9,
                 "note": "marginal edge — reduce sizing"},
        "CHOP": {"filter": "both_no_skip", "exit": "be_at_1R",
                 "n": 129, "wr": 50.4, "avg_r": 0.992, "total_r": 128.0,
                 "pf": 3.7,
                 "note": "JACKPOT — reversal signals love range market"},
        "BEAR": {"filter": "SHORT_GOOD+", "exit": "be_at_1R",
                 "note": "no BEAR data in 7d, fallback to CHOP rules with SHORT only"},
    },
    "exit_strategy": "be_at_1R (CHOP/BEAR) / signal_tpsl (BULL)",
}


# ─── Activation flag (Mongo-based, no env var needed) ──────────────
def enrich_signal(signal: dict) -> dict:
    """Дополняет сырой signal_data полями нужными для evaluate:
    - align_tier (из cluster_delta cache по pair + at_ts)
    - q_score (компонентами из quality_score)

    Returns: обогащённый dict.
    """
    enriched = dict(signal)
    pair = enriched.get('pair') or ''
    ats = enriched.get('at_ts') or 0
    if not (pair and ats):
        return enriched

    # ─── 1. Compute align_tier from cluster_delta cache ───
    try:
        from delta_calculator import (_candle_open_ms, RESONANCE_BARS,
                                       TF_MINUTES, _resonance_from_deltas)
        from database import _get_db
        cd = _get_db().cluster_delta
        ats_ms = int(ats) * 1000
        # Bulk fetch needed candles
        keys = []
        for tf in ('15m', '1h'):
            sig_open = _candle_open_ms(ats_ms, tf)
            for j in range(RESONANCE_BARS):
                bk = sig_open - j * TF_MINUTES[tf] * 60 * 1000
                keys.append({'pair': pair, 'tf': tf, 'open_ms': bk})
        cached = {}
        if keys:
            for doc in cd.find({'$or': keys},
                               {'pair':1,'tf':1,'open_ms':1,'delta_pct':1,'_id':0}):
                cached[(doc['pair'], doc['tf'], doc['open_ms'])] = doc.get('delta_pct')
        # Compute delta + resonance per TF
        for tf in ('15m', '1h'):
            sig_open = _candle_open_ms(ats_ms, tf)
            d = cached.get((pair, tf, sig_open))
            if d is not None:
                enriched[f'delta_{tf}'] = d
            deltas = []
            for j in range(RESONANCE_BARS - 1, -1, -1):
                bk = sig_open - j * TF_MINUTES[tf] * 60 * 1000
                d2 = cached.get((pair, tf, bk))
                if d2 is not None:
                    deltas.append(d2)
            if deltas:
                enriched[f'resonance_{tf}'] = _resonance_from_deltas(deltas)
        # Compute alignment tier
        d15 = enriched.get('delta_15m')
        d1h = enriched.get('delta_1h')
        r15 = enriched.get('resonance_15m')
        r1h = enriched.get('resonance_1h')
        if not all(v is None for v in (d15, d1h, r15, r1h)):
            direction = (enriched.get('direction') or '').upper()
            sgn = 1 if direction == 'LONG' else -1 if direction == 'SHORT' else 0
            if sgn != 0:
                aligned = against = total = 0
                for v in (d15, d1h, r15, r1h):
                    if v is None or v == 0:
                        continue
                    total += 1
                    if (v > 0) == (sgn > 0):
                        aligned += 1
                    else:
                        against += 1
                if total >= 2:
                    if aligned >= total - 1 and aligned >= 2:
                        enriched['align_tier'] = 'match'
                    elif against >= total - 1 and against >= 2:
                        enriched['align_tier'] = 'against'
                    else:
                        enriched['align_tier'] = 'mixed'
    except Exception as e:
        logger.debug(f'[auto-strategy] enrich align_tier fail: {e}')

    # ─── 1b. RSI / SMA(RSI) per TF из signal_rsi_cache ───
    try:
        from rsi_cache import bulk_get_rsi_for_items
        bulk_get_rsi_for_items([enriched])
    except Exception as e:
        logger.debug(f'[auto-strategy] enrich rsi fail: {e}')

    # ─── 1c. Trend per TF из signal_trend_cache ───
    try:
        from trend_cache import bulk_get_trend_for_items
        bulk_get_trend_for_items([enriched])
    except Exception as e:
        logger.debug(f'[auto-strategy] enrich trend fail: {e}')

    # ─── 1d. Anomaly z-score per TF (15m/1h/4h) ───
    try:
        from delta_calculator import _candle_open_ms
        from database import _get_db
        cd = _get_db().cluster_delta
        ats_ms = int(ats) * 1000
        BASELINE_BARS_Z = 30
        tf_min = {'15m': 15, '1h': 60, '4h': 240}
        for tf in ('15m', '1h', '4h'):
            sig_open = _candle_open_ms(ats_ms, tf)
            bucket_ms = tf_min[tf] * 60 * 1000
            range_start = sig_open - BASELINE_BARS_Z * bucket_ms
            try:
                docs = list(cd.find({
                    'pair': pair, 'tf': tf,
                    'open_ms': {'$gte': range_start, '$lte': sig_open},
                }, {'open_ms':1, 'delta_pct':1, '_id':0}).limit(BASELINE_BARS_Z + 5))
            except Exception:
                continue
            if len(docs) < 6:
                continue
            by_open = {d['open_ms']: float(d.get('delta_pct') or 0) for d in docs}
            sorted_opens = sorted(by_open.keys())
            preceding = [by_open[o] for o in sorted_opens if o < sig_open]
            if not preceding or len(preceding) < 5:
                continue
            sig_delta = by_open.get(sig_open)
            if sig_delta is None:
                # Fallback: latest <=sig_open as proxy
                latest = sorted_opens[-1] if sorted_opens else None
                if latest is not None:
                    sig_delta = by_open[latest]
            if sig_delta is None:
                continue
            mean = sum(preceding) / len(preceding)
            var = sum((d - mean)**2 for d in preceding) / len(preceding)
            std = var ** 0.5
            if std < 0.1:
                continue
            enriched[f'delta_zscore_{tf}'] = round((sig_delta - mean) / std, 2)
    except Exception as e:
        logger.debug(f'[auto-strategy] enrich anomaly fail: {e}')

    # ─── 2. Q-score (если ещё не задан) ───
    if enriched.get('q_score') is None:
        try:
            from quality_score import compute_signal_score
            enriched['q_score'] = compute_signal_score(enriched, ctx={'tier': enriched.get('st_tier')})
        except Exception:
            pass

    return enriched


def is_enabled() -> bool:
    """Check если ALPHA-CV strategy active. Сначала Mongo flag, потом env var."""
    import os
    # Env var override (быстрый kill switch)
    env = os.getenv("AUTO_STRATEGY_ALPHA_CV", "")
    if env == "1":
        return True
    if env == "0":
        return False
    # Mongo flag
    try:
        from database import _get_db
        doc = _get_db().system.find_one({"_id": "auto_strategy_alpha_cv"})
        return bool(doc and doc.get("enabled", False))
    except Exception:
        return False


def set_enabled(enabled: bool, note: str = "") -> dict:
    """Включить/выключить через Mongo flag."""
    from datetime import datetime, timezone
    try:
        from database import _get_db
        col = _get_db().system
        col.update_one(
            {"_id": "auto_strategy_alpha_cv"},
            {"$set": {
                "enabled": bool(enabled),
                "updated_at": datetime.now(timezone.utc),
                "note": note,
            }},
            upsert=True,
        )
        return {"ok": True, "enabled": bool(enabled), "note": note}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ─── v3.0 Multi-Regime config ──────────────────────────────────────
# Fresh backtest 7d: разные filter/exit per market regime.
#   BULL — мало edge, LONG_STRICT only, size×0.5
#   CHOP — best edge +0.99R AvgR, both dirs no-SKIP, size×2.0
#   BEAR — no historical data, use CHOP rules с SHORT only
MULTI_REGIME_ENABLED = True

REGIME_CONFIG = {
    'BULL': {
        'allowed_directions': ('LONG',),
        'allowed_verdicts': ('STRONG', 'ELITE'),    # strict only
        'exit_label': 'signal_tpsl',                # legacy TP/SL
        'size_mult': 1.5,                           # marginal edge — small size
        'use_be_at_1R': False,
        'notes': 'BULL: LONG_STRICT (мизер edge +0.086R)',
    },
    'CHOP': {
        'allowed_directions': ('LONG', 'SHORT'),
        'allowed_verdicts': ('GOOD', 'STRONG', 'ELITE'),  # широко — каждый no-SKIP работает
        'exit_label': 'be_at_1R',
        'size_mult': 4.0,                            # БОЛЬШЕ — лучший edge (+0.99R)
        'use_be_at_1R': True,                        # move SL to BE после +1R
        'notes': 'CHOP: both dirs no-SKIP, be_at_1R, AvgR +0.99R, size 4×',
    },
    'BEAR': {
        'allowed_directions': ('SHORT',),
        'allowed_verdicts': ('GOOD', 'STRONG', 'ELITE'),
        'exit_label': 'be_at_1R',
        'size_mult': 3.0,                           # mirror CHOP с небольшим dampening
        'use_be_at_1R': True,
        'notes': 'BEAR: SHORT GOOD+, be_at_1R, size 3×',
    },
}


# Regime cache (5 min TTL — BTC trend меняется не быстро)
_regime_cache = {'regime': None, 'ts': 0}


def _get_btc_ema_cached():
    """Cache BTC 4h/1d EMAs для regime detection."""
    import time as _t
    now = _t.time()
    if _regime_cache.get('emas') and now - _regime_cache.get('ts', 0) < 300:
        return _regime_cache.get('emas')
    try:
        from exchange import get_klines_any
        kl_4h = get_klines_any('BTC/USDT', '4h', 60) or []
        kl_1d = get_klines_any('BTC/USDT', '1d', 60) or []
        if len(kl_4h) < 50 or len(kl_1d) < 50:
            return None
        def _ema(closes, p):
            out = [None]*len(closes)
            out[p-1] = sum(closes[:p])/p
            k = 2/(p+1)
            for i in range(p, len(closes)):
                out[i] = closes[i]*k + out[i-1]*(1-k)
            return out
        c4 = [float(c.get('c', 0)) for c in kl_4h]
        c1d = [float(c.get('c', 0)) for c in kl_1d]
        emas = {
            'c_4h': c4[-1],
            'e20_4h': _ema(c4, 20)[-1],
            'e50_4h': _ema(c4, 50)[-1],
            'e20_1d': _ema(c1d, 20)[-1],
            'e50_1d': _ema(c1d, 50)[-1],
        }
        _regime_cache['emas'] = emas
        _regime_cache['ts'] = now
        return emas
    except Exception as e:
        logger.debug(f'[regime] btc emas fail: {e}')
        return None


def detect_regime() -> str:
    """Returns BULL / BEAR / CHOP based on BTC 4h+1d EMAs.

    BULL: 4h EMA20>EMA50 AND 1d EMA20>EMA50 AND price>EMA20_4h
    BEAR: mirror
    CHOP: mixed / conflicting
    """
    emas = _get_btc_ema_cached()
    if not emas: return 'CHOP'
    bull_4h = emas['e20_4h'] > emas['e50_4h']
    bull_1d = emas['e20_1d'] > emas['e50_1d']
    above_4h = emas['c_4h'] > emas['e20_4h']
    below_4h = emas['c_4h'] < emas['e20_4h']
    if bull_4h and bull_1d and above_4h: return 'BULL'
    if (not bull_4h) and (not bull_1d) and below_4h: return 'BEAR'
    return 'CHOP'


# ─── STRICT-MODE: Verdict-based filter + Auto-pause ────────────────
# Бэктест 5530 сигналов 14d × 32 exits показал:
#   STRICT (ELITE+STRONG) + AUTO-PAUSE (win=10, avgR<0, pause=24h) =
#   N=29, WR 86.2%, AvgR +1.33R, PF 10.6, MaxDD 3R (vs 11.8R baseline)
#
# Trade quality > volume. Auto-pause = safety net на bear market.

STRICT_MODE_ENABLED = True   # включить verdict filter (ELITE/STRONG only)
ALLOWED_VERDICTS = ('ELITE', 'STRONG')

# Auto-pause params (бэктест-optimized win=10, avgR<0, pause=24h)
AUTO_PAUSE_WINDOW = 10              # rolling N trades
AUTO_PAUSE_AVGR_THRESHOLD = 0.0     # если AvgR последних 10 < 0 → pause
AUTO_PAUSE_HOURS = 24               # длительность паузы
AUTO_PAUSE_ELITE_BYPASS = True      # ELITE сигналы игнорируют паузу


def _rsi_tier_for(signal: dict) -> str:
    """Python-version of JS _rsiEdgeInfo. Returns 'top'/'good'/'medium'/'warn'/'avoid'."""
    src = (signal.get('source') or '').lower()
    direction = (signal.get('direction') or '').upper()
    rsi_1h = signal.get('rsi_1h')
    rsi_4h = signal.get('rsi_4h')
    if src in ('volume_surge', 'volcano'): return 'avoid'
    if src == 'triple_confluence' and direction == 'SHORT': return 'avoid'
    if src == 'vol_accum' and direction == 'SHORT': return 'avoid'
    if rsi_1h is None and rsi_4h is None: return 'medium'
    if src == 'cryptovizor' and direction == 'SHORT':
        if rsi_1h is not None and 30 <= rsi_1h < 50: return 'top'
        if rsi_4h is not None and 30 <= rsi_4h < 50: return 'top'
        if rsi_4h is not None and rsi_4h > 70: return 'warn'
        return 'good'
    if src == 'cryptovizor' and direction == 'LONG':
        if rsi_4h is not None and 60 <= rsi_4h < 70: return 'top'
        if rsi_4h is not None and 50 <= rsi_4h < 70: return 'good'
        if rsi_4h is not None and rsi_4h < 40: return 'warn'
    if src == 'second_flip' and direction == 'LONG' and rsi_4h is not None and 60 <= rsi_4h < 70:
        return 'good'
    if src == 'triple_confluence' and direction == 'LONG' and rsi_4h is not None and 60 <= rsi_4h < 80:
        return 'good'
    if direction == 'LONG' and rsi_4h is not None and rsi_4h < 40: return 'warn'
    if direction == 'SHORT' and rsi_4h is not None and rsi_4h > 70: return 'warn'
    return 'medium'


def _sma_tier_for(signal: dict) -> str:
    """SMA(RSI) position tier."""
    src = (signal.get('source') or '').lower()
    direction = (signal.get('direction') or '').upper()
    def pos(tf):
        r = signal.get(f'rsi_{tf}')
        m = signal.get(f'sma_rsi_{tf}')
        if r is None or m is None: return None
        return 'A' if r > m else 'B'
    p1 = pos('1h'); p4 = pos('4h'); pd = pos('1d')
    if src == 'cryptovizor' and direction == 'SHORT':
        if p1 == 'A' and p4 == 'A': return 'top'
        if p1 == 'A': return 'top'
        if p4 == 'A': return 'top'
        if pd == 'B': return 'good'
    if direction == 'LONG' and p4 == 'A' and pd == 'A': return 'top'
    if direction == 'SHORT' and p4 == 'B' and pd == 'B': return 'top'
    if direction == 'LONG' and pd == 'B': return 'avoid'
    if direction == 'LONG' and pd == 'A': return 'good'
    if direction == 'SHORT' and pd == 'B': return 'good'
    if direction == 'SHORT' and pd == 'A': return 'warn'
    return 'medium'


def _trend_tier_for(signal: dict) -> str:
    """EMA20/EMA50 trend tier."""
    src = (signal.get('source') or '').lower()
    direction = (signal.get('direction') or '').upper()
    def t(tf):
        v = signal.get(f'trend_{tf}')
        if not v: return None
        return v[0] if isinstance(v, str) else None  # 'U'/'D'/'F'
    t1 = t('1h'); t4 = t('4h'); td = t('1d')
    if src == 'cryptovizor' and direction == 'SHORT':
        if t1 == 'D' and t4 == 'D' and td == 'D': return 'top'
        if t1 == 'D' and t4 == 'D': return 'top'
        if t4 == 'D': return 'top'
    if direction == 'SHORT' and t1 == 'D' and t4 == 'D': return 'top'
    if direction == 'SHORT' and t1 == 'D': return 'good'
    if direction == 'SHORT' and t4 == 'D': return 'good'
    if direction == 'LONG' and t1 == 'U' and t4 == 'U' and td == 'U': return 'good'
    if direction == 'LONG' and t4 == 'D' and t1 == 'D': return 'avoid'
    if direction == 'LONG' and t4 == 'D': return 'warn'
    if direction == 'SHORT' and t4 == 'U': return 'warn'
    return 'medium'


def _anomaly_tier_for(signal: dict) -> str:
    """Delta z-score anomaly tier."""
    src = (signal.get('source') or '').lower()
    direction = (signal.get('direction') or '').upper()
    z15 = signal.get('delta_zscore_15m')
    z1 = signal.get('delta_zscore_1h')
    z4 = signal.get('delta_zscore_4h')
    if z15 is None and z1 is None and z4 is None: return 'medium'
    if src == 'cryptovizor' and direction == 'LONG':
        if z1 is not None and z1 > 1.5: return 'top'
        if z15 is not None and z15 > 1.5: return 'top'
    if src == 'cryptovizor' and direction == 'SHORT' and z4 is not None and z4 > 1.5:
        return 'top'
    if direction == 'LONG' and z1 is not None and z1 > 1.5: return 'good'
    if direction == 'SHORT' and z15 is not None and z15 < -1.5: return 'good'
    if direction == 'SHORT' and z4 is not None and z4 > 1.5: return 'good'
    if direction == 'LONG' and z1 is not None and z1 < -1.5: return 'warn'
    if direction == 'LONG' and z4 is not None and z4 < -1.5: return 'warn'
    return 'medium'


def compute_verdict(signal: dict) -> dict:
    """Агрегат всех 4 edge tiers в одно решение (ELITE/STRONG/GOOD/OK/MEH/CAUTION/SKIP).

    Эквивалент _verdictInfo в signals.html. Используется для STRICT mode entry filter.
    """
    rsi_t = _rsi_tier_for(signal)
    sma_t = _sma_tier_for(signal)
    trend_t = _trend_tier_for(signal)
    anom_t = _anomaly_tier_for(signal)
    tiers = (rsi_t, sma_t, trend_t, anom_t)
    has_avoid = 'avoid' in tiers
    has_warn = 'warn' in tiers
    top_count = sum(1 for t in tiers if t == 'top')
    good_count = sum(1 for t in tiers if t == 'good')
    if has_avoid:
        verdict = 'SKIP'
    elif top_count >= 3:
        verdict = 'ELITE'
    elif top_count >= 2:
        verdict = 'STRONG'
    elif top_count >= 1:
        verdict = 'GOOD'
    elif has_warn:
        verdict = 'CAUTION'
    elif good_count >= 2:
        verdict = 'OK'
    else:
        verdict = 'MEH'
    return {
        'verdict': verdict,
        'rsi_tier': rsi_t, 'sma_tier': sma_t,
        'trend_tier': trend_t, 'anomaly_tier': anom_t,
        'top_count': top_count,
    }


# ─── Auto-pause state ──────────────────────────────────────────────
def get_pause_state() -> dict:
    """Read pause state from Mongo. Returns dict with paused_until_ts."""
    try:
        from database import _get_db
        doc = _get_db().system.find_one({'_id': 'auto_strategy_pause'})
        return dict(doc) if doc else {}
    except Exception:
        return {}


def set_pause_until(until_ts: int, reason: str = '') -> None:
    try:
        from database import _get_db
        _get_db().system.update_one(
            {'_id': 'auto_strategy_pause'},
            {'$set': {
                'paused_until_ts': int(until_ts),
                'paused_at': datetime.now(timezone.utc),
                'reason': reason,
            }},
            upsert=True,
        )
    except Exception as e:
        logger.warning(f'[auto-strategy] set_pause_until fail: {e}')


def is_paused_now() -> tuple[bool, dict]:
    """Returns (is_paused, state_dict)."""
    import time as _t
    st = get_pause_state()
    pu = st.get('paused_until_ts') or 0
    now_s = int(_t.time())
    return (now_s < pu, st)


def check_and_update_auto_pause() -> None:
    """Считает rolling AvgR последних AUTO_PAUSE_WINDOW closed ALPHA-CV сделок.
    Если AvgR < AUTO_PAUSE_AVGR_THRESHOLD → выставляет pause на AUTO_PAUSE_HOURS.

    ВАЖНО: учитываем только сделки с strict_mode_open=True (открытые на v2.1+
    после внедрения verdict gate). Сделки на v1.2/v2.0 baseline = другая
    стратегия, их статистика не репрезентативна для v2.1.

    Вызывается после каждого закрытия сделки (либо периодически).
    """
    try:
        from database import _get_db
        from datetime import timedelta
        db = _get_db()
        # ТОЛЬКО сделки с strict_mode_open=True (v2.1+)
        cursor = db.paper_trades.find({
            'auto_strategy_label': {'$exists': True, '$nin': [None, '']},
            'strict_mode_open': True,  # ← key filter
            'status': {'$in': ['TP', 'SL', 'CLOSED', 'TIMEOUT']},
            'pnl_pct': {'$exists': True},
        }, {'pnl_pct': 1, 'size_pct': 1, 'sl_pct': 1, 'closed_at': 1, 'pair': 1}).sort('closed_at', -1).limit(AUTO_PAUSE_WINDOW)
        trades = list(cursor)
        if len(trades) < AUTO_PAUSE_WINDOW:
            return  # ещё мало данных
        # Convert pnl_pct → R-multiple примерно: pnl_pct / (sl_pct * leverage)
        # Точнее: R = (exit - entry) / sl_dist. Здесь приближаем через pnl_pct.
        # Если SL hit, R = -1. Если TP, R ≈ pnl_pct/sl_pct.
        # Используем pnl_pct как proxy (нормализованный к leverage уже).
        rs = []
        for t in trades:
            pp = float(t.get('pnl_pct') or 0)
            # Грубое R: pnl_pct >= 0 → R ≈ pp/abs(sl_pct в %), но без leverage info
            # просто берём pp/некий нормализатор. Default: 1R ≈ 5% PnL @ 5x leverage 1% SL
            r = pp / 5.0  # 5% PnL ≈ 1R
            rs.append(r)
        avg_r = sum(rs) / len(rs)
        wins = sum(1 for r in rs if r > 0)
        wr = wins / len(rs) * 100
        if avg_r < AUTO_PAUSE_AVGR_THRESHOLD:
            import time as _t
            until_ts = int(_t.time()) + AUTO_PAUSE_HOURS * 3600
            reason = (f'rolling AvgR({AUTO_PAUSE_WINDOW})={avg_r:+.2f}R '
                      f'WR={wr:.0f}% < threshold {AUTO_PAUSE_AVGR_THRESHOLD}R')
            set_pause_until(until_ts, reason)
            logger.warning(
                f'[auto-strategy] AUTO-PAUSE triggered: {reason} → '
                f'pause until {datetime.fromtimestamp(until_ts, tz=timezone.utc)}'
            )
    except Exception as e:
        logger.debug(f'[auto-strategy] check_and_update_auto_pause fail: {e}')


# ─── Public API ────────────────────────────────────────────────────
def should_enter(signal: dict) -> tuple[bool, str]:
    """Главное решение — входить или нет.
    signal: dict с полями source, direction, align_tier, hour_utc, weekday,
            q_score, и др.
    Returns: (accept: bool, reason: str)
    """
    src = signal.get('source')
    tier = signal.get('align_tier') or signal.get('_align')
    direction = (signal.get('direction') or '').upper()

    # ── Time filters ──
    h = signal.get('hour_utc')
    wd = signal.get('weekday')
    if h is None or wd is None:
        # Compute from at_ts
        ats = signal.get('at_ts')
        if ats:
            try:
                dt = datetime.fromtimestamp(int(ats), tz=timezone.utc)
                h = dt.hour
                wd = dt.weekday()
            except Exception:
                pass
    if h is not None and h in BAD_HOURS:
        return False, f'bad_hour={h}'
    if wd is not None and wd in BAD_WEEKDAYS:
        return False, f'bad_weekday={wd}'

    # ── Q-score sanity (DISABLED) ──
    if MIN_Q_SCORE > 0:
        qs = signal.get('q_score')
        if qs is not None and qs < MIN_Q_SCORE:
            return False, f'q_score={qs}<{MIN_Q_SCORE}'

    # ── v3.0 MULTI-REGIME gate ──
    # Регим определяется по BTC 4h+1d EMAs (cache 5min).
    # Per-regime rules from REGIME_CONFIG (backtest 7d data).
    if MULTI_REGIME_ENABLED:
        regime = detect_regime()
        signal['_regime'] = regime
        cfg = REGIME_CONFIG.get(regime, REGIME_CONFIG['CHOP'])
        # Direction filter
        if direction not in cfg['allowed_directions']:
            return False, f'regime_{regime}_skip_direction={direction}'
        # Verdict filter per regime
        v_info = compute_verdict(signal)
        verdict = v_info['verdict']
        signal['_verdict'] = verdict
        if verdict not in cfg['allowed_verdicts']:
            return False, f'regime_{regime}_skip_verdict={verdict}'
        # Auto-pause gate
        paused, pause_state = is_paused_now()
        if paused:
            if not (AUTO_PAUSE_ELITE_BYPASS and verdict == 'ELITE'):
                pu = pause_state.get('paused_until_ts') or 0
                import time as _t
                remaining_h = (pu - int(_t.time())) / 3600
                return False, (f'auto_pause: {pause_state.get("reason","?")} '
                                f'(осталось {remaining_h:.1f}ч)')
        # Pass — return accept reason with regime
        return True, f'regime_{regime}_{verdict}_{direction}'

    # ── LEGACY STRICT-MODE verdict gate (v2.x, отключено в v3.0) ──
    if STRICT_MODE_ENABLED:
        v = compute_verdict(signal)
        verdict = v['verdict']
        if verdict not in ALLOWED_VERDICTS:
            return False, f'strict_skip_verdict={verdict}'
        # ── PRICE EXTENSION CHECK: не входим если цена уже сделала большое
        # движение в направлении сделки (mean-reversion bounce risk).
        # Например: SHORT на TAC @ 0.01736 после drop -13.4% от 0.01999 →
        # следующий 15m бар +5.9% bounce → instant SL.
        # Проверяем 1h klines: если range от high до low >= 5% и entry в
        # нижних 30% диапазона для SHORT (верхних 30% для LONG) — отказ.
        try:
            from exchange import get_klines_any
            pair = signal.get('pair') or ''
            entry = float(signal.get('entry') or 0)
            if pair and entry > 0:
                kl = get_klines_any(pair, '1h', 4)
                if kl and len(kl) >= 2:
                    closes = [float(k.get('c', 0)) for k in kl]
                    highs = [float(k.get('h', 0)) for k in kl]
                    lows = [float(k.get('l', 0)) for k in kl]
                    range_high = max(highs)
                    range_low = min(lows)
                    range_pct = (range_high - range_low) / range_low * 100 if range_low > 0 else 0
                    if range_pct >= 5.0:  # значимое движение
                        # Position entry в диапазоне (0=low, 1=high)
                        if range_high > range_low:
                            position = (entry - range_low) / (range_high - range_low)
                            # SHORT не открывать в нижних 30% (after drop)
                            if direction == 'SHORT' and position < 0.30:
                                return False, (f'extended_short: цена в нижних '
                                                f'{position*100:.0f}% диапазона '
                                                f'{range_pct:.1f}% за 4h')
                            # LONG не открывать в верхних 30% (after pump)
                            if direction == 'LONG' and position > 0.70:
                                return False, (f'extended_long: цена в верхних '
                                                f'{(1-position)*100:.0f}% диапазона '
                                                f'{range_pct:.1f}% за 4h')
        except Exception:
            pass  # filter best-effort, не блокирует на ошибке
        # ── Auto-pause gate: если в паузе и НЕ ELITE → отказ ──
        paused, pause_state = is_paused_now()
        if paused:
            if not (AUTO_PAUSE_ELITE_BYPASS and verdict == 'ELITE'):
                pu = pause_state.get('paused_until_ts') or 0
                import time as _t
                remaining_h = (pu - int(_t.time())) / 3600
                return False, (f'auto_pause: {pause_state.get("reason","?")} '
                                f'(осталось {remaining_h:.1f}ч)')
        # ── В STRICT mode: trust verdict, пропускаем source whitelist ──
        # Verdict ELITE/STRONG = 2+ top edges (RSI/SMA/Trend/Anomaly).
        # Backtest two_top/three_top: WR 83-99% на mixed sources. Источник
        # не имеет значения если verdict качественный.
        # Avoid sources (volume_surge/volcano/TC SHORT/vol_accum SHORT)
        # → verdict=SKIP → уже отклонены выше через ALLOWED_VERDICTS.
        return True, f'strict_{verdict}_{src or "?"}_{direction}'

    # ── Source-specific rules ──
    if src == 'cryptovizor':
        # Если tier не вычислился (нет cluster_delta для свежей свечи) —
        # default 'mixed' (60.8% WR, +0.75R AvgR — ещё edge). Безопаснее
        # чем skipping: backtest показал даже CV-against = +0.16R.
        if tier is None or tier == '':
            return True, 'cv_no_tier_default_mixed'
        if tier in CV_TIER_ALLOWED:
            return True, f'cv_{tier}_{direction}'
        return False, f'cv_tier={tier}_skipped'

    if src == 'second_flip':
        if direction != SF_REQUIRED['direction']:
            return False, f'sf_filter_failed({direction}/{tier})'
        # tier=None → default match (если cluster_delta cache не дал данных)
        if tier is None or tier == '':
            return True, 'sf_long_no_tier_default'
        if tier == SF_REQUIRED['tier']:
            return True, 'sf_long_match'
        return False, f'sf_filter_failed({direction}/{tier})'

    if src == 'triple_confluence':
        if direction != TC_REQUIRED['direction']:
            return False, f'tc_filter_failed({direction}/{tier})'
        # tier=None → default mixed
        if tier is None or tier == '':
            return True, 'tc_long_no_tier_default'
        if tier == TC_REQUIRED['tier']:
            return True, 'tc_long_mixed'
        return False, f'tc_filter_failed({direction}/{tier})'

    # All other sources: hard avoid
    return False, f'source_not_whitelisted={src}'


def position_size_pct(signal: dict, base_pct: float = 1.0) -> float:
    """Position size в % от капитала. Druckenmiller-scaled на основе edge."""
    src = signal.get('source')
    if not src:
        return base_pct
    for predicate, multiplier, label in SIZING_RULES:
        try:
            if predicate(src, signal):
                return base_pct * multiplier
        except Exception:
            continue
    return base_pct


def get_size_label(signal: dict) -> str:
    """Возвращает label sizing rule для logging."""
    src = signal.get('source')
    for predicate, multiplier, label in SIZING_RULES:
        try:
            if predicate(src, signal):
                return f"{label} ({multiplier}×)"
        except Exception:
            continue
    return "default (1×)"


# ─── Risk gate (capital-level checks) ──────────────────────────────
def get_capital_state() -> dict:
    """Считывает текущее состояние capital из paper_trader / Mongo.

    ВАЖНО: pnl_pct в paper_trades это % от ПОЗИЦИИ (с leverage), не баланса!
    Считаем daily_pnl_pct как (sum pnl_usdt) / balance * 100 — реальный % от баланса.
    """
    try:
        from database import _get_db
        from datetime import datetime, timezone, timedelta
        db = _get_db()
        # Account balance
        state = db.paper_trades.find_one({'_id': 'state'}) or {}
        balance = float(state.get('balance', 1000.0))
        if balance <= 0:
            balance = 1000.0
        # Open positions count (только наши с auto_strategy_label)
        open_count = db.paper_trades.count_documents({
            'status': 'OPEN',
            'auto_strategy_label': {'$exists': True, '$nin': [None, '']}
        })
        # Total exposure (sum of size_pct of open positions)
        cursor = db.paper_trades.find({
            'status': 'OPEN',
            'auto_strategy_label': {'$exists': True, '$nin': [None, '']}
        }, {'size_pct': 1})
        total_exp = sum(d.get('size_pct', 1.0) for d in cursor)
        # Daily PnL (closed today) — считаем USD, делим на balance
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0,
                                                          second=0, microsecond=0)
        cursor = db.paper_trades.find({
            'status': {'$in': ['CLOSED', 'TP', 'SL', 'TIMEOUT']},
            'closed_at': {'$gte': today_start},
            'auto_strategy_label': {'$exists': True, '$nin': [None, '']}
        }, {'pnl_usdt': 1})
        daily_pnl_usdt = sum(d.get('pnl_usdt', 0) or 0 for d in cursor)
        daily_pnl_pct = daily_pnl_usdt / balance * 100.0
        # Current DD: equity curve в USD, peak-to-trough в % от balance
        week_start = datetime.now(timezone.utc) - timedelta(days=7)
        cursor = db.paper_trades.find({
            'status': {'$in': ['CLOSED', 'TP', 'SL', 'TIMEOUT']},
            'closed_at': {'$gte': week_start},
            'auto_strategy_label': {'$exists': True, '$nin': [None, '']}
        }, {'pnl_usdt': 1, 'closed_at': 1})
        equity_usdt = [0.0]
        peak = 0.0
        for d in sorted(cursor, key=lambda x: x.get('closed_at', datetime.min)):
            equity_usdt.append(equity_usdt[-1] + (d.get('pnl_usdt', 0) or 0))
            peak = max(peak, equity_usdt[-1])
        dd_usdt = (peak - equity_usdt[-1]) if equity_usdt else 0
        dd_pct = dd_usdt / balance * 100.0
        return {
            'open_positions': open_count,
            'total_exposure_pct': round(total_exp, 2),
            'daily_pnl_pct': round(daily_pnl_pct, 2),
            'current_dd_pct': round(max(0, dd_pct), 2),
        }
    except Exception as e:
        logger.debug(f'[auto-strategy] capital_state fail: {e}')
        return {
            'open_positions': 0, 'total_exposure_pct': 0.0,
            'daily_pnl_pct': 0.0, 'current_dd_pct': 0.0,
        }


def can_enter_now(open_positions: int = 0,
                  total_exposure_pct: float = 0.0,
                  daily_pnl_pct: float = 0.0,
                  current_dd_pct: float = 0.0) -> tuple[bool, str]:
    """Проверяет capital-level limits перед открытием новой сделки.
    Reasons возвращаются user-friendly с цифрами."""
    if open_positions >= MAX_CONCURRENT_POSITIONS:
        return False, (f'risk_gate: открыто {open_positions} позиций '
                        f'(лимит {MAX_CONCURRENT_POSITIONS})')
    if total_exposure_pct >= MAX_TOTAL_EXPOSURE_PCT:
        return False, (f'risk_gate: exposure {total_exposure_pct:.1f}% '
                        f'(лимит {MAX_TOTAL_EXPOSURE_PCT}%)')
    if daily_pnl_pct <= -DAILY_LOSS_LIMIT_PCT:
        return False, (f'risk_gate: дневной лосс {daily_pnl_pct:.1f}% '
                        f'(лимит {-DAILY_LOSS_LIMIT_PCT}%)')
    if current_dd_pct >= DRAWDOWN_LIMIT_PCT:
        return False, (f'risk_gate: drawdown {current_dd_pct:.1f}% '
                        f'(лимит {DRAWDOWN_LIMIT_PCT}%) — pause 24h')
    return True, 'ok'


# ─── EXIT LOGIC: hybrid TP1 + trailing ─────────────────────────────
# Backtest 14d: signal.tp1 = best edge (+1.13R AvgR) vs forced 5% TP (+0.47R).
# Hybrid: 50% close at signal.tp1, 50% rides с trailing stop.
EXIT_PARTIAL_PCT = 50  # % to close at TP1
EXIT_TRAIL_DISTANCE_R = 0.5  # trailing distance in R (after TP1 hit)
EXIT_TIMEOUT_HOURS = 24


def compute_exit_state_v3(pair: str, direction: str, entry_price: float,
                           sl_price: float, opened_at_ms: int,
                           regime: str = 'CHOP') -> dict:
    """v3.0 exit dispatcher — выбирает exit logic per regime.

    BULL  → signal_tpsl (TP/SL hits)
    CHOP  → be_at_1R (move SL to BE после +1R, иначе hold)
    BEAR  → be_at_1R (same as CHOP)
    """
    cfg = REGIME_CONFIG.get(regime, REGIME_CONFIG['CHOP'])
    use_be = cfg.get('use_be_at_1R', True)
    if not use_be:
        # BULL → используем signal TP/SL (paper_trader сам обрабатывает TP/SL hit).
        # Этот dispatcher возвращает should_exit=False — ничего не делаем,
        # paper_trader проверит SL/TP по price.
        return {'should_exit': False, 'reason': f'{regime}_use_signal_tpsl',
                'trail_active': False}
    # BE_AT_1R: trail SL до BE после +1R favorable
    try:
        from exchange import get_klines_any, get_prices_any
        import time as _t
        is_long = (direction or '').upper() == 'LONG'
        sl_dist = abs(entry_price - sl_price)
        if sl_dist <= 0:
            return {'should_exit': False, 'reason': 'invalid_sl_dist'}
        now_ms = int(_t.time() * 1000)
        hours_open = max(1, int((now_ms - opened_at_ms) / 3_600_000) + 1)
        n_bars = min(72, max(2, hours_open + 1))
        candles = get_klines_any(pair, '1h', n_bars)
        if not candles or len(candles) < 1:
            return {'should_exit': False, 'reason': 'no_klines'}
        # Track max favorable
        max_fav = entry_price
        for c in candles:
            t = c.get('t') or c.get('open_ms')
            if t is None or t < opened_at_ms - 3_600_000: continue
            h = float(c.get('h', 0) or 0)
            l = float(c.get('l', 0) or 0)
            if is_long and h > max_fav: max_fav = h
            elif not is_long and l > 0 and (max_fav == entry_price or l < max_fav): max_fav = l
        # Current price
        sym = pair.replace('/', '').upper()
        if not sym.endswith('USDT'): sym += 'USDT'
        prices = get_prices_any([pair])
        current = prices.get(sym)
        if current is None:
            current = float(candles[-1].get('c', 0) or 0)
        if current <= 0:
            return {'should_exit': False, 'reason': 'no_price'}
        # Update max_fav with current
        if is_long and current > max_fav: max_fav = current
        elif not is_long and (current < max_fav or max_fav == entry_price): max_fav = current
        max_fav_r = (max_fav - entry_price)/sl_dist if is_long else (entry_price - max_fav)/sl_dist
        # SL check
        if is_long and current <= sl_price:
            return {'should_exit': True, 'reason': 'sl_hit',
                    'exit_price': sl_price, 'current_price': current,
                    'max_fav_r': round(max_fav_r,2), 'be_active': False}
        if not is_long and current >= sl_price:
            return {'should_exit': True, 'reason': 'sl_hit',
                    'exit_price': sl_price, 'current_price': current,
                    'max_fav_r': round(max_fav_r,2), 'be_active': False}
        # BE activation после +1R favorable
        be_active = max_fav_r >= 1.0
        if be_active:
            # New SL = entry (break-even)
            new_sl = entry_price
            if is_long and current <= new_sl:
                return {'should_exit': True, 'reason': 'be_hit',
                        'exit_price': new_sl, 'current_price': current,
                        'max_fav_r': round(max_fav_r,2), 'be_active': True,
                        'new_sl': new_sl}
            if not is_long and current >= new_sl:
                return {'should_exit': True, 'reason': 'be_hit',
                        'exit_price': new_sl, 'current_price': current,
                        'max_fav_r': round(max_fav_r,2), 'be_active': True,
                        'new_sl': new_sl}
        return {'should_exit': False, 'reason': 'holding',
                'current_price': current, 'max_fav_r': round(max_fav_r,2),
                'be_active': be_active, 'new_sl': entry_price if be_active else sl_price}
    except Exception as e:
        return {'should_exit': False, 'reason': f'error:{e}'}


def compute_trail_state(pair: str, direction: str, entry_price: float,
                         sl_price: float, opened_at_ms: int) -> dict:
    """Trail-based exit v2.0 — выиграл во всех бэктестах (AvgR +1.36..+2.18R vs +0.18R текущего).

    Logic: trail_1R_0.5R
      1. SL = signal SL (backup, не двигаем вверх)
      2. Track max_favorable price от entry (walk через 1h klines с момента входа)
      3. Когда max_favorable - entry >= 1R (sl_dist) → активировать trailing
      4. trail_sl = max_favorable - 0.5R (only UP для LONG / DOWN для SHORT)
      5. Если current price пробил trail_sl → close

    Backtest 14d × 5530 signals:
      - rsi_top × trail_1R_0.5R: 595 trades, WR 85.7%, AvgR +1.551R, hold 5.6h
      - anomaly_top × trail_1R_0.5R: 252 trades, WR 68.3%, AvgR +2.120R
      - cv_short_trend × trail_1R_0.5R: 399 trades, WR 100%, AvgR +1.658R, hold 1h
      - cv_long_anomaly × trail_1R_0.5R: 226 trades, WR 64.6%, AvgR +2.181R

    Returns: dict с полями
      should_exit: bool
      reason: str ('trail_hit'/'sl_hit'/'trail_inactive'/'error')
      max_fav_price: float — лучшая цена с момента входа
      max_fav_r: float — лучший R reached
      trail_sl: float | None — текущий trailing SL price (None если ещё не активирован)
      current_price: float | None
      trail_active: bool
    """
    try:
        from exchange import get_klines_any, get_prices_any
        import time as _t
        is_long = (direction or '').upper() == 'LONG'
        sl_dist = abs(entry_price - sl_price)
        if sl_dist <= 0:
            return {'should_exit': False, 'reason': 'invalid_sl_dist'}
        # Fetch 1h klines с момента входа (max 72h history)
        now_ms = int(_t.time() * 1000)
        hours_open = max(1, int((now_ms - opened_at_ms) / 3_600_000) + 1)
        n_bars = min(72, max(2, hours_open + 1))
        candles = get_klines_any(pair, '1h', n_bars)
        if not candles or len(candles) < 1:
            return {'should_exit': False, 'reason': 'no_klines'}
        # Filter bars >= opened_at_ms
        relevant = []
        for c in candles:
            t = c.get('t') or c.get('open_ms')
            if t is None: continue
            if t >= opened_at_ms - 3_600_000:  # включаем bar где открылась позиция
                relevant.append(c)
        if not relevant:
            return {'should_exit': False, 'reason': 'no_post_entry_bars'}
        # Track max_favorable
        max_fav = entry_price
        for c in relevant:
            h = float(c.get('h', 0) or 0)
            l = float(c.get('l', 0) or 0)
            if is_long:
                if h > max_fav: max_fav = h
            else:
                if l > 0 and (max_fav == entry_price or l < max_fav): max_fav = l
        # Current price
        sym = pair.replace('/', '').upper()
        if not sym.endswith('USDT'): sym += 'USDT'
        prices = get_prices_any([pair])
        current = prices.get(sym)
        if current is None:
            # fallback to last close
            current = float(relevant[-1].get('c', 0) or 0)
        if current <= 0:
            return {'should_exit': False, 'reason': 'no_price'}
        # Update max_fav with current price too
        if is_long:
            if current > max_fav: max_fav = current
        else:
            if current < max_fav or max_fav == entry_price: max_fav = current
        # Compute R reached
        if is_long:
            max_fav_r = (max_fav - entry_price) / sl_dist
        else:
            max_fav_r = (entry_price - max_fav) / sl_dist
        # Check SL backup (current price)
        if is_long and current <= sl_price:
            return {'should_exit': True, 'reason': 'sl_hit',
                    'exit_price': sl_price, 'current_price': current,
                    'max_fav_price': max_fav, 'max_fav_r': round(max_fav_r, 2),
                    'trail_sl': None, 'trail_active': False}
        if not is_long and current >= sl_price:
            return {'should_exit': True, 'reason': 'sl_hit',
                    'exit_price': sl_price, 'current_price': current,
                    'max_fav_price': max_fav, 'max_fav_r': round(max_fav_r, 2),
                    'trail_sl': None, 'trail_active': False}
        # Trail activation: max_fav_r >= 1.0
        trail_active = max_fav_r >= 1.0
        if not trail_active:
            return {'should_exit': False, 'reason': 'trail_inactive',
                    'current_price': current,
                    'max_fav_price': max_fav, 'max_fav_r': round(max_fav_r, 2),
                    'trail_sl': None, 'trail_active': False}
        # Compute trail_sl
        if is_long:
            trail_sl = max_fav - 0.5 * sl_dist
        else:
            trail_sl = max_fav + 0.5 * sl_dist
        # Check if current price hit trail_sl
        if is_long and current <= trail_sl:
            return {'should_exit': True, 'reason': 'trail_hit',
                    'exit_price': trail_sl, 'current_price': current,
                    'max_fav_price': max_fav, 'max_fav_r': round(max_fav_r, 2),
                    'trail_sl': round(trail_sl, 8), 'trail_active': True}
        if not is_long and current >= trail_sl:
            return {'should_exit': True, 'reason': 'trail_hit',
                    'exit_price': trail_sl, 'current_price': current,
                    'max_fav_price': max_fav, 'max_fav_r': round(max_fav_r, 2),
                    'trail_sl': round(trail_sl, 8), 'trail_active': True}
        # Active trail, but price not hit yet
        return {'should_exit': False, 'reason': 'trailing',
                'current_price': current,
                'max_fav_price': max_fav, 'max_fav_r': round(max_fav_r, 2),
                'trail_sl': round(trail_sl, 8), 'trail_active': True}
    except Exception as e:
        return {'should_exit': False, 'reason': f'error:{e}'}


def compute_exit_signal(pair: str, direction: str) -> dict:
    """[LEGACY v1.2] Проверяет 1h RSI/SMA crossover. Заменён на compute_trail_state в v2.0.
    Оставлен для backward-compat если кто-то вызывает старую функцию.

    Returns: {'should_exit': bool, 'rsi': float, 'sma': float, 'reason': str}
    """
    try:
        from exchange import get_klines_any
        # 50 баров hour = достаточно для RSI(14) + SMA(14) = warmup
        candles = get_klines_any(pair, '1h', 60)
        if not candles or len(candles) < 30:
            return {'should_exit': False, 'reason': 'insufficient_klines'}
        # Берём ВСЕ кроме последнего (текущий незакрытый бар)
        closes = [float(c.get('c', 0)) for c in candles[:-1]]
        if len(closes) < 30:
            return {'should_exit': False, 'reason': 'too_few_closes'}
        # RSI(14) Wilder
        gains, losses = [0.0], [0.0]
        for i in range(1, len(closes)):
            ch = closes[i] - closes[i-1]
            gains.append(max(ch, 0))
            losses.append(max(-ch, 0))
        period = 14
        avg_g = sum(gains[1:period+1]) / period
        avg_l = sum(losses[1:period+1]) / period
        rsi = [None] * (period + 1)
        rsi[period] = 100.0 if avg_l == 0 else (100 - 100/(1 + avg_g/avg_l))
        for i in range(period+1, len(closes)):
            avg_g = (avg_g * (period-1) + gains[i]) / period
            avg_l = (avg_l * (period-1) + losses[i]) / period
            rsi.append(100.0 if avg_l == 0 else (100 - 100/(1 + avg_g/avg_l)))
        # SMA(14) of RSI series
        valid_rsi = [r for r in rsi if r is not None]
        if len(valid_rsi) < 14:
            return {'should_exit': False, 'reason': 'rsi_insufficient'}
        sma_buf = valid_rsi[-14:]
        sma = sum(sma_buf) / 14
        last_rsi = valid_rsi[-1]
        is_long = (direction or '').upper() == 'LONG'
        # Crossover: для LONG exit когда RSI ушёл ниже SMA
        if is_long:
            should_exit = last_rsi < sma
        else:
            should_exit = last_rsi > sma
        return {
            'should_exit': should_exit,
            'rsi': round(last_rsi, 2),
            'sma': round(sma, 2),
            'reason': f'crossover_{direction}' if should_exit else 'no_cross',
        }
    except Exception as e:
        return {'should_exit': False, 'reason': f'error:{e}'}


def get_exit_plan(signal: dict, decision: dict) -> dict:
    """Возвращает exit plan для позиции.
    Используется paper_trader для управления позицией.

    Returns:
        {
          'partial_at_tp1': True,        # close 50% at signal.tp
          'partial_pct': 50,             # %
          'tp1_price': float,            # signal.tp
          'sl_price': float,             # signal.sl
          'be_after_tp1': True,          # SL→BE после TP1
          'trail_after_tp1_r': 0.5,      # trail at 0.5R after TP1
          'timeout_hours': 24,
          'exit_at_tp1_only': False,     # if True — закрыть всё при TP1
        }
    """
    return {
        'partial_at_tp1': True,
        'partial_pct': EXIT_PARTIAL_PCT,
        'tp1_price': signal.get('tp') or signal.get('tp1'),
        'sl_price': signal.get('sl'),
        'be_after_tp1': True,
        'trail_after_tp1_r': EXIT_TRAIL_DISTANCE_R,
        'timeout_hours': EXIT_TIMEOUT_HOURS,
        'exit_at_tp1_only': False,
        'auto_strategy_label': decision.get('size_label', ''),
    }


# ─── Decision wrapper ──────────────────────────────────────────────
def reason_to_human(reason: str, signal: dict) -> str:
    """Человеко-читаемый русский текст причины."""
    src = signal.get('source', '?')
    pair = signal.get('pair', '?')
    direction = signal.get('direction', '?')
    tier = signal.get('align_tier') or signal.get('_align') or '—'
    if reason.startswith('source_not_whitelisted'):
        bad = reason.split('=')[1] if '=' in reason else src
        return f"❌ Источник '{bad}' в чёрном списке (WR<30%, AvgR<-0.3R)"
    if reason == 'cv_tier=against_skipped':
        return (f"❌ {pair} CV против потока — tier={tier} (CV-against AvgR=+0.16R "
                f"мало edge, скипаем для concentration)")
    if reason == 'cv_no_tier_default_mixed':
        return f"✅ {pair} CV (tier=undef → default mixed)"
    if reason == 'sf_long_no_tier_default':
        return f"✅ {pair} second_flip LONG (tier=undef → default)"
    if reason == 'tc_long_no_tier_default':
        return f"✅ {pair} triple_confluence LONG (tier=undef → default)"
    if reason.startswith('sf_filter_failed'):
        return f"❌ second_flip требует LONG+match, у нас {direction}/{tier}"
    if reason.startswith('tc_filter_failed'):
        return f"❌ triple_confluence требует LONG+mixed, у нас {direction}/{tier}"
    if reason.startswith('regime_BULL_skip'):
        return f"❌ BULL рынок: только LONG STRICT (ELITE/STRONG) — {reason}"
    if reason.startswith('regime_BEAR_skip'):
        return f"❌ BEAR рынок: только SHORT GOOD+ — {reason}"
    if reason.startswith('regime_CHOP_skip'):
        return f"❌ CHOP рынок: оба direction GOOD+ — {reason}"
    if reason.startswith('regime_'):
        # accept
        parts = reason.split('_')
        if len(parts) >= 4:
            regime, verdict, direction = parts[1], parts[2], parts[3]
            return f"✅ {regime} regime · {verdict} · {direction}"
    if reason.startswith('strict_skip_verdict'):
        v = reason.split('=')[1] if '=' in reason else '?'
        return f"❌ STRICT-mode: verdict={v} (нужно ELITE или STRONG)"
    if reason.startswith('extended_short'):
        return f"❌ Цена SHORT на дне диапазона — mean-reversion risk ({reason.split(':',1)[1] if ':' in reason else ''})"
    if reason.startswith('extended_long'):
        return f"❌ Цена LONG на вершине диапазона — mean-reversion risk ({reason.split(':',1)[1] if ':' in reason else ''})"
    if reason.startswith('strict_ELITE') or reason.startswith('strict_STRONG'):
        parts = reason.split('_')
        verdict = parts[1] if len(parts) > 1 else '?'
        return f"✅ STRICT accept · verdict={verdict} ({signal.get('source','?')} {signal.get('direction','?')})"
    if reason.startswith('auto_pause:'):
        return f"⏸ AUTO-PAUSE: {reason.split(':',1)[1]}"
    if reason.startswith('bad_hour'):
        h = reason.split('=')[1] if '=' in reason else '?'
        return f"❌ Час {h}:00 UTC в bad-list (WR<31%, AvgR<-0.20R)"
    if reason.startswith('bad_weekday'):
        wd = int(reason.split('=')[1]) if '=' in reason else 0
        names = ['Пн','Вт','Ср','Чт','Пт','Сб','Вс']
        return f"❌ День {names[wd]} в bad-list (Mon -0.36R, Sat -0.29R)"
    if reason.startswith('q_score'):
        return f"❌ Q-Score < 40 (sanity check)"
    if reason.startswith('capital_gate:'):
        return f"❌ {reason.split(':',1)[1]}"
    if reason.startswith('cv_'):
        return f"✅ {pair} CV-{tier} {direction}"
    if reason == 'sf_long_match':
        return f"✅ {pair} second_flip LONG match (secondary edge +0.46R)"
    if reason == 'tc_long_mixed':
        return f"✅ {pair} triple_confluence LONG mixed (narrow edge +0.50R)"
    return reason


def evaluate(signal: dict, capital_state: Optional[dict] = None,
             auto_capital: bool = True) -> dict:
    """Главная функция — возвращает decision dict.

    Returns:
        {
          'accept': bool,
          'reason': str,                 # raw machine reason
          'reason_human': str,           # human-readable RU
          'size_pct': float,             # if accepted
          'size_label': str,
          'exit_plan': dict,             # if accepted
          'metadata': dict,
        }
    """
    # Pre-compute verdict для sizing rules (verdict-based ELITE/STRONG)
    if STRICT_MODE_ENABLED and not signal.get('_verdict'):
        try:
            v_info = compute_verdict(signal)
            signal['_verdict'] = v_info.get('verdict')
        except Exception:
            pass
    accept, reason = should_enter(signal)
    base_metadata = {
        'source': signal.get('source'),
        'direction': signal.get('direction'),
        'tier': signal.get('align_tier') or signal.get('_align'),
        'pair': signal.get('pair'),
        'q_score': signal.get('q_score'),
        'verdict': signal.get('_verdict'),
    }
    if not accept:
        return {
            'accept': False, 'reason': reason,
            'reason_human': reason_to_human(reason, signal),
            'size_pct': 0.0, 'size_label': 'rejected',
            'exit_plan': None,
            'metadata': base_metadata,
        }
    # Capital gate
    if capital_state is None and auto_capital:
        capital_state = get_capital_state()
    if capital_state:
        cap_ok, cap_reason = can_enter_now(**capital_state)
        if not cap_ok:
            full = f'capital_gate:{cap_reason}'
            return {
                'accept': False, 'reason': full,
                'reason_human': reason_to_human(full, signal),
                'size_pct': 0.0, 'size_label': 'capital_blocked',
                'exit_plan': None,
                'metadata': base_metadata,
            }
    size_pct = position_size_pct(signal)
    decision = {
        'accept': True,
        'reason': reason,
        'reason_human': reason_to_human(reason, signal),
        'size_pct': size_pct,
        'size_label': get_size_label(signal),
        'exit_plan': None,
        'metadata': base_metadata,
    }
    decision['exit_plan'] = get_exit_plan(signal, decision)
    return decision


# ─── Daily journal logger ──────────────────────────────────────────
def log_decision(signal: dict, decision: dict):
    """Логирует каждое решение в Mongo collection auto_strategy_log
    для post-week анализа.
    """
    try:
        from database import _get_db
        db = _get_db()
        col = db.auto_strategy_log
        doc = {
            'at': datetime.now(timezone.utc),
            'signal_pair': signal.get('pair'),
            'signal_source': signal.get('source'),
            'signal_direction': signal.get('direction'),
            'signal_at_ts': signal.get('at_ts'),
            'signal_q_score': signal.get('q_score'),
            'signal_tier': signal.get('align_tier') or signal.get('_align'),
            'accept': decision.get('accept'),
            'reason': decision.get('reason'),
            'size_pct': decision.get('size_pct'),
            'size_label': decision.get('size_label'),
        }
        col.insert_one(doc)
    except Exception as e:
        logger.debug(f'[auto-strategy] log fail: {e}')


# ─── Convenience: для вызова из watcher / paper_trader ─────────────
async def evaluate_async(signal: dict, capital_state: Optional[dict] = None) -> dict:
    """Async-обёртка."""
    import asyncio
    decision = await asyncio.to_thread(evaluate, signal, capital_state)
    # Лог тоже в thread
    await asyncio.to_thread(log_decision, signal, decision)
    return decision
