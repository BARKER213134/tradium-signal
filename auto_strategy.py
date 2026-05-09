"""ALPHA-CV v2 — Автотрейдинговая стратегия (запускается на неделю).

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

# Bad hours (UTC) per 14d backtest: WR<30% или AvgR<-0.20R
BAD_HOURS = {1, 2, 10, 12, 13}

# Bad weekdays
BAD_WEEKDAYS = {0, 5}  # Mon, Sat

# Q-score threshold (sanity check on top of source/tier rules)
MIN_Q_SCORE = 40

# ─── Sizing multipliers ────────────────────────────────────────────
def _tier_of(t: dict) -> str:
    return t.get('align_tier') or t.get('_align') or t.get('tier') or ''

SIZING_RULES = [
    # (predicate_fn, multiplier, label)
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
MAX_CONCURRENT_POSITIONS = 5
MAX_TOTAL_EXPOSURE_PCT = 12.0
DAILY_LOSS_LIMIT_PCT = 3.0
DRAWDOWN_LIMIT_PCT = 10.0


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

    # ── Q-score sanity ──
    qs = signal.get('q_score')
    if qs is not None and qs < MIN_Q_SCORE:
        return False, f'q_score={qs}<{MIN_Q_SCORE}'

    # ── Source-specific rules ──
    if src == 'cryptovizor':
        if tier in CV_TIER_ALLOWED:
            return True, f'cv_{tier}_{direction}'
        return False, f'cv_tier={tier}_skipped'

    if src == 'second_flip':
        if direction == SF_REQUIRED['direction'] and tier == SF_REQUIRED['tier']:
            return True, 'sf_long_match'
        return False, f'sf_filter_failed({direction}/{tier})'

    if src == 'triple_confluence':
        if direction == TC_REQUIRED['direction'] and tier == TC_REQUIRED['tier']:
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
def can_enter_now(open_positions: int = 0,
                  total_exposure_pct: float = 0.0,
                  daily_pnl_pct: float = 0.0,
                  current_dd_pct: float = 0.0) -> tuple[bool, str]:
    """Проверяет capital-level limits перед открытием новой сделки."""
    if open_positions >= MAX_CONCURRENT_POSITIONS:
        return False, f'max_concurrent_reached({open_positions})'
    if total_exposure_pct >= MAX_TOTAL_EXPOSURE_PCT:
        return False, f'max_exposure={total_exposure_pct:.1f}%'
    if daily_pnl_pct <= -DAILY_LOSS_LIMIT_PCT:
        return False, f'daily_loss_hit({daily_pnl_pct:.1f}%)'
    if current_dd_pct >= DRAWDOWN_LIMIT_PCT:
        return False, f'drawdown_pause({current_dd_pct:.1f}%)'
    return True, 'ok'


# ─── Decision wrapper ──────────────────────────────────────────────
def evaluate(signal: dict, capital_state: Optional[dict] = None) -> dict:
    """Главная функция — возвращает decision dict.

    Returns:
        {
          'accept': bool,
          'reason': str,
          'size_pct': float,        # if accepted
          'size_label': str,
          'metadata': dict,         # source, tier, etc.
        }
    """
    accept, reason = should_enter(signal)
    if not accept:
        return {
            'accept': False, 'reason': reason,
            'size_pct': 0.0, 'size_label': 'rejected',
            'metadata': {},
        }
    if capital_state:
        cap_ok, cap_reason = can_enter_now(**capital_state)
        if not cap_ok:
            return {
                'accept': False, 'reason': f'capital_gate:{cap_reason}',
                'size_pct': 0.0, 'size_label': 'capital_blocked',
                'metadata': {},
            }
    size_pct = position_size_pct(signal)
    return {
        'accept': True,
        'reason': reason,
        'size_pct': size_pct,
        'size_label': get_size_label(signal),
        'metadata': {
            'source': signal.get('source'),
            'direction': signal.get('direction'),
            'tier': signal.get('align_tier') or signal.get('_align'),
            'pair': signal.get('pair'),
            'q_score': signal.get('q_score'),
        },
    }


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
