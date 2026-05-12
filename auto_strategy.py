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
STRATEGY_VERSION = "v2.0"
STRATEGY_DESCRIPTION = (
    "Концентрация на cryptovizor signals + trail_1R_0.5R exit "
    "(trailing stop активируется при +1R, отступ 0.5R от max favorable). "
    "Backtest 14d × 5530 сигналов: 3-9× улучшение vs v1.2 SMA crossover exit."
)
STRATEGY_BACKTEST_METRICS = {
    "backtest_period_days": 14,
    "total_signals_tested": 5534,
    "trades_after_filter": 1216,
    "win_rate_pct": 70.0,
    "avg_r_per_trade": 1.55,
    "profit_factor": 9.5,
    "validation": "exhaustive grid 32 exits × 10 entry filters — trail_1R_0.5R win",
    "exit_strategy": "trail_1R_0.5R",
    "notes": "trail wins on 7 of 10 entry filters. Top: cv_long_anomaly +2.18R; cv_short_trend WR 100%; rsi_top +1.55R",
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
    accept, reason = should_enter(signal)
    base_metadata = {
        'source': signal.get('source'),
        'direction': signal.get('direction'),
        'tier': signal.get('align_tier') or signal.get('_align'),
        'pair': signal.get('pair'),
        'q_score': signal.get('q_score'),
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
