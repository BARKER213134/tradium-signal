"""Quality Score 0-100 — единая формула для всех сигналов.

Используется в:
  - /api/journal — каждый item получает score
  - /api/hot-signals — TOP N по score
  - hot_alerts.py — отправка BOT15 если score >= 60

Формула основана на бектестах (winners analysis, stack tests):
  - source priority (max 30) — Volcano > Triple Conf > VS > Second Flip > Vol Accum
  - price near level (max 25) — 🎯 in zone — самый ценный признак
  - market alignment ETH/BTC (max 20)
  - ST tier (max 15)
  - RSI healthy (max 10)
  - confluence count (max 10)
  - penalties: bad hour / Wed / SHORT / 1d opposite trend
"""
from __future__ import annotations
from typing import Optional, Any

# Source priority — от backtested edge
SOURCE_SCORE = {
    'volcano': 30,           # WR 38% +2.15% (best edge)
    'volume_surge': 22,
    'triple_confluence': 25, # multi-source confluence
    'second_flip': 20,
    'vol_accum': 12,
    'paper': 18,             # paper trader signal
    'cryptovizor': 15,       # CV pattern triggered
    'tradium': 18,           # Tradium analyst
    'cluster': 25,           # Cluster STRONG (3+ sources)
    'confluence': 8,         # base; +12 if score>=5
    'verified': 20,
    'supertrend': 12,
}

ST_TIER_SCORE = {
    'vip': 15,
    'mtf': 12,
    'daily': 8,
}

# Bad hours (winrate <15% per backtest)
BAD_HOURS = {0, 1, 5, 6, 9, 21, 23}


def compute_signal_score(sig: dict, ctx: Optional[dict] = None) -> int:
    """Returns 0-100 quality score.

    sig — журнал-item с полями source/direction/score/at_ts/entry/symbol/...
    ctx — опциональный контекст (kl_enrich/eth_chg/btc_chg/rsi/strategy_count)
    """
    ctx = ctx or {}
    score = 0
    src = (sig.get('source') or '').lower()

    # === SOURCE (max 30) ===
    base = SOURCE_SCORE.get(src, 5)
    # Confluence STRONG bonus
    if src == 'confluence' and (sig.get('score') or 0) >= 5:
        base = 20
    # CV pattern + AI score >= 70
    if src == 'cryptovizor' and (sig.get('score') or 0) >= 70:
        base = 20
    score += base

    # === PRICE NEAR LEVEL (max 25) ===
    kl = sig.get('_kl') or ctx.get('kl_enrich') or {}
    if kl and kl.get('near_level'):
        dist = abs(kl.get('distance_pct') or 0)
        if dist < 0.05:        # in zone
            score += 25
        elif dist < 0.5:
            score += 20
        elif dist < 1.0:
            score += 15
        else:
            score += 10
        # Strong KL events (breakout/breakdown) bonus +5
        if (kl.get('strength') or '') == 'strong':
            score += 5

    # === MARKET ALIGNMENT (max 20) ===
    direction = (sig.get('direction') or '').upper()
    eth_chg = ctx.get('eth_chg')
    btc_chg = ctx.get('btc_chg')
    if eth_chg is not None:
        if direction == 'LONG' and eth_chg >= 0.2:
            score += 10
        elif direction == 'SHORT' and eth_chg <= -0.2:
            score += 10
    if btc_chg is not None:
        if direction == 'LONG' and btc_chg >= 0.2:
            score += 10
        elif direction == 'SHORT' and btc_chg <= -0.2:
            score += 10

    # === ST TIER (max 15) ===
    tier = (sig.get('st_tier') or ctx.get('tier') or '').lower()
    score += ST_TIER_SCORE.get(tier, 0)

    # === RSI healthy (max 10) ===
    rsi = ctx.get('rsi')
    if rsi is not None:
        if 30 <= rsi <= 70:
            score += 10
        elif rsi < 75:
            score += 5
        # extreme overbought/oversold no bonus

    # === CONFLUENCE COUNT (max 10) ===
    sc = ctx.get('strategy_count', 0)
    if sc >= 4: score += 10
    elif sc >= 3: score += 6
    elif sc >= 2: score += 3

    # === Top Pick bonus ===
    if sig.get('is_top_pick'):
        score += 10
    if sig.get('top_pick_confirmations_count', 0) >= 2:
        score += 5

    # === PENALTIES ===
    # Bad hour
    hour = ctx.get('hour_utc')
    if hour is None and sig.get('at_ts'):
        try:
            from datetime import datetime, timezone
            hour = datetime.fromtimestamp(sig['at_ts'], tz=timezone.utc).hour
        except Exception:
            pass
    if hour is not None and hour in BAD_HOURS:
        score -= 15

    # Wednesday penalty
    weekday = ctx.get('weekday')
    if weekday is None and sig.get('at_ts'):
        try:
            from datetime import datetime, timezone
            weekday = datetime.fromtimestamp(sig['at_ts'], tz=timezone.utc).weekday()
        except Exception:
            pass
    if weekday == 2:  # Wed
        score -= 5

    # SHORT penalty (по бектестам SHORT side слабее)
    if direction == 'SHORT':
        score -= 10

    # Daily trend opposite penalty
    if ctx.get('daily_trend_opposite'):
        score -= 10

    return max(0, min(100, score))


def score_color(score: int) -> str:
    """Returns CSS color for given score."""
    if score >= 70:
        return '#00e5a0'  # green — strong setup
    if score >= 60:
        return '#88d8a3'  # light green — hot
    if score >= 45:
        return '#ffd23e'  # yellow — moderate
    if score >= 30:
        return '#ff8c50'  # orange
    return '#ff4d6d'      # red — low


def score_label(score: int) -> str:
    """Returns short label for score."""
    if score >= 70: return 'STRONG'
    if score >= 60: return 'HOT'
    if score >= 45: return 'MED'
    if score >= 30: return 'LOW'
    return 'WEAK'
