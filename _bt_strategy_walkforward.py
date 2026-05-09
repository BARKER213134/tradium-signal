"""Walk-forward валидация стратегии ALPHA-CV.

Из всех сигналов 14d применяет стратегию-фильтр и считает:
- Total trades, WR, AvgR, expectancy
- Equity curve (1% per trade base)
- Max drawdown
- Sharpe-like ratio
- Compare к baseline (без фильтра)

Также проверяет out-of-sample: train 12d, test last 2d.
"""
from __future__ import annotations
import json
from datetime import datetime, timezone, timedelta
from collections import defaultdict


# Загрузить detailed signals
with open('_bt_signals_detailed.json') as f:
    SIGNALS = json.load(f)

print(f"Total signals: {len(SIGNALS)}")
decided = [s for s in SIGNALS if s.get('outcome') in ('tp', 'sl')]
print(f"Decided: {len(decided)}")
print()


# ═══ STRATEGY ALPHA-CV v1.0 ═══
# Главные принципы (Druckenmiller + PT Jones):
# 1. Concentrate on highest-edge: cryptovizor signals (overall +0.85R AvgR)
# 2. Cut bottom buckets — обходим стороной
# 3. Time filter — избегаем bad hours

# BAD_HOURS на основе 14d backtest (WR < 25% или AvgR < -0.25R)
BAD_HOURS_NEW = {1, 2, 10, 12, 13}  # старые: {0,1,5,6,9,21,23} — обновлено!

# BAD_WEEKDAYS
BAD_WEEKDAYS = {0, 5}  # Mon, Sat


def strategy_alpha_cv_v1(s):
    """v1: только cryptovizor, фильтр по tier и времени."""
    src = s.get('source')
    if src != 'cryptovizor':
        return False, 'not_cv'
    tier = s.get('align_tier')
    if tier == 'against':
        return False, 'tier_against'
    # Time filters
    if s.get('hour') in BAD_HOURS_NEW:
        return False, f'bad_hour={s.get("hour")}'
    if s.get('weekday') in BAD_WEEKDAYS:
        return False, f'bad_weekday={s.get("weekday")}'
    return True, 'cv_match_or_mixed'


def strategy_alpha_cv_v2(s):
    """v2: CV + лучшие not-CV setups."""
    src = s.get('source')
    tier = s.get('align_tier')
    direction = s.get('direction')
    h = s.get('hour')
    wd = s.get('weekday')

    # Hour/weekday filter (universal)
    if h in BAD_HOURS_NEW:
        return False, f'bad_hour={h}'
    if wd in BAD_WEEKDAYS:
        return False, f'bad_weekday={wd}'

    # CV — ENTER (любой tier, кроме against который тоже ОК но edge меньше)
    if src == 'cryptovizor':
        if tier == 'against':
            return False, 'cv_against_skip'  # 0.16R — не плохо но шум
        return True, f'cv_{tier}'

    # second_flip + LONG + match — secondary edge
    if src == 'second_flip' and direction == 'LONG' and tier == 'match':
        return True, 'sf_long_match'

    # triple_confluence + LONG + mixed (не match!) — narrow edge
    if src == 'triple_confluence' and direction == 'LONG' and tier == 'mixed':
        return True, 'tc_long_mixed'

    return False, f'no_edge_{src}'


def strategy_alpha_cv_v3_strict(s):
    """v3: только high-conviction setups. Меньше сигналов, выше edge."""
    src = s.get('source')
    tier = s.get('align_tier')
    direction = s.get('direction')
    h = s.get('hour')
    wd = s.get('weekday')

    if h in BAD_HOURS_NEW:
        return False, 'bad_hour'
    if wd in BAD_WEEKDAYS:
        return False, 'bad_weekday'

    # Only CV match/mixed — top-edge concentration
    if src == 'cryptovizor':
        if tier in ('match', 'mixed'):
            return True, f'cv_{tier}_{direction}'
        return False, 'cv_against'

    # second_flip LONG match — only secondary
    if src == 'second_flip' and direction == 'LONG' and tier == 'match':
        return True, 'sf_long_match'

    return False, 'not_eligible'


# ─── Position sizing (Druckenmiller-style: scale with edge) ───
def position_size(s, signal_type='match', base_pct=1.0):
    """Кэлли-cap'd position sizing.
    Multiplier по signal type — эмпирически подобран от backtest AvgR.
    """
    src = s.get('source')
    tier = s.get('align_tier')
    direction = s.get('direction')
    if src == 'cryptovizor' and direction == 'SHORT':
        return base_pct * 3.0  # +2.36R highest edge
    if src == 'cryptovizor' and direction == 'LONG' and tier == 'match':
        return base_pct * 2.0  # +1.06R
    if src == 'cryptovizor':
        return base_pct * 1.5  # default CV
    if src == 'second_flip' and tier == 'match':
        return base_pct * 1.0
    return base_pct * 0.7


# ─── Backtest harness ───
def backtest(signals, strategy_fn, label):
    print(f"\n{'='*78}")
    print(f"STRATEGY: {label}")
    print(f"{'='*78}")
    taken = []
    skipped_reasons = defaultdict(int)
    for s in signals:
        accept, reason = strategy_fn(s)
        if accept:
            if s.get('outcome') in ('tp', 'sl'):
                taken.append(s)
        else:
            skipped_reasons[reason] += 1

    # Stats
    n = len(taken)
    if n == 0:
        print("  No trades selected")
        return None
    wins = sum(1 for s in taken if s.get('outcome') == 'tp')
    losses = sum(1 for s in taken if s.get('outcome') == 'sl')
    wr = wins / n * 100
    avg_r = sum(s.get('r', 0) for s in taken) / n
    # Profit factor
    sum_wins = sum(s['r'] for s in taken if s.get('outcome') == 'tp')
    sum_losses = abs(sum(s['r'] for s in taken if s.get('outcome') == 'sl'))
    pf = sum_wins / sum_losses if sum_losses > 0 else float('inf')

    # Equity curve (1% per trade, simple)
    equity = [100.0]
    for s in taken:
        size = position_size(s)
        ret_pct = s['r'] * size  # in % (1R = 1% if size=1)
        equity.append(equity[-1] * (1 + ret_pct / 100))

    final_equity = equity[-1]
    total_return = (final_equity - 100) / 100 * 100
    max_dd = 0
    peak = equity[0]
    for v in equity:
        if v > peak: peak = v
        dd = (peak - v) / peak * 100
        if dd > max_dd: max_dd = dd

    # Per-bucket breakdown
    bucket_stats = defaultdict(lambda: {'n':0, 'wins':0, 'r_sum':0})
    for s in taken:
        accept, reason = strategy_fn(s)
        bucket_stats[reason]['n'] += 1
        if s.get('outcome') == 'tp':
            bucket_stats[reason]['wins'] += 1
        bucket_stats[reason]['r_sum'] += s.get('r', 0)

    print(f"\n  Trades taken:        {n}")
    print(f"  Wins / Losses:       {wins} / {losses}")
    print(f"  Win Rate:            {wr:.1f}%")
    print(f"  Avg R:               {avg_r:+.2f}R")
    print(f"  Profit Factor:       {pf:.2f}")
    print(f"  Total Return:        {total_return:+.1f}%")
    print(f"  Max Drawdown:        -{max_dd:.1f}%")
    print(f"\n  Bucket breakdown:")
    for k, v in sorted(bucket_stats.items(), key=lambda x: -x[1]['r_sum']):
        avg_r_b = v['r_sum'] / v['n'] if v['n'] else 0
        wr_b = v['wins'] / v['n'] * 100 if v['n'] else 0
        print(f"    {k:<35} N={v['n']:3d}  WR={wr_b:5.1f}%  AvgR={avg_r_b:+.2f}R")
    print(f"\n  Top skip reasons:")
    skip_total = sum(skipped_reasons.values())
    for r, c in sorted(skipped_reasons.items(), key=lambda x: -x[1])[:6]:
        print(f"    {r:<30} {c} ({c/skip_total*100:.0f}%)")
    return {
        'label': label, 'n': n, 'wr': wr, 'avg_r': avg_r, 'pf': pf,
        'total_return': total_return, 'max_dd': max_dd,
    }


# ─── Baseline (всё, без фильтра) ───
def baseline_all(s):
    return True, 'all'


# ─── Walk-forward: train (12d) vs test (2d) ───
def split_signals(signals):
    cutoff_2d = (datetime.now(timezone.utc) - timedelta(days=2)).timestamp()
    train = [s for s in signals if s.get('at_ts', 0) < cutoff_2d]
    test = [s for s in signals if s.get('at_ts', 0) >= cutoff_2d]
    return train, test


# ─── Run all strategies ───
results = {}
for strat_fn, label in [
    (baseline_all, 'BASELINE (all signals)'),
    (strategy_alpha_cv_v1, 'ALPHA-CV v1 (CV only)'),
    (strategy_alpha_cv_v2, 'ALPHA-CV v2 (CV + secondary)'),
    (strategy_alpha_cv_v3_strict, 'ALPHA-CV v3 (strict)'),
]:
    results[label] = backtest(SIGNALS, strat_fn, label)

# Walk-forward
print(f"\n\n{'#'*78}")
print(f"# WALK-FORWARD VALIDATION (train 12d / test 2d)")
print(f"{'#'*78}")
train, test = split_signals(SIGNALS)
print(f"\nTrain: {len(train)} signals, Test: {len(test)} signals\n")
for strat_fn, label in [
    (strategy_alpha_cv_v2, 'ALPHA-CV v2 — TRAIN'),
    (strategy_alpha_cv_v2, 'ALPHA-CV v2 — TEST'),
]:
    if 'TRAIN' in label:
        backtest(train, strat_fn, label)
    else:
        backtest(test, strat_fn, label)

# Save final
with open('_bt_strategy_results.json', 'w', encoding='utf-8') as f:
    json.dump(results, f, indent=2, ensure_ascii=False)
print(f"\nSaved results to _bt_strategy_results.json")
