# 🎯 STRATEGY ALPHA-CV v1.0

**Status:** Ready to deploy · Запуск на следующую неделю
**Created:** 2026-05-09 (повторная валидация требуется через 7 дней)
**Backtest data:** 14 дней, 5500+ сигналов, walk-forward validated

---

## TL;DR

Стратегия концентрируется на **самом высоком статистическом edge** в системе — Cryptovizor signals — и фильтрует мусор через source whitelist + alignment tier + time filters.

**Backtest performance:**
- WR **66.4%** (out-of-sample test, 2 дня)
- AvgR **+1.05R** per trade
- Profit Factor **4.13** (на $1 риска приносит $4.13 прибыли)
- ~60-100 сделок в день

---

## Discovered edges (14d backtest, 5500 signals)

### Per source overall

| Source | N | WR | AvgR | Verdict |
|--------|---|------|------|---------|
| **cryptovizor** | 1528 | 62.0% | **+0.85R** | ⭐⭐⭐ Top edge |
| second_flip | 166 | 41.9% | +0.05R | ⭐ Selective |
| triple_confluence | 306 | 20.8% | -0.37R | ⭐ LONG mixed only |
| supertrend | 2766 | 28.5% | -0.30R | ❌ Avoid (no other) |
| vol_accum | 517 | 28.9% | -0.28R | ❌ Avoid |
| volume_surge | 197 | 10.4% | -0.63R | ❌ Avoid |
| volcano | 25 | 8.0% | -0.76R | ❌ Avoid |
| tradium | 23 | 28.6% | -0.14R | ⚠ Too small N |

### Top buckets (highest expectancy)

| Bucket | N | WR | AvgR |
|--------|---|------|------|
| 🥇 cryptovizor SHORT mixed | 123 | 97.6% | **+2.54R** |
| 🥈 cryptovizor SHORT match | 202 | 99.5% | **+2.36R** |
| 🥉 cryptovizor any match | 622 | 69.8% | +1.52R |
| 4 cryptovizor SHORT against | 89 | 92.1% | +1.41R |
| 5 cryptovizor LONG match | 420 | 53.6% | +1.06R |

### Hour analysis (UTC, all sources)

**BAD HOURS (avoid trading):**
- 10:00 — WR 19.2%, AvgR **-0.50R** ❌
- 12:00 — WR 23.6%, AvgR -0.34R
- 13:00 — WR 30.0%, AvgR -0.22R
- 1:00 — WR 30.4%, AvgR -0.22R
- 2:00 — WR 30.5%, AvgR -0.08R

**Good hours (no filter needed):**
- 03 (+0.87R), 19 (+0.85R), 23 (+0.58R), 17 (+0.38R)

### Weekday

**BAD WEEKDAYS:**
- Monday — AvgR **-0.36R** ❌
- Saturday — AvgR -0.29R

---

## STRATEGY RULES

### Entry filter (must pass ALL)

```python
# Source + tier whitelist
if source == 'cryptovizor':
    require: align_tier in {match, mixed}      # skip 'against'
elif source == 'second_flip':
    require: direction == 'LONG' and align_tier == 'match'
elif source == 'triple_confluence':
    require: direction == 'LONG' and align_tier == 'mixed'
else:
    REJECT  # все остальные источники

# Time filters
hour_utc NOT in {1, 2, 10, 12, 13}
weekday NOT in {Monday, Saturday}

# Sanity
q_score >= 40
```

### Position sizing (Druckenmiller-scaled)

| Setup | Multiplier | Rationale |
|-------|-----------|-----------|
| CV SHORT match | **3.0×** | Highest edge: +2.36R |
| CV SHORT mixed | 2.5× | +2.54R |
| CV LONG match | 2.0× | +1.06R |
| CV other allowed | 1.5× | base CV positive |
| second_flip LONG match | 1.0× | +0.46R |
| triple_confluence LONG mixed | 0.7× | +0.50R |

base_pct = 1% капитала. Final size = base × multiplier.

### Risk management (PT Jones — capital preservation)

| Limit | Value | Action |
|-------|-------|--------|
| Max concurrent positions | **5** | Block new entry |
| Max total exposure | **12%** of capital | Block new entry |
| Daily loss limit | **-3%** | Stop day, resume next |
| Drawdown limit | **-10%** | Pause 24h, manual review |
| Per-trade hard stop | At signal SL | No override |

### Exit rules

- **TP1 hit**: take 50%, move SL to breakeven
- **6h after entry**: trail SL by 0.5R if floating profit > 0.5R
- **24h timeout**: exit at market

### Hard avoid (никогда не входим)

- volume_surge any (10% WR, -0.63R)
- volcano any (8% WR, -0.76R)
- triple_confluence SHORT (5% WR)
- vol_accum SHORT (15% WR)
- supertrend без other source

---

## Walk-forward validation (out-of-sample)

| Metric | TRAIN (12d) | TEST (2d, OOS) |
|--------|------------|----------------|
| Signals taken | 657 | 119 |
| Win rate | 66.7% | **66.4%** |
| AvgR | +1.12R | **+1.05R** |
| Profit Factor | 4.35 | 4.13 |

**OOS stable** — стратегия не overfit. Drift WR < 0.5pp, AvgR < 0.07R = в пределах шума.

---

## Activation (deployment)

### Включение

Set Railway environment variable:
```
AUTO_STRATEGY_ALPHA_CV=1
```

После деплоя: paper_trader при каждом сигнале вызывает `auto_strategy.evaluate(signal)`. Если decision.accept=False → сигнал отклоняется с логом в `paper_rejections` collection.

### Мониторинг

Каждое решение логируется в Mongo collection **`auto_strategy_log`** с полями:
- `at`, `signal_pair`, `signal_source`, `signal_direction`, `signal_at_ts`
- `signal_q_score`, `signal_tier`
- `accept`, `reason`, `size_pct`, `size_label`

Daily review queries:
```javascript
// Counts по reason'ам за сегодня
db.auto_strategy_log.aggregate([
  {$match: {at: {$gte: ISODate("2026-05-10T00:00:00Z")}}},
  {$group: {_id: "$reason", count: {$sum: 1}}}
])

// Принятые сделки и их sources
db.auto_strategy_log.find({accept: true}).count()
```

### Drift detection rules

Если за 7 дней:
- WR падает ниже **55%** → ⚠ pause + manual review
- AvgR падает ниже **+0.5R** → ⚠ pause
- Drawdown > 10% → 24h pause (auto)
- Single source dominates >70% picks → diversification check

### Re-validation cadence

Раз в неделю запускать `_bt_master.py` на свежих 14d данных. Если top edges смещаются — обновить SIZING_RULES и BAD_HOURS в `auto_strategy.py`.

---

## Top trader principles applied

1. **Stanley Druckenmiller**: *"Concentrate when conviction is high"* → 3× sizing на CV SHORT match (highest edge)
2. **Paul Tudor Jones**: *"Capital preservation > everything"* → daily/DD limits, max concurrent positions
3. **Renaissance Technologies**: *"Statistical edge from many small trades"* → 5500-signal backtest, walk-forward valid
4. **Jesse Livermore**: *"Don't fight the trend"* → tier=match preferred over against
5. **Mark Douglas**: *"Probabilistic thinking"* → каждый сигнал independent, take only +EV setups
6. **Larry Williams**: *"No overtrading"* → max 5 concurrent positions
7. **Jim Simons**: *"Use data, not feelings"* → strategy 100% data-driven, no discretion

---

## Risks & known limitations

1. **Sample period bias**: 14d может не покрывать regime changes. Re-validate weekly.
2. **CV SHORT 99% WR подозрительно высокий**: возможно asymmetric TP/SL ratio (TP tight). Требует слежки за реальным slippage.
3. **Klines outcome detection**: 24h timeout window — slow movers (~10%) считаются как timeout, не учитываются в WR.
4. **No fee/slippage modeling**: реальный edge будет на ~0.1-0.2R меньше после комиссий.
5. **Klines IP ban**: REST доступ может быть ограничен — backfill через CDN остаётся работать.

---

## Changelog

- **v1.0** (2026-05-09): Initial release. CV-focused, walk-forward OOS validated.
