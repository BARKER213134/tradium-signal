# Pre-Pump Predictor — Deployment Summary

**Commit:** `d1673f9` (1536 lines, 11 files)
**Date:** 2026-05-20

## Что реализовано

### 7 новых модулей (leading indicators)

| Модуль | Что делает | Lead time |
|---|---|---|
| `volume_profile.py` | volume 24h vs avg 7d, accumulation flag | 12-48h |
| `open_interest.py` | OI growth % через fapi /openInterestHist | 6-24h |
| `funding_rate.py` | Funding rate skew (short/long squeeze setup) | 6-12h |
| `sectors_coingecko.py` | CoinGecko categories + sector rotation detector | — |
| `bingx_pairs.py` | Filter — только пары на BingX | — |
| `pre_pump_predictor.py` | Composite score 0-100, 7 components weighted | — |
| `prepump_bot.py` | BOT13 Telegram alerts (PRIME only) | — |

### Composite Score (0-100)

```
25 pts — Volume Profile  (vol_24h vs 7d avg ≥ 2.0)
20 pts — Open Interest   (OI ≥ +20% за 24h)
15 pts — Funding Rate    (short skew / long skew)
15 pts — BB Squeeze      (BB width ≤ 20 percentile)
10 pts — Price Flat      (volatility ≤ 3%, change ≤ 4%)
10 pts — Sector Rotation (2+ pairs same sector active)
 5 pts — RSI Compression (low RSI std)
```

**Tiers:**
- 🔥 **PRIME** ≥ 75 — alert через Telegram BOT13
- 🟡 **STRONG** 60-74 — только в UI
- 🟢 **WATCH** 45-59 — только в UI

### Watcher loop

`_pre_pump_predictor_loop()` — каждые 30 мин:
1. Get BingX pairs (filter ~200 пар)
2. Compute volume scores (parallel)
3. Detect sector rotation
4. Predict per pair → composite score
5. Save в Mongo `pre_pump_candidates`
6. PRIME → Telegram alert (rate-limited 4h/pair)

### API endpoints

- `GET /api/prepump/candidates?tier=PRIME&hours=24` — список кандидатов
- `GET /api/prepump/active-sectors?hours=24` — активные секторы

### UI вкладка `/signals?bot=prepump`

- Карточки кандидатов с tier badge + composite score
- 5 leading indicators visual breakdown
- Filter: tier + time window (6h-week)
- Stats dashboard (PRIME/STRONG/WATCH counts)
- Active sectors strip
- Inline chart modal (EMA20/50/200, SuperTrend, RSI)

## Что НЕ реализовано (Phase 7+)

❌ **Auto-trade integration** — DISABLED (variant B safety).
   После shadow mode 1-2 недели → решение enable

❌ **30d Backtest на Railway** — нужен отдельный endpoint
   с historical OI/funding fetch (нет в Vision CDN)

❌ **Cooldown protection** — для будущего auto-trade

## Что юзеру делать когда проснётся

1. **Открой Railway → Variables**:
   ```
   BOT13_BOT_TOKEN=8995252669:AAFOkPzTZz1cvfxWLJH-FKrwaNmSP9QMl7s
   PREPUMP_CHAT_ID=<твой chat_id через @userinfobot>
   PREPUMP_QUIET_HOURS=02-08    # optional, МСК
   ```

2. **Через ~3 мин после деплоя** открой:
   `https://tradium-signal-production.up.railway.app/signals?bot=prepump`

3. **Подожди 30-60 мин** для первого scanner cycle.
   Сначала Mongo collection `pre_pump_candidates` пустая.
   После первого прогона будут results.

4. **Smoke test**:
   - Открой `/health` — должен быть `"ok":true`
   - Открой `/api/prepump/candidates?hours=24` — JSON с items
   - Открой `/signals?bot=prepump` — UI работает

5. **Если BOT13 готов**:
   - Напиши боту `/start` (он сам ничего не делает но активирует чат)
   - PRIME alerts начнут приходить когда первый qualified candidate появится

## Risk notes

- **CoinGecko rate limit** (free tier): 10-30 req/min. Cap 50 пар/cycle защитит
- **fapi OI** rate limit: 2400 req/min. 200 пар × 1 запрос = OK
- **Mongo growth**: pre_pump_candidates collection растёт ~50-200 docs/cycle
  → за день ~5000-10000 docs. Через месяц 150K. Нужно TTL index (TODO)

## Next steps (когда юзер подтвердит что UI работает)

1. **Backtest endpoint** — POST /api/prepump/backtest (запускается через UI)
2. **TTL index** для pre_pump_candidates (auto-cleanup >30d)
3. **Shadow mode review** — manual check каждого PRIME alert'а
4. **Auto-trade integration** — после validates
