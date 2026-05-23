"""🎰 Setup Checker — paste-and-evaluate trading setup analyzer.

User вставляет pair → система выдаёт:
  - ENTER LONG / ENTER SHORT / WAIT verdict
  - Detailed score breakdown
  - Aligned/conflicting indicators
  - What to wait for (if WAIT)

Логика:
  1. Fetch 2H + 4H klines (350 баров)
  2. Compute ST(10,3) 2H + 4H states + duration since flip
  3. Run compute_whale_score на last closed bar (LONG side)
  4. Run compute_shark_score на last closed bar (SHORT side)
  5. Check TOTAL2 bias alignment
  6. RSI 14 + SMA(RSI) check
  7. Compose final verdict
"""
from __future__ import annotations
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


def _normalize_pair(pair: str) -> str:
    """'WIF' → 'WIF/USDT', 'WIFUSDT' → 'WIF/USDT'."""
    p = (pair or "").upper().strip().replace(" ", "")
    if "/" in p:
        if not p.endswith("USDT"):
            p += "USDT" if "/" not in p else ""
        return p
    # No slash — assume USDT pair
    if p.endswith("USDT"):
        base = p[:-4]
    else:
        base = p
    return f"{base}/USDT"


def _compute_supertrend_full(candles: list[dict], period: int = 10,
                              mult: float = 3.0) -> dict:
    """Returns {state, value, last_flip_idx, trend_array}."""
    n = len(candles)
    if n < period + 5:
        return {"state": None, "value": None, "last_flip_idx": None}
    closes = [c["c"] for c in candles]
    highs = [c["h"] for c in candles]
    lows = [c["l"] for c in candles]
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
    last_flip_idx = None
    for i in range(n-1, 0, -1):
        if trend[i] != 0 and trend[i-1] != 0 and trend[i] != trend[i-1]:
            last_flip_idx = i
            break
    state = "UP" if trend[-1] == 1 else ("DOWN" if trend[-1] == -1 else None)
    return {
        "state": state,
        "value": final_lower[-1] if trend[-1] == 1 else final_upper[-1],
        "last_flip_idx": last_flip_idx,
        "trend": trend,
    }


def _fmt_duration(secs: float) -> str:
    if secs < 0: return "—"
    if secs < 3600: return f"{int(secs/60)}m"
    h = int(secs / 3600)
    if h < 48: return f"{h}h"
    d = h // 24; rh = h % 24
    return f"{d}d {rh}h" if rh else f"{d}d"


def check_setup(pair_input: str) -> dict:
    """Главная функция — analyze setup для pair_input.
    Returns dict с detailed verdict.
    """
    pair = _normalize_pair(pair_input)
    symbol = pair.replace("/", "").upper()

    result = {
        "pair": pair,
        "symbol": symbol,
        "verdict": "UNKNOWN",
        "confidence": 0,
        "long_score": 0,
        "short_score": 0,
        "long_tier": None,
        "short_tier": None,
        "long_breakdown": {},
        "short_breakdown": {},
        "long_indicators": {},
        "short_indicators": {},
        "st_2h": None,
        "st_4h": None,
        "total2_bias": None,
        "recent_signals": [],
        "reasons": [],  # human-readable reasons
        "wait_for": [],  # if WAIT — what to wait for
    }

    try:
        # Fetch 2H + 4H klines
        from exchange import get_klines_any
        candles_2h = get_klines_any(pair, '2h', 350)
        candles_4h = get_klines_any(pair, '4h', 200)

        if not candles_2h or len(candles_2h) < 100:
            result["verdict"] = "NO_DATA"
            result["reasons"].append(f"Недостаточно 2H данных для {pair} ({len(candles_2h) if candles_2h else 0} баров)")
            return result

        # ST 2H
        st2h = _compute_supertrend_full(candles_2h)
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        dur_2h_s = None
        if st2h.get("last_flip_idx") is not None:
            dur_2h_s = (now_ms - candles_2h[st2h["last_flip_idx"]]["t"]) / 1000
        result["st_2h"] = {
            "state": st2h["state"],
            "value": st2h.get("value"),
            "duration": _fmt_duration(dur_2h_s) if dur_2h_s else "?",
            "duration_s": int(dur_2h_s) if dur_2h_s else None,
        }

        # ST 4H
        if candles_4h and len(candles_4h) >= 30:
            st4h = _compute_supertrend_full(candles_4h)
            dur_4h_s = None
            if st4h.get("last_flip_idx") is not None:
                dur_4h_s = (now_ms - candles_4h[st4h["last_flip_idx"]]["t"]) / 1000
            result["st_4h"] = {
                "state": st4h["state"],
                "value": st4h.get("value"),
                "duration": _fmt_duration(dur_4h_s) if dur_4h_s else "?",
                "duration_s": int(dur_4h_s) if dur_4h_s else None,
            }

        # Anti-markers from Mongo
        from database import _get_db
        db = _get_db()
        flip_ts_s = int(candles_2h[-2]["t"] / 1000)  # last closed bar

        from whale_detector import check_anti_markers as wh_anti, compute_whale_score
        from shark_detector import check_anti_markers as sh_anti, compute_shark_score

        # WHALE (LONG side) — на последнем закрытом баре
        flip_idx = len(candles_2h) - 2
        wh_flags = wh_anti(db, pair, flip_ts_s, 'LONG')
        wh_res = compute_whale_score(candles_2h, flip_idx, wh_flags)
        result["long_score"] = wh_res["score"]
        result["long_tier"] = wh_res["tier"]
        result["long_breakdown"] = wh_res["breakdown"]
        result["long_indicators"] = wh_res["indicators"]

        # SHARK (SHORT side)
        sh_flags = sh_anti(db, pair, flip_ts_s, 'SHORT')
        st_1d_state = None  # could fetch but optional
        sh_res = compute_shark_score(candles_2h, flip_idx, sh_flags, st_1d_state)
        result["short_score"] = sh_res["score"]
        result["short_tier"] = sh_res["tier"]
        result["short_breakdown"] = sh_res["breakdown"]
        result["short_indicators"] = sh_res["indicators"]

        # TOTAL2 bias
        try:
            from market_total import get_market_bias
            bias = get_market_bias()
            result["total2_bias"] = {
                "bias": bias.get("bias"),
                "label": bias.get("label"),
                "reason": bias.get("reason"),
                "st_4h_state": (bias.get("st_4h") or {}).get("state"),
                "st_1d_state": (bias.get("st_1d") or {}).get("state"),
            }
        except Exception:
            pass

        # === ВСЕ сигналы за 72h из всех источников ===
        now_dt = datetime.now(timezone.utc)
        since = now_dt - timedelta(hours=72)
        pair_or = {"$or": [{"pair": pair}, {"symbol": symbol}]}

        all_sigs: list = []

        # 1. new_strategy_signals (whale/shark/combo/volume_surge/triple_confluence/...)
        try:
            for d in db.new_strategy_signals.find({
                **pair_or, 'created_at': {'$gte': since},
            }, {
                'strategy': 1, 'created_at': 1, 'direction': 1, 'entry': 1,
                'whale_tier': 1, 'shark_tier': 1, 'whale_score': 1,
                'shark_score': 1, 'combo_score': 1, 'state': 1,
                'vol_ratio': 1, 'source_count': 1,
            }).sort('created_at', -1).limit(50):
                dt = d.get('created_at')
                if dt and dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
                all_sigs.append({
                    'source': d.get('strategy'),
                    'at': dt.isoformat() if dt else None,
                    'at_ts': int(dt.timestamp()) if dt else 0,
                    'direction': d.get('direction'),
                    'entry': d.get('entry'),
                    'tier': d.get('whale_tier') or d.get('shark_tier'),
                    'score': d.get('whale_score') or d.get('shark_score') or d.get('combo_score'),
                    'state': d.get('state'),
                })
        except Exception: pass

        # 2. supertrend_signals (vip/mtf — daily игнорим)
        try:
            for s in db.supertrend_signals.find({
                'pair': pair,
                'flip_at': {'$gte': since},
                'tier': {'$in': ['vip', 'mtf']},
            }, {'tier':1,'direction':1,'entry_price':1,'flip_at':1}).sort('flip_at',-1).limit(20):
                dt = s.get('flip_at')
                if dt and dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
                all_sigs.append({
                    'source': f"st_{s.get('tier','mtf')}",
                    'at': dt.isoformat() if dt else None,
                    'at_ts': int(dt.timestamp()) if dt else 0,
                    'direction': s.get('direction'),
                    'entry': s.get('entry_price'),
                    'tier': s.get('tier', '').upper(),
                })
        except Exception: pass

        # 3. confluence
        try:
            for c in db.confluence.find({
                **pair_or, 'detected_at': {'$gte': since},
            }, {'direction':1,'price':1,'detected_at':1,'score':1,'strength':1}).sort('detected_at',-1).limit(20):
                dt = c.get('detected_at')
                if dt and dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
                all_sigs.append({
                    'source': 'confluence',
                    'at': dt.isoformat() if dt else None,
                    'at_ts': int(dt.timestamp()) if dt else 0,
                    'direction': c.get('direction'),
                    'entry': c.get('price'),
                    'score': c.get('score'),
                    'tier': c.get('strength'),
                })
        except Exception: pass

        # 4. cv_flip
        try:
            for f in db.cv_flip_signals.find({
                'pair': pair,
                'cv_triggered_at': {'$gte': since},
                'state': 'FLIPPED',
            }, {'direction':1,'entry':1,'flip_price':1,'flip_at':1,'cv_triggered_at':1}).sort('cv_triggered_at',-1).limit(20):
                dt = f.get('flip_at') or f.get('cv_triggered_at')
                if dt and dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
                all_sigs.append({
                    'source': 'cv_flip',
                    'at': dt.isoformat() if dt else None,
                    'at_ts': int(dt.timestamp()) if dt else 0,
                    'direction': f.get('direction'),
                    'entry': f.get('entry') or f.get('flip_price'),
                })
        except Exception: pass

        # 5. anomalies
        try:
            for a in db.anomalies.find({
                **pair_or, 'detected_at': {'$gte': since},
            }, {'direction':1,'price':1,'detected_at':1,'score':1}).sort('detected_at',-1).limit(15):
                dt = a.get('detected_at')
                if dt and dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
                all_sigs.append({
                    'source': 'anomaly',
                    'at': dt.isoformat() if dt else None,
                    'at_ts': int(dt.timestamp()) if dt else 0,
                    'direction': a.get('direction'),
                    'entry': a.get('price'),
                    'score': a.get('score'),
                })
        except Exception: pass

        # 6. cryptovizor + tradium (pattern_triggered)
        try:
            for s in db.signals.find({
                'pair': pair,
                'source': {'$in': ['cryptovizor', 'tradium']},
                'pattern_triggered': True,
                'pattern_triggered_at': {'$gte': since},
            }, {'source':1,'direction':1,'entry':1,'pattern_price':1,'pattern_triggered_at':1,'ai_score':1,'pattern_name':1}).sort('pattern_triggered_at',-1).limit(10):
                dt = s.get('pattern_triggered_at')
                if dt and dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
                all_sigs.append({
                    'source': s.get('source'),
                    'at': dt.isoformat() if dt else None,
                    'at_ts': int(dt.timestamp()) if dt else 0,
                    'direction': s.get('direction'),
                    'entry': s.get('pattern_price') or s.get('entry'),
                    'score': s.get('ai_score'),
                    'tier': s.get('pattern_name'),
                })
        except Exception: pass

        # 7. clusters
        try:
            for cl in db.clusters.find({
                **pair_or, 'trigger_at': {'$gte': since},
            }, {'direction':1,'trigger_price':1,'trigger_at':1,'strength':1}).sort('trigger_at',-1).limit(10):
                dt = cl.get('trigger_at')
                if dt and dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
                all_sigs.append({
                    'source': 'cluster',
                    'at': dt.isoformat() if dt else None,
                    'at_ts': int(dt.timestamp()) if dt else 0,
                    'direction': cl.get('direction'),
                    'entry': cl.get('trigger_price'),
                    'tier': cl.get('strength'),
                })
        except Exception: pass

        # Sort by time desc + add age_hours
        all_sigs.sort(key=lambda x: x.get('at_ts', 0), reverse=True)
        now_ts = int(now_dt.timestamp())
        for s in all_sigs:
            if s.get('at_ts'):
                s['age_hours'] = round((now_ts - s['at_ts']) / 3600, 1)

        result["recent_signals"] = all_sigs

        # === Signal cluster bias: подсчёт LONG vs SHORT за 72h ===
        # TOP-7 backtest-validated sources только (остальные шумят)
        top_sources = {'whale', 'shark', 'combo', 'triple_confluence',
                       'st_vip', 'confluence', 'cv_flip'}
        long_count = sum(1 for s in all_sigs
                         if s.get('direction') == 'LONG' and s.get('source') in top_sources)
        short_count = sum(1 for s in all_sigs
                          if s.get('direction') == 'SHORT' and s.get('source') in top_sources)
        result["cluster_long"] = long_count
        result["cluster_short"] = short_count
        result["cluster_bias"] = ('LONG' if long_count > short_count + 1
                                   else 'SHORT' if short_count > long_count + 1
                                   else 'NEUTRAL')

        # ── Compose VERDICT ──
        long_ok = wh_res["passes_core"] and result["long_tier"] in ('STANDARD', 'PREMIUM')
        short_ok = sh_res["passes_core"] and result["short_tier"] in ('STANDARD', 'PREMIUM')

        # ST 2H state checks
        st2h_state = result["st_2h"]["state"]
        st4h_state = (result["st_4h"] or {}).get("state") if result["st_4h"] else None
        total2_bias = (result["total2_bias"] or {}).get("bias") if result["total2_bias"] else None

        # Confidence boost from TF alignment + penalty за counter-trend
        align_long_bonus = 0
        align_short_bonus = 0
        long_penalties = []
        short_penalties = []

        # Bonus: оба TF aligned с direction
        if st2h_state == 'UP' and st4h_state == 'UP':
            align_long_bonus += 10
        if st2h_state == 'DOWN' and st4h_state == 'DOWN':
            align_short_bonus += 10

        # Bonus: TOTAL2 bias aligned
        if total2_bias == 'LONG' and st2h_state == 'UP':
            align_long_bonus += 5
        if total2_bias == 'SHORT' and st2h_state == 'DOWN':
            align_short_bonus += 5

        # PENALTY: counter-trend signal (ST идёт против direction)
        # LONG WHALE против ST 2H DOWN — −20 confidence
        if st2h_state == 'DOWN':
            align_long_bonus -= 20
            long_penalties.append('ST 2H = DOWN (counter-trend LONG)')
        # SHORT SHARK против ST 2H UP — −20 confidence (важно для Blow-Off
        # entries — climax в момент pump'а формально UP но reversal часто
        # происходит сразу — поэтому penalty не убивает signal, а сигнализирует
        # что entry рисковый)
        if st2h_state == 'UP':
            align_short_bonus -= 20
            short_penalties.append('ST 2H = UP (counter-trend SHORT — riskier)')

        # PENALTY: 4H тоже против
        if st4h_state == 'DOWN':
            align_long_bonus -= 10
            long_penalties.append('ST 4H = DOWN (deep counter-trend LONG)')
        if st4h_state == 'UP':
            align_short_bonus -= 10
            short_penalties.append('ST 4H = UP (deep counter-trend SHORT)')

        # PENALTY: TOTAL2 bias против direction
        if total2_bias == 'SHORT':
            align_long_bonus -= 10
            long_penalties.append('TOTAL2 bias = SHORT (рынок альтов вниз)')
        if total2_bias == 'LONG':
            align_short_bonus -= 10
            short_penalties.append('TOTAL2 bias = LONG (рынок альтов вверх)')

        # BONUS / PENALTY: signal cluster bias за 72h
        # Если 3+ LONG signals из TOP-7 → +15 LONG, −10 SHORT
        # Если 3+ SHORT signals из TOP-7 → +15 SHORT, −10 LONG
        cluster_bias = result.get("cluster_bias", "NEUTRAL")
        long_cnt = result.get("cluster_long", 0)
        short_cnt = result.get("cluster_short", 0)
        if cluster_bias == 'LONG' and long_cnt >= 3:
            align_long_bonus += 15
            long_penalties.append(f'+15 cluster_72h: {long_cnt} LONG vs {short_cnt} SHORT signals')
            align_short_bonus -= 10
            short_penalties.append(f'cluster_72h дрейфует в LONG ({long_cnt} vs {short_cnt})')
        elif cluster_bias == 'SHORT' and short_cnt >= 3:
            align_short_bonus += 15
            short_penalties.append(f'+15 cluster_72h: {short_cnt} SHORT vs {long_cnt} LONG signals')
            align_long_bonus -= 10
            long_penalties.append(f'cluster_72h дрейфует в SHORT ({short_cnt} vs {long_cnt})')

        adj_long = result["long_score"] + align_long_bonus
        adj_short = result["short_score"] + align_short_bonus

        # Сохраняем для UI
        result["long_alignment"] = align_long_bonus
        result["short_alignment"] = align_short_bonus
        result["long_penalties"] = long_penalties
        result["short_penalties"] = short_penalties

        # Final decision
        def _fmt_align(bonus, penalties):
            """Форматирует +/- bonus с penalties как human-readable."""
            sign = '+' if bonus >= 0 else ''
            s = f"{sign}{bonus} alignment"
            if penalties:
                s += f" ({'; '.join(penalties)})"
            return s

        if long_ok and short_ok:
            # Rare — both fired. Use bias to break tie
            if adj_long > adj_short + 10:
                result["verdict"] = "ENTER_LONG"
                result["confidence"] = max(20, min(95, 50 + adj_long // 2))
                result["reasons"].append(f"🐋 WHALE {result['long_tier']} score {result['long_score']}, {_fmt_align(align_long_bonus, long_penalties)}")
            elif adj_short > adj_long + 10:
                result["verdict"] = "ENTER_SHORT"
                result["confidence"] = max(20, min(95, 50 + adj_short // 2))
                result["reasons"].append(f"🦈 SHARK {result['short_tier']} score {result['short_score']}, {_fmt_align(align_short_bonus, short_penalties)}")
            else:
                result["verdict"] = "WAIT"
                result["reasons"].append("Оба сигнала качают — конфликт")
        elif long_ok:
            result["verdict"] = "ENTER_LONG"
            result["confidence"] = max(20, min(95, 50 + adj_long // 2))
            result["reasons"].append(f"🐋 WHALE {result['long_tier']} score {result['long_score']}, {_fmt_align(align_long_bonus, long_penalties)}")
            if st2h_state == 'UP':
                result["reasons"].append(f"✓ ST 2H = UP ({result['st_2h']['duration']}) — trend-aligned")
            else:
                result["reasons"].append(f"⚠ COUNTER-TREND — ST 2H = {st2h_state} ({result['st_2h']['duration']})")
            # Если confidence ≤ 35 — лучше пропустить
            if result["confidence"] <= 35:
                result["reasons"].append(f"❌ Confidence {result['confidence']}% слишком низкий — пропусти")
        elif short_ok:
            result["verdict"] = "ENTER_SHORT"
            result["confidence"] = max(20, min(95, 50 + adj_short // 2))
            result["reasons"].append(f"🦈 SHARK {result['short_tier']} score {result['short_score']}, {_fmt_align(align_short_bonus, short_penalties)}")
            if st2h_state == 'DOWN':
                result["reasons"].append(f"✓ ST 2H = DOWN ({result['st_2h']['duration']}) — trend-aligned")
            else:
                result["reasons"].append(f"⚠ COUNTER-TREND — ST 2H = {st2h_state} ({result['st_2h']['duration']})")
            if result["confidence"] <= 35:
                result["reasons"].append(f"❌ Confidence {result['confidence']}% слишком низкий — пропусти")
        else:
            # WAIT scenario
            result["verdict"] = "WAIT"
            ind_l = result["long_indicators"]
            ind_s = result["short_indicators"]
            # What's missing for LONG
            wait_long = []
            if not wh_res["passes_core"]:
                if ind_l.get("vol_ratio_max", 0) < 2:
                    wait_long.append(f"vol ratio {ind_l.get('vol_ratio_max', 0)}× < 2× (нет accumulation для WHALE)")
                else:
                    wait_long.append("CORE WHALE не прошёл")
            elif result["long_tier"] == "MARGINAL":
                wait_long.append(f"WHALE MARGINAL score {result['long_score']} < 60 — слабые amplifiers")
            # What's missing for SHORT
            wait_short = []
            if not sh_res["passes_core"]:
                if ind_s.get("vol_ratio_max", 0) < 2:
                    wait_short.append(f"vol ratio {ind_s.get('vol_ratio_max', 0)}× < 2× (нет distribution для SHARK)")
                else:
                    wait_short.append("CORE SHARK не прошёл")
            elif result["short_tier"] == "MARGINAL":
                wait_short.append(f"SHARK MARGINAL score {result['short_score']} < 60")

            result["wait_for"] = []
            if st2h_state == 'DOWN':
                result["wait_for"].append("Жди ST 2H flip UP для LONG entry")
            elif st2h_state == 'UP':
                result["wait_for"].append("Жди ST 2H flip DOWN для SHORT entry")
            result["wait_for"].extend(wait_long[:1])  # самое важное
            result["wait_for"].extend(wait_short[:1])
            result["reasons"].append("Нет качественного setup сейчас")
            result["reasons"].append(f"LONG: {wait_long[0] if wait_long else 'ok'}")
            result["reasons"].append(f"SHORT: {wait_short[0] if wait_short else 'ok'}")

        # Add bias context to reasons
        if total2_bias:
            result["reasons"].append(f"📊 TOTAL2 bias: {total2_bias}")

        return result
    except Exception as e:
        logger.exception(f"[setup-check] {pair_input} fail")
        result["verdict"] = "ERROR"
        result["reasons"].append(f"Error: {str(e)}")
        return result
