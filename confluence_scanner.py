"""Confluence Scanner — ищет идеальные сетапы по всему рынку фьючей.

Confluence = совпадение нескольких факторов:
1. Цена на уровне S/R (4h + 1h)
2. Объём выше среднего на уровне
3. Multi-TF тренд (3+ из 5 TF в одном направлении)
4. Свечной паттерн (разворот или продолжение)
5. ETH корреляция (попутный рынок)
6. FTT подтверждение (разворот на объёме)

Score 5-6 = STRONG, 4 = MEDIUM, <4 = SKIP
"""
import logging
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

FAPI = "https://fapi.binance.com"


def scan_confluence(symbol: str) -> Optional[dict]:
    """Полный confluence-анализ одной пары. Возвращает None если score < 5."""
    from exchange import get_klines_any, get_eth_market_context
    from patterns import detect_patterns, BULLISH, BEARISH
    from continuation_patterns import detect_continuation
    from levels import nearest_levels
    factors = []
    direction_votes = {"LONG": 0, "SHORT": 0}

    # ── 1. Уровни (4h + 1h) ──────────────────────────────────────
    candles_4h = get_klines_any(symbol.replace("USDT", "/USDT"), "4h", limit=60)
    candles_1h = get_klines_any(symbol.replace("USDT", "/USDT"), "1h", limit=60)

    if not candles_1h or len(candles_1h) < 10:
        return None

    price = candles_1h[-1]["c"]
    s1_4h, r1_4h = nearest_levels(candles_4h, price) if candles_4h and len(candles_4h) > 10 else (None, None)
    s1_1h, r1_1h = nearest_levels(candles_1h, price) if candles_1h else (None, None)

    # Берём ближайшие уровни
    s1 = s1_1h
    r1 = r1_1h
    level_strength = 0

    # Проверяем близость к уровню (в пределах 0.5%)
    at_support = False
    at_resistance = False

    if s1 and abs(price - s1) / price < 0.005:
        at_support = True
        level_strength += 1
        direction_votes["LONG"] += 1
        # Совпадение 4h + 1h = сильнее
        if s1_4h and abs(s1 - s1_4h) / s1 < 0.01:
            level_strength += 1

    if r1 and abs(price - r1) / price < 0.005:
        at_resistance = True
        level_strength += 1
        direction_votes["SHORT"] += 1
        if r1_4h and abs(r1 - r1_4h) / r1 < 0.01:
            level_strength += 1

    if at_support or at_resistance:
        factors.append({
            "type": "level",
            "value": f"{'S1' if at_support else 'R1'} @ {s1 if at_support else r1}",
            "strength": level_strength,
        })

    # ── 2. Объём на уровне ────────────────────────────────────────
    if candles_1h and len(candles_1h) >= 5:
        curr_vol = candles_1h[-1].get("v", 0)
        avg_vol = sum(c.get("v", 0) for c in candles_1h[-6:-1]) / 5
        if avg_vol > 0 and curr_vol > avg_vol * 1.3:
            vol_ratio = round(curr_vol / avg_vol, 1)
            factors.append({
                "type": "volume",
                "value": f"×{vol_ratio}",
                "ratio": vol_ratio,
            })

    # ── 3. Multi-TF тренд (используем уже загруженные 4h и 1h) ──
    tf_bullish = 0
    tf_bearish = 0
    tf_results = {}

    # 1h тренд (последние 4 свечи)
    if candles_1h and len(candles_1h) >= 5:
        closes = [c["c"] for c in candles_1h[-4:]]
        if closes[-1] > closes[0]:
            tf_bullish += 1
            tf_results["1h"] = "🟢"
        else:
            tf_bearish += 1
            tf_results["1h"] = "🔴"

    # 4h тренд
    if candles_4h and len(candles_4h) >= 5:
        closes = [c["c"] for c in candles_4h[-4:]]
        if closes[-1] > closes[0]:
            tf_bullish += 1
            tf_results["4h"] = "🟢"
        else:
            tf_bearish += 1
            tf_results["4h"] = "🔴"

    # Дневной тренд из 4h свечей (последние 6×4h = 1 день)
    if candles_4h and len(candles_4h) >= 7:
        if candles_4h[-1]["c"] > candles_4h[-7]["c"]:
            tf_bullish += 1
            tf_results["1d"] = "🟢"
        else:
            tf_bearish += 1
            tf_results["1d"] = "🔴"

    total_tf = tf_bullish + tf_bearish
    if total_tf >= 2:
        if tf_bullish >= 2:
            factors.append({"type": "trend", "value": f"{tf_bullish}/{total_tf} bullish", "details": tf_results})
            direction_votes["LONG"] += 2
        elif tf_bearish >= 2:
            factors.append({"type": "trend", "value": f"{tf_bearish}/{total_tf} bearish", "details": tf_results})
            direction_votes["SHORT"] += 2

    # ── 4. Свечной паттерн на 1h ──────────────────────────────────
    # Определяем предварительное направление
    pre_direction = "LONG" if direction_votes["LONG"] >= direction_votes["SHORT"] else "SHORT"

    detected = detect_patterns(candles_1h, pre_direction)
    continuation = detect_continuation(candles_1h, pre_direction)
    all_patterns = detected + continuation

    if all_patterns:
        strongest = detected[0] if detected else continuation[0]
        factors.append({
            "type": "pattern",
            "value": strongest,
            "count": len(all_patterns),
        })
        if pre_direction == "LONG":
            direction_votes["LONG"] += 1
        else:
            direction_votes["SHORT"] += 1

    # ── 5. ETH корреляция ─────────────────────────────────────────
    try:
        eth_ctx = get_eth_market_context()
        eth_1h = eth_ctx.get("eth_1h", 0)
        # LONG + ETH растёт = попутный рынок
        if (pre_direction == "LONG" and eth_1h > 0.3) or (pre_direction == "SHORT" and eth_1h < -0.3):
            factors.append({
                "type": "eth_corr",
                "value": f"ETH {eth_1h:+.2f}% (попутный)",
                "eth_1h": eth_1h,
            })
            direction_votes[pre_direction] += 1
    except Exception:
        pass

    # ── 6. FTT подтверждение (из уже загруженных 1h свечей) ─────
    try:
        if candles_1h and len(candles_1h) >= 5:
            c = candles_1h[-1]
            o, h, l, cl = c["c"], c["h"], c["l"], c["c"]
            o = c["o"]
            body = abs(cl - o)
            full_range = h - l
            if full_range > 0 and body > 0:
                upper_wick = h - max(o, cl)
                lower_wick = min(o, cl) - l
                is_upper_ftt = upper_wick > body * 1.5 and upper_wick > full_range * 0.45
                is_lower_ftt = lower_wick > body * 1.5 and lower_wick > full_range * 0.45

                if is_upper_ftt or is_lower_ftt:
                    ftt_dir = "SHORT" if is_upper_ftt else "LONG"
                    wick = upper_wick if is_upper_ftt else lower_wick
                    wick_pct = round(wick / full_range, 2)

                    # Проверяем объём
                    prev_vols = [candles_1h[i].get("v", 0) for i in range(-5, -1)]
                    avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 1
                    vol_ratio = round(c.get("v", 0) / avg_vol, 1) if avg_vol > 0 else 0

                    ftt_score = 0
                    if wick_pct > 0.55: ftt_score += 1
                    if vol_ratio > 1.3: ftt_score += 1
                    if ftt_dir == pre_direction: ftt_score += 1

                    if ftt_score >= 2 and ftt_dir == pre_direction:
                        factors.append({
                            "type": "ftt",
                            "value": f"{ftt_dir} {ftt_score}/3",
                            "ftt_score": ftt_score,
                            "wick_ratio": wick_pct,
                            "vol_ratio": vol_ratio,
                        })
                        direction_votes[pre_direction] += 2
    except Exception:
        pass

    # ── Итог ──────────────────────────────────────────────────────
    score = len(factors)
    if score < 5:
        return None

    # Финальное направление
    direction = "LONG" if direction_votes["LONG"] > direction_votes["SHORT"] else "SHORT"
    if direction_votes["LONG"] == direction_votes["SHORT"]:
        direction = "NEUTRAL"

    # Сила: STRONG / MEDIUM
    strength = "STRONG" if score >= 5 else "MEDIUM"

    return {
        "symbol": symbol,
        "pair": symbol.replace("USDT", "/USDT"),
        "price": price,
        "score": score,
        "strength": strength,
        "direction": direction,
        "factors": factors,
        "s1": s1,
        "r1": r1,
        "trend_tf": tf_results,
        "pattern": all_patterns[0] if all_patterns else None,
    }


def scan_confluence_batch(symbols: list[str], min_score: int = 5) -> list[dict]:
    """Сканирует батч пар. Возвращает только score >= min_score."""
    results = []
    for s in symbols:
        try:
            r = scan_confluence(s)
            if r and r["score"] >= min_score:
                results.append(r)
        except Exception as e:
            logger.debug(f"Confluence {s}: {e}")
    return sorted(results, key=lambda x: -x["score"])
