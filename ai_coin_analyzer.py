"""AI Coin Analyzer — полный разбор монеты через Claude.

Собирает:
- Цены (текущая, 24h/7d/30d %, волатильность)
- Историю сигналов по всем источникам (WR, Avg R)
- Технику (RSI, MACD, BB, тренды на 3 TF, S/R)
- Контекст (funding, OI, BTC correlation)
- Конфликты / кластеры

Передаёт в Claude → структурированный JSON-вердикт.
"""
from __future__ import annotations
import json
import logging
from datetime import datetime, timedelta
from typing import Optional

from database import (_signals, _anomalies, _confluence, _clusters,
                       _conflicts, utcnow)
from exchange import get_klines_any, get_price_any

logger = logging.getLogger(__name__)


# ─── Technical indicators ───────────────────────────────────
def _rsi(closes: list[float], period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_g = sum(gains[-period:]) / period
    avg_l = sum(losses[-period:]) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return round(100 - 100 / (1 + rs), 1)


def _ema(values: list[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(values[:period]) / period
    for v in values[period:]:
        ema = v * k + ema * (1 - k)
    return ema


def _macd(closes: list[float]) -> Optional[dict]:
    if len(closes) < 35:
        return None
    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    if ema12 is None or ema26 is None:
        return None
    macd = ema12 - ema26
    # Simple signal line from last 9 macd values
    signals_line = [_ema(closes[:i + 1], 26) and (_ema(closes[:i + 1], 12) - _ema(closes[:i + 1], 26))
                    for i in range(len(closes) - 9, len(closes))]
    signals_line = [s for s in signals_line if s is not None]
    signal = sum(signals_line) / len(signals_line) if signals_line else 0
    hist = macd - signal
    return {
        "macd": round(macd, 4),
        "signal": round(signal, 4),
        "hist": round(hist, 4),
        "bullish": hist > 0,
    }


def _bb(closes: list[float], period: int = 20, mult: float = 2.0) -> Optional[dict]:
    if len(closes) < period:
        return None
    window = closes[-period:]
    mid = sum(window) / period
    var = sum((x - mid) ** 2 for x in window) / period
    std = var ** 0.5
    upper = mid + mult * std
    lower = mid - mult * std
    width = (upper - lower) / mid if mid else 0
    pos = (closes[-1] - lower) / (upper - lower) if upper != lower else 0.5
    return {
        "upper": round(upper, 6),
        "mid": round(mid, 6),
        "lower": round(lower, 6),
        "width_pct": round(width * 100, 2),
        "position_pct": round(pos * 100, 1),
    }


def _swing_sr(candles: list[dict], pivot_window: int = 5) -> dict:
    """Simple S/R from swing high/low."""
    if len(candles) < pivot_window * 2 + 1:
        return {"support": None, "resistance": None}
    last_price = candles[-1]["c"]
    highs, lows = [], []
    for i in range(pivot_window, len(candles) - pivot_window):
        h = candles[i]["h"]
        l = candles[i]["l"]
        left = candles[i - pivot_window:i]
        right = candles[i + 1:i + 1 + pivot_window]
        if all(c["h"] < h for c in left + right):
            highs.append(h)
        if all(c["l"] > l for c in left + right):
            lows.append(l)
    res = [h for h in sorted(highs) if h > last_price]
    sup = [l for l in sorted(lows, reverse=True) if l < last_price]
    return {
        "support": round(sup[0], 6) if sup else None,
        "resistance": round(res[0], 6) if res else None,
    }


def _trend_direction(candles: list[dict]) -> str:
    if len(candles) < 20:
        return "unknown"
    ema20 = _ema([c["c"] for c in candles], 20)
    ema50 = _ema([c["c"] for c in candles], 50) if len(candles) >= 50 else None
    last = candles[-1]["c"]
    if ema50 is None:
        return "up" if last > (ema20 or 0) else "down"
    if last > ema20 > ema50:
        return "strong_up"
    if last < ema20 < ema50:
        return "strong_down"
    if last > ema20 and ema20 < ema50:
        return "recovery"
    return "sideways"


# ─── Signal history ─────────────────────────────────────────
def _history_stats(pair: str, days: int = 30) -> dict:
    """Собирает статистику по сигналам для пары за период."""
    norm_sym = pair.replace("/", "").upper()
    norm_pair = pair.upper() if "/" in pair else pair[:-4].upper() + "/USDT"
    since = utcnow() - timedelta(days=days)
    out = {}

    # Tradium
    tr_docs = list(_signals().find({
        "source": "tradium", "pair": {"$in": [norm_pair, norm_sym]},
        "received_at": {"$gte": since}
    }))
    out["tradium"] = {
        "count": len(tr_docs),
        "triggered": sum(1 for s in tr_docs if s.get("pattern_triggered")),
        "latest": max((s.get("received_at") for s in tr_docs if s.get("received_at")), default=None),
    }

    # Cryptovizor
    cv_docs = list(_signals().find({
        "source": "cryptovizor", "pair": {"$in": [norm_pair, norm_sym]},
        "received_at": {"$gte": since}
    }))
    out["cryptovizor"] = {
        "count": len(cv_docs),
        "triggered": sum(1 for s in cv_docs if s.get("pattern_triggered")),
    }

    # Anomaly
    anom_docs = list(_anomalies().find({
        "$or": [{"symbol": norm_sym}, {"pair": norm_pair}],
        "detected_at": {"$gte": since}
    }))
    out["anomaly"] = {
        "count": len(anom_docs),
        "avg_score": round(sum(a.get("score", 0) for a in anom_docs) / len(anom_docs), 1) if anom_docs else 0,
        "longs": sum(1 for a in anom_docs if a.get("direction") == "LONG"),
        "shorts": sum(1 for a in anom_docs if a.get("direction") == "SHORT"),
    }

    # Confluence
    conf_docs = list(_confluence().find({
        "$or": [{"symbol": norm_sym}, {"pair": norm_pair}],
        "detected_at": {"$gte": since}
    }))
    out["confluence"] = {
        "count": len(conf_docs),
        "strong": sum(1 for c in conf_docs if c.get("score", 0) >= 5),
        "medium": sum(1 for c in conf_docs if c.get("score", 0) == 4),
        "longs": sum(1 for c in conf_docs if c.get("direction") == "LONG"),
        "shorts": sum(1 for c in conf_docs if c.get("direction") == "SHORT"),
    }

    # Clusters
    cl_docs = list(_clusters().find({
        "$or": [{"symbol": norm_sym}, {"pair": norm_pair}],
        "trigger_at": {"$gte": since}
    }))
    tp = sum(1 for c in cl_docs if c.get("status") == "TP")
    sl = sum(1 for c in cl_docs if c.get("status") == "SL")
    out["clusters"] = {
        "count": len(cl_docs),
        "tp": tp,
        "sl": sl,
        "open": sum(1 for c in cl_docs if c.get("status") == "OPEN"),
        "wr": round(tp / (tp + sl) * 100, 1) if (tp + sl) > 0 else None,
        "latest_direction": cl_docs[-1]["direction"] if cl_docs else None,
    }

    # Conflicts (anti-cluster)
    cfl_docs = list(_conflicts().find({
        "$or": [{"symbol": norm_sym}, {"pair": norm_pair}],
        "detected_at": {"$gte": since}
    }))
    out["conflicts"] = {
        "count": len(cfl_docs),
        "nuclear": sum(1 for c in cfl_docs if c.get("severity") == "nuclear"),
        "strong": sum(1 for c in cfl_docs if c.get("severity") == "strong"),
    }

    return out


# ─── Market context ─────────────────────────────────────────
def _funding_rate(pair: str) -> Optional[float]:
    """Текущий funding rate с Binance futures."""
    import httpx
    sym = pair.replace("/", "").upper()
    try:
        r = httpx.get(
            "https://fapi.binance.com/fapi/v1/premiumIndex",
            params={"symbol": sym}, timeout=10
        )
        d = r.json()
        return round(float(d.get("lastFundingRate", 0)) * 100, 4)  # в %
    except Exception:
        return None


def _open_interest(pair: str) -> Optional[float]:
    import httpx
    sym = pair.replace("/", "").upper()
    try:
        r = httpx.get(
            "https://fapi.binance.com/fapi/v1/openInterest",
            params={"symbol": sym}, timeout=10
        )
        d = r.json()
        return round(float(d.get("openInterest", 0)), 0)
    except Exception:
        return None


def _btc_correlation(pair: str, days: int = 7) -> Optional[float]:
    if pair.startswith("BTC"):
        return 1.0
    try:
        c1 = get_klines_any(pair, "1h", 24 * days)
        c2 = get_klines_any("BTC/USDT", "1h", 24 * days)
        if not c1 or not c2 or len(c1) < 24 or len(c2) < 24:
            return None
        # Нормализованные изменения
        n = min(len(c1), len(c2))
        p1 = [c["c"] for c in c1[-n:]]
        p2 = [c["c"] for c in c2[-n:]]
        returns1 = [(p1[i] - p1[i - 1]) / p1[i - 1] for i in range(1, n)]
        returns2 = [(p2[i] - p2[i - 1]) / p2[i - 1] for i in range(1, n)]
        mean1 = sum(returns1) / len(returns1)
        mean2 = sum(returns2) / len(returns2)
        cov = sum((returns1[i] - mean1) * (returns2[i] - mean2) for i in range(len(returns1)))
        var1 = sum((r - mean1) ** 2 for r in returns1)
        var2 = sum((r - mean2) ** 2 for r in returns2)
        if var1 * var2 == 0:
            return None
        return round(cov / ((var1 * var2) ** 0.5), 2)
    except Exception:
        return None


# ─── Main analyzer ──────────────────────────────────────────
def collect_analysis_data(pair: str) -> dict:
    """Собирает все данные для AI-анализа монеты."""
    pair = pair.upper()
    if "/" not in pair and pair.endswith("USDT"):
        pair_display = pair[:-4] + "/USDT"
        sym = pair
    elif "/" in pair:
        pair_display = pair
        sym = pair.replace("/", "")
    else:
        pair_display = pair + "/USDT"
        sym = pair + "USDT"

    # Price data
    price_now = get_price_any(pair_display)
    c1h = get_klines_any(pair_display, "1h", 200)
    c4h = get_klines_any(pair_display, "4h", 100)
    c1d = get_klines_any(pair_display, "1d", 30)

    # Price changes
    changes = {}
    if c1h and len(c1h) > 24:
        changes["24h_pct"] = round((c1h[-1]["c"] - c1h[-25]["c"]) / c1h[-25]["c"] * 100, 2)
    if c1h and len(c1h) > 168:
        changes["7d_pct"] = round((c1h[-1]["c"] - c1h[-169]["c"]) / c1h[-169]["c"] * 100, 2)
    if c1d and len(c1d) >= 30:
        changes["30d_pct"] = round((c1d[-1]["c"] - c1d[-30]["c"]) / c1d[-30]["c"] * 100, 2)

    # Technicals
    closes_1h = [c["c"] for c in c1h] if c1h else []
    closes_4h = [c["c"] for c in c4h] if c4h else []
    closes_1d = [c["c"] for c in c1d] if c1d else []

    tech = {
        "rsi_1h": _rsi(closes_1h),
        "rsi_4h": _rsi(closes_4h),
        "rsi_1d": _rsi(closes_1d),
        "macd_1h": _macd(closes_1h),
        "macd_4h": _macd(closes_4h),
        "bb_1h": _bb(closes_1h),
        "bb_4h": _bb(closes_4h),
        "sr_1h": _swing_sr(c1h) if c1h else {},
        "sr_4h": _swing_sr(c4h) if c4h else {},
        "trend_1h": _trend_direction(c1h) if c1h else "unknown",
        "trend_4h": _trend_direction(c4h) if c4h else "unknown",
        "trend_1d": _trend_direction(c1d) if c1d else "unknown",
    }

    # Volatility — 7d ATR% от средней цены
    if c1h and len(c1h) >= 168:
        recent = c1h[-168:]
        tr = [max(c["h"] - c["l"], abs(c["h"] - c["c"]), abs(c["l"] - c["c"])) for c in recent]
        avg_tr = sum(tr) / len(tr)
        atr_pct = round(avg_tr / price_now * 100, 3) if price_now else None
    else:
        atr_pct = None

    # Signals history
    history = _history_stats(pair_display, days=30)

    # Context
    funding = _funding_rate(pair_display)
    oi = _open_interest(pair_display)
    btc_corr = _btc_correlation(pair_display)

    return {
        "pair": pair_display,
        "symbol": sym,
        "price": price_now,
        "changes": changes,
        "atr_pct": atr_pct,
        "technicals": tech,
        "history_30d": history,
        "context": {
            "funding_pct": funding,
            "open_interest": oi,
            "btc_correlation": btc_corr,
        },
        "collected_at": utcnow().isoformat(),
    }


def _build_prompt(data: dict) -> str:
    """Строит prompt для Claude."""
    d = data
    t = d["technicals"]
    h = d["history_30d"]
    ctx = d["context"]
    ch = d["changes"]

    return f"""Ты профессиональный крипто-трейдер. Проанализируй монету {d['pair']} на основе данных ниже и выдай СТРУКТУРИРОВАННЫЙ JSON-вердикт.

═══ ТЕКУЩАЯ ЦЕНА ═══
Цена: {d['price']}
Изменения: 24h {ch.get('24h_pct','?')}% · 7d {ch.get('7d_pct','?')}% · 30d {ch.get('30d_pct','?')}%
Волатильность (ATR 7d): {d.get('atr_pct','?')}%

═══ ТЕХНИКА ═══
RSI:  1h={t.get('rsi_1h','?')} · 4h={t.get('rsi_4h','?')} · 1d={t.get('rsi_1d','?')}
MACD 1h: {t.get('macd_1h',{}).get('bullish','?') and 'BULLISH' or 'BEARISH'} (hist {t.get('macd_1h',{}).get('hist','?')})
MACD 4h: {t.get('macd_4h',{}).get('bullish','?') and 'BULLISH' or 'BEARISH'}
BB 1h position: {t.get('bb_1h',{}).get('position_pct','?')}% (squeeze width {t.get('bb_1h',{}).get('width_pct','?')}%)
Тренд: 1h={t.get('trend_1h')} · 4h={t.get('trend_4h')} · 1d={t.get('trend_1d')}
Уровни 4h: S={t.get('sr_4h',{}).get('support','?')}, R={t.get('sr_4h',{}).get('resistance','?')}

═══ НАША ИСТОРИЯ (30 дней) ═══
📡 Tradium: {h.get('tradium',{}).get('count',0)} сигналов ({h.get('tradium',{}).get('triggered',0)} trigger'нутых)
🚀 CV: {h.get('cryptovizor',{}).get('count',0)} сигналов
⚠ Anomaly: {h.get('anomaly',{}).get('count',0)} (avg score {h.get('anomaly',{}).get('avg_score','?')}, {h.get('anomaly',{}).get('longs',0)}L/{h.get('anomaly',{}).get('shorts',0)}S)
🎯 Confluence: {h.get('confluence',{}).get('count',0)} (⭐{h.get('confluence',{}).get('strong',0)} STRONG, {h.get('confluence',{}).get('medium',0)} MEDIUM)
💠 Кластеры: {h.get('clusters',{}).get('count',0)} (TP {h.get('clusters',{}).get('tp',0)} / SL {h.get('clusters',{}).get('sl',0)}, WR {h.get('clusters',{}).get('wr','?')}%)
⚠ Anti-cluster конфликтов: {h.get('conflicts',{}).get('count',0)} ({h.get('conflicts',{}).get('nuclear',0)} nuclear)

═══ РЫНОЧНЫЙ КОНТЕКСТ ═══
Funding rate: {ctx.get('funding_pct','?')}% (>0 = longs платят, перегрев; <0 = shorts платят)
Open Interest: {ctx.get('open_interest','?')}
Корреляция с BTC (7d): {ctx.get('btc_correlation','?')}

═══ ТВОЯ ЗАДАЧА ═══
Выдай JSON (БЕЗ markdown, БЕЗ комментариев вне JSON) вот в таком формате:

{{
  "verdict": "BULLISH" | "BEARISH" | "NEUTRAL",
  "confidence": <число 0-100>,
  "bullish_score": <0-100>,
  "bearish_score": <0-100>,
  "key_factors": ["фактор1", "фактор2", "фактор3"],
  "risks": ["риск1", "риск2"],
  "our_system_read": "короткий текст о том что говорит наша статистика",
  "recommended_strategy": "короткий план действий",
  "entry_zone": {{"low": <число>, "high": <число>}},
  "tp_target": <число>,
  "sl_level": <число>,
  "time_horizon": "scalp (часы)" | "swing (1-3 дня)" | "position (неделя+)",
  "summary": "1-2 предложения общий вывод"
}}

ВАЖНО:
- Числа в entry_zone/tp_target/sl_level — конкретные цены с той же точностью что текущая цена
- bullish_score + bearish_score не обязательно = 100, это независимые оценки силы каждой стороны
- verdict выбирай по большей оценке; NEUTRAL если разница < 15
- Если НЕ рекомендуешь заход — всё равно выдай entry_zone/tp/sl как гипотетические
- Учитывай нашу историю (WR кластеров) как сильный фактор"""


def analyze_with_ai(pair: str) -> dict:
    """Полный pipeline: собираем данные + вызываем Claude + парсим JSON."""
    data = collect_analysis_data(pair)
    prompt = _build_prompt(data)

    try:
        from ai_analyzer import client
        from config import ANTHROPIC_MODEL
        msg = client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=1200,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip() if msg.content else ""
        # Извлекаем JSON из ответа
        if raw.startswith("```"):
            raw = raw.strip("`").lstrip("json").strip()
        # Найти первое { ... }
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            raw = raw[start:end + 1]
        verdict = json.loads(raw)
    except Exception as e:
        logger.exception(f"[ai_coin_analyzer] claude call failed: {e}")
        verdict = {
            "verdict": "NEUTRAL",
            "confidence": 0,
            "bullish_score": 50,
            "bearish_score": 50,
            "key_factors": [],
            "risks": [f"AI анализ недоступен: {str(e)[:100]}"],
            "our_system_read": "",
            "recommended_strategy": "",
            "entry_zone": {"low": data["price"], "high": data["price"]},
            "tp_target": data["price"],
            "sl_level": data["price"],
            "time_horizon": "unknown",
            "summary": "AI unavailable",
            "_error": str(e),
        }

    return {
        "data": data,
        "verdict": verdict,
    }
