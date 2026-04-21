"""Определение фазы рынка по BTC/крипто-метрикам.

6 фаз:
  🚀 BULL_TREND     — BTC 4h/1d = UP, ATR% >= 1.5
  📉 BEAR_TREND     — BTC 4h/1d = DOWN, ATR% >= 1.5
  🌀 CHOP            — ATR% < 0.6, флет или конфликт TF
  ⚡ VOLATILE        — ATR% >= 2.5 + конфликт TF (whipsaw)
  🔥 EUPHORIA        — avg funding >= +0.04% + BTC 4h UP
  💀 CAPITULATION    — avg funding <= -0.04% + BTC 4h DOWN
  ❓ NEUTRAL         — ничего из перечисленного (дефолт)

Использует существующую инфру:
  — supertrend_state(BTC, 1h/4h/1d)
  — anomaly_scanner._batch_cache (funding по ~500 парам)
  — get_klines_any для ATR и ETH/BTC

Cache 120s чтобы не грузить ST пересчёты на каждый UI-запрос.
"""
from __future__ import annotations
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

_cache: dict = {}
_CACHE_TTL = 120.0  # 2 мин


PHASE_META = {
    "BULL_TREND":    {"emoji": "🚀", "color": "#00e5a0", "label": "Bull Trend"},
    "BEAR_TREND":    {"emoji": "📉", "color": "#ff4d6d", "label": "Bear Trend"},
    "CHOP":          {"emoji": "🌀", "color": "#7a8ba6", "label": "Chop / Range"},
    "VOLATILE":      {"emoji": "⚡", "color": "#ffa94d", "label": "Volatile"},
    "EUPHORIA":      {"emoji": "🔥", "color": "#ff6b6b", "label": "Euphoria"},
    "CAPITULATION":  {"emoji": "💀", "color": "#8a2be2", "label": "Capitulation"},
    "NEUTRAL":       {"emoji": "❓", "color": "#7a8ba6", "label": "Neutral"},
}

# Рекомендации по фазе (из результатов 7-дневного бектеста)
PHASE_RECOMMENDATIONS = {
    "BULL_TREND": {
        "take": ["MTF LONG (btc_up)", "Confluence factors≥5", "CV Перевёрнутый молот", "ai_score≥60"],
        "avoid": ["VIP SHORT", "CV SHORT", "Confluence SHORT (в сильном тренде)"],
    },
    "BEAR_TREND": {
        "take": ["VIP SHORT", "Confluence SHORT", "Cluster RISKY", "MTF LONG+btc_up (для контр-отскоков)"],
        "avoid": ["MTF SHORT", "CV SHORT (всегда)", "Cluster STRONG", "VIP LONG"],
    },
    "CHOP": {
        "take": ["Cluster MEGA/STRONG (малый размер)", "CV Перевёрнутый молот (≥60 score)"],
        "avoid": ["ST-сигналы любые (whipsaw)", "Confluence без factors≥5"],
    },
    "VOLATILE": {
        "take": ["Только VIP с двойным подтверждением", "Cluster MEGA"],
        "avoid": ["MTF", "CV (все паттерны)", "Anomaly без taker/OI"],
    },
    "EUPHORIA": {
        "take": ["Готовиться к SHORT", "Закрывать LONG с профитом"],
        "avoid": ["Открывать новые LONG", "Увеличивать плечо"],
    },
    "CAPITULATION": {
        "take": ["LONG на отскок (маленькие сайзы)", "Cluster если появится"],
        "avoid": ["SHORT (все)", "Пирамидинг"],
    },
    "NEUTRAL": {
        "take": ["Стандартные фильтры по каждому источнику"],
        "avoid": ["CV SHORT всегда", "MTF SHORT всегда"],
    },
}


def _btc_atr_pct(tf: str = "1h", period: int = 14) -> Optional[float]:
    """ATR% от текущей цены на BTC."""
    try:
        from exchange import get_klines_any
        candles = get_klines_any("BTC/USDT", tf, period + 5)
        if not candles or len(candles) < period + 2:
            return None
        trs = []
        for i in range(1, len(candles)):
            c, pc = candles[i], candles[i-1]["c"]
            trs.append(max(c["h"] - c["l"], abs(c["h"] - pc), abs(c["l"] - pc)))
        atr = sum(trs[-period:]) / period
        price = candles[-1]["c"]
        return round(atr / price * 100, 3) if price else None
    except Exception:
        logger.debug("[market-phase] btc_atr fail", exc_info=True)
        return None


def _avg_funding_top_pairs(n: int = 30) -> Optional[float]:
    """Средний funding по топ-N парам (из anomaly_scanner batch-кеша)."""
    try:
        from anomaly_scanner import _batch_cache
        funding_map = _batch_cache.get("funding") or {}
        if not funding_map:
            return None
        # Берём топ-N ключей (не важно какие — scan_all уже даёт активные)
        vals = list(funding_map.values())[:n] if isinstance(funding_map, dict) else []
        if not vals:
            return None
        return round(sum(vals) / len(vals), 4)
    except Exception:
        logger.debug("[market-phase] avg_funding fail", exc_info=True)
        return None


def _eth_btc_ratio_1h() -> Optional[float]:
    """ETH vs BTC 1h% — altseason индикатор. Возвращает (eth_1h% - btc_1h%)."""
    try:
        from exchange import get_eth_market_context
        ctx = get_eth_market_context()
        eth = ctx.get("eth_1h", 0) or 0
        btc = ctx.get("btc_1h", 0) or 0
        return round(eth - btc, 3)
    except Exception:
        return None


def _btc_st_states() -> dict:
    """ST 1h/4h/1d для BTC. Возвращает {'1h': 'UP'|'DOWN'|'UNK', ...}."""
    from supertrend import supertrend_state
    out = {}
    for tf in ("1h", "4h", "1d"):
        try:
            st = supertrend_state("BTC/USDT", tf, cache_only=False)
            if st and st.get("state"):
                out[tf] = st["state"]
            else:
                out[tf] = "UNK"
        except Exception:
            out[tf] = "UNK"
    return out


def _classify(metrics: dict) -> tuple[str, int]:
    """Возвращает (phase, confidence 0-100)."""
    btc_st = metrics.get("btc_st", {})
    btc_1h = btc_st.get("1h", "UNK")
    btc_4h = btc_st.get("4h", "UNK")
    btc_1d = btc_st.get("1d", "UNK")
    atr_pct = metrics.get("atr_1h_pct") or 0
    funding = metrics.get("avg_funding") or 0
    eth_btc = metrics.get("eth_btc_diff") or 0

    # 1. EUPHORIA — funding extreme + BTC UP
    if funding >= 0.04 and btc_4h == "UP":
        conf = min(100, int(50 + funding * 500))  # 0.04 → 70%, 0.08 → 90%
        return "EUPHORIA", conf

    # 2. CAPITULATION — funding extreme low + BTC DOWN
    if funding <= -0.04 and btc_4h == "DOWN":
        conf = min(100, int(50 + abs(funding) * 500))
        return "CAPITULATION", conf

    # 3. VOLATILE — высокий ATR + TF конфликт
    tfs_match = sum(1 for a, b in [(btc_1h, btc_4h), (btc_4h, btc_1d), (btc_1h, btc_1d)]
                    if a != "UNK" and b != "UNK" and a == b)
    # tfs_match: 3 = все совпадают, 0-2 = конфликт
    if atr_pct >= 2.5 and tfs_match <= 1:
        conf = min(100, int(50 + atr_pct * 10))
        return "VOLATILE", conf

    # 4. BULL TREND — 4h+1d UP и хоть какой тренд
    if btc_4h == "UP" and btc_1d == "UP":
        base = 60
        if btc_1h == "UP": base += 15
        if atr_pct >= 1.5: base += 15
        if eth_btc > 0: base += 10   # альты тянут вверх
        return "BULL_TREND", min(100, base)

    # 5. BEAR TREND
    if btc_4h == "DOWN" and btc_1d == "DOWN":
        base = 60
        if btc_1h == "DOWN": base += 15
        if atr_pct >= 1.5: base += 15
        if eth_btc < 0: base += 10
        return "BEAR_TREND", min(100, base)

    # 6. CHOP — низкий ATR или TF конфликт
    if atr_pct < 0.6 or tfs_match <= 1:
        conf = 55 if atr_pct < 0.6 else 45
        return "CHOP", conf

    # 7. NEUTRAL — всё неопределённо
    return "NEUTRAL", 30


def get_market_phase(force_refresh: bool = False) -> dict:
    """Возвращает {phase, confidence, emoji, label, color, metrics, recommended, avoid}.
    Кешируется 120с.
    """
    now = time.time()
    cached = _cache.get("phase")
    if cached and not force_refresh and (now - cached[0]) < _CACHE_TTL:
        return cached[1]

    btc_st = _btc_st_states()
    atr_pct = _btc_atr_pct("1h")
    avg_funding = _avg_funding_top_pairs()
    eth_btc = _eth_btc_ratio_1h()

    metrics = {
        "btc_st": btc_st,
        "atr_1h_pct": atr_pct,
        "avg_funding": avg_funding,
        "eth_btc_diff": eth_btc,
    }
    phase, confidence = _classify(metrics)
    meta = PHASE_META.get(phase, PHASE_META["NEUTRAL"])
    rec = PHASE_RECOMMENDATIONS.get(phase, PHASE_RECOMMENDATIONS["NEUTRAL"])

    result = {
        "phase": phase,
        "confidence": confidence,
        "emoji": meta["emoji"],
        "label": meta["label"],
        "color": meta["color"],
        "metrics": metrics,
        "recommended": rec["take"],
        "avoid": rec["avoid"],
        "computed_at": now,
    }
    _cache["phase"] = (now, result)
    return result


def record_phase_change(new_phase: str, confidence: int, metrics: dict) -> bool:
    """Записывает смену фазы в коллекцию market_phases. Возвращает True если записано."""
    try:
        from database import _get_db, utcnow
        db = _get_db()
        col = db.market_phases
        last = col.find_one({}, sort=[("at", -1)])
        if last and last.get("phase") == new_phase:
            return False  # не изменилась
        col.insert_one({
            "at": utcnow(),
            "phase": new_phase,
            "confidence": confidence,
            "metrics": metrics,
            "prev_phase": (last or {}).get("phase"),
        })
        return True
    except Exception:
        logger.exception("[market-phase] record fail")
        return False


def get_phase_history(hours: int = 24) -> list:
    try:
        from database import _get_db, utcnow
        from datetime import timedelta
        since = utcnow() - timedelta(hours=hours)
        col = _get_db().market_phases
        docs = list(col.find({"at": {"$gte": since}}).sort("at", -1).limit(100))
        for d in docs:
            d.pop("_id", None)
            if hasattr(d.get("at"), "isoformat"):
                d["at"] = d["at"].isoformat()
        return docs
    except Exception:
        return []
