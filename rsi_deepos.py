# -*- coding: utf-8 -*-
"""🤿 RSI-дно — выход из глубокой перепроданности (LONG, 2h/4h).

Грид-бэктест 16.09.26 (год, 308 пар, 6 ТФ × 8 правил RSI, исходы
TP+10/SL−5/96ч, эдж = avgR − бейзлайн случайного входа того же ТФ):
  2h: avgR +0.31 · WR 39 · эдж +0.89 — КРУПНЕЙШИЙ эдж всего
      исследования MSO+RSI; единственный лонг в абсолютном плюсе на
      году, где случайный лонг терял −0.59 · половины +0.29/+0.33
  4h: avgR +0.23 · WR 39 · эдж +0.69 · половины +0.37/+0.09
  Остальные RSI-правила (кроссы 30/50/70/80, продолжения) — шум или
  минус; шорты по RSI не работают ни на одном ТФ.

Событие: RSI(14) на последнем ЗАКРЫТОМ баре ТФ кроссит ВВЕРХ через 20
(prev < 20 <= cur). Вход по close бара. Дедуп — конкретный бар.
Окно расчёта 300 баров (Wilder-рекурсия к 300-му бару сходится до нуля
разницы с полной серией — оконного смещения нет)."""
from __future__ import annotations

import asyncio
import logging
import math

logger = logging.getLogger(__name__)

STABLE_BASES = {"USDC", "FDUSD", "TUSD", "DAI", "USD1", "USDP", "EURI",
                "AEUR", "XUSD", "PAXG", "XAUT", "WBTC", "BFUSD", "USDE",
                "BUSD", "EUR"}
TF_H = {"2h": 2, "4h": 4}
WINDOW = 300
LEVEL = 20.0
PERIOD = 14
EV_TXT = {"2h": "+0.50%/вход с гейтом «1d не UP» · WR 40 (лучший лонг)",
          "4h": "+0.26%/вход с гейтом «1d не UP» · WR 39"}


def ema_trend(closes: list[float]) -> str:
    """Тренд последнего бара: EMA20 vs EMA50, полоса 0.05% (= trend_cache)."""
    n = len(closes)
    if n < 50:
        return "NA"

    def _ema(period):
        e = sum(closes[:period]) / period
        k = 2 / (period + 1)
        for i in range(period, n):
            e = closes[i] * k + e * (1 - k)
        return e

    e20, e50 = _ema(20), _ema(50)
    if abs(e20 - e50) / max(closes[-1], 1e-12) * 100 < 0.05:
        return "FLAT"
    return "UP" if e20 > e50 else "DOWN"


def rsi_series(closes: list[float], period: int = PERIOD) -> list[float]:
    """Классический Wilder RSI(14) по закрытым барам."""
    nan = float("nan")
    n = len(closes)
    out = [nan] * n
    if n < period + 1:
        return out
    au = ad = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        if d > 0:
            au += d
        else:
            ad -= d
    au /= period
    ad /= period
    out[period] = 100 - 100 / (1 + (au / ad if ad > 0 else float("inf")))
    for i in range(period + 1, n):
        d = closes[i] - closes[i - 1]
        up = d if d > 0 else 0.0
        dn = -d if d < 0 else 0.0
        au = (au * (period - 1) + up) / period
        ad = (ad * (period - 1) + dn) / period
        out[i] = 100 - 100 / (1 + (au / ad if ad > 0 else float("inf")))
    return out


def detect_deepos(candles: list[dict], tf: str, now_ms: float):
    """Кросс RSI вверх через 20 на последнем ЗАКРЫТОМ баре ТФ."""
    if not candles or len(candles) < 60:
        return None
    c = candles[-WINDOW:]
    tf_ms = TF_H[tf] * 3600_000
    idx = len(c) - 1
    if c[idx]["t"] + tf_ms > now_ms + 60_000:
        idx -= 1
    if idx < 40:
        return None
    rsi = rsi_series([x["c"] for x in c[:idx + 1]])
    p, q = rsi[idx - 1], rsi[idx]
    if math.isnan(p) or math.isnan(q) or not (p < LEVEL <= q):
        return None
    return {"bar": c[idx], "bar_close_ms": c[idx]["t"] + tf_ms,
            "rsi_prev": round(p, 1), "rsi_now": round(q, 1)}


async def _pair(pair_norm: str, tf: str) -> bool:
    if pair_norm[:-4] in STABLE_BASES:
        return False
    from database import _get_db, utcnow
    db = _get_db()
    try:
        pc = db.pair_context.find_one({"_id": pair_norm}, {"vitality": 1})
        if pc and pc.get("vitality") == "dead":
            return False
    except Exception:
        pass
    from exchange import get_klines_any
    pair_slash = pair_norm[:-4] + "/USDT"
    try:
        c = await asyncio.to_thread(get_klines_any, pair_slash, tf, WINDOW)
    except Exception:
        return False
    now = utcnow()
    ev = detect_deepos(c, tf, now.timestamp() * 1000)
    if not ev:
        return False
    tf_ms = TF_H[tf] * 3600_000
    if now.timestamp() * 1000 - ev["bar_close_ms"] > 1.25 * tf_ms:
        return False
    # 🚧 гейт 16.09 (тройной бэктест тренд×MSO×RSI): при дневном UP
    # RSI-дно УБЫТОЧНО (−0.81: перепроданность в дневном апе = слом, не
    # отскок); при 1d DOWN +0.50, NA +0.30 — сигналим только «1d не UP»
    try:
        d1 = await asyncio.to_thread(get_klines_any, pair_slash, "1d", 120)
        if d1 and d1[-1]["t"] + 86_400_000 > now.timestamp() * 1000 + 60_000:
            d1 = d1[:-1]
        d1_trend = ema_trend([x["c"] for x in (d1 or [])])
    except Exception:
        d1_trend = "NA"
    if d1_trend == "UP":
        return False
    entry = ev["bar"]["c"]
    bar_t = int(ev["bar"]["t"])
    dup = db.new_strategy_signals.find_one({
        "strategy": "rsi_deepos", "symbol": pair_norm,
        "indicators.tf": tf, "indicators.bar_t": bar_t})
    if dup:
        return False
    sig = {
        "strategy": "rsi_deepos", "direction": "LONG",
        "pair": pair_slash, "symbol": pair_norm,
        "entry": entry,
        "tp": entry * 1.10,
        "sl": entry * 0.95,
        "horizon_h": 96,
        "indicators": {"tf": tf, "bar_t": bar_t,
                       "rsi_prev": ev["rsi_prev"], "rsi_now": ev["rsi_now"],
                       "trend_1d": d1_trend, "close": entry},
    }
    from impulse_detector import store_signal
    stored = await asyncio.to_thread(store_signal, sig, 1)
    if not stored:
        return False
    try:
        from watcher import _bot16
        from config import WHALE_CHAT_ID
        if _bot16 and WHALE_CHAT_ID:
            txt = (f"🤿 <b>RSI-ДНО {tf} · {pair_slash.replace('/USDT', '')}</b>\n"
                   f"🟢 LONG — выход из глубокой перепроданности "
                   f"(RSI {ev['rsi_prev']} → {ev['rsi_now']}, порог {LEVEL:.0f}) · "
                   f"тренд 1d {d1_trend} ✓\n"
                   f"вход {entry:.6g} по закрытию {tf}-бара\n"
                   f"<i>бэктест год: {EV_TXT[tf]} · обе половины в плюсе · "
                   f"дно при дневном UP убыточно (−0.81) — такие отрезаны</i>")
            try:
                from setup_checker import signal_tg_context
                txt += await asyncio.to_thread(
                    signal_tg_context, pair_slash, "LONG")
            except Exception:
                pass
            await _bot16.send_message(WHALE_CHAT_ID, txt, parse_mode="HTML")
    except Exception:
        logger.debug(f"[rsi-deepos] tg fail {pair_norm}", exc_info=True)
    return True


async def check_all(tf: str) -> int:
    """Скан всех tracked-пар. Вызывать после закрытия бара ТФ."""
    from supertrend_tracker import get_tracked_pairs
    pairs = await asyncio.to_thread(get_tracked_pairs)
    fired = 0
    for i, p in enumerate(pairs):
        try:
            if await _pair(p, tf):
                fired += 1
        except Exception:
            logger.debug(f"[rsi-deepos] {p} {tf} fail", exc_info=True)
        if i % 20 == 19:
            await asyncio.sleep(0.5)
    logger.info(f"[rsi-deepos] {tf}: {fired} сигналов")
    return fired
