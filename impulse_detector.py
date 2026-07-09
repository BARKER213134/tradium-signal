"""🚀💥🎣 IMPULSE / IGNITION / FADE — сигналы из research 2026-07-02/06.

Исследование: 240 пар × 62 дня × 1h (295k баров, 48 индикаторов),
single-factor скан → перебор комбо (C(18,2..3) × 2 стороны) → 12h-кулдаун
дедуп → сетка выходов → OOS train/test (45д/17д) → недельная стабильность
→ проверка концентрации по парам.

═══ 🚀 IMPULSE (LONG, momentum continuation) ═══
Условия (все одновременно, на закрытии 1h-бара):
  • RSI14(4h) > 70          — старший ТФ перегрет = сильный тренд, НЕ разворот
  • RSI14(1d) > 65          — дневной моментум подтверждает
  • SuperTrend(10,3) 4h UP  — структурный аптренд
  • ATR%(1h) 0.7–6.0        — живая пара (не стейбл, не хаос)
Выход: TP +8% / SL −4%, тайм-стоп 12ч (закрытие по рынку).
Бэктест: WR 82% (n=102), EV +5.9%/сделку; OOS 84%→77%; все 8 недель
WR 71-94%; 66 уникальных пар, без топ-3 пар WR 81%. ~2-3 сигнала/день.
Медианный MFE24 +17.7% при MAE −2.0%.

═══ 💥 IGNITION (LONG, ранний вход в начале роста) ═══
Research 2026-07-06 по фидбеку «сигнал всегда приходит после роста»:
IMPULSE стреляет когда цена уже +16% от суточного лоу (медиана). IGNITION
ловит момент «зажигания» моментума — вход на ~+5% от лоу, в 3 раза раньше.
Условия (все одновременно, на закрытии 1h-бара):
  • RSI14(4h) в зоне 58–68     — моментум разогрелся, но НЕ перегрет
  • RSI14(4h) прошлой 4h-свечи < 55 — кросс только что произошёл
  • SuperTrend(10,3) 4h UP     — структурный аптренд
  • EMA10(1d) > EMA20(1d)      — дневной тренд вверх
  • ATR%(1h) 0.7–6.0
Выход: TP +6% / SL −3%, тайм-стоп 24ч.
Бэктест 52д: WR 70% (n=167, dedup 12h), EV +2.75%/сделку; OOS 70%→68%;
8/8 недель > breakeven; 82 пары, топ-пара 6.4%; медиана MFE12 +7.05% при
MAE24 −1.67%. Соседние пороги (55-70/60-66, prev<52..58) дают WR 60-84% —
не knife-edge. В 32/171 случаев за IGNITION в ≤48ч следует IMPULSE на той
же паре (медиана опережения 12ч).

═══ 🎣 FADE (SHORT, отскок в даунтренде) ═══
Условия:
  • RSI14(1d) < 35          — пара в дневном даунтренде
  • don_pos > 0.85          — цена в верхних 15% 20-барного диапазона (ралли)
  • SuperTrend(10,3) 4h DOWN
  • BTC RSI14(4h) < 45      — режимный фильтр (без него WR падает с 54 до 47)
  • ATR%(1h) 0.7–6.0
Выход: TP +6% / SL −4% (в short-ценах: −6%/+4%), горизонт 24ч.
Бэктест: WR 54% (n=345 с BTC-фильтром), EV +1.4%/сделку; OOS 48%→58%.
Нестабильнее IMPULSE (по неделям 35-77%) — режимный сигнал.

Кулдаун: 12ч на пару+сторону. Хранение: new_strategy_signals
(strategy='impulse'|'fade') — журнал/графики/families подхватывают.
"""
from __future__ import annotations
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

ATR_GATE = (0.7, 6.0)
COOLDOWN_H = 12
IMPULSE_TP, IMPULSE_SL, IMPULSE_HORIZON_H = 8.0, 4.0, 12
# 💰 TEN (research 2026-07-10, 90д × 30m × 206 пар): то же состояние, что
# IMPULSE (rsi4h>70 + rsi1d>65 + st4 UP + ATR gate), но цель +10% / SL -5%
# / горизонт 96ч. WR 64.9% (n=504, dedup 24h), EV +4.73%/сделку; OOS 79.2%;
# 13/13 недель > BE 33%; месяцы 64/62/73/80%; топ-пара 2%. Кулдаун 24ч.
TEN_TP, TEN_SL, TEN_HORIZON_H, TEN_COOLDOWN_H = 10.0, 5.0, 96, 24
IGNITION_TP, IGNITION_SL, IGNITION_HORIZON_H = 6.0, 3.0, 24
IGNITION_RSI4_LO, IGNITION_RSI4_HI, IGNITION_RSI4_PREV_MAX = 58.0, 68.0, 55.0
FADE_TP, FADE_SL, FADE_HORIZON_H = 6.0, 4.0, 24
BTC_RSI4H_MAX_FOR_FADE = 45.0


def _rsi(closes: list[float], period: int = 14) -> Optional[float]:
    n = len(closes)
    if n < period + 2:
        return None
    avg_g = avg_l = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        avg_g += max(d, 0)
        avg_l += max(-d, 0)
    avg_g /= period
    avg_l /= period
    for i in range(period + 1, n):
        d = closes[i] - closes[i - 1]
        avg_g = (avg_g * (period - 1) + max(d, 0)) / period
        avg_l = (avg_l * (period - 1) + max(-d, 0)) / period
    if avg_l == 0:
        return 100.0
    return 100 - 100 / (1 + avg_g / avg_l)


def _ema_last(values: list[float], span: int) -> Optional[float]:
    if len(values) < span:
        return None
    k = 2.0 / (span + 1)
    e = sum(values[:span]) / span
    for v in values[span:]:
        e = v * k + e * (1 - k)
    return e


def _atr_pct(candles: list[dict], period: int = 14) -> Optional[float]:
    n = len(candles)
    if n < period + 2:
        return None
    trs = []
    for i in range(1, n):
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i - 1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    c = candles[-1]["c"]
    return atr / c * 100 if c else None


def _supertrend_state(candles: list[dict], period: int = 10, mult: float = 3.0) -> Optional[str]:
    n = len(candles)
    if n < period + 5:
        return None
    closes = [c["c"] for c in candles]
    highs = [c["h"] for c in candles]
    lows = [c["l"] for c in candles]
    trs = [highs[0] - lows[0]]
    for i in range(1, n):
        trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1])))
    atr = sum(trs[:period]) / period
    atrs = [atr]
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
        atrs.append(atr)
    # align: atrs[j] соответствует бару period-1+j
    fu = fl = None
    trend = 1
    for i in range(period - 1, n):
        a = atrs[i - (period - 1)]
        hl2 = (highs[i] + lows[i]) / 2
        bu = hl2 + mult * a
        bl = hl2 - mult * a
        if fu is None:
            fu, fl = bu, bl
            continue
        fu = bu if (bu < fu or closes[i-1] > fu) else fu
        fl = bl if (bl > fl or closes[i-1] < fl) else fl
        if trend == 1 and closes[i] < fl:
            trend = -1
        elif trend == -1 and closes[i] > fu:
            trend = 1
    return "UP" if trend == 1 else "DOWN"


def _resample(candles_1h: list[dict], hours: int) -> list[dict]:
    """Группирует 1h свечи в hours-бары по границе времени."""
    out = []
    bucket = None
    key = None
    for c in candles_1h:
        k = c["t"] // (hours * 3600 * 1000)
        if k != key:
            if bucket:
                out.append(bucket)
            bucket = dict(t=c["t"], o=c["o"], h=c["h"], l=c["l"], c=c["c"], v=c["v"])
            key = k
        else:
            bucket["h"] = max(bucket["h"], c["h"])
            bucket["l"] = min(bucket["l"], c["l"])
            bucket["c"] = c["c"]
            bucket["v"] += c["v"]
    if bucket:
        out.append(bucket)
    return out


_btc_ctx_cache: dict = {"ts": 0.0, "rsi_4h": None}


def get_btc_rsi4h() -> Optional[float]:
    """BTC RSI 4h для режимного фильтра FADE. Кэш 10 мин."""
    now = time.time()
    if now - _btc_ctx_cache["ts"] < 600 and _btc_ctx_cache["rsi_4h"] is not None:
        return _btc_ctx_cache["rsi_4h"]
    try:
        from exchange import get_klines_any
        kl = get_klines_any("BTC/USDT", "4h", 60)
        if kl and len(kl) >= 20:
            r = _rsi([c["c"] for c in kl])
            _btc_ctx_cache.update(ts=now, rsi_4h=r)
            return r
    except Exception:
        pass
    return _btc_ctx_cache["rsi_4h"]


def check_pair(pair: str, candles_1h: Optional[list[dict]] = None,
               btc_rsi4h: Optional[float] = None) -> Optional[dict]:
    """Проверяет пару на IMPULSE/FADE. Возвращает dict сигнала или None."""
    try:
        if candles_1h is None:
            from exchange import get_klines_any
            candles_1h = get_klines_any(pair, "1h", 500)
        if not candles_1h or len(candles_1h) < 200:
            return None

        atr_pct = _atr_pct(candles_1h)
        if atr_pct is None or not (ATR_GATE[0] <= atr_pct <= ATR_GATE[1]):
            return None

        c4 = _resample(candles_1h, 4)
        cd = _resample(candles_1h, 24)
        if len(c4) < 30 or len(cd) < 16:
            return None

        rsi_4h = _rsi([c["c"] for c in c4])
        rsi_1d = _rsi([c["c"] for c in cd])
        st4 = _supertrend_state(c4)
        if rsi_4h is None or rsi_1d is None or st4 is None:
            return None

        price = candles_1h[-1]["c"]
        ind = {"rsi_4h": round(rsi_4h, 1), "rsi_1d": round(rsi_1d, 1),
               "st4": st4, "atr_pct": round(atr_pct, 2)}

        # ── 🚀 IMPULSE (LONG) ──
        if rsi_4h > 70 and rsi_1d > 65 and st4 == "UP":
            return {
                "strategy": "impulse", "direction": "LONG",
                "pair": pair, "symbol": pair.replace("/", "").upper(),
                "entry": price,
                "tp": price * (1 + IMPULSE_TP / 100),
                "sl": price * (1 - IMPULSE_SL / 100),
                "horizon_h": IMPULSE_HORIZON_H,
                "indicators": ind,
            }

        # ── 💥 IGNITION (LONG, ранний вход) ──
        # RSI4h только вошёл в 58-68 (прошлая 4h-свеча < 55) при аптренде
        # 4h + 1d. Ловит начало движения: ~+5% от лоу против +16% у IMPULSE.
        if (IGNITION_RSI4_LO <= rsi_4h <= IGNITION_RSI4_HI and st4 == "UP"
                and len(c4) > 30):
            rsi4_prev = _rsi([c["c"] for c in c4[:-1]])
            if rsi4_prev is not None and rsi4_prev < IGNITION_RSI4_PREV_MAX:
                closes_d = [c["c"] for c in cd]
                e10 = _ema_last(closes_d, 10)
                e20 = _ema_last(closes_d, 20)
                if e10 is not None and e20 is not None and e10 > e20:
                    return {
                        "strategy": "ignition", "direction": "LONG",
                        "pair": pair, "symbol": pair.replace("/", "").upper(),
                        "entry": price,
                        "tp": price * (1 + IGNITION_TP / 100),
                        "sl": price * (1 - IGNITION_SL / 100),
                        "horizon_h": IGNITION_HORIZON_H,
                        "indicators": {**ind, "rsi_4h_prev": round(rsi4_prev, 1)},
                    }

        # ── 🎣 FADE (SHORT) ──
        last20 = candles_1h[-20:]
        don_hi = max(c["h"] for c in last20)
        don_lo = min(c["l"] for c in last20)
        don_pos = (price - don_lo) / (don_hi - don_lo) if don_hi > don_lo else 0.5
        ind["don_pos"] = round(don_pos, 2)
        if btc_rsi4h is None:
            btc_rsi4h = get_btc_rsi4h()
        ind["btc_rsi_4h"] = round(btc_rsi4h, 1) if btc_rsi4h is not None else None
        if (rsi_1d < 35 and don_pos > 0.85 and st4 == "DOWN"
                and btc_rsi4h is not None and btc_rsi4h < BTC_RSI4H_MAX_FOR_FADE):
            return {
                "strategy": "fade", "direction": "SHORT",
                "pair": pair, "symbol": pair.replace("/", "").upper(),
                "entry": price,
                "tp": price * (1 - FADE_TP / 100),
                "sl": price * (1 + FADE_SL / 100),
                "horizon_h": FADE_HORIZON_H,
                "indicators": ind,
            }
        return None
    except Exception:
        logger.debug(f"[impulse] check fail {pair}", exc_info=True)
        return None


def store_signal(sig: dict, cooldown_h: Optional[float] = None) -> bool:
    """Сохраняет в new_strategy_signals с кулдауном на пару+strategy
    (default COOLDOWN_H=12; TEN использует 24)."""
    try:
        from database import _get_db, utcnow
        from datetime import timedelta
        db = _get_db()
        col = db.new_strategy_signals
        cutoff = utcnow() - timedelta(hours=cooldown_h or COOLDOWN_H)
        dup = col.find_one({"strategy": sig["strategy"], "pair": sig["pair"],
                            "created_at": {"$gte": cutoff}})
        if dup:
            return False
        doc = {**sig, "created_at": utcnow(), "state": "WAITING"}
        col.insert_one(doc)
        logger.info(f"[{sig['strategy']}] fired {sig['pair']} {sig['direction']} "
                    f"@ {sig['entry']} ind={sig['indicators']}")
        return True
    except Exception:
        logger.exception("[impulse] store fail")
        return False


def scan_universe(max_pairs: int = 300) -> list[dict]:
    """Полный скан ликвидных пар. Вызывается из watcher-лупа (thread)."""
    from futures_data import get_liquid_pairs
    from exchange import get_klines_any
    fired = []
    try:
        pairs = get_liquid_pairs(min_volume_usd=5_000_000)[:max_pairs]
    except Exception:
        logger.debug("[impulse] pairs list fail", exc_info=True)
        return fired
    btc = get_btc_rsi4h()
    for sym in pairs:
        pair = sym.replace("USDT", "/USDT") if "/" not in sym else sym
        try:
            sig = check_pair(pair, btc_rsi4h=btc)
            if sig and store_signal(sig):
                fired.append(sig)
            # 💰 TEN — двойник IMPULSE-состояния с целью +10%/SL-5%/96ч и
            # своим 24ч-кулдауном. Условие то же => если check_pair вернул
            # impulse, состояние TEN тоже активно.
            if sig and sig["strategy"] == "impulse":
                ten = {
                    "strategy": "ten", "direction": "LONG",
                    "pair": sig["pair"], "symbol": sig["symbol"],
                    "entry": sig["entry"],
                    "tp": sig["entry"] * (1 + TEN_TP / 100),
                    "sl": sig["entry"] * (1 - TEN_SL / 100),
                    "horizon_h": TEN_HORIZON_H,
                    "indicators": sig["indicators"],
                }
                if store_signal(ten, cooldown_h=TEN_COOLDOWN_H):
                    fired.append(ten)
        except Exception:
            continue
    return fired
