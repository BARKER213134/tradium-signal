"""Hybrid FVG стратегии v2-v4 с умными фильтрами.

v2 — базовый Hybrid с адекватными порогами:
   min body ratio 50%, min size 0.03% forex / 0.1% crypto, сессия London/NY

v3 — v2 + multi-timeframe trend (MTF):
   берём 1H FVG только если тренд на 4H в сторону сигнала
   (close > close[20] для long, close < close[20] для short)

v4 — v3 + liquidity grab + ATR фильтр:
   до FVG должен быть sweep экстремума (wick пробивает last 5 bars)
   размер FVG >= 0.7 × ATR(14) — доказательство импульса
"""
from __future__ import annotations
from datetime import datetime, timezone
from fvg_detector import FVG, evaluate_conservative, TradeResult


FOREX_CLASSES = {"forex"}
CRYPTO_CLASSES = {"crypto"}


def _atr(candles: list[dict], idx: int, period: int = 14) -> float:
    """ATR по свечам (True Range)."""
    if idx < period:
        return 0
    trs = []
    for j in range(idx - period + 1, idx + 1):
        if j == 0:
            trs.append(candles[j]["h"] - candles[j]["l"])
        else:
            prev_c = candles[j - 1]["c"]
            tr = max(
                candles[j]["h"] - candles[j]["l"],
                abs(candles[j]["h"] - prev_c),
                abs(candles[j]["l"] - prev_c),
            )
            trs.append(tr)
    return sum(trs) / len(trs) if trs else 0


def _is_session_active(unix_ts: int, session: str = "london_ny") -> bool:
    """London 08-16 UTC, NY 13-21 UTC. london_ny = union.
    Для 1D свечей сессия не применима — всегда True."""
    if unix_ts <= 0:
        return True
    h = datetime.fromtimestamp(unix_ts, tz=timezone.utc).hour
    if session == "london":
        return 8 <= h < 16
    if session == "ny":
        return 13 <= h < 21
    if session == "london_ny":
        return 8 <= h < 21  # union
    return True


def _mtf_trend_ok(candles: list[dict], idx: int, direction: str, lookback: int = 20) -> bool:
    """Направление совпадает с трендом на окне lookback свечей назад.
    Простая проверка: close[idx-1] vs close[idx-lookback]."""
    if idx < lookback + 1:
        return True  # недостаточно истории — не блокируем
    trend_up = candles[idx - 1]["c"] > candles[idx - lookback]["c"]
    if direction == "bullish":
        return trend_up
    else:
        return not trend_up


def _liquidity_grab_before(candles: list[dict], fvg: FVG, lookback: int = 5) -> bool:
    """Перед FVG было sweep экстремума — wick свечи i пробила предыдущий high/low.
    Для bullish: свеча 1 (индекс fvg.idx-2) должна иметь low ниже минимума предыдущих lookback.
    Для bearish: high свечи 1 > max high предыдущих lookback.
    """
    i1 = fvg.idx - 2  # первая свеча 3-свечной формации
    if i1 < lookback:
        return True  # не блокируем
    prev = candles[i1 - lookback:i1]
    if not prev:
        return True
    c1 = candles[i1]
    if fvg.direction == "bullish":
        prev_low = min(p["l"] for p in prev)
        return c1["l"] < prev_low  # свеча 1 сделала new low (sweep)
    else:
        prev_high = max(p["h"] for p in prev)
        return c1["h"] > prev_high


# ── Hybrid v2 ──
def evaluate_hybrid_v2(
    fvg: FVG, candles: list[dict], asset_class: str = "forex",
    exit_mode: str = "trailing",
) -> TradeResult:
    min_body = 0.5
    min_size = 0.0010 if asset_class in CRYPTO_CLASSES else 0.0003
    if fvg.impulse_body_ratio < min_body or fvg.size_rel < min_size:
        return TradeResult(fvg.idx, fvg.idx, 0, 0, fvg.direction, 0, "SKIPPED", 0)
    # Session только для intraday
    ts = fvg.time or (candles[fvg.idx].get("t", 0) if fvg.idx < len(candles) else 0)
    # Проверяем только если час не 0 (1D свечи обычно t=00:00 UTC)
    hour = datetime.fromtimestamp(ts, tz=timezone.utc).hour if ts else 12
    if hour != 0 and not _is_session_active(ts, "london_ny"):
        return TradeResult(fvg.idx, fvg.idx, 0, 0, fvg.direction, 0, "SKIPPED", 0)
    return evaluate_conservative(fvg, candles, exit_mode=exit_mode)


# ── Hybrid v3 — v2 + multi-timeframe trend ──
def evaluate_hybrid_v3(
    fvg: FVG, candles: list[dict], asset_class: str = "forex",
    exit_mode: str = "trailing",
) -> TradeResult:
    r = evaluate_hybrid_v2(fvg, candles, asset_class, exit_mode)
    if r.status == "SKIPPED":
        return r
    # Нужен trend в ту же сторону
    if not _mtf_trend_ok(candles, fvg.idx, fvg.direction, lookback=20):
        return TradeResult(fvg.idx, fvg.idx, 0, 0, fvg.direction, 0, "SKIPPED", 0)
    return r


# ── Hybrid v4 — v3 + liquidity grab + ATR ──
def evaluate_hybrid_v4(
    fvg: FVG, candles: list[dict], asset_class: str = "forex",
    exit_mode: str = "trailing",
) -> TradeResult:
    # Сначала v2 базовые проверки
    min_body = 0.5
    min_size = 0.0010 if asset_class in CRYPTO_CLASSES else 0.0003
    if fvg.impulse_body_ratio < min_body or fvg.size_rel < min_size:
        return TradeResult(fvg.idx, fvg.idx, 0, 0, fvg.direction, 0, "SKIPPED", 0)
    ts = fvg.time
    hour = datetime.fromtimestamp(ts, tz=timezone.utc).hour if ts else 12
    if hour != 0 and not _is_session_active(ts, "london_ny"):
        return TradeResult(fvg.idx, fvg.idx, 0, 0, fvg.direction, 0, "SKIPPED", 0)
    # MTF trend
    if not _mtf_trend_ok(candles, fvg.idx, fvg.direction, lookback=20):
        return TradeResult(fvg.idx, fvg.idx, 0, 0, fvg.direction, 0, "SKIPPED", 0)
    # ATR фильтр: размер FVG >= 0.7 × ATR
    atr = _atr(candles, fvg.idx, 14)
    fvg_size = fvg.top - fvg.bottom
    if atr > 0 and fvg_size < 0.7 * atr:
        return TradeResult(fvg.idx, fvg.idx, 0, 0, fvg.direction, 0, "SKIPPED", 0)
    # Liquidity grab перед FVG
    if not _liquidity_grab_before(candles, fvg, lookback=5):
        return TradeResult(fvg.idx, fvg.idx, 0, 0, fvg.direction, 0, "SKIPPED", 0)
    return evaluate_conservative(fvg, candles, exit_mode=exit_mode)
