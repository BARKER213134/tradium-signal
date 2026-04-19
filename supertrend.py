"""SuperTrend indicator — TradingView-совместимый расчёт.

Формула (Wilder's RMA ATR):
  hl2 = (high + low) / 2
  TR[i] = max(high-low, |high-prevClose|, |low-prevClose|)
  ATR[i] = RMA(TR, period)  # prev*(p-1)/p + new/p
  upper = hl2 + mult * ATR
  lower = hl2 - mult * ATR

  # "липкие" бэнды — не ослабляются пока тренд не сменится
  finalUpper[i] = (upper < finalUpper[i-1] OR close[i-1] > finalUpper[i-1]) ? upper : finalUpper[i-1]
  finalLower[i] = (lower > finalLower[i-1] OR close[i-1] < finalLower[i-1]) ? lower : finalLower[i-1]

  # направление
  if close > finalUpper[i-1]: trend = UP,   st = finalLower
  if close < finalLower[i-1]: trend = DOWN, st = finalUpper
  else: сохраняем предыдущий

Используется для:
  1. Отрисовки на графиках (через JS-аналог в base.html)
  2. Обогащения Telegram алертов (через _st_block в watcher.py)
"""
from __future__ import annotations
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

# ── Пресеты по таймфрейму (TradingView-совместимые) ─────────────────
ST_PRESETS = {
    "15m": {"period": 7,  "mult": 2.0},
    "30m": {"period": 7,  "mult": 2.5},
    "1h":  {"period": 10, "mult": 3.0},   # TV default
    "2h":  {"period": 10, "mult": 3.0},
    "4h":  {"period": 10, "mult": 3.0},
    "12h": {"period": 10, "mult": 3.0},
    "1d":  {"period": 14, "mult": 3.0},
}
DEFAULT_PRESET = {"period": 10, "mult": 3.0}


def _pick_preset(tf: str) -> tuple[int, float]:
    p = ST_PRESETS.get((tf or "").lower(), DEFAULT_PRESET)
    return p["period"], p["mult"]


# ── Кеш результатов (ключ = pair|tf, TTL 2 мин) ──────────────────────
_cache: dict[str, tuple[float, dict]] = {}
_CACHE_TTL = 120.0  # сек


def _calc_supertrend(candles: list[dict], period: int, mult: float) -> Optional[dict]:
    """Считает SuperTrend из списка свечей [{t, o, h, l, c, v}, ...].
    Возвращает dict с полями:
      state: 'UP' | 'DOWN' | None
      value: текущее значение линии (float) или None
      last_flip_at: unix ms или None
      prev_state: состояние на предпоследнем баре (для детекта флипа)
    """
    if not candles or len(candles) < period + 2:
        return None

    n = len(candles)
    closes = [c["c"] for c in candles]
    highs = [c["h"] for c in candles]
    lows = [c["l"] for c in candles]

    # True Range
    tr = [highs[0] - lows[0]]
    for i in range(1, n):
        tr.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i] - closes[i-1]),
        ))

    # RMA (Wilder's smoothing): первые period баров — SMA, дальше RMA
    atr = [None] * n
    if n >= period:
        atr[period-1] = sum(tr[:period]) / period
        for i in range(period, n):
            atr[i] = (atr[i-1] * (period - 1) + tr[i]) / period

    # Basic/final bands + trend
    hl2 = [(highs[i] + lows[i]) / 2 for i in range(n)]
    final_upper = [0.0] * n
    final_lower = [0.0] * n
    trend = [0] * n   # 1 = UP, -1 = DOWN
    st = [None] * n

    for i in range(n):
        if atr[i] is None:
            continue
        bu = hl2[i] + mult * atr[i]
        bl = hl2[i] - mult * atr[i]
        if i == 0 or atr[i-1] is None:
            final_upper[i] = bu
            final_lower[i] = bl
            trend[i] = 1
            st[i] = bl
            continue
        final_upper[i] = bu if (bu < final_upper[i-1] or closes[i-1] > final_upper[i-1]) else final_upper[i-1]
        final_lower[i] = bl if (bl > final_lower[i-1] or closes[i-1] < final_lower[i-1]) else final_lower[i-1]

        # Определение направления
        if trend[i-1] == 1 and closes[i] < final_lower[i-1]:
            trend[i] = -1
        elif trend[i-1] == -1 and closes[i] > final_upper[i-1]:
            trend[i] = 1
        else:
            trend[i] = trend[i-1] if trend[i-1] != 0 else 1

        st[i] = final_lower[i] if trend[i] == 1 else final_upper[i]

    # Ищем последний флип
    last_flip_idx = None
    for i in range(n-1, 0, -1):
        if trend[i] != 0 and trend[i-1] != 0 and trend[i] != trend[i-1]:
            last_flip_idx = i
            break

    cur = trend[-1]
    prev = trend[-2] if n >= 2 else 0
    state_str = "UP" if cur == 1 else ("DOWN" if cur == -1 else None)
    prev_str = "UP" if prev == 1 else ("DOWN" if prev == -1 else None)
    return {
        "state": state_str,
        "prev_state": prev_str,
        "value": st[-1],
        "last_flip_at": candles[last_flip_idx]["t"] if last_flip_idx else None,
        "period": period,
        "mult": mult,
    }


def supertrend_state(pair: str, tf: str = "1h",
                     period: Optional[int] = None,
                     mult: Optional[float] = None) -> Optional[dict]:
    """Возвращает состояние SuperTrend для pair/tf. Кеш 2 мин.

    Example:
      >>> supertrend_state("BTC/USDT", "1h")
      {"state": "UP", "value": 67800.5, "tf": "1h", "period": 10, "mult": 3.0,
       "last_flip_at": 1708123456000, "prev_state": "UP"}
    """
    if not pair:
        return None
    if period is None or mult is None:
        period, mult = _pick_preset(tf)

    key = f"{pair}|{tf}|{period}|{mult}"
    now = time.time()
    hit = _cache.get(key)
    if hit and (now - hit[0]) < _CACHE_TTL:
        return hit[1]

    try:
        from exchange import get_klines_any
        # Нужно минимум period*10 свечей для стабильного RMA
        candles = get_klines_any(pair, tf, max(period * 10, 100))
        if not candles:
            return None
        result = _calc_supertrend(candles, period, mult)
        if result:
            result["tf"] = tf
            result["pair"] = pair
            _cache[key] = (now, result)
            # Лениво чистим старые записи
            if len(_cache) > 1000:
                for k in [k for k, v in _cache.items() if (now - v[0]) > _CACHE_TTL * 2]:
                    _cache.pop(k, None)
        return result
    except Exception as e:
        logger.warning(f"[supertrend] calc failed for {pair} {tf}: {e}")
        return None


def format_tg_block(pair: str, direction: str, tf: str = "1h") -> str:
    """Формирует HTML-строку для Telegram алерта.
    Возвращает '' если ST недоступен.

    Example:
      '🟢 <b>ST(1h):</b> UP · по тренду\n'
      '🔴 <b>ST(1h):</b> DOWN · ⚠️ ПРОТИВ тренда\n'
    """
    try:
        st = supertrend_state(pair, tf)
        if not st or not st.get("state"):
            return ""
        is_long = (direction or "").upper() in ("LONG", "BUY", "BULLISH")
        is_up = st["state"] == "UP"
        aligned = is_up == is_long
        emoji = "🟢" if is_up else "🔴"
        tag = "по тренду" if aligned else "⚠️ <b>ПРОТИВ тренда</b>"
        p = st.get("period", "?")
        m = st.get("mult", "?")
        return f"\n{emoji} <b>ST({tf} {p}/{m}):</b> {st['state']} · {tag}\n"
    except Exception as e:
        logger.debug(f"[supertrend] format_tg_block fail: {e}")
        return ""
