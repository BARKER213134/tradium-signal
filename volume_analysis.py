"""Детекция объёмных аномалий на свечных данных.

Условие срабатывания (Volume Alert):
1. Текущая свеча volume > 3× SMA(volume, 20)
2. Цена за эту свечу двинулась > 2% в направлении сигнала
"""
from typing import Optional


def volume_sma(candles: list[dict], n: int = 20) -> float:
    """Среднее volume за последние n свечей (не считая текущую)."""
    window = candles[-(n + 1):-1] if len(candles) > n else candles[:-1]
    if not window:
        return 0.0
    return sum(c.get("v", 0) for c in window) / len(window)


def rvol(candles: list[dict], n: int = 20) -> float:
    """Relative Volume: текущий / средний."""
    avg = volume_sma(candles, n)
    if avg <= 0:
        return 0.0
    return candles[-1].get("v", 0) / avg


def candle_price_change(candle: dict) -> float:
    """Процентное изменение цены за свечу (close vs open)."""
    o = candle.get("o", 0)
    c = candle.get("c", 0)
    if o <= 0:
        return 0.0
    return ((c - o) / o) * 100


def is_volume_spike(
    candles: list[dict],
    direction: str,
    vol_multiplier: float = 3.0,
    price_threshold: float = 2.0,
) -> tuple[bool, dict]:
    """Проверяет volume spike + движение цены в направлении сигнала.

    Returns:
        (triggered, info)
        info: {rvol, volume, avg_volume, price_change_pct, candle}
    """
    if not candles or len(candles) < 21:
        return False, {}

    current = candles[-1]
    avg = volume_sma(candles, 20)
    current_vol = current.get("v", 0)
    rel_vol = current_vol / avg if avg > 0 else 0.0
    price_pct = candle_price_change(current)

    info = {
        "rvol": round(rel_vol, 2),
        "volume": current_vol,
        "avg_volume": round(avg, 2),
        "price_change_pct": round(price_pct, 2),
    }

    # Volume достаточно большой?
    if rel_vol < vol_multiplier:
        return False, info

    # Цена двинулась в нужном направлении > threshold?
    if direction in ("LONG", "BUY"):
        if price_pct < price_threshold:
            return False, info
    elif direction in ("SHORT", "SELL"):
        if price_pct > -price_threshold:
            return False, info
    else:
        return False, info

    return True, info


def format_volume(vol: float) -> str:
    """12500000 → '12.5M', 500000 → '500K'."""
    if vol >= 1_000_000_000:
        return f"{vol / 1_000_000_000:.1f}B"
    if vol >= 1_000_000:
        return f"{vol / 1_000_000:.1f}M"
    if vol >= 1_000:
        return f"{vol / 1_000:.0f}K"
    return f"{vol:.0f}"
