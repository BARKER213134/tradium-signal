"""Парсер сообщений из @TRENDS_Cryptovizor.

Формат:
    👀 Perfectly fit DD.MM
    $TICKER.P    🟢🔴🔴🔴🔴  HH:MM
    $TICKER2.P   🔴🟢🟢🟢🟢  HH:MM

Одно сообщение → список сигналов (по одному на тикер).

Кружки = таймфреймы 30m / 1h / 2h / 4h / 12h (младший → старший).
- 🟢🔴🔴🔴🔴 (GRRRR): младший зелёный (коррекция вверх), старшие красные → SHORT
- 🔴🟢🟢🟢🟢 (RGGGG): младший красный (коррекция вниз), старшие зелёные → LONG
"""
import re
from typing import List, Optional


_LINE_RE = re.compile(
    r"\$([A-Z0-9]+)\.P\s+([🟢🔴]{3,7})\s+(\d{1,2}:\d{2})",
)


def _trend_to_str(circles: str) -> str:
    """🟢🔴🔴🔴🔴 → 'GRRRR'"""
    return "".join("G" if c == "🟢" else "R" for c in circles)


def _direction(trend: str) -> Optional[str]:
    """GRRRR → SHORT, RGGGG → LONG."""
    if not trend:
        return None
    first = trend[0]
    rest = trend[1:]
    if not rest:
        return None
    # LONG: младший красный, остальные зелёные
    if first == "R" and all(c == "G" for c in rest):
        return "LONG"
    # SHORT: младший зелёный, остальные красные
    if first == "G" and all(c == "R" for c in rest):
        return "SHORT"
    # Смешанные: берём мажорное направление старших TF
    higher_greens = rest.count("G")
    higher_reds = rest.count("R")
    if higher_greens > higher_reds:
        return "LONG"
    if higher_reds > higher_greens:
        return "SHORT"
    return None


def parse_cryptovizor_message(text: str) -> List[dict]:
    """Возвращает список сигналов: [{pair, direction, trend, time_str}, ...]."""
    if not text:
        return []
    # Первая строка содержит заголовок Perfectly fit — проверяем что это сигнал
    if "Perfectly fit" not in text and "👀" not in text:
        return []

    out: List[dict] = []
    for m in _LINE_RE.finditer(text):
        ticker, circles, time_str = m.group(1), m.group(2), m.group(3)
        trend = _trend_to_str(circles)
        if len(trend) < 5:
            continue
        direction = _direction(trend[:5])
        if direction is None:
            continue
        out.append({
            "pair": f"{ticker}/USDT",
            "ticker": ticker,
            "direction": direction,
            "trend": trend[:5],
            "time_str": time_str,
        })
    return out
