"""Трендовые паттерны продолжения.

Работают с тем же форматом свечей что и patterns.py: [{'o','h','l','c','v'}, ...].
"""


def _body(c): return abs(c["c"] - c["o"])
def _is_bull(c): return c["c"] > c["o"]
def _is_bear(c): return c["c"] < c["o"]


# ─── Rising/Falling Three Methods ──────────────────────────────────────

def rising_three_methods(cs: list[dict]) -> bool:
    """5 свечей: большая зелёная, 3 мелкие красные (внутри первой), большая зелёная (выше первой)."""
    if len(cs) < 5:
        return False
    c0, c1, c2, c3, c4 = cs[-5:]
    if not _is_bull(c0):
        return False
    if not _is_bull(c4):
        return False
    # 3 средние свечи — небольшие, медвежьи или мелкие, в диапазоне c0
    for m in (c1, c2, c3):
        if _body(m) >= _body(c0):
            return False
        if m["h"] > c0["h"] or m["l"] < c0["l"]:
            return False
    # Последняя закрывается выше первой
    return c4["c"] > c0["c"]


def falling_three_methods(cs: list[dict]) -> bool:
    if len(cs) < 5:
        return False
    c0, c1, c2, c3, c4 = cs[-5:]
    if not _is_bear(c0):
        return False
    if not _is_bear(c4):
        return False
    for m in (c1, c2, c3):
        if _body(m) >= _body(c0):
            return False
        if m["h"] > c0["h"] or m["l"] < c0["l"]:
            return False
    return c4["c"] < c0["c"]


# ─── Bull/Bear Flag ─────────────────────────────────────────────────────

def _linreg_slope(values: list[float]) -> float:
    """Простая регрессия — slope по индексам."""
    n = len(values)
    if n < 2:
        return 0.0
    xs = list(range(n))
    mean_x = sum(xs) / n
    mean_y = sum(values) / n
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, values))
    den = sum((x - mean_x) ** 2 for x in xs)
    return num / den if den else 0.0


def bull_flag(cs: list[dict]) -> bool:
    """Сильный импульс вверх, затем 3-6 свечей в мягком нисходящем канале."""
    if len(cs) < 8:
        return False
    impulse = cs[-10:-5] if len(cs) >= 10 else cs[:-5]
    flag = cs[-5:]
    if not impulse:
        return False
    # импульс — в сумме зелёный, 60%+ свечей bullish
    bulls = sum(1 for c in impulse if _is_bull(c))
    if bulls / len(impulse) < 0.6:
        return False
    imp_move = impulse[-1]["c"] - impulse[0]["o"]
    if imp_move <= 0:
        return False
    # Флаг — slope по closes слегка отрицательный или нулевой
    closes = [c["c"] for c in flag]
    slope = _linreg_slope(closes)
    # Нормализуем к диапазону импульса
    if imp_move == 0:
        return False
    rel = slope / imp_move
    # Флаг не должен зайти ниже 50% импульса
    lowest = min(c["l"] for c in flag)
    retrace = (impulse[-1]["h"] - lowest) / imp_move
    return rel < 0.02 and retrace < 0.5


def bear_flag(cs: list[dict]) -> bool:
    if len(cs) < 8:
        return False
    impulse = cs[-10:-5] if len(cs) >= 10 else cs[:-5]
    flag = cs[-5:]
    if not impulse:
        return False
    bears = sum(1 for c in impulse if _is_bear(c))
    if bears / len(impulse) < 0.6:
        return False
    imp_move = impulse[0]["o"] - impulse[-1]["c"]
    if imp_move <= 0:
        return False
    closes = [c["c"] for c in flag]
    slope = _linreg_slope(closes)
    rel = slope / imp_move
    highest = max(c["h"] for c in flag)
    retrace = (highest - impulse[-1]["l"]) / imp_move
    return rel > -0.02 and retrace < 0.5


# ─── Ascending/Descending Triangle ──────────────────────────────────────

def ascending_triangle(cs: list[dict]) -> bool:
    """Плоское сопротивление + восходящие минимумы. 6-10 свечей."""
    if len(cs) < 6:
        return False
    window = cs[-8:]
    highs = [c["h"] for c in window]
    lows = [c["l"] for c in window]
    max_h = max(highs)
    # Плоский верх: все максимумы в пределах 0.5% от max
    flat = all((max_h - h) / max_h < 0.005 for h in highs[-4:])
    # Восходящие минимумы
    slope_lows = _linreg_slope(lows)
    return flat and slope_lows > 0


def descending_triangle(cs: list[dict]) -> bool:
    if len(cs) < 6:
        return False
    window = cs[-8:]
    highs = [c["h"] for c in window]
    lows = [c["l"] for c in window]
    min_l = min(lows)
    flat = all((l - min_l) / min_l < 0.005 if min_l else False for l in lows[-4:])
    slope_highs = _linreg_slope(highs)
    return flat and slope_highs < 0


# ─── Публичный API ───────────────────────────────────────────────────────

BULLISH_CONT = [
    ("Bull Flag", bull_flag),
    ("Ascending Triangle", ascending_triangle),
    ("Rising Three Methods", rising_three_methods),
]

BEARISH_CONT = [
    ("Bear Flag", bear_flag),
    ("Descending Triangle", descending_triangle),
    ("Falling Three Methods", falling_three_methods),
]


def detect_continuation(candles: list[dict], direction: str) -> list[str]:
    if not candles or len(candles) < 5:
        return []
    pool = BULLISH_CONT if direction in ("LONG", "BUY") else BEARISH_CONT
    out = []
    for name, test in pool:
        try:
            if test(candles):
                out.append(name)
        except Exception:
            pass
    return out
