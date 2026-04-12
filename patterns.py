"""Детектор свечных паттернов.

Принимает список свечей [{'o','h','l','c'}] (старые → новые) и возвращает
список обнаруженных паттернов на последней (или предпоследней) свече.

Категории: bullish / bearish. Используется для подтверждения входа после DCA #4.
"""
from typing import Literal

Direction = Literal["LONG", "SHORT", "BUY", "SELL"]


def _body(c): return abs(c["c"] - c["o"])
def _range(c): return max(c["h"] - c["l"], 1e-12)
def _upper(c): return c["h"] - max(c["o"], c["c"])
def _lower(c): return min(c["o"], c["c"]) - c["l"]
def _is_bull(c): return c["c"] > c["o"]
def _is_bear(c): return c["c"] < c["o"]


# ─── Одиночные свечи ─────────────────────────────────────────────────────

def hammer(c) -> bool:
    """Молот (бычий): маленькое тело вверху, длинная нижняя тень."""
    body = _body(c)
    rng = _range(c)
    return (
        body / rng < 0.35
        and _lower(c) >= 2 * body
        and _upper(c) < body
    )


def inverted_hammer(c) -> bool:
    body = _body(c)
    rng = _range(c)
    return (
        body / rng < 0.35
        and _upper(c) >= 2 * body
        and _lower(c) < body
    )


def shooting_star(c) -> bool:
    """Падающая звезда (медвежий)."""
    return inverted_hammer(c) and _is_bear(c)


def hanging_man(c) -> bool:
    return hammer(c) and _is_bear(c)


def doji(c) -> bool:
    rng = _range(c)
    return _body(c) / rng < 0.1


# ─── Парные свечи ────────────────────────────────────────────────────────

def bullish_engulfing(p, c) -> bool:
    return (
        _is_bear(p) and _is_bull(c)
        and c["o"] <= p["c"] and c["c"] >= p["o"]
        and _body(c) > _body(p)
    )


def bearish_engulfing(p, c) -> bool:
    return (
        _is_bull(p) and _is_bear(c)
        and c["o"] >= p["c"] and c["c"] <= p["o"]
        and _body(c) > _body(p)
    )


def piercing_line(p, c) -> bool:
    if not (_is_bear(p) and _is_bull(c)):
        return False
    mid_prev = (p["o"] + p["c"]) / 2
    return c["o"] < p["l"] and c["c"] > mid_prev and c["c"] < p["o"]


def dark_cloud_cover(p, c) -> bool:
    if not (_is_bull(p) and _is_bear(c)):
        return False
    mid_prev = (p["o"] + p["c"]) / 2
    return c["o"] > p["h"] and c["c"] < mid_prev and c["c"] > p["o"]


def tweezer_bottom(p, c) -> bool:
    return (
        _is_bear(p) and _is_bull(c)
        and abs(p["l"] - c["l"]) / _range(p) < 0.05
    )


def tweezer_top(p, c) -> bool:
    return (
        _is_bull(p) and _is_bear(c)
        and abs(p["h"] - c["h"]) / _range(p) < 0.05
    )


# ─── Тройные свечи ───────────────────────────────────────────────────────

def morning_star(p2, p1, c) -> bool:
    return (
        _is_bear(p2)
        and _body(p1) / _range(p1) < 0.4
        and _is_bull(c)
        and c["c"] > (p2["o"] + p2["c"]) / 2
    )


def evening_star(p2, p1, c) -> bool:
    return (
        _is_bull(p2)
        and _body(p1) / _range(p1) < 0.4
        and _is_bear(c)
        and c["c"] < (p2["o"] + p2["c"]) / 2
    )


def three_white_soldiers(p2, p1, c) -> bool:
    return (
        _is_bull(p2) and _is_bull(p1) and _is_bull(c)
        and p1["c"] > p2["c"] and c["c"] > p1["c"]
        and p1["o"] > p2["o"] and c["o"] > p1["o"]
    )


def three_black_crows(p2, p1, c) -> bool:
    return (
        _is_bear(p2) and _is_bear(p1) and _is_bear(c)
        and p1["c"] < p2["c"] and c["c"] < p1["c"]
        and p1["o"] < p2["o"] and c["o"] < p1["o"]
    )


# ─── Публичный API ───────────────────────────────────────────────────────

BULLISH = [
    ("Молот", lambda cs: hammer(cs[-1])),
    ("Перевёрнутый молот", lambda cs: inverted_hammer(cs[-1]) and _is_bull(cs[-1])),
    ("Бычье поглощение", lambda cs: len(cs) >= 2 and bullish_engulfing(cs[-2], cs[-1])),
    ("Просвет в облаках", lambda cs: len(cs) >= 2 and piercing_line(cs[-2], cs[-1])),
    ("Пинцет на дне", lambda cs: len(cs) >= 2 and tweezer_bottom(cs[-2], cs[-1])),
    ("Утренняя звезда", lambda cs: len(cs) >= 3 and morning_star(cs[-3], cs[-2], cs[-1])),
    ("Три белых солдата", lambda cs: len(cs) >= 3 and three_white_soldiers(cs[-3], cs[-2], cs[-1])),
]

BEARISH = [
    ("Падающая звезда", lambda cs: shooting_star(cs[-1])),
    ("Повешенный", lambda cs: hanging_man(cs[-1])),
    ("Медвежье поглощение", lambda cs: len(cs) >= 2 and bearish_engulfing(cs[-2], cs[-1])),
    ("Завеса тёмных облаков", lambda cs: len(cs) >= 2 and dark_cloud_cover(cs[-2], cs[-1])),
    ("Пинцет на вершине", lambda cs: len(cs) >= 2 and tweezer_top(cs[-2], cs[-1])),
    ("Вечерняя звезда", lambda cs: len(cs) >= 3 and evening_star(cs[-3], cs[-2], cs[-1])),
    ("Три чёрных ворона", lambda cs: len(cs) >= 3 and three_black_crows(cs[-3], cs[-2], cs[-1])),
]


def detect_patterns(candles: list[dict], direction: Direction) -> list[str]:
    """Возвращает паттерны, подтверждающие движение в направлении сделки."""
    if not candles:
        return []
    pool = BULLISH if direction in ("LONG", "BUY") else BEARISH
    out = []
    for name, test in pool:
        try:
            if test(candles):
                out.append(name)
        except Exception:
            pass
    return out
