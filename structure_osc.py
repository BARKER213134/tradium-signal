# -*- coding: utf-8 -*-
"""🏚 Структурный осциллятор (LuxAlgo-класс, 17.08.26) и детектор
«слом структурного перегрева» → SHORT.

Механика: трёхуровневая иерархия свингов (STH/STL 3-бар фракталы →
ITH/ITL над соседними STH → LTH/LTL над ITH), состояние каждого уровня
±1 на пробое close последнего подтверждённого свинга, осциллятор =
EMA5 взвешенной суммы (веса 1/2/3, диапазон −1..+1). Всё причинно:
свинги подтверждаются следующим баром/пунктом, без заглядывания.

Сигнал (год-бэктест, 294 пары, 4h, сетка TP−10/SL+5/96ч, SL первым):
осциллятор ≥0.9 шесть 4h-баров подряд (полный структурный перегрев
вверх) и отступил ниже 0.75 → SHORT. n=453, EV +0.99%/сделку,
WR(TP) 39%, половины [+0.34/+1.65], ~1.2 сигнала/день на рынок.
Лонг-зеркало и 0-кроссы — минус/ноль, НЕ внедрять (bt_ms_oscillator).
"""
from __future__ import annotations
import logging
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


def _swings(h, l):
    sth, stl = [], []
    for i in range(1, len(h) - 1):
        if h[i] > h[i - 1] and h[i] > h[i + 1]:
            sth.append((i, h[i]))
        if l[i] < l[i - 1] and l[i] < l[i + 1]:
            stl.append((i, l[i]))
    return sth, stl


def _upper_tier(pts, is_high):
    out = []
    for j in range(1, len(pts) - 1):
        p0, p1, p2 = pts[j - 1][1], pts[j][1], pts[j + 1][1]
        if (p1 > p0 and p1 > p2) if is_high else (p1 < p0 and p1 < p2):
            out.append((pts[j][0], p1, pts[j + 1][0] + 1))
    return out


def ms_osc(h, l, c) -> np.ndarray:
    """Осциллятор −1..+1 по закрытым барам (причинный)."""
    n = len(c)
    sth, stl = _swings(h, l)
    ith = _upper_tier(sth, True)
    itl = _upper_tier(stl, False)
    lth = _upper_tier([(i, p) for i, p, _ in ith], True)
    ltl = _upper_tier([(i, p) for i, p, _ in itl], False)

    def levels(hi_pts, lo_pts):
        cur_hi = np.full(n, np.nan)
        cur_lo = np.full(n, np.nan)
        k = 0
        v = np.nan
        pts = sorted([(cf, p) for (_, p, cf) in hi_pts])
        for i in range(n):
            while k < len(pts) and pts[k][0] <= i:
                v = pts[k][1]
                k += 1
            cur_hi[i] = v
        k = 0
        v = np.nan
        pts = sorted([(cf, p) for (_, p, cf) in lo_pts])
        for i in range(n):
            while k < len(pts) and pts[k][0] <= i:
                v = pts[k][1]
                k += 1
            cur_lo[i] = v
        return cur_hi, cur_lo

    st_hi, st_lo = levels([(i, p, i + 2) for i, p in sth],
                          [(i, p, i + 2) for i, p in stl])
    it_hi, it_lo = levels(ith, itl)
    lt_hi, lt_lo = levels(lth, ltl)
    states = np.zeros((3, n))
    for t_, (chi, clo) in enumerate(((st_hi, st_lo), (it_hi, it_lo),
                                     (lt_hi, lt_lo))):
        s = 0
        for i in range(n):
            if not np.isnan(chi[i]) and c[i] > chi[i]:
                s = 1
            elif not np.isnan(clo[i]) and c[i] < clo[i]:
                s = -1
            states[t_, i] = s
    raw = (1 * states[0] + 2 * states[1] + 3 * states[2]) / 6
    osc = np.zeros(n)
    a = 2 / 6  # EMA5
    for i in range(n):
        osc[i] = raw[i] if i == 0 else a * raw[i] + (1 - a) * osc[i - 1]
    return osc


def struct_top_signal(pair: str, bars4h: list[dict]) -> Optional[dict]:
    """Проверка последнего ЗАКРЫТОГО 4h-бара. bars4h — закрытые бары
    {'t','o','h','l','c'}, минимум 120 (лучше 300+)."""
    try:
        if not bars4h or len(bars4h) < 120:
            return None
        h = np.array([b["h"] for b in bars4h], dtype=float)
        l = np.array([b["l"] for b in bars4h], dtype=float)
        c = np.array([b["c"] for b in bars4h], dtype=float)
        osc = ms_osc(h, l, c)
        i = len(c) - 1
        if not all(osc[i - j] > 0.9 for j in range(2, 8)):
            return None
        if not (osc[i] < 0.75):
            return None
        price = float(c[i])
        if price <= 0:
            return None
        return {"strategy": "struct_top", "direction": "SHORT",
                "pair": pair, "symbol": pair.replace("/", "").upper(),
                "entry": price, "tp": price * 0.90, "sl": price * 1.05,
                "horizon_h": 96,
                "indicators": {"osc": round(float(osc[i]), 2),
                               "osc_peak": round(float(osc[i - 2]), 2),
                               "bar_t": int(bars4h[i]["t"])}}
    except Exception:
        logger.debug("[struct-top] %s fail", pair, exc_info=True)
        return None
