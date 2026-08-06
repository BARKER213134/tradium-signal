# -*- coding: utf-8 -*-
"""📏 УРОВЕНЬ — сигнал касания уровня-зоны с RSI-гейтом.

Бэктест 05-06.08.26 (год, 303 пары, 200 400 касаний, все ТФ):
- тупое касание = шум (1h EV +0.16R, 1d ≈ 0);
- решает RSI подхода: LONG от поддержки при RSI14 50-70 → WR 68%,
  EV +0.69R (n=1757); SHORT от сопротивления при RSI 30-50 → WR 63%,
  EV +0.58R (n=2613); та же картина независимо на 4h (+0.57/+0.51);
- перепроданность у уровня — ЛОВУШКА: LONG при RSI<30 EV −0.15R,
  SHORT при RSI>70 −0.19R (нож режет уровень) — такие молчат;
- конфлюэнс со старшим ТФ бустит: +1d → WR 81%, EV +1.02R.

Зоны — точный порт клиентского window._kcBuildZones (base.html):
фрактал LW=6, кластеры (tol, кап maxW), отсев пробитых (>2 закрытий
сквозь после последнего касания), полки над/под фитилями, 2 на сторону
в 8% (фолбэк 25%), сила = калиброванная P(отскок).
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

LW = 6


def build_zones(kd: list[dict]) -> list[dict]:
    """Зоны по списку 1h-баров (dict t/o/h/l/c). Порт _kcBuildZones."""
    out: list[dict] = []
    n = len(kd)
    if n < 30:
        return out
    h = [b["h"] for b in kd]
    l = [b["l"] for b in kd]
    c = [b["c"] for b in kd]
    tail0 = max(0, n - 120)
    avg = sum(h[j] - l[j] for j in range(tail0, n)) / max(1, n - tail0)
    px = c[-1]
    if not (avg > 0 and px > 0):
        return out
    tol = min(avg * 0.7, px * 0.008)
    minW = avg * 0.5
    maxW = min(avg * 1.5, px * 0.02)
    ups, dns = [], []
    for i in range(LW, n - LW):
        w_hi = True
        w_lo = True
        for j in range(i - LW, i + LW + 1):
            if h[j] > h[i]:
                w_hi = False
            if l[j] < l[i]:
                w_lo = False
            if not w_hi and not w_lo:
                break
        if w_hi:
            ups.append((h[i], i))
        if w_lo:
            dns.append((l[i], i))

    def cluster(arr):
        cls, cur = [], None
        for p, j in sorted(arr):
            if cur is not None and p - cur["hiP"] <= tol \
                    and p - cur["loP"] <= maxW:
                cur["hiP"] = p
                cur["m"].append(j)
            else:
                cur = {"loP": p, "hiP": p, "m": [j]}
                cls.append(cur)
        for cc in cls:
            cc["touches"] = len(cc["m"])
            cc["lastI"] = max(cc["m"])
        return cls

    def alive(cc, is_res):
        nb = 0
        for j in range(cc["lastI"] + LW, n):
            if (c[j] > cc["hiP"]) if is_res else (c[j] < cc["loP"]):
                nb += 1
                if nb > 2:
                    return False
        return True

    def mk(cc, is_res):
        if (cc["loP"] <= px * 1.002) if is_res else (cc["hiP"] >= px * 0.998):
            return None
        if is_res:
            lo = cc["loP"]
            hi = min(max(cc["hiP"], lo + minW), lo + maxW)
        else:
            hi = cc["hiP"]
            lo = max(min(cc["loP"], hi - minW), hi - maxW)
        if hi - lo < min(avg * 0.15, maxW * 0.5):
            return None
        p = 66.5 if is_res else 61.5          # 1h-базы (см. шапку)
        if cc["touches"] >= 5:
            p += 9
        elif cc["touches"] >= 3:
            p += 3
        wrr = (hi - lo) / avg
        if wrr >= 1.1:
            p += 13
        elif wrr >= 0.7:
            p += 7
        if n - 1 - cc["lastI"] > 72:
            p += 3
        return {"lo": lo, "hi": hi, "res": is_res, "touches": cc["touches"],
                "strength": max(20, min(92, round(p))), "avg": avg}

    for arr, is_res in ((ups, True), (dns, False)):
        cls = [cc for cc in cluster(arr)
               if ((cc["loP"] > px) if is_res else (cc["hiP"] < px))]
        dist = (lambda cc: cc["loP"] / px - 1) if is_res \
            else (lambda cc: 1 - cc["hiP"] / px)
        cls.sort(key=dist)
        acc: list[dict] = []

        def try_add(cc):
            if len(acc) >= 2:
                return
            z = mk(cc, is_res)
            if z is None:
                return
            gap = avg * 0.25
            for oz in acc:
                if z["lo"] < oz["hi"] + gap and oz["lo"] < z["hi"] + gap:
                    return
            acc.append(z)

        for cc in cls:
            if dist(cc) <= 0.08 and alive(cc, is_res):
                try_add(cc)
        if not acc:
            far = [cc for cc in cls if dist(cc) <= 0.25]
            fa = [cc for cc in far if alive(cc, is_res)]
            for cc in (fa if fa else far):
                try_add(cc)
                if acc:
                    break
        out.extend(acc)
    return out


def _rsi14(closes: list[float]) -> Optional[float]:
    if len(closes) < 30:
        return None
    au = ad = 0.0
    rsi = None
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i - 1]
        up, dn = max(ch, 0.0), max(-ch, 0.0)
        if i <= 14:
            au += up / 14
            ad += dn / 14
            if i < 14:
                continue
        else:
            au = (au * 13 + up) / 14
            ad = (ad * 13 + dn) / 14
        rsi = 100.0 if ad == 0 else 100 - 100 / (1 + au / ad)
    return rsi


def mtf_confluence(pair: str, z_lo: float, z_hi: float,
                   is_res: bool) -> Optional[str]:
    """Пересекается ли зона с зоной той же стороны на 4h/1d.
    Возвращает '1d' | '4h' | None. Зовётся ТОЛЬКО после срабатывания
    гейта (редко) — два нативных фетча."""
    try:
        from exchange import get_klines_any
        for tf in ("1d", "4h"):                 # 1d приоритетнее
            kd = get_klines_any(pair, tf, 400)
            if not kd or len(kd) < 60:
                continue
            for z in build_zones(kd):
                if z["res"] == is_res and z["lo"] <= z_hi and z_lo <= z["hi"]:
                    return tf
    except Exception:
        pass
    return None


def check_level_touch(pair: str, kd: list[dict]) -> Optional[dict]:
    """Касание уровня на последнем ЗАКРЫТОМ 1h-баре + RSI-гейт.
    kd — те же 400 баров, что у скана (t в ms)."""
    try:
        if not kd or len(kd) < 120:
            return None
        last = kd[-2]                      # последний закрытый бар
        prev = kd[-3]
        zones = build_zones(kd[:-2])       # зоны БЕЗ касающегося бара
        if not zones:
            return None
        rsi = _rsi14([x["c"] for x in kd[:-1]])
        if rsi is None:
            return None
        for z in zones:
            if z["res"]:
                touched = last["h"] >= z["lo"] and prev["c"] < z["lo"]
                if not touched:
                    continue
                if not (30 <= rsi < 50):
                    return None            # анти-нож: RSI>70 молчим
                direction, edge = "SHORT", z["lo"]
            else:
                touched = last["l"] <= z["hi"] and prev["c"] > z["hi"]
                if not touched:
                    continue
                if not (50 <= rsi < 70):
                    return None
                direction, edge = "LONG", z["hi"]
            # вход = КРАЙ ЗОНЫ (лимитка, исполнена касанием) — критично:
            # проверка 06.08 на году: вход по close бара касания даёт WR
            # 45/48%, вход от края — 63/66% (цели смещаются на ~1% от
            # уровня и съедают весь эдж)
            price = edge
            tp = price * (1.03 if direction == "LONG" else 0.97)
            sl = price * (0.97 if direction == "LONG" else 1.03)
            return {"strategy": "level_touch", "direction": direction,
                    "pair": pair, "symbol": pair.replace("/", "").upper(),
                    "entry": price, "tp": tp, "sl": sl, "horizon_h": 96,
                    "indicators": {
                        "touch_px": last["c"],
                        "rsi1h": round(rsi, 1),
                        "strength": z["strength"],
                        "touches": z["touches"],
                        "zone_lo": z["lo"], "zone_hi": z["hi"],
                        "dist_pct": round(abs(price / edge - 1) * 100, 2),
                        "entry_bar_t": int(last["t"] // 1000),
                        "entry_hint": ("отскок от поддержки"
                                       if direction == "LONG"
                                       else "отбой от сопротивления"),
                    }}
        return None
    except Exception:
        logger.debug(f"level_touch fail {pair}", exc_info=True)
        return None
