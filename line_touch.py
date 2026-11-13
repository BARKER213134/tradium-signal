"""📐 Сигнал «касание линии» — тех самых линий, что рисует график (кнопка 📐).

Юзер (13.11): «хочу сигнал касания линии … то, что я присылал на графиках,
даже если пунктир — хочу видеть сигнал, чтобы отследить самому».

trend_lines() — точный порт window._kcTrendLines из templates/base.html:
зигзаг 3·ATR → от разворота линия через следующий более высокий минимум
(поддержка) / более низкий максимум (сопротивление); пробой = закрытие за
линией на 0.25·ATR. На график идут самая старая и самая свежая живая линия
каждого вида — сигналы только по ним. Менять алгоритм — в ОБОИХ местах.

Два события на последнем закрытом баре (2h, окно 500 баров, как у графика):
  line_touch — бар коснулся СПЛОШНОЙ (уже известной) линии и не пробил её.
               Это ровно «точка» на линии графика.
  line_dash  — ПУНКТИР: линия только что проведена (или подтянута) через
               новый разворот — отскок от него на 3·ATR подтвердился на этом
               баре. Раньше этого момента линии на живом графике не было,
               поэтому сигнал приходит уже после отскока.
Поддержка → LONG, сопротивление → SHORT. Пишем в academy_signals: школа
учится, в ленте Академии и на графиках (из Академии) видны маркеры;
журнал и Telegram не трогаем.
"""
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

WIN = 500            # баров в окне — как грузит график
BAR_MS = 7_200_000   # 2h
TP_PCT, SL_PCT, HZ_H = 0.10, 0.05, 96      # канон школы (для маркера на графике)
NAMES = ("line_touch", "line_dash")
# Событий много (~1.6 в день на монету). Все идут в line_signals — это маркеры
# на графиках. В школу (academy_signals) — только линии с 3+ касаниями, иначе
# новый источник удвоил бы поток paper-сделок.
SCHOOL_MIN_TOUCHES = 3
_IDX = {"ok": False}


def for_school(name, ind):
    return (ind.get("touches") or 0) >= SCHOOL_MIN_TOUCHES


def ensure_index(db):
    if _IDX["ok"]:
        return
    try:
        db.line_signals.create_index([("symbol", 1), ("bar_t", -1)])
        db.line_signals.create_index([("created_at", -1)])
        _IDX["ok"] = True
    except Exception:
        pass


def trend_lines(bars):
    """bars — dict-бары (h, l, c) по возрастанию. → линии, которые показал бы
    график: [{up, a, ap, k, end, broken, touches, len, refits, known, tests,
    pi, pp}] (pi/pp — разворот, задавший текущий наклон)."""
    n = len(bars)
    if n < 60:
        return []
    H = [b["h"] for b in bars]
    L = [b["l"] for b in bars]
    C = [b["c"] for b in bars]
    A = [0.0] * n
    a = None
    for i in range(n):
        tr = H[i] - L[i] if i == 0 else max(H[i] - L[i], abs(H[i] - C[i - 1]), abs(L[i] - C[i - 1]))
        a = tr if a is None else (a * 13 + tr) / 14
        A[i] = a
    piv = []     # (kind, i, p, ci)
    d = 0
    hi, hiI, lo, loI = H[0], 0, L[0], 0
    for i in range(1, n):
        h, l, thr = H[i], L[i], 3 * A[i]
        if d >= 0:
            if h > hi:
                hi, hiI = h, i
            if l < hi - thr and i > hiI:
                piv.append(("H", hiI, hi, i))
                d = -1
                lo, loI = l, i
                continue
        if d <= 0:
            if l < lo:
                lo, loI = l, i
            if h > lo + thr and i > loI:
                piv.append(("L", loI, lo, i))
                d = 1
                hi, hiI = h, i

    def mk(kind):
        up = kind == "L"
        P = [p for p in piv if p[0] == kind]
        by_i = {p[1]: p for p in P}
        res = []

        def tests(ai, ap, kk, known, end):
            t = []
            last_t = -99
            for i in range(known + 1, end + 1):
                lv = ap + kk * (i - ai)
                if (L[i] <= lv + 0.25 * A[i]) if up else (H[i] >= lv - 0.25 * A[i]):
                    if i - last_t >= 3:
                        t.append(i)
                    last_t = i
            return t

        for x in range(len(P) - 1):
            _, ai, ap, aci = P[x]
            k = None
            touches = 1
            last_brk = last_k = dead = None
            refits = 0
            k_known = last_known = None
            pi = pp = lpi = lpp = None
            for i in range(ai + 1, n):
                if (C[i] < ap) if up else (C[i] > ap):
                    dead = i
                    break
                pv = by_i.get(i)
                if k is None:
                    if pv and i - ai >= 5 and ((pv[2] > ap) if up else (pv[2] < ap)):
                        k = (pv[2] - ap) / (i - ai)
                        touches = 2
                        k_known = max(aci, pv[3])
                        pi, pp = pv[1], pv[2]
                        if last_k is not None:
                            refits += 1
                    continue
                lv = ap + k * (i - ai)
                tol = 0.25 * A[i]
                if (C[i] < lv - tol) if up else (C[i] > lv + tol):
                    last_brk, last_k, last_known = i, k, k_known
                    lpi, lpp = pi, pp
                    k = None
                    continue
                if pv:
                    if (pv[2] < lv) if up else (pv[2] > lv):
                        k = (pv[2] - ap) / (i - ai)
                        touches += 1
                        k_known = max(aci, pv[3])
                        pi, pp = pv[1], pv[2]
                    elif abs(pv[2] - lv) <= 0.5 * A[i]:
                        touches += 1
            if k is not None and dead is None:
                if n - 1 - ai >= 12 and ((k > 0) if up else (k < 0)):
                    res.append({"up": up, "a": ai, "ap": ap, "k": k, "end": n - 1, "broken": False,
                                "touches": touches, "len": n - 1 - ai, "refits": refits,
                                "known": k_known, "tests": tests(ai, ap, k, k_known, n - 1),
                                "pi": pi, "pp": pp})
            elif last_k is not None and ((last_k > 0) if up else (last_k < 0)):
                end = last_brk
                if end - ai >= 12:
                    res.append({"up": up, "a": ai, "ap": ap, "k": last_k, "end": end, "broken": True,
                                "touches": touches, "len": end - ai, "refits": refits,
                                "known": last_known, "tests": tests(ai, ap, last_k, last_known, end - 1),
                                "pi": lpi, "pp": lpp})
        return res

    def pick(arr):
        act = sorted((x for x in arr if not x["broken"]), key=lambda x: x["a"])
        br = sorted((x for x in arr if x["broken"] and x["len"] >= 20), key=lambda x: -x["end"])
        sel = []
        if act:
            sel.append(act[0])
            if act[-1] is not act[0]:
                sel.append(act[-1])
        if br:
            sel.append(br[0])
        return sel

    return pick(mk("H")) + pick(mk("L"))


def events(bars):
    """bars — ЗАКРЫТЫЕ 2h-бары (dict t/o/h/l/c). → события на последнем баре:
    [(имя источника, направление, показатели)] — не больше одного на имя+направление."""
    try:
        w = bars[-WIN:]
        n = len(w)
        if n < 120:
            return []
        j = n - 1
        px = float(w[j]["c"])
        if not px > 0:
            return []
        best = {}
        for ln in trend_lines(w):
            if ln["broken"]:
                continue
            name = None
            if ln["known"] == j:
                name = "line_dash"
            elif j in ln["tests"]:
                name = "line_touch"
            if not name:
                continue
            lv = ln["ap"] + ln["k"] * (j - ln["a"])
            ind = {"tf": "2h", "kind": "support" if ln["up"] else "resistance",
                   "stage": "dash" if name == "line_dash" else "solid",
                   "line": lv, "line_gap_pct": round((px / lv - 1) * 100, 2),
                   "touches": ln["touches"], "tests": len(ln["tests"]), "refits": ln["refits"],
                   "age_bars": ln["len"], "slope_pct_day": round(ln["k"] / lv * 12 * 100, 2),
                   "anchor_t": int(w[ln["a"]]["t"]), "anchor_p": ln["ap"], "k": ln["k"],
                   "known_t": int(w[ln["known"]]["t"]) if ln["known"] is not None else None}
            if name == "line_dash" and ln["pi"] is not None:
                ind["pivot_t"] = int(w[ln["pi"]]["t"])
                ind["pivot_p"] = ln["pp"]
                ind["bounce_pct"] = round(abs(px / ln["pp"] - 1) * 100, 2)
            key = (name, "LONG" if ln["up"] else "SHORT")
            if key not in best or ind["touches"] > best[key]["touches"]:
                best[key] = ind
        return [(k[0], k[1], v) for k, v in best.items()]
    except Exception:
        logger.debug("[line] events fail", exc_info=True)
        return []


def note(name, direction, ind):
    side = "поддержки" if direction == "LONG" else "сопротивления"
    if name == "line_dash":
        return (f"📐 ПУНКТИР: линия {side} проведена через новый разворот "
                f"(отскок {ind.get('bounce_pct', 0):.1f}% подтверждён) · касаний {ind['touches']}")
    return f"📐 КАСАНИЕ сплошной линии {side} · касаний {ind['touches']}, проверок {ind['tests']}"


def make_doc(sym, name, direction, ind, px, bar_t, created_at, backfill=False):
    k = 1 if direction == "LONG" else -1
    d = {"strategy": name, "direction": direction,
         "pair": sym[:-4] + "/USDT", "symbol": sym,
         "entry": px, "tp": px * (1 + k * TP_PCT), "sl": px * (1 - k * SL_PCT),
         "horizon_h": HZ_H, "state": "WAITING", "indicators": ind,
         "note": note(name, direction, ind), "bar_t": int(bar_t), "line": True,
         "created_at": created_at, "updated_at": created_at}
    if backfill:
        d["backfill"] = True
    return d


def store(db, sym, found, px, bar_t, now):
    """Записать события последнего бара (один док на источник+монету+направление+бар)."""
    n = 0
    ensure_index(db)
    for name, direction, ind in found:
        try:
            if db.line_signals.find_one({"symbol": sym, "bar_t": int(bar_t), "strategy": name,
                                         "direction": direction}, {"_id": 1}):
                continue
            db.line_signals.insert_one(make_doc(sym, name, direction, ind, px, bar_t, now))
            if for_school(name, ind):
                db.academy_signals.insert_one(make_doc(sym, name, direction, ind, px, bar_t, now))
            n += 1
        except Exception:
            logger.debug(f"[line] store fail {sym} {name}", exc_info=True)
    return n


def bar_close_dt(bar_t):
    """время закрытия 2h-бара — naive UTC (конвенция проекта)"""
    return datetime.utcfromtimestamp((int(bar_t) + BAR_MS) / 1000)
