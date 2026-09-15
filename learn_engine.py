# -*- coding: utf-8 -*-
"""🎓 Академия — самообучающийся слой платформы (17.09.26).

Ночной цикл: строит датасет из свечей и сигналов за окно (как бэктесты —
ничего не штампует в документы и не трогает журнал), агрегирует EV-таблицу
по клеткам, ведёт статусы правил с гистерезисом и тенью, ловит деградацию,
обучает LightGBM-челленджер (shadow) и просит Groq написать краткий отчёт.
Модель — в market_state-стиле документе learn_model (_id='active').

Клетка v1: источник × направление × 🧿 × корзина серии MSO 2h.
Супер-клетки: корзины вкладки «Отбор» (🥇🥈🌅🌡) с её запретами.
Правило активируется только при n>=40, обеих половинах окна одного знака
и подтверждении несколькими пересчётами подряд (или сразу при очень
сильной статистике — bootstrap первой ночи).
"""
import logging
import math
import os
import time
from datetime import timedelta

import numpy as np

logger = logging.getLogger(__name__)

FEE = 0.1                 # той же сеткой считают все бэктесты платформы
WINDOW_DAYS = 45
HORIZON_H = 96            # TP+10 / SL-5 / 96ч — канон
MIN_N = 40
SHRINK_K = 25             # подтяжка малых клеток к родителю
CONFIRM_RUNS = 3          # пересчётов подряд до ACTIVE
STRONG_N = 60             # bootstrap: сразу ACTIVE при очень сильной клетке
DEGRADE_DAYS = 14
DEGRADE_MIN_N = 25

BUCKETS = ["r27", "r1_26", "z0", "g1_4", "g5_9", "g10_15", "g16_26", "g27"]
BUCKET_LABEL = {"r27": "🔴27+", "r1_26": "🔴1-26", "z0": "0",
                "g1_4": "🟢1-4", "g5_9": "🟢5-9", "g10_15": "🟢10-15",
                "g16_26": "🟢16-26", "g27": "🟢27+"}


def bucket(v):
    if v is None:
        return None
    if v >= 27:
        return "g27"
    if v >= 16:
        return "g16_26"
    if v >= 10:
        return "g10_15"
    if v >= 5:
        return "g5_9"
    if v >= 1:
        return "g1_4"
    if v <= -27:
        return "r27"
    if v <= -1:
        return "r1_26"
    return "z0"


# ────────────────────────── индикаторы (как в бэктестах) ──────────────────────────

def _resample(c1, tf_h):
    """1h dict-бары → (t_ms[], o[], h[], l[], c[]) старшего ТФ."""
    tf_ms = tf_h * 3_600_000
    out_t, out_o, out_h, out_l, out_c = [], [], [], [], []
    cur = None
    for b in c1:
        bt = int(b["t"]) // tf_ms * tf_ms
        if cur is None or bt != cur:
            cur = bt
            out_t.append(bt)
            out_o.append(b["o"])
            out_h.append(b["h"])
            out_l.append(b["l"])
            out_c.append(b["c"])
        else:
            out_h[-1] = max(out_h[-1], b["h"])
            out_l[-1] = min(out_l[-1], b["l"])
            out_c[-1] = b["c"]
    return (np.array(out_t, dtype=np.int64), np.array(out_o), np.array(out_h),
            np.array(out_l), np.array(out_c))


def _ema_trend_series(closes):
    n = len(closes)
    e20 = np.full(n, np.nan)
    e50 = np.full(n, np.nan)
    if n >= 20:
        e20[19] = float(np.mean(closes[:20]))
        k = 2 / 21
        for i in range(20, n):
            e20[i] = closes[i] * k + e20[i - 1] * (1 - k)
    if n >= 50:
        e50[49] = float(np.mean(closes[:50]))
        k = 2 / 51
        for i in range(50, n):
            e50[i] = closes[i] * k + e50[i - 1] * (1 - k)
    out = []
    for i in range(n):
        if math.isnan(e20[i]) or math.isnan(e50[i]):
            out.append("NA")
        elif abs(e20[i] - e50[i]) / max(closes[i], 1e-12) * 100 < 0.05:
            out.append("FLAT")
        else:
            out.append("UP" if e20[i] > e50[i] else "DOWN")
    return out


def _streak_series(t2, o2, h2, l2, c2):
    """Знаковая серия MSO 2h по закрытым барам (порог 50)."""
    from mso_retest import mso_series
    bars = [{"t": int(t2[i]), "o": float(o2[i]), "h": float(h2[i]),
             "l": float(l2[i]), "c": float(c2[i])} for i in range(len(t2))]
    osc = mso_series(bars)
    st = np.zeros(len(bars), dtype=int)
    for i in range(1, len(bars)):
        v = osc[i]
        if v is None or (isinstance(v, float) and math.isnan(v)):
            st[i] = 0
        elif v > 50:
            st[i] = st[i - 1] + 1 if st[i - 1] > 0 else 1
        else:
            st[i] = st[i - 1] - 1 if st[i - 1] < 0 else -1
    return st


def _last_closed(t_arr, ts_ms, tf_ms):
    return int(np.searchsorted(t_arr + tf_ms, ts_ms + 1) - 1)


def _outcome(c1, i, sg):
    entry = c1[i]["c"]
    tp = entry * (1 + sg * 0.10)
    sl = entry * (1 - sg * 0.05)
    for m in range(i + 1, min(i + HORIZON_H + 1, len(c1))):
        hi, lo = c1[m]["h"], c1[m]["l"]
        if (lo <= sl) if sg > 0 else (hi >= sl):
            return -5.0 - FEE
        if (hi >= tp) if sg > 0 else (lo <= tp):
            return 10.0 - FEE
    m = min(i + HORIZON_H, len(c1) - 1)
    return (c1[m]["c"] / entry - 1) * 100 * sg - FEE


EXIT_VARIANTS = {
    "base": "TP+10 / SL−5 / 96ч (канон)",
    "be5": "после +5% стоп в безубыток",
    "trail4": "после +5% трейл 4% от макс. закрытия",
    "half5": "на +5% фикс половины, остаток по канону",
    "sl35": "узкий стоп −3.5%",
    "tp15": "дальний тейк +15%",
    "h48": "короткий горизонт 48ч",
}


def _outcome_variants(c1, i, sg):
    """Исход сделки при 7 вариантах выхода. Внутри бара SL приоритетнее
    TP (как в каноне); активация BE/трейла применяется со СЛЕДУЮЩЕГО
    бара (консервативно, без внутрибарного чуда)."""
    entry = c1[i]["c"]
    out = {}

    def px(sig, p):
        return (p / entry - 1) * 100 * sig - FEE

    for name in EXIT_VARIANTS:
        tp_pct = 0.15 if name == "tp15" else 0.10
        sl_pct = 0.035 if name == "sl35" else 0.05
        hor = 48 if name == "h48" else HORIZON_H
        tp = entry * (1 + sg * tp_pct)
        sl = entry * (1 - sg * sl_pct)
        act = entry * (1 + sg * 0.05)          # уровень активации +5%
        armed = False                          # BE/трейл активирован
        peak = entry                           # макс. закрытие в сторону сделки
        half_booked = False
        r = None
        for m in range(i + 1, min(i + hor + 1, len(c1))):
            hi, lo, cl = c1[m]["h"], c1[m]["l"], c1[m]["c"]
            # текущий стоп
            cur_sl = sl
            if name == "be5" and armed:
                cur_sl = entry
            elif name == "trail4" and armed:
                cur_sl = peak * (1 - sg * 0.04)
                if (cur_sl < sl) if sg > 0 else (cur_sl > sl):
                    cur_sl = sl
            hit_sl = (lo <= cur_sl) if sg > 0 else (hi >= cur_sl)
            hit_tp = (hi >= tp) if sg > 0 else (lo <= tp)
            if hit_sl:
                rr = px(sg, cur_sl)
                if name == "half5" and half_booked:
                    rr = 0.5 * (5.0 - FEE) + 0.5 * rr
                r = rr
                break
            if name in ("be5", "trail4") and not armed:
                if (hi >= act) if sg > 0 else (lo <= act):
                    armed = True
            if name == "half5" and not half_booked:
                if (hi >= act) if sg > 0 else (lo <= act):
                    half_booked = True
            if hit_tp:
                rr = tp_pct * 100 - FEE
                if name == "half5" and half_booked:
                    rr = 0.5 * (5.0 - FEE) + 0.5 * rr
                r = rr
                break
            if sg > 0:
                peak = max(peak, cl)
            else:
                peak = min(peak, cl)
        if r is None:
            m = min(i + hor, len(c1) - 1)
            rr = px(sg, c1[m]["c"])
            if name == "half5" and half_booked:
                rr = 0.5 * (5.0 - FEE) + 0.5 * rr
            r = rr
        out[name] = r
    return out


# ────────────────────────── датасет ──────────────────────────

def _load_signals(days):
    from database import _get_db, utcnow
    db = _get_db()
    since = utcnow() - timedelta(days=days)
    sigs = []
    for d in db.new_strategy_signals.find(
            {"created_at": {"$gte": since},
             "direction": {"$in": ["LONG", "SHORT"]}},
            {"pair": 1, "direction": 1, "strategy": 1, "created_at": 1}):
        if d.get("pair") and d.get("strategy"):
            sigs.append({"pair": d["pair"], "dir": d["direction"],
                         "src": d["strategy"],
                         "ts": int(d["created_at"].timestamp() * 1000)})
    for d in db.supertrend_signals.find(
            {"created_at": {"$gte": since},
             "direction": {"$in": ["LONG", "SHORT"]}},
            {"pair": 1, "direction": 1, "tier": 1, "created_at": 1}):
        if d.get("pair"):
            sigs.append({"pair": d["pair"], "dir": d["direction"],
                         "src": "supertrend_" + (d.get("tier") or "?"),
                         "ts": int(d["created_at"].timestamp() * 1000)})
    return sigs


def build_rows(days=WINDOW_DAYS, sleep_s=0.15, progress=None):
    """Полный пересбор датасета из свечей (sync; звать в to_thread).

    Возвращает rows: [{src, dir(1/-1), val, bk, streak, tr1, tr2, tr4,
    breadth, gold/silver/dawn/heat, r, ts, sym}]."""
    from exchange import get_klines_any
    from backtest_supertrend import compute_st_series
    from database import utcnow
    now_ms = int(utcnow().timestamp() * 1000)
    sigs = _load_signals(days)
    by_sym = {}
    for s in sigs:
        by_sym.setdefault(s["pair"], []).append(s)
    packs = {}
    for k, (pair, lst) in enumerate(sorted(by_sym.items())):
        try:
            c1 = get_klines_any(pair, "1h", 1500)
            if not c1 or len(c1) < 250:
                continue
            t1 = np.array([b["t"] for b in c1], dtype=np.int64)
            cl1 = np.array([b["c"] for b in c1])
            t2, o2, h2, l2, c2 = _resample(c1, 2)
            t4, o4, h4, l4, c4 = _resample(c1, 4)
            t12, o12, h12, l12, c12 = _resample(c1, 12)
            b4 = [{"t": int(t4[i]), "o": float(o4[i]), "h": float(h4[i]),
                   "l": float(l4[i]), "c": float(c4[i])} for i in range(len(t4))]
            b12 = [{"t": int(t12[i]), "o": float(o12[i]), "h": float(h12[i]),
                    "l": float(l12[i]), "c": float(c12[i])} for i in range(len(t12))]
            dvol = 0.0
            try:
                tail = c1[-336:]
                dvol = float(np.median([b.get("v", 0) * b["c"]
                                        for b in tail])) * 24
            except Exception:
                pass
            packs[pair] = {
                "c1": c1, "t1": t1, "dvol": dvol,
                "tr1": _ema_trend_series(cl1),
                "t2": t2, "tr2": _ema_trend_series(c2),
                "st2": _streak_series(t2, o2, h2, l2, c2) if len(t2) >= 130 else None,
                "t4": t4, "tr4": _ema_trend_series(c4),
                "st4": compute_st_series(b4, 10, 3.0) if len(b4) >= 30 else None,
                "t12": t12,
                "st12": compute_st_series(b12, 10, 3.0) if len(b12) >= 20 else None,
            }
        except Exception:
            logger.debug(f"[academy] pack fail {pair}", exc_info=True)
        if sleep_s and k % 10 == 9:
            time.sleep(sleep_s)
        if progress and k % 50 == 49:
            progress(k + 1, len(by_sym))
    # ширина рынка по 4h-сетке из tr4 всех пар
    if not packs:
        return []
    tmin = min(s["ts"] for s in sigs) - 5 * 86_400_000
    tmax = max(s["ts"] for s in sigs) + 14_400_000
    grid = np.arange(tmin // 14_400_000 * 14_400_000, tmax, 14_400_000)
    mat = np.full((len(packs), len(grid)), np.nan)
    for r, p in enumerate(packs.values()):
        idx = np.searchsorted(p["t4"] + 14_400_000, grid + 1) - 1
        for g, i in enumerate(idx):
            if 0 <= i < len(p["tr4"]):
                tr = p["tr4"][i]
                mat[r, g] = 1.0 if tr == "UP" else (0.0 if tr == "DOWN" else np.nan)
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        breadth = np.nanmean(mat, axis=0)
    prev_ts = {}
    for s in sorted(sigs, key=lambda x: x["ts"]):
        pr = prev_ts.get(s["pair"])
        s["_prev"] = pr
        prev_ts[s["pair"]] = s["ts"]
    rows = []
    for s in sigs:
        p = packs.get(s["pair"])
        if p is None or p["st2"] is None:
            continue
        ts = s["ts"]
        if now_ms - ts < (HORIZON_H + 2) * 3_600_000:
            continue  # исход ещё не дозрел
        i1 = _last_closed(p["t1"], ts, 3_600_000)
        if i1 < 60 or i1 >= len(p["c1"]) - 2:
            continue
        i2 = _last_closed(p["t2"], ts, 7_200_000)
        if i2 < 115 or i2 >= len(p["st2"]):
            continue
        streak = int(p["st2"][i2])
        tr1 = p["tr1"][i1] if 0 <= i1 < len(p["tr1"]) else "NA"
        tr2 = p["tr2"][i2] if 0 <= i2 < len(p["tr2"]) else "NA"
        i4 = _last_closed(p["t4"], ts, 14_400_000)
        tr4 = p["tr4"][i4] if 0 <= i4 < len(p["tr4"]) else "NA"
        sg = 1 if s["dir"] == "LONG" else -1
        px = p["c1"][i1]["c"]
        dmin = None
        for stt, t_arr, tf_ms in ((p["st4"], p["t4"], 14_400_000),
                                  (p["st12"], p["t12"], 43_200_000)):
            if not stt:
                continue
            i = _last_closed(t_arr, ts, tf_ms)
            if 0 <= i < len(stt):
                line = stt[i].get("st")
                if line and px:
                    dd = abs(px / float(line) - 1) * 100
                    dmin = dd if dmin is None else min(dmin, dd)
        g = int(min(len(grid) - 1,
                    max(0, np.searchsorted(grid + 14_400_000, ts + 1) - 1)))
        br = breadth[g]
        if dmin is None or math.isnan(br):
            val = None
        else:
            contra = (br <= 0.45) if sg > 0 else (br >= 0.55)
            val = bool(dmin <= 4.0 and contra)
        bottom = tr1 == "DOWN" and tr2 == "DOWN" and tr4 == "DOWN"
        dawn = tr1 == "UP" and tr4 == "DOWN"
        gold_f = bool(sg > 0 and val is True and streak < 27)
        silver_f = bool(sg > 0 and val is not True and bottom and streak <= -27)
        dawn_f = bool(sg > 0 and val is not True and dawn and streak < 27)
        heat_f = bool(sg < 0 and streak >= 27)
        fresh = s.get("_prev") is None or (ts - s["_prev"]) > 7 * 86_400_000
        vr = (_outcome_variants(p["c1"], i1, sg)
              if (gold_f or silver_f or dawn_f or heat_f) else None)
        rows.append({
            "fresh": fresh, "dvol": p.get("dvol") or 0.0, "vr": vr,
            "src": s["src"], "sg": sg, "val": val, "streak": streak,
            "bk": bucket(streak), "tr4": tr4,
            "br": None if math.isnan(br) else float(br),
            "gold": gold_f, "silver": silver_f,
            "dawn": dawn_f, "heat": heat_f,
            "r": _outcome(p["c1"], i1, sg), "ts": ts, "sym": s["pair"],
        })
    return rows


# ────────────────────────── агрегация и статусы ──────────────────────────

def _stats(sel, tmid):
    a = np.array([x["r"] for x in sel]) if sel else np.array([])
    if not len(a):
        return {"n": 0}
    ts = np.array([x["ts"] for x in sel])
    h1, h2 = a[ts < tmid], a[ts >= tmid]
    stable = bool(len(h1) > 12 and len(h2) > 12
                  and (h1.mean() > 0) == (h2.mean() > 0))
    return {"n": int(len(a)), "wr": round(float((a > 0).mean() * 100), 1),
            "avg": round(float(a.mean()), 3),
            "h1": round(float(h1.mean()), 2) if len(h1) else None,
            "h2": round(float(h2.mean()), 2) if len(h2) else None,
            "stable": stable}


def aggregate(rows, prev_rules=None):
    """rows → список правил со статистикой, статусами и счётчиками."""
    from database import utcnow
    prev = {r["id"]: r for r in (prev_rules or [])}
    tmid = float(np.median([x["ts"] for x in rows]))
    glob = {1: np.mean([x["r"] for x in rows if x["sg"] > 0] or [0]),
            -1: np.mean([x["r"] for x in rows if x["sg"] < 0] or [0])}
    cut14 = max(x["ts"] for x in rows) - DEGRADE_DAYS * 86_400_000
    rules = []

    def add_rule(rid, label, sel, kind):
        st = _stats(sel, tmid)
        if st["n"] < 15:
            return
        # shrinkage к родителю направления
        sgn = 1 if "LONG" in label else -1
        parent = float(glob[sgn])
        ev = round((st["n"] * st["avg"] + SHRINK_K * parent) / (st["n"] + SHRINK_K), 3)
        cand = None
        if st["n"] >= MIN_N and st["stable"]:
            cand = "show" if st["avg"] > 0.3 else ("hide" if st["avg"] < -0.1 else None)
        pv = prev.get(rid) or {}
        runs = (pv.get("runs", 0) + 1) if cand and cand == pv.get("cand") else (1 if cand else 0)
        status = pv.get("status", "NEUTRAL")
        strong = st["n"] >= STRONG_N and st["stable"] and abs(st["avg"]) > 0.6
        if cand and (runs >= CONFIRM_RUNS or strong):
            status = "ACTIVE_SHOW" if cand == "show" else "ACTIVE_HIDE"
        elif cand:
            status = "SHADOW"
        else:
            status = "NEUTRAL"
        # деградация: свежие 14д против модельного знака
        recent = [x for x in sel if x["ts"] >= cut14]
        deg = None
        if status.startswith("ACTIVE") and len(recent) >= DEGRADE_MIN_N:
            ra = float(np.mean([x["r"] for x in recent]))
            want_pos = status == "ACTIVE_SHOW"
            if (ra > 0) != want_pos:
                deg = round(ra, 2)
                status = "SHADOW"   # разжалование
                runs = 0
        rules.append({"id": rid, "label": label, "kind": kind, **st,
                      "ev": ev, "cand": cand, "runs": runs, "status": status,
                      "degraded": deg,
                      "recent_n": len(recent),
                      "updated": utcnow().isoformat()})

    # супер-клетки Отбора
    add_rule("sc_gold", "🥇 LONG валидные (серия <27)",
             [x for x in rows if x["gold"]], "super")
    add_rule("sc_silver", "🥈 LONG дно+красная 27+",
             [x for x in rows if x["silver"]], "super")
    add_rule("sc_dawn", "🌅 LONG ранний разворот",
             [x for x in rows if x["dawn"]], "super")
    add_rule("sc_heat", "🌡 SHORT зелёная 27+",
             [x for x in rows if x["heat"]], "super")
    add_rule("sc_fresh", "🆕 LONG первое касание 7д (лонг-корзины)",
             [x for x in rows if x["fresh"]
              and (x["gold"] or x["silver"] or x["dawn"])], "super")
    add_rule("sc_stale", "LONG повторное касание (лонг-корзины)",
             [x for x in rows if not x["fresh"]
              and (x["gold"] or x["silver"] or x["dawn"])], "super")
    # клетки источник × направление × 🧿 × корзина
    combos = {}
    for x in rows:
        v = "T" if x["val"] is True else ("F" if x["val"] is False else "N")
        combos.setdefault((x["src"], x["sg"], v, x["bk"]), []).append(x)
    for (src, sg, v, bk), sel in combos.items():
        dl = "LONG" if sg > 0 else "SHORT"
        vl = {"T": "🧿", "F": "без🧿", "N": "🧿?"}[v]
        add_rule(f"c_{src}_{dl}_{v}_{bk}",
                 f"{src} {dl} {vl} {BUCKET_LABEL[bk]}", sel, "cell")
    # клетки источник × направление (родители)
    combos2 = {}
    for x in rows:
        combos2.setdefault((x["src"], x["sg"]), []).append(x)
    for (src, sg), sel in combos2.items():
        dl = "LONG" if sg > 0 else "SHORT"
        add_rule(f"p_{src}_{dl}", f"{src} {dl} (весь)", sel, "parent")
    rules.sort(key=lambda r: -(r.get("ev") or 0))
    return rules


def exits_table(rows):
    """🚪 Самообучаемые выходы: статистика вариантов по супер-клеткам,
    рекомендация — лучший стабильный вариант (обе половины одного знака,
    n>=100), иначе канон."""
    tmid = float(np.median([x["ts"] for x in rows]))
    out = {}
    for bk, label in (("gold", "🥇"), ("silver", "🥈"),
                      ("dawn", "🌅"), ("heat", "🌡")):
        sel = [x for x in rows if x[bk] and x["vr"]]
        if len(sel) < 100:
            continue
        tab = {}
        for v in EXIT_VARIANTS:
            a = np.array([x["vr"][v] for x in sel])
            ts = np.array([x["ts"] for x in sel])
            h1, h2 = a[ts < tmid], a[ts >= tmid]
            stable = bool(len(h1) > 20 and len(h2) > 20
                          and (h1.mean() > 0) == (h2.mean() > 0))
            tab[v] = {"avg": round(float(a.mean()), 3),
                      "wr": round(float((a > 0).mean() * 100), 1),
                      "h1": round(float(h1.mean()), 2) if len(h1) else None,
                      "h2": round(float(h2.mean()), 2) if len(h2) else None,
                      "stable": stable}
        best = max((v for v in tab if tab[v]["stable"]),
                   key=lambda v: tab[v]["avg"], default="base")
        out[bk] = {"n": len(sel), "table": tab, "best": best,
                   "best_label": EXIT_VARIANTS[best],
                   "base_avg": tab["base"]["avg"],
                   "best_avg": tab[best]["avg"]}
    return out


def liq_split(rows):
    """💧 Эдж по ликвидности: портфель супер-клеток, верхняя/нижняя
    половина по суточному $-объёму монеты."""
    sel = [x for x in rows
           if (x["gold"] or x["silver"] or x["dawn"] or x["heat"])
           and x["dvol"] > 0]
    if len(sel) < 200:
        return None
    med = float(np.median([x["dvol"] for x in sel]))

    def st(ss):
        a = np.array([x["r"] for x in ss])
        return {"n": int(len(a)), "avg": round(float(a.mean()), 3),
                "wr": round(float((a > 0).mean() * 100), 1)}
    return {"median_dvol": int(med),
            "hi": st([x for x in sel if x["dvol"] >= med]),
            "lo": st([x for x in sel if x["dvol"] < med])}


def size_tier(rule):
    """Kelly-лайт: размер позиции от силы клетки."""
    if not rule:
        return None
    ev, wr = rule.get("ev") or 0, rule.get("wr") or 0
    if ev >= 3.0 and wr >= 60:
        return "2x"
    if ev >= 1.0:
        return "1x"
    return "0.5x"


# ────────────────────────── LightGBM-челленджер (shadow) ──────────────────────────

def train_lgbm(rows):
    """P(win) на тех же признаках; walk-forward: train 75% → OOS 25%.
    Только тень: метрики в модель, гейтом не является."""
    try:
        import lightgbm as lgb
    except Exception:
        return {"ok": False, "reason": "lightgbm не установлен"}
    if len(rows) < 800:
        return {"ok": False, "reason": f"мало данных ({len(rows)})"}
    srcs = sorted({x["src"] for x in rows})
    s_idx = {s: i for i, s in enumerate(srcs)}
    tr_map = {"UP": 1, "DOWN": -1, "FLAT": 0, "NA": 0}

    import math as _m

    def feats(x):
        return [s_idx[x["src"]], x["sg"],
                1 if x["val"] is True else (-1 if x["val"] is False else 0),
                x["streak"], tr_map.get(x["tr4"], 0),
                -1.0 if x["br"] is None else round(x["br"], 3),
                1 if x.get("fresh") else 0,
                round(_m.log10(max(x.get("dvol") or 1, 1)), 2)]

    rs = sorted(rows, key=lambda x: x["ts"])
    cut = int(len(rs) * 0.75)
    Xtr = np.array([feats(x) for x in rs[:cut]], dtype=float)
    ytr = np.array([1 if x["r"] > 0 else 0 for x in rs[:cut]])
    Xte = np.array([feats(x) for x in rs[cut:]], dtype=float)
    rte = np.array([x["r"] for x in rs[cut:]])
    yte = (rte > 0).astype(int)
    m = lgb.LGBMClassifier(n_estimators=200, num_leaves=15, learning_rate=0.06,
                           min_child_samples=40, verbose=-1,
                           categorical_feature=[0])
    m.fit(Xtr, ytr)
    p = m.predict_proba(Xte)[:, 1]
    # AUC руками (без sklearn)
    order = np.argsort(p)
    ranks = np.empty(len(p))
    ranks[order] = np.arange(1, len(p) + 1)
    n1, n0 = int(yte.sum()), int(len(yte) - yte.sum())
    auc = float((ranks[yte == 1].sum() - n1 * (n1 + 1) / 2) / max(n1 * n0, 1))
    top = p >= np.percentile(p, 80)
    fi = sorted(zip(["src", "dir", "val", "streak", "tr4", "breadth",
                     "fresh", "liq"],
                    m.feature_importances_.tolist()), key=lambda z: -z[1])
    return {"ok": True, "n_train": cut, "n_test": len(rs) - cut,
            "auc": round(auc, 3),
            "oos_all_avg": round(float(rte.mean()), 3),
            "oos_top20_avg": round(float(rte[top].mean()), 3),
            "oos_top20_wr": round(float(yte[top].mean() * 100), 1),
            "oos_top20_n": int(top.sum()),
            "feat_imp": [[a, int(b)] for a, b in fi]}


# ────────────────────────── Groq-аналитик (fail-open) ──────────────────────────

def _groq_key():
    k = os.getenv("GROQ_API_KEY")
    if k:
        return k
    try:
        from database import _get_db
        d = _get_db().system_config.find_one({"_id": "groq_api_key"}) or {}
        return d.get("value")
    except Exception:
        return None


def groq_brief(model):
    key = _groq_key()
    if not key:
        return None
    try:
        import requests
        mdl = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")
        act = [r for r in model["rules"] if r["status"].startswith("ACTIVE")][:14]
        deg = [r for r in model["rules"] if r.get("degraded") is not None]
        lg = model.get("lgbm") or {}
        lines = [f"{r['label']}: {r['status']} n={r['n']} WR={r['wr']} avgR={r['avg']}"
                 for r in act]
        dl = [f"{r['label']}: свежие14д avgR={r['degraded']}" for r in deg]
        prompt = (
            "Ты аналитик крипто-платформы сигналов. Ночной пересчёт "
            "самообучаемой модели дал результат. Напиши краткий отчёт "
            "по-русски (4-6 предложений, без воды, трейдеру): что сейчас "
            "работает, что отвалилось, на что обратить внимание завтра.\n"
            f"Окно {model['window_days']}д, сигналов {model['rows_n']}.\n"
            "Активные правила:\n" + "\n".join(lines) +
            ("\nДеградация:\n" + "\n".join(dl) if dl else "\nДеградаций нет.") +
            (f"\nLightGBM (тень): AUC={lg.get('auc')}, топ-20% сигналов "
             f"avgR={lg.get('oos_top20_avg')} vs все {lg.get('oos_all_avg')}."
             if lg.get("ok") else ""))
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}"},
            json={"model": mdl,
                  "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 600, "temperature": 0.3},
            timeout=30)
        if r.status_code == 200:
            return r.json()["choices"][0]["message"]["content"].strip()
        logger.warning(f"[academy] groq {r.status_code}: {r.text[:200]}")
    except Exception:
        logger.debug("[academy] groq fail", exc_info=True)
    return None


# ────────────────────────── оркестрация ──────────────────────────

def recompute(days=WINDOW_DAYS, progress=None):
    """Полный ночной пересчёт (sync; звать в to_thread). Возвращает модель."""
    from database import _get_db, utcnow
    db = _get_db()
    t0 = time.time()
    rows = build_rows(days=days, progress=progress)
    if len(rows) < 500:
        logger.warning(f"[academy] мало строк: {len(rows)} — модель не обновляю")
        return None
    prev = db.learn_model.find_one({"_id": "active"}) or {}
    rules = aggregate(rows, prev.get("rules"))
    lgbm = train_lgbm(rows)
    model = {
        "_id": "active",
        "version": int(prev.get("version", 0)) + 1,
        "window_days": days, "rows_n": len(rows),
        "syms_n": len({x["sym"] for x in rows}),
        "built_at": utcnow().isoformat(),
        "build_sec": int(time.time() - t0),
        "rules": rules, "lgbm": lgbm,
        "exits": exits_table(rows), "liq": liq_split(rows),
        "n_show": sum(1 for r in rules if r["status"] == "ACTIVE_SHOW"),
        "n_hide": sum(1 for r in rules if r["status"] == "ACTIVE_HIDE"),
        "n_shadow": sum(1 for r in rules if r["status"] == "SHADOW"),
        "degraded": [r["id"] for r in rules if r.get("degraded") is not None],
    }
    model["brief"] = groq_brief(model)
    db.learn_model.replace_one({"_id": "active"}, model, upsert=True)
    hist = dict(model)
    hist["_id"] = f"v{model['version']}_{model['built_at'][:16]}"
    hist.pop("rules", None)   # история — только метаданные, без таблицы
    try:
        db.learn_model_history.insert_one(hist)
    except Exception:
        pass
    logger.info(f"[academy] модель v{model['version']}: {len(rules)} правил, "
                f"{model['n_show']} SHOW / {model['n_hide']} HIDE, "
                f"{model['build_sec']}с")
    return model


# ────────────────────────── live-скоринг для вкладки ──────────────────────────

def score_signal(model, src, direction, validator_ok, streak):
    """Вердикт по штампам сигнала: (status, rule) или (None, None)."""
    if not model:
        return None, None
    rules = {r["id"]: r for r in model.get("rules") or []}
    sgl = "LONG" if direction == "LONG" else "SHORT"
    v = "T" if validator_ok is True else ("F" if validator_ok is False else "N")
    bk = bucket(streak)
    for rid in ([f"c_{src}_{sgl}_{v}_{bk}"] if bk else []) + [f"p_{src}_{sgl}"]:
        r = rules.get(rid)
        if r and r["status"] in ("ACTIVE_SHOW", "ACTIVE_HIDE") and r["n"] >= MIN_N:
            return r["status"], r
    return None, None
