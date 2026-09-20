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


# ₿ РЕЖИМ BTC (20.09): просадка от 30д-макс по ДНЕВНЫМ закрытиям до вчера
# (без look-ahead). Бэктест коррекции 20.05→08.07: шорты +3.97 (WR 65)
# при −8..−15%, лонги 🥇 −0.18; у максимума шорты +0.09, лонги +1.88.
REGIME_BINS = ["top", "dip", "corr", "capit"]
REGIME_LABEL = {"top": "₿ у максимума", "dip": "₿ −3..−8%",
                "corr": "₿ −8..−15% коррекция", "capit": "₿ ≤−15% капитуляция"}
REGIME_DAYS = 180


def regime_bin(dd):
    if dd is None:
        return None
    if dd <= -15:
        return "capit"
    if dd <= -8:
        return "corr"
    if dd <= -3:
        return "dip"
    return "top"


def btc_dd_by_day(c1b):
    """{день: dd%} — закрытие последнего бара ДО начала дня / макс.
    закрытий за предыдущие 30 дней − 1. Только прошлое."""
    if not c1b:
        return {}
    t = np.array([b["t"] for b in c1b], dtype=np.int64)
    c = np.array([b["c"] for b in c1b])
    out = {}
    d0 = int(t[0] // 86_400_000) + 1
    d1 = int(t[-1] // 86_400_000) + 1
    for d in range(d0, d1 + 1):
        end = int(np.searchsorted(t, d * 86_400_000))
        start = int(np.searchsorted(t, (d - 30) * 86_400_000))
        if end == 0 or end - start < 24 * 5:
            continue
        mx = float(c[start:end].max())
        if mx > 0:
            out[d] = round((float(c[end - 1]) / mx - 1) * 100, 2)
    return out


_REGIME_CACHE = {"t": 0.0, "v": (None, None)}


def btc_regime_now():
    """(dd%, bin) сейчас по дневным спот-закрытиям BTC до вчера; кэш 30 мин."""
    if time.time() - _REGIME_CACHE["t"] < 1800:
        return _REGIME_CACHE["v"]
    try:
        from exchange import get_klines_any
        kl = get_klines_any("BTC/USDT", "1d", 40)
        closed = [b for b in kl if b["t"] + 86_400_000 <= time.time() * 1000]
        if len(closed) >= 20:
            cl = [b["c"] for b in closed[-31:]]
            dd = round((cl[-1] / max(cl) - 1) * 100, 2)
            _REGIME_CACHE["v"] = (dd, regime_bin(dd))
            _REGIME_CACHE["t"] = time.time()
    except Exception:
        logger.debug("[academy] btc_regime_now fail", exc_info=True)
    return _REGIME_CACHE["v"]


def _last_closed(t_arr, ts_ms, tf_ms):
    return int(np.searchsorted(t_arr + tf_ms, ts_ms + 1) - 1)


def _last_closed_idx(c1):
    """Индекс последнего ЗАКРЫТОГО 1h-бара (формирующийся не считаем)."""
    now_ms = int(time.time() * 1000)
    j = len(c1) - 1
    while j >= 0 and c1[j]["t"] + 3_600_000 > now_ms:
        j -= 1
    return j


def _outcome(c1, i, sg):
    """Исход по канону → (R%, resolved). resolved=False = сделка «в полёте»:
    ни TP, ни SL не коснулись и окно 96ч ещё не закрыто — R тогда по
    текущей цене и в статистику НЕ идёт (только в пульс режима)."""
    entry = c1[i]["c"]
    tp = entry * (1 + sg * 0.10)
    sl = entry * (1 - sg * 0.05)
    for m in range(i + 1, min(i + HORIZON_H + 1, len(c1))):
        hi, lo = c1[m]["h"], c1[m]["l"]
        if (lo <= sl) if sg > 0 else (hi >= sl):
            return -5.0 - FEE, True
        if (hi >= tp) if sg > 0 else (lo <= tp):
            return 10.0 - FEE, True
    m = min(i + HORIZON_H, len(c1) - 1)
    return ((c1[m]["c"] / entry - 1) * 100 * sg - FEE,
            bool(i + HORIZON_H <= _last_closed_idx(c1)))


def inflight_pulse(rows):
    """✈ Пульс «в полёте»: сигналы последних ~96ч с нерешённым исходом,
    оценка по текущей цене — самый свежий индикатор режима. В статистику
    НЕ входит (ни в EV, ни в выходы, ни в LightGBM)."""
    def st(sel):
        if not sel:
            return {"n": 0, "avg": None, "wr": None}
        a = np.array([x["r"] for x in sel])
        return {"n": int(len(a)), "avg": round(float(a.mean()), 2),
                "wr": int(round(float((a > 0).mean() * 100)))}
    port = [x for x in rows
            if x["gold"] or x["silver"] or x["dawn"] or x["heat"]]
    return {"all": st(rows),
            "long": st([x for x in rows if x["sg"] > 0]),
            "short": st([x for x in rows if x["sg"] < 0]),
            "port": st(port)}


EXIT_VARIANTS = {
    "base": "TP+10 / SL−5 / 96ч (канон)",
    "be5": "после +5% стоп в безубыток",
    "trail4": "после +5% трейл 4% от макс. закрытия",
    "half5": "на +5% фикс половины, остаток по канону",
    "sl35": "узкий стоп −3.5%",
    "tp15": "дальний тейк +15%",
    "tp20": "дальний тейк +20%",
    "hold": "без тейка: стоп −5 и держать до 96ч",
    "h48": "короткий горизонт 48ч",
    "reg65": "выход по режиму: широта 4h ≥65% (лонг) / ≤35% (шорт)",
}


def _outcome_variants(c1, i, sg, bgrid=None):
    """Исход сделки при 7 вариантах выхода. Внутри бара SL приоритетнее
    TP (как в каноне); активация BE/трейла применяется со СЛЕДУЮЩЕГО
    бара (консервативно, без внутрибарного чуда)."""
    entry = c1[i]["c"]
    out = {}
    last = _last_closed_idx(c1)
    vres = True   # все варианты решены (иначе строка не идёт в exits_table)

    def px(sig, p):
        return (p / entry - 1) * 100 * sig - FEE

    for name in EXIT_VARIANTS:
        tp_pct = {"tp15": 0.15, "tp20": 0.20, "hold": 9.0}.get(name, 0.10)
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
            if name == "reg65" and bgrid is not None:
                g_, b_ = bgrid
                gi = int(min(len(g_) - 1, max(0, np.searchsorted(
                    g_ + 14_400_000, c1[m]["t"] + 1) - 1)))
                brm = b_[gi]
                if not math.isnan(brm) and (
                        (sg > 0 and brm >= 0.65) or (sg < 0 and brm <= 0.35)):
                    r = px(sg, cl)
                    break
            if sg > 0:
                peak = max(peak, cl)
            else:
                peak = min(peak, cl)
        if r is None:
            if i + hor > last:
                vres = False   # окно не закрыто — исход этого варианта не решён
            m = min(i + hor, len(c1) - 1)
            rr = px(sg, c1[m]["c"])
            if name == "half5" and half_booked:
                rr = 0.5 * (5.0 - FEE) + 0.5 * rr
            r = rr
        out[name] = r
    return out, vres


# ────────────────────────── датасет ──────────────────────────

def _load_signals(days):
    from database import _get_db, utcnow
    db = _get_db()
    since = utcnow() - timedelta(days=days)
    sigs = []
    for d in db.new_strategy_signals.find(
            {"created_at": {"$gte": since},
             "direction": {"$in": ["LONG", "SHORT"]}},
            {"pair": 1, "direction": 1, "strategy": 1, "created_at": 1,
             "svetofor": 1, "svetofor_score": 1}):
        if d.get("pair") and d.get("strategy"):
            sigs.append({"pair": d["pair"], "dir": d["direction"],
                         "src": d["strategy"],
                         "sv": d.get("svetofor"), "sc": d.get("svetofor_score"),
                         "ts": int(d["created_at"].timestamp() * 1000)})
    for d in db.supertrend_signals.find(
            {"created_at": {"$gte": since},
             "direction": {"$in": ["LONG", "SHORT"]}},
            {"pair": 1, "direction": 1, "tier": 1, "created_at": 1,
             "svetofor": 1, "svetofor_score": 1}):
        if d.get("pair"):
            sigs.append({"pair": d["pair"], "dir": d["direction"],
                         "src": "supertrend_" + (d.get("tier") or "?"),
                         "sv": d.get("svetofor"), "sc": d.get("svetofor_score"),
                         "ts": int(d["created_at"].timestamp() * 1000)})
    # 🎓 academy_signals (20.09): стратегии, выключенные для журнала/TG
    # (аудит 180д), продолжают учиться в Академии — отдельная коллекция
    for d in db.academy_signals.find(
            {"created_at": {"$gte": since},
             "direction": {"$in": ["LONG", "SHORT"]}},
            {"pair": 1, "direction": 1, "strategy": 1, "created_at": 1,
             "svetofor": 1, "svetofor_score": 1}):
        if d.get("pair") and d.get("strategy"):
            sigs.append({"pair": d["pair"], "dir": d["direction"],
                         "src": d["strategy"],
                         "sv": d.get("svetofor"), "sc": d.get("svetofor_score"),
                         "ts": int(d["created_at"].timestamp() * 1000)})
    return sigs


def build_rows(days=WINDOW_DAYS, sleep_s=0.15, progress=None,
               sigs=None, klines_fn=None, fund=True):
    """Полный пересбор датасета из свечей (sync; звать в to_thread).

    Возвращает rows: [{src, dir(1/-1), val, bk, streak, tr1, tr2, tr4,
    breadth, gold/silver/dawn/heat, r, ts, sym}]."""
    from exchange import get_klines_any
    from backtest_supertrend import compute_st_series
    from database import utcnow
    now_ms = int(utcnow().timestamp() * 1000)
    ages = {}
    try:
        from database import _get_db as _gdb_a
        for d in _gdb_a().coin_ages.find({}, {"days": 1}):
            ages[d["_id"]] = d.get("days")
    except Exception:
        pass
    sigs = _load_signals(days) if sigs is None else sigs
    by_sym = {}
    for s in sigs:
        by_sym.setdefault(s["pair"], []).append(s)
    by_sym.setdefault("BTC/USDT", [])   # ₿ режим/вола BTC нужны всегда
    # 💸 история фандинга (8ч-выплаты, limit 1000 ≈ 333д) — фича обучения;
    # локально fapi 451 → fail-open (None)
    fund_hist = {}
    try:
        import requests as _rq
        _probe = _rq.get("https://fapi.binance.com/fapi/v1/fundingRate",
                         params={"symbol": "BTCUSDT", "limit": 1},
                         timeout=8)
        fapi_ok = _probe.status_code == 200
    except Exception:
        fapi_ok = False
    if fapi_ok and fund:
        for k, pair in enumerate(sorted(by_sym)):
            try:
                rr = _rq.get(
                    "https://fapi.binance.com/fapi/v1/fundingRate",
                    params={"symbol": pair.replace("/", ""), "limit": 1000},
                    timeout=10)
                if rr.status_code == 200:
                    fund_hist[pair] = [(int(x["fundingTime"]),
                                        float(x["fundingRate"]))
                                       for x in rr.json()]
            except Exception:
                pass
            if sleep_s and k % 20 == 19:
                time.sleep(sleep_s)
    packs = {}
    for k, (pair, lst) in enumerate(sorted(by_sym.items())):
        try:
            c1 = (klines_fn or get_klines_any)(pair, "1h", 1500)
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
    # ₿ BTC-вола: перцентиль скользящего 24ч-диапазона на момент бара
    # (только по прошлому — без заглядывания вперёд)
    btc_vt, btc_vp = None, None
    bp = packs.get("BTC/USDT")
    if bp is not None:
        c1b = bp["c1"]
        rng = np.array([(b["h"] - b["l"]) / b["c"] * 100 for b in c1b])
        r24 = np.full(len(rng), np.nan)
        for i in range(24, len(rng)):
            r24[i] = rng[i - 24:i].mean()
        btc_vt = bp["t1"]
        btc_vp = np.full(len(rng), np.nan)
        for i in range(60, len(rng)):
            hist = r24[24:i]
            hist = hist[~np.isnan(hist)]
            if len(hist) >= 30:
                btc_vp[i] = (hist < r24[i]).mean() * 100
    btc_dd = btc_dd_by_day(bp["c1"]) if bp is not None else {}
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
        # 20.09: свежие сигналы НЕ выбрасываем (раньше — минус 98ч = модель
        # отставала на 4 дня). Исход берём, если он уже РЕШЁН (TP/SL);
        # «в полёте» помечаем res=False — в статистику не идут, только пульс.
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
        vr, vres = (_outcome_variants(p["c1"], i1, sg, (grid, breadth))
                    if (gold_f or silver_f or dawn_f or heat_f)
                    else (None, True))
        _r, _res = _outcome(p["c1"], i1, sg)
        _age = ages.get(s["pair"])
        _fund = None
        fh = fund_hist.get(s["pair"])
        if fh:
            _fr = [r_ for t_, r_ in fh if t_ <= ts]
            if _fr:
                _fund = _fr[-1]
        _bv = None
        if btc_vt is not None:
            _bi = _last_closed(btc_vt, ts, 3_600_000)
            if 0 <= _bi < len(btc_vp) and not math.isnan(btc_vp[_bi]):
                _bv = round(float(btc_vp[_bi]), 1)
        _sc = s.get("sc")
        _dd = btc_dd.get(int(ts // 86_400_000))
        rows.append({
            "btc_dd": _dd, "rg": regime_bin(_dd),
            "sv": s.get("sv"),
            "sc": (None if _sc is None or _sc <= -50 else _sc),
            "fund": _fund, "btc_vol": _bv,
            "hour": int((ts // 3_600_000) % 24),
            "age": _age,
            "dmin": dmin, "young": (None if _age is None else bool(_age < 70)),
            "fresh": fresh, "dvol": p.get("dvol") or 0.0, "vr": vr,
            "vres": vres, "res": _res,
            "src": s["src"], "sg": sg, "val": val, "streak": streak,
            "bk": bucket(streak), "tr4": tr4,
            "br": None if math.isnan(br) else float(br),
            "gold": gold_f, "silver": silver_f,
            "dawn": dawn_f, "heat": heat_f,
            "r": _r, "ts": ts, "sym": s["pair"],
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


def aggregate(rows, prev_rules=None, live_map=None, only_regime=False):
    """rows → список правил со статистикой, статусами и счётчиками.
    live_map: {rule_id: {n, wr, avg}} из закрытых paper-сделок — живые
    исходы подмешиваются в EV с тройным весом (реальное исполнение
    важнее свечной симуляции)."""
    from database import utcnow
    prev = {r["id"]: r for r in (prev_rules or [])}
    live_map = live_map or {}
    tmid = float(np.median([x["ts"] for x in rows]))
    tmax = max(x["ts"] for x in rows)
    glob = {1: np.mean([x["r"] for x in rows if x["sg"] > 0] or [0]),
            -1: np.mean([x["r"] for x in rows if x["sg"] < 0] or [0])}
    cut14 = max(x["ts"] for x in rows) - DEGRADE_DAYS * 86_400_000
    rules = []

    def add_rule(rid, label, sel, kind):
        if len(sel) < 15:
            return
        # режимные клетки лежат кучно во времени (коррекция = май-июнь) —
        # половины считаем ВНУТРИ режима, иначе «стабильность» по общей
        # медиане окна всегда ложно-отрицательная
        st = _stats(sel, float(np.median([x["ts"] for x in sel]))
                    if kind == "regime" else tmid)
        if st["n"] < 15:
            return
        # shrinkage к родителю направления
        sgn = 1 if "LONG" in label else -1
        parent = float(glob[sgn])
        ev = round((st["n"] * st["avg"] + SHRINK_K * parent) / (st["n"] + SHRINK_K), 3)
        cand = None
        if st["n"] >= MIN_N and st["stable"]:
            cand = "show" if st["avg"] > 0.3 else ("hide" if st["avg"] < -0.1 else None)
        # 📈 спарклайн жизни: WR по 6 последним неделям (свежая слева)
        wk = []
        for w in range(6):
            lo = tmax - (w + 1) * 7 * 86_400_000
            hi = tmax - w * 7 * 86_400_000
            a = [x["r"] for x in sel if lo < x["ts"] <= hi]
            wk.append(round(sum(1 for r_ in a if r_ > 0) / len(a) * 100)
                      if len(a) >= 5 else None)
        # 📜 живые paper-исходы этого правила
        lv = live_map.get(rid)
        if lv and lv["n"] >= 30:
            ev = round((st["n"] * st["avg"] + 3 * lv["n"] * lv["avg"]
                        + SHRINK_K * parent)
                       / (st["n"] + 3 * lv["n"] + SHRINK_K), 3)
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
        # 📜 ЖИВАЯ деградация (17.09): реальные paper-исходы бьют
        # симуляцию — правило с n>=30 живых сделок и сильным минусом
        # выключается, даже если свечная статистика всё ещё «за»
        # (кейс supertrend_vip SHORT 🔴1-26: сим +0.85, живые −5.1)
        live_dem = None
        # катастрофа (avg<-2.5, WR<15) выключается уже с n>=15 — ждать 30
        # сверок для редких правил значило травить паперу неделями (18.09:
        # vip SHORT 🔴1-26 давал ~2 сигнала/день при live 22×−5.1)
        if lv and status == "ACTIVE_SHOW" and (
                (lv["n"] >= 30 and lv["avg"] < -1.0)
                or (lv["n"] >= 15 and lv["avg"] < -2.5 and lv["wr"] < 15)):
            live_dem = lv["avg"]
            status = "ACTIVE_HIDE" if lv["avg"] < -2.5 else "SHADOW"
            runs = 0
            if deg is None:
                deg = round(lv["avg"], 2)
        rules.append({"id": rid, "label": label, "kind": kind, **st,
                      "wk": wk, "live": lv, "live_demoted": live_dem,
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
    add_rule("sc_heat_young", "🌡 SHORT 27+ · монета моложе 70д",
             [x for x in rows if x["heat"] and x["young"] is True], "super")
    add_rule("sc_heat_old", "🌡 SHORT 27+ · монета старше 70д",
             [x for x in rows if x["heat"] and x["young"] is False], "super")
    # 🚦 светофор — контрарианский агрегат трендов (18.09: «ДА» WR 35,
    # «НЕТ» WR 77 на живых paper) — клетки по вердикту×направлению
    for _svv, _svl in (("ДА", "🚦ДА"), ("МОЖНО", "🚦МОЖНО"), ("НЕТ", "🚦НЕТ")):
        for _sg, _dl in ((1, "LONG"), (-1, "SHORT")):
            add_rule(f"sv_{_svv}_{_dl}", f"{_svl} {_dl} (светофор)",
                     [x for x in rows if x["sv"] == _svv and x["sg"] == _sg],
                     "super")
    # ₿ режим BTC (20.09): супер-клетки режим×направление и клетки
    # источник×направление×режим — модель сама включает шорты в коррекции
    # и глушит лонги (бэктест 20.05→08.07); в 45д-окне коррекции может не
    # быть — тогда работает 180д-память (build_regime_rules)
    for _rg in REGIME_BINS:
        for _sg, _dl in ((1, "LONG"), (-1, "SHORT")):
            add_rule(f"rg_{_rg}_{_dl}", f"{REGIME_LABEL[_rg]} {_dl} (режим)",
                     [x for x in rows if x.get("rg") == _rg and x["sg"] == _sg],
                     "regime")
    _combos_rg = {}
    for x in rows:
        if x.get("rg"):
            _combos_rg.setdefault((x["src"], x["sg"], x["rg"]), []).append(x)
    for (_src, _sg, _rg), _sel in _combos_rg.items():
        _dl = "LONG" if _sg > 0 else "SHORT"
        add_rule(f"cr_{_src}_{_dl}_{_rg}",
                 f"{_src} {_dl} · {REGIME_LABEL[_rg]}", _sel, "regime")
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
    if only_regime:
        rules = [r for r in rules if r["kind"] == "regime"]
    rules.sort(key=lambda r: -(r.get("ev") or 0))
    return rules


def _fetch_1h_range(symbol, start_ms, end_ms):
    """Спот 1h-свечи за диапазон (пагинация по 1000, data-api)."""
    import requests as _rq
    from exchange import BINANCE_BASE
    out, cur = [], start_ms
    while cur < end_ms:
        try:
            r = _rq.get(f"{BINANCE_BASE}/api/v3/klines",
                        params={"symbol": symbol, "interval": "1h",
                                "startTime": cur, "endTime": end_ms,
                                "limit": 1000}, timeout=20)
        except Exception:
            time.sleep(1)
            continue
        if r.status_code == 429:
            time.sleep(10)
            continue
        if r.status_code != 200:
            break
        ks = r.json()
        if not ks:
            break
        out.extend({"t": int(k[0]), "o": float(k[1]), "h": float(k[2]),
                    "l": float(k[3]), "c": float(k[4]), "v": float(k[5])}
                   for k in ks)
        cur = int(ks[-1][0]) + 3_600_000
        if len(ks) < 1000:
            break
    return out


def build_regime_rules(days=REGIME_DAYS, prev_rules=None, chunk=150):
    """₿ Долгая память режима: клетки источник×направление×режим BTC на
    `days` днях. В 45д-окне коррекции может не быть, а знать, что в
    коррекции шорты работают, надо ДО того, как она попадёт в окно.
    Свечи — спот постранично, 8 потоков, пары чанками по `chunk`
    (память контейнера); фандинг не тянем (fapi-бан). Только regime-клетки."""
    from collections import Counter
    from concurrent.futures import ThreadPoolExecutor
    from database import utcnow
    t0 = time.time()
    sigs = _load_signals(days)
    by_pair = Counter(s["pair"] for s in sigs)
    pairs = [p for p, _ in by_pair.most_common()]
    now_ms = int(utcnow().timestamp() * 1000)
    s_ms = now_ms - (days + 35) * 86_400_000
    rows, n_syms = [], 0
    for ci in range(0, len(pairs), chunk):
        part_pairs = pairs[ci:ci + chunk]
        cache = {}
        with ThreadPoolExecutor(max_workers=8) as ex:
            futs = {ex.submit(_fetch_1h_range, p.replace("/", ""), s_ms, now_ms): p
                    for p in set(part_pairs) | {"BTC/USDT"}}
            for f, p in futs.items():
                try:
                    cache[p] = f.result()
                except Exception:
                    cache[p] = []
        good = {p: v for p, v in cache.items() if len(v) > 500}
        pset = set(part_pairs)
        csigs = [s for s in sigs if s["pair"] in pset and s["pair"] in good]
        if csigs:
            part = build_rows(days=days, sleep_s=0, sigs=csigs, fund=False,
                              klines_fn=lambda pair, tf, limit=50, _c=good: _c.get(pair, []))
            rows.extend(x for x in part if x.get("res", True))
            n_syms += len(pset & set(good))
        del cache, good
    if len(rows) < 2000:
        logger.warning(f"[academy] regime: мало строк {len(rows)}")
        return None
    rules = aggregate(rows, prev_rules, live_map=None, only_regime=True)
    n_rg = Counter(x.get("rg") for x in rows)
    logger.info(f"[academy] regime: {len(rows)} строк, {n_syms} пар, "
                f"{len(rules)} клеток, {round(time.time() - t0)}с")
    return {"rules": rules, "rows_n": len(rows), "syms_n": n_syms,
            "days": days, "by_regime": {str(k): v for k, v in n_rg.items()},
            "built_at": utcnow().isoformat(),
            "build_sec": int(time.time() - t0)}


def exits_table(rows):
    """🚪 Самообучаемые выходы: статистика вариантов по супер-клеткам,
    рекомендация — лучший стабильный вариант (обе половины одного знака,
    n>=100) И обыгрывающий канон в ОБЕИХ половинах окна (допуск 0.05 —
    19.09: «hold» для 🥇 давал +5.4 vs +3.5 только за счёт августа, в
    свежей половине был хуже канона), иначе канон."""
    # только ПОЛНОСТЬЮ дозревшие окна (ts ≤ now − 98ч) И решённые все
    # варианты (vres). Свежий хвост по одному vres — одни стопы (у hold
    # окно ещё не закрыто) → занижает все варианты (v15: канон 🥇 +2.39
    # vs +2.95). Таблица выходов стратегическая — лаг 4 дня ей не мешает.
    mature_ms = time.time() * 1000 - (HORIZON_H + 2) * 3_600_000
    rows = [x for x in rows if x["vr"] and x.get("vres", True)
            and x["ts"] <= mature_ms]
    if not rows:
        return {}
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
        b1, b2 = tab["base"]["h1"], tab["base"]["h2"]

        def beats_base(v):
            t = tab[v]
            return (t["stable"] and t["h1"] is not None and b1 is not None
                    and t["h1"] >= b1 - 0.05 and t["h2"] >= b2 - 0.05
                    and t["avg"] >= tab["base"]["avg"] + 0.1)
        best = max((v for v in tab if v == "base" or beats_base(v)),
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


def tune_thresholds(rows):
    """🔧 Авто-тюнинг порогов (ТОЛЬКО тень): сетка вокруг боевых констант
    валидатора (дистанция до ST-линии, широта) и порога серии для 🌡.
    Боевые значения не трогаются — вкладка показывает, куда дышит рынок."""
    tmid = float(np.median([x["ts"] for x in rows]))

    def st(sel):
        if len(sel) < 300:
            return None
        a = np.array([x["r"] for x in sel])
        ts = np.array([x["ts"] for x in sel])
        h1, h2 = a[ts < tmid], a[ts >= tmid]
        return {"n": int(len(a)), "avg": round(float(a.mean()), 3),
                "wr": round(float((a > 0).mean() * 100), 1),
                "stable": bool(len(h1) > 30 and len(h2) > 30
                               and (h1.mean() > 0) == (h2.mean() > 0))}

    out = {}
    longs = [x for x in rows if x["sg"] > 0 and x["dmin"] is not None
             and x["br"] is not None and x["streak"] < 27]
    grid = []
    for dist in (3.0, 4.0, 5.0, 6.0):
        for brm in (0.40, 0.45, 0.50):
            s = st([x for x in longs if x["dmin"] <= dist and x["br"] <= brm])
            if s:
                grid.append({"dist": dist, "br": brm, **s})
    cur = next((g for g in grid if g["dist"] == 4.0 and g["br"] == 0.45), None)
    best = max((g for g in grid if g["stable"]), key=lambda g: g["avg"],
               default=None)
    out["gold"] = {"current": cur, "best": best,
                   "grid": sorted(grid, key=lambda g: -g["avg"])[:5]}
    shorts = [x for x in rows if x["sg"] < 0]
    sgrid = []
    for thr in (22, 27, 32):
        s = st([x for x in shorts if x["streak"] >= thr])
        if s:
            sgrid.append({"thr": thr, **s})
    cur_s = next((g for g in sgrid if g["thr"] == 27), None)
    best_s = max((g for g in sgrid if g["stable"]), key=lambda g: g["avg"],
                 default=None)
    out["heat"] = {"current": cur_s, "best": best_s, "grid": sgrid}
    return out


LIVE_WINDOW_DAYS = 21   # живая статистика — скользящее окно (19.09:
                        # иначе правило, выключенное в ралли, никогда не
                        # реабилитируется при смене режима)


def paper_live_map(window_days=LIVE_WINDOW_DAYS):
    """{rule_id: {n, wr, avg}} из закрытых paper-сделок за окно (sync)."""
    from database import _get_db, utcnow
    from datetime import timedelta as _td
    agg = {}
    try:
        for d in _get_db().academy_paper.find(
                {"state": {"$in": ["TP", "SL", "TIMEOUT"]},
                 "rule_id": {"$ne": None},
                 "closed_at": {"$gte": utcnow() - _td(days=window_days)}},
                {"rule_id": 1, "r": 1}):
            if d.get("r") is None:
                continue
            agg.setdefault(d["rule_id"], []).append(float(d["r"]))
    except Exception:
        pass
    return {k: {"n": len(v),
                "wr": round(sum(1 for r in v if r > 0) / len(v) * 100, 1),
                "avg": round(sum(v) / len(v), 3)}
            for k, v in agg.items() if len(v) >= 5}


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
                round(_m.log10(max(x.get("dvol") or 1, 1)), 2),
                (-1 if x.get("young") is None else
                 (1 if x["young"] else 0)),
                x.get("hour") or 0,
                -1.0 if x.get("btc_vol") is None else x["btc_vol"],
                0.0 if x.get("fund") is None else round(x["fund"] * 1e4, 2),
                {"ДА": 2, "МОЖНО": 1, "НЕТ": 0}.get(x.get("sv"), -1),
                -50.0 if x.get("sc") is None else x["sc"],
                -99.0 if x.get("btc_dd") is None else x["btc_dd"]]

    rs = sorted(rows, key=lambda x: x["ts"])
    cut = int(len(rs) * 0.75)
    Xtr = np.array([feats(x) for x in rs[:cut]], dtype=float)
    ytr = np.array([1 if x["r"] > 0 else 0 for x in rs[:cut]])
    Xte = np.array([feats(x) for x in rs[cut:]], dtype=float)
    rte = np.array([x["r"] for x in rs[cut:]])
    yte = (rte > 0).astype(int)
    # нативный API (sklearn-обёртки на проде нет — LightGBMError 16.09)
    try:
        ds = lgb.Dataset(Xtr, label=ytr, categorical_feature=[0],
                         free_raw_data=False)
        # num_threads=2: в контейнере Railway OpenMP видит ядра ХОСТА и
        # плодит треды сверх cgroup-квоты — train висел вечно (16.09)
        m = lgb.train({"objective": "binary", "num_leaves": 15,
                       "learning_rate": 0.06, "min_data_in_leaf": 40,
                       "num_threads": 2, "force_col_wise": True,
                       "verbosity": -1}, ds, num_boost_round=200)
        p = np.asarray(m.predict(Xte))
    except Exception as e:
        return {"ok": False, "reason": f"train: {type(e).__name__}: {e}"[:200]}
    # AUC руками (без sklearn)
    order = np.argsort(p)
    ranks = np.empty(len(p))
    ranks[order] = np.arange(1, len(p) + 1)
    n1, n0 = int(yte.sum()), int(len(yte) - yte.sum())
    auc = float((ranks[yte == 1].sum() - n1 * (n1 + 1) / 2) / max(n1 * n0, 1))
    top = p >= np.percentile(p, 80)
    fi = sorted(zip(["src", "dir", "val", "streak", "tr4", "breadth",
                     "fresh", "liq", "young", "hour", "btc_vol", "fund",
                     "svetofor", "sv_score", "btc_dd"],
                    m.feature_importance().tolist()), key=lambda z: -z[1])
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

def _loop_state(**kw):
    """Диагностика цикла: academy_loop_state в market_state — видно с
    прода без логов Railway (упал/убит/успех)."""
    try:
        from database import _get_db, utcnow
        _get_db().market_state.update_one(
            {"_id": "academy_loop_state"},
            {"$set": {**kw, "at": utcnow().isoformat()}}, upsert=True)
    except Exception:
        pass


def recompute(days=WINDOW_DAYS, progress=None):
    """Полный ночной пересчёт (sync; звать в to_thread). Возвращает модель."""
    from database import _get_db, utcnow
    db = _get_db()
    t0 = time.time()
    import os as _os
    import socket as _sock
    _loop_state(phase="building", host=_sock.gethostname(),
                pid=_os.getpid(), started=utcnow().isoformat(), error=None)
    try:
        rows_all = build_rows(days=days, progress=progress)
    except Exception as e:
        _loop_state(phase="failed", error=f"build: {type(e).__name__}: {e}"[:300])
        raise
    # ✈ «в полёте» (исход не решён) — вне статистики, только пульс режима
    inflight = [x for x in rows_all if not x.get("res", True)]
    rows = [x for x in rows_all if x.get("res", True)]
    if len(rows) < 500:
        logger.warning(f"[academy] мало строк: {len(rows)} — модель не обновляю")
        _loop_state(phase="failed", error=f"мало строк: {len(rows)}")
        return None
    _loop_state(phase="aggregating", rows_n=len(rows))
    try:
        prev = db.learn_model.find_one({"_id": "active"}) or {}
        live_map = paper_live_map()
        rules = aggregate(rows, prev.get("rules"), live_map=live_map)
        _loop_state(phase="lgbm")
        lgbm = train_lgbm(rows)
        _loop_state(phase="oos")
    # 🤖 vs 📊: OOS-сравнение LightGBM-топа с портфелем супер-клеток
        rs_sorted = sorted(rows, key=lambda x: x["ts"])
        test = rs_sorted[int(len(rs_sorted) * 0.75):]
        tbl = [x["r"] for x in test
               if x["gold"] or x["silver"] or x["dawn"] or x["heat"]]
        table_oos = round(float(np.mean(tbl)), 3) if len(tbl) >= 50 else None
        beat = bool(lgbm.get("ok") and table_oos is not None
                    and lgbm["oos_top20_avg"] > table_oos)
        lgbm_streak = (int(prev.get("lgbm_beat_streak", 0)) + 1) if beat else 0
        _loop_state(phase="exits")
        model = {
            "_id": "active",
            "version": int(prev.get("version", 0)) + 1,
            "window_days": days, "rows_n": len(rows),
            "syms_n": len({x["sym"] for x in rows}),
            "inflight": inflight_pulse(inflight),
            "built_at": utcnow().isoformat(),
            "build_sec": int(time.time() - t0),
            "rules": rules, "lgbm": lgbm,
            "exits": exits_table(rows), "liq": liq_split(rows),
            "tuning": tune_thresholds(rows),
            "live_n": sum(v["n"] for v in live_map.values()),
            "table_oos": table_oos, "lgbm_beat_streak": lgbm_streak,
            "lgbm_ready": bool(lgbm_streak >= 28),
            "n_show": sum(1 for r in rules if r["status"] == "ACTIVE_SHOW"),
            "n_hide": sum(1 for r in rules if r["status"] == "ACTIVE_HIDE"),
            "n_shadow": sum(1 for r in rules if r["status"] == "SHADOW"),
            "degraded": [r["id"] for r in rules
                         if r.get("degraded") is not None],
            # ₿ режим: текущий + долгая память (переносится, пересобирается ниже)
            "btc_regime": dict(zip(("dd", "bin"), btc_regime_now())),
            # доля режимов в 45д-окне: память режима главнее обычных клеток,
            # только пока режима в окне <15% (окно ещё «не знает» его)
            "regime_share": {k: round(v / len(rows), 3) for k, v in
                             __import__("collections").Counter(
                                 x.get("rg") for x in rows if x.get("rg")).items()},
            "regime_rules": prev.get("regime_rules"),
            "regime_meta": prev.get("regime_meta"),
        }
        _loop_state(phase="brief")
        model["brief"] = groq_brief(model)
        _loop_state(phase="saving")
    except Exception as e:
        import traceback as _tb
        _loop_state(phase="failed",
                    error=f"post: {type(e).__name__}: {e} | "
                          f"{_tb.format_exc()[-250:]}"[:400])
        raise
    db.learn_model.replace_one({"_id": "active"}, model, upsert=True)
    hist = dict(model)
    hist["_id"] = f"v{model['version']}_{model['built_at'][:16]}"
    hist.pop("rules", None)   # история — только метаданные, без таблицы
    hist.pop("regime_rules", None)
    try:
        db.learn_model_history.insert_one(hist)
    except Exception:
        pass
    # ₿ долгая память режима — ПОСЛЕ сохранения основной модели (не блокирует)
    try:
        _loop_state(phase="regime")
        _rg = build_regime_rules(prev_rules=prev.get("regime_rules"))
        if _rg:
            _meta = {k: v for k, v in _rg.items() if k != "rules"}
            db.learn_model.update_one(
                {"_id": "active"},
                {"$set": {"regime_rules": _rg["rules"], "regime_meta": _meta}})
            model["regime_rules"], model["regime_meta"] = _rg["rules"], _meta
    except Exception:
        logger.exception("[academy] regime build fail — оставляю прошлую память")
    logger.info(f"[academy] модель v{model['version']}: {len(rules)} правил, "
                f"{model['n_show']} SHOW / {model['n_hide']} HIDE, "
                f"{model['build_sec']}с")
    _loop_state(phase="done", version=model["version"],
                build_sec=model["build_sec"], error=None)
    return model


# ────────────────────────── live-скоринг для вкладки ──────────────────────────

def score_signal(model, src, direction, validator_ok, streak, rg=None):
    """Вердикт по штампам сигнала: (status, rule) или (None, None).
    rg — текущий режим BTC (btc_regime_now()[1]). Если рынок НЕ у максимума
    (dip/corr/capit), первыми смотрим клетки источник×направление×режим:
    45д-окно, затем 180д-память (regime_rules), затем супер-клетка режима —
    45д-окно в начале коррекции ещё «бычье» и шорты без памяти не откроет.
    У максимума — обычный путь (серия/🧿 → родитель)."""
    if not model:
        return None, None
    rules = {r["id"]: r for r in model.get("rules") or []}
    sgl = "LONG" if direction == "LONG" else "SHORT"
    v = "T" if validator_ok is True else ("F" if validator_ok is False else "N")
    bk = bucket(streak)
    order = []
    fine = ([(rules, f"c_{src}_{sgl}_{v}_{bk}")] if bk else []) + [(rules, f"p_{src}_{sgl}")]
    if rg and rg != "top":
        rules180 = {r["id"]: r for r in model.get("regime_rules") or []}
        mem = [(rules, f"cr_{src}_{sgl}_{rg}"),
               (rules180, f"cr_{src}_{sgl}_{rg}"),
               (rules180, f"rg_{rg}_{sgl}")]
        share = (model.get("regime_share") or {}).get(rg, 0.0)
        # режим «свежий» для окна (<15% строк) — память первее тонких
        # клеток (они ещё бычьи); режим уже в окне — тонкие клетки
        # (серия/🧿) первее, память только как запасной ответ
        order = mem + fine if share < 0.15 else fine + mem
    else:
        order = fine
    for pool, rid in order:
        r = pool.get(rid)
        if r and r["status"] in ("ACTIVE_SHOW", "ACTIVE_HIDE") and r["n"] >= MIN_N:
            return r["status"], r
    return None, None
