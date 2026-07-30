"""🎰 Setup Checker v2 — направленный разбор «как трейдер» (29.07.2026).

Старые анализы (WHALE/SHARK score, ST 2H/4H, TOTAL2 bias, cluster bias,
MAX VERIFICATION) удалены по запросу. Новый разбор — то, что делалось
вручную для PEOPLEUSDT:
  1. Структура: моментум 24ч/72ч/7д, дистанция от 24ч-хая/лоя,
     последний закрытый бар (отскок/слив в моменте)
  2. Реальный поток: спот CVD24/CVD72 (тайкер-дельта), dz24, объём
  3. Funding, фаза 4h, климат 12h (эдж — в расхождениях этажей)
  4. Свежие сетапы платформы (🌋💨🛟💎🐋) + повторы (🐳 второй кит,
     🔁 усилители, st_break-повторы = мусор)
  5. Вердикт по каждому направлению: ДА / МОЖНО / ЖДАТЬ ОТСКОКА / НЕТ
     + «за»/«против» + план уровней (вход/стоп/цель/R:R)
Все пороги — из бэктестов (blowoff/capitulation/канал ПОТОК/повторы).
"""
from __future__ import annotations
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


def _normalize_pair(pair: str) -> str:
    """'WIF' → 'WIF/USDT', 'WIFUSDT' → 'WIF/USDT'."""
    p = (pair or "").upper().strip().replace(" ", "")
    if "/" in p:
        if not p.endswith("USDT"):
            p += "USDT" if "/" not in p else ""
        return p
    if p.endswith("USDT"):
        base = p[:-4]
    else:
        base = p
    return f"{base}/USDT"


def _rsi(closes: list[float], period: int = 14) -> Optional[float]:
    """Wilder's RSI. Returns last value."""
    n = len(closes)
    if n < period + 2:
        return None
    gains, losses = [], []
    for i in range(1, n):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return round(100 - (100 / (1 + rs)), 1)


def _fmt_num(v) -> str:
    a = abs(v)
    if a >= 1e9:
        return f"{v / 1e9:+.1f}B"
    if a >= 1e6:
        return f"{v / 1e6:+.1f}M"
    if a >= 1e3:
        return f"{v / 1e3:+.1f}K"
    return f"{v:+.0f}"


def _fmt_price(v) -> str:
    if v is None:
        return "—"
    return f"{v:.6g}"


def check_setup(pair_input: str) -> dict:
    """Направленный разбор пары: metrics + LONG/SHORT (за/против/план)."""
    pair = _normalize_pair(pair_input)
    symbol = pair.replace("/", "").upper()
    result = {
        "pair": pair, "symbol": symbol,
        "verdict": "WAIT", "verdict_note": "", "confidence": 0,
        "metrics": {}, "long": {}, "short": {},
        "active_trades": [], "recent_signals": [], "reasons": [],
    }
    try:
        # ── 1. Свечи 1h с тайкер-дельтой (400 баров) ─────────────────
        kd = None
        try:
            from accum_detector import _fetch_klines_delta
            kd = _fetch_klines_delta(pair, 400)
        except Exception:
            pass
        has_delta = bool(kd)
        if not kd:
            from exchange import get_klines_any
            kd = get_klines_any(pair, "1h", 400)
        if not kd or len(kd) < 200:
            result["verdict"] = "NO_DATA"
            result["reasons"].append(f"Недостаточно 1h данных для {pair}")
            return result

        o = [x["o"] for x in kd]; h = [x["h"] for x in kd]
        l = [x["l"] for x in kd]; c = [x["c"] for x in kd]
        v = [x["v"] for x in kd]
        tb = [x.get("tb") for x in kd] if has_delta else None
        price = c[-1]
        hi24, lo24 = max(h[-24:]), min(l[-24:])
        hi72, lo72 = max(h[-72:]), min(l[-72:])
        mom24 = (price / c[-25] - 1) * 100
        mom72 = (price / c[-73] - 1) * 100
        mom7d = (price / c[-169] - 1) * 100 if len(c) >= 169 else None
        dist_hi24 = (price / hi24 - 1) * 100
        dist_lo24 = (price / lo24 - 1) * 100
        dist_hi72 = (price / hi72 - 1) * 100
        dist_lo72 = (price / lo72 - 1) * 100
        rsi1h = _rsi(c)
        c4 = c[::-1][::4][::-1]
        rsi4h = _rsi(c4)
        cvd24 = cvd72 = None
        if has_delta:
            cvd24 = sum(2 * tb[i] - v[i] for i in range(-24, 0))
            cvd72 = sum(2 * tb[i] - v[i] for i in range(-72, 0))
        vs = sorted(v[-240:])
        v_med = vs[len(vs) // 2] or 1e-12
        vol24_x = (sum(v[-24:]) / 24) / v_med
        # последний ЗАКРЫТЫЙ бар — отскок/слив в моменте
        lb_green = c[-2] >= o[-2]
        lb_rng = (h[-2] - l[-2]) or 1e-12
        lb_pos = (c[-2] - l[-2]) / lb_rng * 100

        # ── 2. Контекст платформы ────────────────────────────────────
        from database import _get_db
        db = _get_db()
        funding_pct = None
        try:
            fn = db.market_state.find_one({"_id": "funding_now"}) or {}
            fr = (fn.get("rates") or {}).get(pair)
            if fr is None:
                fr = (fn.get("rates") or {}).get(symbol)
            if fr is not None:
                funding_pct = float(fr) * 100
        except Exception:
            pass
        phase = clim = None
        try:
            from supertrend_tracker import _market_phase_now
            phase = _market_phase_now()
        except Exception:
            pass
        try:
            from trade_grade import climate12
            clim = climate12()
        except Exception:
            pass
        dz24 = None
        try:
            pc = db.pair_context.find_one({"_id": pair}) or {}
            dz24 = pc.get("dz24")
        except Exception:
            pass

        PH = {"LONG": "🟢", "SHORT": "🔴", "NEUTRAL": "⚪"}
        result["metrics"] = {
            "price": price, "mom24": round(mom24, 1), "mom72": round(mom72, 1),
            "mom7d": round(mom7d, 1) if mom7d is not None else None,
            "dist_hi24": round(dist_hi24, 1), "dist_lo24": round(dist_lo24, 1),
            "dist_hi72": round(dist_hi72, 1), "dist_lo72": round(dist_lo72, 1),
            "hi24": hi24, "lo24": lo24,
            "rsi1h": rsi1h, "rsi4h": rsi4h,
            "cvd24": round(cvd24) if cvd24 is not None else None,
            "cvd72": round(cvd72) if cvd72 is not None else None,
            "dz24": dz24, "vol24_x": round(vol24_x, 1),
            "funding_pct": round(funding_pct, 4) if funding_pct is not None else None,
            "phase": phase, "phase_emoji": PH.get(phase, "?"),
            "climate12": clim, "climate12_emoji": PH.get(clim, "?"),
            "last_bar": {"green": lb_green, "close_pos": round(lb_pos)},
        }

        # ── 3. Свежие сетапы платформы (≤24ч) + активные сделки ──────
        now_dt = datetime.now(timezone.utc)
        naive_now = now_dt.replace(tzinfo=None)
        fresh = {}
        try:
            cut24 = naive_now - timedelta(hours=24)
            for d in db.new_strategy_signals.find(
                    {"pair": pair, "created_at": {"$gte": cut24},
                     "strategy": {"$in": ["blowoff", "thin_pump", "capitulation",
                                          "floor_buy", "whale", "shark",
                                          "st_break"]}},
                    {"strategy": 1, "created_at": 1, "entry": 1, "rep_seq": 1,
                     "whale_seq": 1, "whale_rel": 1, "direction": 1,
                     "state": 1}).sort("created_at", -1):
                st = d["strategy"]
                if st not in fresh:
                    d["age_h"] = round((naive_now - d["created_at"])
                                       .total_seconds() / 3600, 1)
                    fresh[st] = d
        except Exception:
            pass
        # активные сделки-сигналы в пути (WAITING, ≤7д)
        try:
            cut7 = naive_now - timedelta(days=7)
            for d in db.new_strategy_signals.find(
                    {"pair": pair, "state": "WAITING",
                     "created_at": {"$gte": cut7},
                     "strategy": {"$in": ["blowoff", "thin_pump",
                                          "capitulation", "floor_buy"]}},
                    {"strategy": 1, "direction": 1, "entry": 1, "tp": 1,
                     "sl": 1, "created_at": 1}).sort("created_at", -1).limit(6):
                e = d.get("entry")
                prog = None
                if e:
                    prog = ((e - price) / e * 100 if d.get("direction") == "SHORT"
                            else (price / e - 1) * 100)
                result["active_trades"].append({
                    "strategy": d["strategy"], "direction": d.get("direction"),
                    "entry": e, "tp": d.get("tp"), "sl": d.get("sl"),
                    "age_h": round((naive_now - d["created_at"])
                                   .total_seconds() / 3600, 1),
                    "pnl_now": round(prog, 1) if prog is not None else None,
                })
        except Exception:
            pass

        # ── 4. SHORT-разбор ──────────────────────────────────────────
        s_pros, s_cons = [], []
        seller = ((cvd24 is not None and cvd24 < 0) or
                  (dz24 is not None and dz24 <= -1))
        buyer = ((cvd24 is not None and cvd24 > 0) or
                 (dz24 is not None and dz24 >= 1))
        near_hi = dist_hi24 >= -3
        late_short = dist_hi24 <= -8
        overheat = (rsi1h is not None and rsi1h >= 68) or mom24 >= 10
        bounce_now = lb_green and lb_pos >= 70

        if seller:
            txt = "продавец реален: "
            parts = []
            if cvd24 is not None and cvd24 < 0:
                parts.append(f"CVD24 {_fmt_num(cvd24)}")
            if cvd72 is not None and cvd72 < 0:
                parts.append(f"CVD72 {_fmt_num(cvd72)}")
            if dz24 is not None and dz24 <= -1:
                parts.append(f"dz24 {dz24}")
            s_pros.append(txt + " · ".join(parts))
        elif buyer:
            s_cons.append(f"в стакане покупатель: CVD24 "
                          f"{_fmt_num(cvd24) if cvd24 is not None else '?'}"
                          + (f" · dz24 {dz24}" if dz24 is not None else ""))
        if near_hi:
            s_pros.append(f"у вершины ({dist_hi24:+.1f}% от 24ч-хая) — "
                          f"структурный стоп близко")
        if late_short:
            s_cons.append(f"поздно по цене: {dist_hi24:+.1f}% от 24ч-хая — "
                          f"пол-движения проехали, вход = догон")
        if overheat:
            s_pros.append(f"перегрев: разгон {mom24:+.1f}%/24ч"
                          + (f", RSI1h {rsi1h:.0f}" if rsi1h else "")
                          + " — материал для вершины (бэктест blowoff)")
        if bounce_now:
            s_cons.append(f"прямо сейчас отскок: последний час зелёный с "
                          f"закрытием в {lb_pos:.0f}% диапазона — не шорти "
                          f"свечу-в-лоб")
        if funding_pct is not None and funding_pct <= -0.05:
            s_cons.append(f"funding {funding_pct:+.3f}% — толпа уже в шорте, "
                          f"сквиз-риск (гейт канала ПОТОК)")
        if clim == "LONG":
            s_pros.append("климат 12h 🟢 — год-бэктест: шорты при 🟢12h "
                          "дают +0.88..+2.73")
        if phase == "LONG" and not overheat:
            s_cons.append("фаза 4h 🟢 без перегрева — шорт против фазы")
        fresh_short = None
        for st in ("blowoff", "thin_pump", "shark"):
            d = fresh.get(st)
            if d and d.get("direction") != "LONG":
                fresh_short = d
                em = {"blowoff": "🌋", "thin_pump": "💨", "shark": "🦈"}[st]
                rep = d.get("rep_seq") or 1
                rep_txt = ""
                if st == "thin_pump" and rep >= 2:
                    rep_txt = f" · 🔁 повтор №{rep} — усилитель (EV +1.44/+1.64)"
                elif st == "blowoff" and rep >= 3:
                    rep_txt = f" · 🔁 вершина №{rep} — усилитель (EV +0.89)"
                s_pros.append(f"{em} свежий {st} {d['age_h']:.0f}ч назад "
                              f"@ {_fmt_price(d.get('entry'))}{rep_txt}")
                break
        stb = fresh.get("st_break")
        if stb and stb.get("direction") == "SHORT" and (stb.get("rep_seq") or 1) >= 3:
            s_cons.append(f"st_break SHORT — {stb['rep_seq']}-й повтор: "
                          f"по бэктесту мусор (EV −0.77, n=2147)")

        if funding_pct is not None and funding_pct <= -0.05:
            s_grade = "НЕТ"
        elif fresh_short and not late_short and not bounce_now:
            s_grade = "ДА"
        elif seller and near_hi and overheat:
            s_grade = "ДА"
        elif seller and late_short:
            s_grade = "ЖДАТЬ ОТСКОКА"
        elif seller or (near_hi and overheat):
            s_grade = "МОЖНО"
        else:
            s_grade = "НЕТ"

        if s_grade == "ЖДАТЬ ОТСКОКА":
            e_lo = price * 1.015
            e_hi = min(hi24 * 0.96, price * 1.06)
            if e_hi <= e_lo:
                e_hi = e_lo * 1.02
            entry_mid = (e_lo + e_hi) / 2
            s_plan = {"entry_zone": [round(e_lo, 10), round(e_hi, 10)],
                      "stop": round(hi24 * 1.01, 10),
                      "target": round(entry_mid * 0.90, 10),
                      "note": "вход на отскоке к сломанной зоне, не по рынку"}
        else:
            entry_mid = price
            s_plan = {"entry_zone": [round(price, 10), round(price, 10)],
                      "stop": round(hi24 * 1.01, 10),
                      "target": round(price * 0.90, 10),
                      "note": "структурный стоп над 24ч-хаем (бэктест blowoff: "
                              "выбивание 31% против 59% у процентного)"}
        try:
            rr = (entry_mid - s_plan["target"]) / max(
                s_plan["stop"] - entry_mid, entry_mid * 0.001)
            s_plan["rr"] = round(rr, 1)
        except Exception:
            pass
        result["short"] = {"grade": s_grade, "pros": s_pros, "cons": s_cons,
                           "plan": s_plan}

        # ── 5. LONG-разбор ───────────────────────────────────────────
        l_pros, l_cons = [], []
        near_lo = dist_lo24 <= 3
        late_long = dist_lo24 >= 8
        oversold = (rsi4h is not None and rsi4h <= 30) or mom24 <= -10
        dump_now = (not lb_green) and lb_pos <= 30

        if buyer:
            parts = []
            if cvd24 is not None and cvd24 > 0:
                parts.append(f"CVD24 {_fmt_num(cvd24)}")
            if cvd72 is not None and cvd72 > 0:
                parts.append(f"CVD72 {_fmt_num(cvd72)}")
            if dz24 is not None and dz24 >= 1:
                parts.append(f"dz24 {dz24}")
            l_pros.append("покупатель реален: " + " · ".join(parts))
        elif seller:
            l_cons.append(f"в стакане продавец: CVD24 "
                          f"{_fmt_num(cvd24) if cvd24 is not None else '?'}"
                          + (f" · dz24 {dz24}" if dz24 is not None else ""))
        if near_lo:
            l_pros.append(f"у дна ({dist_lo24:+.1f}% от 24ч-лоя) — стоп близко; "
                          f"дно — процесс, вход сразу (бэктест капитуляции)")
        if late_long:
            l_cons.append(f"поздно: уже {dist_lo24:+.1f}% от 24ч-лоя — догон")
        if oversold:
            l_pros.append(f"перепроданность: {mom24:+.1f}%/24ч"
                          + (f", RSI4h {rsi4h:.0f}" if rsi4h else ""))
        if dump_now:
            l_cons.append(f"прямо сейчас слив: последний час красный с "
                          f"закрытием в {lb_pos:.0f}% диапазона — нож")
        if funding_pct is not None and funding_pct < 0:
            l_cons.append(f"funding {funding_pct:+.3f}% отрицательный — "
                          f"гейт LONG канала требует ≥0")
        if clim == "SHORT" and phase == "LONG":
            l_pros.append("расхождение этажей 🔴12h + 🟢4h — лучшая ячейка "
                          "LONG года (EV +2.03, WR 58)")
        if clim == "LONG" and phase in (None, "NEUTRAL"):
            l_cons.append("🟢12h + ⚪4h — худшая ячейка LONG года (−2.05)")
        fresh_long = None
        for st in ("capitulation", "floor_buy", "whale"):
            d = fresh.get(st)
            if d and d.get("direction") != "SHORT":
                fresh_long = d
                em = {"capitulation": "🛟", "floor_buy": "💎", "whale": "🐋"}[st]
                extra = ""
                if st == "whale" and (d.get("whale_seq") or 1) >= 2:
                    rel = d.get("whale_rel")
                    extra = (" · 🐳 второй кит " +
                             {"below": "ниже 1-го (EV +2.2 — лучший повтор)",
                              "same": "на уровне 1-го (слабо +0.4 — минус)",
                              "above": "выше 1-го (+1.5)"}.get(rel, ""))
                l_pros.append(f"{em} свежий {st} {d['age_h']:.0f}ч назад "
                              f"@ {_fmt_price(d.get('entry'))}{extra}")
                break

        if funding_pct is not None and funding_pct < -0.05:
            l_grade = "НЕТ"
        elif fresh_long and not late_long:
            l_grade = "ДА"
        elif buyer and near_lo and oversold:
            l_grade = "ДА"
        elif buyer and late_long:
            l_grade = "ЖДАТЬ ОТКАТА"
        elif buyer or (near_lo and oversold):
            l_grade = "МОЖНО"
        else:
            l_grade = "НЕТ"

        if l_grade == "ЖДАТЬ ОТКАТА":
            e_hi = price * 0.985
            e_lo = max(lo24 * 1.04, price * 0.94)
            if e_lo >= e_hi:
                e_lo = e_hi * 0.98
            entry_mid_l = (e_lo + e_hi) / 2
            l_plan = {"entry_zone": [round(e_lo, 10), round(e_hi, 10)],
                      "stop": round(lo24 * 0.99, 10),
                      "target": round(entry_mid_l * 1.10, 10),
                      "note": "вход на откате, не по рынку"}
        else:
            entry_mid_l = price
            l_plan = {"entry_zone": [round(price, 10), round(price, 10)],
                      "stop": round(lo24 * 0.99, 10),
                      "target": round(price * 1.10, 10),
                      "note": "структурный стоп под 24ч-лоем (бэктест "
                              "капитуляции: вход сразу, ждать нельзя)"}
        try:
            rr = (l_plan["target"] - entry_mid_l) / max(
                entry_mid_l - l_plan["stop"], entry_mid_l * 0.001)
            l_plan["rr"] = round(rr, 1)
        except Exception:
            pass
        result["long"] = {"grade": l_grade, "pros": l_pros, "cons": l_cons,
                          "plan": l_plan}

        # ── 6. Общий вердикт ─────────────────────────────────────────
        # 🔥 горячая монета (бэктест 30.07: лонги на горячих лучше,
        # кит — хуже; комбо st4h/2flip/impulse — концентрат)
        try:
            from hot_engine import is_hot
            result["hot"] = bool(is_hot(pair))
        except Exception:
            result["hot"] = False
        rank = {"ДА": 3, "МОЖНО": 2, "ЖДАТЬ ОТСКОКА": 1.5,
                "ЖДАТЬ ОТКАТА": 1.5, "НЕТ": 0}
        rs_, rl_ = rank.get(s_grade, 0), rank.get(l_grade, 0)
        if rl_ >= 3 and rs_ < 3:
            result["verdict"] = "ENTER_LONG"
            side = result["long"]
        elif rs_ >= 3 and rl_ < 3:
            result["verdict"] = "ENTER_SHORT"
            side = result["short"]
        elif rs_ >= 3 and rl_ >= 3:
            result["verdict"] = "WAIT"
            result["verdict_note"] = "конфликт: оба направления дают сетап"
            side = None
        else:
            result["verdict"] = "WAIT"
            best = ("SHORT: " + s_grade if rs_ >= rl_ and rs_ > 0 else
                    "LONG: " + l_grade if rl_ > 0 else "")
            if s_grade == "ЖДАТЬ ОТСКОКА":
                z = result["short"]["plan"]["entry_zone"]
                result["verdict_note"] = (f"шорт интересен на отскоке к "
                                          f"{_fmt_price(z[0])}–{_fmt_price(z[1])}")
            elif l_grade == "ЖДАТЬ ОТКАТА":
                z = result["long"]["plan"]["entry_zone"]
                result["verdict_note"] = (f"лонг интересен на откате к "
                                          f"{_fmt_price(z[0])}–{_fmt_price(z[1])}")
            elif best:
                result["verdict_note"] = best
            side = None
        if side is not None:
            n_p, n_c = len(side["pros"]), len(side["cons"])
            result["confidence"] = max(25, min(90, 45 + 12 * (n_p - n_c)))
            result["reasons"] = side["pros"][:3] + [f"⚠ {x}" for x in side["cons"][:2]]
        else:
            result["confidence"] = 0
            result["reasons"] = ([f"SHORT: {s_grade}"] + s_pros[:1] +
                                 [f"LONG: {l_grade}"] + l_pros[:1])

        # ── 7. Все сигналы за 72ч (лента + маркеры графика) ──────────
        since = naive_now - timedelta(hours=72)
        pair_or = {"$or": [{"pair": pair}, {"symbol": symbol}]}
        all_sigs: list = []
        try:
            for d in db.new_strategy_signals.find({
                **pair_or, "created_at": {"$gte": since},
            }, {
                "strategy": 1, "created_at": 1, "direction": 1, "entry": 1,
                "whale_tier": 1, "shark_tier": 1, "whale_score": 1,
                "shark_score": 1, "state": 1, "rep_seq": 1, "whale_seq": 1,
                "whale_rel": 1,
            }).sort("created_at", -1).limit(50):
                dt = d.get("created_at")
                if dt and dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                all_sigs.append({
                    "source": d.get("strategy"),
                    "at": dt.isoformat() if dt else None,
                    "at_ts": int(dt.timestamp()) if dt else 0,
                    "direction": d.get("direction"),
                    "entry": d.get("entry"),
                    "tier": d.get("whale_tier") or d.get("shark_tier"),
                    "score": d.get("whale_score") or d.get("shark_score"),
                    "state": d.get("state"),
                    "rep_seq": d.get("rep_seq"),
                    "whale_seq": d.get("whale_seq"),
                    "whale_rel": d.get("whale_rel"),
                })
        except Exception:
            pass
        try:
            for s in db.supertrend_signals.find({
                "pair": pair, "flip_at": {"$gte": since},
                "tier": {"$in": ["vip", "mtf"]},
            }, {"tier": 1, "direction": 1, "entry_price": 1,
                "flip_at": 1}).sort("flip_at", -1).limit(20):
                dt = s.get("flip_at")
                if dt and dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                all_sigs.append({
                    "source": f"st_{s.get('tier', 'mtf')}",
                    "at": dt.isoformat() if dt else None,
                    "at_ts": int(dt.timestamp()) if dt else 0,
                    "direction": s.get("direction"),
                    "entry": s.get("entry_price"),
                    "tier": s.get("tier", "").upper(),
                })
        except Exception:
            pass
        try:
            for cf in db.confluence.find({
                **pair_or, "detected_at": {"$gte": since},
            }, {"direction": 1, "price": 1, "detected_at": 1, "score": 1,
                "strength": 1}).sort("detected_at", -1).limit(20):
                dt = cf.get("detected_at")
                if dt and dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                all_sigs.append({
                    "source": "confluence",
                    "at": dt.isoformat() if dt else None,
                    "at_ts": int(dt.timestamp()) if dt else 0,
                    "direction": cf.get("direction"),
                    "entry": cf.get("price"),
                    "score": cf.get("score"),
                    "tier": cf.get("strength"),
                })
        except Exception:
            pass
        all_sigs.sort(key=lambda x: x.get("at_ts", 0), reverse=True)
        now_ts = int(now_dt.timestamp())
        for s in all_sigs:
            if s.get("at_ts"):
                s["age_hours"] = round((now_ts - s["at_ts"]) / 3600, 1)
        try:
            from signal_families import collapse_stacks
            _display = [dict(x, pair=pair) for x in all_sigs]
            result["recent_signals"] = collapse_stacks(_display)
        except Exception:
            result["recent_signals"] = all_sigs

        return result
    except Exception as e:
        logger.exception(f"[setup-check] {pair_input} fail")
        result["verdict"] = "ERROR"
        result["reasons"].append(f"Error: {str(e)}")
        return result


# ════ Compact verdict для хранения в signal docs ════════════
# Используется background loop'ом в watcher — журнал показывает чип.

_verdict_cache: dict = {}  # {pair: (ts, compact_verdict)}
_VERDICT_CACHE_TTL = 300  # 5 мин


def get_compact_verdict(pair_input: str) -> dict:
    """Compact verdict для signal doc storage (кэш 5 мин на пару)."""
    import time
    pair = _normalize_pair(pair_input)
    now = time.time()
    cached = _verdict_cache.get(pair)
    if cached and (now - cached[0]) < _VERDICT_CACHE_TTL:
        return cached[1]

    full = check_setup(pair)
    verdict = full.get("verdict", "UNKNOWN")
    if verdict == "ENTER_LONG":
        tier_label, emoji, color = "LONG", "🟢", "#00e5a0"
    elif verdict == "ENTER_SHORT":
        tier_label, emoji, color = "SHORT", "🔴", "#ff4d6d"
    else:
        tier_label, emoji, color = "WAIT", "⏳", "#ffd23e"
    compact = {
        "verdict": verdict,
        "tier": tier_label,
        "emoji": emoji,
        "color": color,
        "confidence": full.get("confidence", 0),
        "long_grade": (full.get("long") or {}).get("grade"),
        "short_grade": (full.get("short") or {}).get("grade"),
        "note": full.get("verdict_note") or "",
        "computed_at": now,
    }
    _verdict_cache[pair] = (now, compact)
    return compact
