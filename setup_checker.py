"""🎰 Setup Checker v3 — разбор «как трейдер» от УРОВНЕЙ (13.08.2026).

v3 после аудита 180д и серии уровневых бэктестов:
  1. Структура: моментум, дистанции, последний закрытый бар
  2. Поток: CVD24/72, dz24, дельта ПОСЛЕДНЕГО БАРА (💪-логика)
  3. Funding, фаза 4h, климат 12h
  4. ЗОНЫ УРОВНЕЙ (движок 📏): ближайшие поддержка/сопротивление,
     сила, касания — планы входа строятся ОТ КРАЯ зоны
  5. Тренд-паттерн 1h/2h/4h/12h + 🪂/🪤-условия
  6. Свежие сетапы живых источников (📏💪🌋💨💎🎈🧱🪜🕯📐)
  7. Вердикт: ДА / МОЖНО / ЖДАТЬ / НЕТ + план (вход/стоп/цель/R:R)
Мёртвые источники (whale/shark/st_break/capitulation/second_flip)
вычищены; пороги — из бэктестов 13.08 (116k касаний, 13.9k исходов).
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


def _fetch_hist_klines(symbol: str, at_ms: int, n: int = 400):
    """Спот-свечи 1h, кончающиеся ВЫБРАННОЙ свечой (машина времени 15.08:
    даблклик по свече на графике → разбор на момент её закрытия).
    endTime = конец часа выбранной свечи → kd[-1] = эта свеча."""
    import json as _json
    import urllib.request as _ur
    end = int(at_ms) - int(at_ms) % 3600_000 + 3599_999
    url = ("https://data-api.binance.vision/api/v3/klines?symbol="
           f"{symbol}&interval=1h&endTime={end}&limit={n}")
    try:
        with _ur.urlopen(url, timeout=15) as r:
            rows = _json.loads(r.read())
        return [{"t": int(x[0]), "o": float(x[1]), "h": float(x[2]),
                 "l": float(x[3]), "c": float(x[4]), "v": float(x[5]),
                 "tb": float(x[9])} for x in rows]
    except Exception:
        return None


def check_setup(pair_input: str, at_ms: int | None = None) -> dict:
    """Направленный разбор пары: metrics + LONG/SHORT (за/против/план).
    at_ms — исторический режим: разбор на момент закрытия свечи at_ms
    (все метрики из свечей до неё; живой контекст funding/фаза/дельта-кэш
    и активные сделки пропускаются — их истории нет)."""
    pair = _normalize_pair(pair_input)
    symbol = pair.replace("/", "").upper()
    result = {
        "pair": pair, "symbol": symbol,
        "verdict": "WAIT", "verdict_note": "", "confidence": 0,
        "metrics": {}, "long": {}, "short": {},
        "active_trades": [], "recent_signals": [], "reasons": [],
    }
    if at_ms:
        result["historical"] = True
        result["as_of_ts"] = int(at_ms // 1000)
    try:
        # ── 1. Свечи 1h с тайкер-дельтой (400 баров) ─────────────────
        kd = None
        if at_ms:
            kd = _fetch_hist_klines(symbol, at_ms)
        if not kd and at_ms:
            result["verdict"] = "NO_DATA"
            result["reasons"].append(f"Нет исторических свечей {pair}")
            return result
        if not kd:
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

        # ── 2. Контекст платформы (живой — в историческом режиме нет) ─
        from database import _get_db
        db = _get_db()
        funding_pct = None
        phase = clim = None
        dz24 = None
        vitality = None
        if at_ms is None:
            try:
                fn = db.market_state.find_one({"_id": "funding_now"}) or {}
                fr = (fn.get("rates") or {}).get(pair)
                if fr is None:
                    fr = (fn.get("rates") or {}).get(symbol)
                if fr is not None:
                    funding_pct = float(fr) * 100
            except Exception:
                pass
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
            try:
                pc = db.pair_context.find_one({"_id": pair}) or {}
                dz24 = pc.get("dz24")
                vitality = pc.get("vitality")
            except Exception:
                pass
        # 📊 OI (15.08): изменение открытого интереса + квадрант цена×OI
        oi_d4h = oi_d24h = oi_quad = None
        if at_ms is None:
            try:
                _oin = (db.market_state.find_one({"_id": "oi_now"}, {"map": 1})
                        or {}).get("map") or {}
                _o = _oin.get(symbol) or _oin.get(pair.replace("/", ""))
                if _o:
                    oi_d4h = _o.get("d4h")
                    oi_d24h = _o.get("d24h")
                    from oi_collector import oi_quadrant
                    oi_quad = oi_quadrant(oi_d24h, mom24)
            except Exception:
                pass

        # ── 2b. Зоны уровней + тренды + дельта бара (v3, 13.08) ──────
        sup_z = res_z = None
        zavg = None
        try:
            from level_signal import build_zones
            _zs = build_zones(kd[:-1])
            seg = kd[-120:]
            zavg = sum(x["h"] - x["l"] for x in seg) / len(seg)
            for z in (_zs or []):
                if z["res"] and z["lo"] > price:
                    if res_z is None or z["lo"] < res_z["lo"]:
                        res_z = z
                elif not z["res"] and z["hi"] < price:
                    if sup_z is None or z["hi"] > sup_z["hi"]:
                        sup_z = z
        except Exception:
            pass
        d_sup = (price / sup_z["hi"] - 1) * 100 if sup_z else None
        d_res = (res_z["lo"] / price - 1) * 100 if res_z else None
        trend_pat = None
        tdm = {}
        try:
            from accum_detector import _trend_dirs
            tdm = _trend_dirs(kd) or {}
            if tdm:
                trend_pat = "".join(
                    "▲" if tdm.get(x, 0) > 0 else
                    "▼" if tdm.get(x, 0) < 0 else "·"
                    for x in ("1h", "2h", "4h", "12h"))
        except Exception:
            pass
        bar_dz = None
        if has_delta:
            try:
                dls = [2 * kd[i]["tb"] - kd[i]["v"]
                       for i in range(len(kd) - 1)]
                dh = dls[-201:-1]
                dm = sum(dh) / len(dh)
                dsd = (sum((x - dm) ** 2 for x in dh) / len(dh)) ** 0.5
                if dsd > 0:
                    bar_dz = round(dls[-1] / dsd, 2)
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
            "oi_d4h": oi_d4h, "oi_d24h": oi_d24h, "oi_quad": oi_quad,
            "vitality": vitality,
            "phase": phase, "phase_emoji": PH.get(phase, "?"),
            "climate12": clim, "climate12_emoji": PH.get(clim, "?"),
            "last_bar": {"green": lb_green, "close_pos": round(lb_pos)},
            # v3: зоны/тренды/дельта бара
            "trend_pat": trend_pat, "bar_dz": bar_dz,
            "support": ({"lo": sup_z["lo"], "hi": sup_z["hi"],
                         "strength": sup_z["strength"],
                         "touches": sup_z["touches"],
                         "dist_pct": round(d_sup, 2)} if sup_z else None),
            "resistance": ({"lo": res_z["lo"], "hi": res_z["hi"],
                            "strength": res_z["strength"],
                            "touches": res_z["touches"],
                            "dist_pct": round(d_res, 2)} if res_z else None),
        }

        # ── 3. Свежие сетапы платформы (≤24ч) + активные сделки ──────
        now_dt = (datetime.fromtimestamp(at_ms / 1000, tz=timezone.utc)
                  if at_ms else datetime.now(timezone.utc))
        naive_now = now_dt.replace(tzinfo=None)
        fresh = {}
        try:
            cut24 = naive_now - timedelta(hours=24)
            for d in db.new_strategy_signals.find(
                    {"pair": pair,
                     "created_at": {"$gte": cut24, "$lte": naive_now},
                     "strategy": {"$in": ["blowoff", "thin_pump",
                                          "floor_buy", "level_touch",
                                          "flip_retest", "corridor",
                                          "support_defense", "rev_candle",
                                          "channel_top"]}},
                    {"strategy": 1, "created_at": 1, "entry": 1, "rep_seq": 1,
                     "direction": 1, "state": 1,
                     "indicators.strength": 1, "indicators.absorb": 1,
                     "indicators.pullback": 1, "indicators.bulltrap": 1,
                     "indicators.push_against": 1,
                     "indicators.dz": 1}).sort("created_at", -1):
                st = d["strategy"]
                # level_touch различаем по направлению — обе стороны важны
                key = (f"{st}_{d.get('direction', '')}"
                       if st == "level_touch" else st)
                if key not in fresh:
                    d["age_h"] = round((naive_now - d["created_at"])
                                       .total_seconds() / 3600, 1)
                    fresh[key] = d
        except Exception:
            pass
        # активные сделки-сигналы в пути (WAITING, ≤7д; live-only —
        # исторического state нет)
        try:
            cut7 = naive_now - timedelta(days=7)
            for d in ([] if at_ms else db.new_strategy_signals.find(
                    {"pair": pair, "state": "WAITING",
                     "created_at": {"$gte": cut7},
                     "strategy": {"$in": ["blowoff", "thin_pump",
                                          "floor_buy", "level_touch",
                                          "flip_retest", "corridor"]}},
                    {"strategy": 1, "direction": 1, "entry": 1, "tp": 1,
                     "sl": 1, "created_at": 1}).sort("created_at", -1).limit(6)):
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
        # v3: сопротивление рядом — главный шорт-фактор (бэктест 📏)
        rsi_core_s = rsi1h is not None and 30 <= rsi1h < 50
        at_res = res_z is not None and d_res is not None and d_res <= 1.5
        if at_res:
            s_pros.append(f"сопротивление ⚡{res_z['strength']}% "
                          f"({res_z['touches']}кас) в {d_res:+.1f}% — вход "
                          f"лимиткой от края {_fmt_price(res_z['lo'])}"
                          + (" · RSI-ядро 30-50 (год: +0.58R WR63)"
                             if rsi_core_s else ""))
        if trend_pat and tdm.get("1h", 0) > 0 and tdm.get("4h", 0) < 0 \
                and tdm.get("12h", 0) < 0:
            s_pros.append(f"🪤 ловушка быков: отскок при медвежьих 4h+12h "
                          f"({trend_pat}) — год: +0.81R WR72")
        if bar_dz is not None and bar_dz <= -1:
            s_pros.append(f"💪 продавец в последнем баре (Δz {bar_dz:+.1f}) — "
                          f"с сопротивлением +0.77R WR71")
        elif bar_dz is not None and bar_dz >= 2 and at_res:
            s_cons.append(f"агрессивный покупатель в баре (Δz {bar_dz:+.1f}) "
                          f"— зона может не удержать")
        fresh_short = None
        for st in ("level_touch_SHORT", "blowoff", "thin_pump",
                   "rev_candle", "channel_top"):
            d = fresh.get(st)
            if d and d.get("direction") != "LONG":
                fresh_short = d
                em = {"level_touch_SHORT": "📏", "blowoff": "🌋",
                      "thin_pump": "💨", "rev_candle": "🕯",
                      "channel_top": "📐"}[st]
                rep = d.get("rep_seq") or 1
                _di = d.get("indicators") or {}
                rep_txt = ""
                if st == "thin_pump" and rep >= 2:
                    rep_txt = f" · 🔁 повтор №{rep} — усилитель (EV +1.44/+1.64)"
                elif st == "blowoff" and rep >= 3:
                    rep_txt = f" · 🔁 вершина №{rep} — усилитель (EV +0.89)"
                elif st == "level_touch_SHORT":
                    if _di.get("absorb"):
                        rep_txt = " · 💪 поглощение (+0.77..0.83R)"
                    elif _di.get("bulltrap"):
                        rep_txt = " · 🪤 (+0.81R WR72)"
                    elif _di.get("push_against"):
                        rep_txt = " · ⚠ дельта против — зона под давлением"
                s_pros.append(f"{em} свежий {d['strategy']} "
                              f"{d['age_h']:.0f}ч назад "
                              f"@ {_fmt_price(d.get('entry'))}{rep_txt}")
                break

        if funding_pct is not None and funding_pct <= -0.05:
            s_grade = "НЕТ"
        elif fresh_short and not late_short and not bounce_now:
            s_grade = "ДА"
        elif at_res and rsi_core_s and (seller or (bar_dz or 0) <= -1):
            s_grade = "ДА"
        elif seller and near_hi and overheat:
            s_grade = "ДА"
        elif seller and late_short and res_z is None:
            s_grade = "ЖДАТЬ ОТСКОКА"
        elif at_res or seller or (near_hi and overheat):
            s_grade = "МОЖНО"
        else:
            s_grade = "НЕТ"

        # v3: план от ЗОНЫ сопротивления, фолбэк — старая логика
        if res_z is not None and d_res is not None and d_res <= 6:
            entry_mid = res_z["lo"]
            s_plan = {"entry_zone": [round(res_z["lo"], 10),
                                     round(res_z["hi"], 10)],
                      "stop": round(res_z["hi"] + (zavg or 0) * 0.5, 10),
                      "target": round(res_z["lo"] * 0.94, 10),
                      "note": f"лимитка ОТ КРАЯ сопротивления "
                              f"⚡{res_z['strength']}% — вход по бэктесту 📏 "
                              f"(по рынку после отбоя эдж умирает)"}
        elif s_grade == "ЖДАТЬ ОТСКОКА":
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
        # v3: поддержка рядом — главный лонг-фактор (бэктест 📏)
        rsi_core_l = rsi1h is not None and 50 <= rsi1h < 70
        at_sup = sup_z is not None and d_sup is not None and d_sup <= 1.5
        if at_sup:
            l_pros.append(f"поддержка ⚡{sup_z['strength']}% "
                          f"({sup_z['touches']}кас) в {d_sup:+.1f}% — вход "
                          f"лимиткой от края {_fmt_price(sup_z['hi'])}"
                          + (" · RSI-ядро 50-70 (год: +0.69R WR68)"
                             if rsi_core_l else ""))
        if trend_pat and tdm.get("1h", 0) < 0 \
                and (tdm.get("4h", 0) > 0 or tdm.get("12h", 0) > 0):
            l_pros.append(f"🪂 откат при держащих старших ({trend_pat}) — "
                          f"год: +0.7..0.9R WR67-78")
        if bar_dz is not None and bar_dz >= 1:
            l_pros.append(f"💪 покупатель в последнем баре (Δz {bar_dz:+.1f})"
                          f" — с поддержкой +0.93..1.04R WR77-82")
        elif bar_dz is not None and bar_dz <= -2 and at_sup:
            l_cons.append(f"агрессивный продавец в баре (Δz {bar_dz:+.1f}) — "
                          f"поддержку продавливают (−0.30R)")
        fresh_long = None
        for st in ("level_touch_LONG", "floor_buy", "corridor",
                   "support_defense", "flip_retest", "rev_candle"):
            d = fresh.get(st)
            if d and d.get("direction") != "SHORT":
                fresh_long = d
                em = {"level_touch_LONG": "📏", "floor_buy": "💎",
                      "corridor": "🎈", "support_defense": "🧱",
                      "flip_retest": "🪜", "rev_candle": "🕯"}[st]
                _di = d.get("indicators") or {}
                extra = ""
                if st == "level_touch_LONG":
                    if _di.get("absorb"):
                        extra = " · 💪 поглощение (+0.93..1.04R WR77-82)"
                    elif _di.get("pullback"):
                        extra = " · 🪂 (+0.7..0.9R WR67-78)"
                    elif _di.get("push_against"):
                        extra = " · ⚠ дельта против — зона под давлением"
                l_pros.append(f"{em} свежий {d['strategy']} "
                              f"{d['age_h']:.0f}ч назад "
                              f"@ {_fmt_price(d.get('entry'))}{extra}")
                break

        if funding_pct is not None and funding_pct < -0.05:
            l_grade = "НЕТ"
        elif fresh_long and not late_long:
            l_grade = "ДА"
        elif at_sup and rsi_core_l and (buyer or (bar_dz or 0) >= 1):
            l_grade = "ДА"
        elif buyer and near_lo and oversold:
            l_grade = "ДА"
        elif buyer and late_long and sup_z is None:
            l_grade = "ЖДАТЬ ОТКАТА"
        elif at_sup or buyer or (near_lo and oversold):
            l_grade = "МОЖНО"
        else:
            l_grade = "НЕТ"

        # v3: план от ЗОНЫ поддержки, фолбэк — старая логика
        if sup_z is not None and d_sup is not None and d_sup <= 6:
            entry_mid_l = sup_z["hi"]
            l_plan = {"entry_zone": [round(sup_z["lo"], 10),
                                     round(sup_z["hi"], 10)],
                      "stop": round(sup_z["lo"] - (zavg or 0) * 0.5, 10),
                      "target": round(sup_z["hi"] * 1.06, 10),
                      "note": f"лимитка ОТ КРАЯ поддержки "
                              f"⚡{sup_z['strength']}% — вход по бэктесту 📏 "
                              f"(по рынку после отскока эдж умирает)"}
        elif l_grade == "ЖДАТЬ ОТКАТА":
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
                      "note": "структурный стоп под 24ч-лоем"}
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
                **pair_or, "created_at": {"$gte": since, "$lte": naive_now},
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
                "pair": pair, "flip_at": {"$gte": since, "$lte": naive_now},
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
                **pair_or, "detected_at": {"$gte": since, "$lte": naive_now},
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
    _mt = full.get("metrics") or {}
    _res = _mt.get("resistance") or {}
    _sup = _mt.get("support") or {}
    compact = {
        "verdict": verdict,
        "tier": tier_label,
        "emoji": emoji,
        "color": color,
        "confidence": full.get("confidence", 0),
        "long_grade": (full.get("long") or {}).get("grade"),
        "short_grade": (full.get("short") or {}).get("grade"),
        "note": full.get("verdict_note") or "",
        # 18.08: ключевые факты для TG-карточек («сигнал приходит разобранным»)
        "res_dist": _res.get("dist_pct"), "res_str": _res.get("strength"),
        "sup_dist": _sup.get("dist_pct"), "sup_str": _sup.get("strength"),
        # края зон для 🎯 авто-входов (18.08)
        "sup_edge": _sup.get("hi"), "sup_far": _sup.get("lo"),
        "res_edge": _res.get("lo"), "res_far": _res.get("hi"),
        "trend_pat": _mt.get("trend_pat"), "cvd24": _mt.get("cvd24"),
        "computed_at": now,
    }
    _verdict_cache[pair] = (now, compact)
    return compact


def maybe_auto_entry_alarm(pair: str) -> int:
    """🎯 АВТО-ВХОД (18.08, запрос юзера «система анализирует и ставит
    будильник там, где нужно войти»): для пар со свежими (≤3ч) сигналами —
    если разбор даёт край зоны на стороне сигнала в 0.6–8% от цены и
    грейд стороны не НЕТ, ставим авто-будильник на цену края. Дедуп:
    один ARMED-авто на (пара, сторона); не чаще раза в 12ч; мёртвые
    монеты пропускаются. Возвращает число поставленных."""
    try:
        from database import _get_db, utcnow
        from datetime import timedelta
        db = _get_db()
        sym = _normalize_pair(pair).replace("/", "")
        sigs = list(db.new_strategy_signals.find(
            {"symbol": sym, "backfill": {"$exists": False},
             "created_at": {"$gte": utcnow() - timedelta(hours=3)}},
            {"strategy": 1, "direction": 1, "vitality": 1}))
        # 18.08: 🔱/🌀 супертренд-пинги тоже порождают будильники (ZAMA:
        # st_mtf SHORT шёл, постановка его не видела)
        try:
            sigs += [{"strategy": "st_" + (d.get("tier") or "st"),
                      "direction": d.get("direction")}
                     for d in db.supertrend_signals.find(
                         {"pair_norm": sym, "tier": {"$in": ["vip", "mtf"]},
                          "created_at": {"$gte": utcnow() - timedelta(hours=3)}},
                         {"tier": 1, "direction": 1})]
        except Exception:
            pass
        if not sigs:
            return 0
        # 💀-гейт по pair_context (18.08): st-пинги не несут штамп
        # живости — статус монеты надёжнее брать из контекста
        try:
            _pcv = db.pair_context.find_one({"_id": sym}, {"vitality": 1})
            if _pcv and _pcv.get("vitality") == "dead":
                return 0
        except Exception:
            pass
        c = get_compact_verdict(pair)
        placed = 0
        for side in ("LONG", "SHORT"):
            ss = [s for s in sigs if s.get("direction") == side]
            if not ss:
                continue
            if any(s.get("vitality") == "dead" for s in ss):
                continue
            grade = c.get("long_grade" if side == "LONG" else "short_grade")
            if grade == "НЕТ":
                continue
            if side == "LONG":
                edge, far, dist = c.get("sup_edge"), c.get("sup_far"), c.get("sup_dist")
            else:
                edge, far, dist = c.get("res_edge"), c.get("res_far"), c.get("res_dist")
            if not edge or dist is None or not (0.6 <= dist <= 8):
                continue
            # дедуп: ARMED-авто по паре+стороне / ставился <12ч назад.
            # 18.08: повторный сигнал той же стороны при ЖИВОМ будильнике —
            # не спамим новым, но ПЕРЕВЗВОДИМ уровень, если свежая разметка
            # сдвинула край зоны >0.5% (зоны перестраиваются с рынком)
            armed = db.alarms.find_one({
                "symbol": sym, "auto": "entry", "sig_dir": side,
                "state": "ARMED"})
            if armed:
                edge_ = (c.get("sup_edge") if side == "LONG"
                         else c.get("res_edge"))
                if (edge_ and armed.get("price")
                        and abs(edge_ / armed["price"] - 1) > 0.005):
                    db.alarms.update_one(
                        {"_id": armed["_id"]},
                        {"$set": {"price": float(edge_),
                                  "rearmed_at": utcnow(),
                                  "note": (armed.get("note") or "")
                                  + f" · 🔁 уровень обновлён → {edge_:.6g}"}})
                continue
            dup = db.alarms.find_one({
                "symbol": sym, "auto": "entry", "sig_dir": side,
                "created_at": {"$gte": utcnow() - timedelta(hours=12)}})
            if dup:
                continue
            sg = 1 if side == "LONG" else -1
            stop = far * (1 - sg * 0.004) if far else edge * (1 - sg * 0.012)
            risk = abs(edge - stop)
            target = edge + sg * risk * 1.5
            src = ss[0].get("strategy")
            note = (f"🎯 авто-вход по сигналу {src} {side}: лимитка от края "
                    f"зоны {edge:.6g} · стоп за зону {stop:.6g} "
                    f"({abs(stop / edge - 1) * 100:.1f}%) · цель 1.5R {target:.6g}")
            db.alarms.insert_one({
                "symbol": sym, "kind": "price", "price": float(edge),
                "side": "below" if side == "LONG" else "above",
                "state": "ARMED", "price_hit": False,
                "auto": "entry", "sig_src": src, "sig_dir": side,
                "note": note, "created_at": utcnow()})
            placed += 1
        return placed
    except Exception:
        logger.debug("[auto-entry] %s fail", pair, exc_info=True)
        return 0


def signal_tg_context(pair: str, direction: str | None = None) -> str:
    """🎰-контекст для TG-карточки сигнала (18.08, запрос юзера «сигнал
    должен подаваться уже разобранным»): вердикты сторон, ближайшая зона
    на пути сделки с ⚠-предупреждением, тренды со ✓/✗ по рабочему ТФ
    (LONG↔2h, SHORT↔12h), CVD24. Пусто при любой ошибке — карточка не
    ломается. get_compact_verdict кэширован (5 мин + фоновый цикл)."""
    try:
        c = get_compact_verdict(pair)
    except Exception:
        return ""
    try:
        lines = []
        lg, sg = c.get("long_grade") or "?", c.get("short_grade") or "?"
        lines.append(f"🎰 разбор: LONG={lg} · SHORT={sg} · общий {c.get('tier')}")
        f2 = []
        if direction == "LONG" and c.get("res_dist") is not None:
            warn = " ⚠️ путь к цели через зону" if c["res_dist"] < 2 else ""
            f2.append(f"🔴 сопротивление ⚡{c.get('res_str')}% в +{c['res_dist']}%{warn}")
        if direction == "SHORT" and c.get("sup_dist") is not None:
            warn = " ⚠️ путь к цели через зону" if c["sup_dist"] < 2 else ""
            f2.append(f"🟢 поддержка ⚡{c.get('sup_str')}% в −{c['sup_dist']}%{warn}")
        tp_ = c.get("trend_pat")
        if tp_ and len(tp_) == 4:
            ok = (tp_[1] == "▲") if direction == "LONG" else \
                 (tp_[3] == "▼") if direction == "SHORT" else None
            f2.append(f"тренды {tp_}"
                      + (" ✓" if ok else " ✗" if ok is False else ""))
        cv = c.get("cvd24")
        if cv is not None:
            f2.append(f"CVD24 {'+' if cv >= 0 else ''}{cv / 1e6:.1f}M")
        if f2:
            lines.append(" · ".join(f2))
        return "\n" + "\n".join(lines)
    except Exception:
        return ""
