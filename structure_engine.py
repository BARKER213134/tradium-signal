"""📐 Структура рынка: регрессионный канал (13.11.26).

Юзер: «по графикам вроде аптренд, а на самом деле ни хера — можем ли мы
определять тренд/диапазон автоматически». SuperTrend 2h для лонгов школы
не различал ничего (▲ −0.05 / ▼ +0.02), а при ST ▲ настоящий восходящий
канал был только в 20% случаев.

Канал = линейная регрессия закрытий за N закрытых баров. Дрейф линии
меряем в ширинах канала (±2σ остатков): > +0.75 → аптренд (U),
< −0.75 → даунтренд (D), иначе диапазон (R). Плюс место цены в коридоре
max/min окна (низ/середина/верх).

Проверка на 16 037 сделках школы с 20.09 (параметры взяты априори, не
подбирались): LONG U +1.18 / R −0.05 / D −2.85 (WR 44/36/17), порядок
сохраняется в обеих половинах; SHORT лучший — D × середина (WR 61 +2.63).

Ось школы — 2h, N=120 (10 дней). Без заглядывания: на вход подаются
только ЗАКРЫТЫЕ бары.
"""
import numpy as np

N = 120            # баров в окне (2h → 10 дней)
MOVE_THR = 0.75    # порог дрейфа в ширинах канала
KCH_LABEL = {"U": "📐 канал ▲", "R": "📐 диапазон", "D": "📐 канал ▼"}
KPOS_LABEL = {"lo": "низ", "mid": "середина", "hi": "верх"}

# 🟢🔴 MSO старших ТФ — оси школы (14.11, юзер: «добавь оси 4h и 12h в школу»).
# Аудит 14.11 (mso_use.py): MSO — фильтр направления, не точка входа: шорт
# при зелёном 12h ломает 7 источников из 16, лонг при зелёном 4h усиливает
# 5 из 19. Правила выводит школа по клеткам источник×направление×цвет.
# Свечи — РОДНЫЕ 4h/12h с запасом: на коротком окне цвет расходится с
# графиком (12h: 86 баров → 95% совпадений, 250 → 99.4%).
M4_LABEL = {"G": "MSO 4h 🟢", "R": "MSO 4h 🔴"}
M12_LABEL = {"G": "MSO 12h 🟢", "R": "MSO 12h 🔴", "H": "MSO 12h 🌡 перегрев"}
M4_MS, M12_MS = 14_400_000, 43_200_000
M4_BARS, M12_BARS = 400, 300      # сколько старших свечей берёт живой расчёт
M_MIN_BARS = 120                  # меньше — цвет ненадёжен, штампа нет
M12_HOT = 85.0


def m4_bin(v):
    try:
        if v is None or v != v:
            return None
        return "G" if float(v) > 50 else "R"
    except Exception:
        return None


def m12_bin(v):
    try:
        if v is None or v != v:
            return None
        v = float(v)
        return "H" if v >= M12_HOT else ("G" if v > 50 else "R")
    except Exception:
        return None


# 🧭 СОСТОЯНИЕ МОНЕТЫ (14.11, юзер: «как отбирать монеты — они катились, а
# сигналы приходят» → «добавь эти индикаторы в журнал и в таблицу»).
# Аудит coin_select.py (43.5k лонгов, +5/−15/14д, обе половины): выше EMA200
# на 2h 71.4% / +0.20 против ниже 65.0% / −0.79 (лучше у 9 источников из 19,
# хуже ни у одного); у 30-дневного минимума 57.6% / −0.95; MSO 12h красный
# 10+ баров −0.59 против +0.06. Шортам зеркально, НО дневной ST вниз дольше
# 30 дней шорту вредит (−0.42 против +0.33) — «лежит», движение выдохлось.
CS_LABEL = {"A": "выше EMA200", "B": "ниже EMA200", "L": "у 30-дневного минимума"}
CS_LOW_PCT = 5.0      # не выше стольких % от 30-дневного минимума = «у минимума»


def cs_bin(e2, u30):
    try:
        if e2 is None:
            return None
        if u30 is not None and float(u30) <= CS_LOW_PCT:
            return "L"
        return "A" if float(e2) > 0 else "B"
    except Exception:
        return None


def coin_state(closed):
    """closed — закрытые 2h-бары. → {e2: % от EMA200, u30: % от 30д-минимума,
    d30: % от 30д-максимума, r30: рост за 30 дней %, k30: дрейф 30д-канала}."""
    try:
        if not closed or len(closed) < 210:
            return {}
        c = np.array([b["c"] for b in closed], dtype=float)
        h = np.array([b["h"] for b in closed], dtype=float)
        l = np.array([b["l"] for b in closed], dtype=float)
        px = float(c[-1])
        if not px > 0:
            return {}
        a = 2.0 / 201.0
        ema = float(c[0])
        for x in c[1:]:
            ema += a * (float(x) - ema)
        out = {"e2": round((px / ema - 1) * 100, 1)}
        if len(c) >= 300:
            n30 = min(360, len(c))
            out["u30"] = round((px / float(l[-n30:].min()) - 1) * 100, 1)
            out["d30"] = round((px / float(h[-n30:].max()) - 1) * 100, 1)
        if len(c) > 360:
            out["r30"] = round((px / float(c[-361]) - 1) * 100, 1)
            k = channel(c, h, l, n=360)
            if k:
                out["k30"] = k["mv"]
        return out
    except Exception:
        return {}


def _mso_streak(osc):
    """длина текущей серии цвета MSO: + зелёная, − красная"""
    s = 0
    for v in reversed(osc):
        if v is None or v != v:
            break
        side = 1 if v > 50 else -1
        if s == 0:
            s = side
        elif (s > 0) == (side > 0):
            s += side
        else:
            break
    return s


def _st_daily(bars12):
    """Дневной SuperTrend (10, 3.0) из закрытых 12h-баров → (направление ±1,
    сколько дней без переворота) или (0, 0), если истории мало."""
    try:
        days = {}
        for b in bars12:
            d = int(b["t"] // 86_400_000)
            x = days.get(d)
            if x is None:
                days[d] = [b["h"], b["l"], b["c"], 1]
            else:
                x[0] = max(x[0], b["h"])
                x[1] = min(x[1], b["l"])
                x[2] = b["c"]
                x[3] += 1
        rows = [days[d] for d in sorted(days) if days[d][3] == 2]
        if len(rows) < 25:
            return 0, 0
        dr = 1
        since = 0
        ub = lb = a = None
        pc = None
        for i, (hh, ll, cc, _) in enumerate(rows):
            tr = hh - ll if pc is None else max(hh - ll, abs(hh - pc), abs(ll - pc))
            a = tr if a is None else (a * 9 + tr) / 10
            mid = (hh + ll) / 2
            u = mid + 3 * a
            lo = mid - 3 * a
            if i == 0:
                ub, lb = u, lo
            else:
                ub = u if (u < ub or pc > ub) else ub
                lb = lo if (lo > lb or pc < lb) else lb
                nd = 1 if cc > ub else (-1 if cc < lb else dr)
                since = 0 if nd != dr else since + 1
                dr = nd
            pc = cc
        return int(dr), int(since)
    except Exception:
        return 0, 0


def coin_state_map(db):
    """Состояние монет для журнала: {SYM: {cs, e2, u30, d30, r30, rk30, k30,
    m4, m4s, m12, m12s, d1, d1a}} по карте (записи не старше 3 баров)."""
    out = {}
    try:
        bar = _bar_now()
        mp = (db.market_state.find_one({"_id": MAP_ID}, {"map": 1}) or {}).get("map") or {}
        for s, v in mp.items():
            if not v or v.get("e2") is None or (v.get("b") or 0) < bar - MAP_MAX_AGE_BARS:
                continue
            out[s] = {"cs": cs_bin(v.get("e2"), v.get("u30")),
                      "e2": v.get("e2"), "u30": v.get("u30"), "d30": v.get("d30"),
                      "r30": v.get("r30"), "k30": v.get("k30"),
                      "m4": v.get("m4"), "m4s": v.get("m4s"),
                      "m12": v.get("m12"), "m12s": v.get("m12s"),
                      "d1": v.get("d1"), "d1a": v.get("d1a")}
        rs = sorted(x["r30"] for x in out.values() if x.get("r30") is not None)
        if len(rs) >= 50:
            import bisect
            for x in out.values():
                if x.get("r30") is not None:
                    x["rk30"] = round(bisect.bisect_left(rs, x["r30"]) / (len(rs) - 1), 2)
            for x in out.values():
                x["n30"] = len(rs)
    except Exception:
        pass
    return out


def mso_htf_now(pair, klines_fn, now_ms, prev=None):
    """MSO на последнем ЗАКРЫТОМ баре 4h и 12h → {m4, m4b, m12, m12b}.
    prev — прошлая запись карты: если старший бар тот же, свечи не качаем."""
    out = {}
    prev = prev or {}
    for tf, ms, lim, key in (("4h", M4_MS, M4_BARS, "m4"), ("12h", M12_MS, M12_BARS, "m12")):
        try:
            cb = int(now_ms // ms) - 1
            if (prev.get(key) is not None and prev.get(key + "b") == cb
                    and (key + "s") in prev and (key != "m12" or "d1" in prev)):
                for _k in (key, key + "s") + (("d1", "d1a") if key == "m12" else ()):
                    out[_k] = prev.get(_k)
                out[key + "b"] = cb
                continue
            kd = klines_fn(pair, tf, lim)
            closed = [b for b in (kd or []) if b["t"] + ms <= now_ms]
            if len(closed) < M_MIN_BARS:
                continue
            from mso_retest import mso_series
            osc = mso_series(closed)
            v = osc[-1]
            if v is None or v != v:
                continue
            out[key] = round(float(v), 1)
            out[key + "s"] = _mso_streak(osc)
            out[key + "b"] = int(closed[-1]["t"] // ms)
            if key == "m12":      # дневной SuperTrend — из тех же 12h-свечей
                out["d1"], out["d1a"] = _st_daily(closed)
        except Exception:
            continue
    return out

_SX = {}


def _sx(n):
    v = _SX.get(n)
    if v is None:
        x = np.arange(n, dtype=float) - (n - 1) / 2.0
        v = _SX[n] = (x, float(x @ x))
    return v


def channel(c, h, l, n=N):
    """c/h/l — ряды ЗАКРЫТЫХ баров по возрастанию времени (последний —
    самый свежий закрытый). → {ch, mv, pos, kp, top, bot} или None."""
    try:
        if c is None or len(c) < n:
            return None
        cc = np.asarray(c[-n:], dtype=float)
        x, xx = _sx(n)
        k = float(x @ cc) / xx
        res = cc - (cc.mean() + k * x)
        sd = float(res.std())
        if not sd > 0:
            return None
        mv = k * (n - 1) / (4.0 * sd)
        hi = float(np.max(np.asarray(h[-n:], dtype=float)))
        lo = float(np.min(np.asarray(l[-n:], dtype=float)))
        pos = (float(cc[-1]) - lo) / (hi - lo) if hi > lo else 0.5
        return {"ch": "U" if mv > MOVE_THR else "D" if mv < -MOVE_THR else "R",
                "mv": round(float(mv), 2), "pos": round(float(pos), 3),
                "kp": "lo" if pos < 1 / 3 else "hi" if pos > 2 / 3 else "mid",
                "top": hi, "bot": lo}
    except Exception:
        return None


def channel_from_1h(kd, n=N):
    """kd — 1h dict-бары (t мс, h, l, c) по возрастанию, последний может
    быть формирующимся. Ресемпл в 2h как у школы (learn_engine._resample:
    t // 2ч); последнюю 2h-группу отбрасываем — она не закрыта."""
    try:
        t2, h2, l2, c2 = [], [], [], []
        cur = None
        for b in kd:
            g = int(b["t"]) // 7_200_000
            if g != cur:
                cur = g
                t2.append(g)
                h2.append(b["h"])
                l2.append(b["l"])
                c2.append(b["c"])
            else:
                h2[-1] = max(h2[-1], b["h"])
                l2[-1] = min(l2[-1], b["l"])
                c2[-1] = b["c"]
        return channel(c2[:-1], h2[:-1], l2[:-1], n)
    except Exception:
        return None


# ───────────── 🗺 карта структуры по ВСЕМ монетам (13.11, вечер) ─────────────
# Скан рынка (accum_detector → trend_matrix.k) покрывает ~174 ликвидные пары,
# а сигналы школы идут по ~535 монетам: за 3 часа штамп получили 5 сделок из
# 33. Отдельная карта market_state.structure_map: вселенная = монеты с
# сигналами за 30 дней + открытые сделки школы; обновление порциями из цикла
# школы (5 мин), каждая монета — раз на 2h-бар. Запись свежа, пока ей не
# больше MAP_MAX_AGE_BARS баров: канал за 120 баров от одного бара почти не
# меняется, а пустой штамп хуже слегка устаревшего.
MAP_ID = "structure_map"
MAP_MAX_AGE_BARS = 3
MAP_UNIVERSE_DAYS = 30
_BAR_MS = 7_200_000


def _bar_now():
    import time
    return int(time.time() * 1000) // _BAR_MS


def _sym_ok(s):
    return bool(s) and "." not in s and "$" not in s and s.endswith("USDT")


_UNI_INFO = {}     # состав вселенной: свои монеты / фьючерсы Binance / добавлено


def _universe(db, days=MAP_UNIVERSE_DAYS):
    from datetime import timedelta
    from database import utcnow
    since = utcnow() - timedelta(days=days)
    out = set()
    for coll, fields in (("new_strategy_signals", ("symbol", "pair")),
                         ("supertrend_signals", ("pair_norm", "pair")),
                         ("academy_signals", ("symbol", "pair"))):
        try:
            for d in db[coll].find({"created_at": {"$gte": since}},
                                   {f: 1 for f in fields}):
                s = d.get(fields[0]) or (d.get(fields[1]) or "").replace("/", "")
                if s:
                    out.add(s.upper())
        except Exception:
            pass
    try:
        for d in db.academy_paper.find({"state": "OPEN"}, {"sym": 1}):
            if d.get("sym"):
                out.add(d["sym"].upper())
    except Exception:
        pass
    # 14.11 (юзер: «нет фьючерсных монет всех … добавь только те, что на
    # Binance торгуются»): все USDT-фьючерсы Binance, даже без наших сигналов
    # за 30 дней — на них идут линии, новые источники и MSO. Монеты только с
    # BingX, акции и сырьё НЕ добавляем. Список недоступен → вселенная прежняя.
    _UNI_INFO["own"] = len(out)
    try:
        from futures_data import get_all_futures_pairs
        _bn = [str(x).upper() for x in (get_all_futures_pairs() or [])]
        _UNI_INFO["bn_total"] = len(_bn)
        _UNI_INFO["bn_new"] = len([x for x in _bn if _sym_ok(x) and x not in out])
        out.update(x for x in _bn if _sym_ok(x))
    except Exception:
        _UNI_INFO["bn_total"] = None
    return sorted(s for s in out if _sym_ok(s))


def refresh_map(db, priority=None, budget=120, workers=6, deadline_s=25,
                only_missing=False, klines_fn=None):
    """Досчитать структуру порцией монет. priority — монеты текущих
    кандидатов (первыми). only_missing — трогать только тех, у кого записи
    нет или она старше MAP_MAX_AGE_BARS (быстрый проход перед скорингом).
    Возвращает число обновлённых. Ошибки не роняют цикл школы."""
    from concurrent.futures import ThreadPoolExecutor, wait
    from database import utcnow
    try:
        bar = _bar_now()
        doc = db.market_state.find_one({"_id": MAP_ID}) or {}
        mp = doc.get("map") or {}
        uni = doc.get("uni") or []
        sets = {}
        # пересобираем вселенную раз в бар; и сразу, если фьючерсы Binance в неё
        # ещё не подмешивались (нет uni_info или список тогда был недоступен)
        _ui = doc.get("uni_info") or {}
        import time as _tu
        _retry = (not _ui.get("bn_total")) and _tu.time() - _UNI_INFO.get("_try", 0) > 900
        if not only_missing and (doc.get("uni_b") != bar or not uni or _retry):
            _UNI_INFO["_try"] = _tu.time()
            uni = _universe(db)
            sets["uni"] = uni
            sets["uni_b"] = bar
            sets["uni_info"] = {"own": _UNI_INFO.get("own"), "bn_total": _UNI_INFO.get("bn_total"),
                                "bn_new": _UNI_INFO.get("bn_new"), "n": len(uni)}
        pri = [s.upper() for s in (priority or []) if _sym_ok((s or "").upper())]
        seen = set()
        order = []
        # быстрый проход — только монеты текущих кандидатов
        for s in (pri if only_missing else pri + list(uni)):
            if s in seen:
                continue
            seen.add(s)
            b = (mp.get(s) or {}).get("b")
            if only_missing:
                if b is not None and b >= bar - MAP_MAX_AGE_BARS:
                    continue
            elif b == bar:
                continue
            order.append((0 if s in pri else 1, b or 0, s))
        order.sort()
        todo = [s for _, _, s in order[:budget]]
        if todo:
            if klines_fn is None:
                from exchange import get_klines_any as klines_fn

            import time as _tm
            now_ms = int(_tm.time() * 1000)

            def _one(sym):
                """канал 10д + новые источники (new_setups) из одних 2h-свечей;
                запасной путь — 1h×250 только для канала"""
                k = None
                found = []
                px = None
                lev = []
                bt = None
                rs = None
                mh = {}
                cs = {}
                try:
                    pair = sym[:-4] + "/USDT"
                    kd = klines_fn(pair, "2h", 520)     # 500 закрытых — окно графика
                    closed = [b for b in (kd or []) if b["t"] + _BAR_MS <= now_ms]
                    if len(closed) >= N:
                        k = channel([b["c"] for b in closed], [b["h"] for b in closed],
                                    [b["l"] for b in closed])
                        # 🟢🔴 MSO 4h/12h для осей школы (свечи старших ТФ —
                        # только когда закрылся новый старший бар)
                        mh = mso_htf_now(pair, klines_fn, now_ms, mp.get(sym))
                        # 🧭 состояние монеты для журнала (EMA200, 30д-минимум, 30д-канал)
                        cs = coin_state(closed)
                        # сигнал — только если последний закрытый бар свежий
                        if closed[-1]["t"] + 2 * _BAR_MS > now_ms:
                            import new_setups as _ns
                            found = _ns.detect(closed)
                            px = float(closed[-1]["c"])
                            rs = _ns.rsi_last(closed)
                            # 📐 касание линий графика (line_touch / line_dash)
                            try:
                                import line_touch as _lt
                                lev = _lt.events(closed)
                                bt = int(closed[-1]["t"])
                            except Exception:
                                lev = []
                    else:
                        kd1 = klines_fn(pair, "1h", 250)
                        k = channel_from_1h(kd1) if kd1 and len(kd1) >= 245 else None
                except Exception:
                    pass
                return sym, k, found, px, lev, bt, rs, mh, cs

            ex = ThreadPoolExecutor(max_workers=workers)
            futs = [ex.submit(_one, s) for s in todo]
            done, _ = wait(futs, timeout=deadline_s)
            ex.shutdown(wait=False)
            for f in done:
                try:
                    sym, k, _found, _px, _lev, _bt, _rs, _mh, _cs = f.result()
                except Exception:
                    continue
                if _lev and _px and _bt:
                    try:
                        import line_touch as _lt
                        _lt.store(db, sym, _lev, _px, _bt, utcnow())
                    except Exception:
                        pass
                # 🆕 13.11: новые источники range_bottom / low_break → academy_signals
                if _found and _px:
                    try:
                        import new_setups as _ns
                        _ns.store(db, sym, _found, _px, utcnow())
                    except Exception:
                        pass
                # нет свечей (делист/короткая история) — помечаем бар, чтобы
                # не долбить биржу каждый цикл; ch=None → штампа нет
                sets["map." + sym] = (dict({"ch": k["ch"], "mv": k["mv"], "pos": k["pos"],
                                            "kp": k["kp"], "b": bar, "rsi": _rs},
                                           **dict(_mh or {}, **(_cs or {})))
                                      if k else {"ch": None, "b": bar})
            # 🚿 доля перепроданных монет (RSI 2h < 30) по карте → источник
            # flush_climax (new_setups): кульминация объёма в момент смыва рынка
            try:
                _rv = {}
                for _s, _v in mp.items():
                    if (_v or {}).get("rsi") is not None and (_v.get("b") or 0) >= bar - 1:
                        _rv[_s] = _v["rsi"]
                for _key, _v in sets.items():
                    if _key.startswith("map.") and isinstance(_v, dict):
                        if _v.get("rsi") is not None:
                            _rv[_key[4:]] = _v["rsi"]
                        else:
                            _rv.pop(_key[4:], None)
                if len(_rv) >= 100:
                    _mos = sum(1 for _x in _rv.values() if _x < 30) / len(_rv)
                    sets["mkt_os"] = round(_mos, 3)
                    sets["mkt_os_n"] = len(_rv)
                    import new_setups as _ns
                    if _mos >= _ns.FLUSH_OS:
                        _ns.flush_from_climax(db, _mos, utcnow())
            except Exception:
                pass
        if sets:
            sets["updated"] = utcnow().isoformat()
            db.market_state.update_one({"_id": MAP_ID}, {"$set": sets}, upsert=True)
        return sum(1 for k in sets if k.startswith("map."))
    except Exception:
        return 0


def load_map(db, tmrows=None):
    """{SYM: {ch, mv, pos, kp}} для скоринга: карта всех монет (не старше
    MAP_MAX_AGE_BARS баров) поверх канала из скана рынка (trend_matrix.k)."""
    out = {}
    try:
        if tmrows is None:
            tmrows = (db.market_state.find_one({"_id": "trend_matrix"}) or {}).get("rows") or []
        for r in tmrows:
            if r.get("k") and r.get("s"):
                out[r["s"]] = r["k"]
    except Exception:
        pass
    try:
        bar = _bar_now()
        mp = (db.market_state.find_one({"_id": MAP_ID}, {"map": 1}) or {}).get("map") or {}
        for s, v in mp.items():
            if v and v.get("ch") and (v.get("b") or 0) >= bar - MAP_MAX_AGE_BARS:
                out[s] = {"ch": v["ch"], "mv": v.get("mv"), "pos": v.get("pos"), "kp": v.get("kp"),
                          "m4": m4_bin(v.get("m4")), "m12": m12_bin(v.get("m12")),
                          "m4v": v.get("m4"), "m12v": v.get("m12")}
    except Exception:
        pass
    return out


# ───────────── 🌊 ширина структуры рынка (13.11, аудит сигналов) ─────────────
# Доля монет в восходящем канале минус доля в нисходящем по карте всех монет.
# Универсальный вывод аудита: шорты при ширине ниже −0.5 в минусе у 15
# источников в обеих половинах периода → такие шорты не шлём (config).
_MB = {"t": 0.0, "v": None}


def market_breadth(db=None, ttl=300):
    """(U − D) / N по свежим записям карты; None, если монет меньше 100."""
    import time
    if time.time() - _MB["t"] < ttl:
        return _MB["v"]
    v = None
    try:
        if db is None:
            from database import _get_db
            db = _get_db()
        m = load_map(db)
        n = len(m)
        if n >= 100:
            u = sum(1 for x in m.values() if x.get("ch") == "U")
            d = sum(1 for x in m.values() if x.get("ch") == "D")
            v = round((u - d) / n, 3)
    except Exception:
        v = None
    _MB["t"], _MB["v"] = time.time(), v
    return v


def shorts_blocked(db=None):
    """True — рынок глубоко внизу, шорты не шлём (config.MB_SHORT_BLOCK)."""
    try:
        from config import MB_SHORT_BLOCK
        mb = market_breadth(db)
        return mb is not None and mb < MB_SHORT_BLOCK
    except Exception:
        return False
