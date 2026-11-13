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
        if not only_missing and (doc.get("uni_b") != bar or not uni):
            uni = _universe(db)
            sets["uni"] = uni
            sets["uni_b"] = bar
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
                try:
                    pair = sym[:-4] + "/USDT"
                    kd = klines_fn(pair, "2h", 400)
                    closed = [b for b in (kd or []) if b["t"] + _BAR_MS <= now_ms]
                    if len(closed) >= N:
                        k = channel([b["c"] for b in closed], [b["h"] for b in closed],
                                    [b["l"] for b in closed])
                        # сигнал — только если последний закрытый бар свежий
                        if closed[-1]["t"] + 2 * _BAR_MS > now_ms:
                            import new_setups as _ns
                            found = _ns.detect(closed)
                            px = float(closed[-1]["c"])
                    else:
                        kd1 = klines_fn(pair, "1h", 250)
                        k = channel_from_1h(kd1) if kd1 and len(kd1) >= 245 else None
                except Exception:
                    pass
                return sym, k, found, px

            ex = ThreadPoolExecutor(max_workers=workers)
            futs = [ex.submit(_one, s) for s in todo]
            done, _ = wait(futs, timeout=deadline_s)
            ex.shutdown(wait=False)
            for f in done:
                try:
                    sym, k, _found, _px = f.result()
                except Exception:
                    continue
                # 🆕 13.11: новые источники range_bottom / low_break → academy_signals
                if _found and _px:
                    try:
                        import new_setups as _ns
                        _ns.store(db, sym, _found, _px, utcnow())
                    except Exception:
                        pass
                # нет свечей (делист/короткая история) — помечаем бар, чтобы
                # не долбить биржу каждый цикл; ch=None → штампа нет
                sets["map." + sym] = ({"ch": k["ch"], "mv": k["mv"], "pos": k["pos"],
                                       "kp": k["kp"], "b": bar}
                                      if k else {"ch": None, "b": bar})
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
                out[s] = {"ch": v["ch"], "mv": v.get("mv"), "pos": v.get("pos"), "kp": v.get("kp")}
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
