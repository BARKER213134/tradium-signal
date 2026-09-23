# -*- coding: utf-8 -*-
"""🧠 Академия: AI-разбор каждой одобренной позиции (18.09.26).

Провайдер-цепочка: Gemini (если есть ключ) → Groq → пропуск. Ключи —
ТОЛЬКО env/system_config (публичный репо!). Разбираются сигналы, которые
модель Академии одобрила (ACTIVE_SHOW): ~30-60/день — влезает в бесплатные
лимиты с запасом. Результат кэшируется в learn_ai (одна запись на сигнал),
лента вкладки подмешивает текст. Журнал/TG не трогает."""
import logging
import os

logger = logging.getLogger(__name__)

MAX_AGE_H = 6          # разбираем только свежие одобренные
BATCH = 8              # за один проход цикла (цикл ~5 мин → ≤96/час)


def _cfg_key(name, env):
    k = os.getenv(env)
    if k:
        return k
    try:
        from database import _get_db
        d = _get_db().system_config.find_one({"_id": name}) or {}
        return d.get("value")
    except Exception:
        return None


def _ask_gemini(prompt):
    key = _cfg_key("gemini_api_key", "GEMINI_API_KEY")
    if not key:
        return None
    try:
        import requests
        mdl = os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite")
        r = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{mdl}:generateContent",
            params={"key": key},
            json={"contents": [{"parts": [{"text": prompt}]}],
                  "generationConfig": {"maxOutputTokens": 700,
                                       "temperature": 0.3}},
            timeout=25)
        if r.status_code == 200:
            t = r.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
            return {"text": t, "provider": "gemini"} if t else None
        logger.warning(f"[ai] gemini {r.status_code}: {r.text[:150]}")
    except Exception:
        logger.debug("[ai] gemini fail", exc_info=True)
    return None


def _ask_groq(prompt):
    key = _cfg_key("groq_api_key", "GROQ_API_KEY")
    if not key:
        return None
    try:
        import requests
        mdl = os.getenv("GROQ_SIGNAL_MODEL", "openai/gpt-oss-120b")
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}"},
            json={"model": mdl,
                  "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 900, "temperature": 0.3},
            timeout=25)
        if r.status_code == 200:
            t = r.json()["choices"][0]["message"]["content"].strip()
            return {"text": t, "provider": "groq"} if t else None
        logger.warning(f"[ai] groq {r.status_code}: {r.text[:150]}")
    except Exception:
        logger.debug("[ai] groq fail", exc_info=True)
    return None


def analyze(ctx):
    """ctx → короткий структурированный разбор (3 строки) или None."""
    rule = ctx.get("rule") or {}
    ms = ctx.get("ms")
    ser = ("нет данных" if ms is None else
           f"{'зелёная' if ms > 0 else 'красная'}, {abs(ms)} баров")
    vd = ctx.get("verdict")
    vphr = ("одобрила этот сигнал" if vd == "ACTIVE_SHOW" else
            "СКРЫЛА этот сигнал — его клетка исторически в минусе"
            if vd == "ACTIVE_HIDE" else
            "не имеет уверенного вердикта по этому сигналу")
    # 💰 живая цена, цена на сигнале и сдвиг — оцениваем вход ИМЕННО СЕЙЧАС
    sym = ctx.get("sym") or ""
    px = None
    drift_txt = ""
    try:
        from exchange import get_klines_any
        bars = get_klines_any(sym[:-4] + "/USDT", "1h", 120)
        if bars:
            px = float(bars[-1]["c"])
            at = ctx.get("at")
            if at is not None:
                at_ms = int(at.timestamp() * 1000)
                age_h = max(0.0, (bars[-1]["t"] - at_ms) / 3_600_000)
                sig_px = None
                for b in bars:
                    if b["t"] + 3_600_000 > at_ms:
                        break
                    sig_px = float(b["c"])
                if sig_px:
                    dr = (px / sig_px - 1) * 100
                    sgd = 1 if ctx.get("dir") == "LONG" else -1
                    gone = dr * sgd
                    drift_txt = (
                        f"Сигнал был {age_h:.0f}ч назад по ~{sig_px}. С тех "
                        f"пор цена сдвинулась {dr:+.2f}% — "
                        + ("движение УЖЕ ЧАСТИЧНО ОТРАБОТАНО в сторону "
                           "сделки, вход догоняющий" if gone > 1.5 else
                           "цена ушла ПРОТИВ сделки, вход лучше сигнального, "
                           "но проверь не сломан ли сетап" if gone < -1.5 else
                           "цена почти на месте сигнала") + ".\n")
    except Exception:
        pass
    ek = ctx.get("exit_key") or "base"
    tp_pct = 15.0 if ek == "tp15" else 10.0
    sl_pct = 3.5 if ek == "sl35" else 5.0
    sg = 1 if ctx.get("dir") == "LONG" else -1
    cond = {"be5": "после +5% перенести стоп в безубыток",
            "trail4": "после +5% вести трейл-стоп 4% от макс. закрытия",
            "half5": "на +5% зафиксировать половину",
            "h48": "выйти не позже 48 часов"}.get(ek, "")
    if px:
        tp_px = px * (1 + sg * tp_pct / 100)
        sl_px = px * (1 - sg * sl_pct / 100)
        fmt = (lambda v: f"{v:.6f}".rstrip("0").rstrip(".")
               if v < 100 else f"{v:.2f}")
        lvl = (f"цена сейчас {fmt(px)}, TP {fmt(tp_px)} ({tp_pct:+.0f}%), "
               f"SL {fmt(sl_px)} (−{sl_pct}%)"
               + (f", {cond}" if cond else "") + ", горизонт до 96ч")
    else:
        lvl = (f"цена недоступна — уровни в %: TP {tp_pct:+.0f}%, "
               f"SL −{sl_pct}%" + (f", {cond}" if cond else ""))
    # 💸 фандинг и возраст — если достали
    extra = ""
    fund = ctx.get("fund")
    if fund is not None:
        extra += (f"Фандинг перпа: {fund:+.4f}%/8ч"
                  + (" — экстрем, толпа зажата против движения"
                     if abs(fund) >= 0.05 else "") + ".\n")
    age = ctx.get("age_days")
    if age is not None:
        extra += (f"Возраст монеты: {age}д"
                  + (" — МОЛОДАЯ (<70д): перегрев ведёт себя иначе, "
                     "стат. клетка ещё копится" if age < 70 else "") + ".\n")
    lv = rule.get("live")
    live_txt = (f" Живые paper-исходы правила: n={lv['n']}, WR {lv['wr']}, "
                f"avg {lv['avg']}." if lv else "")
    prompt = (
        "Ты опытный крипто-трейдер и риск-менеджер платформы. "
        f"Самообучаемая модель {vphr}. Дай разбор СТРОГО в 5 строках "
        "по-русски, каждая с префиксом:\n"
        "ЗА: <главный аргумент входа, с цифрами>\n"
        "ПРОТИВ: <главная угроза сделке, конкретно>\n"
        f"УРОВНИ: <точные цены: {lvl}>\n"
        "ВЕДЕНИЕ: <как вести позицию: когда двигать стоп, когда фиксировать, "
        "при каком развитии выйти раньше>\n"
        "ВЕРДИКТ: <БРАТЬ СЕЙЧАС / ПРОПУСТИТЬ / ЖДАТЬ ОТКАТ> уверенность "
        "<N>/10 — одной фразой почему\n"
        "ВАЖНО: оценивай вход ИМЕННО СЕЙЧАС по текущей цене — если "
        "движение от сигнала уже отработано, честно говори ПРОПУСТИТЬ "
        "или ЖДАТЬ ОТКАТ. Опирайся ТОЛЬКО на данные ниже, уровни не "
        "выдумывай, размер не советуй.\n\n"
        f"Сигнал: {sym} {ctx.get('dir')}, источник {ctx.get('src')}.\n"
        f"{drift_txt}"
        f"Серия MSO 2h: {ser} ({ms}).\n"
        f"🧿 валидатор (у ST-уровня И против режима): "
        f"{'ДА' if ctx.get('val') is True else 'нет'}.\n"
        f"Правило модели: {rule.get('label')} — n={rule.get('n')}, "
        f"WR {rule.get('wr')}%, avg {rule.get('avg')}%/вход, "
        f"EV {rule.get('ev')}.{live_txt}\n"
        f"Широта рынка (доля пар в 4h-аптренде): {ctx.get('breadth', '?')}%.\n"
        f"Тренды монеты (1h/2h/4h/12h): {ctx.get('trends', '?')}.\n"
        f"{extra}"
        f"План выхода корзины (само-обучение): "
        f"{ctx.get('exit_plan') or 'канон TP+10/SL-5/96ч'}.\n"
        "Законы платформы (из 45д бэктестов): лучшие входы — ПРОТИВ "
        "режима; зелёная серия 27+ для лонга — зона обрыва (-2.15); "
        "красная серия для шорта — запрет; первое касание монеты "
        "сильнее повторных; одобренные лонги бегут дальше +10%. "
        + '₿ Режим BTC (просадка от 30д-макс, бэктест коррекции 20.05→08.07): у максимума — шорты бесполезны (+0.1), работают лонги; коррекция −8..−15% — шорты сильны (+3.9, WR 63: supertrend_daily/mtf, volume_surge, second_flip, shark), лонги от дна умирают (🥇 −0.2); капитуляция ≤−15% — снова лонги (whale/volume_surge +4). Шорты в лайв — только в коррекции. Широта рынка сама по себе ×2-лонги не запрещает (в эйфории >80 они +2.9, тормоз по широте снят 22.09), но ×1-лонги при широте >80 слабы (−0.1). ')
    thr = ctx.get("throttle")
    if thr:
        prompt += ("\nРежим рынка по тормозу лайва: " + str(thr) +
                   " (кап 0 = система стоит; иначе — только контекст, не запрет).")
    lessons = _lessons_text()
    if lessons:
        prompt += ("\n\nУРОКИ ТВОИХ ПРОШЛЫХ РАЗБОРОВ (автосверка вердиктов "
                   "с фактическими исходами paper-сделок — учти их):\n"
                   + lessons)
    return _ask_gemini(prompt) or _ask_groq(prompt)


_LESSONS_CACHE = {"t": 0.0, "txt": None}


def _lessons_text():
    import time as _t
    if _t.time() - _LESSONS_CACHE["t"] < 600:
        return _LESSONS_CACHE["txt"]
    txt = None
    try:
        from database import _get_db
        d = _get_db().system_config.find_one({"_id": "ai_lessons"}) or {}
        txt = d.get("text")
    except Exception:
        pass
    _LESSONS_CACHE.update({"t": _t.time(), "txt": txt})
    return txt


def _parse_verdict(text):
    """'ВЕРДИКТ: БРАТЬ СЕЙЧАС ... 7/10' → ('БРАТЬ', 7)."""
    import re
    m = re.search(r"ВЕРДИКТ:\s*([А-ЯЁ ]+?)(?:\s+уверенность)?\s+(\d+)\s*/\s*10",
                  text or "", re.I)
    if not m:
        m2 = re.search(r"ВЕРДИКТ:\s*([А-ЯЁ ]+)", text or "", re.I)
        if not m2:
            return None, None
        w, cf = m2.group(1), None
    else:
        w, cf = m.group(1), int(m.group(2))
    w = w.upper()
    for key in ("БРАТЬ", "ПРОПУСТИТЬ", "ЖДАТЬ"):
        if key in w:
            return key, cf
    return None, cf


def _distill_pm(db):
    """Выжимка УРОКов из пост-мортемов (21д) в 3-5 правил. Порциями
    60 → 25 → 12 (23.09: на 60 уроках LLM три ночи подряд молчал —
    квота Gemini к 04:xx исчерпана, у Groq лимит на длинный промпт).
    Возвращает готовый блок текста или None."""
    import time as _tm
    from datetime import timedelta as _td
    from database import utcnow as _now
    pm = list(db.learn_ai_post.find(
        {"at": {"$gte": _now() - _td(days=21)}, "lesson": {"$ne": None}},
        {"lesson": 1, "r": 1, "dir": 1, "src": 1, "state": 1}))
    if len(pm) < 5:
        return None
    for take in (60, 25, 12):
        raw = "\n".join(
            f"- [{d.get('src')} {d.get('dir')} {d.get('state')} {(d.get('r') or 0):+.1f}%] {d['lesson']}"
            for d in pm[-take:])
        dp = ("Ниже уроки, которые ты сам вывел из разборов закрытых "
              "сделок. Сожми их в 3-5 ОБОБЩЁННЫХ правил по-русски "
              "(каждое с '- ', одна строка, конкретно: источник/"
              "направление/режим → что делать). Только правила, "
              "повторяющиеся в данных; без воды.\n\n" + raw)
        dist = _ask_gemini(dp) or _ask_groq(dp)
        if dist and dist.get("text"):
            return ("Уроки из разборов закрытых сделок "
                    f"(выжимка из {len(pm)}, порция {take}):\n" + dist["text"].strip())
        _tm.sleep(15)
    return None


def retry_pm_if_stale():
    """Повтор выжимки после 07:00 UTC (сброс квоты Gemini), не чаще раза
    в 2 часа; зовётся из 10-минутного цикла дайджеста (sync; в to_thread)."""
    from database import _get_db, utcnow
    db = _get_db()
    al = db.system_config.find_one({"_id": "ai_lessons"}) or {}
    if not al.get("pm_stale"):
        return False
    now = utcnow()
    if now.hour < 7:
        return False
    try:
        from datetime import datetime as _dt
        last = _dt.fromisoformat(al.get("pm_last_try") or "2000-01-01T00:00:00")
        if (now - last).total_seconds() < 7200:
            return False
    except Exception:
        pass
    block = _distill_pm(db)
    upd = {"pm_last_try": now.isoformat()}
    if block:
        text = al.get("text") or ""
        i = text.find("Уроки из разборов")
        text = (text[:i].rstrip() + "\n" + block) if i >= 0 else (text + "\n" + block)
        upd.update({"text": text, "pm_stale": False})
        _LESSONS_CACHE["t"] = 0.0
        logger.info("[ai-lessons] выжимка пост-мортемов обновлена повтором")
    db.system_config.update_one({"_id": "ai_lessons"}, {"$set": upd})
    return bool(block)


def refresh_lessons():
    """🧠→🎓 Самообучение советчика (ночной вызов): сверка вердиктов AI
    с фактическими исходами paper-сделок (join по общему ключу ns_/st_),
    выжимка системных ошибок → system_config.ai_lessons. Уроки попадают
    в каждый следующий промпт."""
    from database import _get_db, utcnow
    db = _get_db()
    outcomes = {d["_id"]: d for d in db.academy_paper.find(
        {"state": {"$in": ["TP", "SL", "TIMEOUT"]}},
        {"r": 1, "dir": 1})}
    rows = []
    for d in db.learn_ai.find({}, {"text": 1, "dir": 1}):
        o = outcomes.get(d["_id"])
        if not o or o.get("r") is None:
            continue
        v, cf = _parse_verdict(d.get("text"))
        if not v:
            continue
        rows.append({"v": v, "cf": cf, "dir": d.get("dir") or o.get("dir"),
                     "r": float(o["r"])})
    if len(rows) < 15:
        logger.info(f"[ai-lessons] мало сверок ({len(rows)}) — уроки не обновляю")
        return None

    def st(sel):
        a = [x["r"] for x in sel]
        if not a:
            return None
        return {"n": len(a),
                "wr": round(sum(1 for r in a if r > 0) / len(a) * 100),
                "avg": round(sum(a) / len(a), 2)}
    by_v = {v: st([x for x in rows if x["v"] == v])
            for v in ("БРАТЬ", "ПРОПУСТИТЬ", "ЖДАТЬ")}
    lines = []
    b = by_v.get("БРАТЬ")
    p = by_v.get("ПРОПУСТИТЬ")
    if b:
        lines.append(f"- Твои «БРАТЬ» по факту: n={b['n']}, WR {b['wr']}%, "
                     f"avgR {b['avg']:+.2f}."
                     + (" Ты слишком щедр на БРАТЬ — будь строже."
                        if b["avg"] < 0.3 else
                        " Калибровка в порядке — держи планку."))
    if p and b and p["avg"] > b["avg"] + 0.5:
        lines.append(f"- Сделки, которые ты ПРОПУСКАЛ, шли ЛУЧШЕ взятых "
                     f"({p['avg']:+.2f} vs {b['avg']:+.2f}) — ты "
                     f"перестраховываешься, пересмотри критерии отказа.")
    for dr, lbl in (("LONG", "лонгам"), ("SHORT", "шортам")):
        s = st([x for x in rows if x["v"] == "БРАТЬ" and x["dir"] == dr])
        if s and s["n"] >= 8:
            if s["avg"] < -0.5:
                lines.append(f"- Твои «БРАТЬ» по {lbl}: avgR {s['avg']:+.2f} "
                             f"(n={s['n']}) — СИСТЕМНАЯ ОШИБКА, по {lbl} "
                             f"будь заметно строже.")
            else:
                lines.append(f"- «БРАТЬ» по {lbl}: avgR {s['avg']:+.2f} "
                             f"(n={s['n']}).")
    hi = st([x for x in rows if x["v"] == "БРАТЬ" and (x["cf"] or 0) >= 7])
    lo = st([x for x in rows if x["v"] == "БРАТЬ" and 0 < (x["cf"] or 0) <= 5])
    if hi and lo and hi["n"] >= 8 and lo["n"] >= 8:
        if hi["avg"] <= lo["avg"]:
            lines.append(f"- Твоя уверенность НЕ калибрована: 7+/10 дало "
                         f"{hi['avg']:+.2f}, а 5-/10 дало {lo['avg']:+.2f} — "
                         f"не завышай уверенность.")
        else:
            lines.append(f"- Уверенность калибрована: 7+/10 → {hi['avg']:+.2f} "
                         f"vs 5-/10 → {lo['avg']:+.2f}.")
    # 📝 уроки из пост-мортемов закрытых сделок (21д) — выжимка LLM
    pm_stale = False
    try:
        block = _distill_pm(db)
        if block:
            lines.append(block)
        else:
            # не теряем прошлую выжимку, если провайдеры не ответили;
            # повтор — retry_pm_if_stale() после 07:00 UTC (квота Gemini)
            pm_stale = True
            _prev = (db.system_config.find_one({"_id": "ai_lessons"}) or {}).get("text") or ""
            _i = _prev.find("Уроки из разборов")
            if _i >= 0:
                lines.append(_prev[_i:].split("\n(выжимка не обновилась")[0].strip()
                             + "\n(выжимка не обновилась — LLM не ответил, повтор после 07:00 UTC)")
            logger.warning("[ai-lessons] выжимка пост-мортемов: LLM не ответил, оставил прошлую")
    except Exception:
        logger.debug("[ai-lessons] postmortem distill fail", exc_info=True)
    text = "\n".join(lines)
    db.system_config.update_one(
        {"_id": "ai_lessons"},
        {"$set": {"text": text, "n": len(rows), "pm_stale": pm_stale,
                  "pm_last_try": utcnow().isoformat(),
                  "by_verdict": by_v, "updated": utcnow().isoformat()}},
        upsert=True)
    _LESSONS_CACHE["t"] = 0.0
    logger.info(f"[ai-lessons] уроки обновлены: {len(rows)} сверок, "
                f"{len(lines)} уроков")
    return text


def _sig_ctx(db, key):
    """Достать сигнал по ключу ленты (ns_/st_ + ObjectId) → ctx-словарь."""
    from bson import ObjectId
    try:
        col, sid = key.split("_", 1)
        oid = ObjectId(sid)
    except Exception:
        return None
    if col == "ns":
        d = db.new_strategy_signals.find_one({"_id": oid})
        if not d:
            return None
        return {"sym": d.get("symbol") or (d.get("pair") or "").replace("/", ""),
                "src": d.get("strategy") or "?", "dir": d.get("direction"),
                "val": d.get("validator_ok"), "ms": d.get("mso_streak2h"),
                "at": d.get("created_at")}
    if col == "as":
        d = db.academy_signals.find_one({"_id": oid})
        if not d:
            return None
        return {"sym": d.get("symbol") or (d.get("pair") or "").replace("/", ""),
                "src": d.get("strategy") or "?", "dir": d.get("direction"),
                "val": d.get("validator_ok"), "ms": d.get("mso_streak2h"),
                "at": d.get("created_at")}
    if col == "st":
        d = db.supertrend_signals.find_one({"_id": oid})
        if not d:
            return None
        return {"sym": d.get("pair_norm") or (d.get("pair") or "").replace("/", ""),
                "src": "supertrend_" + (d.get("tier") or "?"),
                "dir": d.get("direction"), "val": d.get("validator_ok"),
                "ms": d.get("mso_streak2h"), "at": d.get("created_at")}
    return None


def analyze_key(key):
    """On-demand разбор одной сделки по ключу ленты (sync; в to_thread).
    Кэш тот же learn_ai — повторный клик бесплатный."""
    import learn_engine as le
    from database import _get_db, utcnow
    db = _get_db()
    ex = db.learn_ai.find_one({"_id": key})
    if ex:
        return {"ok": True, "text": ex["text"],
                "provider": ex.get("provider"), "cached": True}
    c = _sig_ctx(db, key)
    if not c:
        return {"ok": False, "err": "сигнал не найден"}
    model = db.learn_model.find_one({"_id": "active"}) or {}
    status, rule = le.score_signal(model, c["src"], c["dir"],
                                   c["val"], c["ms"], rg=le.btc_regime_now()[1])
    if rule is None:
        rules = {r["id"]: r for r in model.get("rules") or []}
        dl = "LONG" if c["dir"] == "LONG" else "SHORT"
        rule = rules.get(f"p_{c['src']}_{dl}")
    c["rule"] = rule
    c["verdict"] = status
    try:
        tm = db.market_state.find_one({"_id": "trend_matrix"}) or {}
        rows = tm.get("rows") or []
        up = [r for r in rows if ((r.get("d") or {}).get("4h") or 0) > 0]
        n4 = [r for r in rows if (r.get("d") or {}).get("4h")]
        c["breadth"] = round(len(up) / max(1, len(n4)) * 100)
        td = next((r.get("d") or {} for r in rows
                   if r.get("s") == c["sym"]), {})
        arrow = {1: "▲", -1: "▼"}
        c["trends"] = "/".join(arrow.get(td.get(tf), "·")
                               for tf in ("1h", "2h", "4h", "12h"))
    except Exception:
        pass
    _ms = c["ms"]
    _bk = ("gold" if c["val"] is True and c["dir"] == "LONG"
           and (_ms is None or _ms < 27)
           else "heat" if c["dir"] == "SHORT" and _ms is not None
           and _ms >= 27 else None)
    _ex = (model.get("exits") or {}).get(_bk) or {}
    c["exit_plan"] = _ex.get("best_label")
    c["exit_key"] = _ex.get("best")
    try:
        _thr = db.system_config.find_one({"_id": "live_throttle"}) or {}
        if _thr.get("level"):
            c["throttle"] = _thr.get("reason")
    except Exception:
        pass
    try:
        _ag = db.coin_ages.find_one(
            {"_id": c["sym"][:-4] + "/USDT"}, {"days": 1})
        if _ag:
            c["age_days"] = _ag.get("days")
    except Exception:
        pass
    try:
        import requests as _rq
        _fr = _rq.get("https://fapi.binance.com/fapi/v1/premiumIndex",
                      params={"symbol": c["sym"]}, timeout=8)
        if _fr.status_code == 200:
            c["fund"] = float(_fr.json().get("lastFundingRate") or 0) * 100
    except Exception:
        pass
    res = analyze(c)
    if not res:
        return {"ok": False, "err": "AI-провайдеры не ответили"}
    db.learn_ai.update_one(
        {"_id": key},
        {"$set": {"sym": c["sym"], "dir": c["dir"], "src": c["src"],
                  "text": res["text"], "provider": res["provider"],
                  "sig_at": c.get("at"), "at": utcnow(),
                  "on_demand": True}},
        upsert=True)
    return {"ok": True, "text": res["text"], "provider": res["provider"]}


POST_BATCH = 4         # 📝 пост-мортемов за цикл (закрытые live первыми)


def _trade_path(pair, t_open, t_close, entry, sg):
    """MFE/MAE (%) по 1h-свечам между входом и выходом — что цена
    реально делала внутри сделки."""
    try:
        from exchange import get_klines_any
        hours = int((t_close - t_open).total_seconds() / 3600) + 3
        c = get_klines_any(pair, "1h", min(max(hours, 5), 200))
        o_ms = int(t_open.timestamp() * 1000)
        c_ms = int(t_close.timestamp() * 1000)
        bars = [b for b in c if o_ms - 3_600_000 <= b["t"] <= c_ms]
        if not bars or not entry:
            return None
        if sg > 0:
            mfe = max(b["h"] for b in bars) / entry - 1
            mae = min(b["l"] for b in bars) / entry - 1
        else:
            mfe = 1 - min(b["l"] for b in bars) / entry
            mae = 1 - max(b["h"] for b in bars) / entry
        return {"mfe": round(mfe * 100, 2), "mae": round(mae * 100, 2),
                "bars": len(bars)}
    except Exception:
        return None


def postmortem(key, force=False):
    """📝 Разбор ЗАКРЫТОЙ сделки после факта (sync; в to_thread): почему
    сработало/нет, был ли прав довходовой вердикт, переносимый урок.
    Кэш learn_ai_post; уроки идут в refresh_lessons → промпт советчика."""
    import re
    from database import _get_db, utcnow
    db = _get_db()
    ex = db.learn_ai_post.find_one({"_id": key})
    pre_doc = db.learn_ai.find_one({"_id": key}) or {}
    if ex and not force:
        return {"ok": True, "text": ex["text"], "pre": pre_doc.get("text"),
                "provider": ex.get("provider"), "cached": True}
    t = db.academy_paper.find_one({"_id": key})
    if not t:
        return {"ok": False, "err": "сделка не найдена"}
    if t.get("state") == "OPEN" or t.get("r") is None:
        return {"ok": False, "err": "сделка ещё открыта — итог будет после закрытия"}
    sg = 1 if t["dir"] == "LONG" else -1
    t_open, t_close = t["opened_at"], t.get("closed_at") or utcnow()
    dur_h = round((t_close - t_open).total_seconds() / 3600, 1)
    path = _trade_path(t.get("pair") or (t["sym"][:-4] + "/USDT"),
                       t_open, t_close, t.get("entry"), sg)
    sc = _sig_ctx(db, key) or {}
    ms, val = sc.get("ms"), sc.get("val")
    pre = pre_doc.get("text")
    pre_v, pre_cf = _parse_verdict(pre)
    r = float(t["r"])
    extra = 0.0
    if t.get("live"):
        try:
            from learn_paper import LIVE_FEE_EXTRA
            extra = LIVE_FEE_EXTRA + (t.get("fund_cost") or 0)
        except Exception:
            extra = 0.1
    net = r - extra
    prompt = (
        "Ты трейдер-аналитик крипто-платформы. Сделка ЗАКРЫТА — дай разбор "
        "ПОСЛЕ ФАКТА, строго в 4 строках по-русски, каждая с префиксом:\n"
        "ИТОГ: <сухо: исход, сколько прошла цена за/против (MFE/MAE), сколько держали>\n"
        "ПОЧЕМУ: <главная причина, почему сработало / не сработало — конкретно>\n"
        "ВЕРДИКТ ДО ВХОДА: <был ли прав довходовой разбор; в чём именно попал или ошибся>\n"
        "УРОК: <ОДНА переносимая фраза для будущих сделок этого типа "
        "(источник/направление/режим/серия) — без воды>\n"
        "Опирайся ТОЛЬКО на данные ниже.\n\n"
        f"Сделка: {t['sym']} {t['dir']}, источник {t.get('src')}, правило "
        f"«{t.get('rule')}» (EV {t.get('ev')}), размер {t.get('size')}"
        f"{', 💎 live-срез' if t.get('live') else ''}"
        f"{', 🔬 проба-разведка' if t.get('probe') else ''}.\n"
        f"Вход {t.get('entry')} в {t_open.strftime('%d.%m %H:%M')} UTC, "
        f"исход {t.get('state')} через {dur_h}ч, результат {r:+.2f}% "
        f"(чистыми с комиссией/фандингом {net:+.2f}%).\n"
        + (f"Путь цены внутри сделки: максимум в пользу +{path['mfe']}%, "
           f"максимум против {path['mae']}% ({path['bars']} часовых баров).\n"
           if path else "")
        + f"Серия MSO 2h на сигнале: {ms}. 🧿 валидатор: "
          f"{'ДА' if val is True else 'нет'}.\n"
        + (f"Довходовой разбор AI (вердикт {pre_v} {pre_cf}/10):\n{pre}\n"
           if pre else "Довходового разбора не было.\n")
        + "Законы платформы: лучшие входы — против режима; зелёная серия "
          "27+ для лонга — обрыв; красная для шорта — запрет; одобренные "
          "лонги бегут дальше +10%; защитные ранние выходы проигрывают. "
        + '₿ Режим BTC (просадка от 30д-макс, бэктест коррекции 20.05→08.07): у максимума — шорты бесполезны (+0.1), работают лонги; коррекция −8..−15% — шорты сильны (+3.9, WR 63: supertrend_daily/mtf, volume_surge, second_flip, shark), лонги от дна умирают (🥇 −0.2); капитуляция ≤−15% — снова лонги (whale/volume_surge +4). Шорты в лайв — только в коррекции. Широта рынка сама по себе ×2-лонги не запрещает (в эйфории >80 они +2.9, тормоз по широте снят 22.09), но ×1-лонги при широте >80 слабы (−0.1). ')
    res = _ask_gemini(prompt) or _ask_groq(prompt)
    if not res:
        return {"ok": False, "err": "AI-провайдеры не ответили"}
    m = re.search(r"УРОК:\s*(.+)", res["text"])
    lesson = m.group(1).strip() if m else None
    db.learn_ai_post.update_one(
        {"_id": key},
        {"$set": {"sym": t["sym"], "dir": t["dir"], "src": t.get("src"),
                  "rule_id": t.get("rule_id"), "state": t.get("state"),
                  "r": r, "net": round(net, 2), "live": bool(t.get("live")),
                  "mfe": path and path["mfe"], "mae": path and path["mae"],
                  "pre_verdict": pre_v, "pre_conf": pre_cf,
                  "text": res["text"], "lesson": lesson,
                  "provider": res["provider"], "closed_at": t_close,
                  "at": utcnow()}},
        upsert=True)
    return {"ok": True, "text": res["text"], "pre": pre,
            "provider": res["provider"]}


def run_postmortems(max_n=POST_BATCH):
    """Автопост-мортемы: закрытые за 48ч без разбора, live первыми."""
    from database import _get_db, utcnow
    from datetime import timedelta
    db = _get_db()
    since = utcnow() - timedelta(hours=48)
    done = 0
    for live_first in (True, False):
        q = {"state": {"$in": ["TP", "SL", "TIMEOUT"]},
             "closed_at": {"$gte": since}}
        if live_first:
            q["live"] = True
        for t in db.academy_paper.find(q, {"_id": 1}).sort("closed_at", -1).limit(60):
            if done >= max_n:
                return done
            if db.learn_ai_post.find_one({"_id": t["_id"]}, {"_id": 1}):
                continue
            res = postmortem(str(t["_id"]))
            if not res.get("ok"):
                if "провайдеры" in str(res.get("err")):
                    return done
                continue
            done += 1
    if done:
        logger.info(f"[ai] пост-мортемов: {done}")
    return done


def run_batch(max_n=BATCH):
    """Разбор свежих одобренных сигналов без анализа (sync; в to_thread)."""
    from datetime import timedelta
    from database import _get_db, utcnow
    import learn_engine as le
    db = _get_db()
    model = db.learn_model.find_one({"_id": "active"})
    if not model:
        return 0
    since = utcnow() - timedelta(hours=MAX_AGE_H)
    cands = []
    for d in db.new_strategy_signals.find(
            {"created_at": {"$gte": since},
             "direction": {"$in": ["LONG", "SHORT"]}},
            {"pair": 1, "symbol": 1, "direction": 1, "strategy": 1,
             "created_at": 1, "validator_ok": 1, "mso_streak2h": 1}):
        cands.append(("ns_" + str(d["_id"]), {
            "sym": d.get("symbol") or (d.get("pair") or "").replace("/", ""),
            "src": d.get("strategy") or "?", "dir": d["direction"],
            "val": d.get("validator_ok"), "ms": d.get("mso_streak2h"),
            "at": d["created_at"]}))
    for d in db.supertrend_signals.find(
            {"created_at": {"$gte": since},
             "direction": {"$in": ["LONG", "SHORT"]}},
            {"pair": 1, "pair_norm": 1, "direction": 1, "tier": 1,
             "created_at": 1, "validator_ok": 1, "mso_streak2h": 1}):
        cands.append(("st_" + str(d["_id"]), {
            "sym": d.get("pair_norm") or (d.get("pair") or "").replace("/", ""),
            "src": "supertrend_" + (d.get("tier") or "?"),
            "dir": d["direction"], "val": d.get("validator_ok"),
            "ms": d.get("mso_streak2h"), "at": d["created_at"]}))
    for d in db.academy_signals.find(
            {"created_at": {"$gte": since},
             "direction": {"$in": ["LONG", "SHORT"]}},
            {"pair": 1, "symbol": 1, "direction": 1, "strategy": 1,
             "created_at": 1, "validator_ok": 1, "mso_streak2h": 1}):
        cands.append(("as_" + str(d["_id"]), {
            "sym": d.get("symbol") or (d.get("pair") or "").replace("/", ""),
            "src": d.get("strategy") or "?", "dir": d["direction"],
            "val": d.get("validator_ok"), "ms": d.get("mso_streak2h"),
            "at": d["created_at"]}))
    cands.sort(key=lambda x: x[1]["at"], reverse=True)
    # рыночный контекст один на батч
    breadth = trends_map = None
    try:
        tm = db.market_state.find_one({"_id": "trend_matrix"}) or {}
        rows = tm.get("rows") or []
        up = [r for r in rows if ((r.get("d") or {}).get("4h") or 0) > 0]
        n4 = [r for r in rows if (r.get("d") or {}).get("4h")]
        breadth = round(len(up) / max(1, len(n4)) * 100)
        trends_map = {r.get("s"): r.get("d") or {} for r in rows}
    except Exception:
        pass
    done = 0
    for key, c in cands:
        if done >= max_n:
            break
        status, rule = le.score_signal(model, c["src"], c["dir"],
                                       c["val"], c["ms"])
        if status != "ACTIVE_SHOW":
            continue
        if db.learn_ai.find_one({"_id": key}, {"_id": 1}):
            continue
        c["rule"] = rule
        c["verdict"] = "ACTIVE_SHOW"
        _ms = c["ms"]
        _bk = ("gold" if c["val"] is True and c["dir"] == "LONG"
               and (_ms is None or _ms < 27)
               else "heat" if c["dir"] == "SHORT" and _ms is not None
               and _ms >= 27 else None)
        _ex = (model.get("exits") or {}).get(_bk) or {}
        c["exit_plan"] = _ex.get("best_label")
        c["exit_key"] = _ex.get("best")
        try:
            _ag = db.coin_ages.find_one(
                {"_id": c["sym"][:-4] + "/USDT"}, {"days": 1})
            if _ag:
                c["age_days"] = _ag.get("days")
        except Exception:
            pass
        c["breadth"] = breadth
        td = (trends_map or {}).get(c["sym"]) or {}
        arrow = {1: "▲", -1: "▼"}
        c["trends"] = "/".join(arrow.get(td.get(tf), "·")
                               for tf in ("1h", "2h", "4h", "12h"))
        res = analyze(c)
        if not res:
            break   # оба провайдера легли — не молотить впустую
        db.learn_ai.update_one(
            {"_id": key},
            {"$set": {"sym": c["sym"], "dir": c["dir"], "src": c["src"],
                      "text": res["text"], "provider": res["provider"],
                      "sig_at": c["at"], "at": utcnow()}},
            upsert=True)
        done += 1
    if done:
        logger.info(f"[ai] разобрано позиций: {done}")
    return done
