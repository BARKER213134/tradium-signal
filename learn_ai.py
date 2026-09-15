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
                  "generationConfig": {"maxOutputTokens": 400,
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
        mdl = os.getenv("GROQ_SIGNAL_MODEL", "openai/gpt-oss-20b")
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}"},
            json={"model": mdl,
                  "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 400, "temperature": 0.3},
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
    prompt = (
        "Ты трейдер-аналитик крипто-платформы. Самообучаемая модель "
        "одобрила сигнал. Дай разбор СТРОГО в 3 строках по-русски, "
        "каждая с префиксом:\nЗА: <главный аргумент входа>\n"
        "РИСК: <главная угроза сделке>\nПЛАН: <вход/выход одной фразой, "
        f"выход: {ctx.get('exit_plan') or 'TP+10% / SL-5% / до 96ч'}>"
        "\nБез воды, конкретно.\n\n"
        f"Сигнал: {ctx.get('sym')} {ctx.get('dir')}, источник "
        f"{ctx.get('src')}.\n"
        f"Серия MSO 2h: {ser} ({ms}).\n"
        f"🧿 валидатор (у ST-уровня и против режима): "
        f"{'ДА' if ctx.get('val') is True else 'нет'}.\n"
        f"Правило модели: {rule.get('label')} — исторически n={rule.get('n')}, "
        f"WR {rule.get('wr')}%, средний исход {rule.get('avg')}%/вход.\n"
        f"Широта рынка (доля пар в 4h-аптренде): {ctx.get('breadth', '?')}%.\n"
        f"Тренды монеты сейчас (1h/2h/4h/12h): {ctx.get('trends', '?')}.\n"
        "Контекст платформы: лучшие входы — против режима; зелёная серия "
        "27+ для лонга — зона обрыва; красная серия для шорта — запрет.")
    return _ask_gemini(prompt) or _ask_groq(prompt)


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
        _ms = c["ms"]
        _bk = ("gold" if c["val"] is True and c["dir"] == "LONG"
               and (_ms is None or _ms < 27)
               else "heat" if c["dir"] == "SHORT" and _ms is not None
               and _ms >= 27 else None)
        c["exit_plan"] = ((model.get("exits") or {}).get(_bk)
                          or {}).get("best_label")
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
