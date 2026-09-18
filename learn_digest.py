# -*- coding: utf-8 -*-
"""🌅 Академия: утренний TG-дайджест (08:00 UTC, 18.09.26).

Одно сообщение в бот сигналов: режим рынка, свежая модель, топ-5
одобренных за 12ч, paper-статистика, деградации, Groq-brief. Отправка
раз в день (маркер в system_config), fail-open."""
import logging
from datetime import timedelta

logger = logging.getLogger(__name__)


def _breadth(db):
    try:
        rows = (db.market_state.find_one({"_id": "trend_matrix"})
                or {}).get("rows") or []
        n4 = [r for r in rows if (r.get("d") or {}).get("4h")]
        up = [r for r in n4 if r["d"]["4h"] > 0]
        return round(len(up) / max(1, len(n4)) * 100) if n4 else None
    except Exception:
        return None


def build_text(db):
    import learn_engine as le
    from database import utcnow
    model = db.learn_model.find_one({"_id": "active"})
    if not model:
        return None
    br = _breadth(db)
    mode = ("🟢 режим лонгов от дна" if br is not None and br <= 45 else
            "🔴 перегрев — смотри шорты" if br is not None and br >= 55 else
            "🟡 нейтрально")
    # топ-5 одобренных за 12ч
    since = utcnow() - timedelta(hours=12)
    cands = []
    for d in db.new_strategy_signals.find(
            {"created_at": {"$gte": since},
             "direction": {"$in": ["LONG", "SHORT"]}},
            {"symbol": 1, "pair": 1, "direction": 1, "strategy": 1,
             "validator_ok": 1, "mso_streak2h": 1, "created_at": 1}):
        cands.append({"sym": d.get("symbol") or (d.get("pair") or "").replace("/", ""),
                      "src": d.get("strategy") or "?", "dir": d["direction"],
                      "val": d.get("validator_ok"), "ms": d.get("mso_streak2h")})
    for d in db.supertrend_signals.find(
            {"created_at": {"$gte": since},
             "direction": {"$in": ["LONG", "SHORT"]}},
            {"pair_norm": 1, "pair": 1, "direction": 1, "tier": 1,
             "validator_ok": 1, "mso_streak2h": 1}):
        cands.append({"sym": d.get("pair_norm") or (d.get("pair") or "").replace("/", ""),
                      "src": "supertrend_" + (d.get("tier") or "?"),
                      "dir": d["direction"], "val": d.get("validator_ok"),
                      "ms": d.get("mso_streak2h")})
    top = []
    seen = set()
    for c in cands:
        status, rule = le.score_signal(model, c["src"], c["dir"],
                                       c["val"], c["ms"])
        if status != "ACTIVE_SHOW" or not rule:
            continue
        k = c["sym"] + c["dir"]
        if k in seen:
            continue
        seen.add(k)
        top.append((rule.get("ev") or 0, c, rule))
    top.sort(key=lambda x: -x[0])
    lines = [
        f"🎓 <b>Академия · утренний дайджест</b>",
        f"Рынок: широта 4h <b>{br}%</b> — {mode}" if br is not None else "",
        f"Модель v{model.get('version')} · правил SHOW "
        f"{model.get('n_show')} / HIDE {model.get('n_hide')}"
        + (f" · живых исходов в обучении: {model.get('live_n')}"
           if model.get("live_n") else ""),
    ]
    if top:
        lines.append("\n<b>Топ одобренных за 12ч:</b>")
        for ev, c, rule in top[:5]:
            de = "🟢 LONG" if c["dir"] == "LONG" else "🔴 SHORT"
            lines.append(f"· <b>{c['sym'].replace('USDT', '')}</b> {de} "
                         f"· EV {ev:+.2f} (WR {rule.get('wr')}, n={rule.get('n')})")
    else:
        lines.append("\nЗа 12ч одобренных сигналов не было.")
    try:
        import learn_paper
        ps = learn_paper.stats(db)
        if ps.get("closed"):
            lines.append(f"\n📜 Paper: {ps['closed']} закрыто · WR {ps.get('wr')} "
                         f"· avgR {ps.get('avg'):+.2f} · открыто {ps.get('open')}"
                         + (" · ⚠️ ранние закрытия смещены к SL, судить после первых таймаутов 96ч" if ps["closed"] < 100 else ""))
        elif ps.get("open"):
            lines.append(f"\n📜 Paper: открыто {ps['open']}, закрытий ещё нет")
    except Exception:
        pass
    try:
        thr = db.system_config.find_one({"_id": "live_throttle"}) or {}
        if thr.get("level"):
            lvl = {1: "🟡 ОСТОРОЖНО (кап 3)", 2: "🔴 СТОП (кап 0)"}.get(
                thr["level"], "?")
            lines.append(f"\n🛑 Режимный тормоз лайва: {lvl} — "
                         f"{thr.get('reason')}")
    except Exception:
        pass
    deg = model.get("degraded") or []
    if deg:
        lines.append(f"\n⚠️ Деградировали и разжалованы: {len(deg)} правил")
    if model.get("lgbm_ready"):
        lines.append("\n🤖 LightGBM 4 недели подряд обгоняет таблицу — "
                     "готов к промоушену, реши вопрос")
    brief = model.get("brief")
    if brief:
        lines.append(f"\n<i>{brief[:600]}</i>")
    return "\n".join(x for x in lines if x)


def send(text):
    if not text:
        return False
    try:
        import requests
        from new_strategies import _bot13_token_sync
        from config import NEW_STRATEGY_CHAT_ID
        tok = _bot13_token_sync()
        if not tok:
            return False
        r = requests.post(
            f"https://api.telegram.org/bot{tok}/sendMessage",
            json={"chat_id": NEW_STRATEGY_CHAT_ID, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=15)
        return r.status_code == 200
    except Exception:
        logger.warning("[digest] send fail", exc_info=True)
        return False


def maybe_send():
    """Раз в день в 08:xx UTC (sync; в to_thread)."""
    from database import _get_db, utcnow
    db = _get_db()
    now = utcnow()
    if now.hour != 8:
        return False
    today = now.strftime("%Y-%m-%d")
    mark = db.system_config.find_one({"_id": "academy_digest_sent"}) or {}
    if mark.get("date") == today:
        return False
    text = build_text(db)
    ok = send(text)
    if ok:
        db.system_config.update_one(
            {"_id": "academy_digest_sent"},
            {"$set": {"date": today, "at": now.isoformat()}}, upsert=True)
        logger.info("[digest] утренний дайджест отправлен")
    return ok
