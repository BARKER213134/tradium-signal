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
    for d in db.academy_signals.find(
            {"created_at": {"$gte": since},
             "direction": {"$in": ["LONG", "SHORT"]}},
            {"symbol": 1, "pair": 1, "direction": 1, "strategy": 1,
             "validator_ok": 1, "mso_streak2h": 1, "created_at": 1}):
        cands.append({"sym": d.get("symbol") or (d.get("pair") or "").replace("/", ""),
                      "src": d.get("strategy") or "?", "dir": d["direction"],
                      "val": d.get("validator_ok"), "ms": d.get("mso_streak2h")})
    top = []
    seen = set()
    _rg = le.btc_regime_now()
    for c in cands:
        status, rule = le.score_signal(model, c["src"], c["dir"],
                                       c["val"], c["ms"], rg=_rg[1])
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
    if _rg[1]:
        _rm = model.get("regime_meta") or {}
        lines.append(f"₿ Режим: {le.REGIME_LABEL.get(_rg[1], _rg[1])} "
                     f"({_rg[0]:+.1f}% от 30д-макс)"
                     + (" — шорты допущены в live-срез" if _rg[1] == "corr" else "")
                     + (f" · память режима {_rm.get('days')}д: "
                        f"{len(model.get('regime_rules') or [])} клеток"
                        if _rm else ""))
    _if = (model.get("inflight") or {}).get("all") or {}
    if _if.get("n"):
        _ip = (model.get("inflight") or {}).get("port") or {}
        lines.append(
            f"✈ В полёте {_if['n']} сигналов: тек. {_if['avg']:+.2f}"
            + (f" · корзины {_ip['n']}: {_ip['avg']:+.2f}"
               if _ip.get("n") else "")
            + " (не в статистике — пульс режима)")
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
        if thr.get("level") or thr.get("cap") == 0:
            _cap = thr.get("cap")
            lvl = ("🔴 СТОП (кап 0)" if _cap == 0 else
                   {1: f"🟡 ОСТОРОЖНО (кап {_cap})",
                    2: f"🟠 ЭЙФОРИЯ (кап {_cap})"}.get(thr.get("level"), "?"))
            lines.append(f"\n🛑 Режимный тормоз лайва: {lvl} — "
                         f"{thr.get('reason')}")
            # 💎 кап лайва и 🧪 тень (22.09)
            import learn_paper as _lp2
            _st2 = _lp2.stats(db)
            _l2 = _st2.get("live2") or {}
            lines.append(
                f"💎 Кап лайва {thr.get('cap')}/день (сегодня "
                f"{_st2.get('live_today', 0)}, открыто {_st2.get('live_open', 0)})"
                + (f" · 🧪 тень (×2+×1, кап {thr.get('cap2')}): n={_l2.get('n')} "
                   f"WR {_l2.get('wr')}% avg {_l2.get('avg_adj'):+.2f} "
                   f"(открыто {_st2.get('live2_open', 0)})"
                   if _l2.get("n") and _l2.get("avg_adj") is not None
                   else f" · 🧪 тень: копим (кап {thr.get('cap2')})"))
    except Exception:
        pass
    # 🎯 цель $20k/мес: подтверждённый темп по закрытым live
    try:
        import learn_paper as _lp3
        lcl = list(db.academy_paper.find(
            {"live": True, "state": {"$in": ["TP", "SL", "TIMEOUT"]}},
            {"r": 1, "fund_cost": 1, "opened_at": 1}))
        if len(lcl) >= 3:
            net = sum(d["r"] - _lp3.LIVE_FEE_EXTRA - (d.get("fund_cost") or 0)
                      for d in lcl if d.get("r") is not None)
            days = max(1.0, (utcnow() - min(
                d["opened_at"] for d in lcl)).total_seconds() / 86400)
            mo = ((1 + net / days * 0.10 / 100) ** 30 - 1) * 100
            need = round(20000 / (mo / 100)) if mo > 1 else None
            import math as _mm
            m1 = round(1000 * mo / 100)
            mt = None
            dly = net / days * 0.10
            if need and dly > 0.05:
                mt = round(_mm.log(need / 1000)
                           / _mm.log(1 + dly / 100) / 30.4, 1)
            lines.append(
                f"\n🎯 Темп {mo:+.1f}%/мес — с $1000 это ~${m1}/мес"
                + (f"; компаундом до $20k/мес ~{mt} мес" if mt else "")
                + f" ({len(lcl)} закрытий, окно {days:.0f}д)"
                + (" · ⚠️ данных мало" if days < 14 or len(lcl) < 60 else ""))
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
