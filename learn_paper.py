# -*- coding: utf-8 -*-
"""📜 Академия: бумажный исполнитель (18.09.26).

Каждый одобренный моделью сигнал автоматически становится paper-сделкой
(вход по последнему 1h-закрытию, канон TP+10/SL−5/96ч). Закрытия
проверяются тем же циклом. Через 2-3 недели у Академии будут ЖИВЫЕ WR
вместо бэктестных — честная проверка модели войной. Реальных денег и
журнала не касается, всё в коллекции academy_paper."""
import logging
from datetime import timedelta

logger = logging.getLogger(__name__)

SIG_MAX_AGE_H = 2      # открываем только по свежим сигналам
OPEN_BATCH = 12        # новых сделок за цикл
CLOSE_BATCH = 150       # проверок закрытия за цикл (свечи 1 раз/монету)


def _last_close(pair):
    from exchange import get_klines_any
    c = get_klines_any(pair, "1h", 3)
    return float(c[-1]["c"]) if c else None


def _open_new(db, model, now):
    import learn_engine as le
    since = now - timedelta(hours=SIG_MAX_AGE_H)
    cands = []
    for d in db.new_strategy_signals.find(
            {"created_at": {"$gte": since},
             "direction": {"$in": ["LONG", "SHORT"]}},
            {"pair": 1, "symbol": 1, "direction": 1, "strategy": 1,
             "created_at": 1, "validator_ok": 1, "mso_streak2h": 1}):
        cands.append(("ns_" + str(d["_id"]), d.get("pair"), {
            "sym": d.get("symbol") or (d.get("pair") or "").replace("/", ""),
            "src": d.get("strategy") or "?", "dir": d["direction"],
            "val": d.get("validator_ok"), "ms": d.get("mso_streak2h"),
            "at": d["created_at"]}))
    for d in db.supertrend_signals.find(
            {"created_at": {"$gte": since},
             "direction": {"$in": ["LONG", "SHORT"]}},
            {"pair": 1, "pair_norm": 1, "direction": 1, "tier": 1,
             "created_at": 1, "validator_ok": 1, "mso_streak2h": 1}):
        cands.append(("st_" + str(d["_id"]), d.get("pair"), {
            "sym": d.get("pair_norm") or (d.get("pair") or "").replace("/", ""),
            "src": "supertrend_" + (d.get("tier") or "?"),
            "dir": d["direction"], "val": d.get("validator_ok"),
            "ms": d.get("mso_streak2h"), "at": d["created_at"]}))
    opened = 0
    for key, pair, c in sorted(cands, key=lambda x: x[2]["at"], reverse=True):
        if opened >= OPEN_BATCH:
            break
        status, rule = le.score_signal(model, c["src"], c["dir"],
                                       c["val"], c["ms"])
        if status != "ACTIVE_SHOW":
            continue
        if db.academy_paper.find_one({"_id": key}, {"_id": 1}):
            continue
        px = _last_close(pair or (c["sym"][:-4] + "/USDT"))
        if not px:
            continue
        ms = c["ms"]
        bucket = ("gold" if c["val"] is True and c["dir"] == "LONG"
                  and (ms is None or ms < 27)
                  else "heat" if c["dir"] == "SHORT" and ms is not None
                  and ms >= 27 else None)
        db.academy_paper.update_one({"_id": key}, {"$set": {
            "sym": c["sym"], "pair": pair, "dir": c["dir"], "src": c["src"],
            "rule": rule.get("label") if rule else None,
            "rule_id": rule.get("id") if rule else None,
            "ev": rule.get("ev") if rule else None,
            "size": le.size_tier(rule), "bucket": bucket,
            "entry": px, "state": "OPEN",
            "sig_at": c["at"], "opened_at": now,
        }}, upsert=True)
        opened += 1
    return opened


def _close_open(db, now):
    """Закрытие созревших. 🔁 Ротация по chk (время последней проверки):
    давно не проверенные первыми — иначе старые вечно-открытые (ждут
    таймаута 96ч) замораживали окно и до свежих сделок проход не доходил
    (17.09: 0 закрытий с rule_id → live_map пуст). Свечи тянем один раз
    на монету, не на сделку."""
    from exchange import get_klines_any
    closed = 0
    batch = list(db.academy_paper.find({"state": "OPEN"})
                 .sort("chk", 1).limit(CLOSE_BATCH))
    by_pair = {}
    for t in batch:
        by_pair.setdefault(t.get("pair") or (t["sym"][:-4] + "/USDT"),
                           []).append(t)
    now_ms = int(now.timestamp() * 1000)
    for pair, trades in by_pair.items():
        c1 = None
        try:
            oldest = min(t["opened_at"] for t in trades)
            need = int(min((now - oldest).total_seconds() / 3600, 100)) + 3
            c1 = get_klines_any(pair, "1h", max(need, 5))
        except Exception:
            logger.debug(f"[paper] klines fail {pair}", exc_info=True)
        for t in trades:
            try:
                db.academy_paper.update_one(
                    {"_id": t["_id"]}, {"$set": {"chk": now}})
                if not c1:
                    continue
                age_h = (now - t["opened_at"]).total_seconds() / 3600
                o_ms = int(t["opened_at"].timestamp() * 1000)
                entry = float(t["entry"])
                sg = 1 if t["dir"] == "LONG" else -1
                tp = entry * (1 + sg * 0.10)
                sl = entry * (1 - sg * 0.05)
                res = None
                for b in c1:
                    if b["t"] <= o_ms or b["t"] + 3_600_000 > now_ms:
                        continue   # только полные закрытые бары после входа
                    if (b["l"] <= sl) if sg > 0 else (b["h"] >= sl):
                        res = ("SL", -5.0 - 0.1)
                        break
                    if (b["h"] >= tp) if sg > 0 else (b["l"] <= tp):
                        res = ("TP", 10.0 - 0.1)
                        break
                if res is None and age_h >= 96:
                    res = ("TIMEOUT",
                           (float(c1[-1]["c"]) / entry - 1) * 100 * sg - 0.1)
                if res:
                    db.academy_paper.update_one(
                        {"_id": t["_id"]},
                        {"$set": {"state": res[0], "r": round(res[1], 2),
                                  "closed_at": now}})
                    closed += 1
            except Exception:
                logger.debug(f"[paper] close fail {t.get('sym')}",
                             exc_info=True)
    return closed


def run_cycle():
    """Открыть новые + закрыть созревшие (sync; в to_thread)."""
    from database import _get_db, utcnow
    db = _get_db()
    model = db.learn_model.find_one({"_id": "active"})
    if not model:
        return 0, 0
    now = utcnow()
    opened = _open_new(db, model, now)
    closed = _close_open(db, now)
    if opened or closed:
        logger.info(f"[paper] открыто {opened} · закрыто {closed}")
    return opened, closed


def lists(db):
    """Открытые и свежезакрытые paper-сделки для вкладки."""
    op = []
    for d in db.academy_paper.find({"state": "OPEN"}).sort(
            "opened_at", -1).limit(60):
        op.append({"key": str(d["_id"]),
                   "sym": d["sym"], "dir": d["dir"], "src": d.get("src"),
                   "entry": d.get("entry"), "size": d.get("size"),
                   "rule": d.get("rule"),
                   "at": d["opened_at"].isoformat()})
    cl = []
    for d in db.academy_paper.find(
            {"state": {"$in": ["TP", "SL", "TIMEOUT"]}}).sort(
            "closed_at", -1).limit(20):
        cl.append({"key": str(d["_id"]),
                   "sym": d["sym"], "dir": d["dir"], "state": d["state"],
                   "r": d.get("r"), "at": (d.get("closed_at")
                                           or d["opened_at"]).isoformat()})
    return op, cl


def stats(db):
    """Сводка для API: общая + по состояниям/направлениям/за 24ч."""
    from database import utcnow
    out = {"open": db.academy_paper.count_documents({"state": "OPEN"})}
    cl = list(db.academy_paper.find(
        {"state": {"$in": ["TP", "SL", "TIMEOUT"]}},
        {"r": 1, "state": 1, "dir": 1, "opened_at": 1, "closed_at": 1}))
    out["closed"] = len(cl)
    if not cl:
        return out

    def _st(sel):
        a = [d["r"] for d in sel if d.get("r") is not None]
        if not a:
            return None
        return {"n": len(a),
                "wr": round(sum(1 for x in a if x > 0) / len(a) * 100, 1),
                "avg": round(sum(a) / len(a), 2), "sum": round(sum(a), 1)}
    tot = _st(cl) or {}
    out.update({"wr": tot.get("wr"), "avg": tot.get("avg"),
                "sum": tot.get("sum")})
    out["tp"] = sum(1 for d in cl if d["state"] == "TP")
    out["sl"] = sum(1 for d in cl if d["state"] == "SL")
    out["to"] = sum(1 for d in cl if d["state"] == "TIMEOUT")
    out["long"] = _st([d for d in cl if d.get("dir") == "LONG"])
    out["short"] = _st([d for d in cl if d.get("dir") == "SHORT"])
    from datetime import timedelta
    cut = utcnow() - timedelta(hours=24)
    out["s24"] = _st([d for d in cl
                      if d.get("closed_at") and d["closed_at"] >= cut])
    durs = sorted((d["closed_at"] - d["opened_at"]).total_seconds() / 3600
                  for d in cl if d.get("closed_at") and d.get("opened_at"))
    if durs:
        out["med_h"] = round(durs[len(durs) // 2])
    return out
