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
PROBE_DAY_CAP = 2      # 🔬 разведка: paper-проб/день на живо-выключенное правило
LIVE_DAY_CAP = 10      # 💎 live-tier: входов в день
LIVE_CONC_CAP = 15     # 💎 одновременных позиций
LIVE2_DAY_CAP = 10     # 🧪 тень (21.09): ×2+×1, ≤1 сделки/2ч-слот, ≤1 монеты/день
LIVE2_SLOT_H = 2
LIVE_FEE_EXTRA = 0.1   # 💎 доп. штраф лайва к R (%): проскальзывание BingX
                       # (канонная сетка уже вычитает 0.1 комиссии)

_BINGX_CACHE = {"t": 0.0, "set": set()}


def _funding_cost(sym, t_open, t_close, direction):
    """💸 Фактический фандинг за окно удержания (%): сумма 8ч-ставок
    fapi между открытием и закрытием. LONG платит положительные ставки.
    fail-open → None (локально fapi 451)."""
    try:
        import requests
        r = requests.get(
            "https://fapi.binance.com/fapi/v1/fundingRate",
            params={"symbol": sym,
                    "startTime": int(t_open.timestamp() * 1000),
                    "endTime": int(t_close.timestamp() * 1000),
                    "limit": 100},
            timeout=10)
        if r.status_code != 200:
            return None
        rates = [float(x.get("fundingRate") or 0) for x in r.json()]
        if not rates:
            return 0.0
        sg = 1 if direction == "LONG" else -1
        return round(sum(rates) * sg * 100, 4)   # + = заплатили
    except Exception:
        return None


def live_throttle(db):
    """🛑 Режимный тормоз live-среза (18.09): сжимает дневной кап при
    перегреве рынка (широта 4h) или просадке скользящего WR последних
    20 live-закрытий. Уровни: 0=норма (кап 10) / 1=осторожно (3) /
    2=стоп (0). Обучение НЕ трогает — paper торгует всё как торговал.
    Состояние → system_config.live_throttle (вкладка/дайджест/AI)."""
    from database import utcnow
    breadth = None
    try:
        rows = (db.market_state.find_one({"_id": "trend_matrix"})
                or {}).get("rows") or []
        n4 = [r for r in rows if (r.get("d") or {}).get("4h")]
        if len(n4) >= 50:
            breadth = round(sum(1 for r in n4 if r["d"]["4h"] > 0)
                            / len(n4) * 100)
    except Exception:
        pass
    wr20 = None
    try:
        last = [d.get("r") for d in db.academy_paper.find(
            {"live": True, "state": {"$in": ["TP", "SL", "TIMEOUT"]}},
            {"r": 1}).sort("closed_at", -1).limit(20)]
        last = [r for r in last if r is not None]
        if len(last) >= 10:
            wr20 = round(sum(1 for r in last if r > 0) / len(last) * 100)
    except Exception:
        pass
    level = 0
    reasons = []
    if breadth is not None and breadth > 60:
        level = 2
        reasons.append(f"широта {breadth}% > 60 — эйфория (тормоз по широте снят 22.09)")
    elif breadth is not None and breadth > 50:
        level = max(level, 1)
        reasons.append(f"широта {breadth}% > 50 — режим против контрарианских лонгов")
    hard_stop = False
    if wr20 is not None and wr20 < 30:
        level = 2
        hard_stop = True
        reasons.append(f"скользящий WR live {wr20}% < 30 — система в просадке, СТОП")
    elif wr20 is not None and wr20 < 45:
        level = max(level, 1)
        reasons.append(f"скользящий WR live {wr20}% < 45")
    # 22.09 (юзер: «рост и широта могут быть месяцами — не открываем сделок»):
    # широта >60 больше НЕ обнуляет лайв — школа при широте >80 давала
    # лонгам +3.8 (n=687), 60-80 +2.1; в бычьем месяце тормоз был закрыт
    # ~70% времени. Уровни 1-2 → кап 5; ноль — только живая просадка
    # (WR20<30) или школа за сутки в минусе по одобренным лонгам (ниже)
    # 22.09 бэктест (bt_throttle): для ×2 широта как тормоз не нужна —
    # без него ΣR выше в обоих окнах (45д +1274 vs +970 vs старый +526;
    # апр-июл +270 vs +104 vs −18). Кап 10 всегда; ноль — только по факту.
    cap = LIVE_DAY_CAP
    if hard_stop:
        cap = 0
    # ₿ режим BTC (20.09): шорты в live-срезе только в коррекции −8..−15%
    # (бэктест 20.05→08.07: шорты WR 65 +3.97 в этой зоне)
    dd, rg = None, None
    try:
        import learn_engine as _le
        dd, rg = _le.btc_regime_now()
        if rg:
            reasons.append(f"{_le.REGIME_LABEL[rg]} ({dd:+.1f}%)"
                           + (" — шорты в live-срезе ОТКРЫТЫ" if rg == "corr" else ""))
    except Exception:
        pass
    cap_short = LIVE_DAY_CAP if rg == "corr" else 0
    # 🧪 кап тени: уровень 0 → 10, 1-2 → 5; ноль — только если ШКОЛА за
    # сутки в минусе по одобренным лонгам (широта — не приговор: 18-21.09
    # при широте 90%+ школа давала лонгам WR 55 +3.2)
    school24 = None
    try:
        from datetime import timedelta as _td24
        _rs = [d.get("r") for d in db.academy_paper.find(
            {"dir": "LONG", "probe": {"$ne": True},
             "state": {"$in": ["TP", "SL", "TIMEOUT"]},
             "closed_at": {"$gte": utcnow() - _td24(hours=24)}}, {"r": 1})]
        _rs = [r for r in _rs if r is not None]
        if len(_rs) >= 20:
            school24 = {"n": len(_rs),
                        "wr": round(sum(1 for r in _rs if r > 0) / len(_rs) * 100),
                        "avg": round(sum(_rs) / len(_rs), 2)}
    except Exception:
        pass
    cap2 = {0: LIVE2_DAY_CAP, 1: 5, 2: 5}[level]
    if school24 and (school24["wr"] < 45 or school24["avg"] < 0):
        cap2 = 0
        cap = 0
        reasons.append(f"школа за сутки: WR {school24['wr']}% {school24['avg']:+.2f} — СТОП (лайв и тень)")
    st = {"level": level, "cap": cap, "cap_short": cap_short,
          "cap2": cap2, "school24": school24,
          "regime": rg, "btc_dd": dd,
          "reason": " · ".join(reasons) if reasons else "норма",
          "breadth": breadth, "wr20": wr20,
          "at": utcnow().isoformat()}
    try:
        prev = db.system_config.find_one({"_id": "live_throttle"}) or {}
        if "regime" in prev and prev.get("regime") != rg and rg:
            try:
                import learn_digest
                learn_digest.send(
                    f"₿ <b>Режим BTC сменился: {prev.get('regime')} → {rg}</b>\n"
                    f"{_le.REGIME_LABEL.get(rg, rg)} ({dd:+.1f}% от 30д-макс)"
                    + ("\nШорты ×2 допущены в live-срез" if rg == "corr"
                       else "\nШорты в live-срезе закрыты"))
            except Exception:
                logger.debug("[live] regime tg fail", exc_info=True)
        if prev.get("level") != level:
            logger.info(f"[live] 🛑 тормоз: уровень {prev.get('level')} → "
                        f"{level} (кап {cap}) — {st['reason']}")
            if "level" in prev:   # не спамить на первом создании дока
                try:
                    import learn_digest
                    ico = ("🔴 СТОП" if cap == 0 else
                           {0: "🟢 НОРМА", 1: "🟡 ОСТОРОЖНО",
                            2: "🟠 ЭЙФОРИЯ"}.get(level, "?"))
                    learn_digest.send(
                        f"🛑 <b>Режимный тормоз лайва: {ico}</b> "
                        f"(кап {cap}/день)\n{st['reason']}"
                        + (f"\nШирота 4h: {breadth}%" if breadth is not None else "")
                        + (f" · WR20: {wr20}%" if wr20 is not None else ""))
                except Exception:
                    logger.debug("[live] throttle tg fail", exc_info=True)
        db.system_config.update_one(
            {"_id": "live_throttle"}, {"$set": st}, upsert=True)
    except Exception:
        pass
    return st


def bingx_set(db):
    """Монеты BingX perpetual (system_config.bingx_universe, кэш 10 мин)."""
    import time as _t
    if _t.time() - _BINGX_CACHE["t"] > 600:
        try:
            d = db.system_config.find_one({"_id": "bingx_universe"}) or {}
            _BINGX_CACHE["set"] = set(d.get("symbols") or [])
        except Exception:
            pass
        _BINGX_CACHE["t"] = _t.time()
    return _BINGX_CACHE["set"]


def refresh_bingx(db):
    """Ночное обновление вселенной BingX (публичный API, fail-open)."""
    try:
        import requests
        from database import utcnow
        r = requests.get(
            "https://open-api.bingx.com/openApi/swap/v2/quote/contracts",
            timeout=20)
        if r.status_code != 200:
            return 0
        syms = sorted({(c.get("symbol") or "").replace("-USDT", "USDT")
                       for c in (r.json().get("data") or [])
                       if (c.get("symbol") or "").endswith("-USDT")})
        if len(syms) > 300:
            db.system_config.update_one(
                {"_id": "bingx_universe"},
                {"$set": {"symbols": syms, "n": len(syms),
                          "updated": utcnow().isoformat()}},
                upsert=True)
            _BINGX_CACHE["t"] = 0.0
            logger.info(f"[live] BingX universe: {len(syms)}")
        return len(syms)
    except Exception:
        logger.debug("[live] bingx refresh fail", exc_info=True)
        return 0
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
    # 🎓 academy_signals: выключенные для журнала стратегии (20.09)
    for d in db.academy_signals.find(
            {"created_at": {"$gte": since},
             "direction": {"$in": ["LONG", "SHORT"]}},
            {"pair": 1, "symbol": 1, "direction": 1, "strategy": 1,
             "created_at": 1, "validator_ok": 1, "mso_streak2h": 1}):
        cands.append(("as_" + str(d["_id"]), d.get("pair"), {
            "sym": d.get("symbol") or (d.get("pair") or "").replace("/", ""),
            "src": d.get("strategy") or "?", "dir": d["direction"],
            "val": d.get("validator_ok"), "ms": d.get("mso_streak2h"),
            "at": d["created_at"]}))
    thr = live_throttle(db)
    rg = thr.get("regime")
    opened = 0
    for key, pair, c in sorted(cands, key=lambda x: x[2]["at"], reverse=True):
        if opened >= OPEN_BATCH:
            break
        status, rule = le.score_signal(model, c["src"], c["dir"],
                                       c["val"], c["ms"], rg=rg)
        probe = False
        if status != "ACTIVE_SHOW":
            # 🔬 разведка боем: живо-выключенное правило продолжаем
            # пробовать малыми дозами (только школа-paper, НЕ лайв) —
            # иначе при смене режима оно никогда не реабилитируется
            if (status == "ACTIVE_HIDE" and rule
                    and rule.get("live_demoted") is not None):
                from datetime import datetime as _dtp
                d0 = _dtp(now.year, now.month, now.day)
                n_pr = db.academy_paper.count_documents(
                    {"probe": True, "rule_id": rule.get("id"),
                     "opened_at": {"$gte": d0}})
                if n_pr >= PROBE_DAY_CAP:
                    continue
                probe = True
            else:
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
        # 💎 live-tier: LONG × ×2 × BingX × капы дня/одновременных;
        # SHORT — только при ₿ коррекции (−8..−15%, cap_short>0; 20.09)
        live = False
        _is_long = c["dir"] == "LONG"
        _short_ok = c["dir"] == "SHORT" and (thr.get("cap_short") or 0) > 0
        try:
            if (not probe and (_is_long or _short_ok)
                    and le.size_tier(rule) == "2x"
                    and c["sym"] in bingx_set(db)):
                from datetime import datetime as _dt
                day0 = _dt(now.year, now.month, now.day)
                today_n = db.academy_paper.count_documents(
                    {"live": True, "opened_at": {"$gte": day0}})
                conc_n = db.academy_paper.count_documents(
                    {"live": True, "state": "OPEN"})
                _cap = thr["cap"] if _is_long else thr.get("cap_short", 0)
                live = today_n < _cap and conc_n < LIVE_CONC_CAP
        except Exception:
            pass
        # 🧪 ТЕНЬ live2 (21.09, по замечанию юзера «упускаем лонги»):
        # ×2 И ×1 · не больше 1 сделки в 2ч-слот и 1 монеты в день (первые
        # 10 по времени = один эпизод: WR 25 −1.45; разнесение: WR 54 +2.93)
        # · тормоз сжимает кап до 5, ноль — только если школа за сутки в
        # минусе. Ничего не торгует — помечает; сравнить с 💎 через неделю.
        live2 = False
        try:
            if (not probe and (_is_long or _short_ok)
                    and le.size_tier(rule) in ("2x", "1x")
                    and c["sym"] in bingx_set(db)):
                from datetime import datetime as _dt2
                day0 = _dt2(now.year, now.month, now.day)
                cap2 = (thr.get("cap2") or 0) if _is_long else (thr.get("cap_short") or 0)
                # слот = 24ч / кап (мин. 2ч): при капе 5 слоты по 4ч — иначе
                # кап кончался к 08 UTC, а лучшие часы лонгов 12-20 UTC
                # оставались пустыми (22.09)
                _slot_h = max(LIVE2_SLOT_H, 24 // max(1, cap2)) if cap2 else LIVE2_SLOT_H
                slot0 = _dt2(now.year, now.month, now.day,
                             (now.hour // _slot_h) * _slot_h)
                today2 = db.academy_paper.count_documents(
                    {"live2": True, "opened_at": {"$gte": day0}})
                slot_n = db.academy_paper.count_documents(
                    {"live2": True, "opened_at": {"$gte": slot0}})
                coin_n = db.academy_paper.count_documents(
                    {"live2": True, "sym": c["sym"], "opened_at": {"$gte": day0}})
                conc2 = db.academy_paper.count_documents(
                    {"live2": True, "state": "OPEN"})
                live2 = (today2 < cap2 and slot_n < 1 and coin_n < 1
                         and conc2 < LIVE_CONC_CAP)
        except Exception:
            pass
        db.academy_paper.update_one({"_id": key}, {"$set": {
            "live": live, "live2": live2, "probe": probe,
            "sym": c["sym"], "pair": pair, "dir": c["dir"], "src": c["src"],
            "rule": rule.get("label") if rule else None,
            "rule_id": rule.get("id") if rule else None,
            "ev": rule.get("ev") if rule else None,
            "size": le.size_tier(rule), "bucket": bucket,
            "entry": px, "state": "OPEN",
            "sig_at": c["at"], "opened_at": now,
            # 22.09: контекст входа для значка ⛰ «не у дна» на 💎
            "ms_open": c.get("ms"), "val_open": c.get("val"),
            "br_open": thr.get("breadth"),
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
                    # 22.09: делист/нет свечей — не висеть вечно (STEEMUSDT
                    # с 18.09): старше 120ч закрываем по входу как таймаут
                    _age = (now - t["opened_at"]).total_seconds() / 3600
                    if _age >= 120:
                        db.academy_paper.update_one(
                            {"_id": t["_id"]},
                            {"$set": {"state": "TIMEOUT", "r": -0.1,
                                      "closed_at": now, "no_candles": True}})
                        closed += 1
                        logger.info(f"[paper] {t.get('sym')}: нет свечей "
                                    f"{round(_age)}ч — закрыт по входу")
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
                    upd = {"state": res[0], "r": round(res[1], 2),
                           "closed_at": now}
                    if t.get("live") or t.get("live2"):
                        fc = _funding_cost(t["sym"], t["opened_at"],
                                           now, t["dir"])
                        if fc is not None:
                            upd["fund_cost"] = fc
                    db.academy_paper.update_one(
                        {"_id": t["_id"]}, {"$set": upd})
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
        op.append({"key": str(d["_id"]), "live": bool(d.get("live")),
                   "probe": bool(d.get("probe")),
                   "sym": d["sym"], "dir": d["dir"], "src": d.get("src"),
                   "entry": d.get("entry"), "size": d.get("size"),
                   "rule": d.get("rule"),
                   "at": d["opened_at"].isoformat()})
    cl = []
    for d in db.academy_paper.find(
            {"state": {"$in": ["TP", "SL", "TIMEOUT"]}}).sort(
            "closed_at", -1).limit(20):
        cl.append({"key": str(d["_id"]), "live": bool(d.get("live")),
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
    # 💎 live-tier срез (закрытые с флагом live) + поправка на исполнение
    lcl = list(db.academy_paper.find(
        {"live": True, "state": {"$in": ["TP", "SL", "TIMEOUT"]}},
        {"r": 1, "closed_at": 1, "opened_at": 1, "fund_cost": 1}))
    lv = _st(lcl)
    if lv:
        # честный adj: комиссия-экстра + ФАКТИЧЕСКИЙ фандинг сделки
        fcs = [d.get("fund_cost") for d in lcl]
        fund_avg = (round(sum(f for f in fcs if f is not None)
                          / max(1, sum(1 for f in fcs if f is not None)), 3)
                    if any(f is not None for f in fcs) else None)
        net = [d["r"] - LIVE_FEE_EXTRA - (d.get("fund_cost") or 0)
               for d in lcl if d.get("r") is not None]
        lv["fund_avg"] = fund_avg
        lv["avg_adj"] = round(sum(net) / len(net), 2) if net else None
        lv["sum_adj"] = round(sum(net), 1) if net else None
    out["live"] = lv
    out["live_open"] = db.academy_paper.count_documents(
        {"live": True, "state": "OPEN"})
    # 🧪 тень новой политики (live2)
    try:
        l2 = list(db.academy_paper.find(
            {"live2": True, "state": {"$in": ["TP", "SL", "TIMEOUT"]}},
            {"r": 1, "closed_at": 1, "opened_at": 1, "fund_cost": 1}))
        lv2 = _st(l2)
        if lv2:
            net2 = [d["r"] - LIVE_FEE_EXTRA - (d.get("fund_cost") or 0)
                    for d in l2 if d.get("r") is not None]
            lv2["avg_adj"] = round(sum(net2) / len(net2), 2) if net2 else None
            lv2["sum_adj"] = round(sum(net2), 1) if net2 else None
        out["live2"] = lv2
        out["live2_open"] = db.academy_paper.count_documents(
            {"live2": True, "state": "OPEN"})
        from datetime import datetime as _dt3
        _n2 = utcnow()
        out["live2_today"] = db.academy_paper.count_documents(
            {"live2": True, "opened_at": {"$gte": _dt3(_n2.year, _n2.month, _n2.day)}})
    except Exception:
        pass
    try:
        from datetime import datetime as _dt2
        _n = utcnow()
        day0 = _dt2(_n.year, _n.month, _n.day)
        out["live_today"] = db.academy_paper.count_documents(
            {"live": True, "opened_at": {"$gte": day0}})
    except Exception:
        pass
    durs = sorted((d["closed_at"] - d["opened_at"]).total_seconds() / 3600
                  for d in cl if d.get("closed_at") and d.get("opened_at"))
    if durs:
        out["med_h"] = round(durs[len(durs) // 2])
    return out
