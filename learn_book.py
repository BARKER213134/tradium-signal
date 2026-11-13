"""⚖ Книга без ставки на рынок (13.11.26, юзер: «делай все три пункта»).

Ревизия 112k сигналов (30.03→31.10): результат сигналов по месяцам почти
целиком = направление рынка (лонги в плюсе в растущие месяцы, шорты — в
падающие). Переносится между фазами только АЛЬФА источника — его средний R
минус средний R всех сигналов того же направления за тот же месяц. Отбор
по первой половине подтвердился на второй у 17 источников из 20, а книга
«хорошие лонги + хорошие шорты одновременно» была в плюсе каждый месяц
второй половины (+1.13 / +1.18 / +1.84 при +0.35..+0.48 у всех сигналов).

Здесь эта книга торгуется вперёд на бумаге:
  • берёт сигналы хороших источников НЕЗАВИСИМО от вердикта школы;
  • обе стороны сразу, капы на КАЖДОЕ направление как у 💎
    (10/день, 3 в 2ч-слот, 1 монета/день, 15 позиций), монета на BingX;
  • исход по канону: TP +10% / SL −5% / 96ч, комиссия −0.1%;
  • своя коллекция academy_book — в школу, лайв, тормоз и исполнитель
    не входит и их статистику не трогает.
Списки зафиксированы отбором по первой половине, руками не подгонять:
пересмотр — только новым прогоном sig_alpha.py.
"""
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

BOOK_ENABLED = True
BOOK_LONG = {"whale", "volume_surge", "volcano", "supertrend_mtf", "ten", "combo",
             "rsi_cross_12h", "verified_go",       # 13.11, аудит
             "range_bottom",                       # 13.11, годовой поиск входов
             "vol_climax",                         # 13.11, поиск «80%»: +5/−15/30д
             "flush_climax"}                       # 13.11, 🚿 смыв: +5/−15/14д
BOOK_SHORT = {"shark", "volume_surge", "triple_confluence", "supertrend_daily",
              "supertrend_vip", "second_flip",
              "rsi_cross_12h", "verified_go",
              "low_break"}                         # 13.11, годовой поиск входов
# Свой план сделки у новых источников (new_setups.PLAN): +10% / −15% / 14 дней —
# их преимущество найдено именно с широким стопом и длинным горизонтом.
# low_break — шорт у 30-дневного минимума — берём и при рынке глубоко внизу:
# это отдельная гипотеза, иначе трека по ней не накопить.
SHORT_BLOCK_EXEMPT = {"low_break"}


def _plan(src):
    try:
        import new_setups as _ns
        p = _ns.PLAN.get(src)
        if p:
            return p["tp"], p["sl"], p["hz"]
    except Exception:
        pass
    return TP_PCT, SL_PCT, TIMEOUT_H
DAY_CAP = 10       # входов в день на направление
SLOT_H = 2
SLOT_CAP = 3       # входов в 2ч-слот на направление
CONC_CAP = 15      # открытых позиций на направление
TP_PCT = 0.10
SL_PCT = 0.05
TIMEOUT_H = 96
CLOSE_BATCH = 80


def _open(db, now):
    import learn_paper as lp
    cands = lp.collect_candidates(db, now - timedelta(hours=lp.SIG_MAX_AGE_H))
    if not cands:
        return 0
    bx = lp.bingx_set(db)
    day0 = datetime(now.year, now.month, now.day)
    slot0 = datetime(now.year, now.month, now.day, (now.hour // SLOT_H) * SLOT_H)
    cnt = {}
    for d in ("LONG", "SHORT"):
        cnt[d] = [db.academy_book.count_documents({"dir": d, "opened_at": {"$gte": day0}}),
                  db.academy_book.count_documents({"dir": d, "opened_at": {"$gte": slot0}}),
                  db.academy_book.count_documents({"dir": d, "state": "OPEN"})]
    opened = 0
    # 🚫 13.11: шорты при рынке глубоко внизу книга не берёт (config.MB_SHORT_BLOCK)
    try:
        import structure_engine as _sen
        _no_short = _sen.shorts_blocked(db)
    except Exception:
        _no_short = False
    for key, pair, c in sorted(cands, key=lambda x: x[2]["at"]):
        d, src, sym = c["dir"], c["src"], c["sym"]
        if src not in (BOOK_LONG if d == "LONG" else BOOK_SHORT) or sym not in bx:
            continue
        if d == "SHORT" and _no_short and src not in SHORT_BLOCK_EXEMPT:
            continue
        n_day, n_slot, n_open = cnt[d]
        if n_day >= DAY_CAP or n_slot >= SLOT_CAP or n_open >= CONC_CAP:
            continue
        if db.academy_book.find_one({"_id": key}, {"_id": 1}):
            continue
        if db.academy_book.find_one({"sym": sym, "dir": d, "opened_at": {"$gte": day0}},
                                    {"_id": 1}):
            continue
        px = lp._last_close(pair or (sym[:-4] + "/USDT"))
        if not px:
            continue
        _tpp, _slp, _hz = _plan(src)
        db.academy_book.insert_one({
            "_id": key, "sym": sym, "pair": pair, "dir": d, "src": src,
            "entry": px, "state": "OPEN", "sig_at": c["at"], "opened_at": now,
            "tp_pct": _tpp, "sl_pct": _slp, "hz_h": _hz})
        cnt[d][0] += 1
        cnt[d][1] += 1
        cnt[d][2] += 1
        opened += 1
    return opened


def _close(db, now):
    """Канон школы: только полные закрытые 1h-бары после входа; стоп раньше
    тейка в одном баре; 96ч — выход по закрытию; нет свечей 120ч — по входу."""
    from exchange import get_klines_any
    batch = list(db.academy_book.find({"state": "OPEN"}).sort("chk", 1).limit(CLOSE_BATCH))
    by_pair = {}
    for t in batch:
        by_pair.setdefault(t.get("pair") or (t["sym"][:-4] + "/USDT"), []).append(t)
    now_ms = int(now.timestamp() * 1000)
    closed = 0
    for pair, trades in by_pair.items():
        c1 = None
        try:
            oldest = min(t["opened_at"] for t in trades)
            _hmax = max((t.get("hz_h") or TIMEOUT_H) for t in trades) + 4
            need = int(min((now - oldest).total_seconds() / 3600, _hmax)) + 3
            c1 = get_klines_any(pair, "1h", max(need, 5))
        except Exception:
            logger.debug(f"[book] klines fail {pair}", exc_info=True)
        for t in trades:
            try:
                db.academy_book.update_one({"_id": t["_id"]}, {"$set": {"chk": now}})
                age_h = (now - t["opened_at"]).total_seconds() / 3600
                if not c1:
                    if age_h >= (t.get("hz_h") or TIMEOUT_H) + 24:
                        db.academy_book.update_one(
                            {"_id": t["_id"]},
                            {"$set": {"state": "TIMEOUT", "r": -0.1, "closed_at": now,
                                      "no_candles": True}})
                        closed += 1
                    continue
                o_ms = int(t["opened_at"].timestamp() * 1000)
                entry = float(t["entry"])
                sg = 1 if t["dir"] == "LONG" else -1
                _tpp = t.get("tp_pct") or TP_PCT
                _slp = t.get("sl_pct") or SL_PCT
                _hz = t.get("hz_h") or TIMEOUT_H
                tp = entry * (1 + sg * _tpp)
                sl = entry * (1 - sg * _slp)
                res = None
                for b in c1:
                    if b["t"] <= o_ms or b["t"] + 3_600_000 > now_ms:
                        continue
                    if (b["l"] <= sl) if sg > 0 else (b["h"] >= sl):
                        res = ("SL", -_slp * 100 - 0.1)
                        break
                    if (b["h"] >= tp) if sg > 0 else (b["l"] <= tp):
                        res = ("TP", _tpp * 100 - 0.1)
                        break
                if res is None and age_h >= _hz:
                    res = ("TIMEOUT", (float(c1[-1]["c"]) / entry - 1) * 100 * sg - 0.1)
                if res:
                    db.academy_book.update_one(
                        {"_id": t["_id"]},
                        {"$set": {"state": res[0], "r": round(res[1], 2), "closed_at": now}})
                    closed += 1
            except Exception:
                logger.debug(f"[book] close fail {t.get('sym')}", exc_info=True)
    return closed


def run(db, now):
    """Один проход: открыть новые + закрыть созревшие (sync; из цикла школы)."""
    if not BOOK_ENABLED:
        return 0, 0
    opened = _open(db, now)
    closed = _close(db, now)
    if opened or closed:
        logger.info(f"[book] ⚖ открыто {opened} · закрыто {closed}")
    return opened, closed


def stats(db):
    """Сводка книги для /api/live и проверок."""
    def _st(v):
        if not v:
            return None
        return {"n": len(v), "wr": round(sum(1 for x in v if x > 0) / len(v) * 100, 1),
                "avg": round(sum(v) / len(v), 2), "sum": round(sum(v), 1)}
    out = {}
    try:
        cl = list(db.academy_book.find(
            {"state": {"$in": ["TP", "SL", "TIMEOUT"]}, "r": {"$ne": None}},
            {"r": 1, "dir": 1, "closed_at": 1}))
        lo = [d["r"] for d in cl if d["dir"] == "LONG"]
        sh = [d["r"] for d in cl if d["dir"] == "SHORT"]
        out["long"], out["short"], out["all"] = _st(lo), _st(sh), _st(lo + sh)
        days = {}
        for d in cl:
            k = (d.get("closed_at") or datetime(1970, 1, 1)).strftime("%m-%d")
            days[k] = days.get(k, 0.0) + d["r"]
        out["days"] = len(days)
        out["days_pos"] = sum(1 for v in days.values() if v > 0)
        out["open_long"] = db.academy_book.count_documents({"dir": "LONG", "state": "OPEN"})
        out["open_short"] = db.academy_book.count_documents({"dir": "SHORT", "state": "OPEN"})
        first = db.academy_book.find_one(sort=[("opened_at", 1)])
        out["since"] = first["opened_at"].isoformat() if first else None
    except Exception:
        logger.debug("[book] stats fail", exc_info=True)
    return out
