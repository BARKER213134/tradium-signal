"""🛰 Cryptovizor: живая лента смен тренда → тень «Perfectly Fit short в первый день падения рынка».

18.11.26, юзер: «мы должны построить стратегию на Perfectly Fit от Cryptovizor… делай тень».

Что проверено на истории (лента Cryptovizor по Bybit за два года, 14.11.2024→17.11.2026,
100 236 реальных PF, исходы по 65 872 на 326 монетах):
  • PF в среднем не зарабатывает: long −0.39%, short +0.28% по канону, оба хуже случайного
    входа той же стороны за ту же неделю;
  • из ~350 условий, отобранных на одной половине, на другой в среднем остаётся ноль;
  • держится в ОБОИХ годах и на трёх выходах одно: PF short (раскладка 30m/1h/2h/4h/12h стала
    UDDDD) после резкого падения рынка за сутки, пока рынок ещё не на дне. Индекс рынка —
    «ракета» из долей монет с трендом вверх по ТФ (веса 1/1/2/2/4, −100…+100):
      упала на 31+ за 24ч и сама ≥ −45 → год 1: 753 сделки, +2.11% канон; год 2: 743, +1.24%;
      упала на 22+ → +1.46% / +0.64%, месяцев в плюсе 15 из 25, t по дням 1.9.
  Результат идёт комками, от «шортить что угодно в такой день» не отделён — поэтому ТЕНЬ:
  сигналы пишутся в academy_signals и торгуются на бумаге в книге ⚖ со своей квотой
  (learn_book.OWN_QUOTA). В лайв, тормоз и исполнитель не входят.

Доступ: официальный коннектор Cryptovizor (MCP, https://mcp.cvizor.com/mcp), OAuth-токен с правом
только на чтение рынка лежит в Mongo system_config {_id: "cv_oauth"} — в код и репозиторий не
попадает. Нагрузка: 2–4 запроса раз в 5 минут при лимите сервиса 60 в минуту.
"""
import asyncio
import json
import logging
import time
from datetime import datetime, timedelta

import requests

logger = logging.getLogger(__name__)

VER = 1
MCP_URL = "https://mcp.cvizor.com/mcp"
MARKET = "bybit_linear"
UA = {"User-Agent": "mcp-client/1.0"}
POLL_SEC = 300
TFS = ("30m", "1h", "2h", "4h", "12h")
W = (1, 1, 2, 2, 4)                  # веса ТФ в индексе рынка (как в бэктесте)
DROP_STRICT = 31                     # «ракета» упала за 24ч на столько пунктов и больше
DROP_SOFT = 22
RK_FLOOR = -45                       # …и сама ещё не ниже этого
SRC_STRICT = "cv_pf_drop"            # падение ≥ 31
SRC_SOFT = "cv_pf_drop22"            # падение 22…31 — тот же сигнал с мягким порогом
SOURCES = (SRC_STRICT, SRC_SOFT)
FRESH_MIN = 50                       # сигнал в книгу — только пока он свежий
DEDUP_H = 6                          # один сигнал источника на монету за 6ч
PF = {"UDDDD": "SHORT", "DUUUU": "LONG"}
_TASKS = []


def _utcnow():
    from database import utcnow
    return utcnow()


def _iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")


class _Client:
    """Минимальный MCP-клиент (streamable HTTP). Токен читает и обновляет в Mongo."""

    def __init__(self, db):
        self.db = db
        self.cfg = db.system_config.find_one({"_id": "cv_oauth"}) or {}
        self.sid = None
        self.n = 0

    def ok(self):
        return bool(self.cfg.get("refresh_token") and self.cfg.get("client_id"))

    def _refresh(self):
        r = requests.post(self.cfg["token_endpoint"], headers=UA, timeout=30, data={
            "grant_type": "refresh_token", "refresh_token": self.cfg["refresh_token"],
            "client_id": self.cfg["client_id"], "resource": MCP_URL})
        if r.status_code >= 300:
            raise RuntimeError(f"обновление токена: {r.status_code} {r.text[:160]}")
        tk = r.json()
        upd = {"access_token": tk["access_token"],
               "exp": time.time() + tk.get("expires_in", 300) - 45,
               "refreshed_at": _utcnow()}
        if tk.get("refresh_token"):
            upd["refresh_token"] = tk["refresh_token"]
        self.cfg.update(upd)
        self.db.system_config.update_one({"_id": "cv_oauth"}, {"$set": upd})

    def _rpc(self, method, params=None, notify=False):
        if time.time() > float(self.cfg.get("exp") or 0):
            self._refresh()
        for att in range(4):
            hd = dict(UA, **{"Authorization": "Bearer " + self.cfg["access_token"],
                             "Content-Type": "application/json",
                             "Accept": "application/json, text/event-stream"})
            if self.sid:
                hd["Mcp-Session-Id"] = self.sid
            body = {"jsonrpc": "2.0", "method": method}
            if params is not None:
                body["params"] = params
            if not notify:
                self.n += 1
                body["id"] = self.n
            try:
                r = requests.post(MCP_URL, headers=hd, json=body, timeout=40)
            except Exception:
                time.sleep(2 + att * 2)
                continue
            if r.status_code == 401:
                self._refresh()
                continue
            if r.status_code == 429:
                time.sleep(min(float(r.headers.get("Retry-After", 5)) + 1, 20))
                continue
            if r.status_code >= 500:
                time.sleep(3 + att * 3)
                continue
            if r.headers.get("Mcp-Session-Id"):
                self.sid = r.headers["Mcp-Session-Id"]
            if notify:
                return None
            if r.status_code >= 300:
                raise RuntimeError(f"{r.status_code} {r.text[:200]}")
            txt = r.text
            if "text/event-stream" in r.headers.get("Content-Type", ""):
                txt = [ln[5:].strip() for ln in txt.splitlines() if ln.startswith("data:")][-1]
            return json.loads(txt)
        raise RuntimeError("нет ответа коннектора")

    def start(self):
        self._rpc("initialize", {"protocolVersion": "2025-03-26", "capabilities": {},
                                 "clientInfo": {"name": "mcp-client", "version": "1.0"}})
        self._rpc("notifications/initialized", notify=True)

    def tool(self, name, args):
        for att in range(3):
            j = self._rpc("tools/call", {"name": name, "arguments": args})
            res = j.get("result") or {}
            txt = "".join(c.get("text", "") for c in res.get("content", [])
                          if c.get("type") == "text")
            if res.get("isError") or "error" in j:
                msg = (txt or json.dumps(j.get("error")))[:240]
                if any(k in msg for k in ("rate_limited", "dependency_unavailable")):
                    time.sleep(5 + att * 5)
                    continue
                raise RuntimeError(msg)
            return json.loads(txt) if txt.strip().startswith("{") else {}
        raise RuntimeError("лимит коннектора")


def index_from_percents(pc):
    """Индекс рынка −100…+100 из долей монет с трендом вверх по ТФ (как в бэктесте)."""
    s = 0.0
    ups = {}
    for tf, w in zip(TFS, W):
        x = pc.get(tf) or {}
        u, d = float(x.get("up") or 0), float(x.get("down") or 0)
        sh = u / (u + d) if (u + d) > 0 else 0.5
        ups[tf] = round(sh, 4)
        s += (2 * sh - 1) * w
    return round(s / sum(W) * 100, 1), ups


def _slot(dt):
    return dt.replace(minute=(dt.minute // 30) * 30, second=0, microsecond=0)


def _market_at(db, t):
    """Индекс рынка на получасовую отметку ≤ t (не старше 90 минут) → doc или None."""
    d = db.cv_market.find_one({"_id": {"$lte": _slot(t)}}, sort=[("_id", -1)])
    if d and (t - d["_id"]) <= timedelta(minutes=90):
        return d
    return None


def tier_for(rk, drop):
    if rk is None or drop is None or rk < RK_FLOOR:
        return None
    if drop >= DROP_STRICT:
        return SRC_STRICT
    if drop >= DROP_SOFT:
        return SRC_SOFT
    return None


def _base(sym):
    return sym[:-2] if sym.endswith(".P") else sym


def tick(db, now=None):
    """Один проход: снимок рынка раз в полчаса + новые строки ленты → PF → правило → сигнал."""
    now = now or _utcnow()
    c = _Client(db)
    if not c.ok():
        db.system_config.update_one({"_id": "cv_feed"}, {"$set": {
            "ver": VER, "last_tick": now, "err": "нет токена Cryptovizor (system_config.cv_oauth)"}},
            upsert=True)
        return {"ok": False}
    st = db.system_config.find_one({"_id": "cv_feed"}) or {}
    c.start()
    # ── 1. рынок: раз в полчаса ──
    slot = _slot(now)
    if not db.cv_market.find_one({"_id": slot}, {"_id": 1}):
        d = c.tool("trends", {"market": MARKET, "limit": 5})
        rk, ups = index_from_percents(d.get("percents") or {})
        real = (d.get("raketa") or {}).get("val")
        db.cv_market.update_one({"_id": slot}, {"$set": {
            "rk": rk, "rk_real": real, "up": ups, "n": d.get("matched"), "at": now}}, upsert=True)
    # ── 2. лента ──
    cur = st.get("cursor")                      # ISO последней обработанной отметки
    rows = (c.tool("trend_history", {"market": MARKET, "limit": 100}).get("rows") or [])
    allr = list(rows)
    floor = _iso((_parse(cur) if cur else now) - timedelta(minutes=60))
    pages = 0
    while rows and min(r[3] for r in rows) > floor and pages < 12:
        tmin = min(r[3] for r in rows)
        at = tmin
        if len(rows) >= 100 and max(r[3] for r in rows) == tmin:
            at = _iso(_parse(tmin) - timedelta(seconds=1))
        rows = (c.tool("trend_history", {"market": MARKET, "limit": 100, "at": at}).get("rows") or [])
        allr += rows
        pages += 1
    seen = set()
    n_pf = n_sig = 0
    newest = cur
    for sym, pat, ch_tf, at_s in sorted(allr, key=lambda r: r[3]):
        if at_s < floor or (sym, ch_tf, at_s) in seen:
            continue
        seen.add((sym, ch_tf, at_s))
        if newest is None or at_s > newest:
            newest = at_s
        dr = PF.get(pat)
        if not dr:
            continue
        t = _parse(at_s)
        # тот же ключ, что у выгруженной истории: монета ленты : время мс : сторона
        ms = int((t - datetime(1970, 1, 1)).total_seconds() * 1000)
        key = f"{sym}:{ms}:{1 if dr == 'LONG' else -1}"
        if db.cv_pf_signals.find_one({"_id": key}, {"_id": 1}):
            continue
        m0 = _market_at(db, t)
        m1 = _market_at(db, t - timedelta(hours=24))
        rk = m0.get("rk") if m0 else None
        drop = round(m1["rk"] - rk, 1) if (m0 and m1 and m1.get("rk") is not None) else None
        tier = tier_for(rk, drop) if dr == "SHORT" else None
        b = _base(sym)
        pair = (b[:-4] + "/USDT") if b.endswith("USDT") else b
        doc = {"symbol": sym, "pair": pair, "dir": dr, "pattern": pat, "changed_tf": ch_tf,
               "at": t, "market": MARKET, "source": "cryptovizor_trend_feed", "backfill": False,
               "seen_at": now, "rk": rk, "rk_24h_ago": (m1 or {}).get("rk"), "drop24": drop,
               "tier": tier}
        n_pf += 1
        if tier and (now - t) <= timedelta(minutes=FRESH_MIN) and b.endswith("USDT"):
            try:
                if not db.academy_signals.find_one(
                        {"strategy": {"$in": list(SOURCES)}, "symbol": b,
                         "created_at": {"$gte": now - timedelta(hours=DEDUP_H)}}, {"_id": 1}):
                    import learn_paper as lp
                    px = lp._last_close(pair)
                    if px:
                        db.academy_signals.insert_one({
                            "strategy": tier, "direction": "SHORT", "pair": pair, "symbol": b,
                            "entry": px, "tp": px * 0.90, "sl": px * 1.05, "horizon_h": 96,
                            "state": "WAITING", "setup": True,
                            "indicators": {"cv_pattern": pat, "cv_at": at_s, "rk": rk,
                                           "rk_drop24": drop, "cv_changed_tf": ch_tf},
                            "note": (f"🛰 PF short в день падения рынка: индекс {rk:+.0f}, "
                                     f"за 24ч −{drop:.0f} · тень, план +10% / −5% / 96ч"),
                            "created_at": now, "updated_at": now})
                        doc["signal"] = True
                        n_sig += 1
                    else:
                        doc["skip"] = "нет цены"
                else:
                    doc["skip"] = "повтор за 6ч"
            except Exception:
                logger.debug("[cv] signal fail", exc_info=True)
        try:
            db.cv_pf_signals.insert_one(dict(doc, _id=key))
        except Exception:
            pass
    m = _market_at(db, now)
    m24 = _market_at(db, now - timedelta(hours=24))
    rk_now = m.get("rk") if m else None
    drop_now = round(m24["rk"] - rk_now, 1) if (m and m24 and m24.get("rk") is not None) else None
    db.system_config.update_one({"_id": "cv_feed"}, {"$set": {
        "ver": VER, "last_tick": now, "err": None, "cursor": newest, "pages": pages,
        "rk": rk_now, "rk_real": (m or {}).get("rk_real"), "drop24": drop_now,
        "cond": tier_for(rk_now, drop_now), "last_pf": n_pf, "last_sig": n_sig}}, upsert=True)
    if n_sig:
        logger.info(f"[cv] 🛰 PF {n_pf}, в тень {n_sig} (индекс {rk_now}, за 24ч −{drop_now})")
    return {"ok": True, "pf": n_pf, "sig": n_sig}


def status(db):
    """Сводка для /api/live."""
    try:
        st = db.system_config.find_one({"_id": "cv_feed"}) or {}
        now = _utcnow()
        day = now - timedelta(hours=24)
        out = {k: st.get(k) for k in ("ver", "err", "rk", "rk_real", "drop24", "cond", "cursor")}
        lt = st.get("last_tick")
        out["last_tick"] = lt.isoformat() if lt else None
        out["age_min"] = round((now - lt).total_seconds() / 60, 1) if lt else None
        out["pf_24h"] = db.cv_pf_signals.count_documents({"backfill": False, "at": {"$gte": day}})
        out["rule_24h"] = db.cv_pf_signals.count_documents(
            {"backfill": False, "at": {"$gte": day}, "tier": {"$ne": None}})
        out["sig_24h"] = db.academy_signals.count_documents(
            {"strategy": {"$in": list(SOURCES)}, "created_at": {"$gte": day}})
        out["thr"] = {"strict": DROP_STRICT, "soft": DROP_SOFT, "floor": RK_FLOOR}
        return out
    except Exception:
        logger.debug("[cv] status fail", exc_info=True)
        return None


def _set_err(db, msg):
    db.system_config.update_one({"_id": "cv_feed"}, {"$set": {
        "ver": VER, "last_tick": _utcnow(), "err": msg}}, upsert=True)


async def cv_loop():
    """Фоновый цикл (watcher): раз в 5 минут, Mongo и сеть — в потоке."""
    await asyncio.sleep(75)
    while True:
        try:
            from database import _get_db
            await asyncio.to_thread(tick, _get_db())
        except Exception as e:
            _msg = f"{type(e).__name__}: {str(e)[:200]}"
            logger.warning(f"[cv] tick fail: {_msg}")
            try:
                from database import _get_db
                await asyncio.to_thread(_set_err, _get_db(), _msg)
            except Exception:
                pass
        await asyncio.sleep(POLL_SEC)


def schedule():
    """Запуск из watcher; ссылка на задачу держится в модуле, чтобы её не собрал GC."""
    _TASKS.append(asyncio.create_task(cv_loop()))
