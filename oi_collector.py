# -*- coding: utf-8 -*-
"""📊 Слой открытого интереса (OI) — 15.08.26.

Сбор per-coin истории OI фьючерсов и живой карты изменений:
  — бутстрап: fapi /futures/data/openInterestHist (period=1h, limit=500
    ≈ 20 дней истории разом), через fapi_budget (418-баны);
  — снапшоты: fapi /fapi/v1/openInterest, фолбэк BingX ccxt
    fetchOpenInterest (fapi с Railway работает с бюджетом, локально 451);
  — хранение: oi_history {s, t(ms), oi(контракты), usd|None, at(TTL 45д)};
  — витрина: market_state._id="oi_now" → map {SYM: {oi, d1h, d4h, d24h}}.

Зачем: цена+объём+дельта уже в системе; OI — независимый столбец той же
природы. Квадрант цена×OI различает «новые деньги» от «сквиза» — то, что
не видно в свечах (разборы ACE/ALICE/EDEN 14-15.08). Сигналы НЕ трогает:
только сбор, штамп indicators.oi4/oi24 и блок в 🎰-разборе.
"""
from __future__ import annotations
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

BOOT_PER_CYCLE = 80      # бутстрапов/лечений истории за цикл (пейсинг 0.45с)
SNAP_PAUSE = 0.12        # пауза между запросами снапшотов
FRESH_H = 26             # история свежее этого — пара считается забутстрапленной


def _fapi_get(path: str, params: dict, tag: str) -> Optional[list | dict]:
    try:
        from fapi_budget import allow
        if not allow(tag=tag):
            return None
    except Exception:
        pass
    try:
        import requests
        r = requests.get(f"https://fapi.binance.com{path}", params=params,
                         timeout=10)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def fetch_oi_hist(sym: str) -> list[dict]:
    """История OI по часу (до ~500 точек). [] если недоступно."""
    rows = _fapi_get("/futures/data/openInterestHist",
                     {"symbol": sym, "period": "1h", "limit": 500}, "oi_hist")
    if not isinstance(rows, list):
        return []
    out = []
    for x in rows:
        try:
            out.append({"t": int(x["timestamp"]),
                        "oi": float(x["sumOpenInterest"]),
                        "usd": float(x.get("sumOpenInterestValue") or 0) or None})
        except Exception:
            continue
    return out


def fetch_oi_now(sym: str) -> Optional[dict]:
    """Текущий OI: fapi → BingX. None если оба недоступны."""
    d = _fapi_get("/fapi/v1/openInterest", {"symbol": sym}, "oi_now")
    if isinstance(d, dict) and d.get("openInterest") is not None:
        try:
            return {"t": int(d.get("time") or time.time() * 1000),
                    "oi": float(d["openInterest"]), "usd": None}
        except Exception:
            pass
    try:
        from exchange import _get_bingx_public
        ex = _get_bingx_public()
        if ex is None:
            return None
        mkt = sym.replace("USDT", "") + "/USDT:USDT"
        r = ex.fetch_open_interest(mkt)
        oi = r.get("openInterestAmount") or r.get("openInterestValue")
        if oi:
            return {"t": int(r.get("timestamp") or time.time() * 1000),
                    "oi": float(oi), "usd": float(r.get("openInterestValue") or 0) or None}
    except Exception:
        logger.debug("[oi] bingx fail %s", sym, exc_info=True)
    return None


def _nearest(rows: list[dict], t_ms: int, tol_ms: int = 80 * 60_000):
    best, bd = None, None
    for r in rows:
        d = abs(r["t"] - t_ms)
        if bd is None or d < bd:
            best, bd = r, d
    return best if best is not None and bd <= tol_ms else None


def run_once() -> dict:
    """Один цикл (sync, из watcher через to_thread). 17.08 v2: часовые
    снапшоты уже делает deriv_collector.oi_poll_loop → oi_hourly
    ({_id: 'SYM:hour_ts', symbol, at, oi, oi_usd}, история с 01.08).
    Здесь: (1) бутстрап ГЛУБИНЫ истории openInterestHist для пар, где
    в oi_hourly <200 точек (новые листинги); (2) витрина
    market_state.oi_now (d1h/d4h/d24h) из oi_hourly."""
    from database import _get_db, utcnow
    from datetime import datetime, timedelta, timezone
    from pymongo import UpdateOne
    db = _get_db()
    col = db.oi_hourly
    now = utcnow()
    now_ms = int(now.timestamp() * 1000)
    syms = [d["_id"] for d in db.pair_context.find({}, {"_id": 1}).limit(330)]
    if not syms:
        return {"ok": False, "err": "нет pair_context"}
    # покрытие за последние 26ч: часовой цикл oi_poll пропускает пары
    # при бюджет-отказах → дыры в истории ломали d4h/d24h (17.08).
    # openInterestHist закрывает разом 500ч — лечим им и дыры, и хвосты.
    win = now - timedelta(hours=26)
    cov = {d["_id"]: d["n"] for d in col.aggregate([
        {"$match": {"symbol": {"$in": syms}, "at": {"$gte": win}}},
        {"$group": {"_id": "$symbol", "n": {"$sum": 1}}}])}
    # бутстрап истории: openInterestHist отдаёт [] и с Railway (гео) —
    # держим малый кап на случай разбана, основное лечение — снапшоты ниже
    boot = [s for s in syms if cov.get(s, 0) < 20][:10]
    booted = 0
    for s in boot:
        rows = fetch_oi_hist(s)
        if not rows:
            break   # гео-блок — не жечь попытки
        ops = []
        for r in rows:
            hs = r["t"] // 1000 // 3600 * 3600
            at_ = datetime.fromtimestamp(hs, tz=timezone.utc).replace(tzinfo=None)
            ops.append(UpdateOne(
                {"_id": f"{s}:{hs}"},
                {"$setOnInsert": {"symbol": s, "at": at_, "oi": r["oi"],
                                  "oi_usd": r.get("usd")}}, upsert=True))
        try:
            col.bulk_write(ops, ordered=False)
            booted += 1
        except Exception:
            logger.warning("[oi] bulk fail %s", s, exc_info=True)
        time.sleep(0.45)
    # 🔧 дозаполнение ТЕКУЩЕГО часа (17.08): часовой oi_poll теряет пары
    # на бюджет-отказах (у BNB <20 точек/26ч) — добираем пропущенных
    # fapi→BingX. Вперёд история становится плотной, d4h через 4ч,
    # d24h через сутки.
    hour_ts = int(now.timestamp()) // 3600 * 3600
    at_h = datetime.fromtimestamp(hour_ts, tz=timezone.utc).replace(tzinfo=None)
    have_now = set(d["symbol"] for d in col.find(
        {"at": at_h, "symbol": {"$in": syms}}, {"symbol": 1}))
    snapped = 0
    for s in syms:
        if s in have_now:
            continue
        if snapped >= BOOT_PER_CYCLE * 3:
            break
        d = fetch_oi_now(s)
        if d and d.get("oi"):
            try:
                col.update_one(
                    {"_id": f"{s}:{hour_ts}"},
                    {"$setOnInsert": {"symbol": s, "at": at_h,
                                      "oi": d["oi"],
                                      "oi_usd": d.get("usd")}}, upsert=True)
                snapped += 1
            except Exception:
                pass
        time.sleep(SNAP_PAUSE)
    # витрина изменений из oi_hourly
    oi_map = {}
    since = now - timedelta(hours=26)
    by_s: dict = {}
    for r in col.find({"at": {"$gte": since}},
                      {"symbol": 1, "at": 1, "oi": 1}):
        by_s.setdefault(r["symbol"], []).append(
            {"t": int(r["at"].timestamp() * 1000), "oi": r["oi"]})
    for s, rows in by_s.items():
        rows.sort(key=lambda x: x["t"])
        last = rows[-1]
        if now_ms - last["t"] > 3 * 3600_000 or last["oi"] <= 0:
            continue
        ent = {"oi": last["oi"], "t": last["t"]}
        for name, hrs in (("d1h", 1), ("d4h", 4), ("d24h", 24)):
            p = _nearest(rows, last["t"] - hrs * 3600_000)
            if p and p["oi"] > 0:
                ent[name] = round((last["oi"] / p["oi"] - 1) * 100, 2)
        oi_map[s] = ent
    try:
        db.market_state.update_one(
            {"_id": "oi_now"},
            {"$set": {"map": oi_map, "at": now, "booted": booted,
                      "snapped": snapped}}, upsert=True)
    except Exception:
        logger.warning("[oi] oi_now store fail", exc_info=True)
    # 💀/😴 живость для монет ВНЕ скан-универсума (17.08): журнал шире
    # 155 сканируемых пар — считаем тем же критерием по Vision-барам для
    # монет с сигналами за 7д без свежего статуса. Кап 60/цикл, стемп
    # vit_ext_at (скан свои пары пишет сам и их не трогаем).
    vit_done = 0
    try:
        import urllib.request as _ur
        import json as _js
        scan_set = set(syms)
        sig_syms = db.new_strategy_signals.distinct(
            "symbol", {"created_at": {"$gte": now - timedelta(days=7)}})
        fresh_vit = now - timedelta(hours=2)
        pc_docs = {d["_id"]: d for d in db.pair_context.find(
            {"_id": {"$in": sig_syms}},
            {"updated_at": 1, "vit_ext_at": 1})}
        todo = []
        for s in sig_syms:
            if not s or s in scan_set:
                continue
            pd_ = pc_docs.get(s)
            if pd_ and ((pd_.get("updated_at") and pd_["updated_at"] > fresh_vit)
                        or (pd_.get("vit_ext_at") and pd_["vit_ext_at"] > fresh_vit)):
                continue
            todo.append(s)
        for s in todo[:60]:
            try:
                url = ("https://data-api.binance.vision/api/v3/klines?"
                       f"symbol={s}&interval=1h&limit=400")
                with _ur.urlopen(url, timeout=10) as r:
                    kl = _js.loads(r.read())
                if not kl or len(kl) < 200:
                    continue
                h_ = [float(x[2]) for x in kl[:-1]]
                l_ = [float(x[3]) for x in kl[:-1]]
                c_ = [float(x[4]) for x in kl[:-1]]
                v_ = [float(x[5]) for x in kl[:-1]]
                px = c_[-1]
                if px <= 0:
                    continue
                trs = [max(h_[i] - l_[i], abs(h_[i] - c_[i - 1]),
                           abs(l_[i] - c_[i - 1])) for i in range(-14, 0)]
                atrp = sum(trs) / 14 / px * 100
                fade = (sum(v_[-120:]) / 120) / max(sum(v_) / len(v_), 1e-12)
                drop = (1 - px / max(h_)) * 100
                vit = None
                if atrp < 0.12:
                    vit = "dead"
                elif atrp < 0.8 and (fade < 0.75 or drop > 35):
                    vit = "dead" if drop > 25 else "sleep"
                db.pair_context.update_one(
                    {"_id": s},
                    {"$set": {"vitality": vit, "vit_ext_at": now}},
                    upsert=True)
                vit_done += 1
            except Exception:
                pass
            time.sleep(SNAP_PAUSE)
    except Exception:
        logger.warning("[oi] vitality-ext fail", exc_info=True)
    res = {"ok": True, "universe": len(syms), "booted": booted,
           "snapped": snapped, "mapped": len(oi_map), "vit_ext": vit_done}
    logger.info("[oi] cycle: %s", res)
    return res


def oi_quadrant(d_oi_24h: Optional[float], mom24: Optional[float]) -> Optional[str]:
    """Квадрант цена×OI: что стоит за движением."""
    if d_oi_24h is None or mom24 is None:
        return None
    up_oi = d_oi_24h >= 1.5
    dn_oi = d_oi_24h <= -1.5
    up_px = mom24 >= 1.5
    dn_px = mom24 <= -1.5
    if up_px and up_oi:
        return "🟢 рост на НОВЫХ деньгах (лонги заходят)"
    if up_px and dn_oi:
        return "🟡 рост на закрытии шортов (сквиз — топливо конечно)"
    if dn_px and up_oi:
        return "🔴 падение на новых шортах (давление растёт)"
    if dn_px and dn_oi:
        return "🟠 падение на разгрузке лонгов (капитуляция)"
    return None
