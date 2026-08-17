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

BOOT_PER_CYCLE = 45      # бутстрапов истории за цикл (бюджет)
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


def _nearest(rows: list[dict], t_ms: int, tol_ms: int = 55 * 60_000):
    best, bd = None, None
    for r in rows:
        d = abs(r["t"] - t_ms)
        if bd is None or d < bd:
            best, bd = r, d
    return best if best is not None and bd <= tol_ms else None


def run_once() -> dict:
    """Один цикл сбора (sync, зовётся из watcher через to_thread)."""
    from database import _get_db, utcnow
    from datetime import timedelta
    from pymongo import UpdateOne
    db = _get_db()
    col = db.oi_history
    try:
        col.create_index([("s", 1), ("t", 1)], unique=True, background=True)
        col.create_index("at", expireAfterSeconds=45 * 86400, background=True)
    except Exception:
        pass
    now = utcnow()
    now_ms = int(now.timestamp() * 1000)
    syms = [d["_id"] for d in db.pair_context.find({}, {"_id": 1}).limit(330)]
    if not syms:
        return {"ok": False, "err": "нет pair_context"}
    fresh_cut = now_ms - FRESH_H * 3600_000
    have = {d["_id"]: d["last_t"] for d in col.aggregate([
        {"$group": {"_id": "$s", "last_t": {"$max": "$t"}}}])}
    boot = [s for s in syms if have.get(s, 0) < fresh_cut][:BOOT_PER_CYCLE]
    booted = snapped = 0
    for s in boot:
        rows = fetch_oi_hist(s)
        if rows:
            ops = [UpdateOne({"s": s, "t": r["t"]},
                             {"$set": {**r, "s": s, "at": now}}, upsert=True)
                   for r in rows]
            try:
                col.bulk_write(ops, ordered=False)
                booted += 1
            except Exception:
                logger.warning("[oi] bulk fail %s", s, exc_info=True)
        time.sleep(SNAP_PAUSE)
    # снапшоты для уже забутстрапленных (свежих) пар
    for s in syms:
        if s in boot or have.get(s, 0) < fresh_cut - 24 * 3600_000:
            continue
        d = fetch_oi_now(s)
        if d:
            try:
                col.update_one({"s": s, "t": d["t"] // 600_000 * 600_000},
                               {"$set": {**d, "s": s, "at": now}}, upsert=True)
                snapped += 1
            except Exception:
                pass
        time.sleep(SNAP_PAUSE)
    # витрина изменений
    oi_map = {}
    since = now_ms - 26 * 3600_000
    cur = col.find({"t": {"$gte": since}}, {"s": 1, "t": 1, "oi": 1})
    by_s: dict = {}
    for r in cur:
        by_s.setdefault(r["s"], []).append(r)
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
            {"$set": {"map": oi_map, "at": now,
                      "n_hist": len(have), "booted": booted,
                      "snapped": snapped}}, upsert=True)
    except Exception:
        logger.warning("[oi] oi_now store fail", exc_info=True)
    res = {"ok": True, "universe": len(syms), "booted": booted,
           "snapped": snapped, "mapped": len(oi_map)}
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
