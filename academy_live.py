# -*- coding: utf-8 -*-
"""💵 Исполнитель Академии (03.10.26): зеркалит сделки 💎 live-среза
(academy_paper, live=True) на биржу через live_trader (per-account ccxt,
BingX perpetual). Школа и её правила не меняются — исполнитель только
повторяет то, что лайв-срез уже решил открыть, и закрывает, когда
paper закрылся (TP/SL ставятся на бирже сразу при входе).

Безопасность:
  • мастер-выключатель system_config.academy_live.enabled (по умолчанию ВЫКЛ);
  • работает ТОЛЬКО с аккаунтами, у которых academy=True (ПОТОК-зеркало их
    не видит — get_enabled_accounts их исключает);
  • фиксированная маржа на сделку (margin_usd) и плечо (leverage), кап
    max_open; плюс per-account safety (пресет «academy», kill switch);
  • идемпотентность: claim на paper-документе + проверка live_trades по
    paper_trade_id перед открытием; повтор при временной ошибке ≤3 раз,
    «не листинг/safety/размер» — пропуск сразу (x_skip);
  • выход по школе: TP по exit_open (base +10 / tp15 / tp20 / hold = без TP),
    SL −5, таймаут 96ч — как у paper-двойника.
Статус: system_config.academy_live_status (каждый тик), /api/live.exec,
/api/academy/exec (GET/POST), компонент здоровья academy_live.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

logger = logging.getLogger(__name__)

CFG_ID = "academy_live"
STATUS_ID = "academy_live_status"
DEFAULTS = {"enabled": False, "margin_usd": 10.0, "leverage": 2,
            "max_open": 15, "fresh_min": 120, "max_try": 3}
TP_PCT = {"base": 0.10, "tp15": 0.15, "tp20": 0.20, "hold": None}
SL_PCT = 0.05
HORIZON_H = 96
_FINAL_ERR = ("not listed", "INACTIVE", "safety", "amount", "notional",
              "not-supported", "exchange not configured")


def _db():
    from database import _get_db
    return _get_db()


def _now():
    from database import utcnow
    return utcnow()


def get_cfg(db=None) -> dict:
    db = _db() if db is None else db
    doc = db.system_config.find_one({"_id": CFG_ID}) or {}
    cfg = dict(DEFAULTS)
    cfg.update({k: v for k, v in doc.items() if k != "_id"})
    return cfg


def set_cfg(db=None, **kw) -> dict:
    """Меняет только известные ключи (enabled/margin_usd/leverage/max_open/
    fresh_min/max_try)."""
    db = _db() if db is None else db
    upd = {}
    for k, v in kw.items():
        if k not in DEFAULTS or v is None:
            continue
        if k == "enabled":
            upd[k] = bool(v)
        elif k == "margin_usd":
            upd[k] = max(1.0, min(float(v), 500.0))
        elif k in ("leverage", "max_open", "fresh_min", "max_try"):
            upd[k] = int(v)
    if not upd:
        return get_cfg(db)
    upd["updated_at"] = _now()
    db.system_config.update_one({"_id": CFG_ID}, {"$set": upd}, upsert=True)
    logger.warning(f"[academy-live] cfg {upd}")
    return get_cfg(db)


def accounts(db=None) -> list:
    db = _db() if db is None else db
    return list(db.live_accounts.find({"academy": True, "enabled": True,
                                       "kill_switch": {"$ne": True}}))


def tp_pct_for(exit_open):
    return TP_PCT.get(exit_open or "base", 0.10)


def status(db=None) -> dict:
    """Короткий статус для 💎 и /api/academy/exec."""
    db = _db() if db is None else db
    cfg = get_cfg(db)
    st = db.system_config.find_one({"_id": STATUS_ID}) or {}
    accs = list(db.live_accounts.find({"academy": True},
                                      {"enabled": 1, "kill_switch": 1, "mode": 1,
                                       "exchange": 1, "balance": 1, "label": 1}))
    now = _now()
    day0 = now.replace(hour=0, minute=0, second=0, microsecond=0)
    q = {"source": "academy"}
    open_n = db.live_trades.count_documents({**q, "status": "OPEN"})
    today = list(db.live_trades.find({**q, "opened_at": {"$gte": day0}},
                                     {"status": 1, "pnl_usdt": 1}))
    closed = list(db.live_trades.find({**q, "status": {"$ne": "OPEN"}},
                                      {"pnl_usdt": 1, "pnl_pct": 1}))
    return {
        "enabled": bool(cfg.get("enabled")), "margin_usd": cfg.get("margin_usd"),
        "leverage": cfg.get("leverage"), "max_open": cfg.get("max_open"),
        "accounts": [{"id": a["_id"], "enabled": bool(a.get("enabled")),
                      "kill": bool(a.get("kill_switch")), "mode": a.get("mode"),
                      "exchange": a.get("exchange"), "balance": a.get("balance"),
                      "label": a.get("label")} for a in accs],
        "open_n": open_n, "today_n": len(today),
        "today_pnl_usdt": round(sum(float(t.get("pnl_usdt") or 0) for t in today), 2),
        "closed_n": len(closed),
        "closed_pnl_usdt": round(sum(float(t.get("pnl_usdt") or 0) for t in closed), 2),
        "last_tick": st.get("at").isoformat() if hasattr(st.get("at"), "isoformat") else st.get("at"),
        "last": {k: st.get(k) for k in ("opened", "closed", "errors", "last_err")},
    }


def _alert(text: str) -> None:
    try:
        import learn_digest
        learn_digest.send(text)
    except Exception:
        logger.debug("[academy-live] alert fail", exc_info=True)


async def tick() -> dict:
    """Один проход: sync биржи → открыть свежие live-paper → закрыть то, что
    paper закрыл / 96ч. Все sync-Mongo вызовы — через to_thread."""
    db = _db()
    now = _now()
    cfg = await asyncio.to_thread(get_cfg, db)
    accs = await asyncio.to_thread(accounts, db)
    stats = {"at": now, "enabled": bool(cfg["enabled"]), "accounts": [a["_id"] for a in accs],
             "opened": 0, "closed": 0, "errors": 0, "last_err": None}
    if not cfg["enabled"] or not accs:
        await asyncio.to_thread(db.system_config.update_one, {"_id": STATUS_ID},
                                {"$set": stats}, True)
        return stats
    import live_trader as lt

    # 1) реконсиляция: TP/SL, исполненные биржей → закрыть документы
    for acc in accs:
        try:
            await asyncio.wait_for(lt.sync_positions_for_account(acc), timeout=25.0)
        except Exception as e:
            logger.debug(f"[academy-live] sync {acc['_id']}: {e}")

    # 2) открытие: свежие live-paper без зеркала
    since = now - timedelta(minutes=int(cfg["fresh_min"]))
    cands = await asyncio.to_thread(lambda: list(db.academy_paper.find(
        {"live": True, "state": "OPEN", "probe": {"$ne": True},
         "opened_at": {"$gte": since}, "x_skip": {"$exists": False}}
    ).sort("opened_at", 1)))
    for acc in accs:
        aid = acc["_id"]
        open_n = await asyncio.to_thread(
            db.live_trades.count_documents,
            {"account_id": aid, "status": "OPEN", "source": "academy"})
        for t in cands:
            if open_n >= int(cfg["max_open"]):
                break
            key = str(t["_id"])
            exists = await asyncio.to_thread(
                db.live_trades.find_one,
                {"paper_trade_id": key, "account_id": aid}, {"_id": 1})
            if exists:
                continue
            claimed = await asyncio.to_thread(
                db.academy_paper.update_one,
                {"_id": t["_id"], f"x_claim.{aid}": {"$exists": False}},
                {"$set": {f"x_claim.{aid}": now}})
            if claimed.modified_count == 0:
                continue
            try:
                entry = float(t["entry"])
            except Exception:
                continue
            sg = 1 if t["dir"] == "LONG" else -1
            tpp = tp_pct_for(t.get("exit_open"))
            tp1 = round(entry * (1 + sg * tpp), 10) if tpp else None
            sl = round(entry * (1 - sg * SL_PCT), 10)
            pair = t.get("pair") or (t["sym"][:-4] + "/USDT")
            signal = {"symbol": t["sym"], "pair": pair, "direction": t["dir"],
                      "entry": entry, "tp1": tp1, "sl": sl, "source": "academy",
                      "paper_trade_id": key,
                      # размер = фикс. маржа: balance×size_pct/100 при size_pct=100
                      "_paper_balance_for_sizing": float(cfg["margin_usd"])}
            decision = {"size_pct": 100.0, "leverage": int(cfg["leverage"]),
                        "tp1": tp1, "sl": sl,
                        "reasoning": (f"academy {t.get('src')} · {t.get('rule')} · "
                                      f"exit={t.get('exit_open') or 'base'}")}
            try:
                res = await asyncio.wait_for(
                    lt.open_position_for_account(signal, decision, acc), timeout=60.0)
            except Exception as e:
                res = {"ok": False, "error": f"{type(e).__name__}: {e}"[:300]}
            res = res or {"ok": False, "error": "no result"}
            if res.get("ok"):
                tr = res.get("trade") or {}
                open_n += 1
                stats["opened"] += 1
                await asyncio.to_thread(
                    db.academy_paper.update_one, {"_id": t["_id"]},
                    {"$set": {f"x_live.{aid}": {
                        "trade_id": tr.get("trade_id"), "entry": tr.get("entry"),
                        "at": now, "tp1": tp1, "sl": sl,
                        "tp_order": tr.get("tp_order_id"), "sl_order": tr.get("sl_order_id")}}})
                _alert(f"💵 <b>LIVE ОТКРЫТ</b> [{aid}] {t['sym']} {t['dir']} ×{cfg['leverage']} "
                       f"${cfg['margin_usd']} · вход {tr.get('entry')} · "
                       f"TP {tp1 if tp1 else 'нет (hold)'} · SL {sl}\n"
                       f"{t.get('src')} · {t.get('rule')} · выход {t.get('exit_open') or 'base'}")
                logger.warning(f"[academy-live] OPEN {aid} {t['sym']} {t['dir']} "
                               f"paper={key} trade#{tr.get('trade_id')}")
            else:
                err = str(res.get("error") or "?")
                stats["errors"] += 1
                stats["last_err"] = f"{t['sym']}: {err}"[:300]
                n_try = int(t.get("x_try") or 0) + 1
                final = n_try >= int(cfg["max_try"]) or any(s in err for s in _FINAL_ERR)
                upd = {"$set": {"x_try": n_try, "x_err": err[:300]}}
                if final:
                    upd["$set"]["x_skip"] = now
                else:
                    upd["$unset"] = {f"x_claim.{aid}": ""}
                await asyncio.to_thread(db.academy_paper.update_one, {"_id": t["_id"]}, upd)
                logger.warning(f"[academy-live] OPEN FAIL {aid} {t['sym']} try {n_try}: {err}")
                if final or n_try == 1:
                    _alert(f"⚠️ <b>LIVE не открыт</b> [{aid}] {t['sym']} {t['dir']}: {err[:200]}"
                           + (" — пропускаю" if final else " — повторю"))

    # 3) закрытие: paper закрылся или 96ч
    for acc in accs:
        aid = acc["_id"]
        lts = await asyncio.to_thread(lambda: list(db.live_trades.find(
            {"account_id": aid, "status": "OPEN", "source": "academy"})))
        for lp in lts:
            key = lp.get("paper_trade_id")
            pd = (await asyncio.to_thread(db.academy_paper.find_one, {"_id": key},
                                          {"state": 1, "r": 1, "exit_open": 1})
                  if key else None)
            age_h = ((now - lp["opened_at"]).total_seconds() / 3600
                     if lp.get("opened_at") else 0)
            reason = None
            if pd and pd.get("state") not in (None, "OPEN"):
                reason = str(pd["state"])
            elif age_h >= HORIZON_H + 0.5:
                reason = "TIMEOUT"
            if not reason:
                continue
            try:
                res = await asyncio.wait_for(
                    lt.mirror_full_close_for_account(lp, reason, acc), timeout=60.0)
            except Exception as e:
                res = {"ok": False, "error": f"{type(e).__name__}: {e}"[:300]}
            res = res or {}
            if res.get("ok"):
                stats["closed"] += 1
                _alert(f"💵 <b>LIVE ЗАКРЫТ</b> [{aid}] {lp.get('symbol')} {lp.get('direction')} "
                       f"по {reason}"
                       + (f" · paper {pd.get('r'):+.1f}%" if pd and pd.get("r") is not None else ""))
                logger.warning(f"[academy-live] CLOSE {aid} {lp.get('symbol')} {reason}")
            else:
                stats["errors"] += 1
                stats["last_err"] = f"close {lp.get('symbol')}: {res.get('error')}"[:300]
                logger.warning(f"[academy-live] CLOSE FAIL {aid} {lp.get('symbol')}: {res.get('error')}")

    await asyncio.to_thread(db.system_config.update_one, {"_id": STATUS_ID},
                            {"$set": stats}, True)
    return stats
