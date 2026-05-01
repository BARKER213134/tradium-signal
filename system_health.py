"""Комплексный health-monitor для платформы.

Возвращает структурированный отчёт о состоянии всех подсистем:
- Telethon userbot (подключён ли)
- Watcher tick (свежесть последних events)
- Сигналы: CV/Tradium/Supertrend (приходят ли)
- Orphan paper trades (paper open без live mirror)
- Database (Mongo connectivity)
- Critical loops (paper_to_live, sync_positions активны?)

Каждый check возвращает:
  - status: "ok" | "warn" | "error"
  - message: человекочитаемое описание
  - details: optional dict с метриками

Используется:
1. /api/system-health — endpoint для UI
2. Background watchdog loop — пишет alerts если 'error'
"""
from __future__ import annotations
from datetime import datetime, timedelta, timezone


def _utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _age_minutes(dt) -> int | None:
    if not dt:
        return None
    if hasattr(dt, "replace"):
        try:
            dt2 = dt.replace(tzinfo=None) if getattr(dt, "tzinfo", None) else dt
            return int((_utcnow() - dt2).total_seconds() / 60)
        except Exception:
            return None
    return None


def check_telethon() -> dict:
    """Подключён ли Telethon userbot."""
    try:
        from userbot import _tg_client, _last_setup_error, _last_setup_at
        connected = _tg_client and _tg_client.is_connected()
        if connected:
            age = _age_minutes(_last_setup_at)
            return {
                "name": "Telethon (userbot)",
                "status": "ok",
                "message": f"Подключён (setup {age}мин назад)" if age is not None else "Подключён",
                "details": {"last_setup_min": age},
            }
        err = (_last_setup_error or "")[:120]
        if "two different IP" in err or "authorization key" in err:
            return {
                "name": "Telethon (userbot)",
                "status": "error",
                "message": f"Сессия заблокирована Telegram'ом — нужен re-login через UI banner",
                "details": {"error": err},
            }
        return {
            "name": "Telethon (userbot)",
            "status": "error",
            "message": f"Не подключён: {err[:80] if err else 'unknown'}",
            "details": {"error": err},
        }
    except Exception as e:
        return {"name": "Telethon (userbot)", "status": "error",
                "message": f"check failed: {e}"}


def check_signals_freshness() -> list[dict]:
    """Свежесть последних сигналов по источникам."""
    try:
        from database import _signals, _supertrend_signals
        from pymongo import DESCENDING
    except Exception as e:
        return [{"name": "Signals freshness", "status": "error",
                 "message": f"db import: {e}"}]

    results = []
    # Cryptovizor
    try:
        last = _signals().find_one({"source": "cryptovizor"},
                                    sort=[("received_at", DESCENDING)])
        if last:
            age = _age_minutes(last.get("received_at"))
            if age is None:
                status, msg = "warn", "received_at не парсится"
            elif age > 240:  # 4 часа
                status = "error"
                msg = f"Последний CV сигнал {age}мин назад — userbot не получает или группа молчит"
            elif age > 60:
                status = "warn"
                msg = f"Последний CV сигнал {age}мин назад — необычно долго"
            else:
                status = "ok"
                msg = f"Последний CV: {age}мин назад"
            results.append({"name": "Cryptovizor приём", "status": status,
                            "message": msg, "details": {"age_min": age}})
    except Exception as e:
        results.append({"name": "Cryptovizor приём", "status": "error",
                        "message": f"query: {e}"})

    # Tradium
    try:
        last = _signals().find_one({"source": "tradium"},
                                    sort=[("received_at", DESCENDING)])
        if last:
            age = _age_minutes(last.get("received_at"))
            if age is None:
                status, msg = "warn", "received_at не парсится"
            elif age > 720:  # 12 часов (Tradium редкий)
                status = "warn"
                msg = f"Tradium {age}мин назад — необычно долго (forum может молчать)"
            else:
                status = "ok"
                msg = f"Последний Tradium: {age}мин назад"
            results.append({"name": "Tradium приём", "status": status,
                            "message": msg, "details": {"age_min": age}})
    except Exception as e:
        results.append({"name": "Tradium приём", "status": "error",
                        "message": f"query: {e}"})

    # Supertrend
    try:
        last = _supertrend_signals().find_one({}, sort=[("flip_at", DESCENDING)])
        if last:
            age = _age_minutes(last.get("flip_at"))
            if age is None:
                status, msg = "warn", "flip_at не парсится"
            elif age > 120:  # 2 часа
                status = "warn"
                msg = f"ST flip {age}мин назад — st-tracker может быть остановлен"
            else:
                status = "ok"
                msg = f"Последний ST flip: {age}мин назад"
            results.append({"name": "SuperTrend tracker", "status": status,
                            "message": msg, "details": {"age_min": age}})
    except Exception as e:
        results.append({"name": "SuperTrend tracker", "status": "error",
                        "message": f"query: {e}"})

    return results


def check_watcher_tick() -> dict:
    """Свежесть watcher heartbeat. Раньше смотрели на _events с типами
    cv_alert_*/anomaly_detected/etc — но эти события пишутся только когда
    реально срабатывает алерт. Если CV-сигналов нет 2ч — false alarm
    "Watcher заглох" хотя watcher жив. Теперь смотрим на heartbeat
    document который watcher пишет КАЖДЫЙ тик (30с) — настоящий liveness."""
    try:
        from database import _get_db
        hb = _get_db().system.find_one({"_id": "watcher_heartbeat"})
        if not hb:
            return {"name": "Watcher tick", "status": "warn",
                    "message": "heartbeat ещё не писался — watcher не стартовал"}
        age = _age_minutes(hb.get("at"))
        stage = hb.get("stage", "?")
        if age is None:
            return {"name": "Watcher tick", "status": "warn",
                    "message": "heartbeat at не парсится"}
        # Heartbeat пишется каждый tick (30с) + при supervisor attempts.
        # Norm: <2 мин. Warn: 2-5 мин (Atlas slow / one tick stuck).
        # Error: >5 мин (loop точно мёртв).
        if age > 5:
            return {"name": "Watcher tick", "status": "error",
                    "message": f"Watcher заглох — heartbeat stage='{stage}' {age}мин назад. Нужен redeploy.",
                    "details": {"age_min": age, "stage": stage}}
        if age > 2:
            return {"name": "Watcher tick", "status": "warn",
                    "message": f"Watcher вял — heartbeat {age}мин назад (stage='{stage}')",
                    "details": {"age_min": age, "stage": stage}}
        return {"name": "Watcher tick", "status": "ok",
                "message": f"Heartbeat свежий: {age}мин назад (stage='{stage}')",
                "details": {"age_min": age, "stage": stage}}
    except Exception as e:
        return {"name": "Watcher tick", "status": "error",
                "message": f"check: {e}"}


def check_orphan_paper_trades() -> dict:
    """Paper позиции открытые но без live mirror на enabled аккаунте.
    Это AZTEC-style баг — failsafe loop должен их подхватывать.
    Если есть orphan'ы возрастом >5 мин — failsafe не работает."""
    try:
        from database import _get_db, _live_trades
        db = _get_db()
        from live_safety import get_enabled_accounts
        accounts = get_enabled_accounts() or []
        if not accounts:
            return {"name": "Orphan paper trades", "status": "ok",
                    "message": "Нет enabled аккаунтов — нечего зеркалить"}
        cutoff = _utcnow() - timedelta(hours=6)
        recent = list(db.paper_trades.find({
            "status": "OPEN",
            "opened_at": {"$gte": cutoff},
            "trade_id": {"$exists": True},
        }, {"trade_id": 1, "symbol": 1, "opened_at": 1}))
        if not recent:
            return {"name": "Orphan paper trades", "status": "ok",
                    "message": "Все paper позиции свежие"}
        ptids = [p["trade_id"] for p in recent]
        existing = set()
        for lt in _live_trades().find(
            {"paper_trade_id": {"$in": ptids}},
            {"paper_trade_id": 1, "account_id": 1},
        ):
            existing.add((lt.get("paper_trade_id"), lt.get("account_id")))
        orphans = []
        for p in recent:
            for a in accounts:
                aid = a["_id"]
                if (p["trade_id"], aid) not in existing:
                    age = _age_minutes(p.get("opened_at"))
                    if age is not None and age > 5:  # старше 5 мин = failsafe должен был сработать
                        orphans.append({"ptid": p["trade_id"],
                                        "symbol": p.get("symbol"),
                                        "account": aid,
                                        "age_min": age})
        if not orphans:
            return {"name": "Orphan paper trades", "status": "ok",
                    "message": f"Все {len(recent)} paper позиций имеют live mirror"}
        return {"name": "Orphan paper trades", "status": "warn",
                "message": f"{len(orphans)} paper позиций БЕЗ live mirror (failsafe не отработал?)",
                "details": {"orphans": orphans[:5]}}
    except Exception as e:
        return {"name": "Orphan paper trades", "status": "error",
                "message": f"check: {e}"}


def check_database() -> dict:
    """Mongo connectivity + базовые команды."""
    try:
        import time as _time
        from database import _signals
        t0 = _time.time()
        cnt = _signals().estimated_document_count()
        elapsed_ms = int((_time.time() - t0) * 1000)
        if elapsed_ms > 5000:
            return {"name": "MongoDB Atlas", "status": "warn",
                    "message": f"Atlas медленный — {elapsed_ms}мс на ping",
                    "details": {"latency_ms": elapsed_ms, "signals_count": cnt}}
        return {"name": "MongoDB Atlas", "status": "ok",
                "message": f"Atlas OK ({elapsed_ms}мс ping, {cnt} signals)",
                "details": {"latency_ms": elapsed_ms, "signals_count": cnt}}
    except Exception as e:
        return {"name": "MongoDB Atlas", "status": "error",
                "message": f"unreachable: {str(e)[:120]}"}


def check_paper_status() -> dict:
    """Paper trader баланс / открытые / closed = sanity."""
    try:
        import paper_trader as pt
        balance = pt.get_balance()
        positions = pt.get_open_positions() or []
        if balance is None:
            return {"name": "Paper Trading", "status": "warn",
                    "message": "Balance не получен"}
        return {"name": "Paper Trading", "status": "ok",
                "message": f"Balance ${balance:.2f}, открытых: {len(positions)}",
                "details": {"balance": balance, "open": len(positions)}}
    except Exception as e:
        return {"name": "Paper Trading", "status": "error",
                "message": f"check: {e}"}


def check_recent_failures() -> dict:
    """Свежие FAILED_OPEN в live trades."""
    try:
        from database import _live_trades
        cutoff = _utcnow() - timedelta(hours=2)
        failed = list(_live_trades().find({
            "status": "FAILED_OPEN",
            "opened_at": {"$gte": cutoff},
        }, {"symbol": 1, "fail_reason": 1, "opened_at": 1}))
        if not failed:
            return {"name": "Live FAILED_OPEN (2ч)", "status": "ok",
                    "message": "Нет ошибок открытия за 2 часа"}
        if len(failed) >= 5:
            return {"name": "Live FAILED_OPEN (2ч)", "status": "error",
                    "message": f"{len(failed)} FAILED_OPEN за 2ч — серьёзная проблема с биржей",
                    "details": {"count": len(failed),
                                "samples": [(f.get("symbol"), (f.get("fail_reason") or "")[:60])
                                            for f in failed[:3]]}}
        return {"name": "Live FAILED_OPEN (2ч)", "status": "warn",
                "message": f"{len(failed)} FAILED_OPEN за 2ч",
                "details": {"count": len(failed)}}
    except Exception as e:
        return {"name": "Live FAILED_OPEN (2ч)", "status": "error",
                "message": f"check: {e}"}


def collect_health() -> dict:
    """Собирает все checks и возвращает aggregated отчёт.
    Status priorities: error > warn > ok."""
    checks: list[dict] = []
    checks.append(check_telethon())
    checks.append(check_database())
    checks.append(check_watcher_tick())
    checks.extend(check_signals_freshness())
    checks.append(check_paper_status())
    checks.append(check_orphan_paper_trades())
    checks.append(check_recent_failures())

    statuses = [c.get("status", "ok") for c in checks]
    if "error" in statuses:
        overall = "error"
    elif "warn" in statuses:
        overall = "warn"
    else:
        overall = "ok"

    n_err = sum(1 for s in statuses if s == "error")
    n_warn = sum(1 for s in statuses if s == "warn")
    n_ok = sum(1 for s in statuses if s == "ok")

    return {
        "overall": overall,
        "n_error": n_err, "n_warn": n_warn, "n_ok": n_ok,
        "checks": checks,
        "ts": _utcnow().isoformat(),
    }
