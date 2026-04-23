import asyncio
import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from typing import Set

from fastapi import FastAPI, Depends, Form, HTTPException, Request, WebSocket, WebSocketDisconnect, status
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse, Response, JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware

from config import ADMIN_USERNAME, ADMIN_PASSWORD, SECRET_KEY, BOTS
from database import get_db, Signal, Session, desc, func, get_events
from exchange import get_prices_any as _sync_get_prices, get_all_usdt_symbols as _sync_get_all_usdt_symbols, get_eth_market_context as _sync_eth_ctx, get_keltner_eth as _sync_kc_eth

from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app):
    """Гарантирует запуск watcher/userbot/bots.

    DEV_MODE=1 в .env → пропускаем запуск Telethon userbot, watcher, bot'ов.
    Нужно когда локальный запуск идёт параллельно с Railway — две копии
    userbot с одной Telegram-сессией вызывают дисконнекты и дубли алертов.
    UI / админка / бектесты работают как обычно.
    """
    _bg_tasks = []
    dev_mode = (os.getenv("DEV_MODE", "").strip() == "1")
    if dev_mode:
        print("[LIFESPAN] DEV_MODE=1 → userbot/watcher/bots НЕ запускаются "
              "(UI + backtests available, Telethon idle)", flush=True)
        yield
        return
    try:
        # Ждём 3с — main.py мог уже вызвать setup() но gather() ещё не запустил tasks
        await asyncio.sleep(3)

        from watcher import _watcher_running
        print(f"[LIFESPAN] watcher_running={_watcher_running}", flush=True)

        if not _watcher_running:
            print("[LIFESPAN] Запускаю watcher/userbot/bots...", flush=True)
            from config import BOT_TOKEN, ADMIN_CHAT_ID, BOT2_BOT_TOKEN, BOT4_BOT_TOKEN
            from database import init_db
            init_db()

            from bot import bot, start_bot
            from bot2 import bot2, start_bot2
            from userbot import set_bot, start_userbot
            from watcher import setup as setup_watcher, start_watcher, _bot as _wb

            if _wb is None:
                set_bot(bot, ADMIN_CHAT_ID)
                bot4 = None
                if BOT4_BOT_TOKEN:
                    try:
                        from aiogram import Bot as _B4
                        from aiogram.client.default import DefaultBotProperties as _DP4
                        from aiogram.enums import ParseMode as _PM4
                        bot4 = _B4(token=BOT4_BOT_TOKEN, default=_DP4(parse_mode=_PM4.HTML))
                    except Exception:
                        pass
                setup_watcher(bot, ADMIN_CHAT_ID, bot2=bot2, bot4=bot4)

            _bg_tasks.append(asyncio.create_task(start_userbot()))
            _bg_tasks.append(asyncio.create_task(start_bot()))
            _bg_tasks.append(asyncio.create_task(start_watcher()))
            if bot2:
                _bg_tasks.append(asyncio.create_task(start_bot2()))

            # CV + SuperTrend 30m flip observation watcher (observation-only)
            try:
                from cv_flip_watcher import start_cv_flip_watcher
                _bg_tasks.append(asyncio.create_task(start_cv_flip_watcher()))
            except Exception as _e:
                print(f"[LIFESPAN] cv_flip_watcher start skipped: {_e}", flush=True)

            # Userbot health watchdog — алерт в Telegram при disconnect >10 мин.
            # Работает независимо от userbot'ного supervisor'а (тот reconnect'ится
            # сам, но молча; этот watchdog даёт юзеру знать что что-то не так).
            try:
                _bg_tasks.append(asyncio.create_task(_userbot_health_watchdog(bot)))
            except Exception as _e:
                print(f"[LIFESPAN] userbot health watchdog skipped: {_e}", flush=True)

            print(f"[LIFESPAN] {len(_bg_tasks)} tasks launched", flush=True)
        else:
            print("[LIFESPAN] Watcher already running, skipping", flush=True)
    except Exception as e:
        import traceback
        print(f"[LIFESPAN] ERROR: {e}", flush=True)
        traceback.print_exc()

    yield

    for t in _bg_tasks:
        t.cancel()


async def _userbot_health_watchdog(_bot):
    """Алертит в ADMIN_CHAT_ID если userbot disconnected дольше DOWN_THRESHOLD.
    Второй алерт — когда вернулся в connected. Один цикл проверки = 60 сек."""
    import userbot as _ubm
    from config import ADMIN_CHAT_ID
    from datetime import timedelta
    DOWN_THRESHOLD = timedelta(minutes=10)
    disconnected_since = None
    alerted = False
    while True:
        try:
            await asyncio.sleep(60)
            client = getattr(_ubm, "_tg_client", None)
            is_conn = bool(client and client.is_connected())
            now = datetime.utcnow()
            if is_conn:
                if alerted:
                    # восстановился — шлём recovery
                    try:
                        await _bot.send_message(
                            ADMIN_CHAT_ID,
                            f"✅ <b>Userbot восстановлен</b> "
                            f"(был disconnected {_human_duration(now - disconnected_since)})",
                            parse_mode="HTML",
                        )
                    except Exception:
                        pass
                disconnected_since = None
                alerted = False
                continue
            # disconnected
            if disconnected_since is None:
                disconnected_since = now
            elif (not alerted) and (now - disconnected_since) >= DOWN_THRESHOLD:
                det = _ubm.get_status_details()
                err = det.get("last_setup_error") or "unknown"
                try:
                    await _bot.send_message(
                        ADMIN_CHAT_ID,
                        f"🚨 <b>Userbot disconnected &gt; 10 мин</b>\n"
                        f"С: <code>{disconnected_since.strftime('%H:%M:%S UTC')}</code>\n"
                        f"Последняя ошибка: <code>{err}</code>\n"
                        f"Reconnect попыток: {det.get('reconnect_count', 0)}\n"
                        f"Проверь Railway логи или Restart сервис.",
                        parse_mode="HTML",
                    )
                    alerted = True
                except Exception as e:
                    logging.getLogger(__name__).warning(f"[userbot-watchdog] alert fail: {e}")
        except asyncio.CancelledError:
            return
        except Exception:
            logging.getLogger(__name__).exception("[userbot-watchdog] loop error")


def _human_duration(delta) -> str:
    s = int(delta.total_seconds())
    if s < 60:
        return f"{s}с"
    m = s // 60
    if m < 60:
        return f"{m}м"
    h, mm = m // 60, m % 60
    return f"{h}ч {mm}м"


# Sentry (опционально — активен только если SENTRY_DSN задан)
try:
    _sentry_dsn = os.getenv("SENTRY_DSN", "").strip()
    if _sentry_dsn:
        import sentry_sdk
        sentry_sdk.init(
            dsn=_sentry_dsn,
            traces_sample_rate=0.1,  # 10% транзакций
            profiles_sample_rate=0.0,
            environment=os.getenv("RAILWAY_ENVIRONMENT", "production"),
            release=os.getenv("RAILWAY_GIT_COMMIT_SHA", "unknown")[:7],
        )
        logging.getLogger(__name__).info("[SENTRY] initialized")
except Exception as _e:
    logging.getLogger(__name__).warning(f"[SENTRY] init failed: {_e}")

app = FastAPI(title="Tradium Screener Admin", lifespan=lifespan)

# Rate limiting (slowapi)
try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.util import get_remote_address
    from slowapi.errors import RateLimitExceeded
    _limiter = Limiter(key_func=get_remote_address, default_limits=["300/minute"])
    app.state.limiter = _limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
except ImportError:
    logging.getLogger(__name__).warning("[RATE-LIMIT] slowapi not installed")
    _limiter = None

templates = Jinja2Templates(directory=os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates"))


# ── Session cookie auth ───────────────────────────────────────────────
SESSION_COOKIE = "tradium_session"
SESSION_TTL = 7 * 24 * 3600  # 7 дней
_SECRET = (SECRET_KEY or "change-me").encode()


def _sign(payload: str) -> str:
    sig = hmac.new(_SECRET, payload.encode(), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(sig).decode().rstrip("=")


def _make_token(username: str) -> str:
    expiry = int(time.time()) + SESSION_TTL
    payload = f"{username}:{expiry}"
    return f"{payload}:{_sign(payload)}"


def _verify_token(token: str | None) -> str | None:
    if not token:
        return None
    try:
        username, expiry, sig = token.rsplit(":", 2)
        payload = f"{username}:{expiry}"
        if not hmac.compare_digest(sig, _sign(payload)):
            return None
        if int(expiry) < int(time.time()):
            return None
        return username
    except Exception:
        return None


_OPEN_PATHS = {"/login", "/static"}


class SessionAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path in ("/login", "/health", "/healthz", "/api/userbot-status", "/api/backfill-missed", "/api/backfill-patterns", "/api/activate-tradium-archive", "/api/backfill-tradium-charts", "/api/peek-tradium", "/api/peek-tradium-topic", "/api/key-levels/recent", "/api/key-levels/enrich", "/api/key-levels/stats", "/api/key-levels/backfill", "/api/key-levels/backfill-status", "/api/key-levels/coverage", "/api/backtest-st", "/api/backtest-st/status",
            "/api/supertrend-signals", "/api/supertrend-signals/by-pair",
            "/api/supertrend-stats", "/api/st-enrich",
            "/api/supertrend/backfill", "/api/supertrend/backfill-status", "/api/bots-status",
            "/api/backtest-st-signals", "/api/backtest-st-signals/status", "/api/paper/started",
            "/api/paper/close", "/api/paper/mode", "/api/paper/learnings", "/api/paper/refresh-ai-memory",
            "/api/paper/ai-prompt", "/api/paper/set-balance", "/api/paper/ai-test",
            "/api/paper/clear-ai-memory",
            "/api/paper/rejections", "/api/paper/be-audit", "/api/paper/close-all",
            "/api/paper/history",
            "/api/backtest-yesterday", "/api/backtest-yesterday/status",
            "/api/backtest-optimize", "/api/backtest-optimize/status",
            "/api/backtest-entry-timing", "/api/backtest-entry-timing/status",
            "/api/backtest-st-proximity", "/api/backtest-st-proximity/status",
            "/api/backtest-cv-st30m", "/api/backtest-cv-st30m/status",
            "/api/backtest-st-flips", "/api/backtest-st-flips/status",
            "/api/st-flips-debug",
            "/api/backtest-triple", "/api/backtest-triple/status",
            "/api/market-phase", "/api/market-phase/history",
            "/api/entry-checker", "/api/entry-checker/ai-opinion", "/api/verified-signals",
            "/api/live/status", "/api/live/set-mode", "/api/live/set-preset",
            "/api/live/set-balance", "/api/live/enable", "/api/live/kill-switch",
            "/api/live/kill-switch/reset", "/api/live/test-connection",
            "/api/live/positions", "/api/live/history", "/api/live/close",
            "/api/live/confirm", "/api/fvg-monitor-debug", "/api/fvg-entry-alert-test", "/api/peek-tradium-setups", "/api/peek-tradium-forum", "/api/inspect-msg-neighbors", "/api/debug-fetch-chart", "/api/reversal-meter", "/api/pending-clusters", "/api/backfill-clusters", "/api/pair-signals", "/api/fvg-signals", "/api/fvg-journal", "/api/fvg-config", "/api/fvg-scan-now", "/api/fvg-candles", "/api/conflicts", "/api/conflicts/check", "/api/smart-levels", "/api/td-quota", "/api/ai-coin-analysis", "/api/top-picks", "/api/top-picks/backfill", "/api/claude-budget", "/api/tv-webhook", "/api/fvg-top-picks", "/api/fvg-rescore-all", "/api/cv-replay-last-alert", "/api/journal/by-symbol", "/api/market-events", "/api/market-events/backfill", "/api/backtest/today") or path.startswith("/static"):
            resp = await call_next(request)
            resp.headers["Cache-Control"] = "no-store"
            return resp
        token = request.cookies.get(SESSION_COOKIE)
        user = _verify_token(token)
        if user:
            resp = await call_next(request)
            # Запрещаем кеш для HTML-страниц админки
            if path == "/" or path.startswith("/signals") or path.startswith("/login"):
                resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
                resp.headers["Pragma"] = "no-cache"
            return resp
        # API → 401 JSON, HTML → redirect на /login
        accept = request.headers.get("accept", "")
        if path.startswith("/api/") or "application/json" in accept:
            return Response(
                status_code=401,
                content=json.dumps({"error": "unauthorized"}),
                media_type="application/json",
            )
        next_url = request.url.path + ("?" + request.url.query if request.url.query else "")
        return RedirectResponse(url=f"/login?next={next_url}", status_code=303)


app.add_middleware(SessionAuthMiddleware)


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, next: str = "/signals", error: str = ""):
    return templates.TemplateResponse(request, "login.html", {
        "next": next,
        "error": error,
    })


@app.post("/login")
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    next: str = Form("/signals"),
):
    if not (secrets.compare_digest(username, ADMIN_USERNAME)
            and secrets.compare_digest(password, ADMIN_PASSWORD)):
        return templates.TemplateResponse(request, "login.html", {
            "next": next, "error": "Неверный логин или пароль",
        }, status_code=401)
    resp = RedirectResponse(url=next or "/signals", status_code=303)
    is_prod = os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("PORT")
    resp.set_cookie(
        SESSION_COOKIE,
        _make_token(username),
        max_age=SESSION_TTL,
        httponly=True,
        samesite="lax",
        secure=bool(is_prod),
        path="/",
    )
    return resp


@app.get("/logout")
async def logout():
    resp = RedirectResponse(url="/login", status_code=303)
    resp.delete_cookie(SESSION_COOKIE, path="/")
    return resp


# ── Telegram auth (только статус) ────────────────────────────────────
_MAIN_SESSION = os.path.join(os.path.dirname(os.path.abspath(__file__)), "session_userbot")


@app.get("/api/tg/status")
async def tg_status():
    """Быстрый статус: проверяем наличие основного session-файла.
    Не создаём клиентов, не трогаем основной userbot."""
    try:
        main_exists = os.path.exists(_MAIN_SESSION + ".session")
        return {
            "authorized": main_exists,
            "user": {"username": "userbot", "first_name": "Tradium", "phone": "*"} if main_exists else None,
        }
    except Exception as e:
        return {"authorized": False, "error": str(e)}




# ── WebSocket manager ─────────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self._connections: Set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self._connections.add(ws)

    async def disconnect(self, ws: WebSocket):
        async with self._lock:
            self._connections.discard(ws)

    async def broadcast(self, msg: dict):
        payload = json.dumps(msg, default=str)
        dead: list[WebSocket] = []
        async with self._lock:
            conns = list(self._connections)
        for ws in conns:
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)
        if dead:
            async with self._lock:
                for ws in dead:
                    self._connections.discard(ws)


manager = ConnectionManager()


def broadcast_event(event_type: str, data: dict | None = None):
    """Синхронный хелпер для публикации из watcher/userbot.
    Планирует задачу в активном event loop, если он есть."""
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(manager.broadcast({"type": event_type, "data": data or {}}))
    except RuntimeError:
        pass  # нет running loop (тесты / отдельные скрипты)


# ── Chart image serve ───────────────────────────────────────────────────────

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _resolve_chart_path(p: str) -> str:
    if not p:
        return ""
    charts_root = os.path.realpath(os.path.join(_BASE_DIR, "charts"))
    if os.path.isabs(p):
        candidate = os.path.realpath(p)
    else:
        candidate = os.path.realpath(os.path.join(_BASE_DIR, p.lstrip("./\\")))
    # Containment: only serve files under charts_root
    try:
        if os.path.commonpath([candidate, charts_root]) != charts_root:
            return ""
    except ValueError:
        return ""
    return candidate if os.path.exists(candidate) else ""


@app.get("/chart/{signal_id}")
async def serve_chart(signal_id: int, db: Session = Depends(get_db)):
    # Cache headers — чарты неизменны после создания, кешируем на 7 дней.
    # CloudFlare/Railway CDN увидят это и закешируют на edge → быстрый ответ.
    cache_headers = {
        "Cache-Control": "public, max-age=604800, immutable",
        "CDN-Cache-Control": "public, max-age=604800",
    }
    # Сначала GridFS (переживает деплой)
    from database import get_chart
    chart_data = await asyncio.to_thread(get_chart, signal_id)
    if chart_data:
        return Response(content=chart_data, media_type="image/jpeg", headers=cache_headers)

    # Фоллбэк на локальный файл
    signal = await asyncio.to_thread(
        lambda: db.query(Signal).filter(Signal.id == signal_id).first()
    )
    if not signal or not signal.chart_path:
        raise HTTPException(status_code=404)
    resolved = _resolve_chart_path(signal.chart_path)
    if not resolved:
        raise HTTPException(status_code=404)
    return FileResponse(resolved, headers=cache_headers)


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """WebSocket авторизуется через session cookie."""
    token = ws.cookies.get(SESSION_COOKIE)
    if not _verify_token(token):
        await ws.close(code=1008)
        return

    await manager.connect(ws)
    try:
        while True:
            # Держим соединение живым; игнорируем входящие
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await manager.disconnect(ws)


@app.get("/healthz")
async def healthz():
    """Lightweight liveness probe для Docker/Railway healthcheck.
    Без I/O — мгновенный ответ. Если FastAPI живой, это вернёт 200.

    Полная диагностика с Mongo-проверками — /health (может тормозить
    при лагах Atlas, использовать для ручного мониторинга)."""
    return {"ok": True}


@app.get("/health")
async def health():
    """Healthcheck для Docker / uptime monitors — без авторизации.
    Возвращает полный статус подсистем для внешнего мониторинга."""
    from datetime import datetime, timedelta
    from database import utcnow as _utcnow
    status = {"ok": True, "checks": {}, "ts": _utcnow().isoformat()}
    # 1. DB ping
    try:
        from database import _signals
        _signals().estimated_document_count()
        status["checks"]["db"] = "ok"
    except Exception as e:
        status["checks"]["db"] = f"fail: {str(e)[:100]}"
        status["ok"] = False
    # 2. Userbot (Telethon)
    try:
        from userbot import _tg_client
        status["checks"]["userbot"] = ("connected" if _tg_client and _tg_client.is_connected()
                                        else "disconnected")
    except Exception as e:
        status["checks"]["userbot"] = f"fail: {str(e)[:100]}"
    # 3. Последние активности по коллекциям (данные не старше 4ч = система живая)
    try:
        from database import _signals, _anomalies, _confluence, _clusters
        from pymongo import DESCENDING
        now = _utcnow()
        for name, col, date_field in [
            ("signals", _signals(), "received_at"),
            ("anomalies", _anomalies(), "detected_at"),
            ("confluence", _confluence(), "detected_at"),
        ]:
            last = col.find_one({}, sort=[(date_field, DESCENDING)])
            if last and last.get(date_field):
                age_min = (now - last[date_field]).total_seconds() / 60
                status["checks"][f"{name}_last_age_min"] = int(age_min)
                if age_min > 240:  # 4ч
                    status["checks"][f"{name}_warning"] = "stale (>4h no new data)"
    except Exception as e:
        status["checks"]["activity"] = f"fail: {str(e)[:80]}"
    return status


@app.post("/api/backfill-missed")
async def api_backfill_missed(payload: dict | None = None):
    """Подтягивает пропущенные сигналы из Tradium и Cryptovizor каналов
    через живой Telethon клиент userbot. Запускается в фоновой задаче.

    Body (optional): {"hours": 24, "only": "cv|tradium|null"}
    """
    payload = payload or {}
    hours = float(payload.get("hours") or 0)
    only = payload.get("only")

    try:
        from userbot import _tg_client
    except Exception:
        _tg_client = None
    if _tg_client is None or not _tg_client.is_connected():
        return {"ok": False, "error": "Telethon client not connected"}

    # Запускаем в фоне чтобы не держать HTTP запрос
    asyncio.create_task(_run_backfill_missed(_tg_client, hours, only))
    return {"ok": True, "started": True, "hours": hours or "since-last-signal", "only": only or "both"}


async def _run_backfill_missed(client, hours: float, only: str | None):
    """Фоновая задача: подтянуть пропущенные сигналы. Использует backfill_missed функции."""
    import logging as _log
    log = _log.getLogger("backfill-missed-api")
    try:
        from datetime import datetime, timezone, timedelta
        from backfill_missed import backfill_cryptovizor, backfill_tradium, _last_signal_time

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        if hours and hours > 0:
            since_cv = since_tr = now - timedelta(hours=hours)
        else:
            last_cv = _last_signal_time("cryptovizor")
            last_tr = _last_signal_time("tradium")
            since_cv = last_cv if last_cv else now - timedelta(hours=24)
            since_tr = last_tr if last_tr else now - timedelta(hours=24)

        log.info(f"[backfill-api] CV since {since_cv}, Tradium since {since_tr}")

        cv_added = tr_added = 0
        if only != "tradium":
            try:
                cv_added = await backfill_cryptovizor(client, since_cv, hard_limit=2000)
            except Exception:
                log.exception("[backfill-api] CV failed")
        if only != "cv":
            try:
                tr_added = await backfill_tradium(client, since_tr, hard_limit=5000)
            except Exception:
                log.exception("[backfill-api] Tradium failed")

        log.info(f"[backfill-api] DONE: CV +{cv_added}, Tradium +{tr_added}")
        # Уведомление в админ-бот если есть
        try:
            from watcher import _bot, _admin_chat_id
            if _bot and _admin_chat_id:
                await _bot.send_message(
                    _admin_chat_id,
                    f"📥 <b>Backfill завершён</b>\n\n"
                    f"🚀 Cryptovizor: +{cv_added}\n"
                    f"📡 Tradium: +{tr_added}",
                    parse_mode="HTML",
                )
        except Exception:
            pass
    except Exception:
        log.exception("[backfill-api] crashed")


@app.get("/api/peek-tradium-forum")
async def api_peek_tradium_forum():
    """Проверяет форумные топики в Tradium группе."""
    try:
        from userbot import _tg_client
        from config import SOURCE_GROUP_ID
    except Exception as e:
        return {"ok": False, "error": str(e)}
    if _tg_client is None or not _tg_client.is_connected():
        return {"ok": False, "error": "Telethon not connected"}
    try:
        from telethon.tl.functions.channels import GetForumTopicsRequest
        entity = await _tg_client.get_entity(SOURCE_GROUP_ID)
        is_forum = getattr(entity, "forum", False)
        info = {
            "chat_id": SOURCE_GROUP_ID,
            "title": getattr(entity, "title", None),
            "is_forum": is_forum,
            "topics": [],
            "topic_samples": {},
        }
        if is_forum:
            try:
                result = await _tg_client(GetForumTopicsRequest(
                    channel=entity, offset_date=None, offset_id=0, offset_topic=0, limit=50
                ))
                for t in result.topics:
                    info["topics"].append({
                        "id": t.id,
                        "title": getattr(t, "title", None),
                        "top_message": t.top_message,
                    })
                for t in result.topics[:5]:
                    samples = []
                    try:
                        async for m in _tg_client.iter_messages(entity, limit=3, reply_to=t.id):
                            samples.append({
                                "id": m.id,
                                "date": m.date.isoformat() if m.date else None,
                                "text_preview": (m.raw_text or "")[:200],
                            })
                    except Exception as e:
                        samples.append({"error": str(e)})
                    info["topic_samples"][f"{t.id}:{t.title}"] = samples
            except Exception as e:
                info["forum_error"] = str(e)
        return {"ok": True, **info}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/peek-tradium-setups")
async def api_peek_tradium_setups(hours: int = 48):
    """Ищет только Tradium Setup сообщения (по тексту) за последние N часов."""
    try:
        from userbot import _tg_client
        from config import SOURCE_GROUP_ID
        from parser import parse_signal
    except Exception as e:
        return {"ok": False, "error": str(e)}
    if _tg_client is None or not _tg_client.is_connected():
        return {"ok": False, "error": "Telethon not connected"}
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
    since = _dt.now(_tz.utc) - _td(hours=hours)
    setups = []
    total = 0
    try:
        async for m in _tg_client.iter_messages(SOURCE_GROUP_ID, limit=10000):
            total += 1
            if m.date and m.date < since:
                break
            text = m.raw_text or ""
            # Фильтруем по признакам Tradium Setup
            if "Tradium Setups" not in text and "Setup Screener" not in text and "#сетап" not in text:
                continue
            parsed = parse_signal(text)
            valid = bool(parsed.get("trend") and parsed.get("tp1") and parsed.get("sl") and parsed.get("entry"))
            setups.append({
                "id": m.id,
                "date": m.date.isoformat() if m.date else None,
                "text_preview": text[:300],
                "parser_valid": valid,
                "parsed": {
                    "pair": parsed.get("pair"),
                    "direction": parsed.get("direction"),
                    "entry": parsed.get("entry"),
                    "sl": parsed.get("sl"),
                    "tp1": parsed.get("tp1"),
                    "trend": parsed.get("trend"),
                    "timeframe": parsed.get("timeframe"),
                },
            })
        return {"ok": True, "scanned_total": total, "setups_found": len(setups), "messages": setups}
    except Exception as e:
        return {"ok": False, "error": str(e), "scanned_total": total, "setups_found": len(setups), "messages": setups}


@app.get("/api/peek-tradium")
async def api_peek_tradium(limit: int = 10):
    """Показывает последние N сообщений из Tradium группы напрямую через Telethon
    (без сохранения в БД). Для диагностики: молчит группа или парсер не ловит."""
    try:
        from userbot import _tg_client
        from config import SOURCE_GROUP_ID
        from parser import parse_signal
    except Exception as e:
        return {"ok": False, "error": str(e)}
    if _tg_client is None or not _tg_client.is_connected():
        return {"ok": False, "error": "Telethon not connected"}
    try:
        out = []
        async for m in _tg_client.iter_messages(SOURCE_GROUP_ID, limit=limit):
            text = (m.raw_text or "")[:300]
            parsed = parse_signal(text) if text else {}
            valid = bool(parsed.get("trend") and parsed.get("tp1") and parsed.get("sl") and parsed.get("entry"))
            out.append({
                "id": m.id,
                "date": m.date.isoformat() if m.date else None,
                "text_preview": text[:200],
                "has_media": m.media is not None,
                "parser_valid_signal": valid,
                "parsed_pair": parsed.get("pair"),
            })
        return {"ok": True, "source_group_id": SOURCE_GROUP_ID, "count": len(out), "messages": out}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# Серверный кеш для /api/key-levels/recent (TTL 30с)
_kl_recent_cache: dict = {}
_KL_RECENT_TTL = 30.0


@app.get("/api/key-levels/recent")
async def api_key_levels_recent(pair: str, hours: int = 48):
    """Список активных Key Levels по паре для отрисовки зон на графиках.
    Кеширование: 30с на сервере (много графиков на странице → один запрос)."""
    from key_levels import get_recent_levels
    key = f"{pair}_{hours}"
    now = time.time()
    hit = _kl_recent_cache.get(key)
    if hit and (now - hit[0]) < _KL_RECENT_TTL:
        return {"pair": pair, "hours": hours, "count": len(hit[1]), "items": hit[1], "cached": True}
    items = await asyncio.to_thread(get_recent_levels, pair, hours)
    _kl_recent_cache[key] = (now, items)
    # Чистим старые записи (lazy eviction)
    if len(_kl_recent_cache) > 500:
        for k in [k for k, v in _kl_recent_cache.items() if (now - v[0]) > _KL_RECENT_TTL * 2]:
            _kl_recent_cache.pop(k, None)
    return {"pair": pair, "hours": hours, "count": len(items), "items": items}


@app.post("/api/key-levels/enrich")
async def api_key_levels_enrich(payload: dict):
    """Батч-обогащение списка сигналов — для таблиц UI.
    Оптимизация: ОДИН mongo-запрос на уникальную пару (вместо N запросов),
    потом для каждого сигнала фильтруем in-memory по ±2h окну.
    payload: {"signals": [{id, pair, direction, at}, ...]}
    Возвращает: {"enrich": {id1: {emoji, label, ...}, id2: null}}
    Кеш 30с по хешу payload — UI дергает этот endpoint после каждого
    /api/journal (polling), а ответ меняется редко."""
    from key_levels import _tf_power, ENRICH_WINDOW_H
    from database import _key_levels
    from datetime import datetime as _dt, timedelta as _td
    from cache_utils import kl_enrich_cache
    import hashlib as _hl, json as _json
    signals = (payload or {}).get("signals", [])

    # Кеш-ключ = хеш сигналов (id+pair+direction+at); при тех же сигналах ответ одинаковый
    try:
        _key_material = _json.dumps(
            [(str(s.get("id")), s.get("pair") or s.get("symbol"),
              s.get("direction"), s.get("at")) for s in signals],
            sort_keys=True, default=str,
        )
        _cache_key = _hl.md5(_key_material.encode()).hexdigest()
        _cached = kl_enrich_cache.get(_cache_key)
        if _cached is not None:
            return _cached
    except Exception:
        _cache_key = None
    # 1) Парсим входные сигналы в нормализованную форму
    norm = []
    pair_times: dict[str, list[_dt]] = {}
    for s in signals:
        sig_id = s.get("id")
        pair_raw = s.get("pair") or s.get("symbol") or ""
        direction = (s.get("direction") or "").upper()
        at_raw = s.get("at")
        at = None
        if at_raw:
            try:
                if isinstance(at_raw, (int, float)):
                    at = _dt.utcfromtimestamp(at_raw)
                else:
                    at = _dt.fromisoformat(str(at_raw).replace("Z", "+00:00"))
                    if at.tzinfo:
                        at = at.replace(tzinfo=None)
            except Exception:
                at = None
        pair_norm = pair_raw.replace("/", "").upper()
        if pair_norm and not pair_norm.endswith("USDT"):
            pair_norm = pair_norm + "USDT"
        norm.append((sig_id, pair_norm, direction, at))
        if pair_norm and at:
            pair_times.setdefault(pair_norm, []).append(at)

    # 2) По каждой паре — один запрос: все KL в широком окне [min(at)-W, max(at)+W]
    def _fetch_pair(pair_norm: str, times: list[_dt]):
        start = min(times) - _td(hours=ENRICH_WINDOW_H)
        end = max(times) + _td(hours=ENRICH_WINDOW_H)
        return pair_norm, list(_key_levels().find({
            "pair_norm": pair_norm,
            "detected_at": {"$gte": start, "$lte": end},
        }, {"event": 1, "tf": 1, "age_days": 1, "zone_low": 1,
            "zone_high": 1, "detected_at": 1, "current_price": 1}))

    pair_cache: dict[str, list[dict]] = {}
    if pair_times:
        # Параллельно через thread-пул (MongoDB driver синхронный, но IO-bound)
        results = await asyncio.gather(*[
            asyncio.to_thread(_fetch_pair, p, times) for p, times in pair_times.items()
        ])
        for pair_norm, items in results:
            pair_cache[pair_norm] = items

    # 3) Для каждого сигнала — выбираем лучший KL из pair_cache по ±2h окну
    def _pick_best(pair_norm: str, direction: str, at: _dt):
        cands = pair_cache.get(pair_norm, [])
        if not cands:
            return None
        lo = at - _td(hours=ENRICH_WINDOW_H)
        hi = at + _td(hours=ENRICH_WINDOW_H)
        filtered = [k for k in cands if k.get("detected_at")
                    and lo <= k["detected_at"] <= hi]
        if not filtered:
            return None
        is_long = direction in ("LONG", "BUY", "BULLISH")
        strong, warning, confirming, range_ev = [], [], [], []
        for kl in filtered:
            ev = kl.get("event", "")
            if is_long:
                if ev == "entered_resistance":
                    strong.append((kl, "🎢", "Breakout UP", "strong"))
                elif ev == "entered_support":
                    warning.append((kl, "🔪", "Falling knife через SUPPORT", "warning"))
                elif ev == "new_support":
                    confirming.append((kl, "⚓", "Новая SUPPORT снизу", "confirming"))
                elif ev == "new_resistance":
                    warning.append((kl, "🔪", "Новая RESISTANCE сверху (риск)", "warning"))
                elif ev.startswith("range_"):
                    range_ev.append((kl, "〰️", f"In range ({ev.replace('range_', '')})", "neutral"))
            else:
                if ev == "entered_support":
                    strong.append((kl, "🧨", "Breakdown DOWN", "strong"))
                elif ev == "entered_resistance":
                    warning.append((kl, "🔪", "Против тренда через RESISTANCE", "warning"))
                elif ev == "new_resistance":
                    confirming.append((kl, "⚓", "Новая RESISTANCE сверху", "confirming"))
                elif ev == "new_support":
                    warning.append((kl, "🔪", "Новая SUPPORT снизу (риск)", "warning"))
                elif ev.startswith("range_"):
                    range_ev.append((kl, "〰️", f"In range ({ev.replace('range_', '')})", "neutral"))

        def best_of(lst):
            if not lst:
                return None
            rank = {"strong": 4, "warning": 3, "confirming": 2, "neutral": 1}
            lst.sort(key=lambda x: (rank.get(x[3], 0), _tf_power(x[0].get("tf", "")), x[0].get("detected_at")), reverse=True)
            return lst[0]

        chosen = best_of(strong) or best_of(warning) or best_of(confirming) or best_of(range_ev)
        if not chosen:
            return None
        kl, emoji, label_prefix, strength = chosen
        tf = kl.get("tf", "?")
        age = kl.get("age_days")
        age_str = f", age {age}d" if age else ""
        kt = kl.get("detected_at")
        return {
            "emoji": emoji,
            "label": f"{label_prefix} {tf}{age_str}",
            "strength": strength,
            "event": kl.get("event"),
            "tf": tf,
            "age_days": age,
            "zone_low": kl.get("zone_low"),
            "zone_high": kl.get("zone_high"),
            "kl_time": kt.isoformat() if hasattr(kt, "isoformat") else None,
            "current_price_at_kl": kl.get("current_price"),
        }

    out = {}
    for sig_id, pair_norm, direction, at in norm:
        if sig_id is None:
            continue
        enrich = _pick_best(pair_norm, direction, at) if (pair_norm and at) else None
        out[str(sig_id)] = enrich
    resp = {"enrich": out, "count": len(out), "pairs": len(pair_cache)}
    if _cache_key:
        try:
            kl_enrich_cache.set(_cache_key, resp)
        except Exception:
            pass
    return resp


@app.get("/api/key-levels/stats")
async def api_key_levels_stats():
    """Количество записей в БД по типам."""
    from database import _key_levels, utcnow as _unow
    from datetime import timedelta as _td
    from collections import Counter
    col = _key_levels()
    total = col.count_documents({})
    since24h = _unow() - _td(hours=24)
    since7d = _unow() - _td(days=7)
    by_event = Counter()
    for kl in col.find({}, {"event": 1}):
        by_event[kl.get("event", "?")] += 1
    return {
        "total": total,
        "last_24h": col.count_documents({"detected_at": {"$gte": since24h}}),
        "last_7d": col.count_documents({"detected_at": {"$gte": since7d}}),
        "by_event": dict(by_event.most_common()),
    }


# Глобальный стейт прогресса KL backfill (не блокирует контейнер)
_kl_backfill_state: dict = {"running": False, "started_at": None, "finished_at": None, "stats": None, "error": None}


async def _run_kl_backfill(limit: int):
    """Фоновая задача: тянет KL-сообщения из Telegram, парсит и пишет в MongoDB.
    Прогресс пишется в _kl_backfill_state."""
    from key_levels import parse_key_level
    from database import _key_levels, utcnow as _unow
    from datetime import datetime as _dt
    TOPICS = {3086: "SUPPORT", 3088: "RANGES", 3091: "RESISTANCE"}
    try:
        from userbot import _tg_client
        from config import SOURCE_GROUP_ID
    except Exception as e:
        _kl_backfill_state["error"] = f"import: {e}"
        _kl_backfill_state["running"] = False
        _kl_backfill_state["finished_at"] = _dt.utcnow().isoformat()
        return
    if _tg_client is None or not _tg_client.is_connected():
        _kl_backfill_state["error"] = "Telethon not connected"
        _kl_backfill_state["running"] = False
        _kl_backfill_state["finished_at"] = _dt.utcnow().isoformat()
        return
    stats = {"fetched": 0, "parsed": 0, "saved": 0, "by_event": {}, "by_topic": {}}
    _kl_backfill_state["stats"] = stats
    try:
        for tid, name in TOPICS.items():
            count = 0
            async for m in _tg_client.iter_messages(SOURCE_GROUP_ID, limit=limit, reply_to=tid):
                stats["fetched"] += 1
                count += 1
                if not m.raw_text:
                    continue
                parsed = parse_key_level(m.raw_text, topic_id=tid)
                if not parsed:
                    continue
                stats["parsed"] += 1
                ev = parsed["event"]
                stats["by_event"][ev] = stats["by_event"].get(ev, 0) + 1
                try:
                    existing = _key_levels().find_one({"message_id": m.id})
                    if existing:
                        continue
                    _key_levels().insert_one({
                        **parsed,
                        "detected_at": m.date.replace(tzinfo=None) if m.date else _unow(),
                        "message_id": m.id,
                        "backfilled": True,
                    })
                    stats["saved"] += 1
                except Exception:
                    pass
                # Кооперативная уступка event-loop'у каждые 50 сообщений,
                # чтобы /healthz и другие эндпоинты отвечали
                if stats["fetched"] % 50 == 0:
                    await asyncio.sleep(0)
            stats["by_topic"][f"{tid}_{name}"] = count
    except Exception as e:
        import traceback
        _kl_backfill_state["error"] = f"{e}\n{traceback.format_exc()[-500:]}"
    finally:
        _kl_backfill_state["running"] = False
        _kl_backfill_state["finished_at"] = _dt.utcnow().isoformat()


@app.post("/api/key-levels/backfill")
async def api_key_levels_backfill(payload: dict | None = None):
    """Запускает фоновый бэкфилл KL. Возвращает сразу — прогресс смотреть через
    /api/key-levels/backfill-status.
    payload: {"limit_per_topic": 2000}"""
    from datetime import datetime as _dt
    if _kl_backfill_state.get("running"):
        return {"ok": False, "error": "already running", "state": _kl_backfill_state}
    limit = int((payload or {}).get("limit_per_topic", 2000))
    _kl_backfill_state.update({
        "running": True,
        "started_at": _dt.utcnow().isoformat(),
        "finished_at": None,
        "stats": {"fetched": 0, "parsed": 0, "saved": 0, "by_event": {}, "by_topic": {}},
        "error": None,
        "limit_per_topic": limit,
    })
    asyncio.create_task(_run_kl_backfill(limit))
    return {"ok": True, "started": True, "limit_per_topic": limit}


@app.get("/api/key-levels/backfill-status")
async def api_key_levels_backfill_status():
    return _kl_backfill_state


# ─── Бектест SuperTrend стратегий ───────────────────────────────
_st_backtest_state: dict = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "progress": None,  # {processed, total, current_pair}
    "result": None,
    "error": None,
}


async def _run_st_backtest(days: int, top_n: int):
    from datetime import datetime as _dt
    from backtest_supertrend import run_backtest

    def _on_progress(i, total, pair):
        _st_backtest_state["progress"] = {
            "processed": i, "total": total, "current_pair": pair,
        }

    try:
        result = await asyncio.to_thread(run_backtest, days, top_n, _on_progress)
        _st_backtest_state["result"] = result
    except Exception as e:
        import traceback
        _st_backtest_state["error"] = f"{e}\n{traceback.format_exc()[-800:]}"
    finally:
        _st_backtest_state["running"] = False
        _st_backtest_state["finished_at"] = _dt.utcnow().isoformat()


@app.post("/api/backtest-st")
async def api_backtest_st(payload: dict | None = None):
    """Запускает фоновый бектест ST-стратегий.
    payload: {"days": 14, "top_n": 200}
    Прогресс через /api/backtest-st/status"""
    from datetime import datetime as _dt
    if _st_backtest_state.get("running"):
        return {"ok": False, "error": "already running", "state": _st_backtest_state}
    days = int((payload or {}).get("days", 14))
    top_n = int((payload or {}).get("top_n", 200))
    _st_backtest_state.update({
        "running": True,
        "started_at": _dt.utcnow().isoformat(),
        "finished_at": None,
        "progress": {"processed": 0, "total": 0, "current_pair": None},
        "result": None,
        "error": None,
        "days": days,
        "top_n": top_n,
    })
    asyncio.create_task(_run_st_backtest(days, top_n))
    return {"ok": True, "started": True, "days": days, "top_n": top_n}


@app.get("/api/backtest-st/status")
async def api_backtest_st_status():
    return _st_backtest_state


# ─── SuperTrend Signals (VIP / Triple MTF / Daily) ───────────────
@app.get("/api/supertrend-signals")
async def api_supertrend_signals(tier: str = "", pair: str = "",
                                 hours: int = 336, limit: int = 300):
    """Список ST-сигналов для UI-таблиц.
    Фильтры: tier (vip/mtf/daily), pair, hours (окно от текущего времени)."""
    from database import _supertrend_signals, utcnow
    from datetime import timedelta
    since = utcnow() - timedelta(hours=hours)
    query: dict = {"flip_at": {"$gte": since}}
    if tier:
        query["tier"] = tier
    if pair:
        p = pair.replace("/", "").upper()
        if not p.endswith("USDT"): p = p + "USDT"
        query["pair_norm"] = p
    items = []
    for doc in _supertrend_signals().find(query).sort("flip_at", -1).limit(limit):
        doc.pop("_id", None)
        for k in ("flip_at", "created_at"):
            v = doc.get(k)
            if hasattr(v, "isoformat"):
                doc[k] = v.isoformat()
        # aligned_bots may have datetime inside
        for ab in doc.get("aligned_bots", []):
            v = ab.get("at")
            if hasattr(v, "isoformat"):
                ab["at"] = v.isoformat()
        items.append(doc)
    return {"ok": True, "count": len(items), "items": items,
            "tier": tier, "pair": pair, "hours": hours}


def _cv_flip_backfill_sync(hours: int = 48) -> dict:
    """Одноразовый backfill cv_flip_signals за последние N часов.

    Для каждого CV signal (source=cryptovizor, любой):
      1. Upsert WAITING-дубль (если ещё нет), cv_triggered_at = received_at
         — т.е. следим за любым сообщением из CV-бота, не ждём pattern_triggered.
      2. Грузит 30m свечи пары и ищет flip ST(7, 2.5) в сторону CV
         (MIN_BARS_UNDER_ST=1, timeout 24ч от received_at) — тот же
         алгоритм что в cv_flip_watcher.
      3. Финализирует state: FLIPPED / TIMEOUT / INVALIDATED / WAITING.
    """
    from database import _signals, _cv_flip_signals, utcnow
    from exchange import get_klines_any
    from backtest_supertrend import compute_st_series
    from datetime import timedelta
    import datetime as _dt

    hours = max(1, min(hours, 168))
    ST_PERIOD, ST_MULT = 7, 2.5
    MIN_BARS_UNDER_ST = 1
    TIMEOUT_H = 24
    since = utcnow() - timedelta(hours=hours)
    now = utcnow()

    cv_col = _signals()
    dup_col = _cv_flip_signals()

    cv_list = list(cv_col.find({
        "source": "cryptovizor",
        "received_at": {"$gte": since},
    }, {
        "_id": 1, "pair": 1, "direction": 1, "pattern_name": 1,
        "received_at": 1,
    }).sort("received_at", 1))

    created = updated = skipped = 0
    counters = {"WAITING": 0, "FLIPPED": 0, "TIMEOUT": 0, "INVALIDATED": 0}
    candle_cache: dict = {}

    def _fetch(pair: str):
        if pair in candle_cache:
            return candle_cache[pair]
        try:
            c = get_klines_any(pair, "30m", 500) or []
        except Exception:
            c = []
        candle_cache[pair] = c
        return c

    for cv in cv_list:
        sid = str(cv["_id"])
        direction = (cv.get("direction") or "").upper()
        pair = cv.get("pair") or ""
        cv_at = cv.get("received_at")
        if direction not in ("LONG", "SHORT") or not pair or cv_at is None:
            skipped += 1
            continue
        want_trend = 1 if direction == "LONG" else -1
        opp_trend = -want_trend

        existing = dup_col.find_one({"cv_signal_id": sid})
        if not existing:
            dup_col.insert_one({
                "cv_signal_id": sid, "pair": pair, "direction": direction,
                "cv_triggered_at": cv_at,
                "cv_pattern_name": cv.get("pattern_name") or "",
                "state": "WAITING", "flip_at": None, "flip_price": None,
                "bars_under_st": 0, "st_tf": "30m",
                "st_params": {"period": ST_PERIOD, "mult": ST_MULT},
                "timeout_h": TIMEOUT_H,
                "created_at": now, "updated_at": now,
                "source": "cv_flip",
            })
            created += 1
            existing = dup_col.find_one({"cv_signal_id": sid})
        doc_id = existing["_id"]

        if existing.get("state") in ("FLIPPED", "TIMEOUT", "INVALIDATED"):
            counters[existing["state"]] += 1
            continue

        newer = cv_col.find_one({
            "source": "cryptovizor",
            "pair": pair, "received_at": {"$gt": cv_at},
        }, {"_id": 1})
        if newer:
            dup_col.update_one({"_id": doc_id},
                               {"$set": {"state": "INVALIDATED", "updated_at": now}})
            counters["INVALIDATED"] += 1
            updated += 1
            continue

        candles = _fetch(pair)
        if not candles or len(candles) < ST_PERIOD + 5:
            counters["WAITING"] += 1
            continue
        closed = candles[:-1]
        try:
            st_series = compute_st_series(closed, ST_PERIOD, ST_MULT)
        except Exception:
            counters["WAITING"] += 1
            continue

        cv_ms = int((cv_at if cv_at.tzinfo else cv_at.replace(tzinfo=_dt.timezone.utc))
                    .timestamp() * 1000)
        timeout_ms = cv_ms + TIMEOUT_H * 3600 * 1000
        first_idx = None
        for i, c in enumerate(closed):
            if c["t"] >= cv_ms:
                first_idx = i
                break
        if first_idx is None:
            counters["WAITING"] += 1
            continue

        flip_idx = None
        for i in range(max(first_idx + 1, 1), len(st_series)):
            if closed[i]["t"] > timeout_ms:
                break
            curr = st_series[i].get("trend")
            prev = st_series[i - 1].get("trend")
            if curr == want_trend and prev == opp_trend:
                bars_before = sum(
                    1 for j in range(first_idx, i)
                    if st_series[j].get("trend") == opp_trend
                )
                if bars_before >= MIN_BARS_UNDER_ST:
                    flip_idx = i
                    break

        if flip_idx is not None:
            flip_bar = closed[flip_idx]
            flip_price = float(flip_bar["c"])
            flip_at = _dt.datetime.utcfromtimestamp(
                (int(flip_bar["t"]) + 30 * 60 * 1000) / 1000.0
            )
            st_val = float(st_series[flip_idx].get("st") or 0.0)
            if st_val <= 0 or (direction == "LONG" and st_val >= flip_price) \
                    or (direction == "SHORT" and st_val <= flip_price):
                st_val = flip_price * (0.99 if direction == "LONG" else 1.01)
            sign = 1 if direction == "LONG" else -1
            risk = abs(flip_price - st_val)
            entry = round(flip_price, 8); sl = round(st_val, 8)
            tp1 = round(flip_price + sign * 1 * risk, 8)
            tp2 = round(flip_price + sign * 2 * risk, 8)
            tp3 = round(flip_price + sign * 3 * risk, 8)
            risk_pct = (risk / flip_price) * 100.0 if flip_price else 0.0
            dup_col.update_one({"_id": doc_id}, {"$set": {
                "state": "FLIPPED", "flip_at": flip_at, "flip_price": flip_price,
                "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
                "st_value_at_flip": st_val,
                "risk_pct": round(risk_pct, 3),
                "updated_at": now,
            }})
            counters["FLIPPED"] += 1
            updated += 1
        else:
            age_h = (now - cv_at).total_seconds() / 3600.0
            if age_h > TIMEOUT_H:
                dup_col.update_one({"_id": doc_id},
                                   {"$set": {"state": "TIMEOUT", "updated_at": now}})
                counters["TIMEOUT"] += 1
                updated += 1
            else:
                bars_before = sum(
                    1 for j in range(first_idx, len(st_series))
                    if st_series[j].get("trend") == opp_trend
                )
                dup_col.update_one({"_id": doc_id}, {"$set": {
                    "bars_under_st": bars_before, "updated_at": now,
                }})
                counters["WAITING"] += 1

    return {
        "ok": True, "hours": hours, "cv_total": len(cv_list),
        "created": created, "updated": updated, "skipped": skipped,
        "states": counters,
    }


@app.post("/api/cv-flip-backfill")
async def api_cv_flip_backfill(payload: dict | None = None):
    """Backfill cv_flip_signals за последние N часов (POST).
    Параметры: {"hours": 48} — default 48ч. Max 168ч (7 дней)."""
    hours = 48
    if payload and isinstance(payload.get("hours"), int):
        hours = max(1, min(payload["hours"], 168))
    return await asyncio.to_thread(_cv_flip_backfill_sync, hours)


def _cv_flip_results_sync(since_hours: int = 24) -> dict:
    """Результаты отработки FLIPPED cv_flip_signals за последние N часов.

    У FLIPPED записи уже сохранены entry/sl/tp1/tp2/tp3. Мы грузим 30m свечи
    от flip_at до now (макс +48ч) и смотрим что сработало первым:
    SL (-1R), TP1 (+1R), TP2 (+2R), TP3 (+3R), или ещё OPEN.

    Pessimistic на одном баре: SL приоритетнее TP (классика для R-бэктестов).
    """
    from database import _cv_flip_signals, utcnow
    from exchange import get_klines_any
    from datetime import timedelta

    since_hours = max(1, min(since_hours, 168))
    MAX_HOLD_H = 48
    since = utcnow() - timedelta(hours=since_hours)

    docs = list(_cv_flip_signals().find({
        "state": "FLIPPED",
        "flip_at": {"$gte": since},
    }).sort("flip_at", -1))

    trades = []
    for d in docs:
        pair = d.get("pair") or ""
        direction = d.get("direction") or ""
        entry = d.get("entry") or d.get("flip_price")
        sl = d.get("sl")
        tp1, tp2, tp3 = d.get("tp1"), d.get("tp2"), d.get("tp3")
        flip_at = d.get("flip_at")
        if not (pair and direction and entry and sl and tp1 and tp2 and tp3 and flip_at):
            continue
        try:
            candles = get_klines_any(pair, "30m", 200) or []
        except Exception:
            candles = []
        if not candles:
            continue
        import datetime as _dt
        flip_ms = int((flip_at if flip_at.tzinfo else flip_at.replace(tzinfo=_dt.timezone.utc))
                      .timestamp() * 1000)
        hold_ms = MAX_HOLD_H * 3600 * 1000
        outcome = "OPEN"
        exit_price = None
        r = 0.0
        hit_tp = 0
        for c in candles:
            t = int(c["t"])
            if t <= flip_ms:
                continue
            if t > flip_ms + hold_ms:
                break
            hi, lo = float(c["h"]), float(c["l"])
            if direction == "LONG":
                if lo <= sl:
                    outcome = "SL"; exit_price = sl; r = -1.0; break
                if hi >= tp3:
                    outcome = "TP3"; exit_price = tp3; r = 3.0; hit_tp = 3; break
                if hi >= tp2: hit_tp = max(hit_tp, 2)
                elif hi >= tp1: hit_tp = max(hit_tp, 1)
            else:  # SHORT
                if hi >= sl:
                    outcome = "SL"; exit_price = sl; r = -1.0; break
                if lo <= tp3:
                    outcome = "TP3"; exit_price = tp3; r = 3.0; hit_tp = 3; break
                if lo <= tp2: hit_tp = max(hit_tp, 2)
                elif lo <= tp1: hit_tp = max(hit_tp, 1)
        if outcome == "OPEN" and hit_tp > 0:
            outcome = f"TP{hit_tp}"
            exit_price = {1: tp1, 2: tp2}.get(hit_tp, tp3)
            r = float(hit_tp)
        now = utcnow()
        age_h = (now - flip_at).total_seconds() / 3600.0 if isinstance(flip_at, _dt.datetime) else 0
        trades.append({
            "pair": pair,
            "direction": direction,
            "entry": round(entry, 8),
            "sl": round(sl, 8),
            "tp1": round(tp1, 8),
            "tp3": round(tp3, 8),
            "exit_price": round(exit_price, 8) if exit_price else None,
            "outcome": outcome,
            "r": round(r, 2),
            "flip_at": flip_at.isoformat() + "Z" if hasattr(flip_at, "isoformat") else str(flip_at),
            "age_hours": round(age_h, 1),
        })

    # Summary
    closed = [t for t in trades if t["outcome"] != "OPEN"]
    wins = [t for t in closed if t["r"] > 0]
    losses = [t for t in closed if t["r"] < 0]
    sum_r = sum(t["r"] for t in closed)
    wr = (100.0 * len(wins) / len(closed)) if closed else 0.0
    avg_r = (sum_r / len(closed)) if closed else 0.0
    by_outcome: dict = {}
    for t in trades:
        by_outcome[t["outcome"]] = by_outcome.get(t["outcome"], 0) + 1
    return {
        "ok": True,
        "since_hours": since_hours,
        "flipped_total": len(docs),
        "resolved": len(trades),
        "closed": len(closed),
        "open": len([t for t in trades if t["outcome"] == "OPEN"]),
        "wins": len(wins),
        "losses": len(losses),
        "winrate_pct": round(wr, 1),
        "sum_r": round(sum_r, 2),
        "avg_r": round(avg_r, 3),
        "by_outcome": by_outcome,
        "trades": trades,
    }


@app.get("/api/cv-flip-results")
async def api_cv_flip_results(since_hours: int = 24):
    """Бэктест отработанных FLIPPED сигналов за N часов (default 24 = сегодня).
    Показывает что сработало первым: SL / TP1 / TP2 / TP3 / OPEN."""
    return await asyncio.to_thread(_cv_flip_results_sync, since_hours)


@app.get("/api/cv-flips")
async def api_cv_flips(state: str = "all", pair: str = "",
                       hours: int = 72, limit: int = 500):
    """CV+ST Flip observation feed для journal.

    state ∈ {all, WAITING, FLIPPED, TIMEOUT, INVALIDATED}
    hours — окно от cv_triggered_at
    """
    from database import _cv_flip_signals, utcnow
    from datetime import timedelta
    since = utcnow() - timedelta(hours=hours)
    query: dict = {"cv_triggered_at": {"$gte": since}}
    if state and state.lower() != "all":
        query["state"] = state.upper()
    if pair:
        p = pair.replace("/", "").upper()
        if p.endswith("USDT") and "/" not in pair:
            # пользователь передал как 'BTCUSDT'; наш pair хранится 'BTC/USDT'
            base = p[:-4]
            query["$or"] = [{"pair": pair}, {"pair": f"{base}/USDT"}]
        else:
            query["pair"] = pair
    items = []
    for doc in _cv_flip_signals().find(query).sort("cv_triggered_at", -1).limit(limit):
        doc["_id"] = str(doc.get("_id"))
        for k in ("cv_triggered_at", "flip_at", "created_at", "updated_at"):
            v = doc.get(k)
            if hasattr(v, "isoformat"):
                doc[k] = v.isoformat()
        items.append(doc)
    return {"ok": True, "count": len(items), "items": items,
            "state": state, "pair": pair, "hours": hours}


# Серверный кеш для /api/supertrend-signals/by-pair (TTL 60с)
# Снижает нагрузку при смене TF и клике по разным графикам одной пары.
_st_by_pair_cache: dict = {}
_ST_BY_PAIR_TTL = 60.0


@app.get("/api/supertrend-signals/by-pair")
async def api_supertrend_signals_by_pair(pair: str, hours: int = 336):
    """Сигналы по одной паре — для рисования маркеров на графиках.
    Кеш 60с — ST сигналы редкие, не меняются при смене TF на графике.

    Дедупликация: в окне 30 минут на одной паре+direction оставляем только
    один сигнал с высшим tier (vip > mtf > daily). Раньше на графике
    появлялась стопка 3-5 одинаковых 🏆 маркеров если VIP/MTF/Daily
    сработали почти одновременно.
    """
    key = f"{pair}|{hours}"
    now = time.time()
    hit = _st_by_pair_cache.get(key)
    if hit and (now - hit[0]) < _ST_BY_PAIR_TTL:
        return hit[1]
    resp = await api_supertrend_signals(tier="", pair=pair, hours=hours, limit=100)

    # Dedupe по окну (direction, 30min bucket) — оставляем высший tier
    tier_prio = {"vip": 3, "mtf": 2, "daily": 1}
    best: dict = {}
    for item in resp.get("items", []):
        flip_at = item.get("flip_at")
        if not flip_at:
            continue
        try:
            from datetime import datetime as _dt
            ts = _dt.fromisoformat(flip_at.replace("Z", "+00:00")).timestamp() \
                 if isinstance(flip_at, str) else flip_at.timestamp()
        except Exception:
            continue
        bucket = int(ts // 1800)  # 30-мин bucket
        key2 = (item.get("direction"), bucket)
        prio = tier_prio.get(item.get("tier"), 0)
        ex = best.get(key2)
        if (not ex) or prio > tier_prio.get(ex.get("tier"), 0):
            best[key2] = item
    deduped = sorted(best.values(), key=lambda x: x.get("flip_at", ""), reverse=True)
    resp = {**resp, "items": deduped, "count": len(deduped), "deduped": True}

    _st_by_pair_cache[key] = (now, resp)
    if len(_st_by_pair_cache) > 300:
        for k in [k for k, v in _st_by_pair_cache.items() if (now - v[0]) > _ST_BY_PAIR_TTL * 2]:
            _st_by_pair_cache.pop(k, None)
    return resp


@app.get("/api/supertrend-stats")
async def api_supertrend_stats(days: int = 14):
    """Агрегированная статистика по tiers (count + распределение по LONG/SHORT)."""
    from database import _supertrend_signals, utcnow
    from datetime import timedelta
    since = utcnow() - timedelta(days=days)
    pipeline = [
        {"$match": {"flip_at": {"$gte": since}}},
        {"$group": {
            "_id": {"tier": "$tier", "direction": "$direction"},
            "n": {"$sum": 1},
        }},
    ]
    by_tier: dict = {"vip": {"LONG": 0, "SHORT": 0, "total": 0},
                     "mtf": {"LONG": 0, "SHORT": 0, "total": 0},
                     "daily": {"LONG": 0, "SHORT": 0, "total": 0}}
    try:
        for row in _supertrend_signals().aggregate(pipeline):
            k = row["_id"]
            t, d, n = k.get("tier"), k.get("direction"), row["n"]
            if t in by_tier and d in ("LONG", "SHORT"):
                by_tier[t][d] = n
                by_tier[t]["total"] += n
    except Exception as e:
        return {"ok": False, "error": str(e)}
    return {"ok": True, "days": days, "by_tier": by_tier}


@app.post("/api/st-enrich")
async def api_st_enrich(payload: dict):
    """Батч-обогащение списка сигналов ST-флагами (для badges в журнале).
    payload: {"signals": [{id, pair, direction, at}, ...]}
    Возвращает: {enrich: {id: {flags: [...]}}}

    Оптимизированная версия: читает ТОЛЬКО из _supertrend_signals collection,
    без fetch свечей с Binance. Tracker loop каждые 5 мин наполняет БД —
    значит для любого сигнала в журнале мы можем найти последний ST-флип ДО
    времени сигнала и вывести флаги по его tier.

    Флаги:
      aligned_1h: последний ST-флип ДО at — в том же направлении (± окно 6ч)
      aligned_4h: последний flip имел tier=mtf (значит 4h тоже aligned)
      aligned_d:  flip имел tier=mtf или daily (1d aligned)
      fresh_flip: последний flip ≤3 часа назад от сигнала
      vip_match:  в ±2ч окне есть VIP ST-сигнал same direction
    """
    from database import _supertrend_signals
    from datetime import datetime as _dt, timedelta as _td
    from collections import defaultdict

    signals = (payload or {}).get("signals", [])
    by_pair: dict[str, list[dict]] = defaultdict(list)
    for s in signals:
        sig_id = s.get("id")
        if sig_id is None:
            continue
        pair = s.get("pair") or s.get("symbol") or ""
        pair_norm = pair.replace("/", "").upper()
        if not pair_norm.endswith("USDT"):
            pair_norm = pair_norm + "USDT"
        direction = (s.get("direction") or "").upper()
        at_raw = s.get("at")
        at_dt = None
        if at_raw:
            try:
                if isinstance(at_raw, (int, float)):
                    at_dt = _dt.utcfromtimestamp(at_raw)
                else:
                    at_dt = _dt.fromisoformat(str(at_raw).replace("Z", "+00:00"))
                    if at_dt.tzinfo:
                        at_dt = at_dt.replace(tzinfo=None)
            except Exception:
                at_dt = None
        if not pair_norm or not at_dt or direction not in ("LONG", "SHORT"):
            continue
        by_pair[pair_norm].append({"id": sig_id, "direction": direction, "at": at_dt})

    if not by_pair:
        return {"enrich": {}, "count": 0}

    out: dict[str, dict] = {}

    def _process_pair_sync(pair_norm: str, sigs: list[dict]) -> dict:
        """Один Mongo запрос на все ST-флипы по паре в широком окне, потом
        in-memory фильтр для каждого сигнала."""
        local: dict[str, dict] = {}
        # Окно = min(at) - 12ч … max(at) + 2ч (чтобы поймать последний флип ДО)
        min_at = min(s["at"] for s in sigs) - _td(hours=12)
        max_at = max(s["at"] for s in sigs) + _td(hours=2)
        flips = list(_supertrend_signals().find({
            "pair_norm": pair_norm,
            "flip_at": {"$gte": min_at, "$lte": max_at},
        }, {"direction": 1, "tier": 1, "flip_at": 1}).sort("flip_at", 1))

        for s in sigs:
            sid = str(s["id"])
            flags: list[str] = []
            at = s["at"]
            is_long = s["direction"] == "LONG"
            # 1. Найти последний flip ДО at (и в том же направлении)
            last_flip = None
            for f in flips:
                fa = f.get("flip_at")
                if not fa or fa > at:
                    break
                if f.get("direction") == s["direction"]:
                    last_flip = f
            if last_flip:
                age_h = (at - last_flip["flip_at"]).total_seconds() / 3600.0
                # aligned_1h если flip в ту же сторону и в разумном окне (≤24h)
                if age_h <= 24:
                    flags.append("aligned_1h")
                    if age_h <= 3:
                        flags.append("fresh_flip")
                    tier = last_flip.get("tier")
                    # mtf означает что 4h и 1d тоже были aligned на момент flip
                    if tier == "mtf":
                        flags.append("aligned_4h")
                        flags.append("aligned_d")
                    elif tier == "daily":
                        flags.append("aligned_d")
            # 2. VIP match — есть ли VIP ST-signal ±2ч
            for f in flips:
                if f.get("tier") != "vip":
                    continue
                if f.get("direction") != s["direction"]:
                    continue
                fa = f.get("flip_at")
                if not fa:
                    continue
                if abs((fa - at).total_seconds()) <= 2 * 3600:
                    flags.append("vip_match")
                    break
            local[sid] = {"flags": flags}
        return local

    # Параллелим по парам (чисто Mongo — очень быстро)
    results = await asyncio.gather(*[
        asyncio.to_thread(_process_pair_sync, p, sigs) for p, sigs in by_pair.items()
    ])
    for d in results:
        out.update(d)
    return {"enrich": out, "count": len(out)}


# Фоновый бэкфилл ST-сигналов
_st_backfill_state: dict = {
    "running": False, "started_at": None, "finished_at": None,
    "progress": None, "stats": None, "error": None,
}


async def _run_st_backfill(days: int, alert: bool):
    from datetime import datetime as _dt
    from supertrend_tracker import backfill
    def _on_progress(i, total, pair):
        _st_backfill_state["progress"] = {"processed": i, "total": total, "current_pair": pair}
    try:
        stats = await backfill(days=days, alert_enabled=alert, on_progress=_on_progress)
        _st_backfill_state["stats"] = stats
    except Exception as e:
        import traceback
        _st_backfill_state["error"] = f"{e}\n{traceback.format_exc()[-800:]}"
    finally:
        _st_backfill_state["running"] = False
        _st_backfill_state["finished_at"] = _dt.utcnow().isoformat()


@app.post("/api/supertrend/backfill")
async def api_supertrend_backfill(payload: dict | None = None):
    """Фоновый backfill ST-сигналов за N дней.
    payload: {"days": 14, "alert": false}
    По умолчанию без Telegram — только заполняет БД."""
    from datetime import datetime as _dt
    if _st_backfill_state.get("running"):
        return {"ok": False, "error": "already running", "state": _st_backfill_state}
    days = int((payload or {}).get("days", 14))
    alert = bool((payload or {}).get("alert", False))
    _st_backfill_state.update({
        "running": True,
        "started_at": _dt.utcnow().isoformat(),
        "finished_at": None,
        "progress": {"processed": 0, "total": 0, "current_pair": None},
        "stats": None,
        "error": None,
        "days": days, "alert": alert,
    })
    asyncio.create_task(_run_st_backfill(days, alert))
    return {"ok": True, "started": True, "days": days, "alert": alert}


@app.get("/api/supertrend/backfill-status")
async def api_supertrend_backfill_status():
    return _st_backfill_state


_st_sigs_backtest_state: dict = {
    "running": False, "started_at": None, "finished_at": None,
    "progress": None, "result": None, "error": None,
}


async def _run_st_sigs_backtest(days: int, tier: str):
    """Бектестит ST-сигналы из _supertrend_signals collection.
    Для каждого сигнала симулирует сделку с помощью simulate_trade из
    backtest_supertrend (entry на close флипа, trailing exit на
    противофлипе, timeout 48h, SL = min(ST_value, 1.5 × ATR))."""
    from datetime import datetime as _dt, timedelta as _td
    from database import _supertrend_signals, utcnow
    from backtest_supertrend import compute_st_series, simulate_trade, VariantStats
    from exchange import get_klines_any

    try:
        since = utcnow() - _td(days=days)
        q = {"flip_at": {"$gte": since}}
        if tier:
            q["tier"] = tier
        sigs = list(_supertrend_signals().find(q, {
            "pair_norm": 1, "pair": 1, "direction": 1, "tier": 1,
            "entry_price": 1, "sl_price": 1, "flip_at": 1,
        }))
        total = len(sigs)
        _st_sigs_backtest_state["progress"] = {"processed": 0, "total": total}

        # Группируем по паре — для каждой пары один fetch свечей
        by_pair: dict[str, list[dict]] = {}
        for s in sigs:
            by_pair.setdefault(s["pair_norm"], []).append(s)

        stats: dict[str, VariantStats] = {
            "vip":   VariantStats("vip"),
            "mtf":   VariantStats("mtf"),
            "daily": VariantStats("daily"),
        }
        stats_by_dir: dict[str, dict[str, VariantStats]] = {
            "vip":   {"LONG": VariantStats("vip_LONG"),   "SHORT": VariantStats("vip_SHORT")},
            "mtf":   {"LONG": VariantStats("mtf_LONG"),   "SHORT": VariantStats("mtf_SHORT")},
            "daily": {"LONG": VariantStats("daily_LONG"), "SHORT": VariantStats("daily_SHORT")},
        }

        processed = 0
        for pair_norm, pair_sigs in by_pair.items():
            pair_slash = pair_norm[:-4] + "/USDT"
            try:
                candles = await asyncio.to_thread(get_klines_any, pair_slash, "1h", 400)
            except Exception:
                processed += len(pair_sigs)
                _st_sigs_backtest_state["progress"] = {"processed": processed, "total": total}
                continue
            if not candles or len(candles) < 30:
                processed += len(pair_sigs)
                _st_sigs_backtest_state["progress"] = {"processed": processed, "total": total}
                continue
            st_series = compute_st_series(candles, period=10, mult=3.0)
            if not st_series:
                processed += len(pair_sigs)
                _st_sigs_backtest_state["progress"] = {"processed": processed, "total": total}
                continue

            for s in pair_sigs:
                processed += 1
                flip_at = s.get("flip_at")
                if not flip_at:
                    continue
                flip_ts_ms = int(flip_at.timestamp() * 1000)
                # Ищем индекс entry-бара
                idx = None
                for i in range(len(st_series)):
                    if st_series[i]["t"] >= flip_ts_ms:
                        idx = i
                        break
                if idx is None or idx >= len(st_series) - 2:
                    continue
                direction = s.get("direction")
                if direction not in ("LONG", "SHORT"):
                    continue
                trade = simulate_trade(st_series, idx, direction,
                                        atr_mult_sl=1.5, max_hold_bars=48)
                if trade is None:
                    continue
                trade.pair = pair_norm
                tier_key = s.get("tier", "daily")
                if tier_key in stats:
                    stats[tier_key].trades.append(trade)
                    if direction in stats_by_dir[tier_key]:
                        stats_by_dir[tier_key][direction].trades.append(trade)

            _st_sigs_backtest_state["progress"] = {"processed": processed, "total": total}

        result = {
            "days": days,
            "tier_filter": tier or "all",
            "total_signals": total,
            "by_tier": {k: stats[k].summary() for k in ["vip", "mtf", "daily"]},
            "by_tier_direction": {
                k: {
                    "LONG":  stats_by_dir[k]["LONG"].summary(),
                    "SHORT": stats_by_dir[k]["SHORT"].summary(),
                }
                for k in ["vip", "mtf", "daily"]
            },
        }
        _st_sigs_backtest_state["result"] = result
    except Exception as e:
        import traceback
        _st_sigs_backtest_state["error"] = f"{e}\n{traceback.format_exc()[-800:]}"
    finally:
        _st_sigs_backtest_state["running"] = False
        _st_sigs_backtest_state["finished_at"] = _dt.utcnow().isoformat()


@app.post("/api/backtest-st-signals")
async def api_backtest_st_signals(payload: dict | None = None):
    """Бектест фактических ST-сигналов из БД по tier.
    payload: {"days": 14, "tier": ""}  # tier пустой = все 3
    """
    from datetime import datetime as _dt
    if _st_sigs_backtest_state.get("running"):
        return {"ok": False, "error": "already running", "state": _st_sigs_backtest_state}
    days = int((payload or {}).get("days", 14))
    tier = (payload or {}).get("tier", "") or ""
    _st_sigs_backtest_state.update({
        "running": True,
        "started_at": _dt.utcnow().isoformat(),
        "finished_at": None,
        "progress": {"processed": 0, "total": 0},
        "result": None, "error": None,
        "days": days, "tier": tier,
    })
    asyncio.create_task(_run_st_sigs_backtest(days, tier))
    return {"ok": True, "started": True, "days": days, "tier": tier}


@app.get("/api/backtest-st-signals/status")
async def api_backtest_st_signals_status():
    return _st_sigs_backtest_state


@app.post("/api/paper/close")
async def api_paper_close(payload: dict):
    """Ручное закрытие позиции (статус MANUAL). payload: {"trade_id": 123}"""
    import paper_trader as pt
    trade_id = (payload or {}).get("trade_id")
    if not trade_id:
        return {"ok": False, "error": "trade_id required"}
    try:
        result = await pt.close_manual(int(trade_id))
        if not result:
            return {"ok": False, "error": "position not found or already closed"}
        return {"ok": True, "result": result}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/paper/close-all")
async def api_paper_close_all():
    """Закрывает ВСЕ открытые paper-позиции по текущим рыночным ценам.
    Возвращает список закрытых + суммарный PnL."""
    import paper_trader as pt
    try:
        result = await pt.close_all_manual()
        return {"ok": True, **result}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/paper/mode")
async def api_paper_get_mode():
    import paper_trader as pt
    m = pt.get_mode()
    return {"ok": True, "mode": m}


@app.post("/api/paper/mode")
async def api_paper_set_mode(payload: dict):
    import paper_trader as pt
    name = (payload or {}).get("name", "aggressive")
    m = pt.set_mode(name)
    return {"ok": True, "mode": m}


@app.get("/api/paper/learnings")
async def api_paper_learnings(limit: int = 100):
    """Уроки AI из закрытых сделок + агрегированная память.
    Кеш 60с — вкладка «Авто-торговля» polling'ит этот endpoint, а тяжёлый
    get_ai_memory/get_learnings читают Mongo каждый раз."""
    import paper_trader as pt
    from cache_utils import paper_learnings_cache

    async def _compute():
        learnings = await asyncio.to_thread(pt.get_learnings, limit)
        memory = await asyncio.to_thread(pt.get_ai_memory)
        return {"learnings": learnings, "memory": memory}

    data = await paper_learnings_cache.get_or_compute(f"limit_{limit}", _compute)
    return {"ok": True, "count": len(data["learnings"]),
            "learnings": data["learnings"], "memory": data["memory"]}


# ═══════════════════════════════════════════════════════════
# LIVE TRADING API (Binance Futures через ccxt)
# ═══════════════════════════════════════════════════════════

@app.post("/api/fvg-entry-alert-test")
async def api_fvg_entry_alert_test():
    """Диагностика: шлёт entry alert на первом ENTERED сигнале.
    Если BOT8 работает для FORMED, должен работать для ENTRY тоже."""
    import traceback as _tb
    try:
        from database import _get_db
        db = _get_db()
        sig = db.fvg_signals.find_one({"status": "ENTERED"}, sort=[("entered_at", -1)])
        if not sig:
            return {"ok": False, "error": "no ENTERED signal found"}
        sig["_id"] = str(sig.get("_id", ""))
        from watcher import _send_fvg_entry_alert, _bot8, _admin_chat_id
        state_before = {
            "bot8_initialized": bool(_bot8),
            "admin_chat_id": _admin_chat_id,
        }
        await _send_fvg_entry_alert(sig)
        from watcher import _bot8 as _bot8_after
        state_after = {"bot8_initialized_after": bool(_bot8_after)}
        return {"ok": True, "sig": {
            "instrument": sig.get("instrument"),
            "direction": sig.get("direction"),
            "entered_at": str(sig.get("entered_at")),
        }, **state_before, **state_after}
    except Exception as e:
        return {"ok": False, "error": str(e), "traceback": _tb.format_exc()[-1500:]}


@app.post("/api/fvg-monitor-debug")
async def api_fvg_monitor_debug():
    """Диагностика: вручную вызывает monitor_signals и возвращает детальный
    trace + все exceptions. Без глотания ошибок как в _check_forex_fvg_monitor."""
    import traceback as _tb
    try:
        from fvg_scanner import monitor_signals
        events = await asyncio.to_thread(monitor_signals)
        summary = {k: len(v) for k, v in events.items()}
        # Пример первых entered/closed для проверки
        samples = {}
        for k in ("entered", "closed_tp", "closed_sl", "expired"):
            sample = events.get(k, [])[:3]
            samples[k] = [
                {"instrument": s.get("instrument"), "direction": s.get("direction"),
                 "entry_price": s.get("entry_price"), "entered_price": s.get("entered_price"),
                 "exit_price": s.get("exit_price"), "outcome_R": s.get("outcome_R")}
                for s in sample
            ]
        return {"ok": True, "summary": summary, "samples": samples}
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "traceback": _tb.format_exc()[-1500:],
        }


@app.get("/api/live/status")
async def api_live_status():
    """Полный статус live trading — для UI badges + status bar."""
    import live_safety as ls
    try:
        summary = await asyncio.to_thread(ls.get_status_summary)
        return {"ok": True, **summary}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/live/set-mode")
async def api_live_set_mode(payload: dict):
    """Переключить режим: paper | testnet | real.
    payload: {"mode": "testnet"}"""
    import live_safety as ls
    mode = (payload or {}).get("mode", "paper")
    if mode not in ("paper", "testnet", "real"):
        return {"ok": False, "error": "mode must be paper|testnet|real"}
    try:
        state = ls.set_mode(mode)
        return {"ok": True, "state": {k: v for k, v in state.items() if k != "_id"}}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/live/set-preset")
async def api_live_set_preset(payload: dict):
    """Переключить safety preset: conservative | moderate | aggressive."""
    import live_safety as ls
    preset = (payload or {}).get("preset", "conservative")
    try:
        state = ls.set_preset(preset)
        return {"ok": True, "preset": preset, "config": ls.get_preset_config()}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/live/set-balance")
async def api_live_set_balance(payload: dict):
    """Установить баланс для testnet или real. payload: {"env":"testnet","amount":1000}"""
    import live_safety as ls
    env = (payload or {}).get("env", "testnet")
    amount = (payload or {}).get("amount")
    if amount is None:
        return {"ok": False, "error": "amount required"}
    try:
        state = ls.set_balance(env, float(amount))
        return {"ok": True, "env": env, "balance": float(amount)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/live/enable")
async def api_live_enable(payload: dict | None = None):
    """Включить автоторговлю. payload: {"enabled": true}"""
    import live_safety as ls
    enabled = bool((payload or {}).get("enabled", True))
    try:
        state = ls.set_enabled(enabled)
        return {"ok": True, "enabled": enabled}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/live/kill-switch")
async def api_live_kill_switch(payload: dict | None = None):
    """Kill switch — блокирует новые + закрывает все открытые на бирже."""
    import live_safety as ls
    import live_trader as lt
    reason = (payload or {}).get("reason", "manual")
    try:
        ls.activate_kill_switch(reason)
        state = ls.get_state()
        env = state.get("mode", "testnet")
        if env in ("testnet", "real"):
            results = await lt.close_all_positions(env, reason=f"KILL_{reason}")
            return {"ok": True, "closed_count": sum(1 for r in results if r and r.get("ok"))}
        return {"ok": True, "closed_count": 0}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/live/kill-switch/reset")
async def api_live_reset_kill():
    """Снять kill switch (только после анализа причины!)."""
    import live_safety as ls
    try:
        state = ls.reset_kill_switch()
        return {"ok": True, "state": {k: v for k, v in state.items() if k != "_id"}}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/live/test-connection")
async def api_live_test_connection(env: str = "testnet"):
    """Проверка Binance API — balance + ping."""
    import live_trader as lt
    try:
        result = await asyncio.to_thread(lt.test_connection, env)
        return result
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/live/positions")
async def api_live_positions(env: str = ""):
    """Открытые live позиции. Если env='' — из текущего режима."""
    import live_trader as lt
    import live_safety as ls
    if not env:
        env = ls.get_state().get("mode", "testnet")
    try:
        positions = await asyncio.to_thread(lt.get_open_positions, env)
        for p in positions:
            p["_id"] = str(p.get("_id", ""))
            if p.get("opened_at") and hasattr(p["opened_at"], "isoformat"):
                p["opened_at"] = p["opened_at"].isoformat()
        return {"ok": True, "env": env, "count": len(positions), "positions": positions}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/live/history")
async def api_live_history(env: str = "", limit: int = 50):
    """История закрытых live сделок."""
    import live_trader as lt
    import live_safety as ls
    if not env:
        env = ls.get_state().get("mode", "testnet")
    try:
        history = await asyncio.to_thread(lt.get_history, env, limit)
        for h in history:
            h["_id"] = str(h.get("_id", ""))
            for f in ("opened_at", "closed_at"):
                if h.get(f) and hasattr(h[f], "isoformat"):
                    h[f] = h[f].isoformat()
        return {"ok": True, "env": env, "count": len(history), "history": history}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/live/close")
async def api_live_close(payload: dict):
    """Ручное закрытие live позиции. payload: {"trade_id": 123}"""
    import live_trader as lt
    trade_id = (payload or {}).get("trade_id")
    if not trade_id:
        return {"ok": False, "error": "trade_id required"}
    try:
        result = await lt.close_position(int(trade_id), reason="MANUAL")
        return result or {"ok": False, "error": "close returned None"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/live/confirm")
async def api_live_confirm(payload: dict):
    """Подтвердить pending ордер (вызывается из BOT11 callback).
    payload: {"token": "...", "action": "approve"|"reject"}"""
    import live_trader as lt
    token = (payload or {}).get("token", "")
    action = (payload or {}).get("action", "")
    if not token or action not in ("approve", "reject"):
        return {"ok": False, "error": "token + action (approve|reject) required"}
    try:
        if action == "approve":
            return await lt.execute_confirmed(token) or {"ok": False}
        else:
            return await lt.execute_rejected(token)
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/paper/rejections")
async def api_paper_rejections(limit: int = 50):
    """Последние отказы AI от сделок — для UI лога «почему не вошёл».
    Кеш 30с — аккордеон polling'ит этот endpoint."""
    import paper_trader as pt
    from cache_utils import paper_rejections_cache

    async def _compute():
        return await asyncio.to_thread(pt.get_rejections, limit)

    items = await paper_rejections_cache.get_or_compute(f"limit_{limit}", _compute)
    return {"ok": True, "count": len(items), "items": items}


@app.post("/api/paper/ai-test")
async def api_paper_ai_test(payload: dict | None = None):
    """Симулирует ai_decide() на синтетическом или переданном сигнале —
    для диагностики почему AI отказывается открывать сделки.

    payload (optional): {"symbol":"BTCUSDT","direction":"LONG","entry":65000,"source":"supertrend","score":5}
    Если пусто — берёт последний CV сигнал из БД.
    """
    import paper_trader as pt
    from database import _signals, utcnow
    from datetime import timedelta
    sig = payload or {}
    if not sig.get("symbol"):
        # Берём последний CV с pattern
        doc = _signals().find_one(
            {"source": "cryptovizor", "pattern_triggered": True},
            sort=[("pattern_triggered_at", -1)],
        )
        if not doc:
            return {"ok": False, "error": "no signal to test with"}
        sig = {
            "symbol": (doc.get("pair") or "").replace("/", "").upper(),
            "pair": doc.get("pair"),
            "direction": doc.get("direction"),
            "entry": doc.get("pattern_price") or doc.get("entry"),
            "source": "cryptovizor",
            "score": doc.get("ai_score"),
            "pattern": doc.get("pattern_name"),
            "is_top_pick": bool(doc.get("is_top_pick")),
        }
    try:
        decision = await pt.ai_decide(sig)
        # Плюс состояние
        return {
            "ok": True,
            "input": sig,
            "decision": decision,
            "mode": pt.get_mode(),
            "open_positions": len(pt.get_open_positions()),
            "max_positions": pt.MAX_POSITIONS,
            "balance": pt.get_balance(),
        }
    except Exception as e:
        import traceback
        return {"ok": False, "error": f"{e}", "traceback": traceback.format_exc()[-800:]}


@app.get("/api/paper/ai-prompt")
async def api_paper_ai_prompt():
    """Возвращает текущий AI-промт по секциям (для UI-гармошки).
    Динамический: меняется с ростом памяти AI и изменением mode/balance."""
    import paper_trader as pt
    try:
        data = await asyncio.to_thread(pt.get_prompt_preview)
        return {"ok": True, **data}
    except Exception as e:
        import traceback
        return {"ok": False, "error": f"{e}\n{traceback.format_exc()[-500:]}"}


@app.post("/api/paper/refresh-ai-memory")
async def api_paper_refresh_memory():
    """Ручной запуск агрегации AI уроков (автоматически раз в сутки)."""
    import paper_trader as pt
    try:
        data = await pt.refresh_ai_memory()
        return {"ok": True, "memory": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/paper/clear-ai-memory")
async def api_paper_clear_memory():
    """Полный reset AI-памяти. Используется когда Claude застрял на неверном
    правиле — например, 'Vol×0 = абсолютный стоп' на основе багованных данных.
    После reset AI работает без уроков до следующего refresh'а."""
    import paper_trader as pt
    try:
        cleared = await asyncio.to_thread(pt.clear_ai_memory)
        return {"ok": True, "cleared": cleared}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/paper/started")
async def api_paper_started():
    """Когда запущена paper trading (autotrading) + базовая статистика.
    Sync Mongo (3 операции) выносим в to_thread."""
    from database import _get_db, utcnow
    from datetime import datetime, timezone

    def _sync():
        db = _get_db()
        state = db.paper_trades.find_one({"_id": "state"})
        if not state:
            return {"ok": False, "error": "paper trading not initialized"}
        started = state.get("started_at")
        now = utcnow()
        if started and started.tzinfo is None:
            started_naive = started
        elif started:
            started_naive = started.replace(tzinfo=None)
        else:
            started_naive = None
        if not started_naive:
            return {"ok": False, "error": "started_at missing"}
        delta = now - started_naive
        total_sec = int(delta.total_seconds())
        days = total_sec // 86400
        hours = (total_sec % 86400) // 3600
        minutes = (total_sec % 3600) // 60
        parts = []
        if days:  parts.append(f"{days} д")
        if hours: parts.append(f"{hours} ч")
        parts.append(f"{minutes} мин")
        total_trades = db.paper_trades.count_documents({"status": {"$in": ["TP", "SL", "MANUAL"]}})
        open_count  = db.paper_trades.count_documents({"status": "OPEN"})
        return {
            "ok": True,
            "started_at": started.isoformat() if hasattr(started, "isoformat") else str(started),
            "now_utc": now.isoformat(),
            "elapsed_seconds": total_sec,
            "elapsed_human": " ".join(parts),
            "balance": state.get("balance"),
            "initial_balance": 1000.0,
            "pnl_pct": round((state.get("balance", 1000) - 1000.0) / 1000.0 * 100, 2),
            "total_trades": total_trades,
            "open_positions": open_count,
        }

    return await asyncio.to_thread(_sync)


@app.get("/api/bots-status")
async def api_bots_status():
    """Диагностика: показывает какие Telegram-боты инициализированы + счётчик
    недавних Confluence алертов за последние 24 часа (для отладки когда
    `в бота X не приходят сигналы`)."""
    from database import _confluence, _signals, _anomalies, _clusters, utcnow
    from datetime import timedelta
    since_24h = utcnow() - timedelta(hours=24)
    since_6h = utcnow() - timedelta(hours=6)

    # Проверка bot instances в watcher
    import watcher as _w
    bots = {
        "bot (tradium)": bool(getattr(_w, "_bot", None)),
        "bot2 (cryptovizor)": bool(getattr(_w, "_bot2", None)),
        "bot4 (ai)": bool(getattr(_w, "_bot4", None)),
        "bot5 (confluence)": bool(getattr(_w, "_bot5", None)),
        "bot7 (cluster)": bool(getattr(_w, "_bot7", None)),
        "bot9 (top picks)": bool(getattr(_w, "_bot9", None)),
        "bot10 (supertrend)": bool(getattr(_w, "_bot10", None)),
    }
    # Tokens present in env
    from config import (BOT_TOKEN, BOT2_BOT_TOKEN, BOT4_BOT_TOKEN,
                        BOT5_BOT_TOKEN, BOT7_BOT_TOKEN, BOT9_BOT_TOKEN,
                        BOT10_BOT_TOKEN)
    tokens = {
        "BOT_TOKEN":        bool(BOT_TOKEN),
        "BOT2_BOT_TOKEN":   bool(BOT2_BOT_TOKEN),
        "BOT4_BOT_TOKEN":   bool(BOT4_BOT_TOKEN),
        "BOT5_BOT_TOKEN":   bool(BOT5_BOT_TOKEN),
        "BOT7_BOT_TOKEN":   bool(BOT7_BOT_TOKEN),
        "BOT9_BOT_TOKEN":   bool(BOT9_BOT_TOKEN),
        "BOT10_BOT_TOKEN":  bool(BOT10_BOT_TOKEN),
    }
    # Recent DB activity — 8 count_documents параллельно через gather(to_thread) вместо последовательно
    from database import _supertrend_signals as _sts

    def _cnt_conf_all():
        return _confluence().count_documents({"detected_at": {"$gte": since_24h}})
    def _cnt_conf_alert_24h():
        return _confluence().count_documents({"detected_at": {"$gte": since_24h}, "score": {"$gte": 5}, "st_passed": True})
    def _cnt_conf_alert_6h():
        return _confluence().count_documents({"detected_at": {"$gte": since_6h}, "score": {"$gte": 5}, "st_passed": True})
    def _cnt_anom():
        return _anomalies().count_documents({"detected_at": {"$gte": since_24h}})
    def _cnt_trad():
        return _signals().count_documents({"source": "tradium", "received_at": {"$gte": since_24h}})
    def _cnt_cv():
        return _signals().count_documents({"source": "cryptovizor", "pattern_triggered": True, "pattern_triggered_at": {"$gte": since_24h}})
    def _cnt_cluster():
        return _clusters().count_documents({"trigger_at": {"$gte": since_24h}})
    def _cnt_st():
        try:
            return _sts().count_documents({"flip_at": {"$gte": since_24h}})
        except Exception:
            return 0

    (conf_all, conf_alertable_24h, conf_alertable_6h, anomaly_24h,
     tradium_24h, cv_24h, cluster_24h, st_24h) = await asyncio.gather(
        asyncio.to_thread(_cnt_conf_all),
        asyncio.to_thread(_cnt_conf_alert_24h),
        asyncio.to_thread(_cnt_conf_alert_6h),
        asyncio.to_thread(_cnt_anom),
        asyncio.to_thread(_cnt_trad),
        asyncio.to_thread(_cnt_cv),
        asyncio.to_thread(_cnt_cluster),
        asyncio.to_thread(_cnt_st),
    )
    return {
        "bots_initialized": bots,
        "tokens_in_env": tokens,
        "signals_last_24h": {
            "confluence_all": conf_all,
            "confluence_alertable_score>=5_st_passed": conf_alertable_24h,
            "confluence_alertable_6h": conf_alertable_6h,
            "anomaly": anomaly_24h,
            "tradium": tradium_24h,
            "cryptovizor_pattern": cv_24h,
            "cluster": cluster_24h,
            "supertrend": st_24h,
        },
        "note": "Confluence alerts отправляются только при score>=5 AND st_passed=True",
    }


@app.get("/api/key-levels/coverage")
async def api_key_levels_coverage(days: int = 14):
    """Сравнивает множество уникальных пар из signals с множеством пар в key_levels
    за окно N дней. Возвращает:
      {
        "signals_pairs": 380,     # сколько уникальных пар с сигналами
        "kl_pairs": 340,          # сколько уникальных пар в KL
        "covered": 320,           # пересечение
        "missing": ["ABCUSDT", ...],  # пары с сигналами но БЕЗ KL
        "orphan_kl": ["XYZUSDT"]  # пары в KL но без сигналов (информативно)
      }
    """
    from database import _signals, _key_levels, utcnow as _unow
    from datetime import timedelta as _td
    since = _unow() - _td(days=days)
    # Уникальные пары из signals за окно (все источники)
    sig_pairs = set()
    for s in _signals().find({"received_at": {"$gte": since}}, {"pair": 1}):
        p = (s.get("pair") or "").replace("/", "").upper()
        if not p:
            continue
        if not p.endswith("USDT"):
            p = p + "USDT"
        sig_pairs.add(p)
    # Уникальные пары из key_levels за окно
    kl_pairs = set()
    for k in _key_levels().find({"detected_at": {"$gte": since}}, {"pair_norm": 1}):
        p = (k.get("pair_norm") or "").upper()
        if p:
            kl_pairs.add(p)
    covered = sig_pairs & kl_pairs
    missing = sorted(sig_pairs - kl_pairs)
    orphan = sorted(kl_pairs - sig_pairs)
    return {
        "days": days,
        "signals_pairs": len(sig_pairs),
        "kl_pairs": len(kl_pairs),
        "covered": len(covered),
        "missing_count": len(missing),
        "missing": missing[:200],  # top 200
        "orphan_count": len(orphan),
        "orphan_sample": orphan[:30],
    }


@app.get("/api/peek-tradium-topic")
async def api_peek_tradium_topic(topic_id: int, limit: int = 5):
    """Показывает последние N сообщений из конкретного топика Tradium группы.
    Используется чтобы посмотреть формат Key Levels / других форум-топиков."""
    try:
        from userbot import _tg_client
        from config import SOURCE_GROUP_ID
    except Exception as e:
        return {"ok": False, "error": str(e)}
    if _tg_client is None or not _tg_client.is_connected():
        return {"ok": False, "error": "Telethon not connected"}
    try:
        out = []
        # reply_to=topic_id — фильтр по топику
        async for m in _tg_client.iter_messages(SOURCE_GROUP_ID, limit=limit, reply_to=topic_id):
            text = (m.raw_text or "")[:800]
            out.append({
                "id": m.id,
                "date": m.date.isoformat() if m.date else None,
                "reply_to_top_id": getattr(getattr(m, "reply_to", None), "reply_to_top_id", None)
                                    or getattr(getattr(m, "reply_to", None), "reply_to_msg_id", None),
                "has_media": m.media is not None,
                "text": text,
            })
        return {"ok": True, "topic_id": topic_id, "count": len(out), "messages": out}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-500:]}


@app.post("/api/debug-fetch-chart")
async def api_debug_fetch_chart(payload: dict):
    """Синхронно обрабатывает ОДИН signal: ищет чарт, скачивает, прогоняет AI, возвращает детальный лог."""
    import os
    from datetime import datetime, timezone
    from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
    log = []
    def L(msg): log.append(str(msg))
    try:
        sig_id = int(payload.get("sig_id") or 0)
        if not sig_id:
            return {"ok": False, "error": "sig_id required"}
        from database import _signals, save_chart
        from ai_analyzer import analyze_chart
        from config import SOURCE_GROUP_ID, TRADIUM_SETUP_TOPIC_ID, CHARTS_DIR
        from userbot import _tg_client
        if not _tg_client or not _tg_client.is_connected():
            return {"ok": False, "error": "Telethon not connected"}
        s = _signals().find_one({"id": sig_id})
        if not s:
            return {"ok": False, "error": f"signal id={sig_id} not found"}
        msg_id = s.get("message_id")
        pair = s.get("pair")
        L(f"signal: id={sig_id} pair={pair} msg_id={msg_id}")
        if not msg_id:
            return {"ok": False, "error": "no message_id", "log": log}

        chart_msg = None
        for cand in range(msg_id + 1, msg_id + 11):
            try:
                m = await _tg_client.get_messages(SOURCE_GROUP_ID, ids=cand)
            except Exception as e:
                L(f"cand={cand} get_messages ERROR: {e}")
                continue
            if m is None:
                L(f"cand={cand} None")
                continue
            top = None
            if m.reply_to:
                top = getattr(m.reply_to, "reply_to_top_id", None) or m.reply_to.reply_to_msg_id
            is_photo = isinstance(m.media, MessageMediaPhoto)
            is_doc_image = (
                isinstance(m.media, MessageMediaDocument)
                and m.media.document.mime_type
                and m.media.document.mime_type.startswith("image/")
            ) if m.media else False
            L(f"cand={cand} topic={top} media={'photo' if is_photo else ('doc' if is_doc_image else 'none')}")
            if top != TRADIUM_SETUP_TOPIC_ID:
                continue
            if is_photo or is_doc_image:
                chart_msg = m
                L(f"  → SELECTED")
                break
        if not chart_msg:
            return {"ok": False, "error": "no chart found", "log": log}

        os.makedirs(CHARTS_DIR, exist_ok=True)
        chart_filename = f"{sig_id}_{chart_msg.id}.jpg"
        chart_path = os.path.join(CHARTS_DIR, chart_filename)
        try:
            await _tg_client.download_media(chart_msg, file=chart_path)
            L(f"downloaded: {chart_path} size={os.path.getsize(chart_path)}")
        except Exception as e:
            return {"ok": False, "error": f"download failed: {e}", "log": log}

        try:
            with open(chart_path, "rb") as f:
                save_chart(sig_id, f.read(), filename=chart_filename)
            L("saved to GridFS")
        except Exception as e:
            L(f"GridFS warn: {e}")

        try:
            chart_data = await analyze_chart(chart_path)
            L(f"AI result: {chart_data}")
        except Exception as e:
            return {"ok": False, "error": f"AI failed: {e}", "log": log}

        def _tof(v):
            try: return float(v) if v is not None else None
            except: return None

        updates = {
            "has_chart": True,
            "chart_message_id": chart_msg.id,
            "chart_path": chart_path,
            "chart_received_at": datetime.now(timezone.utc).replace(tzinfo=None),
        }
        if not chart_data.get("_error"):
            updates.update({
                "chart_analyzed": True,
                "dca1": _tof(chart_data.get("dca1")),
                "dca2": _tof(chart_data.get("dca2")),
                "dca3": _tof(chart_data.get("dca3")),
                "dca4": _tof(chart_data.get("dca4")),
                "chart_notes": chart_data.get("notes") or chart_data.get("pattern", ""),
            })
        _signals().update_one({"_id": s["_id"]}, {"$set": updates})
        L(f"DB updated with dca1-4: {updates.get('dca1')}, {updates.get('dca2')}, {updates.get('dca3')}, {updates.get('dca4')}")
        return {"ok": True, "updates": updates, "log": log}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "traceback": traceback.format_exc(), "log": log}


@app.get("/api/inspect-msg-neighbors")
async def api_inspect_msg_neighbors(msg_id: int, count: int = 5):
    """Показывает msg_id+1..msg_id+count в Tradium группе чтобы понять куда делось фото."""
    try:
        from userbot import _tg_client
        from config import SOURCE_GROUP_ID, TRADIUM_SETUP_TOPIC_ID
        from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
    except Exception as e:
        return {"ok": False, "error": str(e)}
    if _tg_client is None or not _tg_client.is_connected():
        return {"ok": False, "error": "not connected"}
    out = []
    for cand in range(msg_id, msg_id + count + 1):
        try:
            m = await _tg_client.get_messages(SOURCE_GROUP_ID, ids=cand)
        except Exception as e:
            out.append({"id": cand, "error": str(e)})
            continue
        if m is None:
            out.append({"id": cand, "found": False})
            continue
        top = None
        if m.reply_to:
            top = getattr(m.reply_to, "reply_to_top_id", None) or m.reply_to.reply_to_msg_id
        media_type = None
        if isinstance(m.media, MessageMediaPhoto):
            media_type = "photo"
        elif isinstance(m.media, MessageMediaDocument):
            media_type = f"doc:{getattr(m.media.document, 'mime_type', '?')}"
        elif m.media:
            media_type = type(m.media).__name__
        out.append({
            "id": m.id,
            "date": m.date.isoformat() if m.date else None,
            "topic_id": top,
            "is_setup_topic": top == TRADIUM_SETUP_TOPIC_ID,
            "text_preview": (m.raw_text or "")[:100],
            "media_type": media_type,
            "grouped_id": m.grouped_id,
        })
    return {"ok": True, "messages": out}


@app.post("/api/backfill-tradium-charts")
async def api_backfill_tradium_charts(payload: dict | None = None):
    """Для backfilled Tradium сигналов без графика:
    1) ищет в Telegram следующее фото после текстового сообщения (в том же топике)
    2) скачивает фото и сохраняет в GridFS
    3) прогоняет через Claude Vision (analyze_chart) → извлекает DCA1-4
    4) обновляет signal: has_chart=True, dca1-dca4, pattern_price

    Body: {"hours": 72, "limit": 20}
    """
    payload = payload or {}
    hours = float(payload.get("hours") or 72)
    limit = int(payload.get("limit") or 20)
    try:
        from userbot import _tg_client
    except Exception:
        _tg_client = None
    if _tg_client is None or not _tg_client.is_connected():
        return {"ok": False, "error": "Telethon client not connected"}
    asyncio.create_task(_run_backfill_tradium_charts(_tg_client, hours, limit))
    return {"ok": True, "started": True, "hours": hours, "limit": limit}


async def _run_backfill_tradium_charts(client, hours: float, limit: int):
    import logging as _log
    import os
    from datetime import datetime, timezone, timedelta
    from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
    log = _log.getLogger("backfill-tradium-charts")
    try:
        from database import _signals, save_chart
        from ai_analyzer import analyze_chart
        from config import SOURCE_GROUP_ID, TRADIUM_SETUP_TOPIC_ID, CHARTS_DIR
        os.makedirs(CHARTS_DIR, exist_ok=True)

        cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=hours)
        query = {
            "source": "tradium",
            "received_at": {"$gte": cutoff},
            "$or": [{"dca4": None}, {"dca4": {"$exists": False}}, {"has_chart": False}],
            "message_id": {"$ne": None},
            "pair": {"$ne": None},
        }
        docs = list(_signals().find(query).sort("received_at", -1).limit(limit))
        log.info(f"[tradium-charts] {len(docs)} сигналов без графика за {hours}ч")

        updated = charts_found = ai_ok = 0
        for s in docs:
            msg_id = s.get("message_id")
            sig_id = s.get("id")
            pair = s.get("pair", "")
            if not msg_id or not sig_id:
                continue
            log.info(f"[tradium-charts] #{sig_id} {pair} msg_id={msg_id} — ищу график")

            # Ищем фото в пределах 20 следующих сообщений в топике Trade Setup Screener.
            # Пробуем несколько стратегий — Telethon reverse+reply_to нестабилен.
            chart_msg = None
            try:
                # Сначала прямое обращение: берём msg_id+1, msg_id+2, ..., msg_id+20
                for cand_id in range(msg_id + 1, msg_id + 21):
                    try:
                        m = await client.get_messages(SOURCE_GROUP_ID, ids=cand_id)
                    except Exception:
                        m = None
                    if m is None:
                        continue
                    # Убедимся что сообщение относится к нужному топику
                    topic_ok = True
                    if TRADIUM_SETUP_TOPIC_ID and m.reply_to:
                        top = getattr(m.reply_to, "reply_to_top_id", None) or m.reply_to.reply_to_msg_id
                        topic_ok = (top == TRADIUM_SETUP_TOPIC_ID)
                    if not topic_ok:
                        continue
                    is_photo = isinstance(m.media, MessageMediaPhoto)
                    is_doc_image = (
                        isinstance(m.media, MessageMediaDocument)
                        and m.media.document.mime_type
                        and m.media.document.mime_type.startswith("image/")
                    ) if m.media else False
                    if is_photo or is_doc_image:
                        chart_msg = m
                        log.info(f"[tradium-charts] #{sig_id} chart_msg_id={m.id} (offset +{cand_id - msg_id})")
                        break
            except Exception as e:
                log.warning(f"[tradium-charts] #{sig_id} search failed: {e}")
                continue

            if not chart_msg:
                log.info(f"[tradium-charts] #{sig_id} {pair} — график не найден")
                continue

            # Скачиваем
            chart_filename = f"{sig_id}_{chart_msg.id}.jpg"
            chart_path = os.path.join(CHARTS_DIR, chart_filename)
            try:
                await client.download_media(chart_msg, file=chart_path)
            except Exception as e:
                log.warning(f"[tradium-charts] #{sig_id} download failed: {e}")
                continue
            charts_found += 1

            # В GridFS
            try:
                with open(chart_path, "rb") as f:
                    save_chart(sig_id, f.read(), filename=chart_filename)
            except Exception:
                pass

            # AI Vision
            try:
                chart_data = await analyze_chart(chart_path)
            except Exception as e:
                log.warning(f"[tradium-charts] #{sig_id} AI failed: {e}")
                chart_data = {"_error": str(e)}

            def _tof(v):
                try: return float(v) if v is not None else None
                except: return None

            updates = {
                "has_chart": True,
                "chart_message_id": chart_msg.id,
                "chart_path": chart_path,
                "chart_received_at": datetime.now(timezone.utc).replace(tzinfo=None),
            }
            if not chart_data.get("_error"):
                ai_ok += 1
                updates.update({
                    "chart_analyzed": True,
                    "chart_ai_raw": chart_data.get("_raw", ""),
                    "chart_pair": chart_data.get("pair"),
                    "chart_direction": chart_data.get("direction"),
                    "chart_entry": _tof(chart_data.get("entry")),
                    "chart_sl": _tof(chart_data.get("sl")),
                    "chart_tp1": _tof(chart_data.get("tp1")),
                    "chart_tp2": _tof(chart_data.get("tp2")),
                    "chart_tp3": _tof(chart_data.get("tp3")),
                    "chart_notes": chart_data.get("notes") or chart_data.get("pattern", ""),
                    "dca1": _tof(chart_data.get("dca1")),
                    "dca2": _tof(chart_data.get("dca2")),
                    "dca3": _tof(chart_data.get("dca3")),
                    "dca4": _tof(chart_data.get("dca4")),
                })
            _signals().update_one({"_id": s["_id"]}, {"$set": updates})
            updated += 1
            log.info(f"[tradium-charts] ✅ #{sig_id} {pair} chart={charts_found} dca4={updates.get('dca4')}")

        log.info(f"[tradium-charts] DONE: updated={updated}, charts={charts_found}, ai_ok={ai_ok}")
        try:
            from watcher import _bot, _admin_chat_id
            if _bot and _admin_chat_id:
                await _bot.send_message(
                    _admin_chat_id,
                    f"📊 <b>Tradium графики подтянуты</b>\n\n"
                    f"Обработано: {updated}\n"
                    f"📸 Графиков найдено: {charts_found}\n"
                    f"🤖 AI прошло: {ai_ok}",
                    parse_mode="HTML",
                )
        except Exception:
            pass
    except Exception:
        log.exception("[tradium-charts] crashed")


@app.post("/api/activate-tradium-archive")
async def api_activate_tradium_archive(payload: dict | None = None):
    """Переводит backfilled Tradium сетапы из АРХИВ в СЛЕЖУ, сбрасывает флаги,
    после чего watcher сам определит достигли ли они TP/SL/паттерна по рынку.
    is_forwarded=True сохраняется чтобы не слать Telegram алерты задним числом.

    Body (optional): {"hours": 48} — сколько часов назад максимум
    """
    payload = payload or {}
    hours = float(payload.get("hours") or 72)
    asyncio.create_task(_run_activate_tradium_archive(hours))
    return {"ok": True, "started": True, "hours": hours}


async def _run_activate_tradium_archive(hours: float):
    import logging as _log
    from datetime import datetime, timezone, timedelta
    log = _log.getLogger("activate-tradium")
    try:
        from database import _signals
        from exchange import get_prices_any as sync_get_prices
        cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=hours)
        query = {
            "source": "tradium",
            "status": "АРХИВ",
            "received_at": {"$gte": cutoff},
            "pair": {"$ne": None},
            "tp1": {"$ne": None},
            "sl": {"$ne": None},
            "entry": {"$ne": None},
        }
        docs = list(_signals().find(query))
        log.info(f"[activate-tradium] {len(docs)} backfilled Tradium сетапов для активации")
        if not docs:
            return

        # Текущие цены
        pairs = [d.get("pair") for d in docs if d.get("pair")]
        try:
            prices = await asyncio.to_thread(sync_get_prices, pairs)
        except Exception:
            prices = {}

        activated = tp_hit = sl_hit = pattern_hit = 0
        for d in docs:
            pair = d.get("pair")
            entry = d.get("entry")
            tp1 = d.get("tp1")
            sl = d.get("sl")
            direction = d.get("direction")
            cur = prices.get(pair) if pair else None

            # Определяем текущий статус по цене
            new_status = "СЛЕЖУ"
            exit_price = None
            pnl_pct = None
            if cur and direction and tp1 and sl and entry:
                is_long = direction in ("LONG", "BUY")
                if is_long:
                    if cur >= tp1:
                        new_status = "TP"; exit_price = tp1
                        pnl_pct = (tp1 - entry) / entry * 100
                    elif cur <= sl:
                        new_status = "SL"; exit_price = sl
                        pnl_pct = (sl - entry) / entry * 100
                    elif cur >= entry:
                        new_status = "ПАТТЕРН"  # entry reached = setup triggered
                else:  # SHORT
                    if cur <= tp1:
                        new_status = "TP"; exit_price = tp1
                        pnl_pct = (entry - tp1) / entry * 100
                    elif cur >= sl:
                        new_status = "SL"; exit_price = sl
                        pnl_pct = (entry - sl) / entry * 100
                    elif cur <= entry:
                        new_status = "ПАТТЕРН"

            updates = {
                "status": new_status,
                "dca4_triggered": False,
                "pattern_triggered": new_status == "ПАТТЕРН",
                "is_forwarded": True,  # не шлём Telegram алерт задним числом
            }
            if exit_price is not None:
                updates["exit_price"] = exit_price
                updates["closed_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
                updates["pnl_percent"] = pnl_pct
                updates["result"] = new_status
            if new_status == "ПАТТЕРН":
                updates["pattern_triggered_at"] = datetime.now(timezone.utc).replace(tzinfo=None)

            _signals().update_one({"_id": d["_id"]}, {"$set": updates})
            activated += 1
            if new_status == "TP": tp_hit += 1
            elif new_status == "SL": sl_hit += 1
            elif new_status == "ПАТТЕРН": pattern_hit += 1
            log.info(f"[activate-tradium] {pair} {direction} entry={entry} cur={cur} → {new_status}")

        log.info(f"[activate-tradium] DONE: activated={activated}, TP={tp_hit}, SL={sl_hit}, ПАТТЕРН={pattern_hit}")
        try:
            from watcher import _bot, _admin_chat_id
            if _bot and _admin_chat_id:
                await _bot.send_message(
                    _admin_chat_id,
                    f"🔄 <b>Tradium архив активирован</b>\n\n"
                    f"Обработано: {activated}\n"
                    f"✅ TP: {tp_hit}\n"
                    f"❌ SL: {sl_hit}\n"
                    f"🎯 ПАТТЕРН: {pattern_hit}\n"
                    f"👁 СЛЕЖУ: {activated - tp_hit - sl_hit - pattern_hit}",
                    parse_mode="HTML",
                )
        except Exception:
            pass
    except Exception:
        log.exception("[activate-tradium] crashed")


@app.post("/api/backfill-patterns")
async def api_backfill_patterns(payload: dict | None = None):
    """Прогон pattern detection задним числом для backfilled CV сигналов.
    Body: {"limit": 100, "status": "АРХИВ"}
    """
    payload = payload or {}
    limit = int(payload.get("limit") or 100)
    status = payload.get("status") or "АРХИВ"
    asyncio.create_task(_run_backfill_patterns(limit, status))
    return {"ok": True, "started": True, "limit": limit, "status": status}


async def _run_backfill_patterns(limit: int, status: str):
    """Фоновая задача: pattern detection задним числом."""
    import logging as _log
    log = _log.getLogger("backfill-patterns-api")
    try:
        from backfill_patterns import get_klines_around, detect_pattern_on_candles
        from database import _signals
        import time as _t
        query = {
            "source": "cryptovizor",
            "status": status,
            "pattern_triggered": True,
            "$or": [{"pattern_name": None}, {"pattern_name": ""}, {"pattern_name": {"$exists": False}}],
            "pair": {"$ne": None},
            "direction": {"$ne": None},
        }
        docs = list(_signals().find(query).sort("received_at", -1).limit(limit))
        log.info(f"[backfill-patterns] {len(docs)} candidates (status={status})")
        found = no_data = no_pattern = 0
        for s in docs:
            pair = s.get("pair", "")
            direction = s.get("direction", "")
            received_at = s.get("received_at")
            if not received_at:
                continue
            candles = await asyncio.to_thread(get_klines_around, pair, received_at, 24, 60)
            if not candles:
                no_data += 1
                continue
            signal_ms = int(received_at.timestamp() * 1000)
            pattern, candle = detect_pattern_on_candles(candles, direction, signal_ms)
            if not pattern:
                no_pattern += 1
                continue
            from datetime import datetime as _dt, timezone as _tz
            pt_dt = _dt.fromtimestamp(candle["t"] / 1000, tz=_tz.utc).replace(tzinfo=None)
            _signals().update_one(
                {"_id": s["_id"]},
                {"$set": {
                    "status": "ПАТТЕРН",
                    "pattern_triggered_at": pt_dt,
                    "pattern_name": pattern,
                    "pattern_price": candle["c"],
                }}
            )
            found += 1
            log.info(f"[backfill-patterns] ✅ {pair} {direction} → {pattern} @ {pt_dt}")
            await asyncio.sleep(0.05)
        log.info(f"[backfill-patterns] DONE: found={found}, no_pattern={no_pattern}, no_data={no_data}")
        try:
            from watcher import _bot, _admin_chat_id
            if _bot and _admin_chat_id:
                await _bot.send_message(
                    _admin_chat_id,
                    f"📊 <b>Backfill patterns завершён</b>\n\n"
                    f"✅ Найдено паттернов: {found}\n"
                    f"⚪ Без паттерна: {no_pattern}\n"
                    f"❓ Без данных: {no_data}",
                    parse_mode="HTML",
                )
        except Exception:
            pass
    except Exception:
        log.exception("[backfill-patterns] crashed")


@app.get("/api/clusters")
async def api_clusters(status: str = "all", limit: int = 200):
    """Список кластеров для UI вкладки "Кластеры".
    Sync Mongo (2 .find() + full-scan) выносим в to_thread и считаем stats
    одним aggregate pipeline вместо полного чтения коллекции."""
    from database import _clusters
    from pymongo import DESCENDING

    def _sync():
        q = {}
        if status and status != "all":
            q["status"] = status.upper()
        docs = list(_clusters().find(q).sort("trigger_at", DESCENDING).limit(limit))
        out = []
        for d in docs:
            d.pop("_id", None)
            for k in ("trigger_at", "closed_at", "created_at"):
                v = d.get(k)
                if hasattr(v, "isoformat"): d[k] = v.isoformat()
            for s in d.get("signals_in_cluster", []):
                if hasattr(s.get("at"), "isoformat"):
                    s["at"] = s["at"].isoformat()
            out.append(d)
        # Aggregate pipeline вместо list(find({})) — всё считается на сервере Mongo
        pipe = [{"$group": {
            "_id": None,
            "total": {"$sum": 1},
            "wins": {"$sum": {"$cond": [{"$eq": ["$status", "TP"]}, 1, 0]}},
            "losses": {"$sum": {"$cond": [{"$eq": ["$status", "SL"]}, 1, 0]}},
            "open": {"$sum": {"$cond": [{"$eq": ["$status", "OPEN"]}, 1, 0]}},
            "mega": {"$sum": {"$cond": [{"$eq": ["$strength", "MEGA"]}, 1, 0]}},
            "strong": {"$sum": {"$cond": [{"$eq": ["$strength", "STRONG"]}, 1, 0]}},
            "sum_pnl": {"$sum": {"$cond": [
                {"$in": ["$status", ["TP", "SL"]]},
                {"$ifNull": ["$pnl_percent", 0]}, 0]}},
        }}]
        agg = list(_clusters().aggregate(pipe))
        if agg:
            a = agg[0]
            stats = {
                "total": a.get("total", 0),
                "wins": a.get("wins", 0),
                "losses": a.get("losses", 0),
                "open": a.get("open", 0),
                "mega": a.get("mega", 0),
                "strong": a.get("strong", 0),
                "wr": round(a.get("wins", 0) / max(a.get("wins", 0) + a.get("losses", 0), 1) * 100, 1),
                "sum_pnl": round(a.get("sum_pnl", 0), 1),
            }
        else:
            stats = {"total": 0, "wins": 0, "losses": 0, "open": 0, "mega": 0, "strong": 0, "wr": 0, "sum_pnl": 0}
        return {"items": out, "stats": stats}

    return await asyncio.to_thread(_sync)


@app.get("/api/cluster-config")
async def api_cluster_config_get():
    from cluster_detector import get_config
    return await asyncio.to_thread(get_config)


@app.post("/api/cluster-config")
async def api_cluster_config_post(payload: dict):
    from cluster_detector import save_config
    return await asyncio.to_thread(save_config, payload or {})


@app.get("/api/pair-signals")
async def api_pair_signals(pair: str, direction: str = "", window_h: int = 8):
    """Все сигналы по паре + (опциональное) направление за окно.
    Для модалки pending cluster → показать график с маркерами всех сигналов."""
    from cluster_detector import collect_signals_for, _norm_pair
    from database import utcnow
    norm = _norm_pair(pair)
    now = utcnow()
    out = {"pair": norm, "direction": direction, "window_h": window_h, "items": {}}
    dirs = [direction] if direction else ["LONG", "SHORT"]
    for d in dirs:
        sigs = collect_signals_for(norm, d, now, window_h, include_clusters=True)
        out["items"][d] = [
            {
                "source": s["source"],
                "at": s["at"].isoformat() if hasattr(s["at"], "isoformat") else str(s["at"]),
                "at_ts": int(s["at"].timestamp()) if hasattr(s["at"], "timestamp") else 0,
                "price": s.get("price"),
                "meta": s.get("meta", {}),
            }
            for s in sigs
        ]
    return out


@app.get("/api/pending-clusters")
async def api_pending_clusters():
    """Монеты которые сейчас 1/N — ждут второго сигнала. Cache 90s."""
    from cache_utils import pending_clusters_cache
    async def _compute():
        from cluster_detector import get_pending_clusters, get_config
        pending = await asyncio.to_thread(get_pending_clusters, 50)
        cfg = await asyncio.to_thread(get_config)
        return {"items": pending, "config": cfg}
    return await pending_clusters_cache.get_or_compute("pending", _compute)


@app.post("/api/backfill-clusters")
async def api_backfill_clusters(payload: dict | None = None):
    """Пробежать по истории и создать кластеры."""
    payload = payload or {}
    hours = int(payload.get("hours") or 96)
    asyncio.create_task(_run_backfill_clusters(hours))
    return {"ok": True, "started": True, "hours": hours}


async def _run_backfill_clusters(hours: int):
    import logging as _log
    from datetime import datetime, timezone, timedelta
    log = _log.getLogger("backfill-clusters")
    try:
        from cluster_detector import should_trigger_cluster, create_cluster
        from database import _signals, _anomalies, _confluence
        since = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=hours)
        all_sigs = []
        for s in _signals().find({"source": "cryptovizor", "pattern_triggered": True,
                                   "pattern_triggered_at": {"$gte": since},
                                   "direction": {"$ne": None}, "pair": {"$ne": None}}):
            all_sigs.append({"pair": s["pair"], "direction": s["direction"], "at": s["pattern_triggered_at"]})
        for s in _signals().find({"source": "tradium", "received_at": {"$gte": since},
                                   "direction": {"$ne": None}, "pair": {"$ne": None}}):
            all_sigs.append({"pair": s["pair"], "direction": s["direction"], "at": s["received_at"]})
        for a in _anomalies().find({"detected_at": {"$gte": since}, "direction": {"$in": ["LONG", "SHORT"]}}):
            pair = a.get("pair") or a.get("symbol", "").replace("USDT", "/USDT")
            all_sigs.append({"pair": pair, "direction": a["direction"], "at": a["detected_at"]})
        for c in _confluence().find({"detected_at": {"$gte": since}, "direction": {"$in": ["LONG", "SHORT"]}}):
            pair = c.get("pair") or c.get("symbol", "").replace("USDT", "/USDT")
            all_sigs.append({"pair": pair, "direction": c["direction"], "at": c["detected_at"]})
        all_sigs.sort(key=lambda x: x["at"])
        log.info(f"[backfill-clusters] {len(all_sigs)} signals")
        created = 0
        for sig in all_sigs:
            trigger, in_cluster, cnt = await asyncio.to_thread(
                should_trigger_cluster, sig["pair"], sig["direction"], sig["at"]
            )
            if not trigger:
                continue
            cl = await asyncio.to_thread(create_cluster, sig["pair"], sig["direction"], in_cluster, sig["at"])
            if cl: created += 1
        log.info(f"[backfill-clusters] Created {created}")
    except Exception:
        log.exception("[backfill-clusters] crashed")


@app.get("/api/fvg-signals")
async def api_fvg_signals(status: str = "all", limit: int = 200, tf: str = ""):
    """Forex FVG сигналы (active + history).
    tf — фильтр по таймфрейму (например '1H', '4H'). Пусто = все TF.
    Кеш 30с — три коллекции читаются из Mongo + агрегирующий stats,
    раньше тормозило на каждый запрос UI."""
    from fvg_scanner import get_pending_fvgs, get_active_trades, get_stats
    from cache_utils import fvg_signals_cache

    async def _compute():
        waiting = await asyncio.to_thread(get_pending_fvgs, 300)
        entered = await asyncio.to_thread(get_active_trades, 300)
        stats = await asyncio.to_thread(get_stats)
        return {"waiting": waiting, "entered": entered, "stats": stats}

    data = await fvg_signals_cache.get_or_compute("all", _compute)
    waiting = data["waiting"]
    entered = data["entered"]
    if tf:
        tf_up = tf.upper()
        waiting = [s for s in waiting if (s.get("timeframe") or "").upper() == tf_up]
        entered = [s for s in entered if (s.get("timeframe") or "").upper() == tf_up]
    return {"waiting": waiting, "entered": entered, "stats": data["stats"], "tf_filter": tf}


@app.get("/api/fvg-journal")
async def api_fvg_journal(hours: int = 168, status: str = "", instrument: str = "",
                          direction: str = "", limit: int = 300, tf: str = ""):
    """Журнал Forex FVG с фильтрами.
    tf — фильтр по таймфрейму. Пусто = все TF.
    Кеш 30с по комбинации параметров — та же подборка читается часто (polling)."""
    from fvg_scanner import get_journal, get_stats
    from cache_utils import fvg_journal_cache

    cache_key = f"{hours}|{status}|{instrument}|{direction}|{limit}"

    async def _compute():
        items = await asyncio.to_thread(get_journal, hours, status or None,
                                        instrument or None, direction or None, limit)
        stats = await asyncio.to_thread(get_stats)
        return {"items": items, "stats": stats}

    data = await fvg_journal_cache.get_or_compute(cache_key, _compute)
    items = data["items"]
    if tf:
        tf_up = tf.upper()
        items = [i for i in items if (i.get("timeframe") or "").upper() == tf_up]
    return {"items": items, "stats": data["stats"], "total": len(items), "tf_filter": tf}


@app.get("/api/fvg-config")
async def api_fvg_config_get():
    from fvg_scanner import get_config, INSTRUMENTS
    cfg = await asyncio.to_thread(get_config)
    return {"config": cfg, "all_instruments": {k: list(v) for k, v in INSTRUMENTS.items()}}


@app.post("/api/fvg-config")
async def api_fvg_config_post(payload: dict):
    from fvg_scanner import save_config
    return await asyncio.to_thread(save_config, payload or {})


@app.post("/api/fvg-scan-now")
async def api_fvg_scan_now():
    """Ручной триггер скана (для теста)."""
    from fvg_scanner import scan_all, monitor_signals
    scan_stats = await asyncio.to_thread(scan_all)
    mon_events = await asyncio.to_thread(monitor_signals)
    events_summary = {k: len(v) for k, v in mon_events.items()}
    return {"scan": scan_stats, "monitor": events_summary}


@app.post("/api/tv-webhook")
async def api_tv_webhook(request: Request):
    """TradingView Webhook receiver for FVG alerts.

    Payload:
      {"secret": "...", "ticker": "EURUSD", "tf": "60",
       "direction": "bullish"|"bearish", "price": 1.0875,
       "time": "2026-04-16T10:00:01Z"}

    Returns: {"ok": bool, "reason": str, "instrument": str, "direction": str,
              "entry": float, "sl": float, "fvg_id": str}
    """
    from config import TV_WEBHOOK_SECRET
    from tv_webhook import process_tv_webhook
    import logging as _log
    try:
        payload = await request.json()
    except Exception as e:
        return Response(
            status_code=400,
            content=json.dumps({"ok": False, "error": f"invalid JSON: {e}"}),
            media_type="application/json",
        )
    if not isinstance(payload, dict):
        return Response(
            status_code=400,
            content=json.dumps({"ok": False, "error": "payload must be JSON object"}),
            media_type="application/json",
        )
    # Secret check
    provided = (payload.get("secret") or "").strip()
    if not TV_WEBHOOK_SECRET or provided != TV_WEBHOOK_SECRET:
        _log.getLogger(__name__).warning(
            f"[tv-webhook] invalid secret from {request.client.host if request.client else '?'}"
        )
        return Response(
            status_code=401,
            content=json.dumps({"ok": False, "error": "invalid secret"}),
            media_type="application/json",
        )
    try:
        result = await asyncio.to_thread(process_tv_webhook, payload)
    except Exception as e:
        import traceback
        _log.getLogger(__name__).exception(f"[tv-webhook] processing error: {e}")
        return Response(
            status_code=500,
            content=json.dumps({"ok": False, "error": str(e), "trace": traceback.format_exc()[-800:]}),
            media_type="application/json",
        )
    # Send FVG FORMED notification via BOT8 if signal created
    if result.get("ok") and result.get("reason") == "created":
        try:
            from watcher import _send_fvg_formed_alert
            # fire-and-forget — do not block webhook response
            asyncio.create_task(_send_fvg_formed_alert_safe(result))
        except Exception:
            pass
    return result


@app.get("/api/fvg-top-picks")
async def api_fvg_top_picks(hours: int = 168, limit: int = 200, min_score: int = 5):
    """FVG Top Picks — сигналы с confluence_score >= min_score за N часов.
    Cache 60s (async-lock)."""
    from cache_utils import top_picks_cache
    async def _compute():
        from fvg_top_picks import get_top_picks, get_top_picks_stats
        items = await asyncio.to_thread(get_top_picks, hours, limit, min_score)
        stats = await asyncio.to_thread(get_top_picks_stats, 720)
        return {"items": items, "stats": stats, "total": len(items)}
    return await top_picks_cache.get_or_compute(f"fvg_tp_{hours}_{limit}_{min_score}", _compute)


@app.post("/api/cv-replay-last-alert")
async def api_cv_replay_last_alert(payload: dict | None = None):
    """Диагностический endpoint — проигрывает alert для последнего CV-сигнала
    с pattern_triggered=True. Возвращает результат отправки + последние
    cv_alert_error из events.

    payload (optional): {"signal_id": 123} — если хочешь конкретный сигнал.
    """
    import traceback as _tb
    from database import _signals, _events, utcnow as _unow, Signal
    from datetime import timedelta as _td

    # 1. Find target signal from Mongo
    sig_id = (payload or {}).get("signal_id")
    if sig_id:
        doc = _signals().find_one({"id": int(sig_id)})
    else:
        doc = _signals().find_one(
            {"source": "cryptovizor", "pattern_triggered": True},
            sort=[("pattern_triggered_at", -1)],
        )
    if not doc:
        return {"ok": False, "error": "no CV signal with pattern_triggered found"}
    sig_obj = Signal.from_dict(doc)
    target = {
        "id": doc.get("id"),
        "pair": doc.get("pair"),
        "direction": doc.get("direction"),
        "entry": doc.get("entry"),
        "pattern_name": doc.get("pattern_name"),
        "pattern_price": doc.get("pattern_price"),
        "ai_score": doc.get("ai_score"),
    }

    # 2. Try to send via _send_cryptovizor_alert
    import watcher as _w
    result = {
        "ok": True,
        "signal": target,
        "bot2_ready": bool(_w._bot2),
        "bot_ready": bool(_w._bot),
        "admin_chat_id": _w._admin_chat_id,
    }
    try:
        await _w._send_cryptovizor_alert(
            sig_obj,
            target["pattern_name"] or "test",
            target["pattern_price"] or target["entry"] or 0,
            None, None, None,  # s1, r1, chart_png
        )
        result["send_attempted"] = True
    except Exception as e:
        result["ok"] = False
        result["send_exception"] = f"{type(e).__name__}: {e}"
        result["trace"] = _tb.format_exc()[-1800:]

    # 3. Collect recent cv_alert_error events
    since = _unow() - _td(hours=24)
    errors = []
    for ev in _events().find({"type": "cv_alert_error", "at": {"$gte": since}}).sort("at", -1).limit(10):
        d = ev.get("data", {})
        at = ev.get("at")
        errors.append({
            "at": at.isoformat() if hasattr(at, "isoformat") else None,
            "signal_id": d.get("signal_id"),
            "pair": d.get("pair"),
            "error": d.get("error"),
            "trace_tail": (d.get("trace") or "")[-800:],
        })
    result["recent_errors"] = errors
    result["recent_error_count"] = len(errors)
    return result


@app.post("/api/fvg-rescore-all")
async def api_fvg_rescore_all(payload: dict | None = None):
    """Пересчитать confluence_score для всех FVG за N часов (default 168h=7d).
    Используется после изменения логики scoring или бэкфилла 4H алертов."""
    from fvg_top_picks import rescore_all
    hours = int((payload or {}).get("hours", 168))
    stats = await asyncio.to_thread(rescore_all, hours)
    return {"ok": True, "stats": stats}


async def _send_fvg_formed_alert_safe(result: dict):
    """Безопасный wrapper — не валит webhook если alert module отсутствует."""
    try:
        from database import _fvg_signals
        from bson import ObjectId
        fvg_id = result.get("fvg_id")
        if not fvg_id:
            return
        sig = _fvg_signals().find_one({"_id": ObjectId(fvg_id)})
        if not sig:
            return
        try:
            from watcher import _send_fvg_formed_alert
            await _send_fvg_formed_alert(sig)
        except (ImportError, AttributeError):
            # Функция может отсутствовать — TV webhook работает и без алерта
            pass
    except Exception as e:
        import logging as _log
        _log.getLogger(__name__).debug(f"[tv-webhook] formed alert skipped: {e}")


@app.get("/api/claude-budget")
async def api_claude_budget():
    """Мониторинг расхода Claude API токенов."""
    from claude_budget import get_daily_usage
    return await asyncio.to_thread(get_daily_usage)


@app.get("/api/td-quota")
async def api_td_quota():
    """Статистика расхода TwelveData API квоты (за последние 24ч)."""
    from fvg_scanner import get_td_quota_stats
    stats = await asyncio.to_thread(get_td_quota_stats)
    return stats


@app.get("/api/top-picks")
async def api_top_picks(hours: int = 96, limit: int = 200):
    """👑 Top Picks — сигналы подтверждённые STRONG Confluence ≤ 48h.
    Cache 60s (async-lock safe)."""
    from cache_utils import top_picks_cache
    async def _compute():
        from top_picks import get_all_top_picks, get_top_picks_stats
        items = await asyncio.to_thread(get_all_top_picks, hours, limit)
        stats = await asyncio.to_thread(get_top_picks_stats, 720)
        return {"items": items, "stats": stats, "total": len(items)}
    return await top_picks_cache.get_or_compute(f"tp_{hours}_{limit}", _compute)


@app.post("/api/top-picks/backfill")
async def api_top_picks_backfill(payload: dict | None = None):
    """Пройти по истории и проставить is_top_pick на всех существующих сигналах."""
    from top_picks import backfill_top_picks
    days = int((payload or {}).get("days", 30))
    stats = await asyncio.to_thread(backfill_top_picks, days)
    return {"ok": True, "stats": stats}


@app.post("/api/ai-coin-analysis")
async def api_ai_coin_analysis(payload: dict):
    """AI Coin Analyzer — полный разбор монеты через Claude.
    payload: {pair: 'BTC/USDT' или 'BTCUSDT'}"""
    from ai_coin_analyzer import analyze_with_ai
    pair = (payload or {}).get("pair", "").strip().upper()
    if not pair:
        return {"ok": False, "error": "no pair"}
    try:
        result = await asyncio.to_thread(analyze_with_ai, pair)
        return {"ok": True, **result}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "traceback": traceback.format_exc()[-1000:]}


@app.get("/api/smart-levels")
async def api_smart_levels(symbol: str, tf: str = "1h", limit: int = 300,
                           enable: str = "clusters,sr,period,rounds,pivots"):
    """Smart Levels для графика: прошлые кластеры, auto S/R, PDH/PDL, round numbers, pivots.
    Использует Binance klines + нашу clusters collection."""
    from exchange import get_klines_any
    from smart_levels import get_smart_levels
    enable_list = [x.strip() for x in enable.split(",") if x.strip()]
    pair = symbol.replace("USDT", "/USDT") if "USDT" in symbol and "/" not in symbol else symbol
    candles_raw = await asyncio.to_thread(get_klines_any, pair, tf, limit)
    candles = []
    for c in (candles_raw or []):
        t = c.get("t")
        if t and t > 10**12:
            t = int(t / 1000)
        candles.append({
            "time": t,
            "high": c.get("h"),
            "low": c.get("l"),
            "open": c.get("o"),
            "close": c.get("c"),
        })
    levels = await asyncio.to_thread(get_smart_levels, symbol, candles, enable_list)
    return {"ok": True, "symbol": symbol, "levels": levels}


@app.get("/api/conflicts")
async def api_conflicts(hours: int = 4, limit: int = 50):
    """Активные конфликты между сигналами."""
    from anti_cluster_detector import get_active_conflicts, get_conflict_stats
    items = await asyncio.to_thread(get_active_conflicts, hours, limit)
    stats = await asyncio.to_thread(get_conflict_stats, 168)
    return {"items": items, "stats": stats}


@app.post("/api/conflicts/check")
async def api_conflicts_check(payload: dict):
    """Проверить конкретную пару+время на конфликт (для тестов/debug)."""
    from anti_cluster_detector import detect_conflict
    pair = (payload or {}).get("pair", "")
    window_h = int((payload or {}).get("window_h", 4))
    if not pair:
        return {"ok": False, "error": "no pair"}
    res = await asyncio.to_thread(detect_conflict, pair, None, window_h)
    # Сериализация datetime
    for key in ("longs", "shorts"):
        for item in res.get(key, []):
            at = item.get("at")
            if hasattr(at, "isoformat"):
                item["at"] = at.isoformat()
    return {"ok": True, "conflict": res}


@app.get("/api/fvg-candles")
async def api_fvg_candles(instrument: str, tf: str = "1h", limit: int = 150):
    """Свечи для FVG графика.
    Приоритет источников (экономим TD квоту — TD только если кеш пуст):
      1. Кеш из БД (обновляется сканером через TD каждый час)
      2. yfinance (для интрадей клики — бесплатно)
      3. TD (только как последний шанс, не на каждый клик)
    """
    from fvg_scanner import (INSTRUMENTS, fetch_candles, cache_candles,
                             get_cached_candles, fetch_candles_twelvedata)
    key = (instrument or "").upper()
    entry = INSTRUMENTS.get(key)
    if not entry:
        return {"ok": False, "error": f"unknown instrument {instrument}"}
    ticker = entry[0]
    td_symbol = entry[2] if len(entry) >= 3 else None
    period = "7d" if tf in ("15m", "30m", "1h") else "60d" if tf == "4h" else "1y"

    candles = []
    source = None
    # 1. Кеш (свежий — сканер обновляет раз в 60мин)
    if tf == "1h":
        cached = await asyncio.to_thread(get_cached_candles, key, tf, 90)
        if cached:
            candles = cached
            source = "cache"
    # 2. yfinance — бесплатно, без лимита
    if not candles:
        candles = await asyncio.to_thread(fetch_candles, ticker, period, tf)
        if candles:
            source = "yfinance"
            # Кешируем для следующих кликов
            if tf == "1h":
                try:
                    await asyncio.to_thread(cache_candles, key, tf, candles)
                except Exception:
                    pass
    # 3. TD — последний шанс (редко, экономим квоту)
    if not candles and td_symbol:
        candles = await asyncio.to_thread(fetch_candles_twelvedata, td_symbol, tf, min(limit, 200))
        if candles:
            source = "twelvedata"
    if not candles:
        return {"ok": False, "error": f"no data (all sources failed for {key} {tf})"}
    data = [{"time": c["t"], "open": c["o"], "high": c["h"],
             "low": c["l"], "close": c["c"],
             "volume": c.get("v", 0)} for c in candles[-limit:]]
    return {"ok": True, "candles": data, "ticker": ticker, "instrument": key, "source": source}


@app.get("/api/reversal-meter")
async def api_reversal_meter():
    """Composite Reversal Meter: score -100..+100 + компоненты + CV/Conf counts."""
    try:
        from reversal_meter import compute_score
        return await asyncio.to_thread(compute_score)
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()[-2000:]}


@app.post("/api/userbot/reload-session")
async def api_userbot_reload_session():
    """Форсированная перезагрузка session_userbot.session из Mongo.
    Использовать когда в Mongo залита свежая сессия, а файл в контейнере
    устаревший — без этого нужен был бы полный redeploy.

    После записи файла принудительно отключаем текущий клиент;
    supervisor auto-reconnect'ится и подхватит обновлённую сессию."""
    from database import _get_db
    import os
    here = os.path.dirname(os.path.abspath(__file__))
    session_path = os.path.join(here, "session_userbot.session")
    doc = _get_db().system.find_one({"_id": "telethon_session"})
    if not doc or "data" not in doc:
        return {"ok": False, "error": "No session document in Mongo"}
    try:
        with open(session_path, "wb") as f:
            f.write(doc["data"])
    except Exception as e:
        return {"ok": False, "error": f"write failed: {e}"}
    forced = False
    try:
        from userbot import _tg_client
        if _tg_client and _tg_client.is_connected():
            await _tg_client.disconnect()
            forced = True
    except Exception:
        pass
    return {"ok": True, "session_bytes": len(doc["data"]), "forced_reconnect": forced}


@app.get("/api/userbot-status")
async def api_userbot_status():
    """Диагностика userbot: подключён ли Telethon, когда был последний сигнал из каждого канала."""
    from database import _signals, utcnow
    from pymongo import DESCENDING
    try:
        from userbot import _tg_client
    except Exception:
        _tg_client = None
    last_cv = _signals().find_one({"source": "cryptovizor"}, sort=[("received_at", DESCENDING)])
    last_tr = _signals().find_one({"source": "tradium"}, sort=[("received_at", DESCENDING)])
    now = utcnow()
    def _info(doc):
        if not doc or not doc.get("received_at"):
            return {"at": None, "age_minutes": None, "pair": None}
        dt = doc["received_at"]
        return {
            "at": dt.isoformat() if hasattr(dt, "isoformat") else str(dt),
            "age_minutes": int((now - dt).total_seconds() / 60),
            "pair": doc.get("pair"),
        }
    # Расширенные детали supervisor'а: когда setup, сколько reconnect'ов,
    # последняя ошибка setup. Помогает при disconnect'е сразу увидеть причину.
    try:
        from userbot import get_status_details
        details = get_status_details()
    except Exception:
        details = {}
    return {
        "client_connected": _tg_client.is_connected() if _tg_client else False,
        "cryptovizor": _info(last_cv),
        "tradium": _info(last_tr),
        **details,
    }


@app.get("/")
async def root():
    return RedirectResponse(url="/signals")


# ── Signals list ─────────────────────────────────────────────────────────────

@app.get("/signals", response_class=HTMLResponse)
async def signals_list(
    request: Request,
    db: Session = Depends(get_db),
    page: int = 1,
    pair: str = "",
    direction: str = "",
    has_chart: str = "",
    tab: str = "tradium",
    bot: str = "tradium",
):
    return await asyncio.to_thread(
        _signals_list_sync, request, db, page, pair, direction, has_chart, tab, bot,
    )


def _signals_list_sync(request, db, page, pair, direction, has_chart, tab, bot):
    per_page = 20
    query = db.query(Signal).filter(Signal.source == bot)

    # Cryptovizor имеет свои вкладки
    if bot in ("anomaly", "confluence", "journal", "autotrading"):
        return templates.TemplateResponse(request, "signals.html", {
            "signals": [],
            "total": 0,
            "bot": bot, "bots": BOTS,
            "cv_tab": "", "cv_stats": {},
            "tab": tab, "stats": {}, "summary": None,
            "pages": 1, "page": 1, "pairs": [],
            "filter_pair": "", "filter_direction": "", "filter_has_chart": "",
            "eth_ctx": _sync_eth_ctx(cache_only=True), "st_eth": _sync_kc_eth(cache_only=True),
        })

    # API для аномалий
    pass  # endpoints ниже

    if bot == "cryptovizor":
        # Default вкладка = 'active' (Сигнал с паттернами) — самое полезное.
        # Раньше было 'watching' что показывало просто watchlist без сигналов.
        cv_tab = tab if tab in ("watching", "active", "ai_signal", "backtest", "ai_settings") else "active"
        if cv_tab == "watching":
            query = query.filter(Signal.status == "СЛЕЖУ")
        elif cv_tab == "active":
            query = query.filter(Signal.status.in_(["ПАТТЕРН", "AI_SIGNAL"]))
        elif cv_tab == "ai_signal":
            from database import _signals as _sc
            ai_ids = [d["id"] for d in _sc().find(
                {"source": "cryptovizor", "filter_reason": {"$regex": "^AI_SIGNAL"}},
                {"id": 1}
            )]
            if ai_ids:
                query = query.filter(Signal.id.in_(ai_ids))
            else:
                query = query.filter(Signal.status == "__none__")
        # Один агрегатный запрос вместо 4 отдельных
        from database import _signals as _sc2
        _agg = list(_sc2().aggregate([
            {"$match": {"source": "cryptovizor"}},
            {"$group": {"_id": "$status", "cnt": {"$sum": 1}}},
        ]))
        _counts = {d["_id"]: d["cnt"] for d in _agg}
        cv_watching = _counts.get("СЛЕЖУ", 0)
        cv_active = _counts.get("ПАТТЕРН", 0)
        cv_ai = _sc2().count_documents({"source": "cryptovizor", "filter_reason": {"$regex": "^AI_SIGNAL"}})
        cv_stats = {"watching": cv_watching, "active": cv_active, "ai_signal": cv_ai}
        if cv_tab in ("backtest", "ai_settings"):
            # Backtest / AI Settings — JS загружает данные
            return templates.TemplateResponse(request, "signals.html", {
                "signals": [],
                "total": 0,
                "bot": bot, "bots": BOTS,
                "cv_tab": cv_tab, "cv_stats": cv_stats,
                "tab": cv_tab, "stats": {}, "summary": None,
                "pages": 1, "page": 1, "pairs": [],
                "filter_pair": "", "filter_direction": "", "filter_has_chart": "",
                "eth_ctx": _sync_eth_ctx(cache_only=True), "st_eth": _sync_kc_eth(cache_only=True),
            })
        sort_field = Signal.pattern_triggered_at if cv_tab in ("active", "ai_signal") else Signal.received_at
        signals = query.order_by(desc(sort_field)).limit(200).all()
        return templates.TemplateResponse(request, "signals.html", {
            "signals": signals,
            "total": len(signals),
            "bot": bot, "bots": BOTS,
            "cv_tab": cv_tab, "cv_stats": cv_stats,
            "tab": cv_tab, "stats": {}, "summary": None,
            "pages": 1, "page": 1, "pairs": [],
            "filter_pair": "", "filter_direction": "", "filter_has_chart": "",
            "eth_ctx": _sync_eth_ctx(cache_only=True), "st_eth": _sync_kc_eth(cache_only=True),
        })

    # ── Фильтр по вкладке ──
    if tab == "tradium":
        query = query.filter(Signal.status == "СЛЕЖУ")
    elif tab == "dca4":
        query = query.filter(Signal.status == "ОТКРЫТ")
    elif tab == "reversal":
        query = query.filter(Signal.status == "ПАТТЕРН")
    elif tab == "results":
        query = query.filter(Signal.status.in_(["TP", "SL"]))
    elif tab == "archive":
        query = query.filter(Signal.status == "АРХИВ")

    if pair:
        query = query.filter(Signal.pair == pair.upper())
    if direction:
        query = query.filter(Signal.direction == direction.upper())
    if has_chart == "1":
        query = query.filter(Signal.has_chart == True)
    elif has_chart == "0":
        query = query.filter(Signal.has_chart == False)

    total = query.count()
    signals = query.order_by(desc(Signal.received_at)).offset((page - 1) * per_page).limit(per_page).all()
    pairs = [r[0] for r in db.query(Signal.pair).distinct().filter(Signal.pair != None).all()]

    # Сводная статистика одним GROUP BY запросом (только для текущего бота)
    from database import _signals as _signals_col
    agg = _signals_col().aggregate([
        {"$match": {"source": bot}},
        {"$group": {"_id": "$status", "cnt": {"$sum": 1}}},
    ])
    counts_by_status = {d["_id"]: d["cnt"] for d in agg}
    total_all = sum(counts_by_status.values())
    watching = counts_by_status.get("СЛЕЖУ", 0)
    open_ = counts_by_status.get("ОТКРЫТ", 0)
    tp_hit = counts_by_status.get("TP", 0)
    sl_hit = counts_by_status.get("SL", 0)
    closed = tp_hit + sl_hit
    win_rate = round((tp_hit / closed * 100) if closed else 0, 1)
    pattern_cnt = counts_by_status.get("ПАТТЕРН", 0)
    archive_cnt = counts_by_status.get("АРХИВ", 0)
    stats = {
        "total": total_all,
        "watching": watching,
        "dca4": open_,
        "open": open_,
        "tp": tp_hit,
        "sl": sl_hit,
        "win_rate": win_rate,
        "reversal": pattern_cnt,
        "results": closed,
        "archive": archive_cnt,
    }

    # Summary для вкладки Результаты (по всем закрытым сделкам текущего бота)
    summary = None
    if tab == "results":
        closed_signals = (
            db.query(Signal)
            .filter(Signal.source == bot)
            .filter(Signal.status.in_(["TP", "SL"]))
            .all()
        )
        pnls = [s.pnl_percent for s in closed_signals if s.pnl_percent is not None]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        total_pnl = sum(pnls) if pnls else 0.0
        avg_win = (sum(wins) / len(wins)) if wins else 0.0
        avg_loss = (sum(losses) / len(losses)) if losses else 0.0
        profit_factor = (sum(wins) / abs(sum(losses))) if losses and sum(losses) != 0 else 0.0
        summary = {
            "total": len(closed_signals),
            "tp": tp_hit,
            "sl": sl_hit,
            "win_rate": win_rate,
            "total_pnl": total_pnl,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "profit_factor": profit_factor,
            "best": max(pnls) if pnls else 0.0,
            "worst": min(pnls) if pnls else 0.0,
        }

    return templates.TemplateResponse(request, "signals.html", {
        "signals": signals,
        "total": total,
        "stats": stats,
        "summary": summary,
        "tab": tab,
        "bot": bot,
        "bots": BOTS,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page,
        "filter_pair": pair,
        "filter_direction": direction,
        "filter_has_chart": has_chart,
        "pairs": pairs,
        "eth_ctx": _sync_eth_ctx(cache_only=True), "st_eth": _sync_kc_eth(cache_only=True),
    })


# ── Signal detail ─────────────────────────────────────────────────────────────

@app.get("/signals/{signal_id}", response_class=HTMLResponse)
async def signal_detail(request: Request, signal_id: int, db: Session = Depends(get_db)):
    signal = db.query(Signal).filter(Signal.id == signal_id).first()
    if not signal:
        raise HTTPException(status_code=404)
    return templates.TemplateResponse(request, "signal_detail.html", {"signal": signal})


# ── API ───────────────────────────────────────────────────────────────────────

@app.get("/api/prices")
async def api_prices(pairs: str = ""):
    """Возвращает текущие цены. pairs=BTC/USDT,ETH/USDT"""
    if not pairs:
        return {}
    pair_list = [p.strip() for p in pairs.split(",") if p.strip()]
    prices = await asyncio.to_thread(_sync_get_prices, pair_list)
    # вернуть в формате {original_pair: price}
    result = {}
    for p in pair_list:
        norm = p.replace("/", "").replace("-", "").upper()
        if norm in prices:
            result[p] = prices[norm]
    return result


@app.get("/api/symbols")
async def api_symbols():
    """Все USDT-пары с Binance."""
    return sorted(await asyncio.to_thread(_sync_get_all_usdt_symbols))


@app.get("/api/stats")
async def api_stats(db: Session = Depends(get_db)):
    return {
        "total": db.query(Signal).count(),
        "forwarded": db.query(Signal).filter(Signal.is_forwarded == True).count(),
        "buy": db.query(Signal).filter(Signal.direction == "BUY").count(),
        "sell": db.query(Signal).filter(Signal.direction == "SELL").count(),
        "with_chart": db.query(Signal).filter(Signal.has_chart == True).count(),
        "ai_analyzed": db.query(Signal).filter(Signal.chart_analyzed == True).count(),
    }


@app.get("/api/signals-live")
async def api_signals_live(
    db: Session = Depends(get_db),
    tab: str = "tradium",
    bot: str = "tradium",
):
    return await asyncio.to_thread(_signals_live_sync, db, tab, bot)


def _signals_live_sync(db, tab, bot):
    """Live данные для таблицы: сигналы + статистика."""
    query = db.query(Signal).filter(Signal.source == bot)
    if tab == "tradium":
        query = query.filter(Signal.status == "СЛЕЖУ")
    elif tab == "dca4":
        query = query.filter(Signal.status == "ОТКРЫТ")
    elif tab == "reversal":
        query = query.filter(Signal.status == "ПАТТЕРН")
    elif tab == "results":
        query = query.filter(Signal.status.in_(["TP", "SL"]))
    elif tab == "archive":
        query = query.filter(Signal.status == "АРХИВ")

    signals = query.order_by(desc(Signal.received_at)).limit(200).all()

    # Одним запросом агрегируем все статусы (только для текущего бота)
    from database import _signals as _signals_col
    agg = _signals_col().aggregate([
        {"$match": {"source": bot}},
        {"$group": {"_id": "$status", "cnt": {"$sum": 1}}},
    ])
    counts_by_status = {d["_id"]: d["cnt"] for d in agg}
    watching = counts_by_status.get("СЛЕЖУ", 0)
    open_ = counts_by_status.get("ОТКРЫТ", 0)
    pattern_cnt = counts_by_status.get("ПАТТЕРН", 0)
    tp_hit = counts_by_status.get("TP", 0)
    sl_hit = counts_by_status.get("SL", 0)
    archive_cnt = counts_by_status.get("АРХИВ", 0)
    closed = tp_hit + sl_hit
    win_rate = round((tp_hit / closed * 100) if closed else 0, 1)
    total_count = sum(counts_by_status.values())

    return {
        "stats": {
            "total": total_count,
            "archive": archive_cnt,
            "watching": watching,
            "dca4": open_,
            "open": open_,
            "tp": tp_hit,
            "sl": sl_hit,
            "reversal": pattern_cnt,
            "results": closed,
            "win_rate": win_rate,
        },
        "signals": [
            {
                "id": s.id, "pair": s.pair, "direction": s.direction,
                "timeframe": s.timeframe, "entry": s.entry, "tp1": s.tp1, "sl": s.sl,
                "dca4": s.dca4, "risk_reward": s.risk_reward, "trend": s.trend,
                "status": s.status,
                "received_at": s.received_at.isoformat() if s.received_at and hasattr(s.received_at, 'isoformat') else s.received_at,
                "exit_price": s.exit_price, "pnl_percent": s.pnl_percent,
                "ai_score": s.ai_score, "ai_verdict": s.ai_verdict,
            }
            for s in signals
        ],
        "eth_ctx": _sync_eth_ctx(), "st_eth": _sync_kc_eth(),
    }


@app.get("/api/events/{signal_id}")
async def api_events(signal_id: int):
    return await asyncio.to_thread(get_events, signal_id, 100)


class DeleteBody(dict):
    pass


@app.post("/api/signals/delete")
async def api_delete_signals(payload: dict):
    """Удаляет сигналы и их события по списку id. body: {"ids": [1,2,3]}"""
    ids = payload.get("ids") or []
    ids = [int(i) for i in ids if str(i).isdigit()]
    if not ids:
        return {"ok": False, "error": "no ids"}

    def _del():
        from database import _signals, _events
        sres = _signals().delete_many({"id": {"$in": ids}})
        eres = _events().delete_many({"signal_id": {"$in": ids}})
        return sres.deleted_count, eres.deleted_count

    deleted, events = await asyncio.to_thread(_del)
    broadcast_event("signal_deleted", {"ids": ids})
    return {"ok": True, "deleted": deleted, "events_deleted": events}


@app.post("/api/signals/clear-processed")
async def api_clear_processed():
    """Удаляет отработанные Cryptovizor сигналы + сохраняет summary в историю."""
    from database import _signals as _sc, _get_db, utcnow

    # Собираем данные перед удалением
    signals = list(_sc().find(
        {"source": "cryptovizor", "status": {"$in": ["ПАТТЕРН", "VOLUME"]}},
        {"pair": 1, "direction": 1, "pattern_name": 1, "entry": 1, "pattern_price": 1, "ai_score": 1}
    ))

    if not signals:
        return {"ok": True, "deleted": 0}

    # Краткое summary
    coins = []
    for s in signals:
        entry = s.get("entry") or 0
        current = s.get("pattern_price") or entry
        pnl = ((current - entry) / entry * 100) if entry > 0 else 0
        if s.get("direction") in ("SHORT", "SELL"):
            pnl = -pnl
        coins.append({
            "pair": (s.get("pair") or "").replace("/USDT", ""),
            "dir": s.get("direction", ""),
            "pattern": s.get("pattern_name", ""),
            "pnl": round(pnl, 2),
        })

    wins = sum(1 for c in coins if c["pnl"] > 0)
    total_pnl = sum(c["pnl"] for c in coins)

    summary = {
        "date": str(utcnow()),
        "count": len(coins),
        "win_rate": round(wins / len(coins) * 100, 1) if coins else 0,
        "total_pnl": round(total_pnl, 2),
        "avg_pnl": round(total_pnl / len(coins), 2) if coins else 0,
        "coins": coins,
    }

    # Сохраняем в историю
    _get_db().backtest_history.insert_one(summary)

    # Удаляем
    result = _sc().delete_many({
        "source": "cryptovizor",
        "status": {"$in": ["ПАТТЕРН", "VOLUME"]},
    })

    broadcast_event("signal_deleted", {"count": result.deleted_count})
    return {"ok": True, "deleted": result.deleted_count, "summary": summary}


@app.get("/api/backtest/history")
async def api_backtest_history():
    """Возвращает историю бектестов."""
    from database import _get_db
    docs = list(_get_db().backtest_history.find().sort("date", -1).limit(50))
    for d in docs:
        d["_id"] = str(d["_id"])
    return docs


@app.post("/api/backtest/history/delete")
async def api_backtest_history_delete(payload: dict):
    """Удаляет одну запись из истории."""
    from database import _get_db
    from bson import ObjectId
    hid = payload.get("id", "")
    try:
        _get_db().backtest_history.delete_one({"_id": ObjectId(hid)})
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/analyze-coin")
async def api_analyze_coin(payload: dict):
    """Claude анализирует почему монета выросла/упала. Вызывается по кнопке."""
    pair = payload.get("pair", "")
    direction = payload.get("direction", "")
    entry = payload.get("entry")
    current = payload.get("current")
    pnl = payload.get("pnl")
    pattern = payload.get("pattern", "")

    if not pair:
        return {"ok": False, "error": "no pair"}

    # Если текущая цена не передана — получаем с биржи
    if current is None and pair:
        try:
            from exchange import get_prices_any as _gpa
            prices = await asyncio.to_thread(_gpa, [pair])
            norm = pair.replace("/", "").upper()
            current = prices.get(norm)
        except Exception:
            pass

    # Считаем PnL если есть entry и current
    if pnl is None and entry and current:
        try:
            raw = ((float(current) - float(entry)) / float(entry)) * 100
            pnl = round(-raw if direction in ("SHORT", "SELL") else raw, 2)
        except Exception:
            pass

    import anthropic
    from config import ANTHROPIC_API_KEY, ANTHROPIC_MODEL_FAST as ANTHROPIC_MODEL

    ectx = _sync_eth_ctx()
    eth_info = f"ETH 1h: {ectx.get('eth_1h',0):+.2f}% | BTC 1h: {ectx.get('btc_1h',0):+.2f}% | ETH/BTC: {ectx.get('eth_btc','—')}"

    prompt = (
        f"Ты — профессиональный крипто-трейдер. Дай ПОЛНЫЙ анализ сделки.\n\n"
        f"Монета: {pair}\n"
        f"Направление: {direction}\n"
        f"Паттерн: {pattern}\n"
        f"Entry: {entry}\n"
        f"Текущая цена: {current}\n"
        f"PnL: {pnl}%\n"
        f"Рынок сейчас: {eth_info}\n\n"
        f"Ответь в формате:\n\n"
        f"О МОНЕТЕ:\n"
        f"Что за проект, для чего, капитализация, ликвидность. 2-3 предложения.\n\n"
        f"ВЕРДИКТ: ENTER или SKIP\n"
        f"УВЕРЕННОСТЬ: HIGH, MEDIUM или LOW\n"
        f"SCORE: X/10\n\n"
        f"TP1: цена\n"
        f"TP2: цена\n"
        f"SL: цена\n"
        f"R:R: соотношение\n\n"
        f"АНАЛИЗ:\n"
        f"Описание сетапа, структура рынка, уровни. Как ETH/BTC влияют — коррелирует ли монета с рынком. 4-6 предложений.\n\n"
        f"РИСКИ:\n"
        f"⚠ Риск 1\n"
        f"⚠ Риск 2\n"
        f"⚠ Риск 3\n\n"
        f"На русском. БЕЗ markdown, без ## и **. Только plain text."
    )

    try:
        from ai_client import get_ai_client
        client = get_ai_client()
        message = await asyncio.to_thread(
            client.messages.create,
            model=ANTHROPIC_MODEL,
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )
        text = message.content[0].text
        return {"ok": True, "analysis": text}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/analyze-result")
async def api_analyze_result(payload: dict):
    """AI разбор: почему сделка отработала или нет."""
    pair = payload.get("pair", "")
    direction = payload.get("direction", "")
    entry = payload.get("entry", 0)
    status = payload.get("status", "")  # TP или SL
    pnl = payload.get("pnl", 0)
    exit_price = payload.get("exit_price", 0)
    tp = payload.get("tp", 0)
    sl = payload.get("sl", 0)

    import anthropic
    from config import ANTHROPIC_API_KEY, ANTHROPIC_MODEL_FAST as ANTHROPIC_MODEL

    result_text = "✅ TP (прибыль)" if status == "TP" else "❌ SL (убыток)"

    ectx = _sync_eth_ctx()
    eth_info = f"ETH 1h: {ectx.get('eth_1h',0):+.2f}% | BTC 1h: {ectx.get('btc_1h',0):+.2f}%"

    prompt = (
        f"Ты — крипто-аналитик. Разбери закрытую сделку.\n\n"
        f"Монета: {pair}\n"
        f"Направление: {direction}\n"
        f"Entry: {entry}\n"
        f"TP: {tp} | SL: {sl}\n"
        f"Результат: {result_text}\n"
        f"Exit: {exit_price} | PnL: {pnl}%\n"
        f"Рынок: {eth_info}\n\n"
        f"Объясни:\n"
        f"1. Почему сделка {'отработала' if status == 'TP' else 'не отработала'}\n"
        f"2. Как ETH/BTC повлияли — шла ли монета с рынком или против\n"
        f"3. Что было сделано правильно\n"
        f"4. Что можно улучшить\n"
        f"5. Вывод — урок из этой сделки\n\n"
        f"На русском. БЕЗ markdown, без ## и **. Только plain text. 5-7 предложений."
    )

    try:
        from ai_client import get_ai_client
        client = get_ai_client()
        message = await asyncio.to_thread(
            client.messages.create,
            model=ANTHROPIC_MODEL,
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )
        return {"ok": True, "analysis": message.content[0].text}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/anomalies")
async def api_anomalies():
    """Sync Mongo + sync httpx (_sync_eth_ctx) — выносим в to_thread,
    чтобы polling UI не блокировал event loop."""
    from database import _anomalies

    def _sync():
        docs = list(_anomalies().find().sort("detected_at", -1).limit(100))
        for d in docs:
            d["_id"] = str(d["_id"])
            if d.get("detected_at"):
                d["detected_at"] = d["detected_at"].isoformat() if hasattr(d["detected_at"], "isoformat") else str(d["detected_at"])
        return {"items": docs, "eth_ctx": _sync_eth_ctx()}

    return await asyncio.to_thread(_sync)


@app.get("/api/anomalies/scan-status")
async def api_scan_status():
    """Читает состояние скана из watcher."""
    import time as _time
    try:
        from watcher import anomaly_scan_state
        s = dict(anomaly_scan_state)
        next_at = s.pop("next_at", 0)
        if next_at > 0:
            s["next_in"] = max(0, int(next_at - _time.time()))
        else:
            s["next_in"] = 300  # watcher ещё не тикал, ~5 мин
        return s
    except ImportError:
        return {"running": False, "progress": 0, "total": 0, "found": 0, "batch": 0, "batches": 0, "current": "", "next_in": 300}


@app.get("/api/anomaly-cluster")
async def api_anomaly_cluster(symbol: str):
    """Возвращает кластерные данные (aggTrades) для символа."""
    import httpx, time
    FAPI = "https://fapi.binance.com"
    now_ms = int(time.time() * 1000)
    try:
        r = await asyncio.to_thread(
            lambda: httpx.get(f"{FAPI}/fapi/v1/aggTrades",
                              params={"symbol": symbol, "startTime": now_ms - 15 * 60 * 1000, "limit": 1000},
                              timeout=10).json()
        )
        if not r or not isinstance(r, list):
            return {"ok": False, "error": "no trades"}

        prices = [float(t["p"]) for t in r]
        price_min, price_max = min(prices), max(prices)
        price_range = price_max - price_min
        if price_range <= 0:
            return {"ok": False, "error": "no range"}

        n_levels = 25
        step = price_range / n_levels
        clusters = {}
        for t in r:
            p = float(t["p"])
            q = float(t["q"])
            level = round((p - price_min) / step) * step + price_min
            level = round(level, 8)
            if level not in clusters:
                clusters[level] = {"buy": 0.0, "sell": 0.0}
            if t.get("m"):
                clusters[level]["sell"] += q
            else:
                clusters[level]["buy"] += q

        levels = []
        for lv in sorted(clusters.keys()):
            v = clusters[lv]
            levels.append({
                "price": round(lv, 8),
                "buy": round(v["buy"], 4),
                "sell": round(v["sell"], 4),
                "delta": round(v["buy"] - v["sell"], 4),
            })
        current = prices[-1] if prices else 0
        return {"ok": True, "symbol": symbol, "levels": levels, "current": current, "trades": len(r)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/anomaly-analyze")
async def api_anomaly_analyze(payload: dict):
    """AI анализ аномалии — расшифровка + вердикт."""
    symbol = payload.get("symbol", "")
    price = payload.get("price", 0)
    score = payload.get("score", 0)
    direction = payload.get("direction", "NEUTRAL")
    anomalies = payload.get("anomalies", [])

    # Формируем описание аномалий
    anomaly_lines = []
    type_names = {
        "ftt": "FTT (Full Tail Turn) — разворотная свеча с длинной тенью и высоким объёмом",
        "delta_cluster": "Delta Cluster — дисбаланс покупок/продаж на ценовом уровне",
        "trade_speed": "Speed Print — резкое ускорение количества сделок",
        "oi_spike": "OI Spike — резкое изменение открытого интереса",
        "funding_extreme": "Funding Rate — экстремальная ставка финансирования",
        "ls_extreme": "L/S Ratio — перекос лонг/шорт позиций",
        "taker_imbalance": "Taker Imbalance — дисбаланс рыночных ордеров",
        "wall": "Order Book Wall — крупная стена в стакане",
    }
    for a in anomalies:
        t = a.get("type", "")
        v = a.get("value", "")
        desc = type_names.get(t, t)
        anomaly_lines.append(f"- {desc}: значение={v}")
        if t == "ftt":
            anomaly_lines.append(f"  FTT score: {a.get('ftt_score',0)}/5, wick: {a.get('wick_ratio',0)}, vol: ×{a.get('vol_ratio',0)}, TF: {a.get('tf','1h')}")

    anomaly_text = "\n".join(anomaly_lines)

    import anthropic
    from config import ANTHROPIC_API_KEY, ANTHROPIC_MODEL_FAST as ANTHROPIC_MODEL

    ectx = _sync_eth_ctx()
    eth_info = f"ETH 1h: {ectx.get('eth_1h',0):+.2f}% | BTC 1h: {ectx.get('btc_1h',0):+.2f}% | ETH/BTC: {ectx.get('eth_btc','—')}"

    prompt = (
        f"Ты — профессиональный крипто-аналитик. Разбери аномалию на фьючерсном рынке.\n\n"
        f"Монета: {symbol.replace('USDT','')}/USDT\n"
        f"Цена: {price}\n"
        f"Направление сигнала: {direction}\n"
        f"Score аномалии: {score}/15\n"
        f"Количество индикаторов: {len(anomalies)}\n"
        f"Рынок: {eth_info}\n\n"
        f"Обнаруженные аномалии:\n{anomaly_text}\n\n"
        f"Ответь в формате:\n\n"
        f"О МОНЕТЕ:\n"
        f"Что за проект, ликвидность. 1-2 предложения.\n\n"
        f"ВЕРДИКТ: ENTER или SKIP\n"
        f"УВЕРЕННОСТЬ: HIGH, MEDIUM или LOW\n"
        f"SCORE: X/10\n\n"
        f"TP1: цена\n"
        f"TP2: цена\n"
        f"SL: цена\n"
        f"R:R: соотношение\n\n"
        f"АНАЛИЗ:\n"
        f"Что означает совокупность этих аномалий. Почему цена может пойти в направлении {direction}. "
        f"Расшифруй каждый индикатор простым языком — что он говорит о рынке. "
        f"Как коррелирует с ETH/BTC — идёт ли монета с рынком или независимо. "
        f"Вероятность отработки. 5-8 предложений.\n\n"
        f"РИСКИ:\n"
        f"⚠ Риск 1\n"
        f"⚠ Риск 2\n"
        f"⚠ Риск 3\n\n"
        f"На русском. БЕЗ markdown, без ## и **. Только plain text."
    )

    try:
        from ai_client import get_ai_client
        client = get_ai_client()
        message = await asyncio.to_thread(
            client.messages.create,
            model=ANTHROPIC_MODEL,
            max_tokens=800,
            messages=[{"role": "user", "content": prompt}],
        )
        analysis = message.content[0].text

        # Сохраняем в MongoDB
        from database import _anomalies
        _anomalies().update_one(
            {"symbol": symbol, "score": score},
            {"$set": {"comment": analysis}},
        )

        return {"ok": True, "analysis": analysis}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/anomalies/clear")
async def api_anomalies_clear():
    from database import _anomalies
    r = _anomalies().delete_many({})
    return {"ok": True, "deleted": r.deleted_count}


@app.get("/api/anomalies/backtest")
async def api_anomalies_backtest(st: int = 0):
    """Бектест аномалий. st=1 — только прошедшие SuperTrend фильтр."""
    from database import _anomalies
    from exchange import get_futures_prices_only

    query = {}
    if st:
        query["st_passed"] = True
    docs = list(_anomalies().find(query).sort("detected_at", -1).limit(200))
    if not docs:
        return {"ok": True, "results": [], "summary": {}, "st_filter": bool(st)}

    # Текущие цены
    symbols = list({d.get("symbol", "") for d in docs if d.get("symbol")})
    pairs = [s.replace("USDT", "/USDT") for s in symbols]
    prices = await asyncio.to_thread(get_futures_prices_only, pairs)

    results = []
    wins = 0
    losses = 0
    total_pnl = 0

    for d in docs:
        sym = d.get("symbol", "")
        entry = d.get("price")
        if not entry or not sym:
            continue
        current = prices.get(sym)
        if not current:
            continue

        direction = d.get("direction", "NEUTRAL")
        raw_pnl = ((current - entry) / entry) * 100
        pnl = -raw_pnl if direction == "SHORT" else raw_pnl

        is_win = pnl > 0
        if is_win:
            wins += 1
        else:
            losses += 1
        total_pnl += pnl

        types = [a["type"] for a in d.get("anomalies", [])]
        results.append({
            "symbol": sym,
            "direction": direction,
            "score": d.get("score", 0),
            "entry": entry,
            "current": current,
            "pnl": round(pnl, 2),
            "win": is_win,
            "types": types,
            "detected_at": d["detected_at"].isoformat() if hasattr(d.get("detected_at"), "isoformat") else str(d.get("detected_at", "")),
        })

    total = wins + losses
    win_rate = round((wins / total * 100) if total else 0, 1)

    # По типу аномалии
    by_type = {}
    for r in results:
        for t in r["types"]:
            if t not in by_type:
                by_type[t] = {"wins": 0, "losses": 0, "pnl": 0}
            if r["win"]:
                by_type[t]["wins"] += 1
            else:
                by_type[t]["losses"] += 1
            by_type[t]["pnl"] += r["pnl"]
    for t in by_type:
        total_t = by_type[t]["wins"] + by_type[t]["losses"]
        by_type[t]["win_rate"] = round((by_type[t]["wins"] / total_t * 100) if total_t else 0, 1)
        by_type[t]["avg_pnl"] = round(by_type[t]["pnl"] / total_t, 2) if total_t else 0

    # По score
    by_score = {}
    for r in results:
        s = int(r["score"])
        if s not in by_score:
            by_score[s] = {"wins": 0, "losses": 0, "pnl": 0}
        if r["win"]:
            by_score[s]["wins"] += 1
        else:
            by_score[s]["losses"] += 1
        by_score[s]["pnl"] += r["pnl"]
    for s in by_score:
        total_s = by_score[s]["wins"] + by_score[s]["losses"]
        by_score[s]["win_rate"] = round((by_score[s]["wins"] / total_s * 100) if total_s else 0, 1)

    # По направлению
    by_dir = {}
    for r in results:
        d = r["direction"]
        if d not in by_dir:
            by_dir[d] = {"wins": 0, "losses": 0, "pnl": 0}
        if r["win"]:
            by_dir[d]["wins"] += 1
        else:
            by_dir[d]["losses"] += 1
        by_dir[d]["pnl"] += r["pnl"]
    for d in by_dir:
        total_d = by_dir[d]["wins"] + by_dir[d]["losses"]
        by_dir[d]["win_rate"] = round((by_dir[d]["wins"] / total_d * 100) if total_d else 0, 1)
        by_dir[d]["avg_pnl"] = round(by_dir[d]["pnl"] / total_d, 2) if total_d else 0

    # По комбинациям (какие пары типов лучше отрабатывают)
    by_combo = {}
    for r in results:
        key_types = sorted([t for t in r["types"] if t in ("ftt", "delta_cluster", "trade_speed", "oi_spike")])
        if len(key_types) < 2:
            continue
        combo = " + ".join(key_types)
        if combo not in by_combo:
            by_combo[combo] = {"wins": 0, "losses": 0, "pnl": 0}
        if r["win"]:
            by_combo[combo]["wins"] += 1
        else:
            by_combo[combo]["losses"] += 1
        by_combo[combo]["pnl"] += r["pnl"]
    for c in by_combo:
        total_c = by_combo[c]["wins"] + by_combo[c]["losses"]
        by_combo[c]["win_rate"] = round((by_combo[c]["wins"] / total_c * 100) if total_c else 0, 1)
        by_combo[c]["avg_pnl"] = round(by_combo[c]["pnl"] / total_c, 2) if total_c else 0
        by_combo[c]["count"] = total_c

    # Лучшие и худшие
    sorted_results = sorted(results, key=lambda x: x["pnl"], reverse=True)
    best_5 = sorted_results[:5]
    worst_5 = sorted_results[-5:]

    # По часу обнаружения
    by_hour = {}
    for r in results:
        try:
            h = int(r["detected_at"][11:13])
        except Exception:
            h = 0
        if h not in by_hour:
            by_hour[h] = {"wins": 0, "losses": 0, "pnl": 0}
        if r["win"]:
            by_hour[h]["wins"] += 1
        else:
            by_hour[h]["losses"] += 1
        by_hour[h]["pnl"] += r["pnl"]
    for h in by_hour:
        total_h = by_hour[h]["wins"] + by_hour[h]["losses"]
        by_hour[h]["win_rate"] = round((by_hour[h]["wins"] / total_h * 100) if total_h else 0, 1)

    return {
        "ok": True,
        "summary": {
            "total": total, "wins": wins, "losses": losses,
            "win_rate": win_rate,
            "total_pnl": round(total_pnl, 2),
            "avg_pnl": round(total_pnl / total, 2) if total else 0,
        },
        "by_type": by_type,
        "by_score": by_score,
        "by_direction": by_dir,
        "by_combo": by_combo,
        "by_hour": {str(k): v for k, v in sorted(by_hour.items())},
        "best": best_5,
        "worst": worst_5,
        "results": sorted(results, key=lambda x: -abs(x["pnl"]))[:50],
        "st_filter": bool(st),
    }


# ── Confluence API ────────────────────────────────────────────────────

@app.get("/api/confluence")
async def api_confluence():
    """Sync Mongo + sync httpx (_sync_eth_ctx) — выносим в to_thread."""
    from database import _confluence

    def _sync():
        docs = list(_confluence().find().sort("detected_at", -1).limit(100))
        for d in docs:
            d["_id"] = str(d["_id"])
            if d.get("detected_at"):
                d["detected_at"] = d["detected_at"].isoformat() if hasattr(d["detected_at"], "isoformat") else str(d["detected_at"])
        return {"items": docs, "eth_ctx": _sync_eth_ctx()}

    return await asyncio.to_thread(_sync)


@app.get("/api/confluence/scan-status")
async def confluence_scan_status():
    import time as _time
    try:
        from watcher import confluence_scan_state
        s = dict(confluence_scan_state)
        next_at = s.pop("next_at", 0)
        s["next_in"] = max(0, int(next_at - _time.time())) if next_at > 0 else 300
        return s
    except ImportError:
        return {"running": False, "progress": 0, "total": 0, "found": 0, "current": "", "next_in": 300}


@app.post("/api/confluence/clear")
async def api_confluence_clear():
    from database import _confluence
    r = _confluence().delete_many({})
    return {"ok": True, "deleted": r.deleted_count}


@app.get("/api/confluence/backtest")
async def api_confluence_backtest(st: int = 0):
    """st=1 — только прошедшие SuperTrend фильтр."""
    from database import _confluence
    from exchange import get_futures_prices_only

    query = {"st_passed": True} if st else {}
    docs = list(_confluence().find(query).sort("detected_at", -1).limit(200))
    if not docs:
        return {"ok": True, "results": [], "summary": {}}

    symbols = list({d.get("symbol", "") for d in docs if d.get("symbol")})
    pairs = [s.replace("USDT", "/USDT") for s in symbols]
    prices = await asyncio.to_thread(get_futures_prices_only, pairs)

    results = []
    wins = losses = 0
    total_pnl = 0

    for d in docs:
        sym = d.get("symbol", "")
        entry = d.get("price")
        if not entry or not sym:
            continue
        current = prices.get(sym)
        if not current:
            continue

        direction = d.get("direction", "NEUTRAL")
        raw_pnl = ((current - entry) / entry) * 100
        pnl = -raw_pnl if direction == "SHORT" else raw_pnl
        is_win = pnl > 0
        if is_win: wins += 1
        else: losses += 1
        total_pnl += pnl

        ftypes = [f["type"] for f in d.get("factors", [])]
        results.append({
            "symbol": sym, "direction": direction, "score": d.get("score", 0),
            "strength": d.get("strength", ""), "entry": entry, "current": current,
            "pnl": round(pnl, 2), "win": is_win, "factors": ftypes,
            "detected_at": d.get("detected_at", ""),
        })

    total = wins + losses
    wr = round((wins / total * 100) if total else 0, 1)

    # По score
    by_score = {}
    for r in results:
        s = r["score"]
        if s not in by_score:
            by_score[s] = {"wins": 0, "losses": 0, "pnl": 0}
        if r["win"]: by_score[s]["wins"] += 1
        else: by_score[s]["losses"] += 1
        by_score[s]["pnl"] += r["pnl"]
    for s in by_score:
        t = by_score[s]["wins"] + by_score[s]["losses"]
        by_score[s]["win_rate"] = round((by_score[s]["wins"] / t * 100) if t else 0, 1)

    # По strength
    by_strength = {}
    for r in results:
        st = r["strength"]
        if st not in by_strength:
            by_strength[st] = {"wins": 0, "losses": 0, "pnl": 0}
        if r["win"]: by_strength[st]["wins"] += 1
        else: by_strength[st]["losses"] += 1
        by_strength[st]["pnl"] += r["pnl"]
    for st in by_strength:
        t = by_strength[st]["wins"] + by_strength[st]["losses"]
        by_strength[st]["win_rate"] = round((by_strength[st]["wins"] / t * 100) if t else 0, 1)

    sorted_results = sorted(results, key=lambda x: x["pnl"], reverse=True)

    return {
        "ok": True,
        "summary": {
            "total": total, "wins": wins, "losses": losses,
            "win_rate": wr,
            "total_pnl": round(total_pnl, 2),
            "avg_pnl": round(total_pnl / total, 2) if total else 0,
        },
        "by_score": by_score,
        "by_strength": by_strength,
        "best": sorted_results[:5],
        "worst": sorted_results[-5:],
        "results": sorted_results[:50],
        "st_filter": bool(st),
    }


# ── Journal API ──────────────────────────────────────────────────────

# ── Paper Trading API ─────────────────────────────────────────────────

@app.get("/api/header-data")
async def api_header_data():
    """ETH/BTC + Keltner для автообновления шапки."""
    return {"eth_ctx": _sync_eth_ctx(), "st_eth": _sync_kc_eth()}


@app.get("/api/paper/status")
async def api_paper_status():
    import paper_trader as pt
    balance = pt.get_balance()
    positions = pt.get_open_positions()
    stats = pt.get_stats()
    # Текущие цены для PnL
    from exchange import get_prices_any
    if positions:
        pairs = [p.get("pair", p["symbol"].replace("USDT", "/USDT")) for p in positions]
        prices = await asyncio.to_thread(get_prices_any, pairs)
        for p in positions:
            cur = prices.get(p["symbol"])
            if cur and p.get("entry"):
                raw = ((cur - p["entry"]) / p["entry"]) * 100
                pnl = -raw if p["direction"] == "SHORT" else raw
                p["live_pnl"] = round(pnl * p.get("leverage", 1), 2)
                p["live_price"] = cur
            p["_id"] = str(p.get("_id", ""))
            if p.get("opened_at") and hasattr(p["opened_at"], "isoformat"):
                p["opened_at"] = p["opened_at"].isoformat()
    return {
        "balance": balance,
        "initial": pt.INITIAL_BALANCE,
        "pnl_pct": round((balance - pt.INITIAL_BALANCE) / pt.INITIAL_BALANCE * 100, 2),
        "positions": positions,
        "stats": stats,
    }


@app.get("/api/paper/history")
async def api_paper_history(limit: int = 50):
    import paper_trader as pt
    history = pt.get_history(limit)
    for h in history:
        h["_id"] = str(h.get("_id", ""))
        for f in ("opened_at", "closed_at"):
            if h.get(f) and hasattr(h[f], "isoformat"):
                h[f] = h[f].isoformat()
    return {"items": history}


_by_state: dict = {
    "running": False, "started_at": None, "finished_at": None,
    "progress": {"processed": 0, "total": 0, "current": ""},
    "result": None, "error": None,
    "hours": 24, "forward_hours": 48,
}


async def _run_backtest_yesterday(hours: int, forward_hours: int):
    from datetime import datetime as _dt
    try:
        result = await asyncio.to_thread(_backtest_yesterday_sync, hours, forward_hours)
        _by_state["result"] = result
    except Exception as e:
        _by_state["error"] = str(e)
        logging.getLogger(__name__).exception("[backtest-yesterday] crashed")
    finally:
        _by_state["running"] = False
        _by_state["finished_at"] = _dt.utcnow().isoformat()


def _backtest_yesterday_sync(hours: int, forward_hours: int) -> dict:
    """Синхронная версия (вызывается через to_thread из background task)."""
    from database import (_signals, _anomalies, _confluence, _clusters,
                          _supertrend_signals, utcnow)
    from datetime import timedelta
    from exchange import get_klines_any
    import logging as _log
    logger = _log.getLogger(__name__)

    candles_15m: dict[str, list] = {}
    candles_1h:  dict[str, list] = {}

    def _get_15m(pair: str):
        if pair in candles_15m:
            return candles_15m[pair]
        c = get_klines_any(pair, "15m", 500) or []
        candles_15m[pair] = c
        return c

    def _get_1h(pair: str):
        if pair in candles_1h:
            return candles_1h[pair]
        c = get_klines_any(pair, "1h", 30) or []
        candles_1h[pair] = c
        return c

    def _simulate(pair, direction, entry, tp1, sl, opened_at, forward_h):
        if entry is None or sl is None:
            return {"ok": False, "error": "no entry/sl"}
        candles = _get_15m(pair)
        if not candles:
            return {"ok": False, "error": "no candles"}
        opened_ts_ms = int(opened_at.timestamp() * 1000) if hasattr(opened_at, "timestamp") else 0
        end_ms = opened_ts_ms + forward_h * 3600 * 1000
        is_long = direction == "LONG"
        risk = abs(entry - sl)
        if risk <= 0:
            return {"ok": False, "error": "zero risk"}
        bar_count = 0
        last_c = None
        for c in candles:
            if c["t"] < opened_ts_ms or c["t"] > end_ms:
                continue
            bar_count += 1
            last_c = c
            hi, lo = c["h"], c["l"]
            if is_long:
                tp_hit = tp1 and hi >= tp1
                sl_hit = lo <= sl
            else:
                tp_hit = tp1 and lo <= tp1
                sl_hit = hi >= sl
            if sl_hit:
                return {"ok": True, "what": "SL", "exit": sl, "r": -1.0,
                        "pnl_pct": round((sl - entry) / entry * 100 * (1 if is_long else -1), 2),
                        "bars_held": bar_count}
            if tp_hit:
                reward = abs(tp1 - entry)
                return {"ok": True, "what": "TP", "exit": tp1,
                        "r": round(reward / risk, 2),
                        "pnl_pct": round((tp1 - entry) / entry * 100 * (1 if is_long else -1), 2),
                        "bars_held": bar_count}
        if bar_count == 0 or last_c is None:
            return {"ok": False, "error": "no candles in window"}
        last = last_c["c"]
        r = (last - entry) / risk if is_long else (entry - last) / risk
        return {"ok": True, "what": "OPEN", "exit": last, "r": round(r, 3),
                "pnl_pct": round((last - entry) / entry * 100 * (1 if is_long else -1), 2),
                "bars_held": bar_count}

    def _atr_sl_tp(pair, direction, entry):
        candles = _get_1h(pair)
        if not candles or len(candles) < 15:
            return None, None
        trs = []
        for i in range(1, len(candles)):
            c, pc = candles[i], candles[i-1]["c"]
            trs.append(max(c["h"] - c["l"], abs(c["h"] - pc), abs(c["l"] - pc)))
        if not trs:
            return None, None
        atr = sum(trs[-14:]) / min(14, len(trs))
        if direction == "LONG":
            return entry - atr * 1.5, entry + atr * 2.5
        return entry + atr * 1.5, entry - atr * 2.5

    since = utcnow() - timedelta(hours=hours)
    raw: list[dict] = []

    # 1. Tradium
    for s in _signals().find({"source": "tradium", "received_at": {"$gte": since}}):
        at = s.get("pattern_triggered_at") or s.get("received_at")
        raw.append({"source": "tradium", "pair": s.get("pair"),
                    "symbol": (s.get("pair") or "").replace("/", "").upper(),
                    "direction": s.get("direction"),
                    "entry": s.get("entry"), "tp1": s.get("tp1"), "sl": s.get("sl"),
                    "at": at, "score": s.get("ai_score")})
    # 2. Cryptovizor
    for s in _signals().find({"source": "cryptovizor", "pattern_triggered": True,
                              "pattern_triggered_at": {"$gte": since}}):
        raw.append({"source": "cryptovizor", "pair": s.get("pair"),
                    "symbol": (s.get("pair") or "").replace("/", "").upper(),
                    "direction": s.get("direction"),
                    "entry": s.get("pattern_price") or s.get("entry"),
                    "tp1": s.get("dca2"), "sl": s.get("dca1"),
                    "at": s.get("pattern_triggered_at"), "score": s.get("ai_score")})
    # 3. Anomaly — топ-200 по score
    for a in _anomalies().find({"detected_at": {"$gte": since}}).sort("score", -1).limit(200):
        sym = (a.get("symbol") or "").upper()
        pair = a.get("pair") or (sym.replace("USDT", "/USDT") if sym.endswith("USDT") else None)
        raw.append({"source": "anomaly", "pair": pair, "symbol": sym,
                    "direction": a.get("direction"), "entry": a.get("price"),
                    "tp1": None, "sl": None,
                    "at": a.get("detected_at"), "score": a.get("score")})
    # 4. Confluence — топ-300 по score
    for c in _confluence().find({"detected_at": {"$gte": since}}).sort("score", -1).limit(300):
        raw.append({"source": "confluence", "pair": c.get("pair"),
                    "symbol": (c.get("symbol") or "").upper(),
                    "direction": c.get("direction"),
                    "entry": c.get("price"), "tp1": c.get("r1"), "sl": c.get("s1"),
                    "at": c.get("detected_at"), "score": c.get("score")})
    # 5. Cluster
    for cl in _clusters().find({"trigger_at": {"$gte": since}}):
        raw.append({"source": "cluster", "pair": cl.get("pair"),
                    "symbol": (cl.get("pair") or "").replace("/", "").upper(),
                    "direction": cl.get("direction"),
                    "entry": cl.get("trigger_price"),
                    "tp1": cl.get("tp_price"), "sl": cl.get("sl_price"),
                    "at": cl.get("trigger_at"), "score": cl.get("reversal_score")})
    # 6. SuperTrend VIP+MTF
    for s in _supertrend_signals().find({"flip_at": {"$gte": since},
                                         "tier": {"$in": ["vip", "mtf"]}}):
        raw.append({"source": f"supertrend_{s.get('tier','?')}",
                    "pair": s.get("pair"), "symbol": s.get("pair_norm"),
                    "direction": s.get("direction"),
                    "entry": s.get("entry_price"),
                    "tp1": None, "sl": s.get("sl_price"),
                    "at": s.get("flip_at"), "score": None, "tier": s.get("tier")})

    _by_state["progress"]["total"] = len(raw)
    logger.info(f"[backtest-yesterday] собрано сигналов: {len(raw)}")

    items = []
    stats_by_source: dict = {}
    sim_errors = 0
    processed = 0
    for sig in raw:
        processed += 1
        _by_state["progress"]["processed"] = processed
        _by_state["progress"]["current"] = f"{sig.get('source','')} {sig.get('symbol','')}"
        try:
            pair = sig.get("pair")
            direction = sig.get("direction")
            entry = sig.get("entry")
            at = sig.get("at")
            if not (pair and direction in ("LONG", "SHORT") and entry and at):
                continue
            tp1 = sig.get("tp1"); sl = sig.get("sl")
            if not sl:
                sl_atr, tp_atr = _atr_sl_tp(pair, direction, entry)
                sl = sl_atr
                if not tp1: tp1 = tp_atr
            if not sl: continue
            if not tp1:
                _, tp_atr = _atr_sl_tp(pair, direction, entry)
                tp1 = tp_atr
            if not tp1: continue
            sim = _simulate(pair, direction, entry, tp1, sl, at, forward_hours)
            if not sim.get("ok"):
                continue
            src = sig["source"]
            st = stats_by_source.setdefault(src, {"count": 0, "wins": 0, "losses": 0,
                                                  "open": 0, "sum_r": 0.0, "sum_pct": 0.0})
            st["count"] += 1
            if sim["what"] == "TP": st["wins"] += 1
            elif sim["what"] == "SL": st["losses"] += 1
            else: st["open"] += 1
            st["sum_r"] += sim.get("r") or 0
            st["sum_pct"] += sim.get("pnl_pct") or 0
            items.append({"source": src, "symbol": sig.get("symbol"), "pair": pair,
                         "direction": direction, "score": sig.get("score"),
                         "entry": entry, "tp1": round(tp1, 8), "sl": round(sl, 8),
                         "at": at.isoformat() if hasattr(at, "isoformat") else str(at),
                         "what": sim["what"], "exit": sim.get("exit"),
                         "r": sim.get("r"), "pnl_pct": sim.get("pnl_pct"),
                         "bars_held": sim.get("bars_held")})
        except Exception as _e:
            sim_errors += 1

    summary_rows = []
    for src, st in stats_by_source.items():
        closed = st["wins"] + st["losses"]
        wr = round(st["wins"] / closed * 100, 1) if closed else 0
        avg_r = round(st["sum_r"] / st["count"], 3) if st["count"] else 0
        wr_sum = sum(i["r"] for i in items if i.get("source") == src and (i.get("r") or 0) > 0)
        lr_sum = abs(sum(i["r"] for i in items if i.get("source") == src and (i.get("r") or 0) <= 0 and i.get("what") != "OPEN"))
        pf = round(wr_sum / lr_sum, 2) if lr_sum > 0 else None
        summary_rows.append({"source": src, "count": st["count"], "wins": st["wins"],
                            "losses": st["losses"], "open": st["open"],
                            "wr": wr, "avg_r": avg_r, "pf": pf,
                            "sum_r": round(st["sum_r"], 2),
                            "sum_pct": round(st["sum_pct"], 2)})
    summary_rows.sort(key=lambda x: -(x.get("sum_r") or 0))

    total_count = sum(r["count"] for r in summary_rows)
    total_wins = sum(r["wins"] for r in summary_rows)
    total_losses = sum(r["losses"] for r in summary_rows)
    total_r = sum(r["sum_r"] for r in summary_rows)
    total_pct = sum(r["sum_pct"] for r in summary_rows)

    return {
        "ok": True, "hours": hours, "forward_hours": forward_hours,
        "raw_signals": len(raw), "sim_errors": sim_errors,
        "pairs_cached_15m": len(candles_15m),
        "total": {
            "signals": total_count, "wins": total_wins, "losses": total_losses,
            "wr": round(total_wins / max(total_wins + total_losses, 1) * 100, 1),
            "sum_r": round(total_r, 2), "sum_pct": round(total_pct, 2),
        },
        "by_source": summary_rows,
        "items_count": len(items),
        "items_top": items[:80],  # чтобы не раздувать response
    }


@app.post("/api/backtest-yesterday")
async def api_backtest_yesterday_start(payload: dict | None = None):
    """Запускает фоновый бектест всех сигналов за последние N часов.
    Статус через GET /api/backtest-yesterday/status."""
    from datetime import datetime as _dt
    if _by_state.get("running"):
        return {"ok": False, "error": "already running", "state": _by_state}
    hours = int((payload or {}).get("hours", 24))
    forward_hours = int((payload or {}).get("forward_hours", 48))
    _by_state.update({
        "running": True, "started_at": _dt.utcnow().isoformat(),
        "finished_at": None,
        "progress": {"processed": 0, "total": 0, "current": ""},
        "result": None, "error": None,
        "hours": hours, "forward_hours": forward_hours,
    })
    asyncio.create_task(_run_backtest_yesterday(hours, forward_hours))
    return {"ok": True, "started": True, "hours": hours, "forward_hours": forward_hours}


@app.get("/api/backtest-yesterday/status")
async def api_backtest_yesterday_status():
    return _by_state


# ═══════════════════════════════════════════════════════════════════
# BACKTEST OPTIMIZE — прогон бектеста по одному источнику с разными
# фильтрами. Возвращает табличку: фильтр → count/WR/avgR/PF/sumR.
# ═══════════════════════════════════════════════════════════════════
_bo_state: dict = {
    "running": False, "started_at": None, "finished_at": None,
    "progress": {"processed": 0, "total": 0, "current": ""},
    "result": None, "error": None,
    "source": None, "hours": 168, "forward_hours": 72,
}


def _simulate_signal(candles_15m: list, direction: str, entry: float,
                      tp1: float, sl: float, opened_at, forward_h: int) -> dict:
    """Одна симуляция: касание TP или SL на свечах 15m.
    Возвращает {ok, what: TP|SL|OPEN, r, pnl_pct}."""
    if not candles_15m or entry is None or sl is None:
        return {"ok": False}
    opened_ts_ms = int(opened_at.timestamp() * 1000) if hasattr(opened_at, "timestamp") else 0
    end_ms = opened_ts_ms + forward_h * 3600 * 1000
    is_long = direction == "LONG"
    risk = abs(entry - sl)
    if risk <= 0:
        return {"ok": False}
    bar_count = 0
    last_c = None
    for c in candles_15m:
        if c["t"] < opened_ts_ms or c["t"] > end_ms:
            continue
        bar_count += 1
        last_c = c
        hi, lo = c["h"], c["l"]
        if is_long:
            tp_hit = tp1 and hi >= tp1
            sl_hit = lo <= sl
        else:
            tp_hit = tp1 and lo <= tp1
            sl_hit = hi >= sl
        if sl_hit:
            return {"ok": True, "what": "SL", "r": -1.0,
                    "pnl_pct": round((sl - entry) / entry * 100 * (1 if is_long else -1), 2)}
        if tp_hit:
            reward = abs(tp1 - entry)
            return {"ok": True, "what": "TP",
                    "r": round(reward / risk, 2),
                    "pnl_pct": round((tp1 - entry) / entry * 100 * (1 if is_long else -1), 2)}
    if bar_count == 0 or last_c is None:
        return {"ok": False}
    last = last_c["c"]
    r = (last - entry) / risk if is_long else (entry - last) / risk
    return {"ok": True, "what": "OPEN", "r": round(r, 3),
            "pnl_pct": round((last - entry) / entry * 100 * (1 if is_long else -1), 2)}


def _atr_tp_sl(candles_1h: list, direction: str, entry: float) -> tuple[float | None, float | None]:
    if not candles_1h or len(candles_1h) < 15:
        return None, None
    trs = []
    for i in range(1, len(candles_1h)):
        c, pc = candles_1h[i], candles_1h[i-1]["c"]
        trs.append(max(c["h"] - c["l"], abs(c["h"] - pc), abs(c["l"] - pc)))
    if not trs:
        return None, None
    atr = sum(trs[-14:]) / min(14, len(trs))
    if direction == "LONG":
        return entry - atr * 1.5, entry + atr * 2.5
    return entry + atr * 1.5, entry - atr * 2.5


def _btc_trend_at_ts(btc_1h: list, ts_ms: int) -> str:
    """Грубый тренд: close[i] > close[i-3] → UP, иначе DOWN."""
    if not btc_1h:
        return "UNK"
    for idx in range(len(btc_1h) - 1, -1, -1):
        if btc_1h[idx]["t"] <= ts_ms:
            if idx < 3:
                return "UNK"
            return "UP" if btc_1h[idx]["c"] > btc_1h[idx-3]["c"] else "DOWN"
    return "UNK"


def _agg_stats(sims: list[dict]) -> dict:
    """Принимает список {what, r, pnl_pct, direction}. Возвращает агрегат."""
    n = len(sims)
    if n == 0:
        return {"count": 0}
    wins = sum(1 for s in sims if s.get("what") == "TP")
    losses = sum(1 for s in sims if s.get("what") == "SL")
    open_n = sum(1 for s in sims if s.get("what") == "OPEN")
    closed = wins + losses
    wr = round(wins / closed * 100, 1) if closed else 0.0
    sum_r = round(sum(s.get("r") or 0 for s in sims), 2)
    sum_pct = round(sum(s.get("pnl_pct") or 0 for s in sims), 1)
    avg_r = round(sum_r / n, 3)
    wr_sum = sum(s["r"] for s in sims if (s.get("r") or 0) > 0)
    lr_sum = abs(sum(s["r"] for s in sims if (s.get("r") or 0) <= 0 and s.get("what") != "OPEN"))
    pf = round(wr_sum / lr_sum, 2) if lr_sum > 0 else None
    return {"count": n, "wins": wins, "losses": losses, "open": open_n,
            "wr": wr, "avg_r": avg_r, "pf": pf,
            "sum_r": sum_r, "sum_pct": sum_pct}


def _backtest_optimize_sync(source: str, hours: int, forward_hours: int) -> dict:
    """Главная функция. Источники: confluence, cluster, supertrend_vip,
    supertrend_mtf, anomaly, cryptovizor."""
    from database import (_signals, _anomalies, _confluence, _clusters,
                          _supertrend_signals, utcnow)
    from datetime import timedelta
    from exchange import get_klines_any
    import logging as _log
    logger = _log.getLogger(__name__)
    since = utcnow() - timedelta(hours=hours)

    # 1. Собираем raw сигналы с полной мета-инфой для фильтров
    raw: list[dict] = []
    if source == "confluence":
        for c in _confluence().find({"detected_at": {"$gte": since}}):
            raw.append({
                "pair": c.get("pair"),
                "direction": c.get("direction"),
                "entry": c.get("price"), "tp1": c.get("r1"), "sl": c.get("s1"),
                "at": c.get("detected_at"),
                "score": c.get("score") or 0,
                "st_passed": bool(c.get("st_passed")),
                "is_top_pick": bool(c.get("is_top_pick")),
                "factors": len(c.get("factors", [])),
                "pump_score": c.get("pump_score") or 0,
            })
    elif source == "cluster":
        for cl in _clusters().find({"trigger_at": {"$gte": since}}):
            raw.append({
                "pair": cl.get("pair"),
                "direction": cl.get("direction"),
                "entry": cl.get("trigger_price"),
                "tp1": cl.get("tp_price"), "sl": cl.get("sl_price"),
                "at": cl.get("trigger_at"),
                "strength": cl.get("strength", "NORMAL"),
                "sources_count": cl.get("sources_count") or 0,
                "signals_count": cl.get("signals_count") or 0,
                "reversal_score": cl.get("reversal_score") or 0,
            })
    elif source in ("supertrend_vip", "supertrend_mtf"):
        tier = "vip" if source == "supertrend_vip" else "mtf"
        for s in _supertrend_signals().find({"flip_at": {"$gte": since}, "tier": tier}):
            raw.append({
                "pair": s.get("pair"),
                "direction": s.get("direction"),
                "entry": s.get("entry_price"), "tp1": None, "sl": s.get("sl_price"),
                "at": s.get("flip_at"),
                "aligned_tfs": s.get("aligned_tfs", []),
                "aligned_bots_count": len(s.get("aligned_bots", [])),
                "aligned_sources": list({ab.get("source", "?") for ab in s.get("aligned_bots", [])}),
            })
    elif source == "anomaly":
        for a in _anomalies().find({"detected_at": {"$gte": since}}):
            sym = (a.get("symbol") or "").upper()
            pair = a.get("pair") or (sym.replace("USDT", "/USDT") if sym.endswith("USDT") else None)
            raw.append({
                "pair": pair,
                "direction": a.get("direction"),
                "entry": a.get("price"), "tp1": None, "sl": None,
                "at": a.get("detected_at"),
                "score": a.get("score") or 0,
                "types": [x.get("type") for x in a.get("anomalies", [])],
                "types_count": len(a.get("anomalies", [])),
            })
    elif source == "cryptovizor":
        for s in _signals().find({"source": "cryptovizor", "pattern_triggered": True,
                                  "pattern_triggered_at": {"$gte": since}}):
            raw.append({
                "pair": s.get("pair"),
                "direction": s.get("direction"),
                "entry": s.get("pattern_price") or s.get("entry"),
                "tp1": s.get("dca2"), "sl": s.get("dca1"),
                "at": s.get("pattern_triggered_at"),
                "pattern_name": s.get("pattern_name", "?"),
                "ai_score": s.get("ai_score") or 0,
                "st_passed": bool(s.get("st_passed")),
            })
    else:
        return {"error": f"unknown source {source}"}

    _bo_state["progress"]["total"] = len(raw)
    logger.info(f"[backtest-optimize] {source}: собрано {len(raw)} сигналов")

    # 2. Кеш свечей по pair + BTC 1h для context
    candles_15m_cache: dict = {}
    candles_1h_cache: dict = {}
    btc_1h = get_klines_any("BTC/USDT", "1h", max(hours + 24, 200)) or []

    def _get_15m(pair: str):
        if pair in candles_15m_cache:
            return candles_15m_cache[pair]
        c = get_klines_any(pair, "15m", max(forward_hours * 4 + hours * 4, 700)) or []
        candles_15m_cache[pair] = c
        return c

    def _get_1h(pair: str):
        if pair in candles_1h_cache:
            return candles_1h_cache[pair]
        c = get_klines_any(pair, "1h", 30) or []
        candles_1h_cache[pair] = c
        return c

    # 3. Симулируем каждый сигнал → прикрепляем результат
    processed = 0
    for sig in raw:
        processed += 1
        _bo_state["progress"]["processed"] = processed
        _bo_state["progress"]["current"] = sig.get("pair", "")
        try:
            pair = sig.get("pair")
            direction = sig.get("direction")
            entry = sig.get("entry")
            at = sig.get("at")
            if not (pair and direction in ("LONG", "SHORT") and entry and at):
                sig["_sim"] = {"ok": False}
                continue
            tp1, sl = sig.get("tp1"), sig.get("sl")
            if not sl:
                sl_atr, tp_atr = _atr_tp_sl(_get_1h(pair), direction, entry)
                sl = sl_atr
                if not tp1: tp1 = tp_atr
            if not sl:
                sig["_sim"] = {"ok": False}
                continue
            if not tp1:
                _, tp_atr = _atr_tp_sl(_get_1h(pair), direction, entry)
                tp1 = tp_atr
            if not tp1:
                sig["_sim"] = {"ok": False}
                continue
            sim = _simulate_signal(_get_15m(pair), direction, entry, tp1, sl, at, forward_hours)
            # BTC trend на момент сигнала
            at_ms = int(at.timestamp() * 1000) if hasattr(at, "timestamp") else 0
            sig["_btc_trend"] = _btc_trend_at_ts(btc_1h, at_ms)
            sig["_sim"] = sim
        except Exception as _e:
            sig["_sim"] = {"ok": False, "err": str(_e)}

    # 4. Фильтры по источнику
    good = [s for s in raw if s.get("_sim", {}).get("ok")]

    def _apply(name: str, predicate, comment: str = "") -> dict:
        filtered = [s for s in good if predicate(s)]
        sims = [s["_sim"] for s in filtered]
        stats = _agg_stats(sims)
        stats["name"] = name
        stats["comment"] = comment
        # Пропорция LONG/SHORT для диагностики
        if filtered:
            longs = sum(1 for s in filtered if s.get("direction") == "LONG")
            stats["long_pct"] = round(longs / len(filtered) * 100, 0)
        return stats

    rows = [_apply("BASELINE", lambda s: True, "все сигналы")]

    if source == "confluence":
        rows += [
            _apply("score>=5", lambda s: s["score"] >= 5),
            _apply("score>=6", lambda s: s["score"] >= 6),
            _apply("score>=7", lambda s: s["score"] >= 7),
            _apply("st_passed", lambda s: s["st_passed"]),
            _apply("factors>=5", lambda s: s["factors"] >= 5),
            _apply("factors>=6", lambda s: s["factors"] >= 6),
            _apply("LONG only", lambda s: s["direction"] == "LONG"),
            _apply("SHORT only", lambda s: s["direction"] == "SHORT"),
            _apply("score>=6+st_passed", lambda s: s["score"] >= 6 and s["st_passed"]),
            _apply("score>=6+top_pick", lambda s: s["score"] >= 6 and s["is_top_pick"]),
            _apply("score>=6+st+TP", lambda s: s["score"] >= 6 and s["st_passed"] and s["is_top_pick"]),
        ]
    elif source == "cluster":
        rows += [
            _apply("NORMAL", lambda s: s["strength"] == "NORMAL"),
            _apply("STRONG", lambda s: s["strength"] == "STRONG"),
            _apply("MEGA", lambda s: s["strength"] == "MEGA"),
            _apply("RISKY", lambda s: s["strength"] == "RISKY"),
            _apply("sources>=3", lambda s: s["sources_count"] >= 3),
            _apply("sources>=4", lambda s: s["sources_count"] >= 4),
            _apply("rev_score>0", lambda s: s["reversal_score"] > 0),
            _apply("LONG only", lambda s: s["direction"] == "LONG"),
            _apply("SHORT only", lambda s: s["direction"] == "SHORT"),
        ]
    elif source in ("supertrend_vip", "supertrend_mtf"):
        rows += [
            _apply("LONG only", lambda s: s["direction"] == "LONG"),
            _apply("SHORT only", lambda s: s["direction"] == "SHORT"),
            _apply("aligned_bots>=2", lambda s: s["aligned_bots_count"] >= 2),
            _apply("aligned_bots>=3", lambda s: s["aligned_bots_count"] >= 3),
            _apply("+btc_aligned", lambda s: (s["direction"] == "LONG" and s["_btc_trend"] == "UP") or (s["direction"] == "SHORT" and s["_btc_trend"] == "DOWN")),
            _apply("-btc_counter", lambda s: not ((s["direction"] == "LONG" and s["_btc_trend"] == "DOWN") or (s["direction"] == "SHORT" and s["_btc_trend"] == "UP"))),
            _apply("with_cv", lambda s: "cryptovizor" in s.get("aligned_sources", [])),
            _apply("with_conf", lambda s: "confluence" in s.get("aligned_sources", [])),
        ]
        if source == "supertrend_mtf":
            rows.append(_apply("1h+4h+1d", lambda s: set(s.get("aligned_tfs", [])) >= {"1h", "4h", "1d"}))
            rows.append(_apply("LONG+btc_up", lambda s: s["direction"] == "LONG" and s["_btc_trend"] == "UP"))
            rows.append(_apply("SHORT+btc_down", lambda s: s["direction"] == "SHORT" and s["_btc_trend"] == "DOWN"))
    elif source == "anomaly":
        rows += [
            _apply("score>=9", lambda s: s["score"] >= 9),
            _apply("score>=11", lambda s: s["score"] >= 11),
            _apply("score>=13", lambda s: s["score"] >= 13),
            _apply("types>=2", lambda s: s["types_count"] >= 2),
            _apply("types>=3", lambda s: s["types_count"] >= 3),
            _apply("has_oi_spike", lambda s: "oi_spike" in s.get("types", [])),
            _apply("has_funding", lambda s: "funding_extreme" in s.get("types", [])),
            _apply("has_ls_ratio", lambda s: "ls_ratio_extreme" in s.get("types", [])),
            _apply("has_taker", lambda s: "taker_imbalance" in s.get("types", [])),
            _apply("has_volume", lambda s: "volume_spike" in s.get("types", [])),
            _apply("LONG only", lambda s: s["direction"] == "LONG"),
            _apply("SHORT only", lambda s: s["direction"] == "SHORT"),
        ]
    elif source == "cryptovizor":
        # Топ паттернов из данных
        from collections import Counter
        pat_counts = Counter(s.get("pattern_name", "?") for s in good)
        top_patterns = [p for p, _ in pat_counts.most_common(6)]
        rows += [
            _apply("st_passed", lambda s: s["st_passed"]),
            _apply("ai_score>=60", lambda s: s["ai_score"] >= 60),
            _apply("ai_score>=70", lambda s: s["ai_score"] >= 70),
            _apply("ai_score>=80", lambda s: s["ai_score"] >= 80),
            _apply("LONG only", lambda s: s["direction"] == "LONG"),
            _apply("SHORT only", lambda s: s["direction"] == "SHORT"),
            _apply("st_passed+score70", lambda s: s["st_passed"] and s["ai_score"] >= 70),
            _apply("st_passed+score80", lambda s: s["st_passed"] and s["ai_score"] >= 80),
        ]
        for p in top_patterns:
            rows.append(_apply(f"pattern={p[:14]}", lambda s, pp=p: s.get("pattern_name") == pp))

    # 5. Возвращаем отсортированное по sum_r убыв (baseline в начале)
    def _key(r):
        if r["name"] == "BASELINE": return (0, 0)
        return (1, -(r.get("sum_r") or 0))
    rows.sort(key=_key)

    return {
        "ok": True, "source": source, "hours": hours, "forward_hours": forward_hours,
        "raw_count": len(raw), "sim_ok_count": len(good),
        "pairs_cached": len(candles_15m_cache),
        "variants": rows,
    }


async def _run_backtest_optimize(source: str, hours: int, forward_hours: int):
    from datetime import datetime as _dt
    try:
        result = await asyncio.to_thread(_backtest_optimize_sync, source, hours, forward_hours)
        _bo_state["result"] = result
    except Exception as e:
        _bo_state["error"] = str(e)
        logging.getLogger(__name__).exception("[backtest-optimize] crashed")
    finally:
        _bo_state["running"] = False
        _bo_state["finished_at"] = _dt.utcnow().isoformat()


@app.post("/api/backtest-optimize")
async def api_backtest_optimize_start(payload: dict | None = None):
    """Бектест с фильтрами для одного источника.
    payload: {"source": "confluence|cluster|supertrend_vip|supertrend_mtf|anomaly|cryptovizor",
              "hours": 168, "forward_hours": 72}
    Статус через GET /api/backtest-optimize/status."""
    from datetime import datetime as _dt
    if _bo_state.get("running"):
        return {"ok": False, "error": "already running", "state": _bo_state}
    p = payload or {}
    source = p.get("source") or "confluence"
    hours = int(p.get("hours", 168))
    forward_hours = int(p.get("forward_hours", 72))
    _bo_state.update({
        "running": True, "started_at": _dt.utcnow().isoformat(),
        "finished_at": None,
        "progress": {"processed": 0, "total": 0, "current": ""},
        "result": None, "error": None,
        "source": source, "hours": hours, "forward_hours": forward_hours,
    })
    asyncio.create_task(_run_backtest_optimize(source, hours, forward_hours))
    return {"ok": True, "started": True, "source": source, "hours": hours,
            "forward_hours": forward_hours}


@app.get("/api/backtest-optimize/status")
async def api_backtest_optimize_status():
    return _bo_state


# ═══════════════════════════════════════════════════════════════════
# BACKTEST ENTRY TIMING — сравнение 8 стратегий входа
# IMMEDIATE | PULLBACK_0.3/0.5/1.0 | PULLBACK_0.5_FB | EMA20_5M | EMA20_5M_REV | ATR_HALF
# ═══════════════════════════════════════════════════════════════════
_bet_state: dict = {
    "running": False, "started_at": None, "finished_at": None,
    "progress": {"processed": 0, "total": 0, "current": ""},
    "result": None, "error": None,
}


def _bet_collect_signals(hours: int) -> list[dict]:
    """Собирает все сигналы за N часов — универсально (tradium/CV/anomaly/
    confluence/cluster/supertrend)."""
    from database import (_signals, _anomalies, _confluence, _clusters,
                          _supertrend_signals, utcnow)
    from datetime import timedelta
    since = utcnow() - timedelta(hours=hours)
    out = []

    for s in _signals().find({"source": "tradium", "received_at": {"$gte": since}}):
        at = s.get("pattern_triggered_at") or s.get("received_at")
        if s.get("pair") and s.get("direction") in ("LONG", "SHORT"):
            out.append({
                "source": "tradium", "pair": s.get("pair"),
                "direction": s.get("direction"),
                "entry": s.get("entry"), "tp1": s.get("tp1"), "sl": s.get("sl"),
                "at": at,
            })
    for s in _signals().find({"source": "cryptovizor", "pattern_triggered": True,
                              "pattern_triggered_at": {"$gte": since}}).limit(500):
        if s.get("pair") and s.get("direction") in ("LONG", "SHORT"):
            out.append({
                "source": "cryptovizor", "pair": s.get("pair"),
                "direction": s.get("direction"),
                "entry": s.get("pattern_price") or s.get("entry"),
                "tp1": s.get("dca2"), "sl": s.get("dca1"),
                "at": s.get("pattern_triggered_at"),
            })
    for a in _anomalies().find({"detected_at": {"$gte": since}}).sort("score", -1).limit(300):
        sym = (a.get("symbol") or "").upper()
        pair = a.get("pair") or (sym.replace("USDT", "/USDT") if sym.endswith("USDT") else None)
        if pair and a.get("direction") in ("LONG", "SHORT"):
            out.append({
                "source": "anomaly", "pair": pair,
                "direction": a.get("direction"),
                "entry": a.get("price"), "tp1": None, "sl": None,
                "at": a.get("detected_at"),
            })
    for c in _confluence().find({"detected_at": {"$gte": since}}).sort("score", -1).limit(500):
        if c.get("pair") and c.get("direction") in ("LONG", "SHORT"):
            out.append({
                "source": "confluence", "pair": c.get("pair"),
                "direction": c.get("direction"),
                "entry": c.get("price"), "tp1": c.get("r1"), "sl": c.get("s1"),
                "at": c.get("detected_at"),
            })
    for cl in _clusters().find({"trigger_at": {"$gte": since}}):
        if cl.get("pair") and cl.get("direction") in ("LONG", "SHORT"):
            out.append({
                "source": "cluster", "pair": cl.get("pair"),
                "direction": cl.get("direction"),
                "entry": cl.get("trigger_price"),
                "tp1": cl.get("tp_price"), "sl": cl.get("sl_price"),
                "at": cl.get("trigger_at"),
            })
    for s in _supertrend_signals().find({"flip_at": {"$gte": since},
                                         "tier": {"$in": ["vip", "mtf"]}}).limit(400):
        if s.get("pair") and s.get("direction") in ("LONG", "SHORT"):
            out.append({
                "source": f"supertrend_{s.get('tier','?')}",
                "pair": s.get("pair"), "direction": s.get("direction"),
                "entry": s.get("entry_price"),
                "tp1": None, "sl": s.get("sl_price"),
                "at": s.get("flip_at"),
            })
    return out


def _bet_atr_sl_tp(candles_1h: list, direction: str, entry: float):
    """ATR-based SL/TP если отсутствуют."""
    if not candles_1h or len(candles_1h) < 15:
        return None, None
    trs = []
    for i in range(1, len(candles_1h)):
        c, pc = candles_1h[i], candles_1h[i-1]["c"]
        trs.append(max(c["h"] - c["l"], abs(c["h"] - pc), abs(c["l"] - pc)))
    atr = sum(trs[-14:]) / min(14, len(trs))
    if direction == "LONG":
        return entry - atr * 1.5, entry + atr * 2.5
    return entry + atr * 1.5, entry - atr * 2.5


def _bet_ema20(closes: list) -> list:
    """EMA20 для массива closes."""
    if len(closes) < 20:
        return [None] * len(closes)
    result = [None] * 19
    ema = sum(closes[:20]) / 20
    result.append(ema)
    m = 2 / 21
    for c in closes[20:]:
        ema = c * m + ema * (1 - m)
        result.append(ema)
    return result


def _bet_atr_5m(candles_5m: list, idx: int, period: int = 14):
    """ATR 5m на баре idx."""
    if idx < period:
        return None
    trs = []
    for i in range(idx - period + 1, idx + 1):
        if i <= 0:
            continue
        c, pc = candles_5m[i], candles_5m[i-1]["c"]
        trs.append(max(c["h"] - c["l"], abs(c["h"] - pc), abs(c["l"] - pc)))
    return sum(trs) / len(trs) if trs else None


def _bet_is_reversal(bar: dict, prev_bar: dict, direction: str) -> bool:
    """Bullish pin/engulf для LONG, bearish для SHORT."""
    o = bar["o"]; h = bar["h"]; l = bar["l"]; c = bar["c"]
    po = prev_bar["o"]; pc = prev_bar["c"]
    body = abs(c - o)
    prev_body = abs(pc - po)
    if body <= 0 or body < c * 0.002:
        return False
    if direction == "LONG":
        if c <= o: return False
        lower_wick = min(o, c) - l
        is_pin = lower_wick >= 1.5 * body
        is_engulf = (c > po) and (o <= pc) and (c > pc) and (body >= prev_body) and (prev_body >= c * 0.001)
        return is_pin or is_engulf
    else:
        if c >= o: return False
        upper_wick = h - max(o, c)
        is_pin = upper_wick >= 1.5 * body
        is_engulf = (c < po) and (o >= pc) and (c < pc) and (body >= prev_body) and (prev_body >= c * 0.001)
        return is_pin or is_engulf


def _bet_simulate_tp_sl(candles_15m: list, start_ts_ms: int, direction: str,
                         entry: float, tp: float, sl: float, forward_h: int) -> dict:
    """Симулирует TP/SL на 15m от start_ts. Возвращает {what, r, pnl_pct}."""
    if not candles_15m:
        return {"ok": False}
    end_ms = start_ts_ms + forward_h * 3600 * 1000
    is_long = direction == "LONG"
    risk = abs(entry - sl)
    if risk <= 0:
        return {"ok": False}
    last_c = None
    for c in candles_15m:
        if c["t"] < start_ts_ms or c["t"] > end_ms:
            continue
        last_c = c
        hi, lo = c["h"], c["l"]
        if is_long:
            if lo <= sl:
                return {"ok": True, "what": "SL", "r": -1.0,
                        "pnl_pct": round((sl - entry) / entry * 100, 2)}
            if hi >= tp:
                reward = abs(tp - entry)
                return {"ok": True, "what": "TP", "r": round(reward / risk, 2),
                        "pnl_pct": round((tp - entry) / entry * 100, 2)}
        else:
            if hi >= sl:
                return {"ok": True, "what": "SL", "r": -1.0,
                        "pnl_pct": round((sl - entry) / entry * 100 * -1, 2)}
            if lo <= tp:
                reward = abs(tp - entry)
                return {"ok": True, "what": "TP", "r": round(reward / risk, 2),
                        "pnl_pct": round((tp - entry) / entry * 100 * -1, 2)}
    if not last_c:
        return {"ok": False}
    last = last_c["c"]
    r = (last - entry) / risk if is_long else (entry - last) / risk
    return {"ok": True, "what": "OPEN", "r": round(r, 3),
            "pnl_pct": round((last - entry) / entry * 100 * (1 if is_long else -1), 2)}


def _bet_find_entry(strategy: str, candles_5m: list, signal_ts_ms: int,
                    signal_price: float, direction: str, timeout_min: int = 30,
                    fallback_pct: float = 1.5) -> dict:
    """Ищет точку входа согласно стратегии. Возвращает {executed, entry_price, entry_ts, skip_reason}."""
    if not candles_5m:
        return {"executed": False, "skip_reason": "no 5m candles"}
    end_ms = signal_ts_ms + timeout_min * 60 * 1000
    is_long = direction == "LONG"

    # IMMEDIATE — entry сразу по signal_price, entry_ts = signal_ts
    if strategy == "IMMEDIATE":
        return {"executed": True, "entry_price": signal_price, "entry_ts": signal_ts_ms}

    # Найти индекс первой свечи после сигнала
    start_idx = None
    for i, c in enumerate(candles_5m):
        if c["t"] >= signal_ts_ms:
            start_idx = i
            break
    if start_idx is None:
        return {"executed": False, "skip_reason": "no candles after signal"}

    # Pre-compute EMA20 (если нужна)
    closes = [c["c"] for c in candles_5m]
    ema20_arr = _bet_ema20(closes) if strategy in ("EMA20_5M", "EMA20_5M_REV") else None

    # Pullback thresholds
    if strategy.startswith("PULLBACK_"):
        pct_str = strategy.split("_")[1].replace("FB", "")
        try:
            pct = float(pct_str)
        except Exception:
            pct = 0.5
        target = signal_price * (1 - pct / 100.0) if is_long else signal_price * (1 + pct / 100.0)
        fb_target = signal_price * (1 + fallback_pct / 100.0) if is_long else signal_price * (1 - fallback_pct / 100.0)
        use_fallback = "FB" in strategy
    elif strategy == "ATR_HALF":
        atr = _bet_atr_5m(candles_5m, start_idx, 14)
        if not atr:
            return {"executed": False, "skip_reason": "no ATR"}
        target = signal_price - 0.5 * atr if is_long else signal_price + 0.5 * atr

    # Проходим свечи в окне
    for i in range(start_idx, len(candles_5m)):
        bar = candles_5m[i]
        if bar["t"] > end_ms:
            break
        hi, lo = bar["h"], bar["l"]

        if strategy.startswith("PULLBACK_") or strategy == "ATR_HALF":
            if is_long and lo <= target:
                return {"executed": True, "entry_price": target, "entry_ts": bar["t"]}
            if (not is_long) and hi >= target:
                return {"executed": True, "entry_price": target, "entry_ts": bar["t"]}
            if strategy.endswith("_FB") and use_fallback:
                if is_long and hi >= fb_target:
                    # Fallback: цена сильно ушла вверх — вход по fb_target
                    return {"executed": True, "entry_price": fb_target, "entry_ts": bar["t"],
                            "fallback": True}
                if (not is_long) and lo <= fb_target:
                    return {"executed": True, "entry_price": fb_target, "entry_ts": bar["t"],
                            "fallback": True}

        elif strategy == "EMA20_5M":
            e = ema20_arr[i] if i < len(ema20_arr) else None
            if not e: continue
            if is_long and lo <= e:
                return {"executed": True, "entry_price": e, "entry_ts": bar["t"]}
            if (not is_long) and hi >= e:
                return {"executed": True, "entry_price": e, "entry_ts": bar["t"]}

        elif strategy == "EMA20_5M_REV":
            # Ждём (1) касание EMA20, затем (2) reversal-candle
            e = ema20_arr[i] if i < len(ema20_arr) else None
            if not e: continue
            touched = (is_long and lo <= e) or ((not is_long) and hi >= e)
            if touched:
                # Смотрим следующие 1-3 бара на reversal
                for j in range(i + 1, min(i + 4, len(candles_5m))):
                    if candles_5m[j]["t"] > end_ms: break
                    if j - 1 < 0: continue
                    if _bet_is_reversal(candles_5m[j], candles_5m[j - 1], direction):
                        return {"executed": True, "entry_price": candles_5m[j]["c"],
                                "entry_ts": candles_5m[j]["t"]}

    return {"executed": False, "skip_reason": "timeout"}


def _backtest_entry_timing_sync(hours: int, forward_hours: int) -> dict:
    """Главная функция бектеста 8 entry стратегий."""
    from exchange import get_klines_any
    import logging as _log
    logger = _log.getLogger(__name__)

    STRATEGIES = [
        "IMMEDIATE",
        "PULLBACK_0.3", "PULLBACK_0.5", "PULLBACK_1.0",
        "PULLBACK_0.5_FB",
        "EMA20_5M", "EMA20_5M_REV",
        "ATR_HALF",
    ]

    signals = _bet_collect_signals(hours)
    logger.info(f"[bet] собрано {len(signals)} сигналов")
    _bet_state["progress"]["total"] = len(signals)

    # Кеши свечей
    cache_5m: dict = {}
    cache_15m: dict = {}
    cache_1h: dict = {}

    def _get_5m(pair):
        if pair in cache_5m: return cache_5m[pair]
        # 5m × 24ч × 12 = ~3500 баров на 14 дней, берём 1000 (последние ~83ч)
        c = get_klines_any(pair, "5m", 1000) or []
        cache_5m[pair] = c
        return c

    def _get_15m(pair):
        if pair in cache_15m: return cache_15m[pair]
        c = get_klines_any(pair, "15m", 500) or []
        cache_15m[pair] = c
        return c

    def _get_1h(pair):
        if pair in cache_1h: return cache_1h[pair]
        c = get_klines_any(pair, "1h", 30) or []
        cache_1h[pair] = c
        return c

    # Per-strategy агрегаты
    stats = {s: {"executed": 0, "skipped": 0, "fallback": 0,
                 "wins": 0, "losses": 0, "open": 0,
                 "sum_r": 0.0, "sum_pct": 0.0} for s in STRATEGIES}

    processed = 0
    for sig in signals:
        processed += 1
        _bet_state["progress"]["processed"] = processed
        _bet_state["progress"]["current"] = sig.get("pair", "")

        try:
            pair = sig["pair"]
            direction = sig["direction"]
            signal_price = sig["entry"]
            at = sig["at"]
            if not (pair and signal_price and at):
                continue
            signal_ts_ms = int(at.timestamp() * 1000) if hasattr(at, "timestamp") else 0
            if not signal_ts_ms:
                continue

            # TP/SL — если отсутствуют, ATR-based
            tp1, sl = sig.get("tp1"), sig.get("sl")
            if not sl:
                sl_atr, tp_atr = _bet_atr_sl_tp(_get_1h(pair), direction, signal_price)
                sl = sl_atr
                if not tp1: tp1 = tp_atr
            if not sl: continue
            if not tp1:
                _, tp_atr = _bet_atr_sl_tp(_get_1h(pair), direction, signal_price)
                tp1 = tp_atr
            if not tp1: continue

            # Distance relative для пересчёта TP/SL от новой entry
            tp_dist_pct = abs(tp1 - signal_price) / signal_price
            sl_dist_pct = abs(sl - signal_price) / signal_price

            candles_5m = _get_5m(pair)
            candles_15m = _get_15m(pair)
            if not candles_15m:
                continue

            for strat in STRATEGIES:
                r = _bet_find_entry(strat, candles_5m, signal_ts_ms, signal_price, direction,
                                    timeout_min=30, fallback_pct=1.5)
                if not r.get("executed"):
                    stats[strat]["skipped"] += 1
                    continue
                entry_p = r["entry_price"]
                entry_ts = r["entry_ts"]
                # Пересчёт TP/SL от нового entry (сохраняем distance)
                if direction == "LONG":
                    new_tp = entry_p * (1 + tp_dist_pct)
                    new_sl = entry_p * (1 - sl_dist_pct)
                else:
                    new_tp = entry_p * (1 - tp_dist_pct)
                    new_sl = entry_p * (1 + sl_dist_pct)
                sim = _bet_simulate_tp_sl(candles_15m, entry_ts, direction,
                                          entry_p, new_tp, new_sl, forward_hours)
                if not sim.get("ok"):
                    stats[strat]["skipped"] += 1
                    continue
                stats[strat]["executed"] += 1
                if r.get("fallback"):
                    stats[strat]["fallback"] += 1
                if sim["what"] == "TP":
                    stats[strat]["wins"] += 1
                elif sim["what"] == "SL":
                    stats[strat]["losses"] += 1
                else:
                    stats[strat]["open"] += 1
                stats[strat]["sum_r"] += sim.get("r") or 0
                stats[strat]["sum_pct"] += sim.get("pnl_pct") or 0
        except Exception as _e:
            logger.warning(f"[bet] sim fail {sig.get('source')} {sig.get('pair')}: {_e}")

    # Формирование отчёта
    rows = []
    baseline_sum_r = stats.get("IMMEDIATE", {}).get("sum_r", 0)
    for strat in STRATEGIES:
        s = stats[strat]
        closed = s["wins"] + s["losses"]
        wr = round(s["wins"] / closed * 100, 1) if closed else 0
        avg_r = round(s["sum_r"] / s["executed"], 3) if s["executed"] else 0
        rows.append({
            "name": strat,
            "executed": s["executed"], "skipped": s["skipped"], "fallback": s["fallback"],
            "wins": s["wins"], "losses": s["losses"], "open": s["open"],
            "wr": wr, "avg_r_executed": avg_r,
            "sum_r": round(s["sum_r"], 2), "sum_pct": round(s["sum_pct"], 2),
            "vs_immediate_r": round(s["sum_r"] - baseline_sum_r, 2),
        })

    return {
        "ok": True, "hours": hours, "forward_hours": forward_hours,
        "total_signals": len(signals),
        "pairs_5m_cached": len(cache_5m),
        "strategies": rows,
    }


async def _run_backtest_entry_timing(hours: int, forward_hours: int):
    from datetime import datetime as _dt
    try:
        result = await asyncio.to_thread(_backtest_entry_timing_sync, hours, forward_hours)
        _bet_state["result"] = result
    except Exception as e:
        _bet_state["error"] = str(e)
        logging.getLogger(__name__).exception("[backtest-entry-timing] crashed")
    finally:
        _bet_state["running"] = False
        _bet_state["finished_at"] = _dt.utcnow().isoformat()


@app.post("/api/backtest-entry-timing")
async def api_backtest_entry_timing_start(payload: dict | None = None):
    """Бектест 8 стратегий входа (IMMEDIATE vs PULLBACK vs EMA vs ATR).
    payload: {"hours": 168, "forward_hours": 48}."""
    from datetime import datetime as _dt
    if _bet_state.get("running"):
        return {"ok": False, "error": "already running", "state": _bet_state}
    p = payload or {}
    hours = int(p.get("hours", 168))
    forward_hours = int(p.get("forward_hours", 48))
    _bet_state.update({
        "running": True, "started_at": _dt.utcnow().isoformat(),
        "finished_at": None,
        "progress": {"processed": 0, "total": 0, "current": ""},
        "result": None, "error": None,
    })
    asyncio.create_task(_run_backtest_entry_timing(hours, forward_hours))
    return {"ok": True, "started": True, "hours": hours, "forward_hours": forward_hours}


@app.get("/api/backtest-entry-timing/status")
async def api_backtest_entry_timing_status():
    return _bet_state


# ═══════════════════════════════════════════════════════════════════
# BACKTEST ST PROXIMITY TO 4H SUPERTREND
# Гипотеза: ST 1h flip близко к 4h ST линии → высокий WR (отскок от уровня).
# Бакеты расстояния от entry до 4h ST value: ≤0.3 / 0.3-0.5 / 0.5-0.7 /
# 0.7-1.0 / 1.0-2.0 / >2.0%. Direction=LONG/SHORT считаем отдельно.
# ═══════════════════════════════════════════════════════════════════
_bsp_state: dict = {
    "running": False, "started_at": None, "finished_at": None,
    "progress": {"processed": 0, "total": 0, "current": ""},
    "result": None, "error": None,
}


def _backtest_st_proximity_sync(days: int, forward_hours: int) -> dict:
    """Главная sync-функция бектеста ST 1h × distance to 4h ST."""
    from database import _supertrend_signals, utcnow
    from exchange import get_klines_any
    from backtest_supertrend import compute_st_series, st_state_at_ts
    from datetime import timedelta
    import logging as _log
    logger = _log.getLogger(__name__)

    since = utcnow() - timedelta(days=days)
    raw = list(_supertrend_signals().find({
        "flip_at": {"$gte": since},
        "tier": {"$in": ["vip", "mtf"]},
    }).sort("flip_at", -1))
    logger.info(f"[bsp] собрано {len(raw)} ST signals")
    _bsp_state["progress"]["total"] = len(raw)

    # Кеши свечей по паре
    c_4h: dict = {}
    c_15m: dict = {}
    c_1h: dict = {}
    st_4h_series: dict = {}

    BUCKETS = [
        ("≤0.3%",    0.0, 0.3),
        ("0.3-0.5%", 0.3, 0.5),
        ("0.5-0.7%", 0.5, 0.7),
        ("0.7-1.0%", 0.7, 1.0),
        ("1.0-2.0%", 1.0, 2.0),
        (">2.0%",    2.0, 10000.0),
    ]

    # stats[bucket][direction] = {n, wins, losses, open, sum_r}
    stats = {b[0]: {"LONG":  {"n": 0, "wins": 0, "losses": 0, "open": 0, "sum_r": 0.0, "sum_pct": 0.0},
                    "SHORT": {"n": 0, "wins": 0, "losses": 0, "open": 0, "sum_r": 0.0, "sum_pct": 0.0}}
             for b in BUCKETS}

    processed = 0
    skipped_no_4h = 0
    skipped_no_15m = 0
    skipped_no_sl = 0

    for s in raw:
        processed += 1
        _bsp_state["progress"]["processed"] = processed
        pair = s.get("pair")
        direction = s.get("direction")
        flip_at = s.get("flip_at")
        entry = s.get("entry_price")
        sl = s.get("sl_price")
        if not (pair and direction in ("LONG", "SHORT") and flip_at and entry):
            continue
        _bsp_state["progress"]["current"] = pair

        # 4h candles + ST 4h series
        if pair not in c_4h:
            c_4h[pair] = get_klines_any(pair, "4h", 200) or []
        cand_4h = c_4h[pair]
        if not cand_4h or len(cand_4h) < 15:
            skipped_no_4h += 1
            continue
        if pair not in st_4h_series:
            st_4h_series[pair] = compute_st_series(cand_4h, period=10, mult=3.0)
        st_4h = st_4h_series[pair]
        if not st_4h:
            skipped_no_4h += 1
            continue

        # 4h ST value в момент flip_at
        try:
            flip_ms = int(flip_at.timestamp() * 1000)
        except Exception:
            continue
        st_bar = st_state_at_ts(st_4h, flip_ms)
        if (not st_bar) or st_bar.get("st") is None:
            skipped_no_4h += 1
            continue
        st_4h_value = st_bar["st"]
        distance_pct = abs(entry - st_4h_value) / entry * 100

        # Определяем bucket
        bucket_name = None
        for name, lo, hi in BUCKETS:
            if lo <= distance_pct < hi:
                bucket_name = name
                break
        if not bucket_name:
            continue

        # SL: из сигнала или ATR fallback на 1h
        if not sl:
            if pair not in c_1h:
                c_1h[pair] = get_klines_any(pair, "1h", 30) or []
            cand_1h = c_1h[pair]
            if cand_1h and len(cand_1h) >= 15:
                trs = []
                for i in range(1, len(cand_1h)):
                    c, pc = cand_1h[i], cand_1h[i-1]["c"]
                    trs.append(max(c["h"]-c["l"], abs(c["h"]-pc), abs(c["l"]-pc)))
                atr = sum(trs[-14:]) / min(14, len(trs))
                sl = entry - atr * 1.5 if direction == "LONG" else entry + atr * 1.5
        if not sl:
            skipped_no_sl += 1
            continue

        # TP = entry + 2R (R:R 1:2 — стандарт)
        risk = abs(entry - sl)
        if risk <= 0:
            continue
        tp = entry + 2 * risk if direction == "LONG" else entry - 2 * risk

        # Симуляция на 15m
        if pair not in c_15m:
            c_15m[pair] = get_klines_any(pair, "15m", 600) or []
        cand_15m = c_15m[pair]
        if not cand_15m:
            skipped_no_15m += 1
            continue

        end_ms = flip_ms + forward_hours * 3600 * 1000
        is_long = direction == "LONG"
        r_mult = 0.0
        pnl_pct = 0.0
        what = "OPEN"
        hit_found = False
        last_c = None
        for c in cand_15m:
            if c["t"] < flip_ms or c["t"] > end_ms:
                continue
            last_c = c
            hi, lo = c["h"], c["l"]
            if is_long:
                if lo <= sl:
                    r_mult = -1.0; what = "SL"
                    pnl_pct = (sl - entry) / entry * 100
                    hit_found = True; break
                if hi >= tp:
                    r_mult = 2.0; what = "TP"
                    pnl_pct = (tp - entry) / entry * 100
                    hit_found = True; break
            else:
                if hi >= sl:
                    r_mult = -1.0; what = "SL"
                    pnl_pct = (sl - entry) / entry * 100 * -1
                    hit_found = True; break
                if lo <= tp:
                    r_mult = 2.0; what = "TP"
                    pnl_pct = (tp - entry) / entry * 100 * -1
                    hit_found = True; break
        if not hit_found:
            if not last_c:
                continue  # нет свечей в окне
            last_price = last_c["c"]
            r_mult = (last_price - entry) / risk if is_long else (entry - last_price) / risk
            pnl_pct = (last_price - entry) / entry * 100 * (1 if is_long else -1)
            what = "OPEN"

        b = stats[bucket_name][direction]
        b["n"] += 1
        if what == "TP":
            b["wins"] += 1
        elif what == "SL":
            b["losses"] += 1
        else:
            b["open"] += 1
        b["sum_r"] += r_mult
        b["sum_pct"] += pnl_pct

    # Формируем результат
    rows = []
    total_n = 0
    total_sum_r = 0.0
    for bname, _, _ in BUCKETS:
        for dirn in ("LONG", "SHORT"):
            st = stats[bname][dirn]
            closed = st["wins"] + st["losses"]
            wr = round(st["wins"] / closed * 100, 1) if closed else 0.0
            avg_r = round(st["sum_r"] / st["n"], 3) if st["n"] else 0.0
            # PF
            # Для PF нужны выигрышные и убыточные отдельно — в sum_r это уже суммы; приближение:
            pf = round(abs(st["wins"] * 2) / max(abs(st["losses"] * 1), 1), 2) if st["losses"] else None
            rows.append({
                "bucket": bname, "direction": dirn,
                "n": st["n"], "wins": st["wins"], "losses": st["losses"], "open": st["open"],
                "wr": wr, "avg_r": avg_r, "pf": pf,
                "sum_r": round(st["sum_r"], 2),
                "sum_pct": round(st["sum_pct"], 1),
            })
            total_n += st["n"]
            total_sum_r += st["sum_r"]

    return {
        "ok": True, "days": days, "forward_hours": forward_hours,
        "total_signals": len(raw),
        "simulated": total_n,
        "skipped_no_4h": skipped_no_4h,
        "skipped_no_15m": skipped_no_15m,
        "skipped_no_sl": skipped_no_sl,
        "pairs_cached": len(c_4h),
        "total_sum_r": round(total_sum_r, 2),
        "rows": rows,
    }


async def _run_backtest_st_proximity(days: int, forward_hours: int):
    from datetime import datetime as _dt
    try:
        result = await asyncio.to_thread(_backtest_st_proximity_sync, days, forward_hours)
        _bsp_state["result"] = result
    except Exception as e:
        _bsp_state["error"] = str(e)
        logging.getLogger(__name__).exception("[backtest-st-proximity] crashed")
    finally:
        _bsp_state["running"] = False
        _bsp_state["finished_at"] = _dt.utcnow().isoformat()


@app.post("/api/backtest-st-proximity")
async def api_backtest_st_proximity_start(payload: dict | None = None):
    """Бектест ST 1h flips по расстоянию до 4h ST линии.
    payload: {days: 14, forward_hours: 48}
    Background task — статус через GET /api/backtest-st-proximity/status."""
    from datetime import datetime as _dt
    if _bsp_state.get("running"):
        return {"ok": False, "error": "already running", "state": _bsp_state}
    p = payload or {}
    days = int(p.get("days", 14))
    forward_hours = int(p.get("forward_hours", 48))
    _bsp_state.update({
        "running": True, "started_at": _dt.utcnow().isoformat(),
        "finished_at": None,
        "progress": {"processed": 0, "total": 0, "current": ""},
        "result": None, "error": None,
    })
    asyncio.create_task(_run_backtest_st_proximity(days, forward_hours))
    return {"ok": True, "started": True, "days": days, "forward_hours": forward_hours}


@app.get("/api/backtest-st-proximity/status")
async def api_backtest_st_proximity_status():
    return _bsp_state


# ═══════════════════════════════════════════════════════════════════
# BACKTEST CV × ST 30m CONFIRM
# Гипотеза: вместо открытия по CV pattern сразу — ждать flip ST 30m в
# сторону CV сигнала. Если flip не случился за 10ч → пропускаем.
# Сравниваем 3 стратегии: IMMEDIATE / ST30M_FRESH / ST30M_ALIGNED.
# ═══════════════════════════════════════════════════════════════════
_bcst_state: dict = {
    "running": False, "started_at": None, "finished_at": None,
    "progress": {"processed": 0, "total": 0, "current": ""},
    "result": None, "error": None,
}


def _backtest_cv_st30m_sync(days: int) -> dict:
    """Бектест 3 стратегий входа по CV сигналам:
      1) IMMEDIATE    — открываем сразу по pattern_price (baseline)
      2) ST30M_FRESH  — ждём свежий flip ST 30m в сторону CV (10ч timeout)
      3) ST30M_ALIGNED — flip ИЛИ уже в сторону на момент CV
    """
    from database import _signals, utcnow
    from exchange import get_klines_any
    from backtest_supertrend import compute_st_series, st_state_at_ts
    from datetime import timedelta
    import logging as _log
    logger = _log.getLogger(__name__)

    since = utcnow() - timedelta(days=days)
    # Параметры принимаются из payload через _bcst_state.
    TIMEOUT_H = int(_bcst_state.get("timeout_h", 10))
    ST_TF = str(_bcst_state.get("st_tf", "30m")).lower()
    if ST_TF not in ("30m", "1h"):
        ST_TF = "30m"
    BUFFER_PCT = 0.3
    MAX_HOLD_H = 24

    # CV сигналы с pattern_triggered (только они открывают позиции)
    raw = list(_signals().find({
        "source": "cryptovizor",
        "pattern_triggered": True,
        "pattern_triggered_at": {"$gte": since},
    }).sort("pattern_triggered_at", 1))
    _bcst_state["progress"]["total"] = len(raw)
    logger.info(f"[bcst] собрано {len(raw)} CV signals")

    # Группируем по паре для анти-дубля (новый CV → старый инвалидируется)
    from collections import defaultdict
    by_pair: dict = defaultdict(list)
    for s in raw:
        p = s.get("pair") or ""
        by_pair[p].append(s)
    # Если между двумя CV на той же паре разрыв < TIMEOUT_H → старый
    # инвалидируется (ждём flip уже по новому сигналу).
    invalid_ids = set()
    for p, sigs in by_pair.items():
        sigs.sort(key=lambda x: x.get("pattern_triggered_at") or 0)
        for i in range(len(sigs) - 1):
            cur_t = sigs[i].get("pattern_triggered_at")
            next_t = sigs[i+1].get("pattern_triggered_at")
            if cur_t and next_t and (next_t - cur_t) < timedelta(hours=TIMEOUT_H):
                invalid_ids.add(id(sigs[i]))
    logger.info(f"[bcst] timeout={TIMEOUT_H}h, invalid_ids={len(invalid_ids)}")

    # Кеши свечей
    c_st: dict = {}  # свечи под ST (30m или 1h)
    c_15m: dict = {}
    st_cache: dict = {}
    if ST_TF == "1h":
        ST_PERIOD, ST_MULT = 10, 3.0   # пресет 1h
        BARS_PER_HOUR = 1
        FRESH_KEY, ALIGNED_KEY = "ST1H_FRESH", "ST1H_ALIGNED"
    else:
        ST_PERIOD, ST_MULT = 7, 2.5    # пресет 30m
        BARS_PER_HOUR = 2
        FRESH_KEY, ALIGNED_KEY = "ST30M_FRESH", "ST30M_ALIGNED"

    BASE_KEY = "BASE_FLIP_30M" if ST_TF == "30m" else "BASE_FLIP_1H"
    STRICT_KEY = "BASE_STRICT"        # ≥4 bars, 0.5% gap, breakout close
    STRICT_VOL_KEY = "BASE_STRICT_VOL"  # STRICT + volume spike 1.5×
    IMPHASE_KEY = "IMMEDIATE_PHASE"     # IMMEDIATE минус BEAR/CAPITULATION

    # Stats
    stats = {s: {"executed": 0, "skipped": 0, "timeout": 0, "invalidated": 0,
                 "wins": 0, "losses": 0, "open": 0,
                 "sum_r": 0.0, "sum_pct": 0.0,
                 "no_base": 0, "base_ok": 0, "phase_skip": 0, "vol_skip": 0}
             for s in ("IMMEDIATE", FRESH_KEY, ALIGNED_KEY, BASE_KEY,
                       STRICT_KEY, STRICT_VOL_KEY, IMPHASE_KEY)}

    # BTC 4h для фильтра фаз (упрощённый proxy: EMA20 slope + drawdown)
    btc_4h = get_klines_any("BTCUSDT", "4h", 500) or []
    btc_ema20: list = []
    if btc_4h:
        k = 2.0 / (20 + 1)
        ema = btc_4h[0]["c"]
        for c in btc_4h:
            ema = c["c"] * k + ema * (1 - k)
            btc_ema20.append({"t": c["t"], "c": c["c"], "ema": ema})

    def _is_bad_phase(ts_ms: int) -> bool:
        """True если фаза похожа на BEAR_TREND или CAPITULATION на момент ts."""
        if not btc_ema20:
            return False
        # Последний BTC 4h бар на момент ts
        idx = None
        for i in range(len(btc_ema20) - 1, -1, -1):
            if btc_ema20[i]["t"] <= ts_ms:
                idx = i; break
        if idx is None or idx < 10:
            return False
        cur = btc_ema20[idx]
        prev = btc_ema20[idx - 5]
        # BEAR: close < EMA20 и EMA20 снижается
        bear = cur["c"] < cur["ema"] and cur["ema"] < prev["ema"]
        # CAPITULATION: BTC упал > 7% за последние 48ч (12 баров 4h)
        past_idx = max(0, idx - 12)
        past = btc_4h[past_idx]["c"]
        drawdown = (cur["c"] - past) / past * 100 if past > 0 else 0
        capit = drawdown <= -7.0
        return bear or capit

    # Статистика ST 30m direction на момент CV (для проверки гипотезы
    # "CV LONG всегда приходит под ST 30m")
    st_at_cv_stats = {
        "LONG_at_st_up":    0,  # CV LONG пришёл когда ST 30m = UP (подтверждение)
        "LONG_at_st_down":  0,  # CV LONG пришёл когда ST 30m = DOWN (против ST)
        "LONG_at_st_unk":   0,  # ST 30m неизвестен
        "SHORT_at_st_up":   0,
        "SHORT_at_st_down": 0,
        "SHORT_at_st_unk":  0,
    }

    processed = 0
    for s in raw:
        processed += 1
        _bcst_state["progress"]["processed"] = processed
        pair = s.get("pair")
        direction = s.get("direction")
        pat_at = s.get("pattern_triggered_at")
        entry_cv = s.get("pattern_price") or s.get("entry")
        tp1_cv = s.get("dca2")
        sl_cv = s.get("dca1")
        if not (pair and direction in ("LONG", "SHORT") and pat_at and entry_cv):
            for st in stats.values(): st["skipped"] += 1
            continue
        _bcst_state["progress"]["current"] = pair

        pat_ms = int(pat_at.timestamp() * 1000)

        # ── 1) IMMEDIATE — открываем по pattern_price сразу ──
        risk_cv = abs(entry_cv - sl_cv) if (sl_cv and entry_cv) else None
        if risk_cv and tp1_cv:
            if pair not in c_15m:
                c_15m[pair] = get_klines_any(pair, "15m", 500) or []
            cand_15m = c_15m[pair]
            if cand_15m:
                end_ms = pat_ms + 48 * 3600 * 1000
                is_long = direction == "LONG"
                what = "OPEN"; r_mult = 0; pnl_pct = 0.0
                last_c = None
                for c in cand_15m:
                    if c["t"] < pat_ms or c["t"] > end_ms: continue
                    last_c = c
                    hi, lo = c["h"], c["l"]
                    if is_long:
                        if lo <= sl_cv: what = "SL"; r_mult = -1.0; pnl_pct = (sl_cv-entry_cv)/entry_cv*100; break
                        if hi >= tp1_cv:
                            r_mult = abs(tp1_cv-entry_cv)/risk_cv; what = "TP"
                            pnl_pct = (tp1_cv-entry_cv)/entry_cv*100; break
                    else:
                        if hi >= sl_cv: what = "SL"; r_mult = -1.0; pnl_pct = (sl_cv-entry_cv)/entry_cv*100 * -1; break
                        if lo <= tp1_cv:
                            r_mult = abs(tp1_cv-entry_cv)/risk_cv; what = "TP"
                            pnl_pct = (tp1_cv-entry_cv)/entry_cv*100 * -1; break
                if last_c:
                    if what == "OPEN":
                        last = last_c["c"]
                        r_mult = (last-entry_cv)/risk_cv if is_long else (entry_cv-last)/risk_cv
                        pnl_pct = (last-entry_cv)/entry_cv*100 * (1 if is_long else -1)
                    st_im = stats["IMMEDIATE"]
                    st_im["executed"] += 1
                    if what == "TP": st_im["wins"] += 1
                    elif what == "SL": st_im["losses"] += 1
                    else: st_im["open"] += 1
                    st_im["sum_r"] += r_mult; st_im["sum_pct"] += pnl_pct

                    # IMMEDIATE_PHASE: тот же результат, но пропускаем BEAR/CAPITULATION
                    if _is_bad_phase(pat_ms):
                        stats[IMPHASE_KEY]["phase_skip"] += 1
                    else:
                        st_ph = stats[IMPHASE_KEY]
                        st_ph["executed"] += 1
                        if what == "TP": st_ph["wins"] += 1
                        elif what == "SL": st_ph["losses"] += 1
                        else: st_ph["open"] += 1
                        st_ph["sum_r"] += r_mult; st_ph["sum_pct"] += pnl_pct

        # ── 2+3) FRESH / ALIGNED ──
        # Инвалидированные пропускаем для обоих ST-стратегий
        if id(s) in invalid_ids:
            stats[FRESH_KEY]["invalidated"] += 1
            stats[ALIGNED_KEY]["invalidated"] += 1
            continue

        # Загружаем свечи + ST серию под выбранный TF
        if pair not in c_st:
            c_st[pair] = get_klines_any(pair, ST_TF, 500) or []
        cand_st = c_st[pair]
        if not cand_st or len(cand_st) < 20:
            stats[FRESH_KEY]["skipped"] += 1
            stats[ALIGNED_KEY]["skipped"] += 1
            continue
        if pair not in st_cache:
            st_cache[pair] = compute_st_series(cand_st, ST_PERIOD, ST_MULT)
        st_series = st_cache[pair]
        if not st_series:
            stats[FRESH_KEY]["skipped"] += 1
            stats[ALIGNED_KEY]["skipped"] += 1
            continue

        is_long = direction == "LONG"
        target_trend = 1 if is_long else -1
        timeout_ms = pat_ms + TIMEOUT_H * 3600 * 1000

        # ST состояние на момент CV
        st_at_cv = st_state_at_ts(st_series, pat_ms)
        already_aligned = st_at_cv and st_at_cv.get("trend") == target_trend
        # Статистика: в какую сторону ST 30m был в момент CV
        if st_at_cv and st_at_cv.get("trend"):
            trend_at = st_at_cv.get("trend")
            st_state_at = "up" if trend_at == 1 else "down"
            key = f"{direction}_at_st_{st_state_at}"
            if key in st_at_cv_stats:
                st_at_cv_stats[key] += 1
        else:
            st_at_cv_stats[f"{direction}_at_st_unk"] += 1

        # Поиск flip'а в окне [pat_at .. pat_at+10ч]
        fresh_flip_idx = None
        for i in range(len(st_series)):
            bar = st_series[i]
            if bar["t"] < pat_ms: continue
            if bar["t"] > timeout_ms: break
            if bar["trend"] != target_trend: continue
            prev = st_series[i-1] if i > 0 else None
            if prev and prev.get("trend") and prev["trend"] != target_trend:
                fresh_flip_idx = i
                break

        # FRESH: только если есть свежий flip
        if fresh_flip_idx is not None:
            _simulate_st30m_trade(stats[FRESH_KEY], st_series, fresh_flip_idx, is_long,
                                  BUFFER_PCT, MAX_HOLD_H, BARS_PER_HOUR)
        else:
            stats[FRESH_KEY]["timeout"] += 1

        # ALIGNED: flip ИЛИ уже в сторону
        if fresh_flip_idx is not None:
            _simulate_st30m_trade(stats[ALIGNED_KEY], st_series, fresh_flip_idx, is_long,
                                  BUFFER_PCT, MAX_HOLD_H, BARS_PER_HOUR)
        elif already_aligned:
            # берём бар на момент CV (first >= pat_ms) как entry
            entry_idx = None
            for i in range(len(st_series)):
                if st_series[i]["t"] >= pat_ms:
                    entry_idx = i; break
            if entry_idx is not None:
                _simulate_st30m_trade(stats[ALIGNED_KEY], st_series, entry_idx, is_long,
                                      BUFFER_PCT, MAX_HOLD_H, BARS_PER_HOUR)
            else:
                stats[ALIGNED_KEY]["skipped"] += 1
        else:
            stats[ALIGNED_KEY]["timeout"] += 1

        # ── BASE_FLIP: flip + структура (higher low / lower high между CV и flip) ──
        if fresh_flip_idx is None:
            stats[BASE_KEY]["timeout"] += 1
        else:
            # Собираем бары [pat_ms .. flip_idx)
            window = []
            for j in range(len(st_series)):
                b = st_series[j]
                if b["t"] < pat_ms:
                    continue
                if j >= fresh_flip_idx:
                    break
                window.append((j, b.get("low") or b.get("l"), b.get("high") or b.get("h")))
            if len(window) < 3:
                stats[BASE_KEY]["no_base"] += 1
            else:
                if is_long:
                    # capitulation = самый низкий low; после него ≥2 бара с low > cap*(1+0.2%)
                    cap_idx, cap_val, _ = min(window, key=lambda x: x[1])
                    base_bars = [(i, lo, hi) for (i, lo, hi) in window
                                 if i > cap_idx and lo > cap_val * 1.002]
                    if len(base_bars) >= 2:
                        higher_low = min(lo for (_, lo, _) in base_bars)
                        sl_override = higher_low * (1 - BUFFER_PCT / 100.0)
                        stats[BASE_KEY]["base_ok"] += 1
                        _simulate_base_flip_trade(stats[BASE_KEY], st_series, fresh_flip_idx,
                                                  True, sl_override, MAX_HOLD_H, BARS_PER_HOUR)
                    else:
                        stats[BASE_KEY]["no_base"] += 1
                else:
                    # зеркально: capitulation = самый высокий high; ≥2 бара с high < cap*(1-0.2%)
                    cap_idx, _, cap_val = max(window, key=lambda x: x[2])
                    base_bars = [(i, lo, hi) for (i, lo, hi) in window
                                 if i > cap_idx and hi < cap_val * 0.998]
                    if len(base_bars) >= 2:
                        lower_high = max(hi for (_, _, hi) in base_bars)
                        sl_override = lower_high * (1 + BUFFER_PCT / 100.0)
                        stats[BASE_KEY]["base_ok"] += 1
                        _simulate_base_flip_trade(stats[BASE_KEY], st_series, fresh_flip_idx,
                                                  False, sl_override, MAX_HOLD_H, BARS_PER_HOUR)
                    else:
                        stats[BASE_KEY]["no_base"] += 1

        # ── BASE_STRICT: ≥4 bars, ≥0.5% gap, flip-бар закрывается выше max(base highs) ──
        # + BASE_STRICT_VOL: тот же критерий + volume spike на flip-баре
        if fresh_flip_idx is None:
            stats[STRICT_KEY]["timeout"] += 1
            stats[STRICT_VOL_KEY]["timeout"] += 1
        else:
            window2 = []
            for j in range(len(st_series)):
                b = st_series[j]
                if b["t"] < pat_ms:
                    continue
                if j >= fresh_flip_idx:
                    break
                window2.append((j, b["low"], b["high"]))
            flip_bar = st_series[fresh_flip_idx]
            flip_close = flip_bar["close"]
            passed_strict = False
            sl_strict = None
            ref_high_or_low = None  # для breakout-подтверждения
            if len(window2) >= 5:
                if is_long:
                    cap_idx, cap_val, _ = min(window2, key=lambda x: x[1])
                    base_bars2 = [(i, lo, hi) for (i, lo, hi) in window2
                                  if i > cap_idx and lo > cap_val * 1.005]
                    if len(base_bars2) >= 4:
                        base_max_high = max(hi for (_, _, hi) in base_bars2)
                        higher_low = min(lo for (_, lo, _) in base_bars2)
                        # Breakout confirmation
                        if flip_close > base_max_high:
                            sl_strict = higher_low * (1 - BUFFER_PCT / 100.0)
                            ref_high_or_low = higher_low
                            passed_strict = True
                else:
                    cap_idx, _, cap_val = max(window2, key=lambda x: x[2])
                    base_bars2 = [(i, lo, hi) for (i, lo, hi) in window2
                                  if i > cap_idx and hi < cap_val * 0.995]
                    if len(base_bars2) >= 4:
                        base_min_low = min(lo for (_, lo, _) in base_bars2)
                        lower_high = max(hi for (_, _, hi) in base_bars2)
                        if flip_close < base_min_low:
                            sl_strict = lower_high * (1 + BUFFER_PCT / 100.0)
                            ref_high_or_low = lower_high
                            passed_strict = True

            if not passed_strict:
                stats[STRICT_KEY]["no_base"] += 1
                stats[STRICT_VOL_KEY]["no_base"] += 1
            else:
                stats[STRICT_KEY]["base_ok"] += 1
                _simulate_base_flip_trade(stats[STRICT_KEY], st_series, fresh_flip_idx,
                                          is_long, sl_strict, MAX_HOLD_H, BARS_PER_HOUR)
                # Volume filter: flip-бар vol > avg(last 10) × 1.5
                vol_ok = False
                try:
                    flip_vol = float(cand_st[fresh_flip_idx].get("v") or 0.0)
                    vols = [float(cand_st[k].get("v") or 0.0)
                            for k in range(max(0, fresh_flip_idx - 10), fresh_flip_idx)]
                    avg_vol = sum(vols) / len(vols) if vols else 0.0
                    vol_ok = avg_vol > 0 and flip_vol > avg_vol * 1.5
                except Exception:
                    vol_ok = False
                if vol_ok:
                    stats[STRICT_VOL_KEY]["base_ok"] += 1
                    _simulate_base_flip_trade(stats[STRICT_VOL_KEY], st_series, fresh_flip_idx,
                                              is_long, sl_strict, MAX_HOLD_H, BARS_PER_HOUR)
                else:
                    stats[STRICT_VOL_KEY]["vol_skip"] += 1

    # Формируем отчёт
    rows = []
    for name, st in stats.items():
        closed = st["wins"] + st["losses"]
        wr = round(st["wins"] / closed * 100, 1) if closed else 0.0
        avg_r = round(st["sum_r"] / st["executed"], 3) if st["executed"] else 0.0
        # PF
        wins_r = sum_losses_r = 0
        # Приблизительно: PF = wins × avg_win / losses × avg_loss, но у нас только sum_r
        pf = None  # упрощённо
        rows.append({
            "name": name,
            "executed": st["executed"],
            "wins": st["wins"], "losses": st["losses"], "open": st["open"],
            "wr": wr, "avg_r": avg_r,
            "sum_r": round(st["sum_r"], 2),
            "sum_pct": round(st["sum_pct"], 1),
            "timeout": st["timeout"],
            "invalidated": st["invalidated"],
            "skipped": st["skipped"],
            "no_base": st.get("no_base", 0),
            "base_ok": st.get("base_ok", 0),
            "phase_skip": st.get("phase_skip", 0),
            "vol_skip": st.get("vol_skip", 0),
        })

    return {
        "ok": True, "days": days,
        "st_tf": ST_TF, "timeout_h": TIMEOUT_H,
        "total_cv_signals": len(raw),
        "invalidated_by_newer": len(invalid_ids),
        "pairs_cached": len(c_st),
        "strategies": rows,
        "st_at_cv_stats": st_at_cv_stats,
    }


def _simulate_st30m_trade(st_dict: dict, st_series: list, entry_idx: int,
                          is_long: bool, buffer_pct: float, max_hold_h: int,
                          bars_per_hour: int = 2):
    """Симуляция: entry = close[entry_idx], SL = st_value∓buffer,
    exit = opposite flip close или после max_hold_h."""
    if entry_idx >= len(st_series) - 2:
        st_dict["skipped"] += 1
        return
    entry_bar = st_series[entry_idx]
    entry = entry_bar["close"]
    st_val = entry_bar.get("st")
    if st_val is None:
        st_dict["skipped"] += 1
        return
    if is_long:
        sl = st_val * (1 - buffer_pct / 100.0)
        if sl >= entry:
            st_dict["skipped"] += 1
            return
    else:
        sl = st_val * (1 + buffer_pct / 100.0)
        if sl <= entry:
            st_dict["skipped"] += 1
            return
    risk = abs(entry - sl)
    if risk <= 0:
        st_dict["skipped"] += 1
        return

    max_bars = max_hold_h * bars_per_hour  # 30m→2, 1h→1
    end_idx = min(entry_idx + max_bars, len(st_series) - 1)
    target_trend = 1 if is_long else -1
    what = "OPEN"; r_mult = 0.0; pnl_pct = 0.0
    last_bar = None
    for i in range(entry_idx + 1, end_idx + 1):
        bar = st_series[i]
        last_bar = bar
        hi, lo, cl = bar["high"], bar["low"], bar["close"]
        # SL
        if is_long and lo <= sl:
            what = "SL"; r_mult = -1.0
            pnl_pct = (sl - entry) / entry * 100
            break
        if (not is_long) and hi >= sl:
            what = "SL"; r_mult = -1.0
            pnl_pct = (sl - entry) / entry * 100 * -1
            break
        # Opposite flip → exit
        if bar.get("trend") and bar["trend"] != target_trend:
            what = "TRAIL"
            r_mult = (cl - entry) / risk if is_long else (entry - cl) / risk
            pnl_pct = (cl - entry) / entry * 100 * (1 if is_long else -1)
            break
    if what == "OPEN" and last_bar:
        last_c = last_bar["close"]
        r_mult = (last_c - entry) / risk if is_long else (entry - last_c) / risk
        pnl_pct = (last_c - entry) / entry * 100 * (1 if is_long else -1)

    st_dict["executed"] += 1
    if what == "SL": st_dict["losses"] += 1
    elif what == "TRAIL" and r_mult > 0: st_dict["wins"] += 1
    elif what == "TRAIL": st_dict["losses"] += 1
    else: st_dict["open"] += 1
    st_dict["sum_r"] += r_mult
    st_dict["sum_pct"] += pnl_pct


def _simulate_base_flip_trade(st_dict: dict, st_series: list, entry_idx: int,
                              is_long: bool, sl_override: float, max_hold_h: int,
                              bars_per_hour: int = 2, tp_r: float = 3.0):
    """Симуляция BASE_FLIP: entry = close[entry_idx], SL = структурный
    (higher_low/lower_high ± buffer), TP = +tp_r по R, или exit на opposite flip."""
    if entry_idx >= len(st_series) - 2:
        st_dict["skipped"] += 1
        return
    entry_bar = st_series[entry_idx]
    entry = entry_bar["close"]
    sl = sl_override
    if is_long and sl >= entry:
        st_dict["skipped"] += 1
        return
    if (not is_long) and sl <= entry:
        st_dict["skipped"] += 1
        return
    risk = abs(entry - sl)
    if risk <= 0:
        st_dict["skipped"] += 1
        return
    tp = entry + tp_r * risk if is_long else entry - tp_r * risk

    max_bars = max_hold_h * bars_per_hour
    end_idx = min(entry_idx + max_bars, len(st_series) - 1)
    target_trend = 1 if is_long else -1
    what = "OPEN"; r_mult = 0.0; pnl_pct = 0.0
    last_bar = None
    for i in range(entry_idx + 1, end_idx + 1):
        bar = st_series[i]
        last_bar = bar
        hi, lo, cl = bar["high"], bar["low"], bar["close"]
        # SL
        if is_long and lo <= sl:
            what = "SL"; r_mult = -1.0
            pnl_pct = (sl - entry) / entry * 100
            break
        if (not is_long) and hi >= sl:
            what = "SL"; r_mult = -1.0
            pnl_pct = (sl - entry) / entry * 100 * -1
            break
        # TP (+tp_r)
        if is_long and hi >= tp:
            what = "TP"; r_mult = tp_r
            pnl_pct = (tp - entry) / entry * 100
            break
        if (not is_long) and lo <= tp:
            what = "TP"; r_mult = tp_r
            pnl_pct = (tp - entry) / entry * 100 * -1
            break
        # Opposite flip → trail-exit
        if bar.get("trend") and bar["trend"] != target_trend:
            what = "TRAIL"
            r_mult = (cl - entry) / risk if is_long else (entry - cl) / risk
            pnl_pct = (cl - entry) / entry * 100 * (1 if is_long else -1)
            break
    if what == "OPEN" and last_bar:
        last_c = last_bar["close"]
        r_mult = (last_c - entry) / risk if is_long else (entry - last_c) / risk
        pnl_pct = (last_c - entry) / entry * 100 * (1 if is_long else -1)

    st_dict["executed"] += 1
    if what == "TP": st_dict["wins"] += 1
    elif what == "SL": st_dict["losses"] += 1
    elif what == "TRAIL" and r_mult > 0: st_dict["wins"] += 1
    elif what == "TRAIL": st_dict["losses"] += 1
    else: st_dict["open"] += 1
    st_dict["sum_r"] += r_mult
    st_dict["sum_pct"] += pnl_pct


async def _run_backtest_cv_st30m(days: int):
    from datetime import datetime as _dt
    try:
        result = await asyncio.to_thread(_backtest_cv_st30m_sync, days)
        _bcst_state["result"] = result
    except Exception as e:
        _bcst_state["error"] = str(e)
        logging.getLogger(__name__).exception("[bcst] crashed")
    finally:
        _bcst_state["running"] = False
        _bcst_state["finished_at"] = _dt.utcnow().isoformat()


@app.post("/api/backtest-cv-st30m")
async def api_backtest_cv_st30m_start(payload: dict | None = None):
    """Бектест CV сигналов × ST 30m confirm. Read-only, прод не трогаем.
    payload: {days: 7, timeout_h: 24} — таймаут ожидания flip ST 30m."""
    from datetime import datetime as _dt
    if _bcst_state.get("running"):
        return {"ok": False, "error": "already running", "state": _bcst_state}
    days = int((payload or {}).get("days", 7))
    timeout_h = int((payload or {}).get("timeout_h", 10))
    st_tf = str((payload or {}).get("st_tf", "30m")).lower()
    if st_tf not in ("30m", "1h"):
        st_tf = "30m"
    _bcst_state.update({
        "running": True, "started_at": _dt.utcnow().isoformat(),
        "finished_at": None,
        "progress": {"processed": 0, "total": 0, "current": ""},
        "result": None, "error": None,
        "timeout_h": timeout_h,
        "st_tf": st_tf,
    })
    asyncio.create_task(_run_backtest_cv_st30m(days))
    return {"ok": True, "started": True, "days": days, "timeout_h": timeout_h, "st_tf": st_tf}


@app.get("/api/backtest-cv-st30m/status")
async def api_backtest_cv_st30m_status():
    return _bcst_state


# ═══════════════════════════════════════════════════════════════════
# BACKTEST ST FLIPS — чистая стратегия "вход на flip UP, выход на flip DOWN"
# Тестируем 5 TF (15m/30m/1h/4h/1d) на всех фьючерсных парах
# за N дней. Цель: найти самый стабильный TF.
# ═══════════════════════════════════════════════════════════════════
_bstf_state: dict = {
    "running": False, "started_at": None, "finished_at": None,
    "progress": {"processed": 0, "total": 0, "current": ""},
    "result": None, "error": None,
}


def _rsi_wilder(closes: list, period: int = 14) -> list:
    """RSI(Wilder). Возвращает list той же длины что closes, первые period+1
    позиций = None."""
    n = len(closes)
    if n < period + 2:
        return [None] * n
    gains = [0.0] * n
    losses = [0.0] * n
    for i in range(1, n):
        ch = closes[i] - closes[i-1]
        if ch > 0: gains[i] = ch
        elif ch < 0: losses[i] = -ch
    rsi: list = [None] * n
    avg_g = sum(gains[1:period+1]) / period
    avg_l = sum(losses[1:period+1]) / period
    rsi[period] = 100.0 if avg_l == 0 else 100.0 - 100.0 / (1 + avg_g / avg_l)
    for i in range(period + 1, n):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rsi[i] = 100.0 if avg_l == 0 else 100.0 - 100.0 / (1 + avg_g / avg_l)
    return rsi


def _backtest_st_flips_sync(days: int, max_pairs: int,
                            tf_filter: str | None = None,
                            rsi_min_long: float | None = None,
                            rsi_max_short: float | None = None) -> dict:
    """Для каждого TF: перебираем пары, находим все ST flip'ы, симулируем
    bidirectional (LONG на UP flip, SHORT на DOWN flip). Выход = противоположный
    flip. Risk = |entry - ST_value| на баре входа. R = (exit-entry)/risk.

    Опциональные фильтры:
      tf_filter — "1h"/"30m"/etc, если указан — только этот TF
      rsi_min_long — минимальный RSI(14) на баре входа для LONG (skip если ниже)
      rsi_max_short — максимальный RSI(14) для SHORT (skip если выше)"""
    from exchange import get_klines_any
    from backtest_supertrend import compute_st_series
    from anomaly_scanner import get_all_futures_pairs
    import statistics as _stats
    import logging as _log
    import time as _time
    logger = _log.getLogger(__name__)

    TFS = [
        ("15m", 7,  2.0),
        ("30m", 7,  2.5),
        ("1h",  10, 3.0),
        ("4h",  10, 3.0),
        ("1d",  14, 3.0),
    ]
    if tf_filter:
        TFS = [t for t in TFS if t[0] == tf_filter]
    use_rsi = rsi_min_long is not None or rsi_max_short is not None

    all_pairs = get_all_futures_pairs() or []
    # Берём первые max_pairs (по порядку exchangeInfo — там разнообразие)
    pairs = all_pairs[:max_pairs]
    since_ms = int((_time.time() - days * 86400) * 1000)

    _bstf_state["progress"]["total"] = len(pairs) * len(TFS)
    results: dict = {}
    processed = 0

    for tf, period, mult in TFS:
        rsi_skipped = 0
        rsi_passed = 0
        tf_trades = []
        pair_sums: list = []  # [{pair, r, pct, n}]
        for pair in pairs:
            processed += 1
            _bstf_state["progress"]["processed"] = processed
            _bstf_state["progress"]["current"] = f"{tf} {pair}"
            try:
                candles = get_klines_any(pair, tf, 500) or []
            except Exception:
                continue
            if not candles or len(candles) < 30:
                continue
            # Фильтруем по окну в днях
            candles = [c for c in candles if c["t"] >= since_ms]
            if len(candles) < 30:
                continue
            st_series = compute_st_series(candles, period, mult)
            if not st_series:
                continue
            # RSI(14) по тем же свечам
            rsi_arr = _rsi_wilder([c["c"] for c in candles], 14) if use_rsi else None

            open_pos = None
            pair_r = 0.0
            pair_pct = 0.0
            pair_n = 0
            for i in range(1, len(st_series)):
                prev, cur = st_series[i-1], st_series[i]
                if not (prev.get("trend") and cur.get("trend")):
                    continue
                if prev["trend"] == cur["trend"]:
                    continue
                # FLIP detected
                # Close existing pos at cur.close
                if open_pos is not None:
                    exit_p = cur["close"]
                    e = open_pos["entry"]
                    sl = open_pos["sl"]
                    risk = abs(e - sl) if sl else 0
                    if risk <= 0:
                        risk = abs(e) * 0.01  # fallback 1%
                    if open_pos["side"] == "L":
                        r_mult = (exit_p - e) / risk
                        pct = (exit_p - e) / e * 100
                    else:
                        r_mult = (e - exit_p) / risk
                        pct = (e - exit_p) / e * 100
                    pair_r += r_mult
                    pair_pct += pct
                    pair_n += 1
                    tf_trades.append(r_mult)
                # RSI-фильтр на открытие
                side = "L" if cur["trend"] == 1 else "S"
                if use_rsi:
                    rsi_v = rsi_arr[i] if rsi_arr and i < len(rsi_arr) else None
                    if rsi_v is None:
                        open_pos = None
                        continue
                    if side == "L" and rsi_min_long is not None and rsi_v < rsi_min_long:
                        rsi_skipped += 1
                        open_pos = None
                        continue
                    if side == "S" and rsi_max_short is not None and rsi_v > rsi_max_short:
                        rsi_skipped += 1
                        open_pos = None
                        continue
                    rsi_passed += 1
                # Open new pos on this flip
                open_pos = {
                    "entry": cur["close"],
                    "sl": cur.get("st"),
                    "side": side,
                    "idx": i,
                }
            # Закрываем по последней свече (для справедливого учёта open позиций)
            if open_pos is not None:
                last_bar = st_series[-1]
                e = open_pos["entry"]; sl = open_pos["sl"]
                risk = abs(e - sl) if sl else 0
                if risk <= 0: risk = abs(e) * 0.01
                exit_p = last_bar["close"]
                if open_pos["side"] == "L":
                    r_mult = (exit_p - e) / risk
                    pct = (exit_p - e) / e * 100
                else:
                    r_mult = (e - exit_p) / risk
                    pct = (e - exit_p) / e * 100
                pair_r += r_mult; pair_pct += pct; pair_n += 1
                tf_trades.append(r_mult)

            if pair_n > 0:
                pair_sums.append({"pair": pair, "r": pair_r, "pct": pair_pct, "n": pair_n})

        n_trades = len(tf_trades)
        wins = sum(1 for r in tf_trades if r > 0)
        losses = sum(1 for r in tf_trades if r <= 0)
        avg_r = sum(tf_trades) / n_trades if n_trades else 0.0
        sum_r = sum(tf_trades)
        wr = wins / n_trades * 100 if n_trades else 0.0

        # "Стабильность": доля пар с положительным sum_R
        pairs_positive = sum(1 for p in pair_sums if p["r"] > 0)
        pairs_total = len(pair_sums)
        pair_rs = [p["r"] for p in pair_sums]
        std_r = _stats.stdev(pair_rs) if len(pair_rs) > 1 else 0.0
        mean_r = _stats.mean(pair_rs) if pair_rs else 0.0
        sharpe_like = mean_r / std_r if std_r > 0 else 0.0

        top3 = sorted(pair_sums, key=lambda x: -x["r"])[:3]
        bot3 = sorted(pair_sums, key=lambda x: x["r"])[:3]

        results[tf] = {
            "period": period, "mult": mult,
            "trades": n_trades,
            "wins": wins, "losses": losses,
            "wr": round(wr, 1),
            "avg_r": round(avg_r, 3),
            "sum_r": round(sum_r, 2),
            "pairs_total": pairs_total,
            "pairs_positive": pairs_positive,
            "pairs_positive_pct": round(pairs_positive / pairs_total * 100, 1) if pairs_total else 0.0,
            "mean_pair_r": round(mean_r, 2),
            "std_pair_r": round(std_r, 2),
            "sharpe_like": round(sharpe_like, 3),
            "top3": [{"pair": p["pair"], "r": round(p["r"], 2), "n": p["n"]} for p in top3],
            "bot3": [{"pair": p["pair"], "r": round(p["r"], 2), "n": p["n"]} for p in bot3],
            "rsi_skipped": rsi_skipped,
            "rsi_passed": rsi_passed,
        }

    return {
        "ok": True,
        "days": days,
        "pairs_tested": len(pairs),
        "tf_filter": tf_filter,
        "rsi_min_long": rsi_min_long,
        "rsi_max_short": rsi_max_short,
        "tfs": results,
    }


async def _run_backtest_st_flips(days: int, max_pairs: int, tf_filter, rsi_min_long, rsi_max_short):
    from datetime import datetime as _dt
    try:
        result = await asyncio.to_thread(_backtest_st_flips_sync, days, max_pairs,
                                         tf_filter, rsi_min_long, rsi_max_short)
        _bstf_state["result"] = result
    except Exception as e:
        _bstf_state["error"] = str(e)
        logging.getLogger(__name__).exception("[bstf] crashed")
    finally:
        _bstf_state["running"] = False
        _bstf_state["finished_at"] = _dt.utcnow().isoformat()


@app.post("/api/backtest-st-flips")
async def api_backtest_st_flips_start(payload: dict | None = None):
    """payload: {days, max_pairs, tf?, rsi_min_long?, rsi_max_short?}
    Пример: {"days":30,"max_pairs":200,"tf":"1h","rsi_min_long":49,"rsi_max_short":51}"""
    from datetime import datetime as _dt
    if _bstf_state.get("running"):
        return {"ok": False, "error": "already running", "state": _bstf_state}
    p = payload or {}
    days = int(p.get("days", 30))
    max_pairs = int(p.get("max_pairs", 100))
    tf_filter = p.get("tf")
    rsi_min_long = p.get("rsi_min_long")
    rsi_max_short = p.get("rsi_max_short")
    rsi_min_long = float(rsi_min_long) if rsi_min_long is not None else None
    rsi_max_short = float(rsi_max_short) if rsi_max_short is not None else None
    _bstf_state.update({
        "running": True, "started_at": _dt.utcnow().isoformat(),
        "finished_at": None,
        "progress": {"processed": 0, "total": 0, "current": ""},
        "result": None, "error": None,
        "tf_filter": tf_filter, "rsi_min_long": rsi_min_long, "rsi_max_short": rsi_max_short,
    })
    asyncio.create_task(_run_backtest_st_flips(days, max_pairs, tf_filter, rsi_min_long, rsi_max_short))
    return {"ok": True, "started": True, "days": days, "max_pairs": max_pairs,
            "tf": tf_filter, "rsi_min_long": rsi_min_long, "rsi_max_short": rsi_max_short}


@app.get("/api/backtest-st-flips/status")
async def api_backtest_st_flips_status():
    return _bstf_state


@app.get("/api/st-flips-debug")
async def api_st_flips_debug(pair: str = "BTCUSDT", tf: str = "1h", limit: int = 10):
    """Debug: вернуть последние N flip'ов по паре с ST/RSI значениями.
    Чтобы вручную сверить с TradingView — правильно ли мы читаем индикаторы."""
    from exchange import get_klines_any
    from backtest_supertrend import compute_st_series
    from datetime import datetime as _dt
    PRESETS = {"15m": (7, 2.0), "30m": (7, 2.5), "1h": (10, 3.0),
               "4h": (10, 3.0), "1d": (14, 3.0)}
    period, mult = PRESETS.get(tf.lower(), (10, 3.0))
    candles = await asyncio.to_thread(get_klines_any, pair, tf, 500)
    if not candles:
        return {"ok": False, "error": "no candles"}
    st_series = compute_st_series(candles, period, mult)
    rsi_arr = _rsi_wilder([c["c"] for c in candles], 14)
    flips = []
    for i in range(1, len(st_series)):
        prev, cur = st_series[i-1], st_series[i]
        if not (prev.get("trend") and cur.get("trend")): continue
        if prev["trend"] == cur["trend"]: continue
        flips.append({
            "idx": i,
            "time_utc": _dt.utcfromtimestamp(cur["t"] / 1000).isoformat(),
            "direction": "UP" if cur["trend"] == 1 else "DOWN",
            "open":   round(cur["open"], 6),
            "high":   round(cur["high"], 6),
            "low":    round(cur["low"], 6),
            "close":  round(cur["close"], 6),
            "st_value": round(cur["st"], 6) if cur.get("st") else None,
            "atr":    round(cur["atr"], 6) if cur.get("atr") else None,
            "rsi_14": round(rsi_arr[i], 2) if rsi_arr and i < len(rsi_arr) and rsi_arr[i] is not None else None,
        })
    flips_tail = flips[-limit:]
    # Последний бар (current state)
    last = st_series[-1]
    last_rsi = rsi_arr[-1] if rsi_arr else None
    return {
        "ok": True,
        "pair": pair, "tf": tf, "preset": {"period": period, "mult": mult},
        "bars_total": len(candles),
        "flips_total": len(flips),
        "last_bar": {
            "time_utc": _dt.utcfromtimestamp(last["t"] / 1000).isoformat(),
            "close": round(last["close"], 6),
            "trend": "UP" if last["trend"] == 1 else ("DOWN" if last["trend"] == -1 else "—"),
            "st_value": round(last["st"], 6) if last.get("st") else None,
            "rsi_14": round(last_rsi, 2) if last_rsi is not None else None,
        },
        "recent_flips": flips_tail,
    }


# ═══════════════════════════════════════════════════════════════════
# BACKTEST TRIPLE — 3 стратегии на основе ST + RSI + TSI:
#  S1 PULLBACK        — ST trend + TSI confirm + RSI oversold reentry
#  S2 TSI_LEAD        — TSI crosses 0 → wait ≤6 bars ST flip → entry
#  S3 CV_TSI_FILTER   — CV IMMEDIATE + skip if TSI deeply contra
# ═══════════════════════════════════════════════════════════════════
_btri_state: dict = {
    "running": False, "started_at": None, "finished_at": None,
    "progress": {"processed": 0, "total": 0, "current": ""},
    "result": None, "error": None,
}


def _ema_list(arr: list, period: int) -> list:
    n = len(arr)
    if n == 0: return []
    out = [None] * n
    k = 2.0 / (period + 1)
    out[0] = arr[0] if arr[0] is not None else 0.0
    for i in range(1, n):
        v = arr[i] if arr[i] is not None else 0.0
        out[i] = v * k + out[i-1] * (1 - k)
    return out


def _tsi_series(closes: list, long_p: int = 25, short_p: int = 13) -> list:
    n = len(closes)
    if n < long_p + short_p + 2:
        return [None] * n
    pc = [0.0] * n
    apc = [0.0] * n
    for i in range(1, n):
        pc[i] = closes[i] - closes[i-1]
        apc[i] = abs(pc[i])
    pc1 = _ema_list(pc, long_p)
    pc2 = _ema_list(pc1, short_p)
    apc1 = _ema_list(apc, long_p)
    apc2 = _ema_list(apc1, short_p)
    out = [None] * n
    for i in range(n):
        if apc2[i] and abs(apc2[i]) > 1e-12:
            out[i] = 100.0 * pc2[i] / apc2[i]
    return out


def _backtest_triple_sync(days: int, max_pairs: int) -> dict:
    """Запускает 3 стратегии на 1h таймфрейме. Все стратегии bidirectional.
    Risk: |entry - SL| где SL структурный (ST value или базовый low/high) ± 0.3%.
    R = (exit - entry) / risk."""
    from exchange import get_klines_any
    from backtest_supertrend import compute_st_series
    from anomaly_scanner import get_all_futures_pairs
    from database import _signals, utcnow
    from datetime import timedelta
    import statistics as _stats
    import time as _time
    import logging as _log
    logger = _log.getLogger(__name__)

    BUFFER_PCT = 0.3
    ST_PERIOD, ST_MULT = 10, 3.0  # 1h пресет
    since_ms = int((_time.time() - days * 86400) * 1000)

    all_pairs = get_all_futures_pairs() or []
    pairs = all_pairs[:max_pairs]

    # ── Stats для S1, S2 ──
    s_init = lambda: {"executed": 0, "wins": 0, "losses": 0, "open": 0,
                      "sum_r": 0.0, "sum_pct": 0.0,
                      "skipped_filter": 0, "skipped_data": 0,
                      "pair_sums": []}
    S1 = s_init(); S2 = s_init()

    # ── S3 подготовка: берём CV сигналы ──
    cv_since = utcnow() - timedelta(days=days)
    raw_cv = list(_signals().find({
        "source": "cryptovizor", "pattern_triggered": True,
        "pattern_triggered_at": {"$gte": cv_since},
    }).sort("pattern_triggered_at", 1))
    S3 = s_init()
    S3["total_cv"] = len(raw_cv)

    # Cache: TSI по парам на 1h (чтобы S3 мог брать TSI в момент CV)
    tsi_cache_1h: dict = {}  # pair -> (candles, tsi_series)

    def _get_1h_data(pair: str):
        """Вернуть (candles, st_series, rsi, tsi) с кешем."""
        if pair in tsi_cache_1h:
            return tsi_cache_1h[pair]
        try:
            c = get_klines_any(pair, "1h", 500) or []
        except Exception:
            tsi_cache_1h[pair] = (None, None, None, None)
            return tsi_cache_1h[pair]
        if not c or len(c) < 60:
            tsi_cache_1h[pair] = (None, None, None, None)
            return tsi_cache_1h[pair]
        c = [x for x in c if x["t"] >= since_ms - 40 * 3600 * 1000]  # небольшой backfill
        if len(c) < 60:
            tsi_cache_1h[pair] = (None, None, None, None)
            return tsi_cache_1h[pair]
        sts = compute_st_series(c, ST_PERIOD, ST_MULT)
        cls = [x["c"] for x in c]
        rsi = _rsi_wilder(cls, 14)
        tsi = _tsi_series(cls, 25, 13)
        tsi_cache_1h[pair] = (c, sts, rsi, tsi)
        return tsi_cache_1h[pair]

    _btri_state["progress"]["total"] = len(pairs) + len(raw_cv)
    processed = 0

    # ══════════════════════════════════════════════════════════
    # S1 + S2 на всех парах
    # ══════════════════════════════════════════════════════════
    for pair in pairs:
        processed += 1
        _btri_state["progress"]["processed"] = processed
        _btri_state["progress"]["current"] = f"S1S2 {pair}"
        c, sts, rsi, tsi = _get_1h_data(pair)
        if not c or not sts or not rsi or not tsi:
            S1["skipped_data"] += 1; S2["skipped_data"] += 1
            continue

        # ── S1 PULLBACK ─────────────────────────────────────
        # LONG:  ST=UP, TSI>0 & rising, RSI 30-45, RSI[i]>RSI[i-1]
        # SHORT: ST=DN, TSI<0 & falling, RSI 55-70, RSI[i]<RSI[i-1]
        # Entry: close триггер-бара. SL: ST value ∓ 0.3%.
        # Exit: ST flip OR TSI crosses zero OR +3R.
        s1_r = 0.0; s1_pct = 0.0; s1_n = 0
        open_pos = None
        for i in range(30, len(sts)):
            bar = sts[i]
            if bar["t"] < since_ms: continue
            # Close existing by exits
            if open_pos is not None:
                is_long = open_pos["side"] == "L"
                hi, lo, cl = bar["high"], bar["low"], bar["close"]
                exit_r = None; exit_pct = None
                if is_long and lo <= open_pos["sl"]:
                    exit_r = -1.0
                    exit_pct = (open_pos["sl"] - open_pos["entry"]) / open_pos["entry"] * 100
                elif (not is_long) and hi >= open_pos["sl"]:
                    exit_r = -1.0
                    exit_pct = (open_pos["entry"] - open_pos["sl"]) / open_pos["entry"] * 100
                elif is_long and hi >= open_pos["tp"]:
                    exit_r = 3.0
                    exit_pct = (open_pos["tp"] - open_pos["entry"]) / open_pos["entry"] * 100
                elif (not is_long) and lo <= open_pos["tp"]:
                    exit_r = 3.0
                    exit_pct = (open_pos["entry"] - open_pos["tp"]) / open_pos["entry"] * 100
                else:
                    # ST flip или TSI cross 0 против — exit at close
                    st_flipped = (is_long and bar["trend"] == -1) or ((not is_long) and bar["trend"] == 1)
                    tsi_cross = tsi[i] is not None and (
                        (is_long and tsi[i] < 0) or ((not is_long) and tsi[i] > 0))
                    if st_flipped or tsi_cross:
                        if is_long:
                            exit_r = (cl - open_pos["entry"]) / open_pos["risk"]
                            exit_pct = (cl - open_pos["entry"]) / open_pos["entry"] * 100
                        else:
                            exit_r = (open_pos["entry"] - cl) / open_pos["risk"]
                            exit_pct = (open_pos["entry"] - cl) / open_pos["entry"] * 100
                if exit_r is not None:
                    S1["executed"] += 1
                    if exit_r >= 3.0 - 1e-6: S1["wins"] += 1
                    elif exit_r <= -1.0 + 1e-6: S1["losses"] += 1
                    elif exit_r > 0: S1["wins"] += 1
                    else: S1["losses"] += 1
                    S1["sum_r"] += exit_r; S1["sum_pct"] += exit_pct
                    s1_r += exit_r; s1_pct += exit_pct; s1_n += 1
                    open_pos = None

            if open_pos is not None:
                continue

            # Entry-check на этом баре
            r_now = rsi[i]; t_now = tsi[i]
            r_prev = rsi[i-1] if i >= 1 else None
            t_prev = tsi[i-1] if i >= 1 else None
            if r_now is None or t_now is None or r_prev is None or t_prev is None:
                continue
            trend = bar["trend"]; st_val = bar.get("st")
            if not trend or not st_val: continue
            # LONG setup
            if trend == 1 and t_now > 0 and t_now > t_prev and 30 <= r_now <= 45 and r_now > r_prev:
                entry = bar["close"]; sl = st_val * (1 - BUFFER_PCT / 100)
                if sl < entry:
                    risk = entry - sl; tp = entry + 3 * risk
                    open_pos = {"side": "L", "entry": entry, "sl": sl, "tp": tp, "risk": risk}
            # SHORT setup
            elif trend == -1 and t_now < 0 and t_now < t_prev and 55 <= r_now <= 70 and r_now < r_prev:
                entry = bar["close"]; sl = st_val * (1 + BUFFER_PCT / 100)
                if sl > entry:
                    risk = sl - entry; tp = entry - 3 * risk
                    open_pos = {"side": "S", "entry": entry, "sl": sl, "tp": tp, "risk": risk}

        if s1_n > 0: S1["pair_sums"].append({"pair": pair, "r": s1_r, "n": s1_n})

        # ── S2 TSI_LEAD ──────────────────────────────────────
        # TSI crosses 0 → wait ≤6 bars ST flip → entry.
        # SL: min(low)/max(high) в окне ± 0.3%. Exit: TSI cross back OR opposite ST flip.
        s2_r = 0.0; s2_pct = 0.0; s2_n = 0
        open_pos = None
        i = 1
        while i < len(sts):
            bar = sts[i]
            if bar["t"] < since_ms: i += 1; continue
            # Manage open first
            if open_pos is not None:
                is_long = open_pos["side"] == "L"
                hi, lo, cl = bar["high"], bar["low"], bar["close"]
                exit_r = None; exit_pct = None
                if is_long and lo <= open_pos["sl"]:
                    exit_r = -1.0; exit_pct = (open_pos["sl"] - open_pos["entry"]) / open_pos["entry"] * 100
                elif (not is_long) and hi >= open_pos["sl"]:
                    exit_r = -1.0; exit_pct = (open_pos["entry"] - open_pos["sl"]) / open_pos["entry"] * 100
                elif is_long and hi >= open_pos["tp"]:
                    exit_r = 3.0; exit_pct = (open_pos["tp"] - open_pos["entry"]) / open_pos["entry"] * 100
                elif (not is_long) and lo <= open_pos["tp"]:
                    exit_r = 3.0; exit_pct = (open_pos["entry"] - open_pos["tp"]) / open_pos["entry"] * 100
                else:
                    tsi_back = tsi[i] is not None and (
                        (is_long and tsi[i] < 0) or ((not is_long) and tsi[i] > 0))
                    st_against = (is_long and bar["trend"] == -1) or ((not is_long) and bar["trend"] == 1)
                    if tsi_back or st_against:
                        if is_long:
                            exit_r = (cl - open_pos["entry"]) / open_pos["risk"]
                            exit_pct = (cl - open_pos["entry"]) / open_pos["entry"] * 100
                        else:
                            exit_r = (open_pos["entry"] - cl) / open_pos["risk"]
                            exit_pct = (open_pos["entry"] - cl) / open_pos["entry"] * 100
                if exit_r is not None:
                    S2["executed"] += 1
                    if exit_r >= 3.0 - 1e-6: S2["wins"] += 1
                    elif exit_r <= -1.0 + 1e-6: S2["losses"] += 1
                    elif exit_r > 0: S2["wins"] += 1
                    else: S2["losses"] += 1
                    S2["sum_r"] += exit_r; S2["sum_pct"] += exit_pct
                    s2_r += exit_r; s2_pct += exit_pct; s2_n += 1
                    open_pos = None

            if open_pos is not None:
                i += 1; continue

            # TSI zero-crossing detection
            if tsi[i] is None or tsi[i-1] is None:
                i += 1; continue
            bullish_cross = tsi[i-1] <= 0 and tsi[i] > 0
            bearish_cross = tsi[i-1] >= 0 and tsi[i] < 0
            if not (bullish_cross or bearish_cross):
                i += 1; continue
            dir_long = bullish_cross
            # Look forward up to 6 bars for matching ST flip
            flip_idx = None
            for j in range(i, min(i + 7, len(sts))):
                prev = sts[j-1] if j >= 1 else None
                cur = sts[j]
                if prev and cur and prev.get("trend") and cur.get("trend"):
                    if dir_long and prev["trend"] == -1 and cur["trend"] == 1:
                        flip_idx = j; break
                    if (not dir_long) and prev["trend"] == 1 and cur["trend"] == -1:
                        flip_idx = j; break
            if flip_idx is None:
                S2["skipped_filter"] += 1
                i += 1; continue
            # Entry на flip-баре
            flip_bar = sts[flip_idx]
            entry = flip_bar["close"]
            # SL: min(low) / max(high) в окне [i..flip_idx] ± 0.3%
            window = sts[i:flip_idx+1]
            if dir_long:
                w_low = min(b["low"] for b in window)
                sl = w_low * (1 - BUFFER_PCT / 100)
                if sl >= entry:
                    S2["skipped_filter"] += 1; i = flip_idx + 1; continue
                risk = entry - sl; tp = entry + 3 * risk
                open_pos = {"side": "L", "entry": entry, "sl": sl, "tp": tp, "risk": risk}
            else:
                w_high = max(b["high"] for b in window)
                sl = w_high * (1 + BUFFER_PCT / 100)
                if sl <= entry:
                    S2["skipped_filter"] += 1; i = flip_idx + 1; continue
                risk = sl - entry; tp = entry - 3 * risk
                open_pos = {"side": "S", "entry": entry, "sl": sl, "tp": tp, "risk": risk}
            i = flip_idx + 1
        if s2_n > 0: S2["pair_sums"].append({"pair": pair, "r": s2_r, "n": s2_n})

    # ══════════════════════════════════════════════════════════
    # S3 CV + TSI filter — запускаем на CV сигналах
    # ══════════════════════════════════════════════════════════
    for sig in raw_cv:
        processed += 1
        _btri_state["progress"]["processed"] = processed
        pair = sig.get("pair"); direction = sig.get("direction")
        pat_at = sig.get("pattern_triggered_at")
        entry_cv = sig.get("pattern_price") or sig.get("entry")
        tp_cv = sig.get("dca2"); sl_cv = sig.get("dca1")
        if not (pair and direction in ("LONG", "SHORT") and pat_at and entry_cv and tp_cv and sl_cv):
            S3["skipped_data"] += 1; continue
        _btri_state["progress"]["current"] = f"S3 {pair}"
        # TSI в момент CV
        c, sts, rsi, tsi = _get_1h_data(pair)
        if not c or not tsi:
            S3["skipped_data"] += 1; continue
        pat_ms = int(pat_at.timestamp() * 1000)
        tsi_at = None
        for k in range(len(c) - 1, -1, -1):
            if c[k]["t"] <= pat_ms:
                tsi_at = tsi[k]; break
        if tsi_at is None:
            S3["skipped_data"] += 1; continue
        # TSI фильтр: LONG skip если TSI <= -25, SHORT skip если TSI >= 25
        is_long = direction == "LONG"
        if is_long and tsi_at <= -25:
            S3["skipped_filter"] += 1; continue
        if (not is_long) and tsi_at >= 25:
            S3["skipped_filter"] += 1; continue
        # IMMEDIATE симуляция на 15m свечах (pattern_price / dca2 / dca1)
        try:
            cand_15m = get_klines_any(pair, "15m", 500) or []
        except Exception:
            S3["skipped_data"] += 1; continue
        if not cand_15m:
            S3["skipped_data"] += 1; continue
        risk_cv = abs(entry_cv - sl_cv)
        if risk_cv <= 0:
            S3["skipped_data"] += 1; continue
        end_ms = pat_ms + 48 * 3600 * 1000
        what = "OPEN"; r_mult = 0.0; pnl_pct = 0.0
        last_c = None
        for cc in cand_15m:
            if cc["t"] < pat_ms or cc["t"] > end_ms: continue
            last_c = cc
            hi, lo = cc["h"], cc["l"]
            if is_long:
                if lo <= sl_cv:
                    what = "SL"; r_mult = -1.0
                    pnl_pct = (sl_cv - entry_cv) / entry_cv * 100; break
                if hi >= tp_cv:
                    r_mult = abs(tp_cv - entry_cv) / risk_cv; what = "TP"
                    pnl_pct = (tp_cv - entry_cv) / entry_cv * 100; break
            else:
                if hi >= sl_cv:
                    what = "SL"; r_mult = -1.0
                    pnl_pct = (sl_cv - entry_cv) / entry_cv * 100 * -1; break
                if lo <= tp_cv:
                    r_mult = abs(tp_cv - entry_cv) / risk_cv; what = "TP"
                    pnl_pct = (tp_cv - entry_cv) / entry_cv * 100 * -1; break
        if last_c is None:
            S3["skipped_data"] += 1; continue
        if what == "OPEN":
            last_p = last_c["c"]
            r_mult = (last_p - entry_cv) / risk_cv if is_long else (entry_cv - last_p) / risk_cv
            pnl_pct = (last_p - entry_cv) / entry_cv * 100 * (1 if is_long else -1)
        S3["executed"] += 1
        if what == "TP": S3["wins"] += 1
        elif what == "SL": S3["losses"] += 1
        else: S3["open"] += 1
        S3["sum_r"] += r_mult; S3["sum_pct"] += pnl_pct

    def _fmt(stats: dict, name: str) -> dict:
        n = stats["executed"]
        closed = stats["wins"] + stats["losses"]
        wr = round(stats["wins"] / closed * 100, 1) if closed else 0.0
        avg_r = round(stats["sum_r"] / n, 3) if n else 0.0
        pair_rs = [p["r"] for p in stats.get("pair_sums", [])]
        pairs_positive = sum(1 for r in pair_rs if r > 0)
        pairs_total = len(pair_rs)
        std_r = _stats.stdev(pair_rs) if len(pair_rs) > 1 else 0.0
        mean_r = _stats.mean(pair_rs) if pair_rs else 0.0
        top3 = sorted(stats.get("pair_sums", []), key=lambda x: -x["r"])[:3]
        bot3 = sorted(stats.get("pair_sums", []), key=lambda x: x["r"])[:3]
        return {
            "name": name, "executed": n,
            "wins": stats["wins"], "losses": stats["losses"], "open": stats["open"],
            "wr": wr, "avg_r": avg_r,
            "sum_r": round(stats["sum_r"], 2),
            "sum_pct": round(stats["sum_pct"], 1),
            "skipped_filter": stats["skipped_filter"],
            "skipped_data": stats["skipped_data"],
            "pairs_positive": pairs_positive, "pairs_total": pairs_total,
            "pairs_positive_pct": round(pairs_positive / pairs_total * 100, 1) if pairs_total else 0.0,
            "mean_pair_r": round(mean_r, 2), "std_pair_r": round(std_r, 2),
            "top3": [{"pair": p["pair"], "r": round(p["r"], 2), "n": p["n"]} for p in top3],
            "bot3": [{"pair": p["pair"], "r": round(p["r"], 2), "n": p["n"]} for p in bot3],
        }

    return {
        "ok": True, "days": days, "pairs_tested": len(pairs),
        "total_cv_signals": S3.get("total_cv", 0),
        "strategies": [
            _fmt(S1, "S1_PULLBACK"),
            _fmt(S2, "S2_TSI_LEAD"),
            _fmt(S3, "S3_CV_TSI_FILTER"),
        ],
    }


async def _run_backtest_triple(days: int, max_pairs: int):
    from datetime import datetime as _dt
    try:
        result = await asyncio.to_thread(_backtest_triple_sync, days, max_pairs)
        _btri_state["result"] = result
    except Exception as e:
        _btri_state["error"] = str(e)
        logging.getLogger(__name__).exception("[btri] crashed")
    finally:
        _btri_state["running"] = False
        _btri_state["finished_at"] = _dt.utcnow().isoformat()


@app.post("/api/backtest-triple")
async def api_backtest_triple_start(payload: dict | None = None):
    from datetime import datetime as _dt
    if _btri_state.get("running"):
        return {"ok": False, "error": "already running", "state": _btri_state}
    p = payload or {}
    days = int(p.get("days", 7))
    max_pairs = int(p.get("max_pairs", 200))
    _btri_state.update({
        "running": True, "started_at": _dt.utcnow().isoformat(),
        "finished_at": None,
        "progress": {"processed": 0, "total": 0, "current": ""},
        "result": None, "error": None,
    })
    asyncio.create_task(_run_backtest_triple(days, max_pairs))
    return {"ok": True, "started": True, "days": days, "max_pairs": max_pairs}


@app.get("/api/backtest-triple/status")
async def api_backtest_triple_status():
    return _btri_state


# ═══════════════════════════════════════════════════════════════════
# MARKET PHASE — определение текущей фазы рынка + история смен
# ═══════════════════════════════════════════════════════════════════
@app.get("/api/market-phase")
async def api_market_phase(force: int = 0):
    """Возвращает текущую фазу рынка + метрики + рекомендации.
    Кеш 120с, force=1 пересчитать принудительно."""
    import market_phase as mp
    return await asyncio.to_thread(mp.get_market_phase, bool(force))


@app.get("/api/market-phase/history")
async def api_market_phase_history(hours: int = 72):
    """История смен фазы за последние N часов."""
    import market_phase as mp
    return {"items": await asyncio.to_thread(mp.get_phase_history, hours)}


# ═══════════════════════════════════════════════════════════════════
# ENTRY CHECKER — 8-пунктовая проверка перед ручным входом
# ═══════════════════════════════════════════════════════════════════
@app.get("/api/entry-checker")
async def api_entry_checker(pair: str, direction: str = "LONG"):
    """Rule-based проверка входа — 8 пунктов. Логика в verified_entry.check_entry().
    Используется UI вкладки Entry Checker."""
    import verified_entry as ve
    result = await asyncio.to_thread(ve.check_entry, pair, direction)
    if not result.get("ok"):
        return JSONResponse(
            {"ok": False, "error": result.get("error", "unknown"),
             "hint": "Сначала дождись свежего сигнала (за 8ч для Tradium/CV/Anomaly/Confluence/Cluster, 12ч для SuperTrend всех tier)."},
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    return JSONResponse(
        result,
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/api/verified-signals")
async def api_verified_signals(pair: str = "", hours: int = 168, limit: int = 500):
    """Список verified-сигналов (те что прошли 8-пунктовый чек и отправлены в @topmonetabot).
    Используется UI для маркеров на графиках и badge в журнале.
    pair — опционально (для конкретной пары); hours — окно (default 7 дней); limit — кап."""
    from database import _get_db, utcnow
    from datetime import timedelta

    def _sync():
        since = utcnow() - timedelta(hours=hours)
        q = {"created_at": {"$gte": since}}
        if pair:
            pn = pair.replace("/", "").upper()
            if not pn.endswith("USDT"): pn = pn + "USDT"
            q["pair_norm"] = pn
        col = _get_db().verified_signals
        items = []
        for d in col.find(q).sort("created_at", -1).limit(limit):
            d.pop("_id", None)
            created = d.get("created_at")
            if hasattr(created, "isoformat"):
                d["created_at"] = created.isoformat()
                d["created_at_ts"] = int(created.timestamp())
            items.append(d)
        return {"ok": True, "count": len(items), "items": items}

    return await asyncio.to_thread(_sync)


@app.post("/api/entry-checker/ai-opinion")
async def api_entry_checker_ai_opinion(payload: dict):
    """Дополнительное AI-мнение (Haiku) для Entry Checker.
    payload: {pair, direction, checks[], market, signal}.
    Возвращает {opinion: str, verdict: go|caution|skip, adjust: {...}}.
    """
    from ai_client import get_ai_client
    from config import ANTHROPIC_MODEL_FAST
    import paper_trader as pt

    pair = payload.get("pair", "")
    direction = payload.get("direction", "")
    checks = payload.get("checks", [])
    market = payload.get("market", {})
    signal = payload.get("signal", {})

    # Сжатая сводка rule-based checks
    checks_str = ""
    for i, c in enumerate(checks, 1):
        status_icon = {"ok": "✅", "warn": "⚠️", "bad": "❌"}.get(c.get("status"), "?")
        checks_str += f"  {i}. {status_icon} {c.get('name','?')}: {c.get('comment','')}\n"

    # ai_memory для контекста уроков
    try:
        mem = pt.get_ai_memory()
        mem_str = ""
        if mem.get("summary"):
            mem_str = f"\nПАМЯТЬ AI (из закрытых сделок):\n  {mem['summary'][:400]}\n"
            for l in (mem.get("top_lessons") or [])[:3]:
                mem_str += f"  • {l[:200]}\n"
    except Exception:
        mem_str = ""

    btc = market.get("btc_st", {})
    prompt = (
        f"Ты — опытный crypto-трейдер. Даёшь дополнительное мнение на вход после rule-based проверки.\n\n"
        f"СДЕЛКА: {pair} · {direction}\n"
        f"Сигнал: {signal.get('source','?')} ({signal.get('minutes_ago','?')} мин назад), "
        f"entry={signal.get('entry')}, tp1={signal.get('tp1')}, sl={signal.get('sl')}\n\n"
        f"ФАЗА РЫНКА: {market.get('phase_label', market.get('phase','?'))}\n"
        f"BTC ST: 1h={btc.get('1h','?')} 4h={btc.get('4h','?')} 1d={btc.get('1d','?')} | "
        f"ATR%: {market.get('atr_1h_pct')} | Funding: {market.get('avg_funding')}\n\n"
        f"RULE-BASED ПРОВЕРКИ:\n{checks_str}\n"
        f"{mem_str}"
        f"\nТвоя задача — дать короткое (3-4 предложения) ДОПОЛНИТЕЛЬНОЕ мнение:\n"
        f"- Согласен ли ты с rule-based результатом?\n"
        f"- Есть ли нюансы которые rule-based упустил?\n"
        f"- Какие риски или плюсы видишь?\n"
        f"- Итоговый вердикт: GO / CAUTION / SKIP\n\n"
        f"Ответь ТОЛЬКО JSON без markdown:\n"
        f'{{"opinion": "3-4 предложения обоснования", "verdict": "go"|"caution"|"skip", '
        f'"adjust_size_pct": число_рекомендуемого_размера_%, "adjust_leverage": число_плеча}}'
    )

    try:
        client = get_ai_client()
        msg = await asyncio.to_thread(
            client.messages.create,
            model=ANTHROPIC_MODEL_FAST,
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}],
        )
        text = msg.content[0].text.strip()
        # Robust JSON parsing
        import json as _json
        import re as _re
        try:
            data = _json.loads(text)
        except Exception:
            m = _re.search(r"\{[\s\S]*\}", text)
            data = _json.loads(m.group(0)) if m else {"opinion": text[:500], "verdict": "caution"}
        return {
            "ok": True,
            "opinion": data.get("opinion", "")[:800],
            "verdict": data.get("verdict", "caution"),
            "adjust_size_pct": data.get("adjust_size_pct"),
            "adjust_leverage": data.get("adjust_leverage"),
            "model": ANTHROPIC_MODEL_FAST,
        }
    except Exception as e:
        logging.getLogger(__name__).exception("[entry-checker ai] fail")
        return {"ok": False, "error": str(e)[:200]}


@app.get("/api/paper/be-audit")
async def api_paper_be_audit(hours: int = 48):
    """Аудит: для всех сделок закрытых по BE/TRAIL/AI_CLOSE за последние N часов —
    смотрим что было бы если бы оригинальный SL не подтягивался.

    Берём свечи 15m с opened_at, идём вперёд, смотрим касание TP1 или original_sl.
    Сравниваем факт vs гипотеза.
    Возвращает items[] + summary (total actual/hypo PnL, saved/missed).
    """
    from database import _get_db, utcnow
    from datetime import timedelta
    from exchange import get_klines_any

    def _simulate(symbol, direction, entry, tp1, original_sl, opened_at):
        pair = symbol.replace("USDT", "/USDT") if "USDT" in symbol else symbol
        candles = get_klines_any(pair, "15m", 500)
        if not candles:
            return {"ok": False, "error": "no candles"}
        opened_ts_ms = int(opened_at.timestamp() * 1000) if hasattr(opened_at, "timestamp") else 0
        after = [c for c in candles if c["t"] >= opened_ts_ms]
        if not after:
            return {"ok": False, "error": "no candles after open"}
        is_long = direction == "LONG"
        for c in after:
            hi, lo = c["h"], c["l"]
            if is_long:
                tp_hit = tp1 and hi >= tp1
                sl_hit = lo <= original_sl
            else:
                tp_hit = tp1 and lo <= tp1
                sl_hit = hi >= original_sl
            # В одной свече обе сторонки — консервативно считаем SL первым
            if sl_hit:
                return {"ok": True, "what": "SL", "price": original_sl, "at": c["t"]}
            if tp_hit:
                return {"ok": True, "what": "TP", "price": tp1, "at": c["t"]}
        last = after[-1]["c"]
        return {"ok": True, "what": "OPEN", "price": last, "at": after[-1]["t"]}

    def _pct(direction, entry, exit_price, leverage):
        raw = (exit_price - entry) / entry * 100
        if direction == "SHORT":
            raw = -raw
        return round(raw * leverage, 2)

    def _sync():
        db = _get_db()
        since = utcnow() - timedelta(hours=hours)
        trades = list(db.paper_trades.find({
            "status": {"$in": ["BE", "TRAIL", "AI_CLOSE"]},
            "closed_at": {"$gte": since},
        }).sort("closed_at", -1))

        items = []
        total_actual = 0.0
        total_hypo = 0.0
        saved = 0
        missed = 0
        open_n = 0

        for t in trades:
            entry = t.get("entry") or 0
            tp1 = t.get("tp1")
            original_sl = t.get("original_sl") or t.get("sl")
            leverage = t.get("leverage", 1)
            actual_pct = t.get("pnl_pct", 0)
            opened_at = t.get("opened_at")
            symbol = t.get("symbol", "")
            direction = t.get("direction", "")
            status = t.get("status")

            if not (entry and tp1 and original_sl and opened_at):
                items.append({
                    "trade_id": t.get("trade_id"), "symbol": symbol, "direction": direction,
                    "status": status, "actual_pct": actual_pct, "hypo": None,
                    "error": "missing entry/tp/sl/opened_at",
                })
                continue
            sim = _simulate(symbol, direction, entry, tp1, original_sl, opened_at)
            if not sim.get("ok"):
                items.append({
                    "trade_id": t.get("trade_id"), "symbol": symbol, "direction": direction,
                    "status": status, "actual_pct": actual_pct, "hypo": None,
                    "error": sim.get("error"),
                })
                continue
            hypo_pct = _pct(direction, entry, sim["price"], leverage)
            delta = round(hypo_pct - actual_pct, 2)
            total_actual += actual_pct
            total_hypo += hypo_pct
            if sim["what"] == "SL":
                saved += 1
            elif sim["what"] == "TP":
                missed += 1
            else:
                open_n += 1
            items.append({
                "trade_id": t.get("trade_id"),
                "symbol": symbol, "direction": direction, "status": status,
                "entry": entry, "tp1": tp1, "original_sl": original_sl,
                "exit_price": t.get("exit_price"),
                "leverage": leverage,
                "actual_pct": actual_pct,
                "hypo_what": sim["what"],
                "hypo_price": sim["price"],
                "hypo_pct": hypo_pct,
                "delta_pct": delta,
                "opened_at": opened_at.isoformat() if hasattr(opened_at, "isoformat") else str(opened_at),
                "closed_at": t.get("closed_at").isoformat() if hasattr(t.get("closed_at"), "isoformat") else str(t.get("closed_at") or ""),
            })
        return {
            "ok": True, "count": len(trades), "hours": hours,
            "summary": {
                "total_actual_pct": round(total_actual, 2),
                "total_hypo_pct": round(total_hypo, 2),
                "delta_pct": round(total_hypo - total_actual, 2),
                "saved_by_be_trail": saved,
                "missed_profit": missed,
                "still_open": open_n,
            },
            "items": items,
        }

    return await asyncio.to_thread(_sync)


@app.post("/api/paper/reset")
async def api_paper_reset(payload: dict | None = None):
    """Сброс с опциональным custom balance. payload: {"amount": 5000}"""
    import paper_trader as pt
    amount = None
    if payload and payload.get("amount") is not None:
        try:
            amount = float(payload["amount"])
            if amount < 10 or amount > 10_000_000:
                return {"ok": False, "error": "amount out of range [10, 10M]"}
        except (TypeError, ValueError):
            return {"ok": False, "error": "invalid amount"}
    pt.reset_trading(initial_balance=amount)
    return {"ok": True, "balance": amount or 1000.0}


@app.post("/api/paper/set-balance")
async def api_paper_set_balance(payload: dict):
    """Установить баланс без сброса истории — для перехода на реальную
    сумму. payload: {"amount": 5000}"""
    import paper_trader as pt
    amount = (payload or {}).get("amount")
    if amount is None:
        return {"ok": False, "error": "amount required"}
    try:
        amount = float(amount)
    except (TypeError, ValueError):
        return {"ok": False, "error": "invalid amount"}
    if amount < 10 or amount > 10_000_000:
        return {"ok": False, "error": "amount out of range [10, 10M]"}
    new_balance = pt.set_balance(amount)
    return {"ok": True, "balance": new_balance}


@app.get("/api/journal")
async def api_journal():
    """Все сигналы из 4 источников — для вкладки Журнал.
    Server-side limit 14 days + per-source cap. Cache 45s (async-lock safe).
    _compute_journal делает 7 sync Mongo-запросов → выносим в thread,
    иначе блокируется event loop и тормозят параллельные /api/* (candles и т.п.)."""
    from cache_utils import journal_cache

    async def _compute_in_thread():
        return await asyncio.to_thread(_compute_journal_sync)

    return await journal_cache.get_or_compute("journal_all", _compute_in_thread)


@app.get("/api/backtest/today")
async def api_backtest_today(hours: int = 24, tp_pct: float = 3.0, sl_pct: float = 2.0, hold_h: int = 48):
    """Бэктест всех сегодняшних сигналов (или за N часов).
    Возвращает WR/PnL по каждой категории (CV/Cluster/Confluence, ai≥50/70,
    STRONG+MEGA, top_pick, ALL TOP PICKS vs NON-TOP).

    Query params:
      hours   — окно (default 24 = с начала UTC суток)
      tp_pct  — take profit %
      sl_pct  — stop loss %
      hold_h  — максимум баров удержания
    """
    import backtest_today
    result = await asyncio.to_thread(
        backtest_today.run, hours, tp_pct, sl_pct, hold_h,
    )
    return result


@app.post("/api/market-events/backfill")
async def api_market_events_backfill(payload: dict | None = None):
    """Бэктест: вычисляет исторические смены Keltner ETH и Reversal Meter
    за N дней и записывает их в market_events для отображения на графиках.

    payload: {"days": 30}  (default 30)
    """
    days = int((payload or {}).get("days", 30))
    return await asyncio.to_thread(_market_events_backfill_sync, days)


def _market_events_backfill_sync(days: int) -> dict:
    from database import _market_events, _signals, _confluence, _anomalies, utcnow as _unow
    from datetime import timedelta as _td, timezone as _tz, datetime as _dt
    from exchange import get_klines_any, _calc_keltner
    from watcher import _reversal_zone
    from reversal_meter import compute_score

    stats = {"kc_events": 0, "reversal_events": 0, "candles_loaded": 0, "errors": []}
    since = _unow() - _td(days=days)

    # ── 1) KC ETH: ETH 1H свечи за N дней ─────────────────────────
    try:
        # Берём с запасом: чтобы для первой свечи периода уже был валидный KC,
        # нужно period+1 прошлых свечей. period=20, берём +40 сверху.
        total_hours = days * 24 + 40
        candles = get_klines_any("ETH/USDT", "1h", limit=min(total_hours, 1000))
        stats["candles_loaded"] = len(candles)
        if not candles or len(candles) < 25:
            stats["errors"].append("insufficient ETH candles")
        else:
            # Удаляем существующие KC backfill events в этом окне (чтоб не дублировать)
            _market_events().delete_many({
                "type": "kc",
                "backfilled": True,
                "at": {"$gte": since},
            })
            prev_dir = None
            kc_inserts = []
            # Проходим по свечам начиная с index=25 (есть валидный ATR)
            for i in range(25, len(candles)):
                slice_candles = candles[:i + 1]
                d = _calc_keltner(slice_candles)
                ts = candles[i].get("t") or candles[i].get("time")
                if not ts:
                    continue
                # Binance отдаёт timestamp в миллисекундах — нормализуем
                if ts > 10**12:
                    ts = ts // 1000
                at_dt = _dt.fromtimestamp(ts, tz=_tz.utc).replace(tzinfo=None)
                if at_dt < since:
                    prev_dir = d
                    continue
                if prev_dir is None:
                    prev_dir = d
                    continue
                if d != prev_dir:
                    kc_inserts.append({
                        "at": at_dt,
                        "type": "kc",
                        "from": prev_dir,
                        "to": d,
                        "backfilled": True,
                    })
                    prev_dir = d
            if kc_inserts:
                _market_events().insert_many(kc_inserts)
                stats["kc_events"] = len(kc_inserts)
    except Exception as e:
        import traceback
        stats["errors"].append(f"KC fail: {e}\n{traceback.format_exc()[-500:]}")

    # ── 2) Reversal Meter: прогнать compute_score по каждому часу ──
    try:
        # Preload данных один раз для скорости
        cv_preload = list(_signals().find(
            {"source": "cryptovizor", "pattern_triggered": True,
             "pattern_triggered_at": {"$gte": since - _td(hours=3)},
             "direction": {"$ne": None}},
            {"pair": 1, "direction": 1, "pattern_name": 1, "pattern_triggered_at": 1, "_id": 0}
        ))
        cf_preload = list(_confluence().find(
            {"detected_at": {"$gte": since - _td(hours=3)},
             "direction": {"$ne": None}},
            {"symbol": 1, "direction": 1, "detected_at": 1, "_id": 0}
        ))
        an_preload = list(_anomalies().find(
            {"detected_at": {"$gte": since - _td(hours=3)},
             "direction": {"$ne": None}},
            {"symbol": 1, "direction": 1, "detected_at": 1, "score": 1, "_id": 0}
        ))

        # Удаляем существующие Reversal backfill events в окне
        _market_events().delete_many({
            "type": "reversal",
            "backfilled": True,
            "at": {"$gte": since},
        })

        prev_zone = None
        rev_inserts = []
        score_samples = []
        errors_in_loop = {"count": 0, "first": None}
        # Шаг 1 час за N дней
        for h in range(days * 24):
            at = since + _td(hours=h)
            try:
                r = compute_score(at=at,
                                  cv_preloaded=cv_preload,
                                  cf_preloaded=cf_preload,
                                  an_preloaded=an_preload)
                if h % 24 == 0:
                    score_samples.append(r.get("score", 0))
            except Exception as e_inner:
                errors_in_loop["count"] += 1
                if errors_in_loop["first"] is None:
                    import traceback as _tb2
                    errors_in_loop["first"] = f"{type(e_inner).__name__}: {e_inner} | trace: {_tb2.format_exc()[-400:]}"
                continue
            sc = r.get("score", 0)
            zone = _reversal_zone(sc)
            if prev_zone is None:
                prev_zone = zone
                continue
            if zone != prev_zone:
                rev_inserts.append({
                    "at": at,
                    "type": "reversal",
                    "from": prev_zone,
                    "to": zone,
                    "score": sc,
                    "direction": r.get("direction"),
                    "strength": r.get("strength"),
                    "backfilled": True,
                })
                prev_zone = zone
        if rev_inserts:
            _market_events().insert_many(rev_inserts)
            stats["reversal_events"] = len(rev_inserts)
        stats["reversal_diag"] = {
            "cv_preload": len(cv_preload),
            "cf_preload": len(cf_preload),
            "an_preload": len(an_preload),
            "inner_errors": errors_in_loop["count"],
            "first_error": errors_in_loop["first"],
            "score_samples": score_samples[:10],
        }
    except Exception as e:
        import traceback
        stats["errors"].append(f"Reversal fail: {e}\n{traceback.format_exc()[-500:]}")

    stats["total"] = stats["kc_events"] + stats["reversal_events"]
    stats["days"] = days
    return stats


# Серверный кеш market-events (TTL 60с — они меняются редко, глобальные для ETH)
_mkt_events_cache: dict = {}
_MKT_EVENTS_TTL = 60.0


@app.get("/api/market-events")
async def api_market_events(since_ts: int = 0, until_ts: int = 0, types: str = "kc,reversal"):
    """Смены состояния рынка — Keltner ETH и Reversal Meter.
    Используется для маркеров на всех графиках. Кеш 60с (глобальные события,
    не зависят от пары или TF).
    """
    from database import _market_events as _me
    from datetime import datetime as _dt, timezone as _tz

    key = f"{since_ts}|{until_ts}|{types}"
    now = time.time()
    hit = _mkt_events_cache.get(key)
    if hit and (now - hit[0]) < _MKT_EVENTS_TTL:
        return hit[1]

    type_list = [t.strip() for t in types.split(",") if t.strip()]

    def _sync():
        q = {}
        if type_list:
            q["type"] = {"$in": type_list}
        if since_ts or until_ts:
            dq = {}
            if since_ts:
                dq["$gte"] = _dt.fromtimestamp(since_ts, tz=_tz.utc).replace(tzinfo=None)
            if until_ts:
                dq["$lte"] = _dt.fromtimestamp(until_ts, tz=_tz.utc).replace(tzinfo=None)
            q["at"] = dq
        events = []
        for e in _me().find(q).sort("at", 1).limit(500):
            at = e.get("at")
            if not at:
                continue
            events.append({
                "at": at.isoformat() if hasattr(at, "isoformat") else str(at),
                "at_ts": int(at.timestamp()) if hasattr(at, "timestamp") else 0,
                "type": e.get("type"),
                "from": e.get("from"),
                "to": e.get("to"),
                "score": e.get("score"),
                "direction": e.get("direction"),
            })
        return {"events": events, "count": len(events)}

    # sync Mongo cursor — выносим в thread, чтоб не блокировать event loop
    resp = await asyncio.to_thread(_sync)
    _mkt_events_cache[key] = (now, resp)
    if len(_mkt_events_cache) > 100:
        for k in [k for k, v in _mkt_events_cache.items() if (now - v[0]) > _MKT_EVENTS_TTL * 2]:
            _mkt_events_cache.pop(k, None)
    return resp


@app.get("/api/journal/by-symbol")
async def api_journal_by_symbol(symbol: str, days: int = 30):
    """Все сигналы по конкретной монете из всех источников — для ручного
    поиска. Формат ответа идентичен /api/journal.
    Кеш 60с по (symbol, days) — окно графика каждой монеты дергает этот
    endpoint при открытии."""
    from cache_utils import journal_by_symbol_cache

    async def _compute():
        return await asyncio.to_thread(_compute_journal_by_symbol_sync, symbol, days)

    return await journal_by_symbol_cache.get_or_compute(f"{symbol}|{days}", _compute)


def _compute_journal_by_symbol_sync(symbol: str, days: int) -> dict:
    from database import _signals, _anomalies, _confluence, _clusters
    from datetime import timedelta
    from database import utcnow as _utcnow
    since = _utcnow() - timedelta(days=days)
    # Нормализация: "BNB" → "BNBUSDT", "BNB/USDT" → "BNBUSDT", "bnb" → "BNBUSDT"
    sym_clean = (symbol or "").upper().strip().replace("/", "")
    if not sym_clean:
        return {"items": []}
    if not sym_clean.endswith("USDT"):
        sym_clean = sym_clean + "USDT"
    base = sym_clean[:-4]  # "BNB"
    pair_slash = f"{base}/USDT"
    # Queries для каждой collection — ловим и "pair":"BNB/USDT" и "symbol":"BNBUSDT"
    pair_or = {"$or": [{"pair": pair_slash}, {"symbol": sym_clean}]}

    items = []

    # Tradium
    for s in _signals().find({"source": "tradium", **pair_or}, {
        "pair":1, "direction":1, "entry":1, "tp1":1, "sl":1, "trend":1, "comment":1,
        "ai_score":1, "st_passed":1, "pump_score":1, "pattern_triggered":1,
        "is_top_pick":1, "top_pick_confirmations_count":1,
        "received_at":1, "pattern_triggered_at":1,
    }).sort("received_at", -1):
        pat_trig = bool(s.get("pattern_triggered"))
        at_dt = s.get("pattern_triggered_at") if pat_trig and s.get("pattern_triggered_at") else s.get("received_at")
        if not at_dt or at_dt < since:
            continue
        items.append({
            "source": "tradium",
            "symbol": (s.get("pair") or "").replace("/", "").upper(),
            "pair": s.get("pair", ""),
            "direction": s.get("direction", ""),
            "entry": s.get("entry"),
            "tp1": s.get("tp1"),
            "sl": s.get("sl"),
            "pattern": s.get("trend") or s.get("comment") or "",
            "score": s.get("ai_score"),
            "st_passed": s.get("st_passed"),
            "pump_score": s.get("pump_score", 0),
            "pattern_triggered": pat_trig,
            "is_top_pick": bool(s.get("is_top_pick")),
            "top_pick_confirmations_count": s.get("top_pick_confirmations_count", 0),
            "at": at_dt.isoformat() if hasattr(at_dt, "isoformat") else None,
            "at_ts": int(at_dt.timestamp()) if hasattr(at_dt, "timestamp") else 0,
        })

    # Cryptovizor
    for s in _signals().find({
        "source": "cryptovizor", "pattern_triggered": True,
        "pattern_triggered_at": {"$gte": since}, **pair_or,
    }, {
        "pair":1, "direction":1, "entry":1, "pattern_price":1, "dca1":1, "dca2":1,
        "pattern_name":1, "ai_score":1, "st_passed":1, "pump_score":1,
        "is_top_pick":1, "top_pick_confirmations_count":1,
        "received_at":1, "pattern_triggered_at":1,
    }).sort("pattern_triggered_at", -1):
        at_dt = s.get("pattern_triggered_at") or s.get("received_at")
        items.append({
            "source": "cryptovizor",
            "symbol": (s.get("pair") or "").replace("/", "").upper(),
            "pair": s.get("pair", ""),
            "direction": s.get("direction", ""),
            "entry": s.get("pattern_price") or s.get("entry"),
            "tp1": s.get("dca2"),
            "sl": s.get("dca1"),
            "pattern": s.get("pattern_name", ""),
            "score": s.get("ai_score"),
            "st_passed": s.get("st_passed"),
            "pump_score": s.get("pump_score", 0),
            "is_top_pick": bool(s.get("is_top_pick")),
            "top_pick_confirmations_count": s.get("top_pick_confirmations_count", 0),
            "at": at_dt.isoformat() if hasattr(at_dt, "isoformat") else None,
            "at_ts": int(at_dt.timestamp()) if hasattr(at_dt, "timestamp") else 0,
        })

    # Anomalies
    for a in _anomalies().find({"detected_at": {"$gte": since}, **pair_or}, {
        "symbol":1, "pair":1, "direction":1, "price":1, "anomalies":1,
        "score":1, "st_passed":1, "pump_score":1, "detected_at":1,
        "is_top_pick":1, "top_pick_confirmations_count":1,
    }).sort("detected_at", -1):
        types = [x["type"] for x in a.get("anomalies", [])]
        items.append({
            "source": "anomaly",
            "symbol": a.get("symbol", ""),
            "pair": a.get("pair", ""),
            "direction": a.get("direction", ""),
            "entry": a.get("price"),
            "tp1": None, "sl": None,
            "pattern": ", ".join(types[:3]),
            "score": a.get("score"),
            "st_passed": a.get("st_passed"),
            "pump_score": a.get("pump_score", 0),
            "is_top_pick": bool(a.get("is_top_pick")),
            "top_pick_confirmations_count": a.get("top_pick_confirmations_count", 0),
            "at": a["detected_at"].isoformat() if hasattr(a.get("detected_at"), "isoformat") else None,
            "at_ts": int(a["detected_at"].timestamp()) if hasattr(a.get("detected_at"), "timestamp") else 0,
        })

    # Confluence
    for c in _confluence().find({"detected_at": {"$gte": since}, **pair_or}, {
        "symbol":1, "pair":1, "direction":1, "price":1, "r1":1, "s1":1,
        "pattern":1, "strength":1, "factors":1, "score":1,
        "st_passed":1, "pump_score":1, "is_top_pick":1,
        "top_pick_confirmations_count":1, "detected_at":1,
    }).sort("detected_at", -1):
        items.append({
            "source": "confluence",
            "symbol": c.get("symbol", ""),
            "pair": c.get("pair", ""),
            "direction": c.get("direction", ""),
            "entry": c.get("price"),
            "tp1": c.get("r1"),
            "sl": c.get("s1"),
            "pattern": c.get("pattern") or c.get("strength", ""),
            "score": c.get("score"),
            "st_passed": c.get("st_passed"),
            "pump_score": c.get("pump_score", 0),
            "is_top_pick": bool(c.get("is_top_pick")),
            "top_pick_confirmations_count": c.get("top_pick_confirmations_count", 0),
            "at": c["detected_at"].isoformat() if hasattr(c.get("detected_at"), "isoformat") else None,
            "at_ts": int(c["detected_at"].timestamp()) if hasattr(c.get("detected_at"), "timestamp") else 0,
        })

    # Clusters
    for cl in _clusters().find({"trigger_at": {"$gte": since}, **pair_or}).sort("trigger_at", -1):
        at_dt = cl.get("trigger_at")
        strength = cl.get("strength", "NORMAL")
        items.append({
            "source": "cluster",
            "symbol": (cl.get("pair") or cl.get("symbol") or "").replace("/", "").upper(),
            "pair": cl.get("pair", ""),
            "direction": cl.get("direction", ""),
            "entry": cl.get("trigger_price"),
            "tp1": cl.get("tp_price"),
            "sl": cl.get("sl_price"),
            "pattern": f"{strength} · {cl.get('signals_count',0)}×{cl.get('sources_count',0)}",
            "score": abs(cl.get("reversal_score") or 0),
            "cluster_strength": strength,
            "cluster_status": cl.get("status", "OPEN"),
            "cluster_pnl": round(cl.get("pnl_percent") or 0, 2) if cl.get("pnl_percent") is not None else None,
            "cluster_id": cl.get("id"),
            "is_top_pick": bool(cl.get("is_top_pick")),
            "top_pick_confirmations_count": cl.get("top_pick_confirmations_count", 0),
            "sources_count": cl.get("sources_count", 0),
            "signals_count": cl.get("signals_count", 0),
            "at": at_dt.isoformat() if hasattr(at_dt, "isoformat") else None,
            "at_ts": int(at_dt.timestamp()) if hasattr(at_dt, "timestamp") else 0,
        })

    # SuperTrend signals для этой монеты (исключаем daily)
    import calendar as _cal
    try:
        from database import _supertrend_signals as _sts
        for s in _sts().find({
            "pair_norm": sym_clean,
            "flip_at": {"$gte": since},
            "tier": {"$in": ["vip", "mtf"]},
        }).sort("flip_at", -1):
            at_dt = s.get("flip_at")
            tier = s.get("tier", "mtf")
            tier_emoji = {"vip": "🏆", "mtf": "🔱"}.get(tier, "🌀")
            tier_label = {"vip": "VIP", "mtf": "Triple MTF"}.get(tier, tier.upper())
            aligned_bots = s.get("aligned_bots", [])
            aligned_tfs = s.get("aligned_tfs", [])
            if tier == "vip" and aligned_bots:
                src_names = list({ab.get("source", "?") for ab in aligned_bots})[:3]
                pattern = f"{tier_emoji} ST {tier_label} + {'+'.join(src_names)}"
            else:
                pattern = f"{tier_emoji} ST {tier_label} ({'+'.join(aligned_tfs)})"
            if at_dt and hasattr(at_dt, "timetuple"):
                at_ts = _cal.timegm(at_dt.timetuple())
                at_iso = at_dt.isoformat() + "Z"
            else:
                at_ts = 0
                at_iso = None
            items.append({
                "source": "supertrend",
                "symbol": s.get("pair_norm", ""),
                "pair": s.get("pair", ""),
                "direction": s.get("direction", ""),
                "entry": s.get("entry_price"),
                "tp1": None, "sl": s.get("sl_price"),
                "pattern": pattern,
                "score": None, "st_passed": None, "pump_score": 0,
                "is_top_pick": tier == "vip",
                "top_pick_confirmations_count": len(aligned_bots),
                "st_tier": tier,
                "aligned_tfs": aligned_tfs,
                "at": at_iso,
                "at_ts": at_ts,
            })
    except Exception:
        pass

    items.sort(key=lambda x: x.get("at_ts", 0), reverse=True)
    return {"items": items, "symbol": sym_clean, "days": days, "count": len(items)}


def _compute_journal_sync():
    """Синхронная версия — вызывается через asyncio.to_thread из api_journal,
    чтоб блокирующие Mongo-курсоры не тормозили весь event loop."""
    from database import _signals, _anomalies, _confluence
    from datetime import datetime, timedelta
    from database import utcnow as _utcnow
    since_14d = _utcnow() - timedelta(days=14)

    items = []

    # Tradium signals (все — их мало, ~33)
    # Если pattern_triggered (DCA4 hit) — используем pattern_triggered_at как время активации.
    for s in _signals().find({"source": "tradium"}, {
        "pair":1, "direction":1, "entry":1, "tp1":1, "sl":1, "trend":1, "comment":1,
        "ai_score":1, "st_passed":1, "pump_score":1, "pattern_triggered":1,
        "is_top_pick":1, "top_pick_confirmations_count":1,
        "received_at":1, "pattern_triggered_at":1,
    }).sort("received_at", -1).limit(300):
        pat_trig = bool(s.get("pattern_triggered"))
        at_dt = s.get("pattern_triggered_at") if pat_trig and s.get("pattern_triggered_at") else s.get("received_at")
        items.append({
            "source": "tradium",
            "symbol": (s.get("pair") or "").replace("/", "").upper(),
            "pair": s.get("pair", ""),
            "direction": s.get("direction", ""),
            "entry": s.get("entry"),
            "tp1": s.get("tp1"),
            "sl": s.get("sl"),
            "pattern": s.get("trend") or s.get("comment") or "",
            "score": s.get("ai_score"),
            "st_passed": s.get("st_passed"),
            "pump_score": s.get("pump_score", 0),
            "pattern_triggered": pat_trig,
            "is_top_pick": bool(s.get("is_top_pick")),
            "top_pick_confirmations_count": s.get("top_pick_confirmations_count", 0),
            "received_at": s["received_at"].isoformat() if hasattr(s.get("received_at"), "isoformat") else None,
            "at": at_dt.isoformat() if hasattr(at_dt, "isoformat") else str(at_dt or ""),
            "at_ts": int(at_dt.timestamp()) if hasattr(at_dt, "timestamp") else 0,
        })

    # Cryptovizor signals (только с паттерном за последние 14 дней, cap 800)
    for s in _signals().find({
        "source": "cryptovizor", "pattern_triggered": True,
        "pattern_triggered_at": {"$gte": since_14d},
    }, {
        "pair":1, "direction":1, "entry":1, "pattern_price":1, "dca1":1, "dca2":1,
        "pattern_name":1, "ai_score":1, "st_passed":1, "pump_score":1,
        "is_top_pick":1, "top_pick_confirmations_count":1,
        "received_at":1, "pattern_triggered_at":1,
    }).sort("pattern_triggered_at", -1).limit(800):
        # Время = когда паттерн сработал (не когда монета добавлена)
        at_dt = s.get("pattern_triggered_at") or s.get("received_at")
        items.append({
            "source": "cryptovizor",
            "symbol": (s.get("pair") or "").replace("/", "").upper(),
            "pair": s.get("pair", ""),
            "direction": s.get("direction", ""),
            "entry": s.get("pattern_price") or s.get("entry"),
            "tp1": s.get("dca2"),  # R1
            "sl": s.get("dca1"),   # S1
            "pattern": s.get("pattern_name", ""),
            "score": s.get("ai_score"),
            "st_passed": s.get("st_passed"),
            "pump_score": s.get("pump_score", 0),
            "is_top_pick": bool(s.get("is_top_pick")),
            "top_pick_confirmations_count": s.get("top_pick_confirmations_count", 0),
            "at": at_dt.isoformat() if hasattr(at_dt, "isoformat") else str(at_dt or ""),
            "at_ts": int(at_dt.timestamp()) if hasattr(at_dt, "timestamp") else 0,
        })

    # Anomalies (14 дней, cap 800)
    for a in _anomalies().find({"detected_at": {"$gte": since_14d}}, {
        "symbol":1, "pair":1, "direction":1, "price":1, "anomalies":1,
        "score":1, "st_passed":1, "pump_score":1, "detected_at":1,
        "is_top_pick":1, "top_pick_confirmations_count":1,
    }).sort("detected_at", -1).limit(800):
        types = [x["type"] for x in a.get("anomalies", [])]
        items.append({
            "source": "anomaly",
            "symbol": a.get("symbol", ""),
            "pair": a.get("pair", ""),
            "direction": a.get("direction", ""),
            "entry": a.get("price"),
            "tp1": None,
            "sl": None,
            "pattern": ", ".join(types[:3]),
            "score": a.get("score"),
            "st_passed": a.get("st_passed"),
            "pump_score": a.get("pump_score", 0),
            "is_top_pick": bool(a.get("is_top_pick")),
            "top_pick_confirmations_count": a.get("top_pick_confirmations_count", 0),
            "at": a["detected_at"].isoformat() if hasattr(a.get("detected_at"), "isoformat") else str(a.get("detected_at", "")),
            "at_ts": int(a["detected_at"].timestamp()) if hasattr(a.get("detected_at"), "timestamp") else 0,
        })

    # Confluence (14 дней, cap 1500 — их ~1500 за 14 дней)
    for c in _confluence().find({"detected_at": {"$gte": since_14d}}, {
        "symbol":1, "pair":1, "direction":1, "price":1, "r1":1, "s1":1,
        "pattern":1, "strength":1, "factors":1, "score":1,
        "st_passed":1, "pump_score":1, "is_top_pick":1,
        "top_pick_confirmations_count":1, "detected_at":1,
    }).sort("detected_at", -1).limit(1500):
        ftypes = [f["type"] for f in c.get("factors", [])]
        items.append({
            "source": "confluence",
            "symbol": c.get("symbol", ""),
            "pair": c.get("pair", ""),
            "direction": c.get("direction", ""),
            "entry": c.get("price"),
            "tp1": c.get("r1"),
            "sl": c.get("s1"),
            "pattern": c.get("pattern") or c.get("strength", ""),
            "score": c.get("score"),
            "st_passed": c.get("st_passed"),
            "pump_score": c.get("pump_score", 0),
            "is_top_pick": bool(c.get("is_top_pick")),
            "top_pick_confirmations_count": c.get("top_pick_confirmations_count", 0),
            "at": c["detected_at"].isoformat() if hasattr(c.get("detected_at"), "isoformat") else str(c.get("detected_at", "")),
            "at_ts": int(c["detected_at"].timestamp()) if hasattr(c.get("detected_at"), "timestamp") else 0,
        })

    # Paper Trades (BOT6)
    from database import _get_db, _clusters
    pt_col = _get_db().paper_trades
    for t in pt_col.find({"trade_id": {"$exists": True}}).sort("opened_at", -1):
        at_dt = t.get("opened_at")
        pnl = t.get("pnl_pct") or t.get("live_pnl") or 0
        status = t.get("status", "OPEN")
        status_icon = "✅ TP" if status == "TP" else "❌ SL" if status == "SL" else "⏳ OPEN"
        items.append({
            "source": "paper",
            "symbol": t.get("symbol", ""),
            "pair": t.get("pair", t.get("symbol", "").replace("USDT", "/USDT")),
            "direction": t.get("direction", ""),
            "entry": t.get("entry"),
            "tp1": t.get("tp1"),
            "sl": t.get("sl"),
            "pattern": f"×{t.get('leverage',1)} {status_icon}",
            "score": t.get("size_usdt"),
            "st_passed": None,
            "pump_score": 0,
            "at": at_dt.isoformat() if hasattr(at_dt, "isoformat") else str(at_dt or ""),
            "at_ts": int(at_dt.timestamp()) if hasattr(at_dt, "timestamp") else 0,
        })

    # Clusters (композитные сигналы) — полноценные записи с TP/SL
    for c in _clusters().find({}).sort("trigger_at", -1).limit(200):
        at_dt = c.get("trigger_at")
        strength = c.get("strength", "NORMAL")
        status = c.get("status", "OPEN")
        signals_count = c.get("signals_count", 0)
        sources_count = c.get("sources_count", 0)
        pnl = c.get("pnl_percent")
        # Строковый pattern: "MEGA · 4×3 · +1.5R" (если закрыт — добавляем PnL)
        pattern_parts = [strength, f"{signals_count}×{sources_count}"]
        if status == "TP":
            pattern_parts.append("✅")
        elif status == "SL":
            pattern_parts.append("❌")
        items.append({
            "source": "cluster",
            "symbol": (c.get("pair") or c.get("symbol") or "").replace("/", "").upper(),
            "pair": c.get("pair", ""),
            "direction": c.get("direction", ""),
            "entry": c.get("trigger_price"),
            "tp1": c.get("tp_price"),
            "sl": c.get("sl_price"),
            "pattern": " · ".join(pattern_parts),
            "score": abs(c.get("reversal_score") or 0),  # reversal confirmation
            "st_passed": None,
            "pump_score": 0,
            "cluster_strength": strength,
            "cluster_status": status,
            "cluster_pnl": round(pnl, 2) if pnl is not None else None,
            "cluster_id": c.get("id"),
            "is_top_pick": bool(c.get("is_top_pick")),
            "top_pick_confirmations_count": c.get("top_pick_confirmations_count", 0),
            "sources_count": sources_count,
            "signals_count": signals_count,
            "at": at_dt.isoformat() if hasattr(at_dt, "isoformat") else str(at_dt or ""),
            "at_ts": int(at_dt.timestamp()) if hasattr(at_dt, "timestamp") else 0,
        })

    # SuperTrend signals (14 дней) — источник 'supertrend' с tier в pattern
    # ИСКЛЮЧАЕМ daily — их оставляем только на графиках (emoji 🧭) без журнала и бота
    #
    # Дедупликация: в окне 30 мин на одной паре+direction оставляем только
    # один сигнал с высшим tier (vip > mtf). Раньше в журнале появлялись
    # дубли когда VIP и MTF срабатывали почти одновременно.
    import calendar as _cal
    try:
        from database import _supertrend_signals as _sts
        # Собираем все raw записи
        raw_st = list(_sts().find({
            "flip_at": {"$gte": since_14d},
            "tier": {"$in": ["vip", "mtf"]},
        }).sort("flip_at", -1).limit(1500))
        # Dedupe: (pair_norm, direction, bucket_30min) → высший tier
        tier_prio = {"vip": 3, "mtf": 2, "daily": 1}
        best_st: dict = {}
        for s in raw_st:
            flip_at = s.get("flip_at")
            if not flip_at:
                continue
            try:
                bucket = int(flip_at.timestamp() // 1800)  # 30-мин bucket
            except Exception:
                continue
            key = (s.get("pair_norm"), s.get("direction"), bucket)
            prio = tier_prio.get(s.get("tier"), 0)
            ex = best_st.get(key)
            if (not ex) or prio > tier_prio.get(ex.get("tier"), 0):
                best_st[key] = s
        for s in best_st.values():
            flip_dt = s.get("flip_at")
            created_dt = s.get("created_at")
            tier = s.get("tier", "mtf")
            tier_emoji = {"vip": "🏆", "mtf": "🔱"}.get(tier, "🌀")
            tier_label = {"vip": "VIP", "mtf": "Triple MTF"}.get(tier, tier.upper())
            aligned_bots = s.get("aligned_bots", [])
            aligned_tfs = s.get("aligned_tfs", [])
            if tier == "vip" and aligned_bots:
                src_names = list({ab.get("source", "?") for ab in aligned_bots})[:3]
                pattern = f"{tier_emoji} ST {tier_label} + {'+'.join(src_names)}"
            else:
                pattern = f"{tier_emoji} ST {tier_label} ({'+'.join(aligned_tfs)})"
            # Сортировка по created_at (когда сигнал появился в системе и улетел в бота),
            # а не по flip_at (время бара = может быть час назад, из-за чего сигнал
            # проваливался вниз в журнале хотя в бот пришёл только что).
            sort_dt = created_dt or flip_dt
            if sort_dt and hasattr(sort_dt, "timetuple"):
                at_ts = _cal.timegm(sort_dt.timetuple())
                at_iso = sort_dt.isoformat() + "Z"
            else:
                at_ts = 0
                at_iso = str(sort_dt or "")
            # flip_at нужен для графика (маркер на конкретной свече)
            flip_iso = flip_dt.isoformat() + "Z" if (flip_dt and hasattr(flip_dt, "timetuple")) else None
            items.append({
                "source": "supertrend",
                "symbol": s.get("pair_norm", ""),
                "pair": s.get("pair", ""),
                "direction": s.get("direction", ""),
                "entry": s.get("entry_price"),
                "tp1": None,
                "sl": s.get("sl_price"),
                "pattern": pattern,
                "score": None,
                "st_passed": None,
                "pump_score": 0,
                "is_top_pick": tier == "vip",
                "top_pick_confirmations_count": len(aligned_bots),
                "st_tier": tier,
                "aligned_tfs": aligned_tfs,
                "aligned_bots_count": len(aligned_bots),
                "at": at_iso,
                "at_ts": at_ts,
                "flip_at": flip_iso,  # для графика (отдельно от сортировки)
            })
    except Exception as e:
        logging.getLogger(__name__).warning(f"[journal] supertrend fetch fail: {e}")

    # ⏳ CV+ST Flip observation (source='cv_flip') — отдельная observation-ветка,
    # не конкурирует с paper_trader (тот смотрит source='cryptovizor').
    # Lifecycle icons: ⏳ WAITING / 💥 FLIPPED / ❌ TIMEOUT / 🚫 INVALIDATED.
    try:
        from database import _cv_flip_signals
        for d in _cv_flip_signals().find(
            {"cv_triggered_at": {"$gte": since_14d}}
        ).sort("cv_triggered_at", -1).limit(500):
            state = d.get("state", "WAITING")
            state_emoji = {"FLIPPED": "💥", "TIMEOUT": "❌",
                           "INVALIDATED": "🚫"}.get(state, "⏳")
            state_label = {"FLIPPED": "Flipped", "TIMEOUT": "Timeout",
                           "INVALIDATED": "Invalidated",
                           "WAITING": "Waiting"}.get(state, state)
            cv_dt = d.get("cv_triggered_at")
            flip_dt = d.get("flip_at")
            # sort по flip_at если сработало, иначе по cv_triggered_at
            sort_dt = flip_dt or cv_dt
            if sort_dt and hasattr(sort_dt, "timetuple"):
                at_ts = _cal.timegm(sort_dt.timetuple())
                at_iso = sort_dt.isoformat() + "Z"
            else:
                at_ts = 0
                at_iso = str(sort_dt or "")
            flip_iso = flip_dt.isoformat() + "Z" if (flip_dt and hasattr(flip_dt, "timetuple")) else None
            cv_iso = cv_dt.isoformat() + "Z" if (cv_dt and hasattr(cv_dt, "timetuple")) else None
            pair = d.get("pair", "")
            pair_norm = pair.replace("/", "").upper() if pair else ""
            items.append({
                "source": "cv_flip",
                "symbol": pair_norm,
                "pair": pair,
                "direction": d.get("direction", ""),
                "entry": d.get("entry") or d.get("flip_price"),
                "tp1": d.get("tp1"),
                "sl": d.get("sl"),
                "pattern": f"{state_emoji} CV+ST Flip · {state_label}",
                "score": None,
                "st_passed": None,
                "pump_score": 0,
                "is_top_pick": state == "FLIPPED",
                "cv_flip_state": state,
                "cv_triggered_at": cv_iso,
                "flip_at": flip_iso,
                "bars_under_st": d.get("bars_under_st", 0),
                "risk_pct": d.get("risk_pct"),
                "at": at_iso,
                "at_ts": at_ts,
            })
    except Exception as e:
        logging.getLogger(__name__).warning(f"[journal] cv_flip fetch fail: {e}")

    # ✨ Verified Entries (авто-проверка Entry Checker — отправлено в @topmonetabot)
    try:
        from database import _get_db
        vcol = _get_db().verified_signals
        for v in vcol.find({"created_at": {"$gte": since_14d}}).sort("created_at", -1).limit(500):
            at_dt = v.get("created_at")
            verdict = v.get("verdict", "go")
            emoji = "⚠️✨" if verdict == "caution" else "✨"
            vlabel = "VERIFIED" if verdict == "go" else "VERIFIED (caution)"
            src_tag = v.get("signal_source") or "?"
            tier = v.get("signal_tier")
            if tier: src_tag += f"/{tier}"
            pattern_txt = f"{emoji} {vlabel} ({src_tag})"
            counts = v.get("counts") or {}
            pair_raw = v.get("pair") or ""
            pair_norm = v.get("pair_norm") or pair_raw.replace("/", "").upper()
            items.append({
                "source": "verified",
                "symbol": pair_norm,
                "pair": pair_raw,
                "direction": v.get("direction", ""),
                "entry": v.get("entry"),
                "tp1": v.get("tp1"),
                "sl": v.get("sl"),
                "pattern": pattern_txt,
                "score": v.get("signal_score"),
                "st_passed": None,
                "pump_score": 0,
                "is_top_pick": False,
                "top_pick_confirmations_count": 0,
                "verified_verdict": verdict,
                "verified_counts": counts,
                "rr": v.get("rr"),
                "phase": v.get("phase"),
                "at": at_dt.isoformat() if hasattr(at_dt, "isoformat") else str(at_dt or ""),
                "at_ts": int(at_dt.timestamp()) if hasattr(at_dt, "timestamp") else 0,
            })
    except Exception as e:
        logging.getLogger(__name__).warning(f"[journal] verified fetch fail: {e}")

    # Сортируем по дате (новые сверху)
    items.sort(key=lambda x: x.get("at_ts", 0), reverse=True)
    return {"items": items}


# Серверный кеш для /api/journal-candles (TTL per TF)
# Самая медленная часть открытия графика — HTTP к Binance (до 6с cold).
# Повторные запросы в одной сессии → мгновенно из кеша.
# Фоновый прогрев (_candles_prewarm_loop в watcher.py) держит топ-пары горячими.
_candles_cache: dict = {}
_CANDLES_TTL_BY_TF = {
    "15m": 90.0,   # 15m обновляется каждые 15 мин — 90с запас
    "30m": 180.0,  # 3 мин
    "1h":  300.0,  # 5 мин (был 180)
    "2h":  600.0,  # 10 мин
    "4h":  1200.0, # 20 мин (был 600)
    "12h": 3600.0, # 1 час
    "1d":  7200.0, # 2 часа
}
_CANDLES_TTL_DEFAULT = 300.0


def warm_candles_cache(symbol: str, tf: str, limit: int = 200) -> bool:
    """Синхронно прогревает candles cache для заданной пары/TF. Используется
    фоновым циклом в watcher.py. Возвращает True если обновил, False если
    кеш ещё свежий (> 50% TTL осталось) или ошибка."""
    from exchange import get_klines_any
    key = f"{symbol}|{tf}|{limit}"
    now = time.time()
    ttl = _CANDLES_TTL_BY_TF.get((tf or "").lower(), _CANDLES_TTL_DEFAULT)
    hit = _candles_cache.get(key)
    # Если кеш ещё достаточно свежий — пропускаем (экономим Binance rate limit)
    if hit and (now - hit[0]) < ttl * 0.5:
        return False
    pair = symbol.replace("USDT", "/USDT") if "USDT" in symbol else symbol
    try:
        candles = get_klines_any(pair, tf, limit)
        if not candles:
            return False
        data = [{
            "time": int(c["t"] / 1000),
            "open": c["o"], "high": c["h"], "low": c["l"], "close": c["c"],
            "volume": c.get("v", 0),
        } for c in candles]
        _candles_cache[key] = (time.time(), data)
        return True
    except Exception:
        return False


@app.get("/api/journal-candles")
async def api_journal_candles(symbol: str, tf: str = "1h", limit: int = 100):
    """Свечи для Lightweight Charts. Сервер-кеш по TTL per TF со
    stale-while-revalidate: если в кеше есть expired-запись не старше
    STALE_MAX×TTL — сразу возвращаем её + фоном обновляем. Холодный
    запрос к Binance (1-3с) больше не блокирует открытие графика —
    юзер видит чуть устаревшие (максимум minutes old) свечи мгновенно,
    свежие подъедут при следующем открытии.

    При cache-miss (ничего нет вообще) — как раньше, ждём Binance.
    Background prefetch остальных TF запускается всегда при miss.
    """
    from exchange import get_klines_any
    key = f"{symbol}|{tf}|{limit}"
    now = time.time()
    ttl = _CANDLES_TTL_BY_TF.get((tf or "").lower(), _CANDLES_TTL_DEFAULT)
    STALE_MAX = 5.0  # 5×TTL — считаем stale, но ещё приемлемо
    hit = _candles_cache.get(key)
    pair = symbol.replace("USDT", "/USDT") if "USDT" in symbol else symbol

    async def _bg_prefetch_other_tfs():
        for other_tf in ["15m", "30m", "1h", "4h", "1d"]:
            if other_tf == (tf or "").lower():
                continue
            try:
                await asyncio.to_thread(warm_candles_cache, symbol, other_tf, limit)
            except Exception:
                pass

    async def _bg_refresh_current():
        """Фоновое обновление текущего TF (stale-while-revalidate)."""
        try:
            await asyncio.to_thread(warm_candles_cache, symbol, tf, limit)
        except Exception:
            pass

    # Fresh cache hit
    if hit and (now - hit[0]) < ttl:
        return {"ok": True, "candles": hit[1], "cached": True}

    # Stale hit (есть данные, но expired) — мгновенно возвращаем + фоном обновляем
    if hit and (now - hit[0]) < ttl * STALE_MAX:
        try:
            asyncio.create_task(_bg_refresh_current())
        except Exception:
            pass
        return {"ok": True, "candles": hit[1], "stale": True}

    # Hard miss — ждём Binance
    candles = await asyncio.to_thread(get_klines_any, pair, tf, limit)
    if not candles:
        return {"ok": False, "error": "no data"}
    data = []
    for c in candles:
        data.append({
            "time": int(c["t"] / 1000),
            "open": c["o"],
            "high": c["h"],
            "low": c["l"],
            "close": c["c"],
            "volume": c.get("v", 0),
        })
    _candles_cache[key] = (now, data)
    # Lazy eviction старых записей (>STALE_MAX×TTL точно бесполезны)
    if len(_candles_cache) > 500:
        for k in [k for k, v in _candles_cache.items() if (now - v[0]) > ttl * STALE_MAX]:
            _candles_cache.pop(k, None)
    # Background prefetch остальных TF — юзер вероятнее всего переключит
    try:
        asyncio.create_task(_bg_prefetch_other_tfs())
    except Exception:
        pass
    return {"ok": True, "candles": data}


@app.post("/api/save-coin-analysis")
async def api_save_coin_analysis(payload: dict):
    """Сохраняет анализ монеты в comment поле сигнала."""
    from database import _signals
    pair = payload.get("pair", "")
    analysis = payload.get("analysis", "")
    if not pair or not analysis:
        return {"ok": False}
    _signals().update_many(
        {"source": "cryptovizor", "pair": pair, "comment": None},
        {"$set": {"comment": analysis}},
    )
    return {"ok": True}


@app.post("/api/backtest-ai")
async def api_backtest_ai():
    """Бектест только AI-отфильтрованных сигналов."""
    from backtest import run_backtest_filtered
    return await asyncio.to_thread(run_backtest_filtered, "cryptovizor", "AI_SIGNAL")


@app.post("/api/signals/clear-by-pairs")
async def api_clear_by_pairs(payload: dict):
    """Удаляет сигналы по списку пар из бектеста + сохраняет summary."""
    from database import _signals as _sc, _get_db, utcnow
    pairs = payload.get("pairs", [])
    filter_type = payload.get("filter", "pattern")  # "pattern" или "ai"

    if not pairs:
        return {"ok": True, "deleted": 0}

    # Собираем данные для summary
    query = {"source": "cryptovizor", "pair": {"$in": pairs}}
    if filter_type == "ai":
        query["filter_reason"] = {"$regex": "^AI_SIGNAL"}
    else:
        query["status"] = {"$in": ["ПАТТЕРН", "VOLUME"]}

    signals = list(_sc().find(query, {"pair":1, "direction":1, "pattern_name":1, "entry":1, "pattern_price":1}))

    if not signals:
        return {"ok": True, "deleted": 0}

    # Summary
    coins = []
    for s in signals:
        entry = s.get("entry") or 0
        current = s.get("pattern_price") or entry
        pnl = ((current - entry) / entry * 100) if entry > 0 else 0
        if s.get("direction") in ("SHORT", "SELL"):
            pnl = -pnl
        coins.append({
            "pair": (s.get("pair") or "").replace("/USDT", ""),
            "dir": s.get("direction", ""),
            "pattern": s.get("pattern_name", ""),
            "pnl": round(pnl, 2),
        })

    wins = sum(1 for c in coins if c["pnl"] > 0)
    summary = {
        "date": str(utcnow()),
        "type": f"backtest_{filter_type}",
        "count": len(coins),
        "win_rate": round(wins / len(coins) * 100, 1) if coins else 0,
        "total_pnl": round(sum(c["pnl"] for c in coins), 2),
        "avg_pnl": round(sum(c["pnl"] for c in coins) / len(coins), 2) if coins else 0,
        "coins": coins,
    }
    _get_db().backtest_history.insert_one(summary)

    # Удаляем
    result = _sc().delete_many(query)
    broadcast_event("signal_deleted", {"count": result.deleted_count})
    return {"ok": True, "deleted": result.deleted_count, "summary": summary}


@app.post("/api/backtest")
async def api_backtest():
    from backtest import run_backtest
    return await asyncio.to_thread(run_backtest, "cryptovizor")


@app.post("/api/backtest/save")
async def api_backtest_save(payload: dict):
    """Сохраняет результат бектеста в MongoDB (включая все сигналы)."""
    from database import _get_db, utcnow
    payload["saved_at"] = str(utcnow())
    _get_db().settings.update_one(
        {"_id": "last_backtest"},
        {"$set": payload},
        upsert=True,
    )
    return {"ok": True}


@app.get("/api/backtest/saved")
async def api_backtest_saved():
    """Загружает последний сохранённый бектест."""
    from database import _get_db
    doc = _get_db().settings.find_one({"_id": "last_backtest"})
    if not doc:
        return {}
    doc.pop("_id", None)
    return doc


@app.post("/api/ai-criteria/generate")
async def api_ai_criteria_generate():
    """AI анализирует бектест и генерирует список критериев с рекомендациями."""
    from backtest import run_backtest
    bt = await asyncio.to_thread(run_backtest, "cryptovizor")
    if bt.get("error"):
        return {"ok": False, "error": bt["error"]}

    criteria = []
    # По паттернам
    for name, v in bt.get("by_pattern", {}).items():
        if v["count"] >= 2:
            criteria.append({
                "id": f"pattern:{name}",
                "type": "pattern",
                "label": name,
                "count": v["count"],
                "win_rate": v["win_rate"],
                "avg_pnl": v["avg_pnl"],
                "recommended": v["win_rate"] >= 55 and v["avg_pnl"] > 0,
                "enabled": v["win_rate"] >= 55 and v["avg_pnl"] > 0,
            })

    # По направлению
    for d, v in bt.get("by_direction", {}).items():
        criteria.append({
            "id": f"direction:{d}",
            "type": "direction",
            "label": d,
            "count": v["count"],
            "win_rate": v["win_rate"],
            "avg_pnl": v["avg_pnl"],
            "recommended": v["win_rate"] >= 50,
            "enabled": v["win_rate"] >= 50,
        })

    # По часам (группируем в слоты)
    for h, v in bt.get("by_hour", {}).items():
        if v["count"] >= 2:
            criteria.append({
                "id": f"hour:{h}",
                "type": "hour",
                "label": f"{int(h):02d}:00 UTC",
                "count": v["count"],
                "win_rate": v["win_rate"],
                "recommended": v["win_rate"] >= 55,
                "enabled": v["win_rate"] >= 55,
            })

    # Общие пороги
    criteria.append({
        "id": "min_win_rate",
        "type": "threshold",
        "label": f"Min win rate (рекомендация: {max(50, int(bt['overall_win_rate'] - 5))}%)",
        "value": max(50, int(bt["overall_win_rate"] - 5)),
        "recommended": True,
        "enabled": True,
    })
    criteria.append({
        "id": "min_ai_score",
        "type": "threshold",
        "label": "Min AI visual score",
        "value": 40,
        "recommended": True,
        "enabled": True,
    })

    return {
        "ok": True,
        "summary": {
            "total": bt["with_result"],
            "win_rate": bt["overall_win_rate"],
            "avg_pnl": bt["overall_avg_pnl"],
            "top_patterns": bt["top_patterns"],
            "worst_patterns": bt["worst_patterns"],
        },
        "criteria": criteria,
    }


@app.post("/api/ai-criteria/save")
async def api_ai_criteria_save(payload: dict):
    """Сохраняет выбранные пользователем критерии."""
    from database import _get_db, utcnow as _utcnow
    criteria = payload.get("criteria", [])
    _get_db().settings.update_one(
        {"_id": "ai_criteria"},
        {"$set": {"criteria": criteria, "updated_at": str(_utcnow())}},
        upsert=True,
    )
    return {"ok": True, "saved": len(criteria)}


@app.get("/api/ai-criteria")
async def api_ai_criteria_get():
    """Загружает сохранённые критерии."""
    from database import _get_db
    doc = _get_db().settings.find_one({"_id": "ai_criteria"})
    return {"criteria": doc.get("criteria", []) if doc else []}


@app.post("/api/sync-cv")
async def api_sync_cv(limit: int = 500):
    """Синхронизирует сигналы Cryptovizor — подтягивает пропущенные из истории."""
    import userbot as _ub
    from parser_cryptovizor import parse_cryptovizor_message
    from config import BOT2_NAME
    from exchange import get_futures_prices_only as _gp

    client = _ub._tg_client
    if client is None:
        return {"ok": False, "error": "Userbot не запущен"}

    # Ищем Cryptovizor dialog
    cv_id = None
    async for d in client.iter_dialogs():
        if "cryptovizor" in (d.name or "").lower():
            cv_id = d.id
            break
    if not cv_id:
        return {"ok": False, "error": "Cryptovizor dialog не найден"}

    messages = []
    async for m in client.iter_messages(cv_id, limit=limit):
        messages.append(m)
    messages.reverse()

    added = 0
    db = Session()
    try:
        for msg in messages:
            parsed = parse_cryptovizor_message(msg.raw_text or "")
            if not parsed:
                continue
            pairs = [sd["pair"] for sd in parsed]
            prices = await asyncio.to_thread(_gp, pairs)

            for i, sd in enumerate(parsed):
                unique_id = msg.id * 100 + i
                existing = db.query(Signal).filter(
                    Signal.source == BOT2_NAME
                ).filter(Signal.message_id == unique_id).first()
                if existing:
                    continue

                norm = sd["pair"].replace("/", "").upper()
                price = prices.get(norm)
                if price is None:
                    continue

                s = Signal(
                    source=BOT2_NAME,
                    message_id=unique_id,
                    raw_text=msg.raw_text,
                    pair=sd["pair"],
                    direction=sd["direction"],
                    trend=sd["trend"],
                    timeframe="1h",
                    entry=price,
                    status="СЛЕЖУ",
                    received_at=msg.date.replace(tzinfo=None) if msg.date.tzinfo else msg.date,
                )
                db.add(s)
                db.commit()
                db.refresh(s)
                added += 1
    finally:
        db.close()

    return {"ok": True, "scanned": len(messages), "added": added}


@app.post("/api/sync")
async def api_sync(limit: int = 300, bot: str = "tradium"):
    """Прогоняет последние N сообщений из группы: добавляет пропущенные
    Tradium-сигналы и подтягивает отсутствующие графики."""
    import userbot
    from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
    from parser import parse_signal
    from ai_analyzer import analyze_chart, analyze_signal_quality
    from config import SOURCE_GROUP_ID, CHARTS_DIR
    from database import log_event
    from pathlib import Path

    client = userbot._tg_client
    if client is None:
        return {"ok": False, "error": "Userbot не запущен или не авторизован"}

    Path(CHARTS_DIR).mkdir(parents=True, exist_ok=True)
    added = 0
    repaired = 0
    scanned = 0

    def _f(v):
        try: return float(v) if v is not None else None
        except Exception: return None

    # Собираем все сообщения в хронологическом порядке
    messages = []
    async for m in client.iter_messages(SOURCE_GROUP_ID, limit=limit):
        messages.append(m)
    messages.reverse()
    scanned = len(messages)

    db = Session()
    try:
        for i, msg in enumerate(messages):
            if not msg.raw_text:
                continue
            parsed = parse_signal(msg.raw_text)
            if not (parsed.get("trend") and parsed.get("entry")
                    and parsed.get("tp1") and parsed.get("sl")):
                continue

            existing = db.query(Signal).filter(Signal.message_id == msg.id).first()
            if existing is None:
                s = Signal(
                    message_id=msg.id,
                    raw_text=msg.raw_text,
                    source_group_id=str(SOURCE_GROUP_ID),
                    text_pair=parsed.get("pair"),
                    text_direction=parsed.get("direction"),
                    text_entry=parsed.get("entry"),
                    text_sl=parsed.get("sl"),
                    text_tp1=parsed.get("tp1"),
                    pair=parsed.get("pair"),
                    direction=parsed.get("direction"),
                    entry=parsed.get("entry"),
                    sl=parsed.get("sl"),
                    tp1=parsed.get("tp1"),
                    timeframe=parsed.get("timeframe"),
                    risk_reward=parsed.get("risk_reward"),
                    risk_percent=parsed.get("risk_percent"),
                    amount=parsed.get("amount"),
                    tp_percent=parsed.get("tp_percent"),
                    sl_percent=parsed.get("sl_percent"),
                    trend=parsed.get("trend"),
                    comment=parsed.get("comment"),
                    setup_number=parsed.get("setup_number"),
                    status="СЛЕЖУ",
                    dca4_triggered=True,  # backfilled → не триггерить алерты
                    pattern_triggered=True,
                    is_forwarded=True,
                    source=bot,
                    received_at=msg.date.replace(tzinfo=None) if msg.date.tzinfo else msg.date,
                )
                db.add(s)
                db.commit()
                db.refresh(s)
                existing = s
                added += 1
                log_event(s.id, "synced", data={"pair": s.pair}, message="Добавлен через /sync")

            # Если у сигнала нет графика — ищем следующее фото
            if not existing.has_chart:
                # Смотрим 3 следующих сообщения
                for j in range(i + 1, min(i + 4, len(messages))):
                    nm = messages[j]
                    is_photo = isinstance(nm.media, MessageMediaPhoto)
                    is_doc = (
                        isinstance(nm.media, MessageMediaDocument)
                        and nm.media.document.mime_type.startswith("image/")
                    ) if nm.media else False
                    if not (is_photo or is_doc):
                        continue
                    chart_path = os.path.join(CHARTS_DIR, f"{existing.id}_{nm.id}.jpg")
                    try:
                        await client.download_media(nm, file=chart_path)
                    except Exception:
                        break
                    existing.has_chart = True
                    existing.chart_message_id = nm.id
                    existing.chart_path = chart_path
                    db.commit()
                    repaired += 1

                    # AI
                    try:
                        cd = await analyze_chart(chart_path)
                        existing.chart_analyzed = True
                        existing.chart_ai_raw = cd.get("_raw", "")
                        existing.chart_pair = cd.get("pair")
                        existing.chart_direction = cd.get("direction")
                        existing.chart_entry = _f(cd.get("entry"))
                        existing.chart_sl = _f(cd.get("sl"))
                        existing.chart_tp1 = _f(cd.get("tp1"))
                        existing.chart_notes = cd.get("notes") or cd.get("pattern", "")
                        existing.dca1 = _f(cd.get("dca1"))
                        existing.dca2 = _f(cd.get("dca2"))
                        existing.dca3 = _f(cd.get("dca3"))
                        existing.dca4 = _f(cd.get("dca4"))
                        if existing.is_filtered:
                            existing.is_filtered = False
                            existing.filter_reason = None
                        db.commit()

                        q = await analyze_signal_quality(chart_path, {
                            "pair": existing.pair, "direction": existing.direction,
                            "timeframe": existing.timeframe, "entry": existing.entry,
                            "sl": existing.sl, "tp1": existing.tp1, "dca4": existing.dca4,
                            "risk_reward": existing.risk_reward, "trend": existing.trend,
                        })
                        if q and "score" in q:
                            existing.ai_score = q["score"]
                            existing.ai_confidence = q.get("confidence")
                            existing.ai_reasoning = q.get("reasoning")
                            existing.ai_risks = q.get("risks") or []
                            existing.ai_verdict = q.get("verdict")
                            db.commit()
                    except Exception:
                        pass
                    break
    finally:
        db.close()

    return {
        "ok": True,
        "scanned": scanned,
        "added": added,
        "repaired_charts": repaired,
    }


@app.get("/api/signal/{signal_id}")
async def api_signal_one(signal_id: int, db: Session = Depends(get_db)):
    return await asyncio.to_thread(_signal_one_sync, signal_id, db)


def _signal_one_sync(signal_id, db):
    s = db.query(Signal).filter(Signal.id == signal_id).first()
    if not s:
        raise HTTPException(status_code=404)
    return {
        "id": s.id, "source": s.source, "pair": s.pair, "direction": s.direction,
        "entry": s.entry, "sl": s.sl, "tp1": s.tp1,
        "timeframe": s.timeframe, "risk_reward": s.risk_reward,
        "risk_percent": s.risk_percent, "amount": s.amount,
        "tp_percent": s.tp_percent, "sl_percent": s.sl_percent,
        "trend": s.trend, "comment": s.comment,
        "setup_number": s.setup_number, "status": s.status,
        "has_chart": s.has_chart, "chart_notes": s.chart_notes,
        "raw_text": s.raw_text,
        "dca1": s.dca1, "dca2": s.dca2, "dca3": s.dca3, "dca4": s.dca4,
        "dca4_triggered": s.dca4_triggered,
        "pattern_triggered": s.pattern_triggered,
        "pattern_triggered_at": s.pattern_triggered_at.isoformat() if s.pattern_triggered_at and hasattr(s.pattern_triggered_at, 'isoformat') else s.pattern_triggered_at,
        "pattern_name": s.pattern_name,
        "pattern_price": s.pattern_price,
        "ai_score": s.ai_score,
        "ai_confidence": s.ai_confidence,
        "ai_reasoning": s.ai_reasoning,
        "ai_risks": s.ai_risks,
        "ai_verdict": s.ai_verdict,
        "received_at": s.received_at.isoformat() if s.received_at and hasattr(s.received_at, 'isoformat') else s.received_at,
    }


@app.get("/api/signals")
async def api_signals(db: Session = Depends(get_db), limit: int = 50):
    signals = db.query(Signal).order_by(desc(Signal.received_at)).limit(limit).all()
    return [
        {
            "id": s.id,
            "pair": s.pair,
            "direction": s.direction,
            "entry": s.entry,
            "sl": s.sl,
            "tp1": s.tp1,
            "tp2": s.tp2,
            "tp3": s.tp3,
            "has_chart": s.has_chart,
            "chart_analyzed": s.chart_analyzed,
            "chart_pair": s.chart_pair,
            "chart_direction": s.chart_direction,
            "is_forwarded": s.is_forwarded,
            "received_at": s.received_at.isoformat() if s.received_at and hasattr(s.received_at, 'isoformat') else s.received_at,
        }
        for s in signals
    ]
