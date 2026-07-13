import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from typing import Set

from fastapi import FastAPI, Depends, Form, HTTPException, Request, WebSocket, WebSocketDisconnect, status
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse, Response, JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.gzip import GZipMiddleware

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
            print("[LIFESPAN] Запускаю watcher/bots...", flush=True)
            from config import BOT_TOKEN, ADMIN_CHAT_ID, BOT4_BOT_TOKEN
            from database import init_db
            init_db()

            from bot import bot, start_bot
            from watcher import setup as setup_watcher, start_watcher, _bot as _wb

            bot2 = None  # BOT2 (Cryptovizor) удалён
            if _wb is None:
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

            # Auto-restart wrapper для watcher — если start_watcher падает
            # с exception, перезапускаем через 30с (раньше тихо умирал).
            async def _watcher_supervisor():
                attempt = 0
                while True:
                    attempt += 1
                    try:
                        from database import _get_db
                        from datetime import datetime, timezone
                        _get_db().system.update_one(
                            {"_id": "watcher_heartbeat"},
                            {"$set": {"stage": f"supervisor_attempt_{attempt}",
                                      "at": datetime.now(timezone.utc)}},
                            upsert=True,
                        )
                    except Exception:
                        pass
                    try:
                        await start_watcher()
                    except Exception as e:
                        logging.getLogger(__name__).error(
                            f"[watcher-supervisor] start_watcher crashed (attempt {attempt}): {e}",
                            exc_info=True,
                        )
                        try:
                            from database import _get_db
                            from datetime import datetime, timezone
                            _get_db().system.update_one(
                                {"_id": "watcher_heartbeat"},
                                {"$set": {"stage": f"crashed_attempt_{attempt}",
                                          "error": f"{type(e).__name__}: {str(e)[:200]}",
                                          "at": datetime.now(timezone.utc)}},
                                upsert=True,
                            )
                        except Exception:
                            pass
                    await asyncio.sleep(30)

            _bg_tasks.append(asyncio.create_task(start_bot()))
            _bg_tasks.append(asyncio.create_task(_watcher_supervisor()))
            # userbot / bot2 / cv_flip_watcher удалены вместе с CV/Tradium ingestion (2026-07-01)

            # [Phase 3 fix] Миграция chart'ов в GridFS — запускаем в фоне
            # с задержкой 5 минут (даём system'у полностью прогреться, threadpool
            # не должен быть забит миграцией). Старая логика блокировала main.py.
            try:
                async def _bg_migrate_charts():
                    await asyncio.sleep(300)  # 5 мин — system warmup first
                    try:
                        import main as _m
                        await asyncio.to_thread(_m._migrate_charts_to_gridfs)
                    except Exception as _ce:
                        logging.getLogger(__name__).warning(f"[bg-migrate-charts] {_ce}")
                _bg_tasks.append(asyncio.create_task(_bg_migrate_charts()))
            except Exception as _e:
                print(f"[LIFESPAN] bg migrate skipped: {_e}", flush=True)

            # userbot health watchdog удалён вместе с userbot (2026-07-01)

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

# Static files (lightweight-charts.js + future assets) — отдаются с того же
# сервера → HTTP/2 multiplexing + нет stranger TLS. CDN unpkg даёт ~0.9с,
# локально — 50-100мс.
try:
    from fastapi.staticfiles import StaticFiles
    _static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
    if os.path.isdir(_static_dir):
        app.mount("/static",
                  StaticFiles(directory=_static_dir, check_dir=False),
                  name="static")
        # Cache headers устанавливаются через middleware ниже
except Exception as _se:
    logging.getLogger(__name__).warning(f"[STATIC] mount failed: {_se}")


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


class StaticCacheMiddleware(BaseHTTPMiddleware):
    """Cache-Control headers для /static/ — браузер кеширует на 1 год.
    LWC bundle ~196KB, шрифты — immutable. После первого скачивания
    браузер использует disk cache, не делает запросов на сервер.
    Если обновляется JS — нужно переименовать файл (busting).
    """
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/static"):
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        return response


class SessionAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path in ("/login", "/health", "/healthz", "/api/setup-check", "/api/resonance-screenshot", "/api/resonance-status", "/api/backtest-st", "/api/backtest-st/status",
            "/api/supertrend-signals", "/api/supertrend-signals/by-pair",
            "/api/supertrend-stats", "/api/st-enrich",
            "/api/new-strategies", "/api/new-strategies/by-pair",
            "/api/supertrend/backfill", "/api/supertrend/backfill-status", "/api/bots-status",
            "/api/backtest-st-signals", "/api/backtest-st-signals/status", "/api/paper/started",
            "/api/paper/close", "/api/paper/mode", "/api/paper/learnings", "/api/paper/refresh-ai-memory",
            "/api/paper/ai-prompt", "/api/paper/set-balance", "/api/paper/ai-test",
            "/api/paper/test-open", "/api/live/debug-recent", "/api/paper/status",
            "/api/live/snapshot", "/api/live/history-all", "/api/live/rejections-all",
            "/api/binance-symbols", "/api/binance-symbols/refresh",
            "/api/exchange-symbols/all", "/api/exchange-symbols/set-default",
            "/api/admin/cleanup-tests", "/api/admin/wipe-live-trades",
            "/api/admin/recompute-paper-balance", "/api/admin/backfill-mirror",
            "/api/admin/close-all-exchange-positions",
            "/api/admin/normalize-live-symbols",
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
            "/api/live/confirm", "/api/fvg-monitor-debug", "/api/fvg-entry-alert-test", "/api/inspect-msg-neighbors", "/api/debug-fetch-chart", "/api/reversal-meter", "/api/pending-clusters", "/api/backfill-clusters", "/api/pair-signals", "/api/fvg-signals", "/api/fvg-journal", "/api/fvg-config", "/api/fvg-scan-now", "/api/fvg-candles", "/api/conflicts", "/api/conflicts/check", "/api/smart-levels", "/api/td-quota", "/api/ai-coin-analysis", "/api/top-picks", "/api/top-picks/backfill", "/api/claude-budget", "/api/tv-webhook", "/api/fvg-top-picks", "/api/fvg-rescore-all", "/api/admin/health-detail", "/api/journal/by-symbol", "/api/market-events", "/api/market-events/backfill", "/api/backtest/today") or path.startswith("/static") or path.startswith("/api/live/accounts"):
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
app.add_middleware(StaticCacheMiddleware)
# GZipMiddleware — компресс JSON >1KB (CV-flips/ST-signals 100KB → ~10KB).
# minimum_size=1024 чтобы не тратить CPU на маленькие /healthz/login ответы.
app.add_middleware(GZipMiddleware, minimum_size=1024)


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


_HEALTH_CACHE: dict = {"ts": 0.0, "data": None}
_HEALTH_TTL_SEC = 30.0


def _health_sync() -> dict:
    """Синхронная Mongo-часть healthcheck. Вызывается через asyncio.to_thread
    чтобы не блокировать event loop когда Atlas лагает."""
    from database import utcnow as _utcnow
    from pymongo import DESCENDING
    status = {"ok": True, "checks": {}, "ts": _utcnow().isoformat()}
    try:
        from database import _signals
        _signals().estimated_document_count()
        status["checks"]["db"] = "ok"
    except Exception as e:
        status["checks"]["db"] = f"fail: {str(e)[:100]}"
        status["ok"] = False
    try:
        from database import _signals, _anomalies, _confluence
        now = _utcnow()
        for name, col, date_field in [
            ("signals", _signals(), "received_at"),
            ("confluence", _confluence(), "detected_at"),
        ]:
            last = col.find_one({}, sort=[(date_field, DESCENDING)])
            if last and last.get(date_field):
                age_min = (now - last[date_field]).total_seconds() / 60
                status["checks"][f"{name}_last_age_min"] = int(age_min)
                if age_min > 240:
                    status["checks"][f"{name}_warning"] = "stale (>4h no new data)"
        # tradium/cryptovizor per-source breakdown удалён (2026-07-01)
    except Exception as e:
        status["checks"]["activity"] = f"fail: {str(e)[:80]}"
    return status


@app.get("/health")
async def health():
    """Healthcheck для uptime monitors — без авторизации.
    Mongo-вызовы вынесены в thread pool + кеш 30s, чтобы лаг Atlas
    не блокировал event loop и не клал весь сервис.
    Docker/Railway healthcheck использует /healthz (lightweight)."""
    import asyncio as _aio
    import time as _time
    now = _time.time()
    if _HEALTH_CACHE["data"] is not None and (now - _HEALTH_CACHE["ts"]) < _HEALTH_TTL_SEC:
        return _HEALTH_CACHE["data"]
    try:
        data = await _aio.wait_for(_aio.to_thread(_health_sync), timeout=8.0)
    except _aio.TimeoutError:
        data = {"ok": False, "checks": {"db": "timeout >8s (Atlas lag)"},
                "stale": True}
    except Exception as e:
        data = {"ok": False, "checks": {"err": str(e)[:120]}}
    _HEALTH_CACHE["ts"] = now
    _HEALTH_CACHE["data"] = data
    return data


_resonance_cache: dict = {}  # {(symbol, tf): (ts, png_bytes)}
_RESONANCE_TTL_S = 900  # 15 мин
_resonance_lock = asyncio.Lock()  # один Selenium процесс за раз


@app.get("/api/resonance-screenshot")
async def api_resonance_screenshot(symbol: str, tf: str = "H1", force: bool = False):
    """📊 Скриншот кластерного графика с Resonance.vision.

    Selenium-driven (headless Chromium на Railway). Cache 15 минут per
    (symbol, tf), чтобы не делать новый screenshot на каждый refresh
    графика в журнале.

    Returns: image/png Response или JSON error.
    """
    import time
    from fastapi.responses import Response
    sym = (symbol or "").upper().replace("/", "").replace("USDT", "").strip()
    if not sym:
        return {"ok": False, "error": "no symbol"}
    sym_full = sym + "USDT"
    tf_norm = (tf or "H1").strip()
    tf_map = {"15m": "M15", "30m": "M30", "1h": "H1", "4h": "H4", "12h": "H4", "1d": "H4"}
    tf_render = tf_map.get(tf_norm.lower(), tf_norm.upper() if tf_norm.upper() in ("M15","M30","H1","H4") else "H1")
    key = (sym_full, tf_render)
    now = time.time()
    if not force:
        cached = _resonance_cache.get(key)
        if cached and (now - cached[0]) < _RESONANCE_TTL_S:
            return Response(content=cached[1], media_type="image/png",
                            headers={"X-Cache": "HIT",
                                     "Cache-Control": "public, max-age=900"})

    # Selenium tight lock (один процесс зараз — у Railway 0.5-1GB)
    async with _resonance_lock:
        # Re-check cache after acquiring lock (другой request мог уже отрисовать)
        cached = _resonance_cache.get(key)
        if cached and (now - cached[0]) < _RESONANCE_TTL_S and not force:
            return Response(content=cached[1], media_type="image/png",
                            headers={"X-Cache": "HIT-LOCK"})
        try:
            from resonance_chart import get_cluster_screenshot
            png = await asyncio.wait_for(
                asyncio.to_thread(get_cluster_screenshot, sym_full, tf_render),
                timeout=90.0,
            )
        except asyncio.TimeoutError:
            return {"ok": False, "error": "screenshot timeout (>90s)"}
        except Exception as e:
            logging.getLogger(__name__).exception("[resonance] screenshot fail")
            return {"ok": False, "error": str(e)[:300]}

        if not png:
            return {"ok": False, "error": "screenshot returned empty (login/popup issue?)"}

        _resonance_cache[key] = (now, png)
        # Prune old cache (keep last 30)
        if len(_resonance_cache) > 30:
            sorted_keys = sorted(_resonance_cache.items(), key=lambda kv: kv[1][0])
            for k, _ in sorted_keys[:len(_resonance_cache) - 30]:
                _resonance_cache.pop(k, None)
        return Response(content=png, media_type="image/png",
                        headers={"X-Cache": "MISS",
                                 "Cache-Control": "public, max-age=900"})


@app.get("/api/resonance-status")
async def api_resonance_status():
    """Статус Resonance cache + login state."""
    import time
    now = time.time()
    items = []
    for (sym, tf), (ts, png) in _resonance_cache.items():
        items.append({
            "symbol": sym, "tf": tf,
            "size_bytes": len(png),
            "age_minutes": round((now - ts) / 60, 1),
        })
    try:
        from resonance_chart import _session_cookies
        logged_in = bool(_session_cookies)
    except Exception:
        logged_in = False
    try:
        from resonance_chart import _last_switch_debug
        switch_debug = dict(_last_switch_debug)
    except Exception:
        switch_debug = {}
    return {"logged_in": logged_in, "cached": items,
            "lock_held": _resonance_lock.locked(),
            "switch_debug": switch_debug}


def _cap_admin_cache(d: dict, max_size: int = 300) -> None:
    """Cap для admin кэшей. Большинство хранят (ts, value) tuple — удаляем
    самые старые. Запускается inline после каждого write — защита от
    неограниченного роста между _cache_cleanup_loop циклами (5 мин)."""
    if len(d) <= max_size:
        return
    try:
        items = []
        for k, v in d.items():
            ts = 0
            if isinstance(v, tuple) and v:
                if isinstance(v[0], (int, float)) and v[0] > 10**9:
                    ts = v[0]
            items.append((k, ts))
        items.sort(key=lambda kv: kv[1])
        for k, _ in items[:len(d) - max_size]:
            d.pop(k, None)
    except Exception:
        pass









_KL_STATS_TTL = 60.0




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
    Фильтры: tier (vip/mtf/daily), pair, hours (окно от текущего времени).

    Cache 30s + sync find в to_thread. Раньше был sync Mongo прямо в
    event loop + no cache — каждый poll давал 1-3s блокировки.
    """
    from cache_utils import supertrend_signals_cache

    async def _compute():
        def _sync():
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
                for ab in doc.get("aligned_bots", []):
                    v = ab.get("at")
                    if hasattr(v, "isoformat"):
                        ab["at"] = v.isoformat()
                items.append(doc)
            return items
        items = await asyncio.to_thread(_sync)
        return {"ok": True, "count": len(items), "items": items,
                "tier": tier, "pair": pair, "hours": hours}

    cache_key = f"st|{tier}|{pair}|{hours}|{limit}"
    return await supertrend_signals_cache.get_or_compute(cache_key, _compute)




_new_strat_cache: dict = {"ts": 0.0, "data": None}
_NEW_STRAT_TTL = 30.0


@app.get("/api/new-strategies")
async def api_new_strategies(strategy: str = "all", state: str = "all",
                             pair: str = "", hours: int = 168, limit: int = 500):
    """5 backtest-validated стратегий после ST flip.

    strategy ∈ {all, volume_surge, triple_confluence, vol_accum, volcano, second_flip}
    state ∈ {all, WAITING, TP, SL, MANUAL, TIMEOUT}
    hours — окно от created_at (default 7d)
    """
    import time as _t
    cache_key = f"ns|{strategy}|{state}|{pair}|{hours}|{limit}"
    now = _t.time()
    cached = _new_strat_cache.get(cache_key)
    if cached and (now - cached[0]) < _NEW_STRAT_TTL:
        return cached[1]

    def _sync():
        from database import _get_db, utcnow
        from datetime import timedelta
        col = _get_db().new_strategy_signals
        since = utcnow() - timedelta(hours=hours)
        query: dict = {"created_at": {"$gte": since}}
        if strategy and strategy != "all":
            query["strategy"] = strategy
        if state and state.upper() != "ALL":
            query["state"] = state.upper()
        if pair:
            p = pair.replace("/", "").upper()
            if p.endswith("USDT") and "/" not in pair:
                base = p[:-4]
                query["$or"] = [{"pair": pair}, {"pair": f"{base}/USDT"}]
            else:
                query["pair"] = pair
        items = []
        for doc in col.find(query).sort("created_at", -1).limit(limit):
            doc["_id"] = str(doc.get("_id"))
            for k in ("created_at", "updated_at", "st_flip_at"):
                v = doc.get(k)
                if hasattr(v, "isoformat"):
                    doc[k] = v.isoformat()
            items.append(doc)
        # Stats per strategy
        from collections import Counter
        by_strat = Counter()
        by_state = Counter()
        for it in items:
            by_strat[it.get("strategy")] += 1
            by_state[it.get("state", "WAITING")] += 1
        return {
            "ok": True, "count": len(items), "items": items,
            "strategy": strategy, "state": state, "pair": pair, "hours": hours,
            "stats": {
                "by_strategy": dict(by_strat),
                "by_state": dict(by_state),
            },
        }

    data = await asyncio.to_thread(_sync)
    _new_strat_cache[cache_key] = (now, data)
    return data


@app.get("/api/new-strategies/by-pair")
async def api_new_strategies_by_pair(pair: str, hours: int = 72, limit: int = 50):
    """Per-pair endpoint для маркеров на графиках.

    PERF (07.05.2026): hours 336→72, limit 200→50 default. После добавления
    5-й стратегии (♻️ Second Flip) количество записей на пару выросло —
    графики начали тормозить. Снижение лимитов вернуло плавность."""
    return await api_new_strategies(strategy="all", state="all",
                                    pair=pair, hours=hours, limit=limit)


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
    _cap_admin_cache(_st_by_pair_cache, 300)
    if len(_st_by_pair_cache) > 300:
        for k in [k for k, v in _st_by_pair_cache.items() if (now - v[0]) > _ST_BY_PAIR_TTL * 2]:
            _st_by_pair_cache.pop(k, None)
    return resp


@app.get("/api/supertrend-stats")
async def api_supertrend_stats(days: int = 14):
    """Агрегированная статистика по tiers (count + распределение по LONG/SHORT).

    Cache 120s + sync aggregate в to_thread. Раньше aggregate шёл в
    event loop, при медленном Atlas блокировал ~500ms каждый запрос.
    """
    from cache_utils import supertrend_stats_cache

    async def _compute():
        def _sync():
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
        return await asyncio.to_thread(_sync)

    return await supertrend_stats_cache.get_or_compute(f"st_stats_{days}", _compute)


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
    """Ручное закрытие paper-позиции + INSTANT mirror close на live (если есть).
    payload: {"trade_id": 123}"""
    import paper_trader as pt
    trade_id = (payload or {}).get("trade_id")
    if not trade_id:
        return {"ok": False, "error": "trade_id required"}
    try:
        result = await pt.close_manual(int(trade_id))
        if not result:
            return {"ok": False, "error": "position not found or already closed"}
        # ⚡ INSTANT mirror close на live (вместо 15с ожидания фонового loop)
        try:
            import live_trader as lt
            mirror_res = await asyncio.wait_for(lt.paper_to_live_sync_check(), timeout=20.0)
            return {"ok": True, "result": result,
                    "mirror_synced": mirror_res.get("synced", 0) if isinstance(mirror_res, dict) else 0}
        except Exception as me:
            logging.getLogger(__name__).warning(f"[paper-close] instant mirror fail: {me}")
            return {"ok": True, "result": result, "mirror_warning": str(me)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/paper/close-all")
async def api_paper_close_all():
    """Закрывает ВСЕ paper-позиции по рынку + INSTANT mirror close всех live-позиций."""
    import paper_trader as pt
    try:
        result = await pt.close_all_manual()
        # ⚡ INSTANT mirror close всех live позиций (paper всё закрыл → live тоже должен)
        try:
            import live_trader as lt
            mirror_res = await asyncio.wait_for(lt.paper_to_live_sync_check(), timeout=30.0)
            result["mirror_synced"] = mirror_res.get("synced", 0) if isinstance(mirror_res, dict) else 0
        except Exception as me:
            logging.getLogger(__name__).warning(f"[paper-close-all] instant mirror fail: {me}")
            result["mirror_warning"] = str(me)
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


# ════════════════════════════════════════════════════════════════
# Multi-account API (несколько Binance ключей одновременно — для семьи)
# ════════════════════════════════════════════════════════════════

@app.get("/api/live/accounts")
async def api_live_accounts_list():
    """Список всех аккаунтов (без api_secret). С балансом и кол-вом позиций."""
    import live_safety as ls
    try:
        def _sync_load():
            from database import _live_trades
            accs = ls.list_accounts()
            for a in accs:
                a["open_positions"] = _live_trades().count_documents({
                    "account_id": a["_id"], "status": "OPEN",
                })
                for f in ("created_at", "updated_at", "last_trade_at", "kill_at", "daily_reset_at"):
                    if a.get(f) and hasattr(a[f], "isoformat"):
                        a[f] = a[f].isoformat()
                preset_cfg = ls.SAFETY_PRESETS.get(a.get("safety_preset", "paper_mirror"))
                if preset_cfg:
                    a["preset_config"] = preset_cfg
            return accs
        accounts = await asyncio.to_thread(_sync_load)
        return {"ok": True, "count": len(accounts), "accounts": accounts}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/live/accounts")
async def api_live_accounts_add(payload: dict):
    """Создать новый аккаунт.
    payload: {id, owner, label, mode: testnet|real, api_key, api_secret,
              safety_preset, confirmation_required}"""
    import live_safety as ls
    try:
        result = await asyncio.to_thread(ls.add_account, payload or {})
        return result
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.put("/api/live/accounts/{account_id}")
async def api_live_accounts_update(account_id: str, payload: dict):
    """Обновить поля аккаунта."""
    import live_safety as ls
    try:
        return await asyncio.to_thread(ls.update_account, account_id, payload or {})
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.delete("/api/live/accounts/{account_id}")
async def api_live_accounts_delete(account_id: str):
    """Удалить аккаунт (только если нет открытых позиций)."""
    import live_safety as ls
    try:
        return await asyncio.to_thread(ls.delete_account, account_id)
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/live/accounts/{account_id}/test-connection")
async def api_live_accounts_test(account_id: str):
    """Проверка соединения для конкретного аккаунта (использует ключи из БД).
    Возвращает реальный баланс с биржи."""
    import live_safety as ls
    import live_trader as lt
    try:
        account = ls.get_account(account_id)
        if not account:
            return {"ok": False, "error": f"account '{account_id}' не найден"}
        result = await asyncio.to_thread(lt.test_connection_for_account, account)
        # Если соединение OK — обновим балaнс в БД
        if result.get("ok") and "usdt_total" in result:
            await asyncio.to_thread(
                ls.update_account, account_id,
                {"balance": result["usdt_total"]},
            )
        return result
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/live/accounts/{account_id}/enable")
async def api_live_accounts_enable(account_id: str, payload: dict | None = None):
    """Включить/выключить аккаунт. payload: {enabled: bool}."""
    import live_safety as ls
    enabled = (payload or {}).get("enabled", True)
    try:
        return await asyncio.to_thread(
            ls.update_account, account_id, {"enabled": bool(enabled)},
        )
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/live/accounts/{account_id}/kill")
async def api_live_accounts_kill(account_id: str, payload: dict | None = None):
    """Активировать/сбросить kill switch на конкретном аккаунте.
    payload: {kill: true|false}."""
    import live_safety as ls
    kill = (payload or {}).get("kill", True)
    try:
        return await asyncio.to_thread(
            ls.update_account, account_id,
            {"kill_switch": bool(kill), "kill_reason": "manual" if kill else None},
        )
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/live/accounts/{account_id}/balance")
async def api_live_accounts_set_balance(account_id: str, payload: dict):
    """Установить виртуальный балaнс (для расчёта size_pct, не реальный margin).
    Полезно когда хочешь зеркалить paper-balance несмотря на реальный margin
    на бирже. payload: {balance: 2419.61}."""
    import live_safety as ls
    bal = (payload or {}).get("balance")
    try:
        bal = float(bal)
    except Exception:
        return {"ok": False, "error": "balance должен быть число"}
    if bal < 0:
        return {"ok": False, "error": "balance должен быть >= 0"}
    try:
        return await asyncio.to_thread(
            ls.update_account, account_id, {"balance": bal},
        )
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/paper/rejections")
async def api_paper_rejections(limit: int = 200):
    """Последние отказы AI от сделок — для UI лога «почему не вошёл».
    Кеш 30с — аккордеон polling'ит этот endpoint.
    Default 200 (раньше 50) — чтобы видеть полную картину F0/CAUTION/SKIP."""
    import paper_trader as pt
    from cache_utils import paper_rejections_cache

    async def _compute():
        return await asyncio.to_thread(pt.get_rejections, limit)

    items = await paper_rejections_cache.get_or_compute(f"limit_{limit}", _compute)
    return {"ok": True, "count": len(items), "items": items}


@app.post("/api/admin/backfill-mirror")
async def api_admin_backfill_mirror(payload: dict | None = None):
    """Прогнать mirror на live-аккаунты для уже открытых paper-позиций
    у которых нет соответствующего live-трейда. Используется когда mirror
    провалился по сетевой ошибке или неактуальной логике.
    """
    pl = payload or {}
    account_id_filter = pl.get("account_id")  # optional, иначе все enabled

    import paper_trader as pt
    import live_safety as ls
    import live_trader as lt
    from database import _live_trades

    accounts = ls.get_enabled_accounts()
    if account_id_filter:
        accounts = [a for a in accounts if a.get("_id") == account_id_filter]
    if not accounts:
        return {"ok": False, "error": "no enabled accounts"}

    paper_open = pt.get_open_positions()
    results = []
    for acc in accounts:
        aid = acc.get("_id")
        for p in paper_open:
            ptid = p.get("trade_id")
            existing = _live_trades().find_one({
                "account_id": aid,
                "$or": [
                    {"paper_trade_id": ptid},
                    {"symbol": p.get("symbol"), "direction": p.get("direction")},
                ],
            })
            if existing:
                results.append({"account": aid, "paper_id": ptid,
                                "symbol": p.get("symbol"),
                                "skipped": f"existing trade #{existing.get('trade_id')} status={existing.get('status')}"})
                continue
            sig = {
                "symbol": p.get("symbol"),
                "pair": p.get("pair") or p.get("symbol", "").replace("USDT", "/USDT"),
                "direction": p.get("direction"),
                "entry": p.get("entry"),
                "source": p.get("source", "backfill"),
                "paper_trade_id": ptid,
            }
            decision = {
                "enter": True,
                "leverage": p.get("leverage", 5),
                "size_pct": p.get("size_pct", 3),
                "tp1": p.get("tp1"),
                "sl": p.get("sl"),
                "reasoning": p.get("ai_reasoning", "") + " [BACKFILL]",
            }
            try:
                r = await lt.mirror_paper_for_account(sig, decision, acc)
                results.append({
                    "account": aid, "paper_id": ptid,
                    "symbol": p.get("symbol"), "direction": p.get("direction"),
                    "result": {"ok": r.get("ok") if r else None,
                               "error": r.get("error") if r else None,
                               "live_trade_id": (r.get("trade") or {}).get("trade_id") if r and r.get("ok") else None},
                })
            except Exception as e:
                results.append({"account": aid, "paper_id": ptid,
                                "symbol": p.get("symbol"), "error": str(e)[:300]})
    return {"ok": True, "results": results}


@app.post("/api/admin/recompute-paper-balance")
async def api_admin_recompute_paper_balance():
    """Пересчитать paper balance из истории:
    balance = INITIAL_BALANCE + сумма всех закрытых pnl_usdt + realized из partial.
    """
    from database import _get_db, utcnow
    import paper_trader as pt
    db = _get_db()
    initial = pt.INITIAL_BALANCE

    # Закрытые сделки: pnl_usdt уже = realized_pnl + final_leg (см. paper_trader L401)
    # → не надо складывать realized_pnl, иначе double counting.
    closed_pnl = 0.0
    closed_count = 0
    for d in db.paper_trades.find(
        {"status": {"$nin": ["OPEN"]}, "pnl_usdt": {"$type": "double"}},
        {"pnl_usdt": 1}
    ):
        try:
            v = d.get("pnl_usdt")
            if v is None:
                continue
            closed_pnl += float(v)
            closed_count += 1
        except Exception:
            pass

    # Открытые позиции с partials (TP1/TP2 hits) — частично уже фиксировано
    open_partial = 0.0
    for d in db.paper_trades.find(
        {"status": "OPEN"},
        {"realized_pnl_usdt": 1}
    ):
        try:
            open_partial += float(d.get("realized_pnl_usdt") or 0)
        except Exception:
            pass

    new_balance = round(initial + closed_pnl + open_partial, 2)
    db.paper_trades.update_one(
        {"_id": "state"},
        {"$set": {"balance": new_balance, "updated_at": utcnow()}},
        upsert=True,
    )
    return {
        "ok": True,
        "initial": initial,
        "closed_trades_count": closed_count,
        "closed_pnl_total": round(closed_pnl, 2),
        "open_partial_pnl": round(open_partial, 2),
        "new_balance": new_balance,
    }


@app.post("/api/admin/normalize-live-symbols")
async def api_admin_normalize_live_symbols():
    """Привести live_trades.symbol к XXXUSDT формату (как в paper).
    Раньше trades могли записываться в ccxt-формате XXX/USDT:USDT,
    что ломало cross-reference."""
    from database import _live_trades
    fixed = 0
    for t in _live_trades().find({"symbol": {"$regex": "/USDT"}}):
        sym = t.get("symbol", "")
        # ETH/USDT:USDT → ETHUSDT
        new_sym = sym.split(":")[0].replace("/", "")
        if new_sym != sym:
            _live_trades().update_one(
                {"_id": t["_id"]},
                {"$set": {
                    "symbol": new_sym,
                    "pair": new_sym.replace("USDT", "/USDT"),
                    "ccxt_symbol": sym,
                }}
            )
            fixed += 1
    return {"ok": True, "fixed_count": fixed}


@app.post("/api/admin/close-all-exchange-positions")
async def api_admin_close_all_exchange_positions(payload: dict | None = None):
    """Закрывает ВСЕ открытые позиции на бирже для аккаунта (через ccxt).
    Освобождает заблокированную маржу. Работает напрямую через биржу — не
    зависит от того что хранится в MongoDB.

    payload: {"account_id":"super_testnet"}
    """
    pl = payload or {}
    aid = pl.get("account_id")
    import live_trader as lt
    import live_safety as ls
    if not aid:
        # Все enabled аккаунты
        all_results = []
        for acc in ls.get_enabled_accounts():
            r = await api_admin_close_all_exchange_positions({"account_id": acc.get("_id")})
            all_results.append(r)
        return {
            "ok": True,
            "closed_count": sum((r.get("closed_count") or 0) for r in all_results),
            "failed_count": sum((r.get("failed_count") or 0) for r in all_results),
            "by_account": all_results,
        }
    acc = ls.get_account(aid)
    if not acc:
        return {"ok": False, "error": f"account {aid} not found"}
    ex = lt._get_exchange_for_account(acc)
    if ex is None:
        return {"ok": False, "error": "exchange not configured"}

    closed = []
    failed = []
    try:
        positions = await asyncio.to_thread(ex.fetch_positions)
    except Exception as e:
        return {"ok": False, "error": f"fetch_positions fail: {e}"}

    # Определяем биржу для правильных params (BingX hedge mode != reduceOnly)
    ex_name = (acc.get("exchange") or "").lower()
    for p in positions:
        contracts = float(p.get("contracts", 0) or 0)
        if contracts == 0:
            continue
        sym = p.get("symbol")
        side = p.get("side")  # "long" or "short"
        # Закрываем через market: для long → sell; для short → buy
        close_side = "sell" if side == "long" else "buy"
        # BingX hedge mode: positionSide ОБЯЗАТЕЛЬНО, reduceOnly запрещён
        if ex_name == "bingx":
            close_params = {"positionSide": "LONG" if side == "long" else "SHORT"}
        else:
            close_params = {"reduceOnly": True}
        try:
            try:
                amt = float(await asyncio.to_thread(ex.amount_to_precision, sym, contracts))
            except Exception:
                amt = contracts
            order = await asyncio.to_thread(
                ex.create_market_order, sym, close_side, amt,
                None, close_params,
            )
            closed.append({"symbol": sym, "side": side, "contracts": contracts,
                           "exit_price": order.get("average"), "order_id": order.get("id")})
        except Exception as e:
            failed.append({"symbol": sym, "side": side, "error": str(e)[:300]})

    # Также пометим всё OPEN в MongoDB как MANUAL closed
    from database import _live_trades, utcnow
    db_open = list(_live_trades().find({"status": "OPEN", "account_id": aid}))
    for pos in db_open:
        _live_trades().update_one(
            {"trade_id": pos["trade_id"]},
            {"$set": {"status": "MANUAL", "closed_at": utcnow(),
                      "exit_price": pos.get("entry"), "pnl_pct": 0,
                      "pnl_usdt": 0, "manual_close_reason": "wipe-exchange-positions"}},
        )
    return {"ok": True, "closed_count": len(closed), "failed_count": len(failed),
            "closed": closed[:20], "failed": failed[:20], "db_marked_manual": len(db_open)}


@app.post("/api/admin/wipe-live-trades")
async def api_admin_wipe_live_trades(payload: dict | None = None):
    """Удалить ВСЕ live_trades для указанного account_id. Используется для
    чистки после тестов когда трейды зависли в неправильном статусе.
    payload: {"account_id":"super_testnet"}"""
    pl = payload or {}
    aid = pl.get("account_id") or "super_testnet"
    from database import _live_trades, _live_accounts, utcnow
    res = _live_trades().delete_many({"account_id": aid})
    # Reset last_trade_at и kill_switch
    _live_accounts().update_one(
        {"_id": aid},
        {"$set": {"last_trade_at": None, "kill_switch": False,
                  "kill_reason": None, "updated_at": utcnow()}},
    )
    return {"ok": True, "deleted_live_trades": res.deleted_count, "account_id": aid}


@app.post("/api/admin/cleanup-tests")
async def api_admin_cleanup_tests(payload: dict | None = None):
    """Удалить все тестовые трейды (paper + live) и восстановить paper баланс.

    payload:
      - sources: список префиксов source для удаления
        (default: ["manual_test", "test_v", "final_test"])
      - new_balance: новый paper баланс (default: 1000.0 — INITIAL_BALANCE)
    """
    pl = payload or {}
    sources = pl.get("sources") or ["manual_test", "test_v", "final_test"]
    new_balance = float(pl.get("new_balance") or 1000.0)
    from database import _get_db, _live_trades, utcnow
    import re as _re
    db = _get_db()

    # Собираем уникальные source через aggregation (надёжнее чем distinct)
    pipeline = [{"$group": {"_id": "$source"}}]
    all_paper_sources = [d["_id"] for d in db.paper_trades.aggregate(pipeline) if d.get("_id")]
    matching_sources = [
        s for s in all_paper_sources
        if isinstance(s, str) and any(s.startswith(p) for p in sources)
    ]
    paper_filter = {"source": {"$in": matching_sources}} if matching_sources else {"source": "__none__"}

    # Найти и удалить paper trades
    paper_to_delete = list(db.paper_trades.find(paper_filter, {"trade_id":1,"source":1,"status":1}))
    paper_trade_ids = [p["trade_id"] for p in paper_to_delete if p.get("trade_id")]
    paper_del_result = db.paper_trades.delete_many(paper_filter)

    # Удалить соответствующие live_trades по paper_trade_id
    live_filter = {"paper_trade_id": {"$in": paper_trade_ids}}
    live_del_result = _live_trades().delete_many(live_filter) if paper_trade_ids else type("R",(object,),{"deleted_count":0})()

    # Также удалить любые live_trades с source совпадающим (на случай если paper_trade_id null)
    all_live_sources = [d["_id"] for d in _live_trades().aggregate(pipeline) if d.get("_id")]
    matching_live_sources = [
        s for s in all_live_sources
        if isinstance(s, str) and any(s.startswith(p) for p in sources)
    ]
    if matching_live_sources:
        live_extra_del = _live_trades().delete_many({"source": {"$in": matching_live_sources}})
        live_extra_count = live_extra_del.deleted_count
    else:
        live_extra_count = 0

    # Восстановить paper баланс
    db.paper_trades.update_one(
        {"_id": "state"},
        {"$set": {"balance": new_balance, "updated_at": utcnow()}},
        upsert=True,
    )

    return {
        "ok": True,
        "all_paper_sources_in_db": all_paper_sources[:30],
        "all_live_sources_in_db": all_live_sources[:30],
        "matched_paper_sources": matching_sources,
        "matched_live_sources": matching_live_sources,
        "deleted_paper": paper_del_result.deleted_count,
        "deleted_paper_ids": paper_trade_ids,
        "deleted_live_by_paper_id": live_del_result.deleted_count,
        "deleted_live_by_source": live_extra_count,
        "new_balance": new_balance,
        "sources_filter": sources,
    }


@app.get("/api/binance-symbols")
async def api_binance_symbols(only_meta: bool = False, exchange: str = ""):
    """Список USDT-perp символов биржи + метаинфо.
    exchange='binance' | 'bingx' (default — берём default из настроек).
    only_meta=true — без массива символов."""
    from exchange_symbols import get_supported_symbols, get_meta, get_default_exchange
    ex_name = exchange or get_default_exchange()
    meta = get_meta(ex_name)
    if only_meta:
        return {"ok": True, **meta}
    syms = sorted(list(get_supported_symbols(ex_name)))
    return {"ok": True, "symbols": syms, **meta}


@app.post("/api/binance-symbols/refresh")
async def api_binance_symbols_refresh(payload: dict | None = None):
    """Принудительный refresh из exchange API.
    payload: {"exchange": "binance" | "bingx"} — иначе default."""
    from exchange_symbols import refresh_supported_symbols, get_default_exchange
    ex_name = (payload or {}).get("exchange") or get_default_exchange()
    return await asyncio.to_thread(refresh_supported_symbols, ex_name)


@app.get("/api/exchange-symbols/all")
async def api_exchange_symbols_all():
    """Метаинфо по обеим биржам сразу — для UI 2-колоночного отображения."""
    from exchange_symbols import get_meta_all
    return {"ok": True, **get_meta_all()}


@app.post("/api/exchange-symbols/set-default")
async def api_exchange_symbols_set_default(payload: dict):
    """Сменить default exchange (для paper фильтра).
    payload: {"exchange": "bingx" | "binance"}"""
    from exchange_symbols import set_default_exchange
    ex_name = (payload or {}).get("exchange", "")
    return set_default_exchange(ex_name)


def _live_snapshot_sync():
    """Sync Mongo-сборка для /api/live/snapshot. Вызывается через to_thread."""
    from database import _live_trades, _live_accounts
    accounts = list(_live_accounts().find({}))
    open_trades = list(_live_trades().find({"status": "OPEN"}))
    closed_filter = {"status": {"$in": ["TP", "SL", "BE", "TRAIL", "MANUAL", "AI_CLOSE"]}}
    closed_count = _live_trades().count_documents(closed_filter)
    failed_count = _live_trades().count_documents({"status": "FAILED_OPEN"})
    total_pnl = 0.0
    wins = 0
    losses = 0
    for d in _live_trades().find(closed_filter, {"pnl_usdt": 1}):
        v = float(d.get("pnl_usdt") or 0)
        total_pnl += v
        if v > 0: wins += 1
        elif v < 0: losses += 1
    return {
        "accounts": accounts, "open_trades": open_trades,
        "closed_count": closed_count, "failed_count": failed_count,
        "total_pnl": total_pnl, "wins": wins, "losses": losses,
    }


_LIVE_SNAPSHOT_CACHE: dict = {"ts": 0.0, "data": None}
_LIVE_SNAPSHOT_TTL = 8.0  # секунды; UI polling каждые 15с — компромисс свежесть/нагрузка


@app.get("/api/live/snapshot")
async def api_live_snapshot():
    """Агрегат всех enabled live аккаунтов: суммарный капитал, позиции, статы.
    Кеш 8с — снижает нагрузку на Atlas при частом polling UI."""
    import time as _t
    now_ts = _t.time()
    cached = _LIVE_SNAPSHOT_CACHE.get("data")
    if cached is not None and (now_ts - _LIVE_SNAPSHOT_CACHE["ts"]) < _LIVE_SNAPSHOT_TTL:
        return cached

    import live_safety as ls
    from exchange import get_prices_any

    raw = await asyncio.to_thread(_live_snapshot_sync)
    accounts = raw["accounts"]
    open_trades = raw["open_trades"]
    closed_count = raw["closed_count"]
    failed_count = raw["failed_count"]
    total_pnl = raw["total_pnl"]
    wins = raw["wins"]
    losses = raw["losses"]
    enabled = [a for a in accounts if a.get("enabled")]

    total_balance = 0.0
    accounts_status = []
    for a in accounts:
        aid = a.get("_id")
        bal = float(a.get("balance") or 0.0)
        total_balance += bal
        accounts_status.append({
            "id": aid,
            "label": a.get("label") or aid,
            "owner": a.get("owner"),
            "mode": a.get("mode"),
            "enabled": a.get("enabled"),
            "kill_switch": a.get("kill_switch"),
            "balance": bal,
        })

    if open_trades:
        pairs = list({(p.get("symbol", "") or "").replace("USDT", "/USDT") for p in open_trades if p.get("symbol")})
        try:
            prices = await asyncio.to_thread(get_prices_any, pairs) or {}
        except Exception:
            prices = {}
        for p in open_trades:
            cur = prices.get(p.get("symbol", ""))
            if cur and p.get("entry"):
                raw = ((cur - p["entry"]) / p["entry"]) * 100
                pnl = -raw if p.get("direction") == "SHORT" else raw
                p["live_pnl"] = round(pnl * (p.get("leverage", 1) or 1), 2)
                p["live_price"] = cur
            p["_id"] = str(p.get("_id", ""))
            for k in ("opened_at", "closed_at"):
                if p.get(k) and hasattr(p[k], "isoformat"):
                    p[k] = p[k].isoformat()

    win_rate = round(wins / (wins + losses) * 100, 1) if (wins + losses) else 0

    # PnL% = насколько отличается текущий баланс от стартового депозита.
    # Используем cumulative_pnl как baseline: initial = current - cumulative_pnl
    # Это эквивалентно (current - initial) / initial × 100.
    initial_balance_est = total_balance - total_pnl
    if initial_balance_est <= 0:
        initial_balance_est = total_balance  # fallback
    pnl_pct = round((total_balance - initial_balance_est) / initial_balance_est * 100, 2) if initial_balance_est > 0 else 0

    # max_positions = агрегат из presets всех enabled
    max_positions_total = 0
    for a in enabled:
        preset = ls.SAFETY_PRESETS.get(a.get("safety_preset", "paper_mirror"), {})
        max_positions_total += preset.get("max_positions", 0)

    response = {
        "ok": True,
        "count_accounts": len(accounts),
        "count_enabled": len(enabled),
        "total_balance": round(total_balance, 2),
        "total_pnl": round(total_pnl, 2),
        "pnl_pct": pnl_pct,
        "open_count": len(open_trades),
        "max_positions": max_positions_total,
        "stats": {
            "total": closed_count,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "total_pnl": round(total_pnl, 2),
            "avg_pnl": round(total_pnl / closed_count, 2) if closed_count else 0,
            "failed_attempts": failed_count,
        },
        "positions": open_trades,
        "accounts": accounts_status,
    }
    _LIVE_SNAPSHOT_CACHE["ts"] = now_ts
    _LIVE_SNAPSHOT_CACHE["data"] = response
    return response


@app.get("/api/live/history-all")
async def api_live_history_all(limit: int = 200):
    """Последние N закрытых live трейдов (агрегат по всем аккаунтам)."""
    def _sync():
        from database import _live_trades
        closed_filter = {"status": {"$in": ["TP", "SL", "BE", "TRAIL", "MANUAL", "AI_CLOSE"]}}
        items = list(_live_trades().find(closed_filter).sort("closed_at", -1).limit(int(limit)))
        for it in items:
            it["_id"] = str(it.get("_id", ""))
            for k in ("opened_at", "closed_at"):
                if it.get(k) and hasattr(it[k], "isoformat"):
                    it[k] = it[k].isoformat()
        return items
    items = await asyncio.to_thread(_sync)
    return {"ok": True, "count": len(items), "items": items}


@app.get("/api/live/rejections-all")
async def api_live_rejections_all(limit: int = 100):
    """Последние N FAILED_OPEN попыток (символ + причина)."""
    def _sync():
        from database import _live_trades
        items = list(_live_trades().find({"status": "FAILED_OPEN"}).sort("opened_at", -1).limit(int(limit)))
        for it in items:
            it["_id"] = str(it.get("_id", ""))
            for k in ("opened_at", "closed_at"):
                if it.get(k) and hasattr(it[k], "isoformat"):
                    it[k] = it[k].isoformat()
        return items
    items = await asyncio.to_thread(_sync)
    return {"ok": True, "count": len(items), "items": items}


@app.get("/api/admin/health-detail")
async def api_admin_health_detail():
    """Детальная диагностика runtime состояния:
       memory %, asyncio tasks count, размеры кэшей, Mongo conn pool.
    Помогает диагностировать recurring hangs (28.04.2026 prod залипал
    через 1.5ч стабильной работы → подозрение на memory leak / task leak)."""
    import asyncio as _aio
    from database import utcnow
    detail = {"ok": True, "at": utcnow().isoformat()}

    # Memory + CPU (psutil опциональный — может не быть установлен)
    try:
        import psutil, os as _os
        proc = psutil.Process(_os.getpid())
        mem = proc.memory_info()
        detail["process"] = {
            "memory_rss_mb": round(mem.rss / 1024 / 1024, 1),
            "memory_percent": round(proc.memory_percent(), 2),
            "num_threads": proc.num_threads(),
        }
        vm = psutil.virtual_memory()
        detail["system_memory"] = {
            "total_gb": round(vm.total / 1024**3, 2),
            "percent_used": vm.percent,
        }
    except ImportError:
        # psutil не установлен — используем resource (POSIX) как fallback
        try:
            import resource as _res
            ru = _res.getrusage(_res.RUSAGE_SELF)
            detail["process"] = {
                "memory_rss_mb_approx": round(ru.ru_maxrss / 1024, 1),
                "note": "psutil not installed; using resource fallback",
            }
        except Exception:
            detail["process"] = {"note": "psutil not installed"}
    except Exception as e:
        detail["process_error"] = f"{type(e).__name__}: {str(e)[:200]}"

    # Asyncio tasks
    try:
        all_tasks = _aio.all_tasks()
        # Считаем по name префиксу
        from collections import Counter as _C
        names = _C()
        for t in all_tasks:
            n = t.get_name() or "unnamed"
            # Группируем mirror задач
            if "live-mirror-" in n:
                names["live-mirror-*"] += 1
            elif "Task-" in n:
                names["Task-*"] += 1
            else:
                names[n] += 1
        detail["asyncio_tasks"] = {
            "total": len(all_tasks),
            "by_name": dict(names.most_common(20)),
        }
    except Exception as e:
        detail["tasks_error"] = str(e)

    # Cache sizes
    cache_sizes = {}
    try:
        from exchange import (_symbols_cache, _price_cache, _futures_cache,
                              _kc_cache, _pump_cache, _eth_ctx_cache, _volume_cache)
        cache_sizes["exchange"] = {
            "symbols": len(_symbols_cache),
            "price": len(_price_cache),
            "futures": len(_futures_cache),
            "kc": len(_kc_cache),
            "pump": len(_pump_cache),
            "eth_ctx": len(_eth_ctx_cache),
            "volume": len(_volume_cache),
        }
    except Exception as e:
        cache_sizes["exchange_err"] = str(e)
    try:
        from live_trader import _leverage_cache, _account_locks, _exchange_per_account
        cache_sizes["live_trader"] = {
            "leverage": len(_leverage_cache),
            "account_locks": len(_account_locks),
            "exchange_instances": len(_exchange_per_account),
        }
    except Exception as e:
        cache_sizes["live_trader_err"] = str(e)
    try:
        cache_sizes["admin"] = {
            "st_by_pair": len(_st_by_pair_cache),
            "candles": len(_candles_cache),
        }
    except Exception:
        pass
    detail["caches"] = cache_sizes

    # Mongo conn pool (если PyMongo доступен)
    try:
        from database import _get_db
        db = _get_db()
        srv = db.client._topology._description.server_descriptions()
        detail["mongo"] = {
            "servers": len(srv),
            "address": str(list(srv.keys())[0]) if srv else None,
        }
    except Exception as e:
        detail["mongo_err"] = str(e)

    return detail


@app.get("/api/live/debug-recent")
async def api_live_debug_recent(limit: int = 10):
    """Последние live_trades любого статуса — для диагностики mirror/sync."""
    def _sync():
        from database import _live_trades
        return list(_live_trades().find({}).sort("opened_at", -1).limit(int(limit)))
    docs = await asyncio.to_thread(_sync)
    out = []
    for d in docs:
        out.append({
            "trade_id": d.get("trade_id"),
            "account_id": d.get("account_id"),
            "env": d.get("env"),
            "symbol": d.get("symbol"),
            "direction": d.get("direction"),
            "status": d.get("status"),
            "paper_trade_id": d.get("paper_trade_id"),
            "tp_order_id": d.get("tp_order_id"),
            "sl_order_id": d.get("sl_order_id"),
            "tp_error": d.get("tp_error"),
            "sl_error": d.get("sl_error"),
            "exchange_order_id": d.get("exchange_order_id"),
            "entry": d.get("entry"),
            "exit_price": d.get("exit_price"),
            "size_usdt": d.get("size_usdt"),
            "leverage": d.get("leverage"),
            "sync_detected": d.get("sync_detected"),
            "opened_at": d.get("opened_at").isoformat() if d.get("opened_at") else None,
            "closed_at": d.get("closed_at").isoformat() if d.get("closed_at") else None,
            "pnl_pct": d.get("pnl_pct"),
        })
    return {"ok": True, "count": len(out), "trades": out}


@app.post("/api/paper/test-open")
async def api_paper_test_open(payload: dict | None = None):
    """Открыть тестовую позицию в paper (+ зеркало в testnet/real).

    payload:
      - force=true (по умолч.): вызывает pt.open_position() напрямую,
        минуя фильтры verified_entry. Используется для проверки UI и
        потока mirror.
      - force=false: вызывает _paper_on_signal() — полный пайплайн с
        rule-based проверками.
      - symbol/direction/entry/tp1/sl/leverage/size_pct — параметры (опц.)
    """
    sig = payload or {}
    force = sig.get("force", True)
    if not sig.get("symbol"):
        sig["symbol"] = "BTCUSDT"
        sig["pair"] = "BTC/USDT"
        sig["direction"] = "LONG"
        sig["source"] = "manual_test"
        sig["is_top_pick"] = True
    if not sig.get("entry"):
        from exchange import get_prices_any
        prices = await asyncio.to_thread(get_prices_any, [sig.get("pair", "BTC/USDT")])
        sig["entry"] = prices.get(sig["symbol"]) or 65000

    try:
        from database import _live_trades
        import paper_trader as pt
        import live_safety as ls
        import live_trader as lt
        import watcher as w

        before_paper = len(pt.get_open_positions())
        before_live_ids = {t["trade_id"] for t in _live_trades().find({"status": "OPEN"}, {"trade_id":1})}

        paper_pos = None
        if force:
            # Прямой вызов open_position минуя ai_decide / verified_entry
            entry = float(sig["entry"])
            tp1 = float(sig.get("tp1") or entry * (1.02 if sig["direction"] == "LONG" else 0.98))
            sl = float(sig.get("sl") or entry * (0.98 if sig["direction"] == "LONG" else 1.02))
            paper_pos = pt.open_position(
                symbol=sig["symbol"],
                direction=sig["direction"],
                entry=entry,
                tp1=tp1,
                sl=sl,
                leverage=int(sig.get("leverage", 5)),
                size_pct=float(sig.get("size_pct", 3)),
                source=sig.get("source", "manual_test"),
                reasoning="🧪 manual test position (force=true)",
            )
            try:
                await pt._send_open_alert(paper_pos, {"reasoning": paper_pos.get("ai_reasoning","")})
            except Exception:
                pass
            # Зеркало на live аккаунты — синхронно (чтобы тест получил реальный результат)
            mirror_results = []
            if paper_pos:
                sig["paper_trade_id"] = paper_pos.get("trade_id")
                accounts = ls.get_enabled_accounts()
                mirror_decision = {
                    "enter": True,
                    "leverage": paper_pos.get("leverage", 5),
                    "size_pct": paper_pos.get("size_pct", 3),
                    "tp1": paper_pos.get("tp1"),
                    "sl": paper_pos.get("sl"),
                    "reasoning": paper_pos.get("ai_reasoning", ""),
                }
                for acc in accounts:
                    try:
                        r = await lt.mirror_paper_for_account(sig, mirror_decision, acc)
                        mirror_results.append({"account": acc.get("_id"), "result": r})
                    except Exception as me:
                        import traceback as _tb
                        mirror_results.append({
                            "account": acc.get("_id"),
                            "error": str(me),
                            "tb": _tb.format_exc()[-500:],
                        })
        else:
            await w._paper_on_signal(sig)
            paper_pos = next(
                (p for p in pt.get_open_positions()
                 if p.get("symbol") == sig["symbol"] and p.get("direction") == sig["direction"]),
                None
            )

        # Дать время async create_task для live
        await asyncio.sleep(3.0)

        after_paper = pt.get_open_positions()
        after_live = list(_live_trades().find(
            {"status": "OPEN"},
            {"trade_id":1,"symbol":1,"direction":1,"env":1,"account_id":1,
             "paper_trade_id":1,"tp_order_id":1,"sl_order_id":1,
             "entry":1,"size_usdt":1,"leverage":1}
        ))
        new_live = [t for t in after_live if t["trade_id"] not in before_live_ids]

        # Конвертация ObjectId/datetime в строки
        def _safe(d):
            if not d: return None
            r = {}
            for k, v in d.items():
                if hasattr(v, "isoformat"): r[k] = v.isoformat()
                elif k == "_id": r[k] = str(v)
                else: r[k] = v
            return r

        return {
            "ok": True,
            "force": bool(force),
            "input": sig,
            "paper_opened": bool(paper_pos),
            "paper_position": _safe(paper_pos),
            "mirror_results": mirror_results if force else None,
            "live_opened_count": len(new_live),
            "live_positions": [_safe(t) for t in new_live],
            "before_paper_count": before_paper,
            "after_paper_count": len(after_paper),
        }
    except Exception as e:
        import traceback
        return {"ok": False, "error": f"{e}", "traceback": traceback.format_exc()[-1500:]}


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
        return {"ok": False, "error": "payload required (CV fallback удалён 2026-07-01)"}
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
        "bot4 (ai)": bool(getattr(_w, "_bot4", None)),
        "bot5 (confluence)": bool(getattr(_w, "_bot5", None)),
        "bot7 (cluster)": bool(getattr(_w, "_bot7", None)),
        "bot9 (top picks)": bool(getattr(_w, "_bot9", None)),
        "bot10 (supertrend)": bool(getattr(_w, "_bot10", None)),
    }
    # Tokens present in env
    from config import (BOT_TOKEN, BOT4_BOT_TOKEN,
                        BOT5_BOT_TOKEN, BOT7_BOT_TOKEN, BOT9_BOT_TOKEN,
                        BOT10_BOT_TOKEN)
    tokens = {
        "BOT_TOKEN":        bool(BOT_TOKEN),
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
    def _cnt_cluster():
        return _clusters().count_documents({"trigger_at": {"$gte": since_24h}})
    def _cnt_st():
        try:
            return _sts().count_documents({"flip_at": {"$gte": since_24h}})
        except Exception:
            return 0

    (conf_all, conf_alertable_24h, conf_alertable_6h,
     cluster_24h, st_24h) = await asyncio.gather(
        asyncio.to_thread(_cnt_conf_all),
        asyncio.to_thread(_cnt_conf_alert_24h),
        asyncio.to_thread(_cnt_conf_alert_6h),
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
            "cluster": cluster_24h,
            "supertrend": st_24h,
        },
        "note": "Confluence alerts отправляются только при score>=5 AND st_passed=True",
    }




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
    # FORMED-алерты отключены по запросу пользователя.
    # В BOT8 отправляются только ENTRY (retest сработал) и TP/SL события
    # — по ним принимают решения, а "следим/формируется" было шумом.
    # Alerts живут в watcher._send_fvg_entry_alert / _send_fvg_close_alert.
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


@app.post("/api/fvg-rescore-all")
async def api_fvg_rescore_all(payload: dict | None = None):
    """Пересчитать confluence_score для всех FVG за N часов (default 168h=7d).
    Используется после изменения логики scoring или бэкфилла 4H алертов."""
    from fvg_top_picks import rescore_all
    hours = int((payload or {}).get("hours", 168))
    stats = await asyncio.to_thread(rescore_all, hours)
    return {"ok": True, "stats": stats}


@app.post("/api/fvg/test-alert")
async def api_fvg_test_alert(payload: dict | None = None):
    """Шлёт тестовый ENTRY-алерт в BOT8 + возвращает диагностику:
      - есть ли BOT8_BOT_TOKEN
      - есть ли ADMIN_CHAT_ID
      - инициализировался ли _bot8
      - результат httpx getMe() / send_message
    Body (optional): {"fvg_id": "..."} — конкретный сигнал."""
    import os
    from database import _fvg_signals
    from bson import ObjectId
    payload = payload or {}
    diag: dict = {}

    # 1. ENV checks
    bot8_token = os.getenv("BOT8_BOT_TOKEN", "").strip()
    admin_chat_id = os.getenv("ADMIN_CHAT_ID", "").strip()
    diag["bot8_token_set"] = bool(bot8_token)
    diag["bot8_token_len"] = len(bot8_token)
    diag["admin_chat_id_set"] = bool(admin_chat_id)
    diag["admin_chat_id_value"] = admin_chat_id or "(empty)"

    if not bot8_token:
        return {"ok": False, "reason": "BOT8_BOT_TOKEN не задан в Railway Variables",
                "diag": diag}
    if not admin_chat_id:
        return {"ok": False, "reason": "ADMIN_CHAT_ID не задан в Railway Variables",
                "diag": diag}

    # 2. Прямой httpx-вызов в Telegram getMe — проверка токена
    import httpx
    try:
        r = httpx.get(f"https://api.telegram.org/bot{bot8_token}/getMe", timeout=5)
        getme = r.json()
        diag["getme_ok"] = getme.get("ok", False)
        if getme.get("ok"):
            diag["bot_username"] = "@" + (getme.get("result", {}).get("username") or "?")
        else:
            diag["getme_error"] = getme.get("description", "?")
            return {"ok": False, "reason": f"BOT8 токен невалидный: {diag['getme_error']}",
                    "diag": diag}
    except Exception as e:
        diag["getme_exception"] = str(e)
        return {"ok": False, "reason": f"Не смог достучаться до Telegram API: {e}",
                "diag": diag}

    # 3. Берём сигнал
    sig = None
    fid = payload.get("fvg_id")
    if fid:
        try:
            sig = _fvg_signals().find_one({"_id": ObjectId(fid)})
        except Exception:
            pass
    if not sig:
        sig = _fvg_signals().find_one({}, sort=[("formed_at", -1)])
    if not sig:
        return {"ok": False, "reason": "В fvg_signals нет записей", "diag": diag}

    # 4. Прямая отправка через httpx (минуя watcher._bot8 aiogram wrapper)
    # — это даст точный error message если send_message провалится.
    instr = sig.get("instrument", "?")
    direction = sig.get("direction", "?")
    dir_emoji = "🟢" if direction == "bullish" else "🔴"
    entry = sig.get("entry_price", "?")
    sl = sig.get("sl_price", "?")
    tf = sig.get("timeframe", "?")
    text = (
        f"🧪 <b>TEST · FVG ENTRY</b>\n"
        f"{dir_emoji} <b>{instr}</b> · {direction.upper()} · {tf}\n\n"
        f"Entry: <code>{entry}</code>\n"
        f"SL:    <code>{sl}</code>\n\n"
        f"<i>Это тестовое сообщение от /api/fvg/test-alert</i>"
    )
    try:
        r = httpx.post(
            f"https://api.telegram.org/bot{bot8_token}/sendMessage",
            json={"chat_id": admin_chat_id, "text": text, "parse_mode": "HTML"},
            timeout=5,
        )
        resp = r.json()
        diag["send_ok"] = resp.get("ok", False)
        diag["send_response"] = resp
        if resp.get("ok"):
            return {"ok": True,
                    "sent_for": instr, "direction": direction, "entry": entry,
                    "note": f"TEST alert sent via {diag.get('bot_username','BOT8')}",
                    "diag": diag}
        else:
            return {"ok": False,
                    "reason": f"Telegram вернул ok:false — {resp.get('description','?')}",
                    "diag": diag}
    except Exception as e:
        diag["send_exception"] = str(e)
        return {"ok": False, "reason": f"httpx.post упал: {e}", "diag": diag}


async def _send_fvg_formed_alert_safe(result: dict):
    """Безопасный wrapper — не валит webhook если alert module отсутствует.
    Больше не вызывается из api_tv_webhook (FORMED alerts отключены),
    но функция оставлена для совместимости и возможного возврата."""
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


_SYSTEM_HEALTH_CACHE = {"ts": 0.0, "data": None}
_SYSTEM_HEALTH_TTL = 15.0  # 15с — UI polling может быть чаще, нагрузку снизим


@app.get("/api/system-health")
async def api_system_health():
    """Комплексный health-monitor: все подсистемы + видимые проблемы.
    Cache 15с — все checks делают Mongo find_one, не хочется тяжелить
    при частом UI polling."""
    import time as _t
    now = _t.time()
    cached = _SYSTEM_HEALTH_CACHE.get("data")
    if cached is not None and (now - _SYSTEM_HEALTH_CACHE["ts"]) < _SYSTEM_HEALTH_TTL:
        return cached
    from system_health import collect_health
    data = await asyncio.to_thread(collect_health)
    _SYSTEM_HEALTH_CACHE["ts"] = now
    _SYSTEM_HEALTH_CACHE["data"] = data
    return data


@app.get("/")
async def root():
    return RedirectResponse(url="/signals")


# ── 🚀🎣 IMPULSE / FADE (momentum detector, research 2026-07-02) ─────────────

@app.get("/api/momentum-signals")
async def api_momentum_signals(hours: int = 336):
    """Сигналы impulse_detector (new_strategy_signals, strategy in impulse/fade)
    + статусы аутком-трекера (WAITING/TP/SL/TIMEOUT, pnl_pct)."""
    def _sync():
        from database import _get_db, utcnow
        from datetime import timedelta
        since = utcnow() - timedelta(hours=max(1, min(hours, 2160)))
        col = _get_db().new_strategy_signals
        items = []
        for n in col.find(
            {"strategy": {"$in": ["impulse", "fade", "ignition", "rider_short", "ten"]},
             "created_at": {"$gte": since}},
            {"strategy": 1, "pair": 1, "direction": 1, "entry": 1, "tp": 1,
             "sl": 1, "horizon_h": 1, "indicators": 1, "state": 1,
             "pnl_pct": 1, "exit_price": 1, "exit_at": 1, "created_at": 1},
        ).sort("created_at", -1).limit(500):
            at = n.get("created_at")
            ex = n.get("exit_at")
            items.append({
                "strategy": n.get("strategy"),
                "pair": n.get("pair"),
                "direction": n.get("direction"),
                "entry": n.get("entry"),
                "tp": n.get("tp"),
                "sl": n.get("sl"),
                "horizon_h": n.get("horizon_h"),
                "indicators": n.get("indicators") or {},
                "state": n.get("state", "WAITING"),
                "pnl_pct": n.get("pnl_pct"),
                "exit_price": n.get("exit_price"),
                "exit_at": ex.isoformat() if hasattr(ex, "isoformat") else None,
                "at": at.isoformat() if hasattr(at, "isoformat") else None,
            })
        return {"items": items, "count": len(items)}
    return await asyncio.to_thread(_sync)


@app.get("/api/health")
async def api_health():
    """🚦 Здоровье платформы: пульсы фоновых циклов (heartbeats) + свежесть
    данных в Mongo. Для индикатора «всё собирается» в шапке.
    Компонент: ok — в пределах интервала; warn — опаздывает; down — стоит."""
    def _collect():
        from database import _get_db, utcnow
        db = _get_db()
        now = utcnow()

        def age_min(dt):
            if dt is None:
                return None
            try:
                return (now - dt).total_seconds() / 60.0
            except Exception:
                return None

        comps = []

        def add(name, label, amin, warn_m, down_m):
            st = ("down" if amin is None or amin > down_m
                  else "warn" if amin > warn_m else "ok")
            comps.append({"name": name, "label": label,
                          "age_min": round(amin, 1) if amin is not None else None,
                          "warn_after_min": warn_m, "down_after_min": down_m,
                          "status": st})

        hb = {d["_id"]: d.get("at") for d in db.heartbeats.find()}
        # циклы (пульс пишется в начале каждой итерации)
        add("momentum", "💥💰 Momentum-скан (автоторговля)", age_min(hb.get("momentum")), 35, 90)
        add("accum", "🧊 Скан баз/ширины/Δ24ч", age_min(hb.get("accum")), 45, 120)
        add("st_tracker", "🌀 SuperTrend tracker", age_min(hb.get("st_tracker")), 15, 45)
        add("outcomes", "🎯 Трекер исходов TP/SL", age_min(hb.get("outcomes")), 15, 45)
        add("live_mirror", "💵 Paper→Live зеркало", age_min(hb.get("live_mirror")), 5, 15)
        # данные (артефакты сборщиков)
        ms = db.market_state.find_one({"_id": "breadth"}) or {}
        add("breadth", "🎯 Ширина рынка (бейдж стороны)", age_min(ms.get("updated_at")), 45, 120)
        ds = db.delta_state.find_one(sort=[("updated_at", -1)]) or {}
        add("delta24", "Δ24ч скринера", age_min(ds.get("updated_at")), 45, 120)
        ws = db.cluster_delta.find_one(sort=[("cached_at", -1)]) or {}
        add("delta_ws", "⚡ Realtime-дельта (WebSocket)", age_min(ws.get("cached_at")), 10, 30)
        sig = db.new_strategy_signals.find_one(sort=[("created_at", -1)]) or {}
        add("signals", "📡 Поток сигналов (любой источник)", age_min(sig.get("created_at")), 180, 480)
        return comps

    try:
        comps = await asyncio.to_thread(_collect)
        overall = ("down" if any(c["status"] == "down" for c in comps)
                   else "warn" if any(c["status"] == "warn" for c in comps)
                   else "ok")
        return {"overall": overall, "components": comps}
    except Exception as e:
        return {"overall": "down", "error": str(e), "components": []}


@app.get("/api/market-side")
async def api_market_side():
    """🟢LONG/🔴SHORT/⚪НЕЙТРАЛ для всего рынка: ширина (доля пар с RSI4h>SMA
    по универсуму, из 30-мин скана) + BTC ST4h. Бэктест год (~130k входов):
    ширина >60% — рынок разогрет, edge съеден у обеих сторон; 40-60% — сторона
    по BTC (UP: лонги 38.7% vs BE 33; DOWN: шорты 41.7% vs BE 40);
    <40% — шорт-рынок при любом BTC (шорты 43-44%, лонги 27%)."""
    try:
        from database import _get_db
        from rider_detector import get_btc_st4
        doc = await asyncio.to_thread(
            lambda: _get_db().market_state.find_one({"_id": "breadth"}))
        st = await asyncio.to_thread(get_btc_st4)
        if not doc or doc.get("pct") is None:
            return {"side": "?", "reason": "нет данных ширины — ждём скан (30 мин)"}
        pct = doc["pct"]
        if pct > 60:
            side = "NEUTRAL"
            reason = (f"ширина {pct:.0f}% — рынок разогрет, статистического "
                      f"преимущества нет ни у лонгов, ни у шортов")
        elif pct < 40:
            side = "SHORT"
            reason = (f"ширина {pct:.0f}% — слабость по всему рынку: "
                      f"шорты +3-4пп к безубытку, лонги мертвы (27%)")
        elif st == "UP":
            side = "LONG"
            reason = (f"ширина {pct:.0f}% + BTC ST4h UP — бычий рынок "
                      f"с запасом хода: лонги 38.7% vs безубыток 33")
        elif st == "DOWN":
            side = "SHORT"
            reason = (f"ширина {pct:.0f}% + BTC ST4h DOWN — "
                      f"шорты +2-4пп к безубытку")
        else:
            side = "NEUTRAL"
            reason = f"ширина {pct:.0f}%, BTC ST4h недоступен"
        upd = doc.get("updated_at")
        return {"side": side, "breadth_pct": pct, "btc_st4": st,
                "n_pairs": doc.get("total"), "reason": reason,
                "updated_at": upd.isoformat() if hasattr(upd, "isoformat") else None}
    except Exception as e:
        return {"side": "?", "error": str(e)}


@app.get("/api/btc-st4")
async def api_btc_st4():
    """BTC SuperTrend 4h (UP/DOWN) — гейт для шорт-пресета в журнале."""
    try:
        from rider_detector import get_btc_st4
        st = await asyncio.to_thread(get_btc_st4)
        return {"state": st or "?"}
    except Exception as e:
        return {"state": "?", "error": str(e)}


# ── 🧊 ACCUMULATION watchlist ────────────────────────────────────────────────

@app.get("/api/accumulation")
async def api_accumulation():
    """Монеты в фазе накопления сейчас (снапшот из watcher, обновление 30 мин).
    Не торговый сигнал — watchlist (research 2026-07-10)."""
    def _sync():
        from database import _get_db
        items = []
        upd = None
        for d in _get_db().accum_state.find().sort("hours", -1).limit(100):
            u = d.get("updated_at")
            if u is not None and upd is None:
                upd = u.isoformat() if hasattr(u, "isoformat") else None
            items.append({k: d.get(k) for k in
                          ("pair", "symbol", "price", "base_hi", "base_lo",
                           "rng_pct", "hours", "dist_up_pct", "dist_dn_pct",
                           "delta_dz")})
        try:
            from accum_detector import _last_scan
            scan_debug = dict(_last_scan)
        except Exception:
            scan_debug = {}
        return {"items": items, "count": len(items), "updated_at": upd,
                "scan_debug": scan_debug}
    return await asyncio.to_thread(_sync)


@app.get("/api/accum-momentum")
async def api_accum_momentum(hours: int = 24):
    """🧊💥 База→Вход: momentum-сигналы (ignition/ten) по монетам, которые
    были в накоплении на момент сигнала (или база разрешилась ≤6ч до него).
    Окно 24ч — сетап «висит день»."""
    def _sync():
        from database import _get_db, utcnow
        from datetime import timedelta
        db = _get_db()
        now = utcnow()
        since = now - timedelta(hours=max(1, min(hours, 96)))
        # 1. активные базы (снапшот) + недавние разрешения
        active = {d["pair"]: d for d in db.accum_state.find()}
        recent_res = {}
        for e in db.accum_events.find({"resolved_at": {"$gte": since - timedelta(hours=24)}}):
            recent_res.setdefault(e["pair"], []).append(e)
        # 2. momentum-сигналы за окно
        items = []
        for s in db.new_strategy_signals.find(
                {"strategy": {"$in": ["ignition", "ten"]},
                 "created_at": {"$gte": since}}).sort("created_at", -1):
            pair = s.get("pair")
            base = None
            src = None
            a = active.get(pair)
            if a:
                # база активна сейчас — сигнал должен попадать в её интервал
                started = now - timedelta(hours=a.get("hours") or 0)
                if s["created_at"] >= started - timedelta(hours=2):
                    base = {"hours": a.get("hours"), "rng_pct": a.get("rng_pct"),
                            "delta_dz": a.get("delta_dz"), "status": "в базе"}
                    src = "active"
            if base is None:
                for e in recent_res.get(pair, []):
                    b_start = e["resolved_at"] - timedelta(hours=e.get("hours") or 0)
                    # сигнал в интервале базы либо ≤24ч после пробоя —
                    # монета «из накопления» ещё сутки (по запросу 10.07)
                    if b_start - timedelta(hours=2) <= s["created_at"] <= \
                       e["resolved_at"] + timedelta(hours=24):
                        base = {"hours": e.get("hours"), "rng_pct": e.get("rng_pct"),
                                "delta_dz": e.get("delta_dz"),
                                "status": f"пробой {'ВВЕРХ' if e.get('resolution') == 'UP' else 'ВНИЗ'}"}
                        src = "resolved"
                        break
            if base is None:
                continue
            at = s.get("created_at")
            items.append({
                "pair": pair, "strategy": s.get("strategy"),
                "direction": s.get("direction"), "entry": s.get("entry"),
                "tp": s.get("tp"), "sl": s.get("sl"),
                "state": s.get("state", "WAITING"), "pnl_pct": s.get("pnl_pct"),
                "at": at.isoformat() if hasattr(at, "isoformat") else None,
                "base": base, "base_src": src,
            })
        return {"items": items, "count": len(items)}
    return await asyncio.to_thread(_sync)


_kd_cache: dict = {}   # {(sym, tf, lim): (ts, payload)} — ускорение открытия графика
_KD_TTL_S = 90


@app.get("/api/klines-delta")
async def api_klines_delta(symbol: str, tf: str = "1h", limit: int = 500):
    """Свечи с тайкер-дельтой для 📈 KChart-графиков.
    fapi (фьючерсы) -> Vision (спот) -> get_klines_any (BingX, без дельты).
    Кэш 90с — график открывается мгновенно при повторных заходах."""
    def _sync():
        import time as _t
        import requests as _rq
        sym = (symbol or "").upper().replace("/", "")
        if not sym.endswith("USDT"):
            sym += "USDT"
        itv = tf if tf in ("1m", "5m", "15m", "30m", "1h", "4h", "12h", "1d") else "1h"
        lim = max(50, min(int(limit or 500), 1000))
        key = (sym, itv, lim)
        cached = _kd_cache.get(key)
        if cached and _t.time() - cached[0] < _KD_TTL_S:
            return cached[1]
        payload = None
        for url in ("https://fapi.binance.com/fapi/v1/klines",
                    "https://data-api.binance.vision/api/v3/klines"):
            try:
                r = _rq.get(url, params=dict(symbol=sym, interval=itv, limit=lim),
                            timeout=10)
                if r.status_code != 200:
                    continue
                rows = r.json()
                if not rows or len(rows) < 10:
                    continue
                payload = {"ok": True, "symbol": sym, "tf": itv,
                           "bars": [{"t": int(x[0]), "o": float(x[1]), "h": float(x[2]),
                                     "l": float(x[3]), "c": float(x[4]), "v": float(x[5]),
                                     "d": round(2 * float(x[9]) - float(x[5]), 2)}
                                    for x in rows]}
                break
            except Exception:
                continue
        if payload is None:
            # Пара не на Binance (спот/фьючи) — BingX-фолбэк без дельты
            try:
                from exchange import get_klines_any
                kl = get_klines_any(sym[:-4] + "/USDT", itv, lim)
                if kl and len(kl) >= 10:
                    payload = {"ok": True, "symbol": sym, "tf": itv, "delta": False,
                               "bars": [{"t": int(c["t"]), "o": c["o"], "h": c["h"],
                                         "l": c["l"], "c": c["c"], "v": c.get("v", 0),
                                         "d": None} for c in kl]}
            except Exception:
                pass
        if payload is None:
            payload = {"ok": False, "error": "no data", "bars": []}
        else:
            _kd_cache[key] = (_t.time(), payload)
            if len(_kd_cache) > 200:
                for k, _ in sorted(_kd_cache.items(), key=lambda kv: kv[1][0])[:80]:
                    _kd_cache.pop(k, None)
        return payload
    return await asyncio.to_thread(_sync)


@app.get("/api/delta-map")
async def api_delta_map():
    """Текущая фьючерсная тайкер-дельта 24ч (z-норм.) по сканируемым парам.
    Источник: fapi klines поле 9, обновление вместе с 🧊-сканом (30 мин)."""
    def _sync():
        from database import _get_db
        mp = {}
        upd = None
        for d in _get_db().delta_state.find():
            mp[d.get("symbol") or ""] = d.get("dz24")
            if upd is None:
                u = d.get("updated_at")
                upd = u.isoformat() if hasattr(u, "isoformat") else None
        return {"map": mp, "count": len(mp), "updated_at": upd}
    return await asyncio.to_thread(_sync)


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
    if bot in ("confluence", "journal", "autotrading"):
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

    # cryptovizor page удалена вместе с CV ingestion (2026-07-01)

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


@app.get("/api/watcher-heartbeat")
async def api_watcher_heartbeat():
    """Состояние watcher main loop. Показывает stage (где сейчас находится
    цикл) + последний tick + если crash — error и traceback."""
    def _sync():
        from database import _get_db
        doc = _get_db().system.find_one({"_id": "watcher_heartbeat"})
        if not doc:
            return {"ok": False, "error": "no heartbeat — watcher не стартует"}
        from datetime import datetime, timezone
        at = doc.get("at")
        age_sec = None
        if at:
            try:
                at_naive = at.replace(tzinfo=None) if getattr(at, "tzinfo", None) else at
                age_sec = int((datetime.now(timezone.utc).replace(tzinfo=None) - at_naive).total_seconds())
            except Exception:
                pass
        return {
            "ok": True,
            "stage": doc.get("stage"),
            "at": at.isoformat() if hasattr(at, "isoformat") else str(at),
            "age_sec": age_sec,
            "data": doc.get("data") or {},
            "error": doc.get("error"),
        }
    return await asyncio.to_thread(_sync)


@app.post("/api/confluence/force-scan")
async def api_confluence_force_scan():
    """Запускает _check_confluence прямо сейчас."""
    try:
        import watcher
        asyncio.create_task(watcher._check_confluence())
        return {"ok": True, "started": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


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
    def _sync():
        from database import _confluence
        return _confluence().delete_many({}).deleted_count
    deleted = await asyncio.to_thread(_sync)
    return {"ok": True, "deleted": deleted}


@app.get("/api/confluence/backtest")
async def api_confluence_backtest(st: int = 0):
    """st=1 — только прошедшие SuperTrend фильтр."""
    from exchange import get_futures_prices_only

    def _fetch():
        from database import _confluence
        query = {"st_passed": True} if st else {}
        return list(_confluence().find(query).sort("detected_at", -1).limit(200))
    docs = await asyncio.to_thread(_fetch)
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


_PAPER_STATUS_CACHE: dict = {"ts": 0.0, "data": None}
_PAPER_STATUS_TTL = 6.0  # 6с — UI polling каждые 15с, swing комфортный


@app.get("/api/paper/status")
async def api_paper_status():
    """Paper Trading статус. Cache 6с — раньше при лагах Atlas
    функция занимала 25+ секунд (балансы + xref live trades + цены),
    UI polling каждые 15с забивал loop. 6с TTL — компромис свежесть/нагрузка."""
    import time as _t
    now_ts = _t.time()
    cached = _PAPER_STATUS_CACHE.get("data")
    if cached is not None and (now_ts - _PAPER_STATUS_CACHE["ts"]) < _PAPER_STATUS_TTL:
        return cached

    import paper_trader as pt
    from exchange import get_prices_any

    def _sync_load():
        balance = pt.get_balance()
        positions = pt.get_open_positions()
        stats = pt.get_stats()
        live_by_paper_id: dict = {}
        live_by_sym_dir: dict = {}
        if positions:
            try:
                from database import _live_trades
                paper_trade_ids = [p.get("trade_id") for p in positions if p.get("trade_id")]
                paper_sym_dirs = [(p.get("symbol"), p.get("direction")) for p in positions]
                paper_symbols = list({sd[0] for sd in paper_sym_dirs if sd[0]})
                query = {
                    "$or": [
                        {"paper_trade_id": {"$in": paper_trade_ids}},
                        {"symbol": {"$in": paper_symbols}},
                    ]
                }
                live_relevant = list(_live_trades().find(
                    query,
                    {"paper_trade_id": 1, "symbol": 1, "direction": 1, "env": 1,
                     "account_id": 1, "trade_id": 1, "status": 1, "fail_reason": 1,
                     "opened_at": 1}
                ).sort("opened_at", -1).limit(200))
                for lt in live_relevant:
                    pid = lt.get("paper_trade_id")
                    if pid is not None:
                        live_by_paper_id.setdefault(pid, []).append(lt)
                    key = f"{lt.get('symbol')}_{lt.get('direction')}"
                    live_by_sym_dir.setdefault(key, []).append(lt)
            except Exception as _le:
                logging.getLogger(__name__).debug(f"[paper-status] live xref fail: {_le}")
        initial = pt.INITIAL_BALANCE
        try:
            from database import _get_db
            state_doc = _get_db().paper_trades.find_one({"_id": "state"}, {"initial_balance": 1})
            if state_doc and state_doc.get("initial_balance"):
                initial = float(state_doc["initial_balance"])
        except Exception:
            pass
        return balance, positions, stats, live_by_paper_id, live_by_sym_dir, initial

    balance, positions, stats, live_by_paper_id, live_by_sym_dir, initial = await asyncio.to_thread(_sync_load)
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
            # Cross-reference: paper_trade_id matching, fallback по symbol+direction
            matches = live_by_paper_id.get(p.get("trade_id"), [])
            if not matches:
                matches = live_by_sym_dir.get(f"{p.get('symbol')}_{p.get('direction')}", [])
            # Только последний live-трейд по аккаунту (самый свежий)
            seen_acct = set()
            unique_matches = []
            for m in matches:
                key = (m.get("env"), m.get("account_id"))
                if key in seen_acct:
                    continue
                seen_acct.add(key)
                unique_matches.append(m)
            p["live_envs"] = [
                {"env": m.get("env"), "account_id": m.get("account_id"),
                 "live_trade_id": m.get("trade_id"),
                 "live_status": m.get("status"),
                 "fail_reason": m.get("fail_reason")}
                for m in unique_matches
            ]
    pnl_pct = round((balance - initial) / initial * 100, 2) if initial > 0 else 0
    response = {
        "balance": balance,
        "initial": initial,
        "pnl_pct": pnl_pct,
        "positions": positions,
        "stats": stats,
    }
    _PAPER_STATUS_CACHE["ts"] = now_ts
    _PAPER_STATUS_CACHE["data"] = response
    return response


@app.get("/api/paper/accepts")
async def api_paper_accepts(limit: int = 200):
    """Лог входов в позиции — что совпало для принятия решения.
    Объединяет открытые + закрытые сделки, добавляет verified_entry checks
    из verified_signals collection если есть.

    Используется UI 'Входы' гармошкой (атрейдинг)."""
    def _sync():
        from database import _get_db
        db = _get_db()
        # Берём последние paper_trades с trade_id (открытые + закрытые)
        trades = list(db.paper_trades.find(
            {"trade_id": {"$exists": True}},
            sort=[("opened_at", -1)],
        ).limit(int(limit)))
        # Cross-reference с verified_signals для получения checks
        results = []
        for t in trades:
            sym = t.get("symbol", "")
            pair = (sym.replace("USDT", "/USDT") if sym.endswith("USDT") else sym)
            opened_at = t.get("opened_at")
            # Ищем verified_signals в окне ±5 мин от opened_at
            entry_checks = []
            verdict = None
            try:
                from datetime import timedelta as _td
                if opened_at:
                    win_start = opened_at - _td(minutes=5)
                    win_end = opened_at + _td(minutes=5)
                    vs = db.verified_signals.find_one(
                        {"pair": pair, "direction": t.get("direction"),
                         "created_at": {"$gte": win_start, "$lte": win_end}},
                        sort=[("created_at", -1)],
                    )
                    if vs:
                        entry_checks = vs.get("checks", [])
                        verdict = vs.get("verdict")
            except Exception:
                pass

            results.append({
                "trade_id": t.get("trade_id"),
                "symbol": sym,
                "pair": pair,
                "direction": t.get("direction"),
                "source": t.get("source"),
                "entry": t.get("entry"),
                "tp1": t.get("tp1"),
                "sl": t.get("sl"),
                "leverage": t.get("leverage"),
                "size_pct": t.get("size_pct"),
                "size_usdt": t.get("size_usdt"),
                "score": t.get("score"),
                "is_top_pick": bool(t.get("is_top_pick")),
                "ai_reasoning": t.get("ai_reasoning"),
                # v3.0 fields: regime/verdict/version при открытии
                "auto_strategy_regime": t.get("auto_strategy_regime"),
                "auto_strategy_verdict": t.get("auto_strategy_verdict"),
                "auto_strategy_version": t.get("auto_strategy_version"),
                "auto_strategy_label": t.get("auto_strategy_label"),
                "auto_strategy_reason": t.get("auto_strategy_reason"),
                # RSI per TF на момент входа (для UI "Входы")
                "rsi_15m": t.get("rsi_15m"),
                "rsi_1h": t.get("rsi_1h"),
                "rsi_4h": t.get("rsi_4h"),
                "rsi_1d": t.get("rsi_1d"),
                "sma_rsi_15m": t.get("sma_rsi_15m"),
                "sma_rsi_1h": t.get("sma_rsi_1h"),
                "sma_rsi_4h": t.get("sma_rsi_4h"),
                "sma_rsi_1d": t.get("sma_rsi_1d"),
                "status": t.get("status"),
                "pnl_usdt": t.get("pnl_usdt"),
                "pnl_pct": t.get("pnl_pct"),
                "opened_at": opened_at.isoformat() if hasattr(opened_at, "isoformat") else str(opened_at),
                "closed_at": t.get("closed_at").isoformat() if hasattr(t.get("closed_at"), "isoformat") else None,
                "verdict": verdict,
                "entry_checks": entry_checks,
            })
        return {"ok": True, "count": len(results), "items": results}

    return await asyncio.to_thread(_sync)


@app.get("/api/paper/history")
async def api_paper_history(limit: int = 50):
    """Cache 30s — UI polling каждые 15с, история меняется редко (только
    при закрытии позиции). Раньше зависало 15с при лагах Atlas."""
    from cache_utils import paper_history_cache
    import paper_trader as pt

    async def _compute():
        def _sync():
            history = pt.get_history(limit)
            for h in history:
                h["_id"] = str(h.get("_id", ""))
                for f in ("opened_at", "closed_at"):
                    if h.get(f) and hasattr(h[f], "isoformat"):
                        h[f] = h[f].isoformat()
            return history
        return {"items": await asyncio.to_thread(_sync)}

    return await paper_history_cache.get_or_compute(f"history|{limit}", _compute)


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

    # Tradium/Cryptovizor источники удалены (2026-07-05)
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
    # cryptovizor ветка удалена (2026-07-05)
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
    # cryptovizor ветка удалена (2026-07-05)

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
    from futures_data import get_all_futures_pairs
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
    from futures_data import get_all_futures_pairs
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


_verified_cache: dict = {}
_VERIFIED_TTL = 90.0  # сек, in-process cache (без него mongo query шёл 2.4с на каждый chart open)


@app.get("/api/verified-signals")
async def api_verified_signals(pair: str = "", hours: int = 168, limit: int = 500):
    """Список verified-сигналов (те что прошли 8-пунктовый чек и отправлены в @topmonetabot).
    Используется UI для маркеров на графиках и badge в журнале.
    pair — опционально (для конкретной пары); hours — окно (default 7 дней); limit — кап.

    In-memory cache 90с на (pair, hours, limit) — chart marker fetch'ы
    несколько раз в секунду на одну пару при смене TF, без cache они
    шли 2.4с каждый.
    """
    from database import _get_db, utcnow
    from datetime import timedelta

    cache_key = f"{pair}|{hours}|{limit}"
    now_ts = time.time()
    hit = _verified_cache.get(cache_key)
    if hit and (now_ts - hit[0]) < _VERIFIED_TTL:
        return hit[1]

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

    result = await asyncio.to_thread(_sync)
    _verified_cache[cache_key] = (now_ts, result)
    _cap_admin_cache(_verified_cache, 200)
    # Lazy eviction
    if len(_verified_cache) > 200:
        for k in [k for k, v in _verified_cache.items() if (now_ts - v[0]) > _VERIFIED_TTL * 3]:
            _verified_cache.pop(k, None)
    return result


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


@app.get("/api/journal/stacks")
async def api_journal_stacks(window_h: int = 6, min_stack: int = 3, limit: int = 50):
    """MOONSHOT detector: pairs с накоплением сигналов от разных источников.

    Использует данные journal_cache (заполнено _compute_journal_sync). Группирует
    items по pair, считает distinct (source, direction) в окне window_h hours,
    возвращает топ pairs с stack >= min_stack.

    Полезно для UI вкладки "🚀 Moonshot watch" — где пары на грани pump'а:
    accumulation density > N source-confirmations.
    """
    from cache_utils import journal_cache
    full = journal_cache.get("journal_all")
    if not full:
        # Compute если кэш пустой
        full = await asyncio.to_thread(_compute_journal_sync)
    items = (full or {}).get('items', [])
    if not items:
        return {"ok": True, "pairs": [], "window_h": window_h, "min_stack": min_stack}

    import time as _t
    from collections import defaultdict
    now_s = int(_t.time())
    cutoff = now_s - window_h * 3600
    by_pair: dict = defaultdict(list)
    for it in items:
        p = it.get('pair') or ''
        ts = it.get('at_ts') or 0
        if not (p and ts) or ts < cutoff:
            continue
        by_pair[p].append(it)

    results = []
    for p, sigs in by_pair.items():
        distinct = len(set((s.get('source'), (s.get('direction') or '').upper()) for s in sigs))
        if distinct < min_stack:
            continue
        long_n = sum(1 for s in sigs if (s.get('direction') or '').upper() == 'LONG')
        short_n = sum(1 for s in sigs if (s.get('direction') or '').upper() == 'SHORT')
        sources = sorted({s.get('source') for s in sigs if s.get('source')})
        last = max(sigs, key=lambda s: s.get('at_ts', 0))
        results.append({
            'pair': p,
            'symbol': last.get('symbol'),
            'stack_count': len(sigs),
            'stack_distinct': distinct,
            'long_count': long_n,
            'short_count': short_n,
            'sources': sources,
            'is_top_mover': bool(last.get('is_top_mover')),
            'change_24h': last.get('change_24h'),
            'last_at': last.get('at'),
            'last_at_ts': last.get('at_ts'),
            'last_direction': last.get('direction'),
            'last_source': last.get('source'),
            'delta_15m': last.get('delta_15m'),
            'delta_1h':  last.get('delta_1h'),
            'resonance_15m': last.get('resonance_15m'),
            'resonance_1h':  last.get('resonance_1h'),
        })

    # Sort: stack_distinct desc, потом stack_count desc
    results.sort(key=lambda x: (-x['stack_distinct'], -x['stack_count']))
    return {
        'ok': True,
        'window_h': window_h,
        'min_stack': min_stack,
        'count': len(results),
        'pairs': results[:limit],
    }


@app.get("/api/journal")
async def api_journal(limit: int = 1500, refresh: int = 0, debug: int = 0):
    """Все сигналы из 4 источников — для вкладки Журнал.
    Server-side limit 14 days + per-source cap. Cache 45s (async-lock safe).
    _compute_journal делает 7 sync Mongo-запросов → выносим в thread,
    иначе блокируется event loop и тормозят параллельные /api/* (candles и т.п.).

    limit (default 500): отдаём только последние N items (по at_ts desc).
    refresh=1: сбросить кеш и пересчитать с нуля (для диагностики).
    debug=1: вернуть breakdown по source/tier для проверки.
    Раньше возвращалось ~4270 за 14 дней (весь fetch занимал 15.7с,
    блокировал графики). 500 — разумный default для UI (юзер не видит
    дальше первых страниц), Mongo query всё равно собирает всё за 14д,
    но JSON-сериализация и transfer уменьшаются ×8.
    """
    from cache_utils import journal_cache

    if refresh:
        journal_cache.invalidate("journal_all")

    async def _compute_in_thread():
        return await asyncio.to_thread(_compute_journal_sync)

    # HARD TIMEOUT 40s — защита от зависающих inline fill'ов / Mongo stalls.
    # Compute обычно 8-15с на Railway (Atlas Stockholm), 30-40с локально.
    # Увеличили с 25→40 после добавления resonance prior-candles range query
    # (~3х больше Mongo-данных но всё ещё в пределах разумного на Railway).
    try:
        full = await asyncio.wait_for(
            journal_cache.get_or_compute("journal_all", _compute_in_thread),
            timeout=150.0,  # было 90s, бампнули после добавления ema_cross блока
        )
    except asyncio.TimeoutError:
        # Возвращаем stale cache если есть, иначе пустой
        full = journal_cache.get("journal_all") or {"items": []}
        logging.getLogger(__name__).warning(
            "[api/journal] compute timeout 150s — returning stale/empty"
        )
    items = full.get("items", []) if isinstance(full, dict) else []
    total = len(items)

    # Debug breakdown по source/tier — для проверки что Daily ST в потоке
    if debug:
        by_source: dict = {}
        st_by_tier: dict = {"vip": 0, "mtf": 0, "daily": 0, "?": 0}
        for it in items:
            src = it.get("source", "?")
            by_source[src] = by_source.get(src, 0) + 1
            if src == "supertrend":
                t = it.get("st_tier", "?")
                st_by_tier[t] = st_by_tier.get(t, 0) + 1
        return {
            "total": total,
            "by_source": by_source,
            "supertrend_by_tier": st_by_tier,
            "first_3_daily": [
                {"at": it.get("at"), "symbol": it.get("symbol"),
                 "direction": it.get("direction"), "pattern": it.get("pattern")}
                for it in items
                if it.get("source") == "supertrend" and it.get("st_tier") == "daily"
            ][:3],
        }

    # Slice после cache (cache хранит full result, slice — почти free)
    if limit and limit > 0 and total > limit:
        items = items[:limit]
    return {"items": items, "total": total, "returned": len(items)}


@app.get("/api/journal/fast")
async def api_journal_fast(limit: int = 1500, refresh: int = 0):
    """⚡ Быстрый журнал — только Mongo fetches + stack_count, без enrichments.
    Latency ~2-3s vs 30-40s для /api/journal. UI делает оба запроса параллельно
    и мерджит enrichment-поля (4h/12h state, divergence, top_movers, delta,
    resonance, rsi, trend, quality_score, squeeze) после загрузки full.

    Cache: 30s (короче чем 60s у full — fast endpoint обновляется чаще для свежих
    сигналов; full всё равно подтянет enrich при следующем poll).
    """
    from cache_utils import journal_fast_cache

    if refresh:
        journal_fast_cache.invalidate("journal_fast")

    async def _compute_in_thread():
        return await asyncio.to_thread(_compute_journal_fast_sync)

    try:
        full = await asyncio.wait_for(
            journal_fast_cache.get_or_compute("journal_fast", _compute_in_thread),
            timeout=15.0,
        )
    except asyncio.TimeoutError:
        full = journal_fast_cache.get("journal_fast") or {"items": []}
        logging.getLogger(__name__).warning(
            "[api/journal/fast] compute timeout 15s — returning stale/empty"
        )
    items = full.get("items", []) if isinstance(full, dict) else []
    total = len(items)
    if limit and limit > 0 and total > limit:
        items = items[:limit]
    return {"items": items, "total": total, "returned": len(items), "fast": True}


@app.get("/api/prepump/candidates")
async def api_prepump_candidates(tier: str = "", limit: int = 200, hours: int = 24):
    """Pre-Pump candidates за последние N часов. Filter by tier.
    Returns list of {pair, tier, composite_score, components, sectors, ...}."""
    from datetime import datetime, timezone, timedelta
    from database import _get_db
    db = _get_db()
    col = db.pre_pump_candidates
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    q = {'detected_at': {'$gte': cutoff}}
    if tier:
        q['tier'] = tier.upper()

    def _query():
        items = []
        for d in col.find(q).sort('detected_at', -1).limit(limit):
            items.append({
                'pair': d.get('pair'),
                'tier': d.get('tier'),
                'composite_score': d.get('composite_score'),
                'direction': d.get('direction', 'LONG'),
                'components': d.get('components', {}),
                'volume_data': d.get('volume_data', {}),
                'oi_data': d.get('oi_data', {}),
                'funding_data': d.get('funding_data', {}),
                'sectors': d.get('sectors', []),
                'sector_active': d.get('sector_active', False),
                'detected_at': d.get('detected_at').isoformat() if d.get('detected_at') else None,
                'detected_at_ts': d.get('detected_at_ts'),
                'is_new_prime': d.get('is_new_prime', False),
            })
        return items

    items = await asyncio.to_thread(_query)
    # Dedup by pair — оставляем самый свежий
    seen = {}
    for it in items:
        p = it['pair']
        if p not in seen or it['detected_at_ts'] > seen[p]['detected_at_ts']:
            seen[p] = it
    deduped = sorted(seen.values(),
                      key=lambda x: (-(x.get('composite_score') or 0),
                                      -(x.get('detected_at_ts') or 0)))
    return {'items': deduped, 'count': len(deduped)}


@app.post("/api/precondition-analysis/start")
async def api_precondition_analysis_start():
    import precondition_analysis as pa
    state = pa._get_state()
    if state.get('running'):
        return {'started': False, 'state': state}
    asyncio.create_task(asyncio.to_thread(pa.run_precondition_analysis))
    return {'started': True}


@app.get("/api/precondition-analysis/status")
async def api_precondition_analysis_status():
    import precondition_analysis as pa
    from database import _get_db
    state = pa._get_state()
    out = {'state': state}
    try:
        db = _get_db()
        report = db.precondition_analysis_report.find_one({}, {'_id': 0})
        if report:
            for k in ['started_at', 'finished_at']:
                v = report.get(k)
                if hasattr(v, 'isoformat'):
                    report[k] = v.isoformat()
            out['report'] = report
    except Exception:
        pass
    return out


@app.post("/api/signal-quality/backtest/start")
async def api_signal_quality_backtest_start():
    """Запускает backtest качества каждого источника signals (14d)."""
    import signal_quality_backtest as sq
    state = sq._get_state()
    if state.get('running'):
        return {'started': False, 'state': state}
    asyncio.create_task(asyncio.to_thread(sq.run_signal_quality_backtest))
    return {'started': True}


@app.get("/api/signal-quality/backtest/status")
async def api_signal_quality_backtest_status():
    import signal_quality_backtest as sq
    from database import _get_db
    state = sq._get_state()
    out = {'state': state}
    try:
        db = _get_db()
        report = db.signal_quality_backtest_report.find_one({}, {'_id': 0})
        if report:
            for k in ['started_at', 'finished_at']:
                v = report.get(k)
                if hasattr(v, 'isoformat'):
                    report[k] = v.isoformat()
            out['report'] = report
    except Exception:
        pass
    return out


@app.post("/api/whale/backtest/start")
async def api_whale_backtest_start(payload: dict | None = None):
    """Запускает WHALE 30-day backtest + сравнение с COMBO/ST_VIP/TC."""
    import whale_backtest as wb
    state = wb.get_state()
    if state.get('running'):
        return {'started': False, 'state': state}
    pair_limit = int((payload or {}).get('pair_limit', 150))
    asyncio.create_task(asyncio.to_thread(wb.run_whale_backtest, pair_limit))
    return {'started': True, 'pair_limit': pair_limit}


@app.post("/api/whale/scan-now")
async def api_whale_scan_now(lookback_hours: int = 6):
    """🐋 Manual safety-net scan: проверяет всё что пропустилось real-time'ом.
    Возвращает stats о fired сигналах. Можно дёргать из UI кнопкой."""
    import whale_detector as wd
    stats = await asyncio.to_thread(
        wd.scan_recent_flips_for_whale, None, lookback_hours)
    return stats


@app.post("/api/shark/backtest/start")
async def api_shark_backtest_start(payload: dict | None = None):
    """🦈 Запускает SHARK 30-day backtest (mirror WHALE)."""
    import shark_backtest as sb
    state = sb.get_state()
    if state.get('running'):
        return {'started': False, 'state': state}
    pair_limit = int((payload or {}).get('pair_limit', 600))
    asyncio.create_task(asyncio.to_thread(sb.run_shark_backtest, pair_limit))
    return {'started': True, 'pair_limit': pair_limit}


@app.get("/api/shark/backtest/status")
async def api_shark_backtest_status():
    import shark_backtest as sb
    from database import _get_db
    state = sb.get_state()
    out = {'state': state}
    try:
        db = _get_db()
        report = db.shark_backtest_report.find_one({}, {'_id': 0})
        if report:
            for k in ['started_at', 'finished_at']:
                v = report.get(k)
                if hasattr(v, 'isoformat'):
                    report[k] = v.isoformat()
            out['report'] = report
    except Exception:
        pass
    return out


@app.post("/api/shark/backfill")
async def api_shark_backfill(days: int = 14):
    """🦈 Backfill SHARK сигналов за N дней."""
    import shark_backtest as sb
    state = sb.get_state()
    if state.get('running'):
        return {'started': False, 'state': state}
    sb.LOOKBACK_DAYS = int(days)
    sb.KLINES_15M_DAYS = int(days) + 3
    asyncio.create_task(asyncio.to_thread(sb.run_shark_backtest, 600))
    return {'started': True, 'days': days}


@app.post("/api/shark/scan-now")
async def api_shark_scan_now(lookback_hours: int = 6):
    """🦈 Manual safety-net scan."""
    import shark_detector as sd
    stats = await asyncio.to_thread(
        sd.scan_recent_flips_for_shark, None, lookback_hours)
    return stats


@app.get("/api/setup-check")
async def api_setup_check(pair: str):
    """🎰 Paste-and-evaluate setup checker.
    Возвращает ENTER_LONG / ENTER_SHORT / WAIT verdict с breakdown."""
    import setup_checker as sc
    return await asyncio.to_thread(sc.check_setup, pair)


@app.get("/api/market-bias")
async def api_market_bias(force: bool = False):
    """TOTAL2 SuperTrend → LONG/SHORT/WAIT bias для шапки журнала."""
    import market_total as mt
    return await asyncio.to_thread(mt.get_market_bias, force)


@app.post("/api/whale/backfill")
async def api_whale_backfill(days: int = 14):
    """Backfill WHALE сигналов за N дней (default 14). Журнал получит whale signals."""
    import whale_backtest as wb
    state = wb.get_state()
    if state.get('running'):
        return {'started': False, 'state': state}
    # Backfill использует тот же run_whale_backtest — он сам делает journal_inserted
    # Универс берёт ВСЕ pairs из supertrend_signals + cryptovizor
    # LOOKBACK_DAYS меняем через ENV-style — проще запустить с patched constant
    wb.LOOKBACK_DAYS = int(days)
    wb.KLINES_15M_DAYS = int(days) + 3
    asyncio.create_task(asyncio.to_thread(wb.run_whale_backtest, 600))
    return {'started': True, 'days': days}


@app.get("/api/whale/backtest/status")
async def api_whale_backtest_status():
    import whale_backtest as wb
    from database import _get_db
    state = wb.get_state()
    out = {'state': state}
    try:
        db = _get_db()
        report = db.whale_backtest_report.find_one({}, {'_id': 0})
        if report:
            for k in ['started_at', 'finished_at']:
                v = report.get(k)
                if hasattr(v, 'isoformat'):
                    report[k] = v.isoformat()
            out['report'] = report
    except Exception:
        pass
    return out


@app.post("/api/prepump/early-backtest/start")
async def api_prepump_early_backtest_start():
    """Запускает EARLY_ENTRY backtest на исторических signals (без composite)."""
    import prepump_early_entry_backtest as eb
    state = eb._get_state()
    if state.get('running'):
        return {'started': False, 'reason': 'already running', 'state': state}
    asyncio.create_task(asyncio.to_thread(eb.run_early_entry_backtest))
    return {'started': True}


@app.get("/api/prepump/early-backtest/status")
async def api_prepump_early_backtest_status():
    import prepump_early_entry_backtest as eb
    from database import _get_db
    state = eb._get_state()
    out = {'state': state}
    try:
        db = _get_db()
        report = db.prepump_early_backtest_report.find_one({}, {'_id': 0})
        if report:
            for k in ['started_at', 'finished_at']:
                v = report.get(k)
                if hasattr(v, 'isoformat'):
                    report[k] = v.isoformat()
            out['report'] = report
    except Exception:
        pass
    return out


@app.get("/api/prepump/early-entries")
async def api_prepump_early_entries(pair: str = "", hours: int = 168, limit: int = 200):
    """Early entry triggers — pair was в pre_pump WATCH+ tier И прилетел свежий signal.
    Это раннее раннее entry до того как composite PRIME сработает поздно."""
    from datetime import datetime, timezone, timedelta
    from database import _get_db
    db = _get_db()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    q = {'at': {'$gte': cutoff}}
    if pair:
        q['pair'] = pair

    def _query():
        items = []
        for d in db.pre_pump_early_entries.find(q, {'_id': 0}).sort('at', -1).limit(limit):
            if hasattr(d.get('at'), 'isoformat'):
                d['at'] = d['at'].isoformat()
            items.append(d)
        return items

    return {'items': await asyncio.to_thread(_query)}


@app.get("/api/prepump/scanner-status")
async def api_prepump_scanner_status():
    """Returns scanner state — для UI countdown до следующего скана."""
    try:
        from watcher import get_prepump_scanner_state
        return get_prepump_scanner_state()
    except Exception as e:
        return {'error': str(e)}


@app.post("/api/prepump/backtest/start")
async def api_prepump_backtest_start():
    """Запускает 30d backtest на Railway (async, в фоне).
    Возвращает task id; status через GET /api/prepump/backtest/status."""
    import prepump_backtest as bt
    state = bt._get_state()
    if state.get('running'):
        return {'started': False, 'reason': 'already running',
                'state': state}
    # Run in thread (sync function but blocking I/O)
    asyncio.create_task(asyncio.to_thread(bt.run_backtest_sync))
    return {'started': True, 'message': 'backtest запущен, проверяй через /api/prepump/backtest/status'}


@app.get("/api/prepump/backtest/status")
async def api_prepump_backtest_status():
    """Текущий прогресс backtest. Если завершён — содержит report."""
    import prepump_backtest as bt
    from database import _get_db
    state = bt._get_state()
    out = {'state': state}
    # Если есть готовый report — отдаём
    try:
        db = _get_db()
        report = db.prepump_backtest_report.find_one({}, {'_id': 0})
        if report:
            # Конвертим datetime в isoformat для JSON
            for k in ['started_at', 'finished_at']:
                v = report.get(k)
                if hasattr(v, 'isoformat'):
                    report[k] = v.isoformat()
            out['report'] = report
    except Exception:
        pass
    return out


@app.get("/api/prepump/backtest/triggers")
async def api_prepump_backtest_triggers(tier: str = "", limit: int = 100):
    """Возвращает индивидуальные backtest triggers (для UI review)."""
    from database import _get_db
    db = _get_db()
    q = {}
    if tier:
        q['tier'] = tier.upper()

    def _query():
        items = []
        for d in db.prepump_backtest_results.find(q, {'_id': 0}).sort('mfe_pct', -1).limit(limit):
            if hasattr(d.get('at'), 'isoformat'):
                d['at'] = d['at'].isoformat()
            items.append(d)
        return items

    return {'items': await asyncio.to_thread(_query)}


@app.get("/api/prepump/active-sectors")
async def api_prepump_active_sectors(hours: int = 24):
    """Возвращает активные секторы (rotation) последние N часов."""
    from datetime import datetime, timezone, timedelta
    from database import _get_db
    from collections import Counter
    db = _get_db()
    col = db.pre_pump_candidates
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    def _query():
        sector_pairs = {}
        sector_scores = {}
        for d in col.find({'detected_at': {'$gte': cutoff},
                            'sector_active': True}):
            for s in (d.get('sectors') or []):
                sector_pairs.setdefault(s, set()).add(d.get('pair', ''))
                sector_scores.setdefault(s, []).append(d.get('composite_score', 0))
        result = []
        for sect, pairs in sector_pairs.items():
            scores = sector_scores.get(sect, [])
            result.append({
                'sector': sect, 'pair_count': len(pairs),
                'pairs': sorted(pairs),
                'avg_score': sum(scores) / len(scores) if scores else 0,
            })
        result.sort(key=lambda x: (-x['pair_count'], -x['avg_score']))
        return result

    return {'sectors': await asyncio.to_thread(_query)}


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
        an_preload = []  # anomaly удалён (2026-07-02)

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
    _cap_admin_cache(_mkt_events_cache, 200)
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

    # Tradium + Cryptovizor journal блоки удалены (2026-07-01)

    # Anomalies удалены (2026-07-02)

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

    # Clusters удалены (2026-07-02)

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
            tier_emoji = {"vip": "🏆", "mtf": "🔱", "daily": "🧭"}.get(tier, "🌀")
            tier_label = {"vip": "VIP", "mtf": "Triple MTF", "daily": "Daily Filter"}.get(tier, tier.upper())
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

    # 🧊 ACCUM — разрешившиеся базы по монете (для chart markers)
    try:
        from database import _get_db as _gdb_ac2
        for a in _gdb_ac2().accum_events.find(
                {"resolved_at": {"$gte": since},
                 "$or": [{"pair": pair_slash}, {"symbol": sym_clean}]}
        ).sort("resolved_at", -1).limit(100):
            rat = a.get("resolved_at")
            arrow = "ВВЕРХ" if a.get("resolution") == "UP" else "ВНИЗ"
            _dz = a.get("delta_dz")
            _dz_txt = ""
            if _dz is not None:
                _dz_txt = (" · 🟢 покупатель" if _dz > 1 else
                           " · 🔴 продавец" if _dz < -1 else "") + f" (dz {_dz:+.1f})"
            items.append({
                "source": "accum",
                "symbol": (a.get("pair") or "").replace("/", "").upper(),
                "pair": a.get("pair", ""),
                "direction": a.get("direction", ""),
                "entry": a.get("res_price"),
                "tp1": None, "sl": None,
                "pattern": (f"🧊 База {a.get('rng_pct')}% × {a.get('hours')}ч "
                            f"→ пробой {arrow}{_dz_txt}"),
                "score": 0, "st_passed": None, "pump_score": 0,
                "is_top_pick": False, "top_pick_confirmations_count": 0,
                "at": rat.isoformat() if hasattr(rat, "isoformat") else None,
                "at_ts": int(rat.timestamp()) if hasattr(rat, "timestamp") else 0,
            })
    except Exception:
        pass

    # New Strategy Signals — whale/shark/combo/volume_surge/triple_confluence/
    # vol_accum/volcano/second_flip. Без этого блока эмодзи 🐋/🦈/🧠 etc
    # не отображались на per-coin chart журнала.
    # Projection + limit(200) для ускорения (на пары с большой историей
    # backfill могло быть >500 записей без projection — медленно).
    try:
        from database import _get_db
        nss = _get_db().new_strategy_signals
        STRAT_EMOJI = {"volume_surge": "🌊", "triple_confluence": "🐉",
                        "vol_accum": "🔋", "volcano": "🌋",
                        "second_flip": "♻️", "combo": "🧠",
                        "whale": "🐋", "shark": "🦈",
                        "impulse": "🚀", "fade": "🎣", "ignition": "💥",
                        "rider_short": "🏄", "ten": "💰", "delta_series": "🫧"}
        STRAT_LABEL = {"volume_surge": "Volume Surge",
                       "triple_confluence": "Triple Confluence",
                       "vol_accum": "Vol Accum", "volcano": "Volcano",
                       "second_flip": "Second Flip", "combo": "COMBO",
                       "whale": "WHALE", "shark": "SHARK",
                       "impulse": "IMPULSE", "fade": "FADE", "ignition": "IGNITION",
                       "rider_short": "RIDER SHORT", "ten": "TEN",
                       "delta_series": "Серия дельт"}
        for n in nss.find({"created_at": {"$gte": since}, **pair_or}, {
            "strategy": 1, "pair": 1, "direction": 1, "entry": 1,
            "tp": 1, "sl": 1, "created_at": 1, "state": 1,
            "whale_tier": 1, "whale_score": 1, "whale_indicators": 1,
            "shark_tier": 1, "shark_score": 1, "shark_indicators": 1,
            "combo_score": 1, "vol_ratio": 1, "source_count": 1,
            "setup_verdict": 1,  # server-side verdict from setup_checker
        }).sort("created_at", -1).limit(200):
            at_dt = n.get("created_at")
            strat = n.get("strategy", "?")
            em = STRAT_EMOJI.get(strat, "✨")
            label = STRAT_LABEL.get(strat, strat)
            # Strategy-specific extras для tooltip pattern_txt
            extra_parts = []
            if strat == "whale":
                if n.get("whale_tier"): extra_parts.append(f"tier {n['whale_tier']}")
                if n.get("whale_score"): extra_parts.append(f"score {n['whale_score']}")
            elif strat == "shark":
                if n.get("shark_tier"): extra_parts.append(f"tier {n['shark_tier']}")
                if n.get("shark_score"): extra_parts.append(f"score {n['shark_score']}")
                ind = n.get('shark_indicators') or {}
                if ind.get('multi_top_count'):
                    extra_parts.append(f"multi-top×{ind['multi_top_count']}")
            elif strat == "combo" and n.get('combo_score'):
                extra_parts.append(f"score {n['combo_score']}")
            elif strat == "volume_surge" and n.get('vol_ratio'):
                extra_parts.append(f"vol {n['vol_ratio']}×")
            pattern_txt = f"{em} {label}"
            if extra_parts:
                pattern_txt += " · " + " · ".join(extra_parts)
            if at_dt and hasattr(at_dt, "isoformat"):
                at_iso = at_dt.isoformat()
                at_ts = int(at_dt.timestamp())
            else:
                at_iso = None; at_ts = 0
            items.append({
                "source": strat,  # 'whale' / 'shark' / 'combo' / ...
                "symbol": (n.get("pair") or "").replace("/", "").upper(),
                "pair": n.get("pair", ""),
                "direction": n.get("direction", ""),
                "entry": n.get("entry"),
                "tp1": n.get("tp"),
                "sl": n.get("sl"),
                "pattern": pattern_txt,
                "score": n.get("source_count") or n.get("vol_ratio") or 0,
                "st_passed": None,
                "pump_score": 0,
                "is_top_pick": False,
                "top_pick_confirmations_count": 0,
                "ns_strategy": strat,
                "ns_state": n.get("state", "WAITING"),
                "whale_tier": n.get("whale_tier"),
                "whale_score": n.get("whale_score"),
                "shark_tier": n.get("shark_tier"),
                "shark_score": n.get("shark_score"),
                "at": at_iso,
                "at_ts": at_ts,
            })
    except Exception:
        pass

    # BIG BUY блок удалён вместе с Cryptovizor ingestion (2026-07-01)

    items.sort(key=lambda x: x.get("at_ts", 0), reverse=True)

    # ✨ Verified Entries per-coin (для chart markers; было только в главном журнале)
    try:
        from database import _get_db as _gdb_v
        vcol = _gdb_v().verified_signals
        for v in vcol.find({"created_at": {"$gte": since},
                            "$or": [{"pair": pair_slash}, {"pair_norm": sym_clean}],
                            }).sort("created_at", -1).limit(200):
            at_dt = v.get("created_at")
            verdict = v.get("verdict", "go")
            emoji = "⚠️✨" if verdict == "caution" else "✨"
            items.append({
                "source": "verified",
                "symbol": v.get("pair_norm") or sym_clean,
                "pair": v.get("pair") or pair_slash,
                "direction": v.get("direction", ""),
                "entry": v.get("entry"),
                "tp1": v.get("tp1"), "sl": v.get("sl"),
                "pattern": f"{emoji} VERIFIED ({v.get('signal_source') or '?'})",
                "score": v.get("signal_score"),
                "st_passed": None, "pump_score": 0,
                "is_top_pick": False, "top_pick_confirmations_count": 0,
                "verified_verdict": verdict,
                "at": at_dt.isoformat() if hasattr(at_dt, "isoformat") else None,
                "at_ts": int(at_dt.timestamp()) if hasattr(at_dt, "timestamp") else 0,
            })
    except Exception:
        pass

    # 🧩 Семейная группировка для chart markers — один 🧩 вместо колонны
    try:
        from signal_families import collapse_stacks
        items = collapse_stacks(items)
    except Exception:
        pass

    # 🎰 Inject setup_verdict из pair_verdicts (single pair lookup)
    try:
        pv = _get_db().pair_verdicts.find_one({'pair': pair_slash}, {'verdict': 1})
        if pv and pv.get('verdict'):
            v = pv['verdict']
            for it in items:
                if not it.get('setup_verdict'):
                    it['setup_verdict'] = v
    except Exception:
        pass

    # Inject q_score (HOT marker нужен на графиках по конкретной паре)
    try:
        from quality_score import compute_signal_score
        from collections import defaultdict
        ns_count: dict = defaultdict(int)
        ns_sources = ('volume_surge', 'triple_confluence', 'vol_accum',
                      'volcano', 'second_flip')
        for it in items:
            if it.get('source') in ns_sources:
                bucket = (it.get('at_ts') or 0) // 1800
                key = (it.get('symbol') or '', it.get('direction') or '', bucket)
                ns_count[key] += 1
        for it in items:
            ctx = {}
            if it.get('source') in ns_sources:
                bucket = (it.get('at_ts') or 0) // 1800
                key = (it.get('symbol') or '', it.get('direction') or '', bucket)
                ctx['strategy_count'] = ns_count.get(key, 1)
            if it.get('st_tier'):
                ctx['tier'] = it['st_tier']
            try:
                it['q_score'] = compute_signal_score(it, ctx)
            except Exception:
                it['q_score'] = 0
    except Exception:
        pass

    return {"items": items, "symbol": sym_clean, "days": days, "count": len(items)}


def _compute_journal_fast_sync():
    """⚡ FAST journal compute — только Mongo fetches + stack count enrichment.
    Без divergence/squeeze/rsi4h/rsi12h/top_movers/cluster_delta/RSI/trend enrichments.

    Используется в /api/journal/fast endpoint для мгновенного рендера UI (~2-3с).
    UI делает второй параллельный запрос на /api/journal (full) и мерджит
    enrichments на лету.

    Reuses _compute_journal_sync() но обрезает на этапе stack_count enrichment.
    """
    full = _compute_journal_sync(_fast_only=True)
    return full


def _compute_journal_sync(_fast_only: bool = False):
    """Синхронная версия — вызывается через asyncio.to_thread из api_journal,
    чтоб блокирующие Mongo-курсоры не тормозили весь event loop.

    _fast_only=True: выход после Mongo fetches + sort + stack_count enrich
    (используется в /api/journal/fast endpoint для UI первичной отрисовки)."""
    from database import _signals, _anomalies, _confluence
    from datetime import datetime, timedelta
    from database import utcnow as _utcnow
    since_14d = _utcnow() - timedelta(days=14)

    items = []

    # Tradium + Cryptovizor journal блоки удалены (2026-07-01)

    # Anomalies удалены (2026-07-02)

    # Confluence (14 дней, cap 1500 — их ~1500 за 14 дней)
    for c in _confluence().find({"detected_at": {"$gte": since_14d}}, {
        "symbol":1, "pair":1, "direction":1, "price":1, "r1":1, "s1":1,
        "pattern":1, "strength":1, "factors":1, "score":1,
        "st_passed":1, "pump_score":1, "is_top_pick":1,
        "top_pick_confirmations_count":1, "detected_at":1,
    }).sort("detected_at", -1).limit(3000):
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

    # Clusters удалены (2026-07-02)

    # SuperTrend signals (14 дней) — источник 'supertrend' с tier в pattern
    # Включаем все 3 tier (vip, mtf, daily) — юзер хочет видеть Daily 🧭 в журнале.
    #
    # Дедупликация: в окне 30 мин на одной паре+direction оставляем только
    # один сигнал с высшим tier (vip > mtf > daily). При совпадении VIP и
    # Daily на одной паре в журнале остаётся VIP.
    import calendar as _cal
    try:
        from database import _supertrend_signals as _sts
        # Собираем все raw записи (увеличили limit под 3 tier)
        raw_st = list(_sts().find({
            "flip_at": {"$gte": since_14d},
            "tier": {"$in": ["vip", "mtf", "daily"]},
        }).sort("flip_at", -1).limit(6000))
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
            tier_emoji = {"vip": "🏆", "mtf": "🔱", "daily": "🧭"}.get(tier, "🌀")
            tier_label = {"vip": "VIP", "mtf": "Triple MTF", "daily": "Daily Filter"}.get(tier, tier.upper())
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

    # cv_flip journal блок удалён вместе с CV ingestion (2026-07-01)

    # ✨ Verified Entries (авто-проверка Entry Checker — отправлено в @topmonetabot)
    try:
        from database import _get_db
        vcol = _get_db().verified_signals
        for v in vcol.find({"created_at": {"$gte": since_14d}}).sort("created_at", -1).limit(1500):
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

    # 🧊 ACCUM — разрешившиеся базы (инфо-событие: база → ВВЕРХ/ВНИЗ).
    # Живые базы висят в панели журнала, сюда попадают только пробои.
    try:
        from database import _get_db as _gdb_ac, utcnow as _ac_now
        from datetime import timedelta as _ac_td
        for a in _gdb_ac().accum_events.find(
                {"resolved_at": {"$gte": _ac_now() - _ac_td(hours=336)}}
        ).sort("resolved_at", -1).limit(500):
            rat = a.get("resolved_at")
            arrow = "ВВЕРХ" if a.get("resolution") == "UP" else "ВНИЗ"
            _dz = a.get("delta_dz")
            _dz_txt = ""
            if _dz is not None:
                _dz_txt = (" · 🟢 покупатель" if _dz > 1 else
                           " · 🔴 продавец" if _dz < -1 else "") + f" (dz {_dz:+.1f})"
            items.append({
                "source": "accum",
                "symbol": (a.get("pair") or "").replace("/", "").upper(),
                "pair": a.get("pair", ""),
                "direction": a.get("direction", ""),
                "entry": a.get("res_price"),
                "tp1": None, "sl": None,
                "pattern": (f"🧊 База {a.get('rng_pct')}% × {a.get('hours')}ч "
                            f"→ пробой {arrow}{_dz_txt}"),
                "score": 0, "st_passed": None, "pump_score": 0,
                "is_top_pick": False, "top_pick_confirmations_count": 0,
                "at": rat.isoformat() if hasattr(rat, "isoformat") else None,
                "at_ts": int(rat.timestamp()) if hasattr(rat, "timestamp") else 0,
            })
    except Exception as e:
        logging.getLogger(__name__).warning(f"[journal] accum fetch fail: {e}")

    # 🌊🐉🔋 New Strategies — последние сигналы 3 backtest-validated стратегий
    try:
        from database import _get_db, utcnow as _ns_utcnow
        from datetime import timedelta as _td
        nss_col = _get_db().new_strategy_signals
        nss_since = _ns_utcnow() - _td(hours=336)  # last 14d (раньше 7d)
        STRAT_EMOJI = {"volume_surge": "🌊", "triple_confluence": "🐉",
                        "vol_accum": "🔋", "volcano": "🌋",
                        "second_flip": "♻️", "combo": "🧠", "whale": "🐋",
                        "shark": "🦈", "impulse": "🚀", "fade": "🎣", "ignition": "💥",
                        "rider_short": "🏄", "ten": "💰", "delta_series": "🫧"}
        STRAT_LABEL = {"volume_surge": "Volume Surge", "triple_confluence": "Triple Confluence",
                       "vol_accum": "Vol Accum", "volcano": "Volcano Breakout",
                       "second_flip": "Second Flip", "combo": "COMBO",
                       "whale": "WHALE", "shark": "SHARK",
                       "impulse": "IMPULSE", "fade": "FADE", "ignition": "IGNITION",
                       "rider_short": "RIDER SHORT", "ten": "TEN",
                       "delta_series": "Серия дельт"}
        for n in nss_col.find({"created_at": {"$gte": nss_since}}).sort("created_at", -1).limit(2000):
            at_dt = n.get("created_at")
            strat = n.get("strategy", "?")
            em = STRAT_EMOJI.get(strat, "✨")
            label = STRAT_LABEL.get(strat, strat)
            pair_raw = n.get("pair") or ""
            pair_norm = pair_raw.replace("/", "").upper()
            extra = ""
            if strat == "delta_series":
                _di = n.get("indicators") or {}
                extra = (f" · Σ {_di.get('sigma', '?')}σ · vol {_di.get('vol_ratio', '?')}×"
                         f" · инфо (направление 50/50 по бэктесту)")
            elif strat == "volume_surge" and n.get("vol_ratio"):
                extra = f" · vol {n['vol_ratio']}×"
            elif strat == "triple_confluence" and n.get("source_count"):
                extra = f" · {n['source_count']}src"
            elif strat == "vol_accum":
                extra = " · 3 bars rising"
            elif strat == "volcano":
                # Highest-edge strategy: WR 38%, AvgRet +2.15% (winners bt)
                parts = []
                if n.get("vol_ratio"): parts.append(f"vol {n['vol_ratio']}×")
                if n.get("body_atr"): parts.append(f"body {n['body_atr']}×ATR")
                if n.get("rsi") is not None: parts.append(f"RSI {n['rsi']}")
                extra = " · " + " · ".join(parts) if parts else ""
            elif strat == "combo":
                # COMBO: composite score из preconditions analysis 14d backtest
                # Win markers: st_vip, triple_confluence, confluence
                # Anti markers: st_mtf, cv_flip, cryptovizor, vol_accum, st_daily
                parts = []
                sc = n.get('combo_score')
                if sc is not None: parts.append(f"score {sc}")
                trig = n.get('trigger_source')
                if trig: parts.append(f"via {trig}")
                ps = n.get('preceding_sources') or []
                if ps: parts.append(f"prec: {','.join(ps[:4])}")
                extra = " · " + " · ".join(parts) if parts else ""
            elif strat == "second_flip":
                # Confirmation flip: WR 28%, AvgRet +0.83% (45%/+1.83% strict)
                parts = []
                if n.get("gap_h"): parts.append(f"gap {n['gap_h']}h")
                if n.get("strict_pattern"):
                    parts.append("strict L→S→L")
                else:
                    parts.append("consecutive")
                extra = " · " + " · ".join(parts) if parts else ""
            elif strat == "whale":
                # WHALE: backtest 30d STANDARD WR 53.8% MFE 5.56% — top LONG signal
                parts = []
                tier = n.get('whale_tier')
                if tier: parts.append(f"tier {tier}")
                sc = n.get('whale_score')
                if sc is not None: parts.append(f"score {sc}")
                ind = n.get('whale_indicators') or {}
                if ind.get('base_days'): parts.append(f"base {ind['base_days']}d")
                if ind.get('vol_ratio_max'): parts.append(f"vol {ind['vol_ratio_max']}×")
                if ind.get('prior_downtrend_pct'):
                    parts.append(f"DT {ind['prior_downtrend_pct']}%")
                if ind.get('capitulation_wick'): parts.append("cap_wick")
                if ind.get('rsi_cross'): parts.append("rsi×")
                extra = " · " + " · ".join(parts) if parts else ""
            elif strat == "shark":
                # SHARK: backtest 30d STANDARD WR 60.4% MFE 4.96% — top SHORT signal
                parts = []
                tier = n.get('shark_tier')
                if tier: parts.append(f"tier {tier}")
                sc = n.get('shark_score')
                if sc is not None: parts.append(f"score {sc}")
                ind = n.get('shark_indicators') or {}
                if ind.get('multi_top_count'):
                    parts.append(f"multi-top×{ind['multi_top_count']}")
                if ind.get('distribution_days'):
                    parts.append(f"distrib {ind['distribution_days']}d")
                if ind.get('vol_ratio_max'):
                    parts.append(f"vol {ind['vol_ratio_max']}×")
                if ind.get('prior_uptrend_pct'):
                    parts.append(f"UT {ind['prior_uptrend_pct']}%")
                if ind.get('blow_off_max_move_pct', 0) >= 20:
                    parts.append(f"blow-off {ind['blow_off_max_move_pct']}%")
                if ind.get('failed_breakout_wick'): parts.append("upper_wick")
                if ind.get('rsi_cross_below_sma'): parts.append("rsi×")
                extra = " · " + " · ".join(parts) if parts else ""
            pattern_txt = f"{em} {label}{extra}"
            items.append({
                "source": strat,  # 'volume_surge' / 'triple_confluence' / 'vol_accum'
                "symbol": pair_norm,
                "pair": pair_raw,
                "direction": n.get("direction", ""),
                "entry": n.get("entry"),
                "tp1": n.get("tp"),
                "sl": n.get("sl"),
                "pattern": pattern_txt,
                "score": n.get("source_count") or n.get("vol_ratio") or 0,
                "st_passed": None,
                "pump_score": 0,
                "is_top_pick": False,
                "top_pick_confirmations_count": 0,
                "ns_strategy": strat,
                "ns_state": n.get("state", "WAITING"),
                "ns_vol_ratio": n.get("vol_ratio"),
                "ns_sources": n.get("sources"),
                # WHALE tier (используется в TOP-7 filter в журнале)
                "whale_tier": n.get("whale_tier"),
                "whale_score": n.get("whale_score"),
                # SHARK tier (тот же mechanism — для filtering и UI tooltip)
                "shark_tier": n.get("shark_tier"),
                "shark_score": n.get("shark_score"),
                # 🎰 Setup Checker verdict (server-side, заполняется background
                # loop'ом в watcher через get_compact_verdict). Если есть —
                # client JS использует его эмодзи/tier напрямую.
                "setup_verdict": n.get("setup_verdict"),
                "tp_R": n.get("tp_R"),
                "rr": n.get("tp_R"),
                "at": at_dt.isoformat() if hasattr(at_dt, "isoformat") else str(at_dt or ""),
                "at_ts": int(at_dt.timestamp()) if hasattr(at_dt, "timestamp") else 0,
                # Cluster Delta + Resonance (информативно, не влияет на сигналы)
                "delta_15m": n.get("delta_15m"),
                "delta_1h":  n.get("delta_1h"),
                "resonance_15m": n.get("resonance_15m"),
                "resonance_1h":  n.get("resonance_1h"),
            })
    except Exception as e:
        logging.getLogger(__name__).warning(f"[journal] new_strategies fetch fail: {e}")

    # ─── RSI/SMA crossover 12h signals ───
    try:
        from database import _get_db
        col = _get_db().rsi_sma_cross_signals
        for rs in col.find({'detected_at': {'$gte': since_14d}},
                           sort=[('detected_at', -1)], limit=500):
            pair = rs.get('pair', '')
            at_dt = rs.get('detected_at')
            items.append({
                'source': 'rsi_cross_12h',
                'symbol': rs.get('symbol', pair.replace('/', '').upper()),
                'pair': pair,
                'direction': rs.get('direction', 'LONG'),
                'entry': rs.get('entry'),
                'tp1': None, 'sl': None,
                'pattern': (f"{rs.get('cross_type','?')} cross "
                            f"rsi={rs.get('rsi','?')} vol={rs.get('vol_ratio','?')}×"),
                'score': None,
                'is_top_pick': False,
                'rsi_at_cross': rs.get('rsi'),
                'vol_ratio_at_cross': rs.get('vol_ratio'),
                'vol_24h_usdt': rs.get('vol_24h_usdt'),
                'at': at_dt.isoformat() if hasattr(at_dt, 'isoformat') else str(at_dt or ''),
                'at_ts': int(at_dt.timestamp()) if hasattr(at_dt, 'timestamp') else 0,
            })
    except Exception as e:
        logging.getLogger(__name__).warning(f"[journal] rsi_cross fetch fail: {e}")

    # BIG BUY блок удалён вместе с Cryptovizor ingestion (2026-07-01)

    # V-Bottom signals — DISABLED по запросу юзера (15.05.26).
    # Сигналы не показываются в журнале. Collection v_bottom_signals
    # остаётся в БД но не отображается.

    # Сортируем по дате (новые сверху)
    items.sort(key=lambda x: x.get("at_ts", 0), reverse=True)

    # ─── 🎰 Inject setup_verdict из pair_verdicts collection ───
    # Single Mongo query на все уникальные pairs items, batch lookup.
    try:
        from database import _get_db as _g_db
        _db = _g_db()
        unique_pairs = list(set(it.get('pair') for it in items if it.get('pair')))
        pv_map = {}
        for pv in _db.pair_verdicts.find(
            {'pair': {'$in': unique_pairs}},
            {'pair': 1, 'verdict': 1}
        ):
            pv_map[pv['pair']] = pv.get('verdict')
        for it in items:
            v = pv_map.get(it.get('pair'))
            if v and not it.get('setup_verdict'):
                it['setup_verdict'] = v
    except Exception as _e:
        logging.getLogger(__name__).debug(f'[journal] pair_verdicts join fail: {_e}')

    # ─── Stack count enrichment (MOONSHOT detection) ───
    # Для каждого item считаем distinct (source, direction) на той же паре
    # в окне ±3h (6h всего) — мера accumulation density.
    # Высокий stack (4+) = multiple sources align = pre-pump pattern.
    try:
        import bisect
        from collections import defaultdict
        STACK_WINDOW_S = 3 * 3600  # ±3h = 6h window
        pair_sigs: dict = defaultdict(list)  # pair -> [(at_ts, source, direction), ...]
        for it in items:
            p = it.get('pair') or ''
            ts = it.get('at_ts') or 0
            if p and ts:
                pair_sigs[p].append((ts, it.get('source', '?'),
                                     (it.get('direction', '') or '').upper()))
        # Sort each pair's signals by ts ascending (для bisect)
        for p in pair_sigs:
            pair_sigs[p].sort(key=lambda x: x[0])
        # Per-item stack_count + stack_distinct + stack_long/short split
        for it in items:
            p = it.get('pair') or ''
            ts = it.get('at_ts') or 0
            if not (p and ts):
                it['stack_count'] = 0
                it['stack_distinct'] = 0
                continue
            sigs = pair_sigs[p]
            times = [s[0] for s in sigs]
            lo = bisect.bisect_left(times, ts - STACK_WINDOW_S)
            hi = bisect.bisect_right(times, ts + STACK_WINDOW_S)
            window = sigs[lo:hi]
            it['stack_count'] = len(window)
            # 🧩 distinct = НЕЗАВИСИМЫЕ СЕМЕЙСТВА × направление (было source ×
            # direction — ST-эхо надувало счёт: один флип = 5-6 "источников").
            from signal_families import family_of as _fam
            it['stack_distinct'] = len(set((_fam(s[1]), s[2]) for s in window))
            it['stack_long']  = sum(1 for s in window if s[2] == 'LONG')
            it['stack_short'] = sum(1 for s in window if s[2] == 'SHORT')
    except Exception as _se:
        logging.getLogger(__name__).warning(f"[journal] stack_count enrich fail: {_se}")

    # 🧩 Семейная группировка: цепочки эхо-сигналов (пара+направление,
    # разрыв <=30 мин) схлопываются в одну строку source='stack'.
    # По анализу 56д: один ST-флип порождал 4-6 строк — теперь одна.
    try:
        from signal_families import collapse_stacks
        items = collapse_stacks(items)
    except Exception as _cse:
        logging.getLogger(__name__).warning(f"[journal] collapse fail: {_cse}")

    # ⚡ FAST EXIT: для /api/journal/fast endpoint — UI получает данные мгновенно,
    # все enrichments ниже (squeeze/divergence/rsi4h/rsi12h/top_movers/delta/rsi/trend)
    # выполняются только в полном compute через /api/journal.
    if _fast_only:
        return {"items": items}

    # ─── Squeeze score для high-stack pairs ───
    # BB(20)/SMA(60) compression — индикатор близкого breakout.
    # Считаем ТОЛЬКО для pairs с stack_count >= 3 (moonshot candidates) — ~20-50 пар.
    # Cache в _squeeze_cache (TTL 10min) — не recomputим каждый journal load.
    try:
        global _squeeze_cache
    except NameError:
        _squeeze_cache = {}
    try:
        from concurrent.futures import ThreadPoolExecutor as _SQExec, as_completed as _sq_as_completed
        from delta_calculator import compute_squeeze_score
        import time as _t_sq
        if '_squeeze_cache' not in globals():
            globals()['_squeeze_cache'] = {}
        sq_cache = globals()['_squeeze_cache']
        sq_now = int(_t_sq.time())
        SQ_TTL = 600  # 10min
        # Уникальные high-stack pairs
        moonshot_pairs = set()
        for it in items:
            if (it.get('stack_distinct') or 0) >= 3:
                p = it.get('pair') or ''
                if p:
                    moonshot_pairs.add(p)
        # Filter pairs needing compute (no cache OR cache expired)
        need_compute = [p for p in moonshot_pairs
                        if sq_now - sq_cache.get(p, {}).get('ts', 0) > SQ_TTL]
        need_compute = need_compute[:40]  # cap чтобы не блокировать journal надолго
        if need_compute:
            sq_ex = _SQExec(max_workers=8)
            try:
                futs = {sq_ex.submit(compute_squeeze_score, p, '1h', 80): p
                        for p in need_compute}
                try:
                    for f in _sq_as_completed(futs, timeout=8.0):
                        p = futs[f]
                        try:
                            score = f.result(timeout=0.1)
                            sq_cache[p] = {'ts': sq_now, 'score': score}
                        except Exception:
                            sq_cache[p] = {'ts': sq_now, 'score': None}
                except Exception:
                    pass
            finally:
                sq_ex.shutdown(wait=False, cancel_futures=True)
        # Apply squeeze_score to ALL items (для top-moonshot pairs)
        for it in items:
            p = it.get('pair') or ''
            entry = sq_cache.get(p) if p else None
            it['squeeze_score'] = entry.get('score') if entry else None
    except Exception as _sqe:
        logging.getLogger(__name__).debug(f"[journal] squeeze fail: {_sqe}")
        for it in items:
            it.setdefault('squeeze_score', None)

    # ─── MOONSHOT alert dispatch ───
    # Track pairs which JUST crossed stack >= 4 threshold (first detection).
    # Send Telegram alert (BOT12) once per pair per 1h window.
    try:
        from database import _get_db as _gdb_m
        import time as _t_m
        db_m = _gdb_m()
        moon_col = db_m.moonshot_alerts
        try:
            moon_col.create_index('pair', unique=False)
            moon_col.create_index([('at', -1)])
        except Exception:
            pass
        now_m = int(_t_m.time())
        # Top 1 most recent item per pair (для detection)
        pair_latest: dict = {}
        for it in items:
            p = it.get('pair') or ''
            d = it.get('stack_distinct') or 0
            # 🧩 порог 3 независимых СЕМЕЙСТВА (после family-дедупа 4 старых
            # source-голоса ≈ 2 семейства; 3 fam = настоящий multi-confirm)
            if d < 3 or not p: continue
            ts = it.get('at_ts') or 0
            cur = pair_latest.get(p)
            if not cur or ts > cur.get('at_ts', 0):
                pair_latest[p] = it
        # Filter: pair NOT alerted in last 1h
        for p, it in pair_latest.items():
            try:
                last_alert = moon_col.find_one({'pair': p}, sort=[('at', -1)])
                if last_alert and (now_m - int(last_alert.get('at_ts', 0)) < 3600):
                    continue  # уже алертили <1h назад
                # Mark as alerted (insert)
                from datetime import timezone as _tz_moon
                moon_col.insert_one({
                    'pair': p,
                    'at': datetime.now(_tz_moon.utc),
                    'at_ts': now_m,
                    'stack_distinct': it.get('stack_distinct'),
                    'stack_count': it.get('stack_count'),
                    'long_count': it.get('stack_long'),
                    'short_count': it.get('stack_short'),
                    'last_source': it.get('source'),
                    'last_direction': it.get('direction'),
                    'squeeze_score': it.get('squeeze_score'),
                    'change_24h': it.get('change_24h'),
                    'is_top_mover': bool(it.get('is_top_mover')),
                })
                # Send Telegram alert (BOT12) — fire-and-forget
                try:
                    from config import BOT12_BOT_TOKEN, ADMIN_CHAT_ID
                    if BOT12_BOT_TOKEN and ADMIN_CHAT_ID:
                        import httpx as _hx
                        dist = it.get('stack_distinct', 0)
                        emoji = '💎' if dist >= 6 else '🚀'
                        long_n = it.get('stack_long', 0)
                        short_n = it.get('stack_short', 0)
                        sq = it.get('squeeze_score')
                        ch = it.get('change_24h')
                        text = (
                            f"{emoji} <b>MOONSHOT: {p}</b>\n\n"
                            f"📊 Stack: <b>{dist}</b> distinct sources × direction "
                            f"(<b>{it.get('stack_count',0)}</b> total signals в окне ±3h)\n"
                            f"📈 Bias: <span>{long_n}× LONG</span> · <span>{short_n}× SHORT</span>\n"
                        )
                        if sq is not None:
                            sq_emoji = '🔥' if sq >= 80 else '⚠' if sq >= 60 else ''
                            text += f"💨 BB Squeeze: <b>{sq}/100</b> {sq_emoji}\n"
                        if ch is not None:
                            ch_emoji = '🔥' if ch > 15 else '📈' if ch > 5 else ''
                            text += f"⏱ 24h change: <b>{ch:+.1f}%</b> {ch_emoji}\n"
                        text += (
                            f"\nLast: <b>{it.get('source','?')}</b> {it.get('direction','?')} "
                            f"@ {it.get('entry','?')}\n"
                            f"<i>Multiple sources align → pre-pump signature</i>"
                        )
                        _hx.post(
                            f'https://api.telegram.org/bot{BOT12_BOT_TOKEN}/sendMessage',
                            json={'chat_id': int(ADMIN_CHAT_ID), 'text': text,
                                  'parse_mode': 'HTML', 'disable_web_page_preview': True},
                            timeout=5.0,
                        )
                except Exception as _bt:
                    logging.getLogger(__name__).debug(f"[moonshot] BOT12 send fail {p}: {_bt}")
            except Exception:
                pass
    except Exception as _me:
        logging.getLogger(__name__).debug(f"[moonshot] dispatch fail: {_me}")

    # ─── Divergence + V-pattern + Confluence Score enrichment ───
    # Для recent items (last 12h) computе RSI divergence + V-pattern,
    # потом combine с уже-computed stack/sma/anomaly через confluence_score.
    # Cache divergence per (pair, hour_bucket) — TTL 10min, не пересчитываем
    # каждый journal load одни и те же swings.
    try:
        global _div_cache, _vp_cache
    except NameError:
        _div_cache = {}
        _vp_cache = {}
    try:
        import time as _t_cs
        if '_div_cache' not in globals(): globals()['_div_cache'] = {}
        if '_vp_cache' not in globals(): globals()['_vp_cache'] = {}
        div_cache = globals()['_div_cache']
        vp_cache  = globals()['_vp_cache']
        cs_now = int(_t_cs.time())
        CS_TTL = 600  # 10min
        from divergence import detect_divergence
        from v_pattern import detect_v_pattern
        from confluence_score import compute_score
        # Recent items only (last 12h) → limit compute load
        recent_cutoff = cs_now - 12 * 3600
        recent_pairs: set = set()
        for it in items:
            if (it.get('at_ts') or 0) >= recent_cutoff:
                p = it.get('pair') or ''
                if p:
                    recent_pairs.add(p)
        # Compute divergence + v_pattern for unique pairs (cap 30 per render)
        need_compute = [p for p in recent_pairs
                        if cs_now - div_cache.get(p, {}).get('ts', 0) > CS_TTL]
        need_compute = need_compute[:30]
        if need_compute:
            from concurrent.futures import ThreadPoolExecutor as _CSExec, as_completed as _cs_done
            def _compute_one(p):
                try:
                    div = detect_divergence(p, '1h', 100)
                    vp = detect_v_pattern(p, '15m', 50, 3.0)
                    return (p, div, vp)
                except Exception:
                    return (p, None, None)
            cs_ex = _CSExec(max_workers=8)
            try:
                futs = {cs_ex.submit(_compute_one, p): p for p in need_compute}
                try:
                    for f in _cs_done(futs, timeout=12.0):
                        try:
                            p, d, v = f.result(timeout=0.2)
                            div_cache[p] = {'ts': cs_now, 'val': d}
                            vp_cache[p]  = {'ts': cs_now, 'val': v}
                        except Exception:
                            pass
                except Exception:
                    pass
            finally:
                cs_ex.shutdown(wait=False, cancel_futures=True)
        # Apply divergence/vp/score per item
        for it in items:
            p = it.get('pair') or ''
            if p:
                d_entry = div_cache.get(p)
                v_entry = vp_cache.get(p)
                it['divergence'] = (d_entry.get('val') if d_entry else None)
                it['v_pattern'] = (v_entry.get('val') if v_entry else None)
            # Compute confluence score using already-enriched fields
            try:
                cs = compute_score(it)
                it['confluence_score'] = cs['score']
                it['confluence_direction'] = cs['direction']
                it['confluence_components'] = cs['components']
            except Exception:
                it['confluence_score'] = 0
                it['confluence_direction'] = None
                it['confluence_components'] = {}
    except Exception as _cse:
        logging.getLogger(__name__).debug(f"[journal] confluence enrich fail: {_cse}")

    # ─── RSI/SMA 4h STATE filter enrichment ───
    # Parallel to 12h logic. Shorter TF = faster reaction signal.
    try:
        from concurrent.futures import ThreadPoolExecutor as _4hExec, as_completed as _4h_done
        import time as _t_r4
        import rsi4h_state as _r4_mod
        rc_now_4 = int(_t_r4.time())
        r4_t0 = _t_r4.time()
        R4_BUDGET_S = 12.0
        recent_cutoff_4 = rc_now_4 - 24 * 3600
        recent_pairs_4: set = set()
        for it in items:
            if (it.get('at_ts') or 0) >= recent_cutoff_4:
                p = it.get('pair') or ''
                if p: recent_pairs_4.add(p)
        cold_pairs_4 = [p for p in recent_pairs_4
                        if not _r4_mod._cache.get(p)
                        or (rc_now_4 - _r4_mod._cache[p].get('ts', 0))
                           > (1800 if _r4_mod._cache[p].get('state') else 300)]
        cold_pairs_4 = cold_pairs_4[:80]
        if cold_pairs_4 and (_t_r4.time() - r4_t0) < R4_BUDGET_S:
            ex_r4 = _4hExec(max_workers=10)
            try:
                remaining = max(2.0, R4_BUDGET_S - (_t_r4.time() - r4_t0))
                futs = {ex_r4.submit(_r4_mod.get_state, p): p for p in cold_pairs_4}
                try:
                    for f in _4h_done(futs, timeout=remaining):
                        try: f.result(timeout=0.2)
                        except Exception: pass
                except Exception: pass
            finally:
                ex_r4.shutdown(wait=False, cancel_futures=True)
        for it in items:
            p = it.get('pair') or ''
            if not p:
                it['rsi4h_state'] = None; it['rsi4h_match'] = None
                continue
            entry = _r4_mod._cache.get(p)
            if not entry:
                it['rsi4h_state'] = None; it['rsi4h_match'] = None
                continue
            state = entry.get('state')
            it['rsi4h_state'] = state
            it['rsi4h_value'] = entry.get('rsi')
            it['rsi4h_sma'] = entry.get('sma')
            d = (it.get('direction') or '').upper()
            if state == 'bullish' and d == 'LONG': it['rsi4h_match'] = True
            elif state == 'bearish' and d == 'SHORT': it['rsi4h_match'] = True
            elif state is None: it['rsi4h_match'] = None
            else: it['rsi4h_match'] = False
    except Exception as _r4e:
        logging.getLogger(__name__).debug(f"[journal] rsi4h enrich fail: {_r4e}")

    # ─── RSI/SMA 12h STATE filter enrichment ───
    # Используем shared module rsi12h_state — single cache, без duplicate code.
    # paper_trader.on_signal тоже использует same cache → consistency.
    # HARD TIME BUDGET: total enrichment must finish < 12s (от 40s journal budget).
    try:
        from concurrent.futures import ThreadPoolExecutor as _RsiExec, as_completed as _rsi_done
        import time as _t_r12
        import rsi12h_state as _r12_mod
        rc_now = int(_t_r12.time())
        r12_t0 = _t_r12.time()
        R12_BUDGET_S = 15.0  # hard cap
        # Get unique pairs for recent items (last 24h только — экономим compute)
        recent_cutoff = rc_now - 24 * 3600
        recent_pairs: set = set()
        for it in items:
            if (it.get('at_ts') or 0) >= recent_cutoff:
                p = it.get('pair') or ''
                if p: recent_pairs.add(p)
        # Cold pairs (no cache) — pre-warm cap 20
        cold_pairs = [p for p in recent_pairs
                      if not _r12_mod._cache.get(p)
                      or (rc_now - _r12_mod._cache[p].get('ts', 0))
                         > (3600 if _r12_mod._cache[p].get('state') else 300)]
        cold_pairs = cold_pairs[:80]
        if cold_pairs and (_t_r12.time() - r12_t0) < R12_BUDGET_S:
            ex_rc = _RsiExec(max_workers=10)
            try:
                remaining = max(2.0, R12_BUDGET_S - (_t_r12.time() - r12_t0))
                futs = {ex_rc.submit(_r12_mod.get_state, p): p for p in cold_pairs}
                try:
                    for f in _rsi_done(futs, timeout=remaining):
                        try: f.result(timeout=0.2)
                        except Exception: pass
                except Exception: pass
            finally:
                ex_rc.shutdown(wait=False, cancel_futures=True)
        # Apply per item from cache (instant, no fetch)
        for it in items:
            p = it.get('pair') or ''
            if not p:
                it['rsi12h_state'] = None
                it['rsi12h_match'] = None
                continue
            # ONLY use cached data — не делаем fresh fetch здесь
            entry = _r12_mod._cache.get(p)
            if not entry:
                it['rsi12h_state'] = None
                it['rsi12h_match'] = None
                continue
            info = entry  # already has state/rsi/sma/ts
            state = info.get('state')
            it['rsi12h_state'] = state
            it['rsi12h_value'] = info.get('rsi')
            it['rsi12h_sma'] = info.get('sma')
            d = (it.get('direction') or '').upper()
            if state == 'bullish' and d == 'LONG':
                it['rsi12h_match'] = True
            elif state == 'bearish' and d == 'SHORT':
                it['rsi12h_match'] = True
            elif state is None:
                it['rsi12h_match'] = None
            else:
                it['rsi12h_match'] = False
    except Exception as _r12e:
        logging.getLogger(__name__).debug(f"[journal] rsi12h enrich fail: {_r12e}")

    # ─── EMA50/EMA200 cross state на 1h (за время сигнала) ───
    # Возвращает last cross GOLDEN/DEATH ДО at_ts сигнала + bars_ago.
    # По бэктесту 7d: 80% WR на death cross SHORT в bear-week — ценный
    # фильтр для определения regime ПРИ приходе сигнала.
    try:
        from concurrent.futures import ThreadPoolExecutor as _ECExec, as_completed as _ec_done
        import time as _t_ec
        import ema_cross_state as _ec_mod
        ec_now = int(_t_ec.time())
        ec_t0 = _t_ec.time()
        EC_BUDGET_S = 12.0
        # Recent items (24h) — для каждого считаем cross на момент at_ts
        recent_items_ec = [it for it in items if (it.get('at_ts') or 0) >= ec_now - 24 * 3600
                            and (it.get('pair') or '')]
        # Уникальные (pair, at_ts_bucket_1h) — чтоб не дублировать одинаковые
        unique_keys: set = set()
        ec_tasks: list = []
        for it in recent_items_ec:
            p = it['pair']; ats = it['at_ts']
            key = (p, ats // 3600)
            if key in unique_keys: continue
            unique_keys.add(key)
            cache_key = f"{p}|{ats // 3600}"
            if not _ec_mod._cache.get(cache_key):
                ec_tasks.append((p, ats))
        ec_tasks = ec_tasks[:40]
        if ec_tasks and (_t_ec.time() - ec_t0) < EC_BUDGET_S:
            ex_ec = _ECExec(max_workers=10)
            try:
                remaining = max(2.0, EC_BUDGET_S - (_t_ec.time() - ec_t0))
                futs = {ex_ec.submit(_ec_mod.get_state, p, ats): (p, ats) for p, ats in ec_tasks}
                try:
                    for f in _ec_done(futs, timeout=remaining):
                        try: f.result(timeout=0.2)
                        except Exception: pass
                except Exception: pass
            finally:
                ex_ec.shutdown(wait=False, cancel_futures=True)
        # Apply per item from cache
        for it in items:
            p = it.get('pair') or ''
            ats = it.get('at_ts') or 0
            if not (p and ats):
                it['ema_cross'] = None; it['ema_cross_bars_ago'] = None
                it['ema_cross_match'] = None
                continue
            cache_key = f"{p}|{ats // 3600}"
            entry = _ec_mod._cache.get(cache_key)
            if not entry:
                it['ema_cross'] = None; it['ema_cross_bars_ago'] = None
                it['ema_cross_match'] = None
                continue
            cross = entry.get('last_cross')
            it['ema_cross'] = cross
            it['ema_cross_bars_ago'] = entry.get('bars_ago')
            it['ema_cross_t'] = entry.get('cross_t')
            d = (it.get('direction') or '').upper()
            if cross == 'GOLDEN' and d == 'LONG': it['ema_cross_match'] = True
            elif cross == 'DEATH' and d == 'SHORT': it['ema_cross_match'] = True
            elif cross is None: it['ema_cross_match'] = None
            else: it['ema_cross_match'] = False
    except Exception as _ece:
        logging.getLogger(__name__).debug(f"[journal] ema_cross enrich fail: {_ece}")

    # ─── Top movers enrichment (BingX 24h gainers) ───
    # Опрашиваем BingX tickers, помечаем pairs которые сейчас в top 30
    # по 24h price change. Helps catch pre/early pumps.
    try:
        import ccxt
        ex_bx = ccxt.bingx({'options': {'defaultType': 'swap'},
                            'enableRateLimit': True,
                            'timeout': 5000})  # 5s timeout — был unlimited
        tickers = ex_bx.fetch_tickers()
        # Sort by percentage change desc
        pct_by_pair: dict = {}
        for sym, t in (tickers or {}).items():
            pct = (t or {}).get('percentage')
            if pct is None: continue
            # 'BTC/USDT:USDT' → 'BTC/USDT'
            base = sym.split(':')[0] if ':' in sym else sym
            pct_by_pair[base] = float(pct)
        # Top 30 by % change (gainers)
        top_gainers = sorted(pct_by_pair.items(), key=lambda x: -x[1])[:30]
        top_set = set(p for p, _ in top_gainers if _ > 5.0)  # минимум +5%
        for it in items:
            p = it.get('pair') or ''
            it['is_top_mover'] = p in top_set
            it['change_24h'] = pct_by_pair.get(p)
    except Exception as _te:
        logging.getLogger(__name__).debug(f"[journal] top movers fail: {_te}")
        for it in items:
            it.setdefault('is_top_mover', False)
            it.setdefault('change_24h', None)

    # ─── Cluster Delta + Resonance enrichment (для ВСЕХ источников) ───
    # Bulk-lookup cluster_delta cache по (pair, tf, candle_open_ms). Если
    # кэш пустой — fire-and-forget background fetch (заполнится ко 2-му load).
    try:
        from delta_calculator import _candle_open_ms, get_delta_snapshot
        from database import _get_db
        import time as _t_enrich
        cd_col = _get_db().cluster_delta
        # PERFORMANCE: Enrich только last 48h items (новых много, старее
        # юзер не смотрит часто, экономим ~80% query weight).
        delta_cutoff_ts = int(_t_enrich.time()) - 48 * 3600
        # Собираем (pair, tf) → (min_open_ms, max_open_ms) для range queries.
        # ВАЖНО: для резонанса нужны signal candle + RESONANCE_BARS-1 предыдущих.
        # Раньше fetch'или только signal candle через $or compound — cached_map
        # не содержал prior candles → резонанс всегда None ("резонанс пропал").
        # Сейчас одна range query на (pair, tf) даёт ВСЕ нужные свечи + buffer.
        from delta_calculator import RESONANCE_BARS as _RB, TF_MINUTES as _TFM
        pair_tf_range: dict = {}  # (pair, tf) → [min_ms, max_ms]
        for it in items:
            ats = it.get('at_ts') or 0
            pair = it.get('pair') or ''
            if not (ats and pair):
                continue
            if ats < delta_cutoff_ts:
                continue  # старее 48h — skip enrichment
            for tf in ('15m', '1h', '4h'):
                minutes = _TFM.get(tf, 15)
                sig_open = _candle_open_ms(ats * 1000, tf)
                # Range: от signal_open back до RESONANCE_BARS-1 свечей
                min_ms = sig_open - (_RB - 1) * minutes * 60 * 1000
                key = (pair, tf)
                if key not in pair_tf_range:
                    pair_tf_range[key] = [min_ms, sig_open]
                else:
                    if min_ms < pair_tf_range[key][0]:
                        pair_tf_range[key][0] = min_ms
                    if sig_open > pair_tf_range[key][1]:
                        pair_tf_range[key][1] = sig_open
        cached_map: dict = {}
        if pair_tf_range:
            # Bulk $or range queries — каждая cond = (pair, tf, range).
            # Chunk 50 — каждая cond может match много docs, осторожнее с BSON 16MB.
            conds_all = [
                {'pair': p, 'tf': t, 'open_ms': {'$gte': mn, '$lte': mx}}
                for (p, t), (mn, mx) in pair_tf_range.items()
            ]
            CHUNK = 50
            for i in range(0, len(conds_all), CHUNK):
                try:
                    for doc in cd_col.find({'$or': conds_all[i:i+CHUNK]},
                                           {'pair':1,'tf':1,'open_ms':1,
                                            'delta_pct':1,'_id':0}):
                        k = (doc.get('pair'), doc.get('tf'), doc.get('open_ms'))
                        cached_map[k] = doc
                except Exception:
                    pass
            # Распихиваем cached delta по items + считаем резонанс на лету
            # (резонанс = N подряд свечей до сигнальной включительно)
            from delta_calculator import RESONANCE_BARS, TF_MINUTES, _resonance_from_deltas
            for it in items:
                ats = it.get('at_ts') or 0
                pair = it.get('pair') or ''
                if not (ats and pair):
                    continue
                for tf in ('15m', '1h', '4h'):
                    minutes = TF_MINUTES.get(tf, 15)
                    sig_open = _candle_open_ms(ats * 1000, tf)
                    sig_doc = cached_map.get((pair, tf, sig_open))
                    # Set signal delta if available (skip if already set from source doc)
                    if sig_doc and it.get(f'delta_{tf}') is None:
                        it[f'delta_{tf}'] = sig_doc.get('delta_pct')
                    # Резонанс: соберём N свечей до signal candle.
                    # ВАЖНО: считаем резонанс ДАЖЕ если signal candle ещё не закрыта
                    # (раньше тут был `if not sig_doc: continue` — резонанс терялся для
                    # всех свежих сигналов где текущая свеча ещё открыта). Резонанс
                    # = N consecutive same-sign deltas в окне предыдущих свечей,
                    # signal candle включается опционально если уже есть delta.
                    if it.get(f'resonance_{tf}') is None:
                        deltas: list[float] = []
                        for j in range(RESONANCE_BARS - 1, -1, -1):
                            bk = sig_open - j * minutes * 60 * 1000
                            if bk == sig_open and not sig_doc:
                                # signal candle ещё не закрыта и нет данных — используем
                                # уже выставленную delta_tf (если есть, из inline fill).
                                v = it.get(f'delta_{tf}')
                                if v is not None:
                                    deltas.append(float(v))
                                continue
                            d = cached_map.get((pair, tf, bk))
                            if d:
                                deltas.append(d.get('delta_pct') or 0.0)
                        # Считаем резонанс если есть минимум 3 свечи (раньше требовалось 5,
                        # это терялось для свежих сигналов)
                        if len(deltas) >= 3:
                            it[f'resonance_{tf}'] = _resonance_from_deltas(deltas)

            # ─── INLINE fast fill для свежих сигналов (last 6h) ───
            # Через klines (1 запрос/TF/пара = 5 свечей резонанса).
            # Параллельно через ThreadPoolExecutor, бюджет 4с total.
            import time as _t
            from concurrent.futures import ThreadPoolExecutor, as_completed
            now_s = int(_t.time())
            recent_cutoff = now_s - 6 * 3600
            inline_keys: set = set()
            for it in items:
                ats = it.get('at_ts') or 0
                pair = it.get('pair') or ''
                if not (ats and pair) or ats < recent_cutoff:
                    continue
                # FIX: было OR → если 1h уже был заполнен из cache, 15m тоже не пытались
                # заполнить. Теперь AND — добавляем в inline_keys если хотя бы один TF
                # отсутствует. Затем _fetch_one добавит недостающие.
                if (it.get('delta_15m') is not None and it.get('delta_1h') is not None
                        and it.get('delta_4h') is not None):
                    continue
                inline_keys.add((pair, ats))
            inline_list = list(inline_keys)[:30]  # больше пар т.к. signal-only fast
            inline_results: dict = {}
            if inline_list:
                # SIGNAL-ONLY (1 aggTrades call/TF): ~0.5-1s/pair (fast).
                # ВАЖНО: НЕ пробуем klines first — get_delta_snapshot_fast тянет 5
                # свечей резонанса × 3 TF = ~11s, выпадает за budget 2s, futures
                # не завершаются → данных нет. Идём прямо в aggTrades signal-only
                # (0.6s/pair), резонанс посчитаем из cluster_delta prior candles
                # (если в кэше уже есть) или пометим как None.
                from delta_calculator import get_signal_delta_only
                def _fetch_one(p, ts):
                    try:
                        snap = get_signal_delta_only(p, ts * 1000)
                        return (p, ts, snap)
                    except Exception:
                        return (p, ts, None)
                # FIX: with ThreadPoolExecutor блокирует на __exit__ ожидая
                # все futures. Используем manual shutdown(wait=False, cancel_futures)
                # чтобы не зависать на медленных fetch'ах.
                ex_delta = ThreadPoolExecutor(max_workers=16)
                try:
                    futs = [ex_delta.submit(_fetch_one, p, ts) for (p, ts) in inline_list]
                    try:
                        # budget 4s — даём reasonable окно для 16 параллельных
                        # aggTrades fetches (~0.6-1s каждый)
                        for f in as_completed(futs, timeout=4.0):
                            try:
                                p, ts, snap = f.result(timeout=0.05)
                                if snap:
                                    inline_results[(p, ts)] = snap
                            except Exception:
                                pass
                    except Exception:
                        pass
                finally:
                    ex_delta.shutdown(wait=False, cancel_futures=True)
                # Распихиваем результаты обратно в items + считаем резонанс
                # из cluster_delta cache prior candles, если inline snap дал только
                # signal-only delta (aggTrades path не возвращает резонанс).
                for it in items:
                    ats = it.get('at_ts') or 0
                    pair = it.get('pair') or ''
                    snap = inline_results.get((pair, ats))
                    if not snap:
                        continue
                    for tf in ('15m', '1h', '4h'):
                        s = snap.get(tf)
                        if not s:
                            continue
                        minutes = TF_MINUTES.get(tf, 15)
                        sig_open = _candle_open_ms(ats * 1000, tf)
                        if it.get(f'delta_{tf}') is None:
                            it[f'delta_{tf}'] = s.get('delta_pct')
                        # Резонанс: предпочитаем значение из snap (klines path даёт),
                        # иначе считаем из cluster_delta cache prior candles + signal delta.
                        if it.get(f'resonance_{tf}') is None:
                            if s.get('resonance') is not None:
                                it[f'resonance_{tf}'] = s.get('resonance')
                            else:
                                deltas: list[float] = []
                                for j in range(RESONANCE_BARS - 1, -1, -1):
                                    bk = sig_open - j * minutes * 60 * 1000
                                    if bk == sig_open:
                                        v = it.get(f'delta_{tf}')
                                        if v is not None:
                                            deltas.append(float(v))
                                        continue
                                    d = cached_map.get((pair, tf, bk))
                                    if d:
                                        deltas.append(d.get('delta_pct') or 0.0)
                                if len(deltas) >= 3:
                                    it[f'resonance_{tf}'] = _resonance_from_deltas(deltas)

            # ─── Background fill для остальных miss'ов (last 24h) ───
            cutoff_s = now_s - 24 * 3600
            missing_keys: set = set()
            for it in items:
                ats = it.get('at_ts') or 0
                pair = it.get('pair') or ''
                if not (ats and pair) or ats < cutoff_s:
                    continue
                if it.get('delta_15m') is not None or it.get('delta_1h') is not None:
                    continue
                missing_keys.add((pair, ats))
            if missing_keys:
                import threading as _th
                missing_list = list(missing_keys)[:30]
                def _bg_fill():
                    try:
                        from delta_calculator import (get_delta_snapshot_fast,
                                                       get_delta_snapshot)
                        for (p, ts_s) in missing_list:
                            try:
                                # Try klines first, fallback to aggTrades
                                snap = get_delta_snapshot_fast(p, ts_s * 1000)
                                if not (snap and (snap.get('15m') or snap.get('1h'))):
                                    get_delta_snapshot(p, ts_s * 1000)
                            except Exception:
                                pass
                    except Exception:
                        pass
                _th.Thread(target=_bg_fill, daemon=True,
                          name='delta-bg-fill').start()

            # ─── ANOMALY DETECTION: z-score delta_pct сигнальной свечи vs
            # 30 свечей до неё. Запускается ПОСЛЕ inline fill чтобы свежие
            # baseline candles были в cluster_delta. Time-budgeted 2.5s.
            try:
                import time as _t_anom
                _anom_t0 = _t_anom.time()
                _ANOM_BUDGET_S = 2.5
                from collections import defaultdict
                BASELINE_BARS = 30
                sig_by_pt: dict = defaultdict(list)
                for it in items:
                    ats = it.get('at_ts') or 0
                    pair = it.get('pair') or ''
                    if not (ats and pair):
                        continue
                    if ats < delta_cutoff_ts:
                        continue
                    for tf in ('15m', '1h', '4h'):
                        sig_open = _candle_open_ms(ats * 1000, tf)
                        sig_by_pt[(pair, tf)].append((it, sig_open))
                tf_minutes = {'15m': 15, '1h': 60, '4h': 240}
                or_conds = []
                for (pair, tf), sig_list in sig_by_pt.items():
                    bucket_ms = tf_minutes[tf] * 60 * 1000
                    min_open = min(o for _, o in sig_list)
                    max_open = max(o for _, o in sig_list)
                    range_start = min_open - BASELINE_BARS * bucket_ms
                    or_conds.append({
                        'pair': pair, 'tf': tf,
                        'open_ms': {'$gte': range_start, '$lte': max_open},
                    })
                docs_by_pt: dict = defaultdict(dict)
                CHUNK_ANOM = 50
                for i in range(0, len(or_conds), CHUNK_ANOM):
                    if _t_anom.time() - _anom_t0 > _ANOM_BUDGET_S:
                        logging.getLogger(__name__).debug(
                            f"[journal] anomaly z-score budget exceeded, partial")
                        break
                    try:
                        for doc in cd_col.find(
                                {'$or': or_conds[i:i+CHUNK_ANOM]},
                                {'pair':1,'tf':1,'open_ms':1,
                                 'delta_pct':1,'_id':0}):
                            docs_by_pt[(doc['pair'], doc['tf'])][
                                doc['open_ms']] = float(doc.get('delta_pct') or 0)
                    except Exception:
                        pass
                for (pair, tf), sig_list in sig_by_pt.items():
                    by_open = docs_by_pt.get((pair, tf), {})
                    if not by_open:
                        continue
                    sorted_opens = sorted(by_open.keys())
                    for it, sig_open in sig_list:
                        preceding = [by_open[o] for o in sorted_opens
                                      if o < sig_open][-BASELINE_BARS:]
                        sig_delta = by_open.get(sig_open)
                        # FIX: для 4h сигнальная свеча часто ещё открыта и не
                        # пишется в cache. Используем самую свежую closed
                        # свечу <= sig_open как proxy для signal delta.
                        if sig_delta is None:
                            latest_le = [by_open[o] for o in sorted_opens
                                          if o <= sig_open]
                            if latest_le:
                                sig_delta = latest_le[-1]
                                # Убираем эту свечу из preceding чтобы не было self-correlation
                                preceding = [by_open[o] for o in sorted_opens
                                              if o < sorted_opens[len(latest_le)-1]][-BASELINE_BARS:]
                        # FIX: понизил baseline threshold 10 → 5 (новые пары)
                        if sig_delta is None or len(preceding) < 5:
                            continue
                        mean = sum(preceding) / len(preceding)
                        var = sum((d - mean)**2 for d in preceding) / len(preceding)
                        std = var ** 0.5
                        if std < 0.1:
                            continue
                        z = (sig_delta - mean) / std
                        it[f'delta_zscore_{tf}'] = round(z, 2)
            except Exception as _e:
                logging.getLogger(__name__).debug(f"[journal] anomaly z-score fail: {_e}")
    except Exception as e:
        logging.getLogger(__name__).warning(f"[journal] delta enrich fail: {e}")

    # Quality Score 0-100 на каждый item (для HOT NOW + journal score column).
    # Считаем confluence count: сколько разных new_strategy сигналов на той
    # же паре + direction в окне 30 мин (для score bonus +3..+10).
    try:
        from quality_score import compute_signal_score
        # Pre-compute confluence count: group new_strategy items
        from collections import defaultdict
        ns_count: dict = defaultdict(int)
        ns_sources = ('volume_surge', 'triple_confluence', 'vol_accum',
                      'volcano', 'second_flip')
        for it in items:
            if it.get('source') in ns_sources:
                bucket = (it.get('at_ts') or 0) // 1800  # 30min
                key = (it.get('symbol') or '', it.get('direction') or '', bucket)
                ns_count[key] += 1
        for it in items:
            ctx = {}
            if it.get('source') in ns_sources:
                bucket = (it.get('at_ts') or 0) // 1800
                key = (it.get('symbol') or '', it.get('direction') or '', bucket)
                ctx['strategy_count'] = ns_count.get(key, 1)
            if it.get('st_tier'):
                ctx['tier'] = it['st_tier']
            try:
                it['q_score'] = compute_signal_score(it, ctx)
            except Exception:
                it['q_score'] = 0
    except Exception as e:
        logging.getLogger(__name__).warning(f"[journal] q_score fail: {e}")

    # ─── RSI 15m/1h/4h/1d enrichment из Mongo + INLINE fill для top-10 свежих ───
    # Smart fill: только 10 самых свежих пар где cache miss, budget 3s через FAPI.
    # Остальные fill'ы — fire-and-forget background.
    try:
        from rsi_cache import bulk_get_rsi_for_items, fill_pair_rsi
        import time as _t_rsi
        # PERF: enrich только items за last 48h (cache TTL 48h, старые не имеют smysl)
        # Раньше прогоняли все 11000 items × 4 TF = 44000 lookups (~8с).
        # Теперь ~last 1000 items × 4 TF = 4000 lookups = 1-2с.
        _cutoff_48h = int(_t_rsi.time()) - 48 * 3600
        _recent_items = [it for it in items if (it.get('at_ts') or 0) >= _cutoff_48h]
        bulk_get_rsi_for_items(_recent_items)
        import threading as _th_rsi
        from concurrent.futures import ThreadPoolExecutor, as_completed
        # Сортируем missing pairs по recency (свежие первые)
        missing_with_ts = []
        for it in _recent_items:
            ats = it.get('at_ts') or 0
            pair = it.get('pair') or ''
            if not (ats and pair):
                continue
            if (it.get('rsi_1h') is None or it.get('rsi_4h') is None
                    or it.get('rsi_1d') is None):
                missing_with_ts.append((ats, pair))
        # Top-10 свежих pairs (sort by ats desc, dedup pair)
        missing_with_ts.sort(key=lambda x: -x[0])
        seen = set()
        top_recent = []
        for ats, p in missing_with_ts:
            if p in seen:
                continue
            seen.add(p)
            top_recent.append(p)
            if len(top_recent) >= 10:
                break
        if top_recent:
            _t_start = _t_rsi.time()
            ex_rsi = ThreadPoolExecutor(max_workers=10)
            try:
                futs = [ex_rsi.submit(fill_pair_rsi, p) for p in top_recent]
                try:
                    for f in as_completed(futs, timeout=3.0):
                        try:
                            f.result(timeout=0.05)
                        except Exception:
                            pass
                except Exception:
                    pass
            finally:
                ex_rsi.shutdown(wait=False, cancel_futures=True)
            # PERF: re-read ТОЛЬКО для заполненных пар (10 шт), не всех 10000+
            # Раньше бесполезно повторял bulk_get по всему items (8 сек).
            if _t_rsi.time() - _t_start < 3.5:
                try:
                    filled_set = set(top_recent)
                    sub_items = [it for it in items if it.get('pair') in filled_set]
                    if sub_items:
                        bulk_get_rsi_for_items(sub_items)
                except Exception:
                    pass
        # Background fill для остальных свежих миссов (без блокировки render)
        rest_missing = list(seen)[10:30]  # next 20 pairs в фоне
        if rest_missing:
            def _bg_fill_rsi():
                for p in rest_missing:
                    try:
                        fill_pair_rsi(p)
                    except Exception:
                        pass
            _th_rsi.Thread(target=_bg_fill_rsi, daemon=True,
                            name='rsi-bg-fill').start()
    except Exception as e:
        logging.getLogger(__name__).warning(f"[journal] rsi enrich fail: {e}")

    # ─── Trend enrichment — INLINE fill для top-10 свежих + bg остальные ───
    try:
        from trend_cache import bulk_get_trend_for_items, fill_pair_trend
        import time as _t_tr
        # PERF: enrich только items за last 48h (cache TTL 48h)
        _cutoff_48h_tr = int(_t_tr.time()) - 48 * 3600
        _recent_items_tr = [it for it in items if (it.get('at_ts') or 0) >= _cutoff_48h_tr]
        bulk_get_trend_for_items(_recent_items_tr)
        import threading as _th_tr
        from concurrent.futures import ThreadPoolExecutor as _TPE_tr, as_completed as _ac_tr
        missing_with_ts_tr = []
        for it in _recent_items_tr:
            ats = it.get('at_ts') or 0
            pair = it.get('pair') or ''
            if not (ats and pair):
                continue
            if (it.get('trend_1h') is None or it.get('trend_4h') is None):
                missing_with_ts_tr.append((ats, pair))
        missing_with_ts_tr.sort(key=lambda x: -x[0])
        seen_tr = set()
        top_recent_tr = []
        for ats, p in missing_with_ts_tr:
            if p in seen_tr:
                continue
            seen_tr.add(p)
            top_recent_tr.append(p)
            if len(top_recent_tr) >= 10:
                break
        if top_recent_tr:
            _t_start_tr = _t_tr.time()
            ex_tr = _TPE_tr(max_workers=10)
            try:
                futs = [ex_tr.submit(fill_pair_trend, p) for p in top_recent_tr]
                try:
                    for f in _ac_tr(futs, timeout=3.0):
                        try:
                            f.result(timeout=0.05)
                        except Exception:
                            pass
                except Exception:
                    pass
            finally:
                ex_tr.shutdown(wait=False, cancel_futures=True)
            # PERF: re-read только для заполненных пар (10 шт)
            if _t_tr.time() - _t_start_tr < 3.5:
                try:
                    filled_tr_set = set(top_recent_tr)
                    sub_items_tr = [it for it in items if it.get('pair') in filled_tr_set]
                    if sub_items_tr:
                        bulk_get_trend_for_items(sub_items_tr)
                except Exception:
                    pass
        rest_missing_tr = list(seen_tr)[10:30]
        if rest_missing_tr:
            def _bg_fill_trend():
                for p in rest_missing_tr:
                    try:
                        fill_pair_trend(p)
                    except Exception:
                        pass
            _th_tr.Thread(target=_bg_fill_trend, daemon=True,
                          name='trend-bg-fill').start()
    except Exception as e:
        logging.getLogger(__name__).warning(f"[journal] trend enrich fail: {e}")

    return {"items": items}


@app.get("/api/hot-signals/count")
async def api_hot_signals_count(hours: int = 3, min_score: int = 60):
    """Lightweight счётчик HOT signals для navigation badge.
    Cache 30s через journal_cache (тот же что /api/hot-signals)."""
    from cache_utils import journal_cache
    full = await journal_cache.get_or_compute(
        "journal_all",
        lambda: asyncio.to_thread(_compute_journal_sync),
    )
    items = full.get("items", []) if isinstance(full, dict) else []
    if not items:
        return {"count": 0, "hours": hours, "min_score": min_score}
    import time as _t
    cutoff_ts = int(_t.time()) - hours * 3600
    # Dedup by (pair, direction, 30min bucket)
    seen = set()
    count = 0
    for s in items:
        if (s.get('at_ts') or 0) < cutoff_ts:
            continue
        if (s.get('q_score') or 0) < min_score:
            continue
        key = (s.get('symbol') or '', s.get('direction') or '',
               (s.get('at_ts') or 0) // 1800)
        if key in seen:
            continue
        seen.add(key)
        count += 1
    return {"count": count, "hours": hours, "min_score": min_score}


@app.get("/api/hot-signals")
async def api_hot_signals(limit: int = 5, hours: int = 3):
    """🔥 HOT NOW — TOP signals by quality score за последние N часов.

    Pulls items from /api/journal cache, filters by recent time, dedupes
    by (pair, direction, 30min bucket) keeping max-score, returns top N.
    """
    from cache_utils import journal_cache
    import time as _t

    cache_key = f"hot_{limit}_{hours}"
    now = _t.time()

    async def _compute():
        full = await journal_cache.get_or_compute(
            "journal_all",
            lambda: asyncio.to_thread(_compute_journal_sync),
        )
        items = full.get("items", []) if isinstance(full, dict) else []
        if not items:
            return []
        # Filter by recency
        cutoff_ts = int(_t.time()) - hours * 3600
        recent = [s for s in items if (s.get('at_ts') or 0) >= cutoff_ts]
        # Dedupe (pair, direction, 30min bucket) — keep max score
        from collections import defaultdict
        best: dict = {}
        groups: dict = defaultdict(list)
        for s in recent:
            sym = s.get('symbol') or ''
            direction = s.get('direction') or ''
            bucket = (s.get('at_ts') or 0) // 1800
            key = (sym, direction, bucket)
            groups[key].append(s)
            if (key not in best
                    or (s.get('q_score', 0) > best[key].get('q_score', 0))):
                best[key] = s
        # Add strategy stack info (for emoji display)
        ns_sources = {'volume_surge', 'triple_confluence', 'vol_accum',
                      'volcano', 'second_flip'}
        for key, head in best.items():
            stack = []
            for s in groups[key]:
                if s.get('source') in ns_sources and s['source'] not in stack:
                    stack.append(s['source'])
            head['_strategy_stack'] = stack
        # Sort by score, return top N
        result = sorted(best.values(), key=lambda x: -x.get('q_score', 0))
        return result[:max(1, min(limit, 50))]

    items = await _compute()
    return {"items": items, "count": len(items),
            "hours": hours, "limit": limit, "now_ts": int(now)}


@app.get("/api/cluster-delta")
async def api_cluster_delta(pair: str, at_ts: int = 0, tf: str = "15m,1h"):
    """Cluster Delta + Resonance snapshot (Binance Futures aggTrades).

    pair: BTC/USDT or BTC (auto-normalized to BTCUSDT)
    at_ts: signal timestamp в seconds UTC (0 = сейчас)
    tf: comma-separated, default '15m,1h'

    Returns: {pair, at_ts, snapshot: {15m: {...}, 1h: {...}}}
    """
    from delta_calculator import get_delta_snapshot_async
    timeframes = tuple(t.strip() for t in (tf or "").split(",") if t.strip())
    if not timeframes:
        timeframes = ('15m', '1h')
    at_ts_ms = int(at_ts) * 1000 if at_ts else None
    snap = await get_delta_snapshot_async(pair, at_ts_ms, timeframes)
    return {
        "pair": pair,
        "at_ts": at_ts,
        "snapshot": snap,
    }


# Глобальный статус массового backfill — для прогресса в UI
_delta_backfill_status: dict = {
    'running': False,
    'started_at': None,
    'pairs_total': 0,
    'pairs_done': 0,
    'candles_written': 0,
    'errors': 0,
    'last_pair': '',
    'finished_at': None,
}


@app.post("/api/cluster-delta/backfill-all")
async def api_cluster_delta_backfill_all(days: int = 14):
    """МАССОВЫЙ backfill через klines (полная история, не 24h limit).

    Сканирует ВСЕ signals в журнале за last N дней (max 60), группирует
    по уникальным парам, фетчит klines (15m + 1h) и пишет в cluster_delta
    cache. Запускается в background thread — endpoint возвращается мгновенно
    с running=true. Прогресс через GET /api/cluster-delta/backfill-status.

    Полностью НЕ влияет на сигналы — пишет в кэш дельт. Журнал автоматически
    подхватит данные при следующем рендере (TTL 60с).
    """
    global _delta_backfill_status
    if _delta_backfill_status.get('running'):
        return {'error': 'already running', 'status': _delta_backfill_status}
    days = max(1, min(60, int(days)))

    # Собираем уникальные пары + временной диапазон по всем коллекциям журнала
    def _collect_pairs():
        from database import (_get_db, _signals, _anomalies, _confluence,
                              utcnow as _u)
        from datetime import timedelta as _td
        since = _u() - _td(days=days)
        # Pair → (min_ts, max_ts)
        ranges: dict = {}
        def _add(pair, ts):
            if not pair or not ts:
                return
            if hasattr(ts, 'timestamp'):
                ts_s = int(ts.timestamp())
            else:
                ts_s = int(ts)
            if not ts_s:
                return
            cur = ranges.get(pair)
            if cur is None:
                ranges[pair] = (ts_s, ts_s)
            else:
                ranges[pair] = (min(cur[0], ts_s), max(cur[1], ts_s))
        # Tradium
        try:
            for s in _signals().find({'source': 'tradium'},
                                     {'pair': 1, 'received_at': 1,
                                      'pattern_triggered_at': 1}).limit(1000):
                _add(s.get('pair'), s.get('pattern_triggered_at') or s.get('received_at'))
        except Exception:
            pass
        # Confluence
        try:
            for s in _confluence().find({'detected_at': {'$gte': since}},
                                        {'symbol': 1, 'detected_at': 1}).limit(2000):
                pair = s.get('symbol', '')
                if pair and not pair.endswith('/USDT'):
                    pair = pair.replace('USDT', '/USDT') if pair.endswith('USDT') else pair
                _add(pair, s.get('detected_at'))
        except Exception:
            pass
        # CV signals (signals collection с source!=tradium)
        try:
            for s in _signals().find(
                    {'source': {'$in': ['cryptovizor', 'cluster', 'paper',
                                        'verified', 'supertrend']},
                     'received_at': {'$gte': since}},
                    {'pair': 1, 'received_at': 1}).limit(3000):
                _add(s.get('pair'), s.get('received_at'))
        except Exception:
            pass
        # New strategy signals
        try:
            nss = _get_db().new_strategy_signals
            for s in nss.find({'created_at': {'$gte': since}},
                              {'pair': 1, 'created_at': 1, 'st_flip_at': 1}).limit(5000):
                _add(s.get('pair'), s.get('st_flip_at') or s.get('created_at'))
        except Exception:
            pass
        # Supertrend signals (если есть отдельная коллекция)
        try:
            from database import _supertrend_signals
            for s in _supertrend_signals().find({'created_at': {'$gte': since}},
                                                 {'pair': 1, 'created_at': 1}).limit(3000):
                _add(s.get('pair'), s.get('created_at'))
        except Exception:
            pass
        return ranges

    pair_ranges = await asyncio.to_thread(_collect_pairs)
    if not pair_ranges:
        return {'error': 'no signals found', 'days': days}

    # Reset status & launch background thread
    import time as _t
    _delta_backfill_status.update({
        'running': True,
        'started_at': int(_t.time()),
        'pairs_total': len(pair_ranges),
        'pairs_done': 0,
        'candles_written': 0,
        'errors': 0,
        'last_pair': '',
        'finished_at': None,
    })

    def _worker():
        global _delta_backfill_status
        try:
            from delta_calculator import bulk_fill_pair_history
            for pair, (min_ts, max_ts) in pair_ranges.items():
                try:
                    # Запас по времени для резонанса (5 свечей до min_ts)
                    # 1h × 5 = 5h в мс
                    pad_ms = 5 * 3600 * 1000
                    start_ms = (min_ts * 1000) - pad_ms
                    end_ms = (max_ts * 1000) + 60 * 60 * 1000  # +1h после
                    res = bulk_fill_pair_history(pair, start_ms, end_ms,
                                                 timeframes=('15m', '1h'))
                    written = sum(res.values()) if res else 0
                    _delta_backfill_status['candles_written'] += written
                    _delta_backfill_status['last_pair'] = pair
                except Exception as e:
                    _delta_backfill_status['errors'] += 1
                    logging.getLogger(__name__).debug(f'[delta-backfill-all] {pair}: {e}')
                _delta_backfill_status['pairs_done'] += 1
                # Throttle между парами — Binance weight limit ~2400/min, klines weight=2,
                # 30 пар × 4 calls = 120 weight = ОК; sleep 0.2с safety
                _t.sleep(0.2)
            # Инвалидируем journal cache чтобы UI сразу увидел данные
            try:
                from cache_utils import journal_cache
                journal_cache.invalidate("journal_all")
            except Exception:
                pass
        except Exception:
            logging.getLogger(__name__).exception('[delta-backfill-all] worker fail')
        finally:
            _delta_backfill_status['running'] = False
            _delta_backfill_status['finished_at'] = int(_t.time())

    import threading as _th
    _th.Thread(target=_worker, daemon=True, name='delta-backfill-all').start()
    return {
        'started': True,
        'pairs_total': len(pair_ranges),
        'days': days,
        'message': 'Background backfill started. Poll /api/cluster-delta/backfill-status for progress.',
    }


@app.get("/api/auto-strategy/log")
async def api_auto_strategy_log(hours: int = 6, limit: int = 200):
    """Лог решений ALPHA-CV стратегии — кто принят, кто отклонён, и почему.

    Returns: {accepted, rejected, by_reason, recent_decisions}
    """
    from database import _get_db, utcnow
    from datetime import timedelta as _td
    from collections import Counter
    db = _get_db()
    col = db.auto_strategy_log
    since = utcnow() - _td(hours=int(hours))
    cursor = col.find({'at': {'$gte': since}}).sort('at', -1).limit(int(limit))
    items = []
    accept_count = 0
    reject_count = 0
    reasons_accept = Counter()
    reasons_reject = Counter()
    for doc in cursor:
        accepted = doc.get('accept', False)
        reason = doc.get('reason', '?')
        if accepted:
            accept_count += 1
            reasons_accept[reason] += 1
        else:
            reject_count += 1
            reasons_reject[reason] += 1
        items.append({
            'at': doc.get('at').isoformat() if doc.get('at') else None,
            'pair': doc.get('signal_pair'),
            'source': doc.get('signal_source'),
            'direction': doc.get('signal_direction'),
            'tier': doc.get('signal_tier'),
            'q_score': doc.get('signal_q_score'),
            'accept': accepted,
            'reason': reason,
            'size_pct': doc.get('size_pct'),
            'size_label': doc.get('size_label'),
        })
    return {
        'hours': hours,
        'accepted': accept_count,
        'rejected': reject_count,
        'top_accept_reasons': reasons_accept.most_common(10),
        'top_reject_reasons': reasons_reject.most_common(10),
        'recent': items,
    }


@app.post("/api/auto-strategy/enable")
async def api_auto_strategy_enable(enabled: bool = True, note: str = ""):
    """Включить/выключить ALPHA-CV стратегию через Mongo flag.
    Hot reload без перезапуска контейнера."""
    from auto_strategy import set_enabled
    result = await asyncio.to_thread(set_enabled, enabled, note)
    return result


@app.post("/api/auto-strategy/close-all-and-start")
async def api_auto_strategy_close_all_and_start():
    """Закрыть все open paper позиции по market price + включить ALPHA-CV.
    Используется для clean transition к новой стратегии."""
    import paper_trader as pt
    from auto_strategy import set_enabled
    # 1. Close all
    close_result = await pt.close_all_manual()
    # 2. Enable strategy
    enable_result = await asyncio.to_thread(set_enabled, True,
        "auto-enabled via /api/auto-strategy/close-all-and-start")
    return {
        "closed_positions": close_result.get("total_count", 0),
        "total_pnl_usdt": close_result.get("total_pnl_usdt", 0),
        "closed": close_result.get("closed", []),
        "strategy_enabled": enable_result.get("enabled"),
    }


@app.get("/api/auto-strategy/info")
async def api_auto_strategy_info():
    """Метаданные активной стратегии для UI вкладки.

    С 2026-07-11 автоторговля = MOMENTUM-ONLY: paper/live открывают
    ТОЛЬКО 💥 ignition и 💰 ten (гейт в paper_trader.on_signal,
    env PAPER_MOMENTUM_ONLY, дефолт вкл). ALPHA-CV снята с сигналов.
    """
    try:
        import paper_trader as _pt
        enabled = os.getenv("PAPER_MOMENTUM_ONLY", "1") == "1"
        mode = await asyncio.to_thread(_pt.get_mode)
        return {
            "name": "MOMENTUM",
            "version": "v1.0",
            "description": (
                "Торгуются только два валидированных momentum-источника: "
                "💥 IGNITION — ранний вход в начале импульса (1h, объём+дельта, "
                "TP +6% / SL −3%, горизонт 72ч) и 💰 TEN — импульс на 30m с целью "
                "+10% (TP +10% / SL −5%, горизонт 96ч). Родные TP/SL стратегий, "
                "без ATR-пересчёта чекера. Остальные источники дают сигналы и "
                "алерты как раньше, но автоторговля их не открывает "
                "(90д-бэктест: все ниже безубытка)."
            ),
            "enabled": enabled,
            "backtest": {
                "win_rate_pct": "65–70",
                "avg_r_per_trade": "~1.0",
                "profit_factor": "≥2",
                "trades_after_filter": "90д валидация",
            },
            "rules": {
                "entry_whitelist": [
                    "💥 ignition — WR 70%, EV +2.75%/сделку (TP6/SL3, 72ч)",
                    "💰 ten — WR 65%, EV +4.73%/сделку (TP10/SL5, 96ч)",
                ],
                "blocked_sources": [
                    "supertrend / st_mtf / st_daily (ST4h-флип: WR 34.8% LONG, 27.7% SHORT — ниже BE 38)",
                    "confluence (WR 41.6% лучший срез — на грани, нестабилен)",
                    "cluster / whale / shark / accum (нет направленного edge)",
                    "rider_short (PF 1.29, но lumpy — алерты остаются, автовход нет)",
                    "impulse / fade (родители ignition/ten — поздний вход)",
                ],
            },
            "sizing": [
                {"setup": f"режим {mode['name'].upper()} — size {mode['size_min']}–{mode['size_max']}%",
                 "multiplier": 1.0, "base_pct": (mode['size_min'] + mode['size_max']) / 2},
                {"setup": f"плечо ×{mode['lev_min']}–{mode['lev_max']} (low-cap ≤4×)",
                 "multiplier": 1.0, "base_pct": 0},
            ],
            "limits": {
                "max_concurrent_positions": _pt.MAX_POSITIONS,
                "max_total_exposure_pct": _pt.MAX_POSITIONS * mode['size_max'],
                "daily_loss_limit_pct": "—",
                "drawdown_limit_pct": "—",
                "per_trade_hard_cap_pct": mode['size_max'],
            },
            "exit_logic": {
                "method": "родные TP/SL источника (first-touch)",
                "long_exit": "💥 TP +6% / SL −3% · 💰 TP +10% / SL −5%",
                "short_exit": "momentum-источники только LONG",
                "backup_sl": "SL стратегии (hard stop)",
                "max_hold_hours": 96,
            },
        }
    except Exception as e:
        import traceback
        return {"error": str(e), "trace": traceback.format_exc()[:500]}


@app.get("/api/auto-strategy/status")
async def api_auto_strategy_status():
    """Текущее состояние стратегии: открытые позиции, capital state, daily PnL."""
    try:
        from auto_strategy import (is_enabled, get_capital_state,
                                    MAX_CONCURRENT_POSITIONS,
                                    MAX_TOTAL_EXPOSURE_PCT,
                                    DAILY_LOSS_LIMIT_PCT, DRAWDOWN_LIMIT_PCT)
        enabled = is_enabled()
        cap = await asyncio.to_thread(get_capital_state)
    except Exception as e:
        enabled = False
        cap = {'error': str(e)}
    return {
        'enabled': enabled,
        'capital_state': cap,
        'limits': {
            'max_concurrent': MAX_CONCURRENT_POSITIONS,
            'max_exposure_pct': MAX_TOTAL_EXPOSURE_PCT,
            'daily_loss_pct': DAILY_LOSS_LIMIT_PCT,
            'drawdown_pct': DRAWDOWN_LIMIT_PCT,
        },
    }


@app.get("/api/cluster-delta/diag")
async def api_cluster_delta_diag():
    """Диагностика: deployed version + sample fast fetch."""
    import time as _t, sys
    out = {
        'has_get_delta_snapshot_fast': False,
        'has_inline_in_journal': False,
        'sample_fetch': None,
        'sample_fetch_time': None,
    }
    try:
        from delta_calculator import get_delta_snapshot_fast
        out['has_get_delta_snapshot_fast'] = True
    except ImportError:
        pass
    # Check journal source code for inline fetch marker
    import inspect
    try:
        src = inspect.getsource(_compute_journal_sync)
        out['has_inline_in_journal'] = 'INLINE fast fill' in src or 'get_delta_snapshot_fast' in src
        out['compute_journal_size_bytes'] = len(src)
    except Exception as e:
        out['inspect_err'] = str(e)
    # Sample fetch — multiple pairs + timestamps
    if out['has_get_delta_snapshot_fast']:
        from delta_calculator import (get_delta_snapshot_fast,
                                       _delta_from_klines_batch,
                                       _normalize_symbol, _candle_open_ms,
                                       BINANCE_FAPI, _http_client)
        out['samples'] = {}
        # Test direct HTTP first
        try:
            t0 = _t.time()
            r = _http_client.get(f"{BINANCE_FAPI}/fapi/v1/klines",
                                 params={'symbol':'BIOUSDT','interval':'1h','limit':3})
            out['raw_http'] = {
                'status': r.status_code,
                'time': round(_t.time()-t0, 3),
                'first_candle_open': r.json()[0][0] if r.status_code==200 and r.json() else None,
                'count': len(r.json()) if r.status_code==200 else 0,
            }
        except Exception as e:
            out['raw_http_err'] = repr(e)
        # Test _delta_from_klines_batch directly
        try:
            t0 = _t.time()
            sym = _normalize_symbol('BIO/USDT')
            now_ms = int(_t.time() * 1000)
            sig_open = _candle_open_ms(now_ms, '1h')
            start = sig_open - 5 * 60 * 60 * 1000
            end = sig_open + 60 * 60 * 1000
            candles = _delta_from_klines_batch(sym, '1h', start, end)
            out['batch_test'] = {
                'sym': sym,
                'start': start,
                'end': end,
                'candles_count': len(candles),
                'first': candles[0] if candles else None,
                'time': round(_t.time()-t0, 3),
            }
        except Exception as e:
            out['batch_err'] = repr(e)
        # Test with explicit at_ts (1h ago — closed candle)
        try:
            t0 = _t.time()
            ats_ms = int(_t.time() * 1000) - 3600 * 1000
            res = await asyncio.to_thread(get_delta_snapshot_fast, 'BIO/USDT', ats_ms)
            out['samples']['BIO_1h_ago'] = {
                'time': round(_t.time()-t0, 3),
                'res': res,
            }
        except Exception as e:
            out['samples']['BIO_1h_ago_err'] = repr(e)
        # Test BTC current
        try:
            t0 = _t.time()
            res = await asyncio.to_thread(get_delta_snapshot_fast, 'BTC/USDT', None)
            out['samples']['BTC_now'] = {
                'time': round(_t.time()-t0, 3),
                'res': res,
            }
        except Exception as e:
            out['samples']['BTC_now_err'] = repr(e)
    return out


@app.post("/api/cluster-delta/backfill-cdn")
async def api_cluster_delta_backfill_cdn(days: int = 14):
    """МАССОВЫЙ backfill через Binance Vision CDN (БЕЗ rate limit, БЕЗ банов).

    Скачивает daily klines ZIP'ы (https://data.binance.vision) для всех
    уникальных пар × timeframes × дни. Текущий день недоступен на CDN —
    для today используй /api/cluster-delta/backfill (REST aggTrades).

    Запускается в background thread, прогресс через /backfill-status.
    """
    global _delta_backfill_status
    if _delta_backfill_status.get('running'):
        return {'error': 'already running', 'status': _delta_backfill_status}
    days = max(1, min(60, int(days)))

    def _collect():
        from database import (_get_db, _signals, _anomalies, _confluence,
                              utcnow as _u)
        from datetime import timedelta as _td
        since = _u() - _td(days=days)
        pairs: set = set()
        try:
            for s in _signals().find(
                {'$or': [{'source': 'tradium'},
                         {'source': {'$in': ['cryptovizor', 'paper', 'cluster',
                                              'verified', 'supertrend']},
                          'received_at': {'$gte': since}}]},
                {'pair': 1}).limit(10000):
                if s.get('pair'):
                    pairs.add(s['pair'])
        except Exception:
            pass
        for col_name in ('anomalies', 'confluence'):
            try:
                for s in _get_db()[col_name].find({'detected_at': {'$gte': since}},
                                                   {'symbol': 1}).limit(5000):
                    sym = s.get('symbol', '')
                    if sym and sym.endswith('USDT') and not sym.endswith('/USDT'):
                        pairs.add(sym.replace('USDT', '/USDT'))
            except Exception:
                pass
        for col_name in ('new_strategy_signals', 'supertrend_signals',
                         ):
            try:
                ts_field = ('flip_at' if col_name == 'supertrend_signals'
                            else 'created_at')
                for s in _get_db()[col_name].find({ts_field: {'$gte': since}},
                                                   {'pair': 1}).limit(10000):
                    if s.get('pair'):
                        pairs.add(s['pair'])
            except Exception:
                pass
        return sorted(pairs)

    pairs = await asyncio.to_thread(_collect)
    if not pairs:
        return {'error': 'no pairs found', 'days': days}

    import time as _t
    _delta_backfill_status.update({
        'running': True,
        'started_at': int(_t.time()),
        'pairs_total': len(pairs),
        'pairs_done': 0,
        'candles_written': 0,
        'errors': 0,
        'last_pair': '',
        'finished_at': None,
        'mode': 'cdn',
    })

    def _worker():
        global _delta_backfill_status
        try:
            from delta_calculator import bulk_fill_pair_history_cdn
            from datetime import datetime, timezone, timedelta as _td
            from concurrent.futures import ThreadPoolExecutor, as_completed
            end_dt = datetime.now(timezone.utc)
            start_dt = end_dt - _td(days=days)

            def _one(pair):
                try:
                    res = bulk_fill_pair_history_cdn(pair, start_dt, end_dt,
                                                      timeframes=('15m', '1h'))
                    return (pair, sum(res.values()) if res else 0, None)
                except Exception as e:
                    return (pair, 0, str(e))

            # 20 workers — CDN это статика, тянет хорошо
            with ThreadPoolExecutor(max_workers=20) as ex:
                futs = {ex.submit(_one, p): p for p in pairs}
                for f in as_completed(futs):
                    pair, written, err = f.result()
                    _delta_backfill_status['pairs_done'] += 1
                    _delta_backfill_status['candles_written'] += written
                    _delta_backfill_status['last_pair'] = pair
                    if err:
                        _delta_backfill_status['errors'] += 1
            try:
                from cache_utils import journal_cache
                journal_cache.invalidate("journal_all")
            except Exception:
                pass
        except Exception:
            logging.getLogger(__name__).exception('[delta-cdn-backfill] fail')
        finally:
            _delta_backfill_status['running'] = False
            _delta_backfill_status['finished_at'] = int(_t.time())

    import threading as _th
    _th.Thread(target=_worker, daemon=True, name='delta-cdn-backfill').start()
    return {
        'started': True,
        'pairs_total': len(pairs),
        'days': days,
        'mode': 'cdn',
        'message': 'CDN backfill started (no rate limits). Poll /api/cluster-delta/backfill-status.',
    }


@app.get("/api/cluster-delta/backfill-status")
async def api_cluster_delta_backfill_status():
    """Прогресс /api/cluster-delta/backfill-all."""
    s = dict(_delta_backfill_status)
    # ETA estimate
    if s.get('running') and s.get('pairs_done', 0) > 0 and s.get('started_at'):
        import time as _t
        elapsed = max(1, int(_t.time()) - s['started_at'])
        rate = s['pairs_done'] / elapsed
        remaining = s.get('pairs_total', 0) - s['pairs_done']
        s['eta_sec'] = int(remaining / rate) if rate > 0 else None
        s['rate_pairs_per_min'] = round(rate * 60, 1)
    return s


@app.post("/api/cluster-delta/backfill")
async def api_cluster_delta_backfill(hours: int = 24, limit: int = 100):
    """Дозаполняет cluster_delta для new_strategy_signals за last N часов
    у которых ещё нет delta_15m. aggTrades живут только ~24h на Binance —
    старее не получится.

    hours: окно (max 24)
    limit: max сигналов за вызов (default 100)
    """
    from database import _get_db, utcnow
    from delta_calculator import get_delta_snapshot_async
    from datetime import timedelta as _td
    hours = max(1, min(24, int(hours)))
    limit = max(1, min(500, int(limit)))
    col = _get_db().new_strategy_signals
    since = utcnow() - _td(hours=hours)
    # Не было ещё delta — дозаполняем. Группируем по (pair, st_flip_at) чтобы 1
    # HTTP вызов на пару+момент.
    cursor = col.find({
        'created_at': {'$gte': since},
        'delta_15m': {'$exists': False},
    }).limit(limit)
    docs = list(cursor)
    if not docs:
        return {"processed": 0, "filled": 0, "hours": hours}
    by_key: dict = {}
    for d in docs:
        flip_at = d.get('st_flip_at') or d.get('created_at')
        if not flip_at:
            continue
        key = (d.get('pair', ''), int(flip_at.timestamp()) if hasattr(flip_at, 'timestamp') else 0)
        by_key.setdefault(key, []).append(d)
    filled = 0
    for (pair, ts_s), grp in by_key.items():
        if not pair or not ts_s:
            continue
        try:
            snap = await get_delta_snapshot_async(pair, ts_s * 1000)
            if not snap:
                continue
            d15 = (snap.get('15m') or {}).get('delta_pct')
            d1h = (snap.get('1h')  or {}).get('delta_pct')
            r15 = (snap.get('15m') or {}).get('resonance')
            r1h = (snap.get('1h')  or {}).get('resonance')
            ids = [doc['_id'] for doc in grp]
            def _save():
                col.update_many(
                    {'_id': {'$in': ids}},
                    {'$set': {
                        'cluster_delta': snap,
                        'delta_15m': d15, 'delta_1h': d1h,
                        'resonance_15m': r15, 'resonance_1h': r1h,
                    }},
                )
            await asyncio.to_thread(_save)
            filled += len(grp)
        except Exception as e:
            logging.getLogger(__name__).debug(f'[delta-backfill] {pair}: {e}')
            continue
    # Инвалидируем journal cache чтобы UI сразу показал новые поля
    try:
        from cache_utils import journal_cache
        journal_cache.invalidate("journal_all")
    except Exception:
        pass
    return {"processed": len(docs), "filled": filled, "hours": hours}


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
# Single-flight registry: in-progress Binance fetch'и. Concurrent users
# на одну пару шерят результат одного fetch вместо N параллельных.
_candles_inflight: dict = {}


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


def _find_compatible_cache(symbol: str, tf: str, limit: int):
    """Ищет в _candles_cache любую запись для (symbol, tf) с >= limit
    свечей. Если есть — возвращает (key, age_sec, sliced_data).
    Это даёт fuzzy match: prewarm ставит limit=200, а UI может запрашивать
    150/300/720 — раньше каждый разный limit делал cold miss, теперь
    переиспользуем существующий cache."""
    prefix = f"{symbol}|{tf}|"
    now = time.time()
    best_key = None
    best_data = None
    best_age = 9e9
    for k, (ts, data) in _candles_cache.items():
        if not k.startswith(prefix):
            continue
        if data is None or len(data) < limit:
            continue
        age = now - ts
        # Берём наиболее свежий совместимый
        if age < best_age:
            best_age = age
            best_key = k
            best_data = data
    if best_data is None:
        return None
    # Возвращаем последние `limit` свечей (новейшие)
    return (best_key, best_age, best_data[-limit:])


@app.get("/api/levels")
async def api_levels(pair: str, tf: str = "1h"):
    """📐 Levels Engine: собственные S/R зоны для пары на TF.

    Читает из Mongo computed_levels (фоновый refresher в watcher обновляет
    активные пары каждые 10 мин). Если пары нет в кэше или данные старше
    30 мин — считает on-demand (~0.5-1с: klines + свинги + кластеризация).

    Ответ: {ok, pair, tf, zones: [{low, high, mid, kind, touches,
            strength, dist_pct, ...}], source: 'cache'|'ondemand'}
    """
    from levels_engine import get_levels_cached, compute_levels, TF_CONFIG
    p = (pair or "").upper().strip()
    if "/" not in p:
        p = p.replace("USDT", "") + "/USDT"
    if tf not in TF_CONFIG:
        return {"ok": False, "error": f"tf must be one of {list(TF_CONFIG)}"}

    cached = await asyncio.to_thread(get_levels_cached, p, tf)
    if cached is not None:
        return {"ok": True, "pair": p, "tf": tf, "source": "cache", **cached}

    zones = await asyncio.to_thread(compute_levels, p, tf)
    # Сохраняем результат чтобы следующий запрос был из кэша
    try:
        def _save():
            from database import _get_db, utcnow
            _get_db().computed_levels.update_one(
                {"pair": p, "tf": tf},
                {"$set": {"zones": zones, "updated_at": utcnow()}},
                upsert=True,
            )
        await asyncio.to_thread(_save)
    except Exception:
        pass
    return {"ok": True, "pair": p, "tf": tf, "source": "ondemand", "zones": zones}


@app.get("/api/journal-candles")
async def api_journal_candles(symbol: str, tf: str = "1h", limit: int = 100,
                               response: Response = None):
    """Свечи для Lightweight Charts. Сервер-кеш по TTL per TF со
    stale-while-revalidate + fuzzy limit-match + browser Cache-Control.

    Lookup order:
      1. Точный {sym}|{tf}|{limit} hit → fresh / stale-revalidate
      2. Fuzzy compatible: cache с тем же sym+tf и >= limit → slice
         (prewarm всегда даёт limit=200, UI бывает 150/300/720 etc)
      3. Hard miss → Binance fetch (~1-3с), потом cache + bg prefetch
    """
    from exchange import get_klines_any
    key = f"{symbol}|{tf}|{limit}"
    now = time.time()
    ttl = _CANDLES_TTL_BY_TF.get((tf or "").lower(), _CANDLES_TTL_DEFAULT)
    STALE_MAX = 5.0  # 5×TTL — считаем stale, но ещё приемлемо
    hit = _candles_cache.get(key)
    pair = symbol.replace("USDT", "/USDT") if "USDT" in symbol else symbol
    # Browser cache: re-открытие того же графика берёт из браузера, не сервера.
    # max-age = половина TTL чтобы клиент чаще обновлялся чем сервер expirится.
    if response is not None:
        browser_cache_s = max(30, int(ttl / 2))
        response.headers["Cache-Control"] = f"public, max-age={browser_cache_s}"

    async def _bg_prefetch_other_tfs():
        for other_tf in ["15m", "30m", "1h", "4h", "12h", "1d"]:
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

    # Fuzzy match: ищем cache для того же (sym, tf) с >= limit свечей.
    # Это типичный кейс: prewarm ставит {sym}|{tf}|200, но юзер запросил
    # limit=150 (другой ключ → не было hit). Берём из совместимого ключа.
    fuzzy = _find_compatible_cache(symbol, tf, limit)
    if fuzzy is not None:
        fkey, fage, fdata = fuzzy
        # Если совместимая ещё свежая — возвращаем как cached
        if fage < ttl:
            return {"ok": True, "candles": fdata, "cached": True, "fuzzy_from": fkey}
        # Stale но в пределах STALE_MAX — возвращаем + bg refresh
        if fage < ttl * STALE_MAX:
            try:
                asyncio.create_task(_bg_refresh_current())
            except Exception:
                pass
            return {"ok": True, "candles": fdata, "stale": True, "fuzzy_from": fkey}

    # Hard miss — ждём Binance.
    # Single-flight: concurrent users одной пары шерят результат одного fetch.
    # Timeout 10s — если BingX/Binance тормозит, не висим на UI.
    inflight = _candles_inflight.get(key)
    if inflight is None:
        loop = asyncio.get_event_loop()
        inflight = loop.create_future()
        _candles_inflight[key] = inflight
        try:
            candles = await asyncio.wait_for(
                asyncio.to_thread(get_klines_any, pair, tf, limit),
                timeout=10.0,
            )
            inflight.set_result(candles)
        except asyncio.TimeoutError:
            inflight.set_exception(asyncio.TimeoutError("candles fetch >10s"))
            _candles_inflight.pop(key, None)
            return {"ok": False, "error": "fetch timeout >10s"}
        except Exception as _ex:
            inflight.set_exception(_ex)
            _candles_inflight.pop(key, None)
            raise
        finally:
            _candles_inflight.pop(key, None)
    else:
        try:
            candles = await asyncio.wait_for(inflight, timeout=10.0)
        except asyncio.TimeoutError:
            return {"ok": False, "error": "shared fetch timeout"}

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
    # Lazy eviction старых записей (>STALE_MAX×TTL точно бесполезны).
    # 2000 entries × ~200 candles × ~50 bytes ≈ 20 MB — комфортно для
    # active multi-symbol browsing, экономит cold fetches.
    if len(_candles_cache) > 2000:
        for k in [k for k, v in _candles_cache.items() if (now - v[0]) > ttl * STALE_MAX]:
            _candles_cache.pop(k, None)
    # Background prefetch остальных TF — юзер вероятнее всего переключит
    try:
        asyncio.create_task(_bg_prefetch_other_tfs())
    except Exception:
        pass
    return {"ok": True, "candles": data}


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
