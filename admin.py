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
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware

from config import ADMIN_USERNAME, ADMIN_PASSWORD, SECRET_KEY, BOTS
from database import get_db, Signal, Session, desc, func, get_events
from exchange import get_prices_any as _sync_get_prices, get_all_usdt_symbols as _sync_get_all_usdt_symbols, get_eth_market_context as _sync_eth_ctx, get_keltner_eth as _sync_kc_eth

from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app):
    """Гарантирует запуск watcher/userbot/bots."""
    _bg_tasks = []
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


app = FastAPI(title="Tradium Screener Admin", lifespan=lifespan)
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
        if path in ("/login", "/health", "/api/userbot-status", "/api/backfill-missed", "/api/backfill-patterns", "/api/activate-tradium-archive", "/api/backfill-tradium-charts", "/api/peek-tradium", "/api/peek-tradium-setups", "/api/peek-tradium-forum", "/api/inspect-msg-neighbors", "/api/debug-fetch-chart", "/api/reversal-meter", "/api/pending-clusters", "/api/backfill-clusters", "/api/pair-signals", "/api/fvg-signals", "/api/fvg-journal", "/api/fvg-config", "/api/fvg-scan-now", "/api/fvg-candles", "/api/conflicts", "/api/conflicts/check", "/api/smart-levels", "/api/td-quota", "/api/ai-coin-analysis", "/api/top-picks", "/api/top-picks/backfill") or path.startswith("/static"):
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
    # Сначала GridFS (переживает деплой)
    from database import get_chart
    chart_data = await asyncio.to_thread(get_chart, signal_id)
    if chart_data:
        return Response(content=chart_data, media_type="image/jpeg")

    # Фоллбэк на локальный файл
    signal = await asyncio.to_thread(
        lambda: db.query(Signal).filter(Signal.id == signal_id).first()
    )
    if not signal or not signal.chart_path:
        raise HTTPException(status_code=404)
    resolved = _resolve_chart_path(signal.chart_path)
    if not resolved:
        raise HTTPException(status_code=404)
    return FileResponse(resolved)


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


@app.get("/health")
async def health():
    """Healthcheck для Docker / uptime monitors — без авторизации."""
    try:
        from database import _signals
        _signals().estimated_document_count()
        return {"ok": True, "db": "ok"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


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
    """Список кластеров для UI вкладки "Кластеры"."""
    from database import _clusters
    from pymongo import DESCENDING
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
    all_docs = list(_clusters().find({}))
    wins = sum(1 for x in all_docs if x.get("status") == "TP")
    losses = sum(1 for x in all_docs if x.get("status") == "SL")
    open_n = sum(1 for x in all_docs if x.get("status") == "OPEN")
    mega = sum(1 for x in all_docs if x.get("strength") == "MEGA")
    strong = sum(1 for x in all_docs if x.get("strength") == "STRONG")
    total_pnl = sum((x.get("pnl_percent") or 0) for x in all_docs if x.get("status") in ("TP", "SL"))
    return {
        "items": out,
        "stats": {
            "total": len(all_docs), "wins": wins, "losses": losses, "open": open_n,
            "mega": mega, "strong": strong,
            "wr": round(wins / max(wins + losses, 1) * 100, 1),
            "sum_pnl": round(total_pnl, 1),
        },
    }


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
    """Монеты которые сейчас 1/N — ждут второго сигнала."""
    from cluster_detector import get_pending_clusters, get_config
    pending = await asyncio.to_thread(get_pending_clusters, 50)
    cfg = await asyncio.to_thread(get_config)
    return {"items": pending, "config": cfg}


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
async def api_fvg_signals(status: str = "all", limit: int = 200):
    """Forex FVG сигналы (active + history)."""
    from fvg_scanner import get_pending_fvgs, get_active_trades, get_journal, get_stats
    waiting = await asyncio.to_thread(get_pending_fvgs, 100)
    entered = await asyncio.to_thread(get_active_trades, 100)
    stats = await asyncio.to_thread(get_stats)
    return {"waiting": waiting, "entered": entered, "stats": stats}


@app.get("/api/fvg-journal")
async def api_fvg_journal(hours: int = 168, status: str = "", instrument: str = "",
                          direction: str = "", limit: int = 300):
    """Журнал Forex FVG с фильтрами."""
    from fvg_scanner import get_journal, get_stats
    items = await asyncio.to_thread(get_journal, hours, status or None,
                                    instrument or None, direction or None, limit)
    stats = await asyncio.to_thread(get_stats)
    return {"items": items, "stats": stats, "total": len(items)}


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


@app.get("/api/td-quota")
async def api_td_quota():
    """Статистика расхода TwelveData API квоты (за последние 24ч)."""
    from fvg_scanner import get_td_quota_stats
    stats = await asyncio.to_thread(get_td_quota_stats)
    return stats


@app.get("/api/top-picks")
async def api_top_picks(hours: int = 96, limit: int = 200):
    """👑 Top Picks — сигналы подтверждённые STRONG Confluence ≤ 48h."""
    from top_picks import get_all_top_picks, get_top_picks_stats
    items = await asyncio.to_thread(get_all_top_picks, hours, limit)
    stats = await asyncio.to_thread(get_top_picks_stats, 720)  # 30 days
    return {"items": items, "stats": stats, "total": len(items)}


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
    Приоритет источников:
      форекс:  TwelveData → yfinance → кеш
      остальное: yfinance → кеш
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
    # 1. Форекс → TwelveData
    if td_symbol:
        candles = await asyncio.to_thread(fetch_candles_twelvedata, td_symbol, tf, min(limit, 200))
        if candles:
            source = "twelvedata"
    # 2. yfinance (как fallback для форекса или основной для прочего)
    if not candles:
        candles = await asyncio.to_thread(fetch_candles, ticker, period, tf)
        if candles:
            source = "yfinance"
    # 3. Кеш из БД
    if candles:
        if tf == "1h":
            try:
                await asyncio.to_thread(cache_candles, key, tf, candles)
            except Exception:
                pass
    else:
        cached = await asyncio.to_thread(get_cached_candles, key, tf, 240)
        if cached:
            candles = cached
            source = "cache"
    if not candles:
        return {"ok": False, "error": f"no data (all sources failed for {key} {tf})"}
    data = [{"time": c["t"], "open": c["o"], "high": c["h"],
             "low": c["l"], "close": c["c"]} for c in candles[-limit:]]
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
    return {
        "client_connected": _tg_client.is_connected() if _tg_client else False,
        "cryptovizor": _info(last_cv),
        "tradium": _info(last_tr),
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
            "eth_ctx": _sync_eth_ctx(), "st_eth": _sync_kc_eth(),
        })

    # API для аномалий
    pass  # endpoints ниже

    if bot == "cryptovizor":
        cv_tab = tab if tab in ("watching", "active", "ai_signal", "backtest", "ai_settings") else "watching"
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
                "eth_ctx": _sync_eth_ctx(), "st_eth": _sync_kc_eth(),
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
            "eth_ctx": _sync_eth_ctx(), "st_eth": _sync_kc_eth(),
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
        "eth_ctx": _sync_eth_ctx(), "st_eth": _sync_kc_eth(),
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
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
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
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
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
    from database import _anomalies
    docs = list(_anomalies().find().sort("detected_at", -1).limit(100))
    for d in docs:
        d["_id"] = str(d["_id"])
        if d.get("detected_at"):
            d["detected_at"] = d["detected_at"].isoformat() if hasattr(d["detected_at"], "isoformat") else str(d["detected_at"])
    return {"items": docs, "eth_ctx": _sync_eth_ctx()}


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
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
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
    from database import _confluence
    docs = list(_confluence().find().sort("detected_at", -1).limit(100))
    for d in docs:
        d["_id"] = str(d["_id"])
        if d.get("detected_at"):
            d["detected_at"] = d["detected_at"].isoformat() if hasattr(d["detected_at"], "isoformat") else str(d["detected_at"])
    return {"items": docs, "eth_ctx": _sync_eth_ctx()}


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


@app.post("/api/paper/reset")
async def api_paper_reset():
    import paper_trader as pt
    pt.reset_trading()
    return {"ok": True}


@app.get("/api/journal")
async def api_journal():
    """Все сигналы из 4 источников — для вкладки Журнал."""
    from database import _signals, _anomalies, _confluence

    items = []

    # Tradium signals
    # Если pattern_triggered (DCA4 hit) — используем pattern_triggered_at как время активации,
    # чтобы «зрелый» сигнал не отфильтровывался по окну (при received_at 2 дня назад).
    for s in _signals().find({"source": "tradium"}).sort("received_at", -1):
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

    # Cryptovizor signals (только с паттерном, сортируем по времени паттерна)
    for s in _signals().find({"source": "cryptovizor", "pattern_triggered": True}).sort("pattern_triggered_at", -1):
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

    # Anomalies
    for a in _anomalies().find().sort("detected_at", -1):
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
            "at": a["detected_at"].isoformat() if hasattr(a.get("detected_at"), "isoformat") else str(a.get("detected_at", "")),
            "at_ts": int(a["detected_at"].timestamp()) if hasattr(a.get("detected_at"), "timestamp") else 0,
        })

    # Confluence
    for c in _confluence().find().sort("detected_at", -1):
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

    # Сортируем по дате (новые сверху)
    items.sort(key=lambda x: x.get("at_ts", 0), reverse=True)
    return {"items": items}


@app.get("/api/journal-candles")
async def api_journal_candles(symbol: str, tf: str = "1h", limit: int = 100):
    """Свечи для Lightweight Charts."""
    from exchange import get_klines_any
    pair = symbol.replace("USDT", "/USDT") if "USDT" in symbol else symbol
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
        })
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
    from database import _get_db
    criteria = payload.get("criteria", [])
    _get_db().settings.update_one(
        {"_id": "ai_criteria"},
        {"$set": {"criteria": criteria, "updated_at": str(__import__('datetime').datetime.utcnow())}},
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
