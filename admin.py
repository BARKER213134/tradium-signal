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
from exchange import get_prices_any as _sync_get_prices, get_all_usdt_symbols as _sync_get_all_usdt_symbols, get_eth_market_context as _sync_eth_ctx

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
        if path == "/login" or path == "/health" or path.startswith("/static"):
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
    if bot == "anomaly":
        return templates.TemplateResponse(request, "signals.html", {
            "signals": [],
            "total": 0,
            "bot": bot, "bots": BOTS,
            "cv_tab": "", "cv_stats": {},
            "tab": tab, "stats": {}, "summary": None,
            "pages": 1, "page": 1, "pairs": [],
            "filter_pair": "", "filter_direction": "", "filter_has_chart": "",
            "eth_ctx": _sync_eth_ctx(),
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
                "eth_ctx": _sync_eth_ctx(),
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
            "eth_ctx": _sync_eth_ctx(),
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
        "eth_ctx": _sync_eth_ctx(),
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
        "eth_ctx": _sync_eth_ctx(),
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

    prompt = (
        f"Ты — профессиональный крипто-трейдер. Дай ПОЛНЫЙ анализ сделки.\n\n"
        f"Монета: {pair}\n"
        f"Направление: {direction}\n"
        f"Паттерн: {pattern}\n"
        f"Entry: {entry}\n"
        f"Текущая цена: {current}\n"
        f"PnL: {pnl}%\n\n"
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
        f"Описание сетапа, структура рынка, уровни. 4-6 предложений.\n\n"
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

    prompt = (
        f"Ты — крипто-аналитик. Разбери закрытую сделку.\n\n"
        f"Монета: {pair}\n"
        f"Направление: {direction}\n"
        f"Entry: {entry}\n"
        f"TP: {tp} | SL: {sl}\n"
        f"Результат: {result_text}\n"
        f"Exit: {exit_price} | PnL: {pnl}%\n\n"
        f"Объясни:\n"
        f"1. Почему сделка {'отработала' if status == 'TP' else 'не отработала'}\n"
        f"2. Что было сделано правильно\n"
        f"3. Что можно улучшить\n"
        f"4. Вывод — урок из этой сделки\n\n"
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

    prompt = (
        f"Ты — профессиональный крипто-аналитик. Разбери аномалию на фьючерсном рынке.\n\n"
        f"Монета: {symbol.replace('USDT','')}/USDT\n"
        f"Цена: {price}\n"
        f"Направление сигнала: {direction}\n"
        f"Score аномалии: {score}/15\n"
        f"Количество индикаторов: {len(anomalies)}\n\n"
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
async def api_anomalies_backtest():
    """Бектест аномалий: проверяет движение цены после обнаружения."""
    from database import _anomalies
    from exchange import get_futures_prices_only

    docs = list(_anomalies().find().sort("detected_at", -1).limit(200))
    if not docs:
        return {"ok": True, "results": [], "summary": {}}

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
    }


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
        "id": s.id, "pair": s.pair, "direction": s.direction,
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
