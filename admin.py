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
from exchange import get_prices_any as _sync_get_prices, get_all_usdt_symbols as _sync_get_all_usdt_symbols

app = FastAPI(title="Tradium Screener Admin")
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
    resp.set_cookie(
        SESSION_COOKIE,
        _make_token(username),
        max_age=SESSION_TTL,
        httponly=True,
        samesite="lax",
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
    if bot == "cryptovizor":
        cv_tab = tab if tab in ("watching", "active", "volume") else "watching"
        if cv_tab == "watching":
            query = query.filter(Signal.status == "СЛЕЖУ")
        elif cv_tab == "active":
            query = query.filter(Signal.status == "ПАТТЕРН")
        elif cv_tab == "volume":
            query = query.filter(Signal.status == "VOLUME")
        cv_watching = db.query(Signal).filter(Signal.source == "cryptovizor").filter(Signal.status == "СЛЕЖУ").count()
        cv_active = db.query(Signal).filter(Signal.source == "cryptovizor").filter(Signal.status == "ПАТТЕРН").count()
        cv_volume = db.query(Signal).filter(Signal.source == "cryptovizor").filter(Signal.status == "VOLUME").count()
        cv_stats = {"watching": cv_watching, "active": cv_active, "volume": cv_volume}
        signals = query.order_by(desc(Signal.received_at)).limit(200).all()
        return templates.TemplateResponse(request, "signals.html", {
            "signals": signals,
            "total": len(signals),
            "bot": bot, "bots": BOTS,
            "cv_tab": cv_tab, "cv_stats": cv_stats,
            "tab": cv_tab, "stats": {}, "summary": None,
            "pages": 1, "page": 1, "pairs": [],
            "filter_pair": "", "filter_direction": "", "filter_has_chart": "",
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
                "received_at": s.received_at.isoformat() if s.received_at else None,
                "exit_price": s.exit_price, "pnl_percent": s.pnl_percent,
                "ai_score": s.ai_score, "ai_verdict": s.ai_verdict,
            }
            for s in signals
        ],
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
        "received_at": s.received_at.isoformat() if s.received_at else None,
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
            "received_at": s.received_at.isoformat() if s.received_at else None,
        }
        for s in signals
    ]
