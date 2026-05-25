import asyncio
import logging
import os
from pathlib import Path
from telethon import TelegramClient, events
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument

from config import (
    API_ID, API_HASH, PHONE,
    SOURCE_GROUP_ID, CHART_WAIT_SECONDS, CHARTS_DIR,
    BOT2_SOURCE_GROUP, BOT2_NAME,
    TRADIUM_SETUP_TOPIC_ID,
)
from database import SessionLocal, Signal, utcnow, log_event
from parser import parse_signal, format_signal_message
from parser_cryptovizor import parse_cryptovizor_message
from ai_analyzer import analyze_chart, analyze_signal_quality, merge_signal_data
from exchange import get_prices, get_prices_any, get_futures_prices_only

logger = logging.getLogger(__name__)

# Глобальные ссылки
_bot = None
_admin_chat_id = None
_tg_client = None  # экспортируется для /api/sync

# Diagnostics — для /api/userbot/status и admin lifespan watchdog'а.
# Позволяет видеть из админки: когда был последний setup,
# сколько раз переподключались, какая последняя ошибка.
_last_setup_at = None          # datetime успешного setup
_last_setup_error = None       # str последней ошибки из _setup_telethon_client
_last_disconnect_at = None     # datetime последнего run_until_disconnected exit
_reconnect_count = 0           # сколько раз перезапускал setup
_cryptovizor_id_resolved = None  # int если CV handler зарегистрирован, иначе None
_cryptovizor_resolve_method = None  # 'env' | 'iter_dialogs' | None
_handlers_registered = {"tradium": False, "cryptovizor": False, "kl": False}

# ── Login flow state (для UI re-login через браузер) ─────────────────
# Когда session expired/blocked, админ может через UI:
# 1. POST /api/userbot/login/start { phone } → шлём SMS код
# 2. POST /api/userbot/login/code { code } → авторизация
# 3. POST /api/userbot/login/2fa { password } → если двухфакторка
# Pending клиент хранится в _login_state до завершения flow.
_login_state: dict = {
    "client": None,         # активный TelegramClient в процессе авторизации
    "phone": None,
    "phone_code_hash": None,
    "needs_2fa": False,
    "started_at": None,
}


async def login_start(phone: str) -> dict:
    """Шаг 1: послать код подтверждения на phone (Telegram пришлёт SMS/in-app).
    Возвращает {ok, message} или {ok: False, error}.

    Все блокирующие операции обёрнуты в timeout/to_thread — login через UI
    не должен ронять event loop, даже если Telegram/disk залипли.
    """
    global _login_state
    from telethon import TelegramClient
    from datetime import datetime, timezone
    # Закрыть предыдущую попытку если была — с timeout, иначе залипший
    # disconnect утащит весь event loop
    try:
        if _login_state.get("client"):
            await asyncio.wait_for(_login_state["client"].disconnect(), timeout=5.0)
    except (asyncio.TimeoutError, Exception):
        pass
    # Удаляем старый session_userbot.session чтобы создать новый — file I/O в thread
    here = os.path.dirname(os.path.abspath(__file__))
    session_path = os.path.join(here, "session_userbot")
    def _cleanup_files():
        try:
            for ext in ("", ".session", ".session-journal"):
                p = session_path + ext
                if os.path.exists(p):
                    os.remove(p)
        except Exception as _e:
            logger.warning(f"[login] cleanup session fail: {_e}")
    try:
        await asyncio.wait_for(asyncio.to_thread(_cleanup_files), timeout=5.0)
    except asyncio.TimeoutError:
        logger.warning("[login] session cleanup timed out — продолжаем")
    # Создаём новый клиент с новой сессией
    client = TelegramClient(session_path, API_ID, API_HASH)
    try:
        await asyncio.wait_for(client.connect(), timeout=15.0)
    except asyncio.TimeoutError:
        return {"ok": False, "error": "connect timeout (15s) — Telegram/network не отвечает"}
    except Exception as e:
        return {"ok": False, "error": f"connect failed: {e}"}
    try:
        sent = await asyncio.wait_for(client.send_code_request(phone), timeout=20.0)
    except asyncio.TimeoutError:
        try:
            await asyncio.wait_for(client.disconnect(), timeout=5.0)
        except Exception:
            pass
        return {"ok": False, "error": "send_code timeout (20s)"}
    except Exception as e:
        try:
            await asyncio.wait_for(client.disconnect(), timeout=5.0)
        except Exception:
            pass
        return {"ok": False, "error": f"send_code: {e}"}
    _login_state.update({
        "client": client,
        "phone": phone,
        "phone_code_hash": sent.phone_code_hash,
        "needs_2fa": False,
        "started_at": datetime.now(timezone.utc),
    })
    return {"ok": True, "message": f"Код отправлен на {phone}. Проверь Telegram."}


async def login_complete(code: str, password: str | None = None) -> dict:
    """Шаг 2: завершить login кодом из Telegram (+ опционально 2FA password).

    Все блокирующие операции (sign_in, disconnect, sync Mongo, file I/O)
    обёрнуты в wait_for/to_thread, чтобы залипший Telegram или Mongo
    не утащили event loop платформы.
    """
    global _tg_client, _login_state, _last_setup_at, _last_setup_error
    from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError
    from datetime import datetime, timezone
    client = _login_state.get("client")
    phone = _login_state.get("phone")
    phone_code_hash = _login_state.get("phone_code_hash")
    if not (client and phone and phone_code_hash):
        return {"ok": False, "error": "no_pending_login — сначала вызови /login/start"}
    try:
        await asyncio.wait_for(
            client.sign_in(phone, code, phone_code_hash=phone_code_hash),
            timeout=30.0,
        )
    except asyncio.TimeoutError:
        return {"ok": False, "error": "sign_in timeout (30s) — Telegram не отвечает"}
    except SessionPasswordNeededError:
        if not password:
            _login_state["needs_2fa"] = True
            return {"ok": False, "needs_2fa": True,
                    "error": "Для аккаунта включена 2FA — нужен пароль"}
        try:
            await asyncio.wait_for(client.sign_in(password=password), timeout=30.0)
        except asyncio.TimeoutError:
            return {"ok": False, "error": "2fa sign_in timeout (30s)"}
        except Exception as e:
            return {"ok": False, "error": f"2fa fail: {e}"}
    except PhoneCodeInvalidError:
        return {"ok": False, "error": "Неверный код"}
    except Exception as e:
        return {"ok": False, "error": f"sign_in: {e}"}
    # Авторизация прошла → disconnect старого pending клиента (с timeout!)
    try:
        await asyncio.wait_for(client.disconnect(), timeout=5.0)
    except (asyncio.TimeoutError, Exception):
        pass
    # Persist session bytes в Mongo — sync I/O вынесен в thread с timeout.
    # Это была одна из причин зависаний: open()+read()+update_one() в event loop.
    here = os.path.dirname(os.path.abspath(__file__))
    session_path = os.path.join(here, "session_userbot.session")

    def _persist_session_sync():
        if not os.path.exists(session_path):
            return None
        from database import _get_db
        with open(session_path, "rb") as f:
            data = f.read()
        _get_db().system.update_one(
            {"_id": "telethon_session"},
            {"$set": {"data": data, "size": len(data),
                      "updated_at": datetime.now(timezone.utc)}},
            upsert=True,
        )
        return len(data)

    try:
        size = await asyncio.wait_for(asyncio.to_thread(_persist_session_sync), timeout=15.0)
        if size:
            logger.info(f"✅ Telethon session saved to Mongo ({size} bytes)")
    except asyncio.TimeoutError:
        logger.warning("[login] persist to Mongo timed out (15s) — session не сохранена в Atlas")
    except Exception as e:
        logger.warning(f"[login] persist to Mongo fail: {e}")
    _login_state.update({
        "client": None, "phone": None, "phone_code_hash": None,
        "needs_2fa": False, "started_at": None,
    })
    # Trigger supervisor reconnect — отключаем старый _tg_client с timeout.
    # Раньше: if залип disconnect, эта строка вешала весь POST handler
    # (а заодно и event loop, т.к. pending Future никогда не разрешался).
    try:
        if _tg_client:
            await asyncio.wait_for(_tg_client.disconnect(), timeout=5.0)
    except (asyncio.TimeoutError, Exception):
        pass
    _last_setup_error = None
    return {"ok": True, "message": "Авторизация успешна! Сессия сохранена. Userbot подключится автоматически."}


def get_login_state() -> dict:
    """Текущее состояние login flow для UI."""
    return {
        "in_progress": bool(_login_state.get("phone_code_hash")),
        "phone": _login_state.get("phone"),
        "needs_2fa": _login_state.get("needs_2fa", False),
        "started_at": _login_state.get("started_at"),
    }

# Буфер: message_id текста -> Signal.id в БД
# Ждём следующее фото для этого сигнала
_pending_charts: dict[int, int] = {}  # telegram_msg_id -> signal DB id


def set_bot(bot, admin_chat_id: int):
    global _bot, _admin_chat_id
    _bot = bot
    _admin_chat_id = admin_chat_id


def ensure_charts_dir():
    Path(CHARTS_DIR).mkdir(parents=True, exist_ok=True)


async def handle_text_message(event, client):
    """Обрабатывает текстовое сообщение с сигналом."""
    text = event.raw_text
    if not text or len(text) < 10:
        return

    message_id = event.message.id
    db = SessionLocal()
    try:
        # Дубликат?
        existing = db.query(Signal).filter(Signal.message_id == message_id).first()
        if existing:
            return

        # Парсим текст
        parsed = parse_signal(text)

        # Только полноценные Tradium Setups: есть trend + entry + tp + sl
        if not (parsed.get("trend") and parsed.get("entry")
                and parsed.get("tp1") and parsed.get("sl")):
            logger.debug(f"msg={message_id} — не Tradium setup, пропускаем")
            return

        signal = Signal(
            source="tradium",
            message_id=message_id,
            raw_text=text,
            source_group_id=str(SOURCE_GROUP_ID),
            text_pair=parsed.get("pair"),
            text_direction=parsed.get("direction"),
            text_entry=parsed.get("entry"),
            text_sl=parsed.get("sl"),
            text_tp1=parsed.get("tp1"),
            text_tp2=parsed.get("tp2"),
            text_tp3=parsed.get("tp3"),
            pair=parsed.get("pair"),
            direction=parsed.get("direction"),
            entry=parsed.get("entry"),
            sl=parsed.get("sl"),
            tp1=parsed.get("tp1"),
            tp2=parsed.get("tp2"),
            tp3=parsed.get("tp3"),
            timeframe=parsed.get("timeframe"),
            risk_reward=parsed.get("risk_reward"),
            risk_percent=parsed.get("risk_percent"),
            amount=parsed.get("amount"),
            tp_percent=parsed.get("tp_percent"),
            sl_percent=parsed.get("sl_percent"),
            trend=parsed.get("trend"),
            comment=parsed.get("comment"),
            setup_number=parsed.get("setup_number"),
            has_chart=False,
            chart_analyzed=False,
            received_at=utcnow()
        )
        db.add(signal)
        db.commit()
        db.refresh(signal)

        logger.info(f"Текст сигнала #{signal.id}: {signal.pair} {signal.direction} (msg_id={message_id})")
        log_event(
            signal.id, "created",
            data={"pair": signal.pair, "direction": signal.direction, "source": "userbot"},
            message="Сигнал получен из Telegram",
        )
        try:
            from admin import broadcast_event
            broadcast_event("signal_new", {"id": signal.id})
        except Exception:
            pass

        # Регистрируем ожидание графика
        _pending_charts[message_id] = signal.id
        logger.info(f"Ожидаем график для сигнала #{signal.id}...")

        # Через CHART_WAIT_SECONDS + буфер — если графика нет, отправляем без него
        asyncio.create_task(
            send_signal_after_timeout(signal.id, message_id)
        )

    finally:
        db.close()


async def handle_photo_message(event, client):
    """Обрабатывает фото — ищет к какому сигналу оно относится."""
    message_id = event.message.id

    # Ищем в буфере ожидания — берём САМЫЙ РАННИЙ pending сигнал (FIFO),
    # чтобы при двух подряд текстовых сигналах первый график ушёл к первому тексту.
    if not _pending_charts:
        logger.debug("Получено фото, но нет ожидающих сигналов")
        return

    first_text_msg_id = min(_pending_charts.keys())
    signal_db_id = _pending_charts.pop(first_text_msg_id, None)

    if not signal_db_id:
        return

    logger.info(f"График для сигнала #{signal_db_id} (фото msg_id={message_id})")

    # Скачиваем фото
    ensure_charts_dir()
    chart_filename = f"{signal_db_id}_{message_id}.jpg"
    chart_path = os.path.join(CHARTS_DIR, chart_filename)

    try:
        await client.download_media(event.message, file=chart_path)
        logger.info(f"График скачан: {chart_path}")
    except Exception as e:
        logger.error(f"Ошибка скачивания графика: {e}")
        return

    # Сохраняем в GridFS (переживёт деплой)
    try:
        from database import save_chart
        with open(chart_path, "rb") as f:
            save_chart(signal_db_id, f.read(), filename=chart_filename)
        logger.info(f"График сохранён в GridFS: #{signal_db_id}")
    except Exception as e:
        logger.error(f"GridFS save error: {e}")

    # Сохраняем путь к графику в БД
    db = SessionLocal()
    try:
        signal = db.query(Signal).filter(Signal.id == signal_db_id).first()
        if not signal:
            return

        signal.has_chart = True
        signal.chart_message_id = message_id
        signal.chart_path = chart_path
        signal.chart_received_at = utcnow()
        db.commit()

        # Анализируем через Claude Vision
        logger.info(f"Запускаю AI анализ графика для сигнала #{signal_db_id}")
        chart_data = await asyncio.to_thread(analyze_chart_sync, chart_path)

        # Сохраняем данные из графика
        signal.chart_analyzed = True
        signal.chart_ai_raw = chart_data.get("_raw", "")
        signal.chart_pair = chart_data.get("pair")
        signal.chart_direction = chart_data.get("direction")
        signal.chart_entry = _to_float(chart_data.get("entry"))
        signal.chart_sl = _to_float(chart_data.get("sl"))
        signal.chart_tp1 = _to_float(chart_data.get("tp1"))
        signal.chart_tp2 = _to_float(chart_data.get("tp2"))
        signal.chart_tp3 = _to_float(chart_data.get("tp3"))
        signal.chart_notes = chart_data.get("notes") or chart_data.get("pattern", "")

        # Мержим текст + график в финальные поля
        text_data = {
            "pair": signal.text_pair,
            "direction": signal.text_direction,
            "entry": signal.text_entry,
            "sl": signal.text_sl,
            "tp1": signal.text_tp1,
            "tp2": signal.text_tp2,
            "tp3": signal.text_tp3,
        }
        merged = merge_signal_data(text_data, chart_data)
        signal.pair = merged.get("pair")
        signal.direction = merged.get("direction")
        signal.entry = merged.get("entry")
        signal.sl = merged.get("sl")
        signal.tp1 = merged.get("tp1")
        signal.tp2 = merged.get("tp2")
        signal.tp3 = merged.get("tp3")
        db.commit()

        # Сохраняем DCA уровни с графика
        signal.dca1 = _to_float(chart_data.get("dca1"))
        signal.dca2 = _to_float(chart_data.get("dca2"))
        signal.dca3 = _to_float(chart_data.get("dca3"))
        signal.dca4 = _to_float(chart_data.get("dca4"))
        db.commit()

        logger.info(
            f"AI #{signal_db_id}: {signal.pair} {signal.direction} DCA4={signal.dca4}"
        )
        if chart_data.get("_error"):
            log_event(
                signal_db_id, "ai_failed",
                data={"error": chart_data.get("_error"), "attempts": chart_data.get("_attempts")},
                message=f"AI анализ провален: {chart_data.get('_error')}",
            )
        else:
            log_event(
                signal_db_id, "ai_analyzed",
                data={
                    "dca4": signal.dca4, "tp1": signal.tp1, "sl": signal.sl,
                    "attempts": chart_data.get("_attempts", 1),
                },
                message=f"График проанализирован, DCA4={signal.dca4}",
            )

            # ─ AI quality score ─
            try:
                quality = await analyze_signal_quality(chart_path, {
                    "pair": signal.pair,
                    "direction": signal.direction,
                    "timeframe": signal.timeframe,
                    "entry": signal.entry,
                    "sl": signal.sl,
                    "tp1": signal.tp1,
                    "dca4": signal.dca4,
                    "risk_reward": signal.risk_reward,
                    "trend": signal.trend,
                    "pattern": signal.chart_notes,
                })
                if quality and "score" in quality:
                    signal.ai_score = quality["score"]
                    signal.ai_confidence = quality.get("confidence")
                    signal.ai_reasoning = quality.get("reasoning")
                    signal.ai_risks = quality.get("risks") or []
                    signal.ai_verdict = quality.get("verdict")
                    db.commit()
                    log_event(
                        signal_db_id, "ai_scored",
                        data={
                            "score": signal.ai_score,
                            "confidence": signal.ai_confidence,
                            "verdict": signal.ai_verdict,
                        },
                        message=f"AI оценка: {signal.ai_score}/100 ({signal.ai_verdict})",
                    )
                    logger.info(f"AI quality #{signal_db_id}: {signal.ai_score}/100 {signal.ai_verdict}")
            except Exception as e:
                logger.error(f"AI quality ошибка #{signal_db_id}: {e}")
        # НЕ форвардим сразу — watcher отправит при достижении DCA4

    finally:
        db.close()


def analyze_chart_sync(chart_path: str) -> dict:
    """Синхронная обёртка для вызова в thread."""
    import asyncio as _asyncio
    loop = _asyncio.new_event_loop()
    try:
        return loop.run_until_complete(analyze_chart(chart_path))
    finally:
        loop.close()


async def send_signal_after_timeout(signal_db_id: int, text_msg_id: int):
    """Если через таймаут графика нет — просто убираем из pending."""
    await asyncio.sleep(CHART_WAIT_SECONDS + 3)
    if text_msg_id in _pending_charts:
        _pending_charts.pop(text_msg_id, None)
        logger.info(f"Таймаут графика для #{signal_db_id} — без графика")


async def forward_signal(signal_db_id: int, with_chart: bool):
    """Отправляет сигнал в Telegram бот."""
    if not _bot or not _admin_chat_id:
        return

    db = SessionLocal()
    try:
        signal = db.query(Signal).filter(Signal.id == signal_db_id).first()
        if not signal or signal.is_forwarded:
            return

        # Формируем текст
        direction = signal.direction or "—"
        emoji = "🟢" if direction == "BUY" else "🔴" if direction == "SELL" else "⚪"

        ai_block = ""
        if signal.chart_analyzed and signal.chart_notes:
            ai_block = f"\n🤖 <b>AI с графика:</b> <i>{signal.chart_notes[:200]}</i>"

        text = (
            f"{emoji} <b>СИГНАЛ #{signal.id}</b>\n\n"
            f"📊 <b>Пара:</b> {signal.pair or '—'}\n"
            f"📈 <b>Направление:</b> {direction}\n"
            f"🎯 <b>Вход:</b> {signal.entry or '—'}\n"
            f"🛑 <b>Stop Loss:</b> {signal.sl or '—'}\n"
            f"✅ <b>TP1:</b> {signal.tp1 or '—'}\n"
            f"✅ <b>TP2:</b> {signal.tp2 or '—'}\n"
            f"✅ <b>TP3:</b> {signal.tp3 or '—'}\n"
            f"{ai_block}\n"
            f"{'📸 <i>С графиком</i>' if with_chart else '📝 <i>Только текст</i>'}"
        )

        # Отправляем фото + текст или просто текст
        if with_chart and signal.chart_path and os.path.exists(signal.chart_path):
            from aiogram.types import FSInputFile
            photo = FSInputFile(signal.chart_path)
            await _bot.send_photo(
                _admin_chat_id,
                photo=photo,
                caption=text,
                parse_mode="HTML"
            )
        else:
            await _bot.send_message(_admin_chat_id, text, parse_mode="HTML")

        signal.is_forwarded = True
        signal.forwarded_at = utcnow()
        db.commit()
        logger.info(f"Сигнал #{signal_db_id} отправлен {'с графиком' if with_chart else 'без графика'}")

    except Exception as e:
        logger.error(f"Ошибка отправки сигнала #{signal_db_id}: {e}")
    finally:
        db.close()


def _to_float(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except Exception:
        return None


async def _setup_telethon_client():
    """Создаёт Telethon клиент, резолвит каналы, регистрирует хендлеры.
    Возвращает подключённый клиент или None если не получилось."""
    global _tg_client, _last_setup_at, _last_setup_error
    session_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "session_userbot")
    client = TelegramClient(session_path, API_ID, API_HASH)
    try:
        await client.connect()
    except Exception as e:
        logger.error(f"[userbot] connect failed: {e}")
        _last_setup_error = f"connect: {e}"
        return None
    if not await client.is_user_authorized():
        _last_setup_error = "not_authorized (session revoked?)"
        logger.error(
            "❌ Userbot не авторизован! Запусти `python authorize.py` чтобы войти. "
            "Сервер продолжит работать без live-сигналов из Telegram."
        )
        await client.disconnect()
        return None
    _tg_client = client
    _last_setup_at = utcnow()
    _last_setup_error = None
    logger.info("✅ Userbot подключён")

    # Прогреваем кэш Tradium
    try:
        await client.get_entity(SOURCE_GROUP_ID)
    except Exception as e:
        logger.warning(f"[userbot] Tradium entity warmup failed: {e}")

    # Резолв Cryptovizor: сначала пробуем CRYPTOVIZOR_CHANNEL_ID env (надёжно),
    # затем fallback на iter_dialogs по имени с 3 retry.
    # 02.05.2026: name resolution часто фейлит при reconnect (iter_dialogs
    # возвращает короткий список). CRYPTOVIZOR_CHANNEL_ID=5703939817 для
    # 'TRENDS Cryptovizor' (нашли через прямой Telethon login). Tradium
    # уже использует raw ID (SOURCE_GROUP_ID), CV теперь так же.
    global _cryptovizor_id_resolved, _cryptovizor_resolve_method
    cryptovizor_id = None

    # ── Шаг 1: всегда проходим iter_dialogs для прогрева entity cache ─────
    # Telethon хранит access_hash в session. Если канал никогда не был
    # explicitly доступен (через iter_dialogs / get_entity / прошлые messages),
    # то client.get_messages(channel_id) или events.NewMessage(chats=channel_id)
    # падают с "Could not find the input entity". Cold cache problem.
    # iter_dialogs форсит populate cache для всех диалогов аккаунта.
    cv_id_env = os.getenv("CRYPTOVIZOR_CHANNEL_ID", "").strip()
    cv_id_from_env = None
    if cv_id_env:
        try:
            raw_id = int(cv_id_env)
            if raw_id > 0 and len(str(raw_id)) >= 9:
                cv_id_from_env = int(f"-100{raw_id}")
                logger.info(f"[userbot] CV из env auto-fixed: raw={raw_id} → {cv_id_from_env}")
            else:
                cv_id_from_env = raw_id
        except ValueError:
            logger.warning(f"[userbot] CRYPTOVIZOR_CHANNEL_ID '{cv_id_env}' не int — игнор")

    # Прогрев: iter_dialogs всегда (max 3 попытки). Параллельно ищем CV
    # по имени — но ТОЛЬКО channels/megagroups (исключаем users/bots с
    # именем как у канала). Pulse mismatch 10:10 показал что без фильтра
    # iter_dialogs возвращал id=5703939817 (positive) — это user/bot,
    # не канал. Handler регистрировался на user → push'и от канала
    # (PeerChannel) не матчили фильтр.
    cv_from_dialogs = None
    cv_dialog_name = (BOT2_SOURCE_GROUP or "TRENDS Cryptovizor").lower()
    for attempt in range(3):
        try:
            dialog_count = 0
            async for d in client.iter_dialogs():
                dialog_count += 1
                # ФИЛЬТР: только channel или megagroup. is_user/is_bot — исключаем
                if cv_dialog_name in (d.name or "").lower():
                    is_chan = bool(getattr(d, "is_channel", False))
                    is_user = bool(getattr(d, "is_user", False))
                    logger.info(
                        f"[userbot] dialog match '{d.name}': id={d.id} "
                        f"is_channel={is_chan} is_user={is_user}"
                    )
                    if is_chan and not is_user:
                        cv_from_dialogs = d.id
                        logger.info(f"✅ CV channel найден: id={d.id}")
                        break  # Берём первый channel-match, не перезаписываем
            logger.info(f"[userbot] iter_dialogs прогрел {dialog_count} диалогов")
            if cv_from_dialogs is not None:
                break
        except Exception as e:
            logger.error(f"[userbot] iter_dialogs fail (attempt {attempt+1}): {e}")
            await asyncio.sleep(5)

    # Шаг 2: выбор финального ID. Приоритет — iter_dialogs (channel-фильтр),
    # затем env (auto-fixed). iter_dialogs возвращает channel в формате
    # -100<raw>, env auto-fix даёт то же самое — должны совпасть.
    if cv_from_dialogs is not None:
        cryptovizor_id = cv_from_dialogs
        _cryptovizor_resolve_method = "iter_dialogs"
        if cv_id_from_env is not None and cv_id_from_env != cv_from_dialogs:
            logger.warning(
                f"[userbot] env id {cv_id_from_env} != dialogs id "
                f"{cv_from_dialogs} — используем dialogs"
            )
    elif cv_id_from_env is not None:
        cryptovizor_id = cv_id_from_env
        _cryptovizor_resolve_method = "env"
        logger.info(f"✅ CV из env (iter_dialogs channel не нашёл): id={cryptovizor_id}")
    else:
        logger.error(
            "[userbot] CV channel НЕ найден ни в iter_dialogs ни в env — "
            "handler НЕ зарегистрирован. Задайте CRYPTOVIZOR_CHANNEL_ID или "
            "проверьте что аккаунт есть в канале."
        )

    # ── Handler Tradium (текст/фото) — только топик "Trade Setup Screener" ──
    @client.on(events.NewMessage(chats=SOURCE_GROUP_ID))
    async def handler(event):
        try:
            msg = event.message
            # Фильтр по форум-топику: пропускаем всё что не из Trade Setup Screener
            if TRADIUM_SETUP_TOPIC_ID and msg.reply_to:
                top_id = getattr(msg.reply_to, "reply_to_top_id", None) or msg.reply_to.reply_to_msg_id
                if top_id != TRADIUM_SETUP_TOPIC_ID:
                    return
            elif TRADIUM_SETUP_TOPIC_ID and not msg.reply_to:
                # Сообщения без reply_to = General topic, пропускаем
                return
            is_photo = isinstance(msg.media, MessageMediaPhoto)
            is_doc_image = (
                isinstance(msg.media, MessageMediaDocument) and
                msg.media.document.mime_type.startswith("image/")
            ) if msg.media else False
            if is_photo or is_doc_image:
                await handle_photo_message(event, client)
            elif msg.raw_text and len(msg.raw_text.strip()) > 5:
                await handle_text_message(event, client)
        except Exception:
            logger.exception("[userbot] Tradium handler crashed")

    logger.info(f"👂 Слушаем Tradium группу: {SOURCE_GROUP_ID} (topic {TRADIUM_SETUP_TOPIC_ID})")

    # ── BACKFILL Tradium: подтягиваем пропущенные Trade Setup сообщения ──
    # User: 'тредиум проврь теперь' — Tradium последний 28h назад. Может
    # были пропущены сигналы во время watchdog-loop'а. Читаем 100 последних
    # сообщений из группы, фильтруем по топику Trade Setup Screener,
    # вызываем handle_text_message для текстов (уже дедуплицирует по msg_id).
    # Photos пропускаем — AI analysis затратный, лучше лайв.
    try:
        from database import _events as _ev_tr
        tr_imported = 0
        tr_skipped_topic = 0
        tr_skipped_dup = 0
        tr_photos = 0

        msgs = await asyncio.wait_for(
            client.get_messages(SOURCE_GROUP_ID, limit=100), timeout=30.0,
        )
        for m in reversed(msgs):
            # Фильтр по топику
            if not m.reply_to:
                tr_skipped_topic += 1
                continue
            top_id = (getattr(m.reply_to, "reply_to_top_id", None)
                      or m.reply_to.reply_to_msg_id)
            if top_id != TRADIUM_SETUP_TOPIC_ID:
                tr_skipped_topic += 1
                continue

            # Photos — пропускаем (требуют AI)
            is_photo = isinstance(m.media, MessageMediaPhoto)
            is_doc_image = (
                isinstance(m.media, MessageMediaDocument) and
                m.media.document.mime_type.startswith("image/")
            ) if m.media else False
            if is_photo or is_doc_image:
                tr_photos += 1
                continue

            # Текстовые: handle_text_message сам проверит дубликат по message_id
            if not m.raw_text or len(m.raw_text.strip()) <= 5:
                continue

            # Эмулируем event-like объект для handle_text_message.
            # У него используется только event.raw_text и event.message.id.
            class _FakeEvent:
                raw_text = m.raw_text
                message = m
            try:
                await handle_text_message(_FakeEvent(), client)
                tr_imported += 1
            except Exception as e:
                logger.debug(f"[tr-backfill] handle fail msg_id={m.id}: {e}")

        logger.info(
            f"[tr-backfill] imported={tr_imported} photos_skipped={tr_photos} "
            f"non_topic={tr_skipped_topic}"
        )
        try:
            await asyncio.to_thread(_ev_tr().insert_one, {
                "at": utcnow(),
                "type": "userbot_tradium_backfill_done",
                "data": {
                    "imported": tr_imported,
                    "photos_skipped": tr_photos,
                    "non_topic_skipped": tr_skipped_topic,
                    "msgs_seen": len(msgs),
                    "topic_id": TRADIUM_SETUP_TOPIC_ID,
                },
            })
        except Exception:
            pass
    except Exception as e:
        logger.warning(f"[tr-backfill] fail: {e}")
        try:
            from database import _events as _ev_tr
            await asyncio.to_thread(_ev_tr().insert_one, {
                "at": utcnow(),
                "type": "userbot_tradium_backfill_fail",
                "data": {"error": f"{type(e).__name__}: {str(e)[:200]}"},
            })
        except Exception:
            pass

    # ── Handler Key Levels (топики 3086 SUPPORT / 3088 RANGES / 3091 RESISTANCE) ──
    KL_TOPICS = {3086, 3088, 3091}

    @client.on(events.NewMessage(chats=SOURCE_GROUP_ID))
    async def kl_handler(event):
        try:
            msg = event.message
            if not msg.raw_text:
                return
            if not msg.reply_to:
                return
            top_id = getattr(msg.reply_to, "reply_to_top_id", None) or msg.reply_to.reply_to_msg_id
            if top_id not in KL_TOPICS:
                return
            # Парсим и сохраняем
            try:
                from key_levels import parse_key_level, save_key_level
                parsed = parse_key_level(msg.raw_text, topic_id=top_id)
                if parsed:
                    save_key_level(parsed, message_id=msg.id)
            except Exception as e:
                logger.warning(f"[userbot] KL parse/save fail: {e}")
        except Exception:
            logger.exception("[userbot] KL handler crashed")

    logger.info(f"👂 Слушаем Key Levels топики: {KL_TOPICS}")

    # ── Handler Cryptovizor ───────────────────────────────────────
    # КРИТИЧНО: events.NewMessage(chats=int) трактует int как PeerUser
    # если положительный (Telethon entity routing). Когда от канала
    # приходит push, peer у события — PeerChannel(channel_id=raw),
    # и фильтр chats=positive_id не матчит → handler не вызывается.
    #
    # Pulse mismatch 2026-05-04 09:54 показал: канал постит активно
    # (CTK/VVV/KSM в 09:30), но events.NewMessage handler не срабатывал.
    # Решение: передаём entity объект (cached в session через iter_dialogs),
    # Telethon резолвит правильный Peer для фильтра.
    if cryptovizor_id is not None:
        cv_entity = None
        try:
            cv_entity = await asyncio.wait_for(
                client.get_entity(cryptovizor_id), timeout=15.0,
            )
            entity_type = type(cv_entity).__name__
            logger.info(
                f"[userbot] CV entity resolved: {entity_type} "
                f"title='{getattr(cv_entity,'title','?')}' "
                f"id={getattr(cv_entity,'id','?')} "
                f"username={getattr(cv_entity,'username','?')}"
            )
            try:
                from database import _events as _ev
                await asyncio.to_thread(_ev().insert_one, {
                    "at": utcnow(),
                    "type": "userbot_cv_entity_resolved",
                    "data": {
                        "type": entity_type,
                        "title": getattr(cv_entity, "title", None),
                        "id": getattr(cv_entity, "id", None),
                        "username": getattr(cv_entity, "username", None),
                    },
                })
            except Exception:
                pass

            # CV — это BOT (@cvizorbot), не канал! Сигналы приходят как DM
            # от бота. Если юзер заблокировал бота или Telegram прекратил
            # push (например после долгого disconnect), бот не шлёт. Fix:
            # отправить /start чтобы сбросить block/inactive state.
            # Это no-op если бот уже работает.
            if entity_type == "User":
                try:
                    await asyncio.wait_for(
                        client.send_message(cv_entity, "/start"),
                        timeout=10.0,
                    )
                    logger.info("[userbot] sent /start to CV bot — реактивация push'ей")
                    try:
                        from database import _events as _ev
                        await asyncio.to_thread(_ev().insert_one, {
                            "at": utcnow(),
                            "type": "userbot_cv_bot_restart_ok",
                            "data": {"username": getattr(cv_entity, "username", None)},
                        })
                    except Exception:
                        pass
                except Exception as e:
                    err = f"{type(e).__name__}: {str(e)[:200]}"
                    logger.warning(f"[userbot] /start to CV bot fail: {err}")
                    try:
                        from database import _events as _ev
                        await asyncio.to_thread(_ev().insert_one, {
                            "at": utcnow(),
                            "type": "userbot_cv_bot_restart_fail",
                            "data": {"error": err},
                        })
                    except Exception:
                        pass
            else:
                # Если бы CV был Channel — попробуем join (на всякий случай)
                try:
                    from telethon.tl.functions.channels import JoinChannelRequest
                    await asyncio.wait_for(
                        client(JoinChannelRequest(cv_entity)), timeout=15.0,
                    )
                    logger.info(f"[userbot] JoinChannelRequest OK for CV")
                except Exception as e:
                    logger.debug(f"[userbot] JoinChannelRequest skip: {e}")

            # catch_up() форсит подтянуть пропущенные updates после долгого
            # disconnect. Если CV bot шлёт push'и но userbot их пропустил —
            # catch_up их догонит (Telegram держит ~3 дня pending updates).
            try:
                await asyncio.wait_for(client.catch_up(), timeout=30.0)
                logger.info("[userbot] catch_up() OK — pending updates подтянуты")
                try:
                    from database import _events as _ev
                    await asyncio.to_thread(_ev().insert_one, {
                        "at": utcnow(),
                        "type": "userbot_catchup_ok",
                        "data": {},
                    })
                except Exception:
                    pass
            except Exception as e:
                logger.warning(f"[userbot] catch_up fail: {e}")

            # BACKFILL: на старте подтягиваем последние 100 сообщений из CV
            # и импортируем все Perfectly fit, которых нет в БД. Это покрывает
            # случай когда push events пропускались из-за watchdog-loop.
            try:
                from database import _signals as _sig, _events as _ev
                from pymongo import DESCENDING
                last_cv = await asyncio.to_thread(
                    _sig().find_one, {"source": "cryptovizor"},
                    sort=[("received_at", DESCENDING)],
                )
                db_latest = last_cv.get("received_at") if last_cv else None
                if db_latest and hasattr(db_latest, "tzinfo") and db_latest.tzinfo:
                    db_latest = db_latest.replace(tzinfo=None)

                logger.info(f"[backfill] reading 100 messages from CV bot (db_latest={db_latest})")
                msgs = await asyncio.wait_for(
                    client.get_messages(cv_entity, limit=100), timeout=30.0,
                )
                imported = 0
                skipped = 0
                for m in reversed(msgs):
                    m_dt = m.date.replace(tzinfo=None) if m.date else None
                    if not m_dt:
                        continue
                    if db_latest and m_dt <= db_latest:
                        skipped += 1
                        continue
                    txt = m.raw_text or ""
                    if "Perfectly fit" not in txt:
                        continue
                    try:
                        await handle_cryptovizor_message(txt, m.id)
                        imported += 1
                    except Exception as e:
                        logger.debug(f"[backfill] handle fail msg_id={m.id}: {e}")
                logger.info(f"[backfill] imported={imported} skipped={skipped} total_msgs={len(msgs)}")
                try:
                    await asyncio.to_thread(_ev().insert_one, {
                        "at": utcnow(),
                        "type": "userbot_backfill_done",
                        "data": {
                            "imported": imported,
                            "skipped_old": skipped,
                            "msgs_seen": len(msgs),
                            "db_latest_before": db_latest.isoformat() if db_latest else None,
                        },
                    })
                except Exception:
                    pass
            except Exception as e:
                logger.warning(f"[backfill] fail: {e}")
                try:
                    from database import _events as _ev
                    await asyncio.to_thread(_ev().insert_one, {
                        "at": utcnow(),
                        "type": "userbot_backfill_fail",
                        "data": {"error": f"{type(e).__name__}: {str(e)[:200]}"},
                    })
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"[userbot] get_entity({cryptovizor_id}) fail: {e}")
            try:
                from database import _events as _ev
                await asyncio.to_thread(_ev().insert_one, {
                    "at": utcnow(),
                    "type": "userbot_cv_entity_fail",
                    "data": {"error": str(e)[:300], "tried_id": cryptovizor_id},
                })
            except Exception:
                pass

        # ⚠ Telethon фильтры:
        #   chats=     — для групп/каналов
        #   from_users= — для приватных сообщений от User (вкл. боты)
        #
        # CV — это бот @cvizorbot (User type). При chats=cv_entity handler
        # НЕ срабатывает на DM от бота (Telethon ожидает group/channel).
        # Pulse_mismatch 13:35 показал: бот шлёт "Perfectly fit $TRX...",
        # но handler молчит. Wildcard diag после 5-мин окна тоже не ловил.
        #
        # Fix: регистрируем БЕЗ filter и проверяем chat_id внутри.
        # Это надёжнее любых filter combinations и работает для User/Channel/Chat.
        cv_target_id = getattr(cv_entity, "id", cryptovizor_id) if cv_entity else cryptovizor_id

        @client.on(events.NewMessage())
        async def cryptovizor_handler(event):
            try:
                # Match по chat_id в любом формате (Telethon dialog id или raw)
                ev_chat_id = event.chat_id
                if ev_chat_id != cv_target_id and ev_chat_id != cryptovizor_id:
                    # Также пробуем -100<raw> и raw без -100
                    abs_id = abs(ev_chat_id) if ev_chat_id else 0
                    if abs_id < 1000000000000:
                        return
                    # стрипуем -100 префикс
                    raw_from_neg = abs_id - 1000000000000 if str(abs_id).startswith("100") else 0
                    if raw_from_neg != cryptovizor_id and raw_from_neg != cv_target_id:
                        return
                if not event.raw_text:
                    return
                # Логируем что handler сработал — для диагностики
                try:
                    from database import _events as _ev
                    await asyncio.to_thread(_ev().insert_one, {
                        "at": utcnow(),
                        "type": "userbot_cv_handler_fired",
                        "data": {"chat_id": ev_chat_id, "preview": event.raw_text[:80]},
                    })
                except Exception:
                    pass
                # ── 🔔 BIG BUY parser (Cryptovizor шлёт оба типа: Perfectly fit + BIG BUY) ──
                try:
                    from bigbuy_parser import is_bigbuy_message, parse_bigbuy_message, store_bigbuy_signal
                    if is_bigbuy_message(event.raw_text):
                        parsed = parse_bigbuy_message(event.raw_text)
                        if parsed:
                            ok = await asyncio.to_thread(store_bigbuy_signal, parsed, event.message.id)
                            if ok:
                                # Forward to BOT16 (общий с WHALE/SHARK)
                                try:
                                    from watcher import _bigbuy_send_telegram
                                    await _bigbuy_send_telegram(parsed)
                                except Exception as _tge:
                                    logger.debug(f"[bigbuy] tg send fail: {_tge}")
                            return  # BIG BUY обработан, не идём в CV handler
                except Exception:
                    logger.exception("[bigbuy] parse/store crashed")
                await handle_cryptovizor_message(event.raw_text, event.message.id)
            except Exception:
                logger.exception("[userbot] Cryptovizor handler crashed")
        _cryptovizor_id_resolved = cryptovizor_id
        _handlers_registered["cryptovizor"] = True
        logger.info(f"👂 Слушаем Cryptovizor: chat_id={cryptovizor_id} cv_target_id={cv_target_id} entity={cv_entity is not None}")

    # ── DIAG: ПОСТОЯННЫЙ wildcard для незнакомых chat_id ─────────────────
    # Логирует events от любых chat_id КРОМЕ наших собственных ботов
    # (BOT13/BOT10/BOT5/BOT6/Verified). Цель: видеть приходят ли push'и
    # от CV-бота (5703939817), Tradium (-1002423680272) или каких-то
    # других неожиданных источников.
    KNOWN_OWN_BOTS = {
        8733222668,   # BOT13 New Strategies
        8727995143,   # BOT10 SuperTrend
        8761488135,   # BOT5 Confluence
        8507466695,   # BOT6 Paper
        8760128791,   # Verified Entry
    }

    @client.on(events.NewMessage())
    async def _diag_wildcard(event):
        try:
            chat_id = event.chat_id
            if chat_id in KNOWN_OWN_BOTS:
                return  # пропускаем наших ботов
            text_preview = (event.raw_text or "")[:100]
            try:
                from database import _events as _ev
                await asyncio.to_thread(_ev().insert_one, {
                    "at": utcnow(),
                    "type": "userbot_diag_event",
                    "data": {"chat_id": chat_id, "preview": text_preview},
                })
            except Exception:
                pass
        except Exception:
            pass
    logger.info("[userbot] DIAG: wildcard event tap для unknown chat_id (постоянный)")

    _handlers_registered["tradium"] = True
    _handlers_registered["kl"] = True

    return client


async def _channel_pulse_check(client):
    """Каждые 5 мин читает историю CV (limit=10), сравнивает с БД и
    ИМПОРТИРУЕТ свежие сообщения через handle_cryptovizor_message —
    polling fallback если push events не доставляются.

    User сообщил что push events иногда не приходят даже при правильно
    зарегистрированном handler. Решение: pull mechanism через get_messages.
    Это надёжнее push'ей и работает всегда пока есть авторизация."""
    from database import _signals, _events
    from pymongo import DESCENDING
    PULSE_EVERY = 5 * 60  # 5 мин — частый polling для CV сигналов
    cv_id_env = os.getenv("CRYPTOVIZOR_CHANNEL_ID", "5703939817").strip()
    try:
        raw = int(cv_id_env)
        # Auto-fix: положительный raw channel id → -100<raw> (Telethon format)
        cv_id = int(f"-100{raw}") if raw > 0 and len(str(raw)) >= 9 else raw
    except Exception:
        return
    while True:
        try:
            await asyncio.sleep(PULSE_EVERY)
            if not client.is_connected():
                logger.warning("[pulse] client disconnected — exit pulse")
                try:
                    await asyncio.to_thread(_events().insert_one, {
                        "at": utcnow(),
                        "type": "userbot_pulse_exit",
                        "data": {"reason": "client_disconnected"},
                    })
                except Exception:
                    pass
                return
            try:
                # limit=50 чтобы захватить все пропущенные за last 5min + buffer
                msgs = await asyncio.wait_for(
                    client.get_messages(cv_id, limit=50), timeout=20.0,
                )
            except Exception as e:
                err = f"{type(e).__name__}: {str(e)[:200]}"
                logger.warning(f"[pulse] get_messages fail: {err}")
                try:
                    await asyncio.to_thread(_events().insert_one, {
                        "at": utcnow(),
                        "type": "userbot_pulse_error",
                        "data": {"phase": "get_messages", "error": err, "cv_id": cv_id},
                    })
                except Exception:
                    pass
                continue
            if not msgs:
                continue

            # Получаем последнее received_at в БД
            last_cv = await asyncio.to_thread(
                _signals().find_one,
                {"source": "cryptovizor"},
                sort=[("received_at", DESCENDING)],
            )
            db_latest = last_cv.get("received_at") if last_cv else None
            if db_latest and hasattr(db_latest, "tzinfo") and db_latest.tzinfo:
                db_latest = db_latest.replace(tzinfo=None)

            # POLLING FALLBACK: импортируем все сообщения новее db_latest.
            # msgs приходят DESC (newest first), переворачиваем чтоб сохранять
            # в хронологическом порядке.
            imported = 0
            skipped_old = 0
            for m in reversed(msgs):
                m_dt = m.date.replace(tzinfo=None) if m.date else None
                if not m_dt:
                    continue
                if db_latest and m_dt <= db_latest:
                    skipped_old += 1
                    continue
                txt = m.raw_text or ""
                if not txt or "Perfectly fit" not in txt:
                    continue
                try:
                    await handle_cryptovizor_message(txt, m.id)
                    imported += 1
                    logger.info(f"[pulse] imported CV msg id={m.id} at={m_dt}")
                except Exception as e:
                    logger.warning(f"[pulse] handle_cryptovizor_message fail: {e}")

            latest_msg = msgs[0]
            latest_msg_dt = latest_msg.date.replace(tzinfo=None) if latest_msg.date else None
            try:
                ev_type = "userbot_pulse_imported" if imported > 0 else (
                    "userbot_pulse_mismatch"
                    if (latest_msg_dt and (not db_latest or latest_msg_dt > db_latest))
                    else "userbot_pulse_ok"
                )
                await asyncio.to_thread(_events().insert_one, {
                    "at": utcnow(),
                    "type": ev_type,
                    "data": {
                        "channel_msg_at": latest_msg_dt.isoformat() if latest_msg_dt else None,
                        "db_latest_at": db_latest.isoformat() if db_latest else None,
                        "imported": imported,
                        "skipped_old": skipped_old,
                        "msgs_seen": len(msgs),
                    },
                })
            except Exception:
                pass
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("[pulse] error")


async def _watchdog(client):
    """Мониторит активность каналов; пишет heartbeat каждые 5 мин.

    ВАЖНО (04.05.2026): убран auto-disconnect on silence. Старая логика
    «оба канала молчат >3ч → force reconnect» создавала бесконечный цикл:

      setup_ok → 10 min grace → watchdog detect silence → disconnect →
      supervisor reconnect → 10 min grace → watchdog detect silence → ...

    Reconnect НЕ лечит тишину канала: если CV не постит сообщения, он не
    начнёт постить после реконнекта. А каждый disconnect ломает свежие
    subscriptions и может ронять успевшие прийти push'и.

    Reconnect нужен только при РЕАЛЬНОМ disconnect'е от Telegram (event
    обрабатывается через client.run_until_disconnected → return → supervisor
    reconnect). Тишину каналов не лечим — это сигнал юзеру через
    system_health, не повод для авто-восстановления.

    Для recovery в плохом состоянии есть POST /api/userbot/force-restart.
    """
    HEARTBEAT_EVERY = 5 * 60
    last_heartbeat = 0
    while True:
        try:
            await asyncio.sleep(60)
            if not client.is_connected():
                logger.warning("[userbot] watchdog: client not connected, exiting watchdog")
                return
            now_ts = utcnow().timestamp()
            if now_ts - last_heartbeat >= HEARTBEAT_EVERY:
                logger.info("[userbot] alive (watchdog heartbeat)")
                last_heartbeat = now_ts
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("[userbot] watchdog error")


def _persist_supervisor_event(kind: str, data: dict | None = None):
    """Пишет событие supervisor'а в _events для пост-фактум диагностики
    (когда логи Railway эфемерны и недоступны)."""
    try:
        from database import _events
        _events().insert_one({
            "at": utcnow(),
            "type": f"userbot_{kind}",
            "data": data or {},
        })
    except Exception:
        pass


async def start_userbot():
    """Supervisor: поднимает Telethon клиент в бесконечном цикле с reconnect.
    Каждый setup/disconnect/crash логируется в _events для диагностики."""
    global _reconnect_count, _last_disconnect_at
    ensure_charts_dir()
    while True:
        client = None
        watchdog_task = None
        try:
            client = await _setup_telethon_client()
            if client is None:
                logger.warning("[userbot] setup returned None, retrying in 60s")
                _persist_supervisor_event("setup_none", {"error": _last_setup_error})
                await asyncio.sleep(60)
                continue
            _reconnect_count += 1
            logger.info(f"✅ Userbot запущен (с auto-reconnect, #{_reconnect_count})")
            _persist_supervisor_event("setup_ok", {
                "reconnect_count": _reconnect_count,
                "cv_id": _cryptovizor_id_resolved,
                "cv_method": _cryptovizor_resolve_method,
            })
            watchdog_task = asyncio.create_task(_watchdog(client))
            pulse_task = asyncio.create_task(_channel_pulse_check(client))
            await client.run_until_disconnected()
            _last_disconnect_at = utcnow()
            logger.warning("[userbot] disconnected, reconnecting in 30s")
            _persist_supervisor_event("disconnect", {"reconnect_count": _reconnect_count})
        except Exception as e:
            logger.exception("[userbot] supervisor crash")
            _last_disconnect_at = utcnow()
            _persist_supervisor_event("crash", {"error": str(e)[:300]})
        finally:
            if watchdog_task and not watchdog_task.done():
                watchdog_task.cancel()
            try:
                if 'pulse_task' in locals() and pulse_task and not pulse_task.done():
                    pulse_task.cancel()
            except Exception:
                pass
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass
        await asyncio.sleep(30)


async def peek_channel_history(channel_id: int, limit: int = 5) -> dict:
    """Читает последние сообщения канала через действующий Telethon клиент.
    Используется чтобы отличить «канал молчит» от «handler сломан»."""
    global _tg_client
    if not _tg_client:
        return {"ok": False, "error": "no client"}
    if not _tg_client.is_connected():
        return {"ok": False, "error": "client disconnected"}
    try:
        entity = await asyncio.wait_for(_tg_client.get_entity(channel_id), timeout=15.0)
        msgs = await asyncio.wait_for(_tg_client.get_messages(entity, limit=limit), timeout=15.0)
        out = []
        for m in msgs:
            out.append({
                "msg_id": m.id,
                "date": m.date.isoformat() if m.date else None,
                "preview": (m.raw_text or "")[:200],
            })
        title = getattr(entity, "title", str(entity))
        return {"ok": True, "channel_title": title, "channel_id": channel_id,
                "count": len(out), "messages": out}
    except asyncio.TimeoutError:
        return {"ok": False, "error": "timeout"}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


async def force_restart() -> dict:
    """Force-disconnect текущий Telethon клиент чтобы supervisor пересоздал
    его. Возвращает status. Используется для admin recovery когда userbot
    залип в плохом состоянии (например session expired или handler dead)."""
    global _tg_client
    result = {"action": "force_restart", "was_connected": False}
    if _tg_client:
        try:
            result["was_connected"] = bool(_tg_client.is_connected())
            await _tg_client.disconnect()
            result["disconnected"] = True
        except Exception as e:
            result["error"] = str(e)[:200]
    else:
        result["error"] = "no client to restart"
    _persist_supervisor_event("force_restart", result)
    return result


def get_status_details() -> dict:
    """Диагностика для /api/userbot/status и admin lifespan watchdog.
    Возвращает "is_connected" + timestamps + reconnect stats + last error.

    cryptovizor_handler_registered=False ⇒ задайте CRYPTOVIZOR_CHANNEL_ID
    в Railway env (5703939817 для TRENDS Cryptovizor)."""
    return {
        "is_connected": bool(_tg_client and _tg_client.is_connected()),
        "last_setup_at": _last_setup_at.isoformat() + "Z" if _last_setup_at else None,
        "last_disconnect_at": _last_disconnect_at.isoformat() + "Z" if _last_disconnect_at else None,
        "last_setup_error": _last_setup_error,
        "reconnect_count": _reconnect_count,
        "cryptovizor_id_resolved": _cryptovizor_id_resolved,
        "cryptovizor_resolve_method": _cryptovizor_resolve_method,
        "cryptovizor_env_set": bool(os.getenv("CRYPTOVIZOR_CHANNEL_ID", "").strip()),
        "handlers_registered": dict(_handlers_registered),
    }


async def handle_cryptovizor_message(text: str, message_id: int):
    """Парсит сообщение Cryptovizor и сохраняет сигналы (по одному на тикер).

    FIX 2026-05-04: раньше использовался get_futures_prices_only — пары не
    торгующиеся на Binance Futures (AR/SAHARA/WOO/ACX) пропускались БЕЗ ЛОГА.
    Теперь fallback spot→futures (get_prices_any), а при полном отсутствии
    цены сохраняем сигнал с entry=None и пишем event userbot_cv_skipped_price.
    """
    try:
        signals_data = parse_cryptovizor_message(text)
        if not signals_data:
            return

        # Fallback chain: futures (Cryptovizor = перпетуалы) → spot.
        # AR/SAHARA/WOO/ACX/etc. могут быть на Binance Spot но не на Futures.
        pairs = [s["pair"] for s in signals_data]
        try:
            prices_raw = await asyncio.to_thread(get_prices_any, pairs)
        except Exception as e:
            logger.exception(f"[CV] prices_any fetch failed: {e}")
            prices_raw = {}

        # Sync DB section вынесен в to_thread — раньше блокировал event loop.
        def _persist_cv_signals():
            from database import _events
            db = SessionLocal()
            results = []
            try:
                created_local = 0
                for sd in signals_data:
                    pair = sd["pair"]
                    norm = pair.replace("/", "").upper()
                    current_price = prices_raw.get(norm)  # может быть None
                    unique_msg_id = message_id * 100 + created_local
                    existing = db.query(Signal).filter(
                        Signal.source == BOT2_NAME
                    ).filter(Signal.message_id == unique_msg_id).first()
                    if existing:
                        continue
                    # Логируем когда нет цены — для видимости проблемных пар
                    if current_price is None:
                        try:
                            _events().insert_one({
                                "at": utcnow(),
                                "type": "userbot_cv_skipped_price",
                                "data": {"pair": pair, "norm": norm,
                                         "msg_id": message_id,
                                         "direction": sd["direction"]},
                            })
                        except Exception:
                            pass
                        # НЕ пропускаем — сохраняем с entry=None.
                        # Watcher при detect pattern подтянет цену сам через свой
                        # get_klines_any (который пробует другие биржи).
                    signal = Signal(
                        source=BOT2_NAME, message_id=unique_msg_id, raw_text=text,
                        pair=pair, direction=sd["direction"], trend=sd["trend"],
                        timeframe="1h", entry=current_price, status="СЛЕЖУ",
                        received_at=utcnow(),
                    )
                    db.add(signal)
                    db.commit()
                    db.refresh(signal)
                    created_local += 1
                    results.append({
                        "id": signal.id, "pair": pair,
                        "direction": sd["direction"],
                        "price": current_price,  # может быть None
                    })
            finally:
                db.close()
            return results

        created_signals = await asyncio.to_thread(_persist_cv_signals)
        try:
            created = len(created_signals)
            for sig_info in created_signals:
                logger.info(
                    f"[CV] #{sig_info['id']} {sig_info['pair']} "
                    f"{sig_info['direction']} entry={sig_info['price']}"
                )
                log_event(
                    sig_info['id'], "created",
                    data={"pair": sig_info['pair'], "direction": sig_info['direction'],
                          "source": BOT2_NAME},
                    message="Cryptovizor signal parsed",
                )
                try:
                    from admin import broadcast_event
                    broadcast_event("signal_new", {"id": sig_info['id'], "source": BOT2_NAME})
                except Exception:
                    pass
        except Exception:
            logger.exception("[CV] log/broadcast post-persist failed")
        # Прямой Telegram-алерт ОТКЛЮЧЁН (28.04.2026): pattern alert
        # шлётся отдельно из watcher._check_cryptovizor.
    except Exception:
        logger.exception(f"[CV] handle_cryptovizor_message crashed (msg_id={message_id})")


async def _send_cv_basic_alert(signal_id: int, pair: str, direction: str,
                                trend: str, price: float):
    """Прямой Telegram-алерт каждого CV-сигнала в BOT2.
    Без AI/паттернов/чартов — минимальный текст: pair + direction + trend + price.
    Использует httpx напрямую (минуя aiogram) чтобы не зависеть от watcher state.
    Метит cv_alert_sent в _events для диагностики через /api/cv-alert-diag.
    """
    # Маячок что функция вызвана (для диагностики что create_task сработал)
    try:
        from database import _events, utcnow
        _events().insert_one({
            "at": utcnow(),
            "type": "cv_alert_called",
            "data": {"signal_id": signal_id, "pair": pair,
                     "direction": direction, "kind": "basic"},
        })
    except Exception:
        pass

    from config import BOT2_BOT_TOKEN, ADMIN_CHAT_ID
    if not BOT2_BOT_TOKEN or not ADMIN_CHAT_ID:
        logger.warning(f"[CV-basic-alert] BOT2 не настроен — skip #{signal_id} "
                       f"(token={'set' if BOT2_BOT_TOKEN else 'EMPTY'}, "
                       f"chat={ADMIN_CHAT_ID})")
        try:
            from database import _events, utcnow
            _events().insert_one({
                "at": utcnow(),
                "type": "cv_alert_error",
                "data": {"signal_id": signal_id, "pair": pair,
                         "error": f"BOT2 token/chat empty (token_set={bool(BOT2_BOT_TOKEN)}, chat={ADMIN_CHAT_ID})",
                         "kind": "basic"},
            })
        except Exception:
            pass
        return
    is_long = direction in ("LONG", "BUY")
    dir_emoji = "🟢" if is_long else "🔴"
    dir_label = "LONG" if is_long else "SHORT"
    # Trend rendering: GRRRR → 🟢🔴🔴🔴🔴
    trend_emoji = "".join("🟢" if c == "G" else "🔴" for c in (trend or ""))
    # Цена форматирование под масштаб
    if price is None:
        price_str = "—"
    elif abs(price) >= 1000:
        price_str = f"{price:,.2f}"
    elif abs(price) >= 1:
        price_str = f"{price:.4f}"
    elif abs(price) >= 0.01:
        price_str = f"{price:.5f}"
    else:
        price_str = f"{price:.8f}"

    sym = pair.replace("/", "").upper()
    base = pair.split("/")[0] if "/" in pair else pair.replace("USDT", "")
    text = (
        f"🚀 <b>CRYPTOVIZOR</b>\n\n"
        f"<b>{base}/USDT</b> · {dir_emoji} <b>{dir_label}</b>\n"
        f"<code>{sym}</code>\n\n"
        f"Тренд: {trend_emoji} ({trend})\n"
        f"Цена: <code>{price_str}</code>\n"
    )
    try:
        import httpx
        url = f"https://api.telegram.org/bot{BOT2_BOT_TOKEN}/sendMessage"
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(url, json={
                "chat_id": ADMIN_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            })
            resp = r.json()
            if resp.get("ok"):
                msg_id = resp.get("result", {}).get("message_id")
                logger.info(f"[CV-basic-alert] sent #{signal_id} {pair} → msg_id={msg_id}")
                try:
                    from database import _events, utcnow
                    _events().insert_one({
                        "at": utcnow(),
                        "type": "cv_alert_sent",
                        "data": {"signal_id": signal_id, "pair": pair,
                                 "message_id": msg_id, "kind": "basic"},
                    })
                except Exception:
                    pass
            else:
                logger.warning(f"[CV-basic-alert] BOT2 error #{signal_id}: {resp}")
                try:
                    from database import _events, utcnow
                    _events().insert_one({
                        "at": utcnow(),
                        "type": "cv_alert_error",
                        "data": {"signal_id": signal_id, "pair": pair,
                                 "error": str(resp)[:200], "kind": "basic"},
                    })
                except Exception:
                    pass
    except Exception as e:
        logger.warning(f"[CV-basic-alert] send fail #{signal_id}: {e}")
        try:
            from database import _events, utcnow
            _events().insert_one({
                "at": utcnow(),
                "type": "cv_alert_error",
                "data": {"signal_id": signal_id, "pair": pair,
                         "error": str(e)[:200], "kind": "basic"},
            })
        except Exception:
            pass
