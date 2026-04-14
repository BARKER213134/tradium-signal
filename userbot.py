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
    global _tg_client
    session_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "session_userbot")
    client = TelegramClient(session_path, API_ID, API_HASH)
    try:
        await client.connect()
    except Exception as e:
        logger.error(f"[userbot] connect failed: {e}")
        return None
    if not await client.is_user_authorized():
        logger.error(
            "❌ Userbot не авторизован! Запусти `python authorize.py` чтобы войти. "
            "Сервер продолжит работать без live-сигналов из Telegram."
        )
        await client.disconnect()
        return None
    _tg_client = client
    logger.info("✅ Userbot подключён")

    # Прогреваем кэш Tradium
    try:
        await client.get_entity(SOURCE_GROUP_ID)
    except Exception as e:
        logger.warning(f"[userbot] Tradium entity warmup failed: {e}")

    # Резолвим Cryptovizor диалог по имени (пересоздаём на каждом reconnect)
    cryptovizor_id = None
    if BOT2_SOURCE_GROUP:
        try:
            async for d in client.iter_dialogs():
                if BOT2_SOURCE_GROUP.lower() in (d.name or "").lower():
                    cryptovizor_id = d.id
                    logger.info(f"✅ Cryptovizor найден: id={d.id} name='{d.name}'")
                    break
            if cryptovizor_id is None:
                logger.warning(f"[userbot] Cryptovizor '{BOT2_SOURCE_GROUP}' не найден среди диалогов")
        except Exception as e:
            logger.error(f"[userbot] Не удалось найти Cryptovizor: {e}")

    # ── Handler Tradium (текст/фото) ──────────────────────────────
    @client.on(events.NewMessage(chats=SOURCE_GROUP_ID))
    async def handler(event):
        try:
            msg = event.message
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

    logger.info(f"👂 Слушаем Tradium группу: {SOURCE_GROUP_ID}")

    # ── Handler Cryptovizor ───────────────────────────────────────
    if cryptovizor_id is not None:
        @client.on(events.NewMessage(chats=cryptovizor_id))
        async def cryptovizor_handler(event):
            try:
                if not event.raw_text:
                    return
                await handle_cryptovizor_message(event.raw_text, event.message.id)
            except Exception:
                logger.exception("[userbot] Cryptovizor handler crashed")
        logger.info(f"👂 Слушаем Cryptovizor: {cryptovizor_id}")

    return client


async def _watchdog(client):
    """Мониторит активность каналов; форсирует disconnect если оба замолчали > 30 мин.
    Также пишет heartbeat каждые 5 минут."""
    from database import _signals
    from pymongo import DESCENDING
    SILENCE_LIMIT = 30 * 60  # 30 минут
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
            # Проверяем тишину обоих каналов
            try:
                last_cv = _signals().find_one({"source": "cryptovizor"}, sort=[("received_at", DESCENDING)])
                last_tr = _signals().find_one({"source": "tradium"}, sort=[("received_at", DESCENDING)])
                cv_age = (utcnow() - last_cv["received_at"]).total_seconds() if last_cv and last_cv.get("received_at") else 9e9
                tr_age = (utcnow() - last_tr["received_at"]).total_seconds() if last_tr and last_tr.get("received_at") else 9e9
                # Если оба молчат больше лимита — подозреваем что подписки умерли, форсируем reconnect
                if cv_age > SILENCE_LIMIT and tr_age > SILENCE_LIMIT:
                    logger.warning(f"[userbot] watchdog: both channels silent (CV {cv_age/60:.0f}m, Tradium {tr_age/60:.0f}m) → force reconnect")
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    return
            except Exception as e:
                logger.debug(f"[userbot] watchdog db check: {e}")
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("[userbot] watchdog error")


async def start_userbot():
    """Supervisor: поднимает Telethon клиент в бесконечном цикле с reconnect."""
    ensure_charts_dir()
    while True:
        client = None
        watchdog_task = None
        try:
            client = await _setup_telethon_client()
            if client is None:
                logger.warning("[userbot] setup returned None, retrying in 60s")
                await asyncio.sleep(60)
                continue
            logger.info("✅ Userbot запущен (с auto-reconnect)")
            watchdog_task = asyncio.create_task(_watchdog(client))
            await client.run_until_disconnected()
            logger.warning("[userbot] disconnected, reconnecting in 30s")
        except Exception:
            logger.exception("[userbot] supervisor crash")
        finally:
            if watchdog_task and not watchdog_task.done():
                watchdog_task.cancel()
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass
        await asyncio.sleep(30)


async def handle_cryptovizor_message(text: str, message_id: int):
    """Парсит сообщение Cryptovizor и сохраняет сигналы (по одному на тикер)."""
    try:
        signals_data = parse_cryptovizor_message(text)
        if not signals_data:
            return

        # Cryptovizor = перпетуалы → сразу futures API
        pairs = [s["pair"] for s in signals_data]
        try:
            prices_raw = await asyncio.to_thread(get_futures_prices_only, pairs)
        except Exception as e:
            logger.exception(f"[CV] futures prices fetch failed: {e}")
            prices_raw = {}

        db = SessionLocal()
        try:
            created = 0
            for sd in signals_data:
                pair = sd["pair"]
                norm = pair.replace("/", "").upper()
                current_price = prices_raw.get(norm)

                # Если ни spot ни futures не дали цену — пропускаем
                if current_price is None:
                    logger.debug(f"[CV] {pair} — нет цены на futures, пропускаем")
                    continue

                # Уникальность: не создаём дубликаты если message_id уже был с тем же pair
                unique_msg_id = message_id * 100 + created
                existing = db.query(Signal).filter(
                    Signal.source == BOT2_NAME
                ).filter(Signal.message_id == unique_msg_id).first()
                if existing:
                    continue

                signal = Signal(
                    source=BOT2_NAME,
                    message_id=unique_msg_id,
                    raw_text=text,
                    pair=pair,
                    direction=sd["direction"],
                    trend=sd["trend"],
                    timeframe="1h",
                    entry=current_price,
                    status="СЛЕЖУ",
                    received_at=utcnow(),
                )
                db.add(signal)
                db.commit()
                db.refresh(signal)
                created += 1
                logger.info(
                    f"[CV] #{signal.id} {pair} {sd['direction']} entry={current_price}"
                )
                log_event(
                    signal.id, "created",
                    data={"pair": pair, "direction": sd["direction"], "source": BOT2_NAME},
                    message="Cryptovizor signal parsed",
                )
                try:
                    from admin import broadcast_event
                    broadcast_event("signal_new", {"id": signal.id, "source": BOT2_NAME})
                except Exception:
                    pass
        finally:
            db.close()
    except Exception:
        logger.exception(f"[CV] handle_cryptovizor_message crashed (msg_id={message_id})")
