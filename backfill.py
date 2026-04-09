"""Парсит последние N сообщений из группы-источника и сохраняет в БД."""
import asyncio
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument

from config import API_ID, API_HASH, PHONE, SOURCE_GROUP_ID, CHARTS_DIR
from database import SessionLocal, Signal, init_db
from parser import parse_signal
from ai_analyzer import analyze_chart, analyze_signal_quality, merge_signal_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("backfill")

LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 40


def _to_float(val):
    try:
        return float(val) if val is not None else None
    except Exception:
        return None


async def main():
    init_db()
    Path(CHARTS_DIR).mkdir(parents=True, exist_ok=True)

    client = TelegramClient("session_userbot", API_ID, API_HASH)
    await client.start(
        phone=PHONE,
        code_callback=lambda: "10286",
        password=lambda: "1240Maxim",
    )
    logger.info(f"Загружаем последние {LIMIT} сообщений из группы {SOURCE_GROUP_ID}")

    messages = []
    async for msg in client.iter_messages(SOURCE_GROUP_ID, limit=LIMIT):
        messages.append(msg)
    messages.reverse()  # старые → новые
    logger.info(f"Получено {len(messages)} сообщений")

    db = SessionLocal()
    try:
        last_text_signal_id = None

        for msg in messages:
            is_photo = isinstance(msg.media, MessageMediaPhoto)
            is_doc_image = (
                isinstance(msg.media, MessageMediaDocument)
                and msg.media.document.mime_type.startswith("image/")
            ) if msg.media else False

            # ── Текст с сигналом ────────────────────────────────────
            if msg.raw_text and len(msg.raw_text.strip()) > 5 and not (is_photo or is_doc_image):
                existing = db.query(Signal).filter(Signal.message_id == msg.id).first()
                if existing:
                    last_text_signal_id = existing.id
                    continue

                parsed = parse_signal(msg.raw_text)
                # Только полноценные Tradium Setups (есть trend + TP + SL + entry)
                if not (parsed.get("trend") and parsed.get("tp1") and parsed.get("sl") and parsed.get("entry")):
                    continue

                # Backfilled сигналы старше 2 часов не триггерят алерты в бот
                from datetime import datetime, timezone, timedelta
                msg_date = msg.date
                if msg_date.tzinfo is not None:
                    msg_date_naive = msg_date.replace(tzinfo=None)
                    now = datetime.now(timezone.utc).replace(tzinfo=None)
                else:
                    msg_date_naive = msg_date
                    now = datetime.utcnow()
                is_old = (now - msg_date_naive) > timedelta(hours=2)
                # Для старых сигналов ставим dca4_triggered=True сразу,
                # чтобы watcher не пытался их алертить
                pre_triggered = is_old

                signal = Signal(
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
                    status="АРХИВ" if pre_triggered else "СЛЕЖУ",
                    dca4_triggered=pre_triggered,
                    pattern_triggered=pre_triggered,
                    is_forwarded=pre_triggered,
                    has_chart=False,
                    chart_analyzed=False,
                    received_at=msg.date,
                )
                db.add(signal)
                db.commit()
                db.refresh(signal)
                last_text_signal_id = signal.id
                logger.info(f"[TEXT] #{signal.id} {signal.pair} {signal.direction} msg={msg.id}")

            # ── Фото/график ──────────────────────────────────────────
            elif (is_photo or is_doc_image) and last_text_signal_id is not None:
                signal = db.query(Signal).filter(Signal.id == last_text_signal_id).first()
                if not signal or signal.has_chart:
                    continue

                chart_path = os.path.join(CHARTS_DIR, f"{signal.id}_{msg.id}.jpg")
                try:
                    await client.download_media(msg, file=chart_path)
                except Exception as e:
                    logger.error(f"Ошибка скачивания: {e}")
                    continue

                signal.has_chart = True
                signal.chart_message_id = msg.id
                signal.chart_path = chart_path
                signal.chart_received_at = msg.date
                db.commit()
                logger.info(f"[CHART] #{signal.id} -> {chart_path}")

                # AI анализ
                try:
                    chart_data = await analyze_chart(chart_path)
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
                    signal.dca1 = _to_float(chart_data.get("dca1"))
                    signal.dca2 = _to_float(chart_data.get("dca2"))
                    signal.dca3 = _to_float(chart_data.get("dca3"))
                    signal.dca4 = _to_float(chart_data.get("dca4"))

                    text_data = {
                        "pair": signal.text_pair, "direction": signal.text_direction,
                        "entry": signal.text_entry, "sl": signal.text_sl,
                        "tp1": signal.text_tp1, "tp2": signal.text_tp2, "tp3": signal.text_tp3,
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
                    logger.info(f"[AI] #{signal.id} проанализирован")

                    # AI quality score
                    try:
                        quality = await analyze_signal_quality(chart_path, {
                            "pair": signal.pair, "direction": signal.direction,
                            "timeframe": signal.timeframe, "entry": signal.entry,
                            "sl": signal.sl, "tp1": signal.tp1, "dca4": signal.dca4,
                            "risk_reward": signal.risk_reward, "trend": signal.trend,
                            "pattern": signal.chart_notes,
                        })
                        if quality and "score" in quality:
                            signal.ai_score = quality["score"]
                            signal.ai_confidence = quality.get("confidence")
                            signal.ai_reasoning = quality.get("reasoning")
                            signal.ai_risks = quality.get("risks") or []
                            signal.ai_verdict = quality.get("verdict")
                            db.commit()
                            logger.info(f"[QUALITY] #{signal.id} score={signal.ai_score}")
                    except Exception as e:
                        logger.error(f"Quality ошибка #{signal.id}: {e}")
                except Exception as e:
                    logger.error(f"AI ошибка для #{signal.id}: {e}")
    finally:
        db.close()
        await client.disconnect()
        logger.info("Готово")


if __name__ == "__main__":
    asyncio.run(main())
