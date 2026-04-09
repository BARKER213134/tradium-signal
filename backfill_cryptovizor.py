"""Подтягивает историю из @TRENDS_Cryptovizor и сохраняет спаршеные сигналы.

Usage:
    python backfill_cryptovizor.py [limit]
        limit — сколько последних сообщений забрать (по умолчанию 200)

Каждое сообщение может содержать несколько тикеров — парсер разбивает на
отдельные Signal-записи с source='cryptovizor', status='СЛЕЖУ'.
Сигналы старше 2 часов получают status='АРХИВ' чтобы не триггерить watcher.
"""
import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from telethon import TelegramClient

from config import API_ID, API_HASH, BOT2_SOURCE_GROUP, BOT2_NAME
from database import SessionLocal, Signal, utcnow, init_db, log_event
from exchange import get_prices
from parser_cryptovizor import parse_cryptovizor_message

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("cv-backfill")


async def main():
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 200
    init_db()

    session = os.path.join(os.path.dirname(os.path.abspath(__file__)), "session_userbot")
    client = TelegramClient(session, API_ID, API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        logger.error("Userbot не авторизован! Запусти auth_step1/step2.py")
        await client.disconnect()
        return

    # Резолв диалога
    target_name = BOT2_SOURCE_GROUP or "TRENDS Cryptovizor"
    target_id = None
    async for d in client.iter_dialogs():
        if target_name.lower() in (d.name or "").lower():
            target_id = d.id
            logger.info(f"Диалог: id={d.id} name='{d.name}'")
            break
    if target_id is None:
        logger.error(f"Диалог '{target_name}' не найден")
        await client.disconnect()
        return

    logger.info(f"Загружаю {limit} сообщений…")
    messages = []
    async for m in client.iter_messages(target_id, limit=limit):
        messages.append(m)
    messages.reverse()

    db = SessionLocal()
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        added = 0
        for msg in messages:
            parsed = parse_cryptovizor_message(msg.raw_text or "")
            if not parsed:
                continue
            msg_date = msg.date
            if msg_date.tzinfo is not None:
                msg_date_naive = msg_date.replace(tzinfo=None)
            else:
                msg_date_naive = msg_date
            is_old = (now - msg_date_naive) > timedelta(hours=2)
            # Текущие цены для всех тикеров в сообщении
            pairs = [sd["pair"] for sd in parsed]
            prices_raw = await asyncio.to_thread(get_prices, pairs)

            for i, sd in enumerate(parsed):
                unique_id = msg.id * 100 + i
                existing = (
                    db.query(Signal)
                    .filter(Signal.source == BOT2_NAME)
                    .filter(Signal.message_id == unique_id)
                    .first()
                )
                if existing:
                    continue

                norm = sd["pair"].replace("/", "").upper()
                current_price = prices_raw.get(norm)

                s = Signal(
                    source=BOT2_NAME,
                    message_id=unique_id,
                    raw_text=msg.raw_text,
                    pair=sd["pair"],
                    direction=sd["direction"],
                    trend=sd["trend"],
                    timeframe="30m",
                    entry=current_price,
                    status="АРХИВ" if is_old else "СЛЕЖУ",
                    pattern_triggered=is_old,
                    received_at=msg_date_naive,
                )
                db.add(s)
                db.commit()
                db.refresh(s)
                added += 1
                logger.info(
                    f"[+] #{s.id} {sd['pair']} {sd['direction']} "
                    f"{'ARCHIVE' if is_old else 'WATCH'} entry={current_price}"
                )
                log_event(
                    s.id, "created",
                    data={"source": BOT2_NAME, "backfill": True},
                    message="Backfilled from history",
                )
        logger.info(f"Готово. Добавлено: {added}")
    finally:
        db.close()
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
