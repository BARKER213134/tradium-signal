"""Подтягивает пропущенные сигналы из Tradium и Cryptovizor каналов.

Скрипт идёт по истории каждого канала с момента последнего сохранённого сигнала
в БД (или за последние N часов если задан --hours), парсит и сохраняет всё что
было пропущено пока userbot был оффлайн.

Usage:
    python backfill_missed.py                  # с момента последнего сигнала в БД
    python backfill_missed.py --hours 24       # за последние 24 часа в обоих каналах
    python backfill_missed.py --only cv        # только Cryptovizor
    python backfill_missed.py --only tradium   # только Tradium

Сигналы старше 2 часов получают status='АРХИВ' (и pattern_triggered=True)
чтобы watcher не пытался триггерить алерты задним числом.
"""
import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from telethon import TelegramClient

from config import API_ID, API_HASH, SOURCE_GROUP_ID, BOT2_SOURCE_GROUP, BOT2_NAME
from database import SessionLocal, Signal, utcnow, init_db, log_event, _signals
from exchange import get_futures_prices_only
from parser import parse_signal
from parser_cryptovizor import parse_cryptovizor_message
from pymongo import DESCENDING

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("backfill-missed")


def _last_signal_time(source: str) -> datetime | None:
    """Возвращает дату последнего сигнала указанного source из БД (naive UTC)."""
    field = "received_at"
    doc = _signals().find_one({"source": source}, sort=[(field, DESCENDING)])
    if not doc or not doc.get(field):
        return None
    dt = doc[field]
    if hasattr(dt, "tzinfo") and dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt


async def _resolve_cv_id(client) -> int | None:
    target_name = BOT2_SOURCE_GROUP or "TRENDS Cryptovizor"
    async for d in client.iter_dialogs():
        if target_name.lower() in (d.name or "").lower():
            logger.info(f"Cryptovizor: id={d.id} name='{d.name}'")
            return d.id
    logger.warning(f"Cryptovizor '{target_name}' не найден")
    return None


async def backfill_cryptovizor(client, since: datetime, hard_limit: int = 2000) -> int:
    """Идёт по истории Cryptovizor с момента since (UTC naive)."""
    cv_id = await _resolve_cv_id(client)
    if cv_id is None:
        return 0

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    logger.info(f"[CV] Загружаю сообщения с {since} до сейчас (max {hard_limit})…")

    # iter_messages возвращает от новых к старым; останавливаемся когда msg.date < since
    messages = []
    async for m in client.iter_messages(cv_id, limit=hard_limit):
        msg_date = m.date
        if msg_date.tzinfo is not None:
            msg_date_naive = msg_date.replace(tzinfo=None)
        else:
            msg_date_naive = msg_date
        if msg_date_naive < since:
            break
        if not m.raw_text:
            continue
        messages.append((m, msg_date_naive))
    messages.reverse()  # старые → новые
    logger.info(f"[CV] Найдено {len(messages)} сообщений в окне")

    db = SessionLocal()
    added = 0
    skipped = 0
    try:
        for msg, msg_date_naive in messages:
            parsed = parse_cryptovizor_message(msg.raw_text or "")
            if not parsed:
                continue
            is_old = (now - msg_date_naive) > timedelta(hours=2)

            pairs = [sd["pair"] for sd in parsed]
            try:
                prices_raw = await asyncio.to_thread(get_futures_prices_only, pairs)
            except Exception as e:
                logger.warning(f"[CV] futures prices fail for msg {msg.id}: {e}")
                prices_raw = {}

            for i, sd in enumerate(parsed):
                unique_id = msg.id * 100 + i
                existing = (
                    db.query(Signal)
                    .filter(Signal.source == BOT2_NAME)
                    .filter(Signal.message_id == unique_id)
                    .first()
                )
                if existing:
                    skipped += 1
                    continue

                norm = sd["pair"].replace("/", "").upper()
                current_price = prices_raw.get(norm)
                if current_price is None:
                    continue

                s = Signal(
                    source=BOT2_NAME,
                    message_id=unique_id,
                    raw_text=msg.raw_text,
                    pair=sd["pair"],
                    direction=sd["direction"],
                    trend=sd["trend"],
                    timeframe="1h",
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
                    f"[CV +] #{s.id} {sd['pair']} {sd['direction']} "
                    f"{'ARCHIVE' if is_old else 'WATCH'} @ {current_price} ({msg_date_naive})"
                )
                log_event(
                    s.id, "created",
                    data={"source": BOT2_NAME, "backfill": "missed"},
                    message="Backfilled missed CV signal",
                )
    finally:
        db.close()
    logger.info(f"[CV] Done: добавлено {added}, пропущено существующих {skipped}")
    return added


async def backfill_tradium(client, since: datetime, hard_limit: int = 500) -> int:
    """Идёт по истории Tradium с момента since (UTC naive). Только текст, без графиков."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    logger.info(f"[TR] Загружаю сообщения с {since} до сейчас (max {hard_limit})…")

    messages = []
    async for m in client.iter_messages(SOURCE_GROUP_ID, limit=hard_limit):
        msg_date = m.date
        if msg_date.tzinfo is not None:
            msg_date_naive = msg_date.replace(tzinfo=None)
        else:
            msg_date_naive = msg_date
        if msg_date_naive < since:
            break
        if not m.raw_text or len(m.raw_text.strip()) <= 5:
            continue
        messages.append((m, msg_date_naive))
    messages.reverse()
    logger.info(f"[TR] Найдено {len(messages)} текстовых сообщений в окне")

    db = SessionLocal()
    added = 0
    skipped = 0
    try:
        for msg, msg_date_naive in messages:
            existing = db.query(Signal).filter(Signal.message_id == msg.id).first()
            if existing:
                skipped += 1
                continue

            parsed = parse_signal(msg.raw_text)
            # Только полноценные Tradium Setups (есть trend + TP + SL + entry)
            if not (parsed.get("trend") and parsed.get("tp1") and parsed.get("sl") and parsed.get("entry")):
                continue

            is_old = (now - msg_date_naive) > timedelta(hours=2)

            signal = Signal(
                source="tradium",
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
                status="АРХИВ" if is_old else "СЛЕЖУ",
                dca4_triggered=is_old,
                pattern_triggered=is_old,
                is_forwarded=is_old,
                has_chart=False,
                chart_analyzed=False,
                received_at=msg_date_naive,
            )
            db.add(signal)
            db.commit()
            db.refresh(signal)
            added += 1
            logger.info(
                f"[TR +] #{signal.id} {signal.pair} {signal.direction} "
                f"{'ARCHIVE' if is_old else 'WATCH'} entry={signal.entry} ({msg_date_naive})"
            )
            log_event(
                signal.id, "created",
                data={"source": "tradium", "backfill": "missed"},
                message="Backfilled missed Tradium signal",
            )
    finally:
        db.close()
    logger.info(f"[TR] Done: добавлено {added}, пропущено существующих {skipped}")
    return added


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=None,
                    help="Принудительно подтянуть за последние N часов (иначе с момента последнего сигнала)")
    ap.add_argument("--only", choices=["cv", "tradium"], default=None,
                    help="Только один источник")
    ap.add_argument("--limit-cv", type=int, default=2000)
    ap.add_argument("--limit-tr", type=int, default=500)
    args = ap.parse_args()

    init_db()
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    # since: либо явно --hours, либо последний сигнал из БД, либо 24ч назад
    if args.hours is not None:
        since_cv = since_tr = now - timedelta(hours=args.hours)
    else:
        last_cv = _last_signal_time("cryptovizor")
        last_tr = _last_signal_time("tradium")
        # Если ничего нет — берём 24 часа
        since_cv = last_cv if last_cv else now - timedelta(hours=24)
        since_tr = last_tr if last_tr else now - timedelta(hours=24)

    logger.info(f"CV: с {since_cv} (последний в БД: {_last_signal_time('cryptovizor')})")
    logger.info(f"TR: с {since_tr} (последний в БД: {_last_signal_time('tradium')})")

    session = os.path.join(os.path.dirname(os.path.abspath(__file__)), "session_userbot")
    client = TelegramClient(session, API_ID, API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        logger.error("Userbot не авторизован! Запусти authorize.py чтобы войти.")
        await client.disconnect()
        return

    try:
        cv_added = tr_added = 0
        if args.only != "tradium":
            cv_added = await backfill_cryptovizor(client, since_cv, args.limit_cv)
        if args.only != "cv":
            tr_added = await backfill_tradium(client, since_tr, args.limit_tr)
        logger.info(f"=== ИТОГ: CV +{cv_added}, Tradium +{tr_added} ===")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
