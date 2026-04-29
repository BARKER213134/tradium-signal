import asyncio
import base64
import logging
import os

import uvicorn

from database import init_db
from config import BOT_TOKEN, ADMIN_CHAT_ID, API_ID, API_HASH, BOT2_BOT_TOKEN, BOT4_BOT_TOKEN


def _bootstrap_session():
    """Session восстанавливается с приоритетом:
    1) MongoDB collection 'system' документ _id='telethon_session' —
       живое хранилище, авто-обновляется через _persist_session_to_mongo
       при каждой успешной авторизации. Всегда свежее всех других
       источников.
    2) env TELETHON_SESSION_B64 — fallback когда Mongo пуст (первый запуск).

    Было: env был приоритетом 1 → при устаревшем env'е Mongo-копия
    игнорировалась и Railway поднимал сломанную сессию.
    """
    here = os.path.dirname(os.path.abspath(__file__))
    session_path = os.path.join(here, "session_userbot.session")
    if os.path.exists(session_path):
        return

    # 1) MongoDB (primary — всегда актуальнее env)
    try:
        from database import _get_db
        col = _get_db().system
        doc = col.find_one({"_id": "telethon_session"})
        if doc and "data" in doc:
            with open(session_path, "wb") as f:
                f.write(doc["data"])
            logging.info(f"✅ Session восстановлен из Mongo ({len(doc['data'])} bytes)")
            return
    except Exception as e:
        logging.error(f"Mongo session load failed: {e}")

    # 2) env var — fallback когда Mongo пуст (редко, только при первом
    # деплое). При успешном старте userbot сохранит сессию в Mongo,
    # и следующие рестарты пойдут через Mongo.
    b64 = os.getenv("TELETHON_SESSION_B64")
    if b64:
        try:
            with open(session_path, "wb") as f:
                f.write(base64.b64decode(b64))
            logging.info(f"✅ Session восстановлен из TELETHON_SESSION_B64 ({len(b64)} chars)")
            return
        except Exception as e:
            logging.error(f"TELETHON_SESSION_B64 decode failed: {e}")


def _persist_session_to_mongo():
    """Сохраняет текущий session-файл в Mongo (вызывается после авторизации
    или при старте если файл есть а в Mongo нет)."""
    here = os.path.dirname(os.path.abspath(__file__))
    session_path = os.path.join(here, "session_userbot.session")
    if not os.path.exists(session_path):
        return
    try:
        from database import _get_db
        col = _get_db().system
        with open(session_path, "rb") as f:
            data = f.read()
        col.update_one(
            {"_id": "telethon_session"},
            {"$set": {"data": data, "size": len(data)}},
            upsert=True,
        )
        logging.info(f"✅ Session сохранён в Mongo ({len(data)} bytes)")
    except Exception as e:
        logging.error(f"Mongo session save failed: {e}")


_bootstrap_session()


def _migrate_charts_to_gridfs():
    """При старте мигрирует локальные графики в GridFS (если ещё не там)."""
    try:
        from database import _get_db, save_chart, get_chart
        db = _get_db()
        signals = list(db.signals.find({"has_chart": True, "chart_path": {"$exists": True}}))
        migrated = 0
        for s in signals:
            sid = s.get("id")
            if get_chart(sid):
                continue  # уже в GridFS
            path = s.get("chart_path", "")
            here = os.path.dirname(os.path.abspath(__file__))
            candidates = [path, os.path.join(here, path.lstrip("./\\"))]
            for c in candidates:
                c = os.path.normpath(c)
                if os.path.exists(c):
                    with open(c, "rb") as f:
                        save_chart(sid, f.read(), filename=os.path.basename(c))
                    migrated += 1
                    break
        if migrated:
            logging.info(f"Migrated {migrated} charts to GridFS")
    except Exception as e:
        logging.error(f"Chart migration: {e}")


# [Phase 3 fix] Раньше вызывалось синхронно на module load — блокировало
# main.py startup на 5-10 минут (1000+ chart × 2-5MB IO + GridFS sync write).
# Теперь admin.lifespan запускает миграцию в фоне через asyncio.create_task.
# _migrate_charts_to_gridfs()  # отключено — см. admin.lifespan

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def _serve_admin():
    """Запуск uvicorn в том же event loop."""
    port = int(os.getenv("PORT", "8000"))
    config = uvicorn.Config(
        "admin:app",
        host="0.0.0.0",
        port=port,
        log_level="info",
        loop="asyncio",
        lifespan="on",
    )
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    # Проверяем обязательные переменные
    if not API_ID or not API_HASH:
        logger.error("Не заданы API_ID и API_HASH в .env!")
        return
    if not BOT_TOKEN:
        logger.error("Не задан BOT_TOKEN в .env!")
        return

    # Threadpool для asyncio.to_thread() — Railway Pro (24 vCPU / 24GB) =>
    # 120 потоков для максимального concurrent throughput на I/O-bound операциях
    # (ccxt API calls, MongoDB queries, Telethon iter).
    try:
        from concurrent.futures import ThreadPoolExecutor
        loop = asyncio.get_running_loop()
        loop.set_default_executor(ThreadPoolExecutor(max_workers=120, thread_name_prefix="async-pool"))
        logger.info("[main] threadpool: 120 workers (Railway Pro 24 vCPU)")
    except Exception as _te:
        logger.warning(f"[main] threadpool tuning fail: {_te}")

    init_db()
    logger.info("База данных инициализирована")

    from bot import bot, start_bot
    from bot2 import bot2, start_bot2
    from userbot import set_bot, start_userbot
    from watcher import setup as setup_watcher, start_watcher

    set_bot(bot, ADMIN_CHAT_ID)

    if bot2:
        logger.info("✅ BOT2 (Cryptovizor) инициализирован")
    bot4 = None
    if BOT4_BOT_TOKEN:
        try:
            from aiogram import Bot as _B4
            from aiogram.client.default import DefaultBotProperties as _DP4
            from aiogram.enums import ParseMode as _PM4
            bot4 = _B4(token=BOT4_BOT_TOKEN, default=_DP4(parse_mode=_PM4.HTML))
            logger.info("✅ BOT4 (AI Signal) инициализирован")
        except Exception as e:
            logger.error(f"BOT4 init fail: {e}")

    setup_watcher(bot, ADMIN_CHAT_ID, bot2=bot2, bot4=bot4)

    logger.info("Запуск admin (watcher/bots стартуют через lifespan)…")
    # Watcher, bots, userbot запускаются в lifespan event FastAPI
    # чтобы избежать deadlock с asyncio.gather + uvicorn
    await _serve_admin()


if __name__ == "__main__":
    asyncio.run(main())
