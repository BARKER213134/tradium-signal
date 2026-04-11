import asyncio
import base64
import logging
import os

import uvicorn

from database import init_db
from config import BOT_TOKEN, ADMIN_CHAT_ID, API_ID, API_HASH, BOT2_BOT_TOKEN, BOT4_BOT_TOKEN


def _bootstrap_session():
    """Session можно восстановить из:
    1) env TELETHON_SESSION_B64 (быстро, но ограничен размером)
    2) MongoDB collection 'system' документ _id='telethon_session'
    """
    here = os.path.dirname(os.path.abspath(__file__))
    session_path = os.path.join(here, "session_userbot.session")
    if os.path.exists(session_path):
        return

    # 1) env var
    b64 = os.getenv("TELETHON_SESSION_B64")
    if b64:
        try:
            with open(session_path, "wb") as f:
                f.write(base64.b64decode(b64))
            logging.info(f"✅ Session восстановлен из TELETHON_SESSION_B64 ({len(b64)} chars)")
            return
        except Exception as e:
            logging.error(f"TELETHON_SESSION_B64 decode failed: {e}")

    # 2) MongoDB
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

    setup_watcher(bot, ADMIN_CHAT_ID, bot2=bot2, bot3=bot3, bot4=bot4)

    logger.info("Запуск admin / bots / userbot / watcher…")
    tasks = [
        _serve_admin(),
        start_userbot(),
        start_bot(),
        start_watcher(),
    ]
    if bot2:
        tasks.append(start_bot2())
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
