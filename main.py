import asyncio
import logging

import uvicorn

from database import init_db
from config import BOT_TOKEN, ADMIN_CHAT_ID, API_ID, API_HASH, BOT2_BOT_TOKEN

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def _serve_admin():
    """Запуск uvicorn в том же event loop."""
    config = uvicorn.Config(
        "admin:app",
        host="0.0.0.0",
        port=8000,
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
    setup_watcher(bot, ADMIN_CHAT_ID, bot2=bot2)

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
