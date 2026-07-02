import asyncio
import base64
import logging
import os

import uvicorn

from database import init_db
from config import BOT_TOKEN, ADMIN_CHAT_ID, BOT4_BOT_TOKEN


# Telethon session bootstrap удалён — Telegram-сканирование отключено (2026-07-01)


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
    if not BOT_TOKEN:
        logger.error("Не задан BOT_TOKEN в .env!")
        return

    # Threadpool для asyncio.to_thread() — Railway Pro (24 vCPU / 24GB) =>
    # 120 потоков для максимального concurrent throughput на I/O-bound операциях
    # (ccxt API calls, MongoDB queries).
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
    from watcher import setup as setup_watcher, start_watcher

    # BOT2 (Cryptovizor) удалён вместе с CV подпиской
    bot2 = None
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
    # Watcher и bots запускаются в lifespan event FastAPI
    # чтобы избежать deadlock с asyncio.gather + uvicorn
    await _serve_admin()


if __name__ == "__main__":
    asyncio.run(main())
