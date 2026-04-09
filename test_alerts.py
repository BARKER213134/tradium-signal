"""Отправляет тестовые алерты всех типов в бот."""
import asyncio
import logging

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from config import BOT_TOKEN, ADMIN_CHAT_ID
from database import SessionLocal, Signal
import watcher

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("test")


async def main():
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    watcher.setup(bot, ADMIN_CHAT_ID)

    db = SessionLocal()
    s = db.query(Signal).first()
    if not s:
        logger.error("Нет сигналов для теста")
        return

    # Искусственные значения для демонстрации
    logger.info(f"Тестим на сигнале #{s.id} {s.pair}")

    # 1. DCA4 алерт
    await watcher._send_dca4_alert(s, s.dca4 or (s.entry * 0.98 if s.entry else 1))
    await asyncio.sleep(1)

    # 2. Паттерн алерт
    await watcher._send_pattern_alert(s, "Bullish Engulfing", s.dca4 or s.entry or 1)
    await asyncio.sleep(1)

    # 3. TP алерт
    if s.tp1 and s.entry:
        pnl = ((s.tp1 - s.entry) / s.entry) * 100
        if s.direction in ("SHORT", "SELL"): pnl = -pnl
        await watcher._send_close_alert(s, "TP", s.tp1, pnl)
    await asyncio.sleep(1)

    # 4. SL алерт
    if s.sl and s.entry:
        pnl = ((s.sl - s.entry) / s.entry) * 100
        if s.direction in ("SHORT", "SELL"): pnl = -pnl
        await watcher._send_close_alert(s, "SL", s.sl, pnl)

    db.close()
    await bot.session.close()
    logger.info("Готово")


if __name__ == "__main__":
    asyncio.run(main())
