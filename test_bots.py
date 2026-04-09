"""Тест обоих ботов: отправляет по одному сообщению каждым на ADMIN_CHAT_ID."""
import asyncio

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from config import BOT_TOKEN, BOT2_BOT_TOKEN, ADMIN_CHAT_ID


async def main():
    # Tradium bot
    try:
        b1 = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        me1 = await b1.get_me()
        await b1.send_message(
            ADMIN_CHAT_ID,
            (
                f"✅ <b>Tradium bot test</b>\n"
                f"@{me1.username} работает и может писать тебе.\n"
                f"Сюда будут приходить алерты DCA4 / Pattern / TP / SL."
            ),
        )
        await b1.session.close()
        print(f"[OK] Tradium @{me1.username}")
    except Exception as e:
        print(f"[FAIL] Tradium: {type(e).__name__}: {e}")

    # Cryptovizor bot
    try:
        b2 = Bot(token=BOT2_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        me2 = await b2.get_me()
        await b2.send_message(
            ADMIN_CHAT_ID,
            (
                f"✅ <b>Cryptovizor bot test</b>\n"
                f"@{me2.username} работает и может писать тебе.\n"
                f"Сюда будут приходить алерты о паттернах на 30m."
            ),
        )
        await b2.session.close()
        print(f"[OK] Cryptovizor @{me2.username}")
    except Exception as e:
        print(f"[FAIL] Cryptovizor: {type(e).__name__}: {e}")


if __name__ == "__main__":
    asyncio.run(main())
