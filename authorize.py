"""Интерактивная авторизация Telethon. Запускать ОТДЕЛЬНО от main.py.
Создаст session_userbot.session, после чего main.py подхватит его."""
import asyncio
import os

from telethon import TelegramClient

from config import API_ID, API_HASH, PHONE


async def main():
    session_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "session_userbot")
    client = TelegramClient(session_path, API_ID, API_HASH)
    print(f"Авторизация для {PHONE}…")
    await client.start(phone=PHONE)
    me = await client.get_me()
    print(f"✅ Готово: {me.first_name} (@{me.username}) id={me.id}")
    print(f"Session: {session_path}.session")
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
