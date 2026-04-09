"""Шаг 1: запросить SMS код. Сохраняет phone_code_hash в .auth_state.json"""
import asyncio
import json
import os

from telethon import TelegramClient
from config import API_ID, API_HASH, PHONE


async def main():
    session_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "session_userbot")
    client = TelegramClient(session_path, API_ID, API_HASH)
    await client.connect()
    if await client.is_user_authorized():
        print("Уже авторизован, ничего делать не надо.")
        await client.disconnect()
        return
    print(f"Запрашиваю код для {PHONE}…")
    result = await client.send_code_request(PHONE)
    state = {"phone": PHONE, "phone_code_hash": result.phone_code_hash}
    with open(os.path.join(os.path.dirname(__file__), ".auth_state.json"), "w") as f:
        json.dump(state, f)
    print("✅ Код отправлен! Пришли его в чат, я выполню вход.")
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
