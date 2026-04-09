"""Проверяет жива ли session_userbot.session и пытается узнать состояние аккаунта.

Если сессия живая:
- Покажет имя, username, phone
- Покажет список активных авторизаций (все устройства)
- Предложит terminate-вариант
"""
import asyncio
import os
import sys

from telethon import TelegramClient
from telethon.tl.functions.account import GetAuthorizationsRequest, ResetAuthorizationRequest

from config import API_ID, API_HASH


async def main():
    session = os.path.join(os.path.dirname(os.path.abspath(__file__)), "session_userbot")
    if not os.path.exists(session + ".session"):
        print("[-] session файл не найден")
        return

    client = TelegramClient(session, API_ID, API_HASH)
    print("[*] Connect...")
    await client.connect()

    authorized = await client.is_user_authorized()
    print(f"[*] is_user_authorized: {authorized}")

    if not authorized:
        print("[-] сессия мертва, угонщик её терминировал")
        await client.disconnect()
        return

    try:
        me = await client.get_me()
        print(f"[+] Активна. Имя: {me.first_name} @{me.username} phone={me.phone} id={me.id}")
    except Exception as e:
        print(f"[-] get_me failed: {e}")
        await client.disconnect()
        return

    # Список всех авторизаций
    print("\n[*] Получаю список всех активных сессий аккаунта...")
    try:
        auths = await client(GetAuthorizationsRequest())
        print(f"Всего сессий: {len(auths.authorizations)}")
        for i, a in enumerate(auths.authorizations):
            is_current = getattr(a, "current", False)
            flag = " <-- ТЕКУЩАЯ (наша)" if is_current else ""
            print(f"  [{i}] hash={a.hash} device='{a.device_model}' platform='{a.platform}' app='{a.app_name}' country='{a.country}' ip={a.ip} date={a.date_active}{flag}")
    except Exception as e:
        print(f"[-] Failed: {e}")

    await client.disconnect()
    print("\n[*] Done. Если сессия жива и видны ЧУЖИЕ — можно terminate их через ResetAuthorizationRequest(hash).")


if __name__ == "__main__":
    asyncio.run(main())
