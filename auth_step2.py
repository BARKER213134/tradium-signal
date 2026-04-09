"""Шаг 2: войти с кодом."""
import asyncio
import json
import os
import sys

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from config import API_ID, API_HASH


async def main():
    code = sys.argv[1] if len(sys.argv) > 1 else None
    password = sys.argv[2] if len(sys.argv) > 2 else None
    if not code:
        print("usage: auth_step2.py <code> [password]")
        return

    here = os.path.dirname(os.path.abspath(__file__))
    state_path = os.path.join(here, ".auth_state.json")
    with open(state_path) as f:
        state = json.load(f)

    session_path = os.path.join(here, "session_userbot")
    client = TelegramClient(session_path, API_ID, API_HASH)
    await client.connect()
    try:
        await client.sign_in(
            phone=state["phone"],
            code=code,
            phone_code_hash=state["phone_code_hash"],
        )
    except SessionPasswordNeededError:
        if not password:
            print("ERROR: 2FA enabled, pass password as 2nd arg")
            await client.disconnect()
            return
        await client.sign_in(password=password)

    me = await client.get_me()
    print("OK", me.first_name, me.username, me.id)
    await client.disconnect()
    try:
        os.remove(state_path)
    except Exception:
        pass


if __name__ == "__main__":
    asyncio.run(main())
