"""Берёт последние 30 сообщений из группы и показывает, как их парсит parser.py"""
import asyncio
import os
import sys

from telethon import TelegramClient

from config import API_ID, API_HASH, PHONE, SOURCE_GROUP_ID
from parser import parse_signal


async def main():
    session = os.path.join(os.path.dirname(os.path.abspath(__file__)), "session_userbot")
    client = TelegramClient(session, API_ID, API_HASH)
    await client.start(phone=PHONE)

    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    async for msg in client.iter_messages(SOURCE_GROUP_ID, limit=limit):
        if not msg.raw_text:
            continue
        parsed = parse_signal(msg.raw_text)
        # Показываем только те где есть пара и trend
        head = msg.raw_text.split("\n")[0][:80]
        print(f"\n─── msg={msg.id} {msg.date.strftime('%m-%d %H:%M')} ───")
        print(f"head: {head}")
        print(f"parsed: pair={parsed.get('pair')} tf={parsed.get('timeframe')} "
              f"dir={parsed.get('direction')} entry={parsed.get('entry')} "
              f"tp1={parsed.get('tp1')} sl={parsed.get('sl')} "
              f"trend={parsed.get('trend')}")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
