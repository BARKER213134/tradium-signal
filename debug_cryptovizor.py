"""Находит группу 'TRENDS Cryptovizor' и показывает последние сообщения."""
import asyncio
import os
import sys

from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument

from config import API_ID, API_HASH, BOT2_SOURCE_GROUP


async def main():
    session = os.path.join(os.path.dirname(os.path.abspath(__file__)), "session_userbot")
    client = TelegramClient(session, API_ID, API_HASH)
    await client.start()

    target_name = BOT2_SOURCE_GROUP or "TRENDS Cryptovizor"
    target_name_lc = target_name.lower()

    print(f"[debug] ищу диалог '{target_name}'...")

    found = None
    async for dialog in client.iter_dialogs():
        name = (dialog.name or "").lower()
        if target_name_lc in name:
            print(f"  match: id={dialog.id} name='{dialog.name}' entity={type(dialog.entity).__name__}")
            found = dialog

    if not found:
        print("ничего не найдено")
        await client.disconnect()
        return

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cryptovizor_dump.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(f"FOUND: id={found.id} name={found.name} entity={type(found.entity).__name__}\n\n")
        async for msg in client.iter_messages(found.id, limit=20):
            media = ""
            if isinstance(msg.media, MessageMediaPhoto):
                media = " [PHOTO]"
            elif isinstance(msg.media, MessageMediaDocument):
                media = f" [DOC:{msg.media.document.mime_type}]"
            f.write(f"\n=== msg={msg.id} {msg.date.strftime('%m-%d %H:%M')}{media} ===\n")
            f.write(msg.raw_text or "(empty)")
            f.write("\n")
    print(f"[debug] saved to {out_path}")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
