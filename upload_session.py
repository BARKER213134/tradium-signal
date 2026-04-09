"""Однократно загружает session_userbot.session в MongoDB.
Запускать локально ОДИН РАЗ перед деплоем на Railway — после этого любой
инстанс с тем же MONGO_URL восстановит session автоматически при старте."""
import os

from database import _get_db


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(here, "session_userbot.session")
    if not os.path.exists(path):
        print(f"ERROR: {path} не найден")
        return
    with open(path, "rb") as f:
        data = f.read()
    col = _get_db().system
    col.update_one(
        {"_id": "telethon_session"},
        {"$set": {"data": data, "size": len(data)}},
        upsert=True,
    )
    print(f"OK: session uploaded to Mongo ({len(data)} bytes)")


if __name__ == "__main__":
    main()
