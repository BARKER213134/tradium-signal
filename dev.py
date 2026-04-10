"""Локальный dev-сервер — только админка, без userbot/watcher/bots.
Безопасно запускать параллельно с Railway (не трогает Telethon session)."""
import os
import sys

# Убеждаемся что cwd — папка с кодом (чтобы templates/static резолвились)
here = os.path.dirname(os.path.abspath(__file__))
os.chdir(here)
sys.path.insert(0, here)

import uvicorn
from database import init_db

init_db()

if __name__ == "__main__":
    uvicorn.run("admin:app", host="127.0.0.1", port=8001, reload=True)
