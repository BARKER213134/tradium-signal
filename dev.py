"""Локальный dev-сервер — только админка, без userbot/watcher/bots.

БЕЗОПАСНО запускать параллельно с Railway. Выставляет DEV_MODE=1 до
импорта admin — lifespan видит флаг и не стартует Telethon.
"""
import os
import sys

# КРИТИЧНО: DEV_MODE должен быть установлен ДО импорта admin.py
# иначе lifespan не увидит флаг и запустит userbot.
os.environ["DEV_MODE"] = "1"

# cwd — папка с кодом (templates/static резолвились)
here = os.path.dirname(os.path.abspath(__file__))
os.chdir(here)
sys.path.insert(0, here)

import uvicorn
from database import init_db

init_db()

if __name__ == "__main__":
    print("=" * 58)
    print(" Tradium Signal — DEV mode (Telethon disabled)")
    print("=" * 58)
    print(" UI:    http://127.0.0.1:8001")
    print(" Login: see ADMIN_USERNAME / ADMIN_PASSWORD in .env")
    print("=" * 58)
    uvicorn.run("admin:app", host="127.0.0.1", port=8001, reload=False)
