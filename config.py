import os
from dotenv import load_dotenv

load_dotenv(override=True)

# Telegram Userbot (Telethon)
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
PHONE = os.getenv("PHONE", "")

# Telegram Bot (Aiogram)
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))

# Группа Tradium WORKSPACE (супергруппа)
# Telethon: супергруппы = -100 + числовой ID
SOURCE_GROUP_ID = int(os.getenv("SOURCE_GROUP_ID", "-1002423680272"))

# Сколько секунд ждать график после текстового сообщения
CHART_WAIT_SECONDS = int(os.getenv("CHART_WAIT_SECONDS", "5"))

# Anthropic API (Claude Vision) для анализа графиков
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = "claude-opus-4-5"

# Папка для хранения скачанных графиков
CHARTS_DIR = os.getenv("CHARTS_DIR", "./charts")

# База данных
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./signals.db")
MONGO_URL = os.getenv("MONGO_URL", "")
MONGO_DB = os.getenv("MONGO_DB", "tradium")

# Второй бот — Cryptovizor
BOT2_NAME = os.getenv("BOT2_NAME", "cryptovizor")
BOT2_LABEL = os.getenv("BOT2_LABEL", "Cryptovizor")
BOT2_BOT_TOKEN = os.getenv("BOT2_BOT_TOKEN", "")
BOT2_SOURCE_GROUP = os.getenv("BOT2_SOURCE_GROUP", "")


# Четвёртый бот — AI Signal (лучшие сигналы)
BOT4_BOT_TOKEN = os.getenv("BOT4_BOT_TOKEN", "")

# Список ботов для UI
BOTS = [
    {"id": "tradium", "label": "Tradium"},
    {"id": BOT2_NAME, "label": BOT2_LABEL},
]

# Админка
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
SECRET_KEY = os.getenv("SECRET_KEY", "supersecretkey123")
