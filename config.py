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
# Топик (форум) внутри группы с Trade Setup Screener — только отсюда парсим сетапы
TRADIUM_SETUP_TOPIC_ID = int(os.getenv("TRADIUM_SETUP_TOPIC_ID", "3204"))

# Сколько секунд ждать график после текстового сообщения
CHART_WAIT_SECONDS = int(os.getenv("CHART_WAIT_SECONDS", "5"))

# Anthropic API (Claude Vision) для анализа графиков
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = "claude-sonnet-4-6"        # Vision (графики)
ANTHROPIC_MODEL_FAST = "claude-haiku-4-5-20251001"  # Текст (анализ, фильтр, TP/SL)

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

# Третий бот — Anomaly Alerts
BOT3_BOT_TOKEN = os.getenv("BOT3_BOT_TOKEN", "")

# Пятый бот — Confluence Scanner
BOT5_BOT_TOKEN = os.getenv("BOT5_BOT_TOKEN", "")

# Шестой бот — Paper Trading
BOT6_BOT_TOKEN = os.getenv("BOT6_BOT_TOKEN", "")

# Список ботов для UI
BOTS = [
    {"id": "tradium", "label": "Tradium"},
    {"id": BOT2_NAME, "label": BOT2_LABEL},
    {"id": "anomaly", "label": "Аномалии"},
    {"id": "confluence", "label": "Confluence"},
    {"id": "journal", "label": "Журнал"},
    {"id": "autotrading", "label": "Авто-торговля"},
]

# Админка
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
SECRET_KEY = os.getenv("SECRET_KEY", "tradium_secret_2026")
if ADMIN_PASSWORD == "admin123":
    import logging as _log
    _log.warning("⚠ ADMIN_PASSWORD=admin123 (дефолтный) — задайте свой в .env или Railway Variables")
