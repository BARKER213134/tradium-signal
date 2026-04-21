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
# Топик для ✨ Verified Entries (Entry Checker автопроверка).
# @topmonetabot = BOT9 (Top Picks) — verified сообщения шлются через него
# в ADMIN_CHAT_ID. Если задан VERIFIED_TOPIC_ID — в конкретный топик форума,
# иначе — в общую ленту чата.
_vt = os.getenv("VERIFIED_TOPIC_ID", "").strip()
VERIFIED_TOPIC_ID = int(_vt) if _vt.isdigit() else None

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

# Седьмой бот — Cluster Alerts
BOT7_BOT_TOKEN = os.getenv("BOT7_BOT_TOKEN", "")

# Восьмой бот — Forex FVG Alerts
BOT8_BOT_TOKEN = os.getenv("BOT8_BOT_TOKEN", "")

# BOT9 — Top Picks alerts (сигналы подтверждённые STRONG Confluence)
BOT9_BOT_TOKEN = os.getenv("BOT9_BOT_TOKEN", "")

# BOT10 — SuperTrend signals (VIP / Triple MTF / Daily Filter)
# Токен выдан пользователем, хранить в Railway Variables
BOT10_BOT_TOKEN = os.getenv("BOT10_BOT_TOKEN", "")

# BOT11 — Live Trading confirmation (⚠️ реальные деньги!)
# AI шлёт алерт с inline-кнопками ✅/❌ в этот бот, исполнение только
# по подтверждению. Токен задаётся в Railway когда готовы к live.
BOT11_BOT_TOKEN = os.getenv("BOT11_BOT_TOKEN", "")

# ═════ Binance Futures API для реальной торговли ═════
# API keys — создавать с минимальными правами (только futures trade, без withdraw)
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")

# Режим торговли: paper | testnet | real
# По умолчанию paper — чтобы случайный рестарт контейнера НЕ запускал
# реальные ордера. Переключается через UI (/api/live/set-mode).
DEFAULT_TRADING_MODE = os.getenv("DEFAULT_TRADING_MODE", "paper")

# ═════ Safety лимиты по умолчанию (можно менять через UI presets) ═════
# Конкретные значения берутся из live_safety presets (консерв / умеренный / agress)

# TwelveData API — для форекс-данных (надёжнее чем yfinance для форекса)
# Free tier: 800 req/day, 8 req/min
# Регистрация: https://twelvedata.com/
TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")

# TradingView Webhook secret — защита от посторонних POST'ов
# Должен совпадать с "secret" полем в JSON template в TV alert
TV_WEBHOOK_SECRET = os.getenv("TV_WEBHOOK_SECRET", "tv_f9c3a8b2d4e7f6a1b8c5d9e2f3a7b4c6")

# Список ботов для UI
# category: 'crypto' | 'stocks' (forex/metals/indices/energy через FVG)
BOTS = [
    {"id": "tradium", "label": "Tradium", "category": "crypto"},
    {"id": BOT2_NAME, "label": BOT2_LABEL, "category": "crypto"},
    {"id": "anomaly", "label": "Аномалии", "category": "crypto"},
    {"id": "confluence", "label": "Confluence", "category": "crypto"},
    {"id": "clusters", "label": "Кластеры", "category": "crypto"},
    {"id": "top_picks", "label": "👑 Top Picks", "category": "crypto"},
    {"id": "supertrend", "label": "🌀 SuperTrend", "category": "crypto"},
    # {"id": "conflicts", "label": "⚠ Conflicts", "category": "crypto"},  # кнопка скрыта
    # — функционал остаётся: endpoint /api/conflicts, вкладка по прямому URL
    # /signals?bot=conflicts работает, данные детектируются в фоне через
    # anti_cluster_detector. Включить обратно: раскомментировать строку.
    {"id": "entry_checker", "label": "🎯 Entry Checker", "category": "crypto"},
    {"id": "journal", "label": "Журнал", "category": "crypto"},
    {"id": "autotrading", "label": "Авто-торговля", "category": "crypto"},
    {"id": "forex_fvg", "label": "Forex FVG 1H", "category": "stocks"},
    {"id": "forex_fvg_4h", "label": "🏆 Forex FVG 4H", "category": "stocks"},
    {"id": "forex_journal", "label": "Forex Журнал", "category": "stocks"},
]

# Админка
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
SECRET_KEY = os.getenv("SECRET_KEY", "tradium_secret_2026")
if ADMIN_PASSWORD == "admin123":
    import logging as _log
    _log.warning("⚠ ADMIN_PASSWORD=admin123 (дефолтный) — задайте свой в .env или Railway Variables")
