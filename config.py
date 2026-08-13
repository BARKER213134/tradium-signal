import os
from dotenv import load_dotenv

load_dotenv(override=True)

# Telethon userbot удалён — Telegram-сканирование отключено (2026-07-01)

# ✂ 13.08.26: полугодовой аудит живых исходов (44k сигналов) — стабильный
# минус/ноль при огромном потоке. Генерация выключена, старые доки в Mongo
# остаются, из журнала/графиков скрыты. vol_anomaly/vol_anomaly4h оставлены
# по решению юзера. Вернуть стратегию = убрать из этого сета.
DISABLED_STRATEGIES = {
    "st_break", "vol_accum", "second_flip", "volume_surge",
    "whale", "shark", "impulse", "ignition", "ten", "combo",
    "capitulation",
}

# Telegram Bot (Aiogram)
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))

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

# BOT2 (Cryptovizor) удалён вместе с подпиской (2026-07-01)

# Четвёртый бот — AI Signal (лучшие сигналы)
BOT4_BOT_TOKEN = os.getenv("BOT4_BOT_TOKEN", "")

# BOT3 (Anomaly Alerts) удалён вместе с источником (2026-07-02)

# Пятый бот — Confluence Scanner
BOT5_BOT_TOKEN = os.getenv("BOT5_BOT_TOKEN", "")

# Шестой бот — Paper Trading
BOT6_BOT_TOKEN = os.getenv("BOT6_BOT_TOKEN", "")
# 🌊 условное депо paper-канала ПОТОК для $-учёта (03.08)
POTOK_DEPO_USD = float(os.getenv("POTOK_DEPO_USD", "1000"))

# BOT7 (Cluster Alerts) удалён вместе с источником (2026-07-02)


# BOT9 — Top Picks alerts (сигналы подтверждённые STRONG Confluence)
BOT9_BOT_TOKEN = os.getenv("BOT9_BOT_TOKEN", "")

# BOT10 — SuperTrend signals (VIP / Triple MTF / Daily Filter)
# Токен выдан пользователем, хранить в Railway Variables
BOT10_BOT_TOKEN = os.getenv("BOT10_BOT_TOKEN", "")

# BOT11 — Live Trading confirmation (⚠️ реальные деньги!)
# AI шлёт алерт с inline-кнопками ✅/❌ в этот бот, исполнение только
# по подтверждению. Токен задаётся в Railway когда готовы к live.
BOT11_BOT_TOKEN = os.getenv("BOT11_BOT_TOKEN", "")

# BOT12 — использовался для CV flip, теперь транспорт для MOONSHOT stack-алертов.
BOT12_BOT_TOKEN = os.getenv("BOT12_BOT_TOKEN", "")
_cv_flip_chat = os.getenv("CV_FLIP_CHAT_ID", "").strip()
CV_FLIP_CHAT_ID = (int(_cv_flip_chat) if _cv_flip_chat.lstrip("-").isdigit()
                   else ADMIN_CHAT_ID)

# 13-й бот — Pre-Pump Predictor (leading indicators: volume/OI/funding/BB squeeze).
# Алерты только PRIME tier (composite_score >= 75) — редкие но качественные.
# Rate limit 4h per pair. Если не задан — alerts не шлются, scanner работает.
BOT13_BOT_TOKEN = os.getenv("BOT13_BOT_TOKEN", "")
_pp_chat = os.getenv("PREPUMP_CHAT_ID", "").strip()
PREPUMP_CHAT_ID = (int(_pp_chat) if _pp_chat.lstrip("-").isdigit()
                   else ADMIN_CHAT_ID)
# Quiet hours для BOT13 alerts (МСК часы, range "HH-HH")
_pp_qh = os.getenv("PREPUMP_QUIET_HOURS", "").strip()
PREPUMP_QUIET_HOURS = _pp_qh  # пример: "02-08" = 02:00-08:00 МСК молчим

# BOT13 — New Strategy alerts (🌊 Volume Surge / 🐉 Triple Confluence /
# 🔋 Volume Accumulation). Triggered after each ST flip.
# Backtest validated 14d, 11.5k signals, OOS holdout: WR 67-72%.
BOT13_BOT_TOKEN = os.getenv("BOT13_BOT_TOKEN", "")
_new_strat_chat = os.getenv("NEW_STRATEGY_CHAT_ID", "").strip()
NEW_STRATEGY_CHAT_ID = (int(_new_strat_chat) if _new_strat_chat.lstrip("-").isdigit()
                       else ADMIN_CHAT_ID)

# BOT15 — 🔥 HOT signals (только score >= 60).
# Дедуп: 1 alert/час на (pair, direction). Глобальный rate-limit: 10/час.
BOT15_BOT_TOKEN = os.getenv(
    "BOT15_BOT_TOKEN",
    "8559565442:AAG4WdjTE0T7XLNuuZMZe_77McsycLbEFj4",
)
_hot_chat = os.getenv("HOT_SIGNALS_CHAT_ID", "").strip()
HOT_SIGNALS_CHAT_ID = (int(_hot_chat) if _hot_chat.lstrip("-").isdigit()
                       else ADMIN_CHAT_ID)
HOT_SIGNALS_MIN_SCORE = int(os.getenv("HOT_SIGNALS_MIN_SCORE", "60"))

# BOT16 — 🐋 WHALE signals (Range Breakout pattern из 20-chart analysis).
# Тиры: PREMIUM (≥80) → "lottery moonshot", STANDARD (60-79) → workhorse.
# MARGINAL отрезан (40-59) — WR < baseline.
# Trigger: ST flip 2H UP + vol spike ≥2x + amplifiers.
# Backtest 30d: STANDARD WR 53.8%, MFE 5.56% — лучше всех LONG signals.
BOT16_BOT_TOKEN = os.getenv("BOT16_BOT_TOKEN", "")
_whale_chat = os.getenv("WHALE_CHAT_ID", "").strip()
WHALE_CHAT_ID = (int(_whale_chat) if _whale_chat.lstrip("-").isdigit()
                 else ADMIN_CHAT_ID)
WHALE_MIN_TIER = os.getenv("WHALE_MIN_TIER", "STANDARD")  # STANDARD or PREMIUM

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
# category: 'crypto' | 'stocks'
# tradium / cryptovizor / cv_flip вкладки удалены — ingestion отключён (2026-07-01)
BOTS = [
    # ✂ 13.08: вкладки мёртвых источников скрыты (аудит 180д + чистка);
    # все живы по прямым URL /signals?bot=<id>. Вернуть: раскомментировать.
    # {"id": "confluence", "label": "Confluence", "category": "crypto"},
    {"id": "supertrend", "label": "🌀 SuperTrend", "category": "crypto"},
    {"id": "new_strategies", "label": "🌊 New Strategy", "category": "crypto"},
    # {"id": "conflicts", "label": "⚠ Conflicts", "category": "crypto"},  # кнопка скрыта
    # — функционал остаётся: endpoint /api/conflicts, вкладка по прямому URL
    # /signals?bot=conflicts работает, данные детектируются в фоне через
    # anti_cluster_detector. Включить обратно: раскомментировать строку.
    {"id": "entry_checker", "label": "🎯 Entry Checker", "category": "crypto"},
    # {"id": "whale", "label": "🐋 WHALE", "category": "crypto"},      # ✂ 13.08
    # {"id": "shark", "label": "🦈 SHARK", "category": "crypto"},      # ✂ 13.08
    {"id": "stack", "label": "🧩 Stack", "category": "crypto"},
    # {"id": "momentum", "label": "🚀 Impulse·Fade", "category": "crypto"},  # ✂ 13.08 (fade — в журнале)
    # {"id": "st_break", "label": "🧨 ST-Пробой", "category": "crypto"},     # ✂ 13.08
    {"id": "st_break4h", "label": "💣 ST-Пробой 4h", "category": "crypto"},
    {"id": "blowoff", "label": "🌋 Blowoff", "category": "crypto"},
    # {"id": "capitulation", "label": "🛟 Капитуляция", "category": "crypto"},  # ✂ 13.08
    {"id": "footprint", "label": "🧮 Кластеры", "category": "crypto"},
    {"id": "potok", "label": "🌊 ПОТОК (авто)", "category": "crypto"},
    {"id": "thin_pump", "label": "💨 Тонкий памп", "category": "crypto"},
    {"id": "vol_anomaly", "label": "⚡ Аномалии объёма", "category": "crypto"},
    {"id": "accum_entry", "label": "🧊💥 База→Вход", "category": "crypto"},
    # 📈 График убран из навигации (2026-07-11) — графики есть в журнале
    # (клик по паре → KChart). Вкладка жива по прямому URL /signals/bigchart.
    # {"id": "bigchart", "label": "📈 График", "category": "crypto"},
    {"id": "setup_check", "label": "🎰 Setup Check", "category": "crypto"},
    # 📈 Тренды — своя замена вкладки CryptoVizor (07.08.26): матрица
    # SuperTrend-направлений по ТФ из скана, спидометр широты рынка
    {"id": "today", "label": "🧭 Сегодня", "category": "crypto"},
    {"id": "trends", "label": "📈 Тренды", "category": "crypto"},
    {"id": "journal", "label": "Журнал", "category": "crypto"},
    # 💱 FOREX: проп FundingPips — уикенд-гэп на кроссах (единственная
    # модель, пережившая стенды M1 Dukascopy 03.08.26). Сканер gap_scanner.
    {"id": "fundingpips", "label": "💱 FundingPips", "category": "stocks"},
    # Pre-Pump удалён — backtest показал что edge только в triple_confluence
    # и st_vip которые уже есть в журнале. Заменён на 🧠 COMBO signal.
    # {"id": "prepump", "label": "🔥 Pre-Pump", "category": "crypto"},
    # HOT NOW убран — не работает нормально. Template остаётся для direct URL access.
    # {"id": "hot_now", "label": "🔥 HOT NOW", "category": "crypto"},
    # {"id": "autotrading", "label": "Авто-торговля", "category": "crypto"},
    # — кнопка скрыта 26.07: заменена каналом 🌊 ПОТОК. Старая вкладка жива
    # по прямому URL /signals?bot=autotrading (momentum-paper продолжает
    # работать в фоне). Вернуть: раскомментировать строку.
]

# Админка
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
SECRET_KEY = os.getenv("SECRET_KEY", "tradium_secret_2026")
if ADMIN_PASSWORD == "admin123":
    import logging as _log
    _log.warning("⚠ ADMIN_PASSWORD=admin123 (дефолтный) — задайте свой в .env или Railway Variables")
