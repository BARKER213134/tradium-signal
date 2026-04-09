# Tradium Signal Monitor

Мониторинг торговых сигналов из Telegram с AI-анализом, паттернами, алертами в бот и веб-админкой.

Два источника сигналов:
1. **Tradium Setups** (группа с графиками) — парсит текст + график, извлекает DCA уровни через Claude Vision, следит когда цена дойдёт до DCA #4, ждёт свечной паттерн подтверждения
2. **Cryptovizor** (@TRENDS_Cryptovizor) — парсит списки тикеров с трендовыми кружками, следит на 30m свечах за reversal+continuation паттернами

## Стек

- **FastAPI** — админка + WebSocket + REST API
- **Telethon** — userbot читает сообщения из Telegram
- **aiogram** — боты шлют алерты
- **MongoDB Atlas** — хранилище сигналов и событий
- **Anthropic Claude** — Vision для графиков + quality scoring
- **mplfinance** — генерация PNG-графиков для алертов
- **Binance Vision API** — цены и свечи

## Быстрый старт (Docker)

### 1. Подготовь секреты

```bash
cp .env.example .env
nano .env  # заполни все поля
```

Что нужно получить:
- `API_ID` + `API_HASH` — на https://my.telegram.org/apps
- `BOT_TOKEN`, `BOT2_BOT_TOKEN` — от @BotFather (создай 2 бота)
- `ANTHROPIC_API_KEY` — https://console.anthropic.com
- `MONGO_URL` — https://cloud.mongodb.com (бесплатный M0 кластер)
- `ADMIN_CHAT_ID` — узнай у @userinfobot на том аккаунте где хочешь получать алерты

### 2. Авторизуй Telethon-сессию (ОДИН РАЗ)

```bash
docker compose run --rm tradium python auth_step1.py
# придёт код в Telegram
docker compose run --rm tradium python auth_step2.py 12345 "2FA_password_if_any"
```

Созданный `session_userbot.session` будет замонтирован из `./data/` в контейнер.

### 3. Запусти стек

```bash
docker compose up -d
docker compose logs -f tradium
```

Админка на **http://localhost:8000** (логин `admin` / пароль из `.env`).

## Запуск без Docker (локально)

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env
# заполни .env

python auth_step1.py
python auth_step2.py <code>
python main.py
```

## Backfill прошлых сигналов

```bash
# Tradium (последние N сообщений из группы)
python backfill.py 500

# Cryptovizor (последние N сообщений)
python backfill_cryptovizor.py 200
```

## Архитектура

```
┌──────────────────┐     ┌────────────┐
│ Telegram groups  │────▶│  userbot   │ (Telethon)
└──────────────────┘     └─────┬──────┘
                               │ парсит
                               ▼
                         ┌──────────────┐
                         │  MongoDB     │ ◀────┐
                         └─────┬────────┘      │
                               │               │
                ┌──────────────┼───────────────┤
                ▼              ▼               │
         ┌──────────┐   ┌─────────────┐        │
         │ watcher  │   │   FastAPI   │        │
         │ 15s loop │   │ /admin + WS │        │
         └────┬─────┘   └─────────────┘        │
              │                                │
              ├─ check DCA#4 → alert           │
              ├─ check patterns (candle)       │
              ├─ check TP/SL                   │
              ├─ Cryptovizor pattern loop      │
              └─ AI quality scoring            │
                     │                         │
                     ▼                         │
             ┌────────────────┐                │
             │  aiogram bots  │────────────────┘
             │ Tradium + CV   │    (updates status)
             └────────────────┘
```

## Файлы

| Файл | Что делает |
|---|---|
| `main.py` | Entry point — asyncio.gather(admin, bot, userbot, watcher) |
| `userbot.py` | Telethon клиент, парсит сообщения из двух источников |
| `bot.py` | Aiogram бот для Tradium (команды /stats /last /signal) |
| `watcher.py` | Фоновый loop — DCA4/paттерны/TP/SL + Cryptovizor |
| `admin.py` | FastAPI админка + WebSocket + REST |
| `database.py` | MongoDB-обёртка с ORM-подобным API |
| `parser.py` | Парсер Tradium Setups |
| `parser_cryptovizor.py` | Парсер Cryptovizor сообщений |
| `patterns.py` | Разворотные свечные паттерны |
| `continuation_patterns.py` | Паттерны продолжения тренда |
| `levels.py` | Pivot S1/R1 |
| `exchange.py` | Binance public API (prices + klines) |
| `ai_analyzer.py` | Claude Vision + quality scoring |
| `chart_renderer.py` | mplfinance → PNG для алертов |
| `backfill.py` | Backfill Tradium истории |
| `backfill_cryptovizor.py` | Backfill Cryptovizor истории |
| `auth_step1.py`, `auth_step2.py` | Неинтерактивная авторизация Telethon |

## Тесты

```bash
python -m pytest tests/ -v
```

55+ тестов: parser, patterns, levels, exchange (mocked), api (mongomock), watcher.

## Важно про безопасность

- **Никогда** не коммить `.env` и `*.session` файлы — они в `.gitignore`
- `session_userbot.session` = токен Telegram-аккаунта, при краже угонщик получает доступ ко всем диалогам
- 2FA пароль **разный** на Telegram и других сервисах
- Регулярно ротируй `ANTHROPIC_API_KEY` если подозреваешь утечку
- `SECRET_KEY` в `.env` — для HMAC session cookie админки, ставь случайную строку

## Troubleshooting

**`Userbot не авторизован`** — запусти `auth_step1.py` + `auth_step2.py`

**`aiogram TelegramUnauthorizedError`** — bot token невалидный, пересоздай через @BotFather

**`AuthKeyDuplicatedError`** — session использовалась одновременно в двух процессах. Удали `.session` файл и авторизуйся заново. Не запускай `session_check.py`/`auth_step*` при работающем сервере.

**`Binance 400 Unauthorized`** — один из тикеров не торгуется на Binance. Код автоматически падает на per-symbol fallback — это ОК.

**Cryptovizor сигналы не приходят** — проверь что `BOT2_SOURCE_GROUP=TRENDS Cryptovizor` точно совпадает с именем диалога, и userbot-аккаунт написал туда хоть одно сообщение (иначе Telethon его не видит в `iter_dialogs`).

## Команды бота

**Tradium bot** (`/start` для активации):

- `/stats` — сводная статистика
- `/last` — последние 10 сигналов
- `/watching` — ждущие DCA#4
- `/open` — открытые (ОТКРЫТ или ПАТТЕРН)
- `/results` — закрытые TP/SL
- `/signal <id>` — детали конкретного сигнала
