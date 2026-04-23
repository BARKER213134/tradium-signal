# Перенос проекта на новый ПК — 3 шага

## На старом компьютере

1. Запусти `python pack-portable.py` (или через двойной клик на `pack-portable.ps1` если PowerShell разрешает)
2. Получаешь `tradium-signal-portable-ГГГГ-ММ-ДД.zip` на Рабочем столе
3. Копируй на флешку / в облако

## На новом компьютере

1. Установи **Python 3.11+** → https://www.python.org/downloads/
   _При установке обязательно галка "Add Python to PATH"_
2. Распакуй архив в удобную папку
3. **Двойной клик на `START.bat`** (Windows) или `./START.sh` (Mac/Linux)

Первый запуск 2-3 мин. UI откроется на **http://localhost:8001**.

---

## ⚠️ КРИТИЧНО: Telegram сессия и Railway

В Telegram сессия — это файл `*.session`. Если его использовать из двух мест одновременно (Railway + локальный ПК), Telethon начнёт получать `AUTH_KEY_DUPLICATED` ошибки, клиенты будут дисконнектить друг друга, могут приходить дубли алертов.

### `START.bat` запускает в DEV MODE — безопасно

По умолчанию `START.bat` → `dev.py` → выставляет `DEV_MODE=1` → **userbot/watcher/bots НЕ стартуют** на локальной машине. Работает только:
- UI / админка / логин
- Бэктесты (`/api/backtest-*`)
- Журнал сигналов
- Просмотр графиков

**Railway продолжает быть единственным production-инстансом** — ловит сигналы Cryptovizor/Tradium, ведёт автоторговлю, отправляет алерты. Новый ПК не мешает.

### Когда хочешь полноценно переключить production на новый ПК

1. Останови Railway (Dashboard → сервис → Pause / Remove deployment)
2. На новом ПК: убери `DEV_MODE=1` из `.env` (или закомментируй)
3. Запусти через `python main.py` (полный режим) или убери DEV_MODE из dev.py

После этого на новом ПК будет работать userbot — Railway можно выключать навсегда.

---

## Что содержит архив

- Весь код + `.git/` история
- **Секреты** (`.env`) — API ключи, MongoDB URI, Telegram credentials
- **Telegram сессия** (`*.session`, `_session_b64.txt`) — userbot авторизован
- `requirements.txt` + launcher-скрипты

Исключено (мусор):
- `.venv/`, `__pycache__/`, `.pytest_cache/`
- `charts/`, `logs/`, `data/`, `tmp/`
- `_mongo_backup.json`, `*.log`, `*.pyc`

## Типичные проблемы

**"Python not found"** → поставь Python 3.11+ и перезапусти START

**"No module named X"** → удали `.venv/.deps_installed` и запусти START снова

**"AUTH_KEY_DUPLICATED" в логах** → ты забыл DEV_MODE=1 и запустил userbot параллельно с Railway. Останови локальный процесс немедленно, иначе Telegram может забанить аккаунт на несколько часов.

**"Session expired"** → пересоздай через `python authorize.py`, введи код из SMS.

**"Mongo connection refused"** → MongoDB Atlas блокирует новый IP. https://cloud.mongodb.com → Network Access → Add IP → 0.0.0.0/0 (или твой текущий IP)

**`git push` не работает** → `git config user.name "Your Name"` + `git config user.email "your@email.com"`

---

## Claude Code продолжает работать

Папка `.claude/` в архиве — настройки проекта (slash-команды, permissions) переносятся автоматом. Личная memory-папка не переносится, Claude наработает новую по мере работы.
