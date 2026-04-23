# Перенос проекта на новый ПК — 3 шага

## На старом компьютере

1. Правый клик на `pack-portable.ps1` → **Run with PowerShell**
2. Получаешь `tradium-signal-portable-ГГГГ-ММ-ДД.zip` на Рабочем столе
3. Копируй на флешку / в облако

## На новом компьютере

1. Установи **Python 3.11+** → https://www.python.org/downloads/
   _При установке обязательно галка "Add Python to PATH"_
2. Распакуй архив в удобную папку
3. **Двойной клик на `START.bat`** (Windows) или `./START.sh` (Mac/Linux)

Первый запуск займёт 2-3 минуты (создаст `.venv`, поставит зависимости). Следующие запуски — мгновенно.

UI откроется на **http://localhost:8000**. Логин из `.env` (переменные `ADMIN_USERNAME` / `ADMIN_PASSWORD`).

---

## Что содержит архив

Всё нужное для продолжения работы:

- **Весь код** + история коммитов (`.git/`)
- **Секреты** (`.env`) — API ключи, MongoDB URI, Telegram credentials
- **Telegram сессия** (`*.session`, `_session_b64.txt`) — userbot авторизован
- **`requirements.txt`** + launcher-скрипты

Исключено (мусор, не нужно):
- `.venv/`, `__pycache__/`, `.pytest_cache/`
- `charts/`, `logs/`, `data/`, `tmp/`
- `_mongo_backup.json`, `*.log`, `*.pyc`

## Что делать если упало

**"Python not found"** → поставь Python 3.11+ и перезапусти START

**"No module named X"** → удали `.venv\.deps_installed` и запусти START снова — переставит зависимости

**"Unauthorized" / "Session expired"** → Telegram сессия протухла. Беги `python authorize.py` и введи код из SMS.

**"Mongo connection refused"** → MongoDB Atlas блокирует новый IP. Зайди https://cloud.mongodb.com → Network Access → Add IP → 0.0.0.0/0 (или свой IP)

**`git push` не работает** → `git config user.name "Your Name"` и `git config user.email "your@email.com"`

---

## Работа продолжается без потерь

После START.bat сразу:
- Работает `git pull` / `git push` (история сохранена)
- Работает Telegram userbot (сессия перенесена)
- Работает MongoDB (URI в .env, база облачная)
- Claude Code сам подхватит `.claude/` настройки проекта

**Railway production** — отдельный облачный деплой, не зависит от компьютера вообще. `git push origin main` из любого ПК → Railway деплоит.
