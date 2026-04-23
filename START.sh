#!/usr/bin/env bash
# ══════════════════════════════════════════════════════
#  Tradium Signal — one-click start на macOS/Linux
# ══════════════════════════════════════════════════════
set -e
cd "$(dirname "$0")"

echo ""
echo "══════════════════════════════════════════════════════"
echo " Tradium Signal"
echo "══════════════════════════════════════════════════════"
echo ""

# ── 1. Python ──
if ! command -v python3 &>/dev/null; then
    echo "[X] Python 3.11+ не установлен."
    echo ""
    echo "macOS:  brew install python@3.13"
    echo "Ubuntu: sudo apt install python3.13 python3.13-venv"
    echo ""
    exit 1
fi
PYVER=$(python3 --version | awk '{print $2}')
echo "[✓] Python $PYVER"

# ── 2. .env ──
if [ ! -f ".env" ]; then
    echo "[X] .env не найден в папке проекта."
    echo ""
    echo "Убедись что распаковал АРХИВ из pack-portable.ps1,"
    echo "а не склонировал с GitHub (там .env нет)."
    echo ""
    exit 1
fi
echo "[✓] .env найден"

# ── 3. venv ──
if [ ! -f ".venv/bin/python" ]; then
    echo ""
    echo "Первый запуск — создаю виртуальное окружение..."
    python3 -m venv .venv
    echo "[✓] .venv создан"
fi

# ── 4. deps ──
source .venv/bin/activate
if [ ! -f ".venv/.deps_installed" ]; then
    echo ""
    echo "Устанавливаю зависимости (2-3 минуты в первый раз)..."
    python -m pip install --upgrade pip --quiet
    python -m pip install -r requirements.txt
    touch .venv/.deps_installed
    echo "[✓] Зависимости установлены"
else
    echo "[✓] Зависимости уже установлены"
fi

# ── 5. git config (если не настроено) ──
if [ -d ".git" ]; then
    if ! git config user.name &>/dev/null; then
        git config user.name "James Wood"
        git config user.email "jameswood@tradium.com"
        echo "[✓] Git identity настроена"
    fi
fi

echo ""
echo "══════════════════════════════════════════════════════"
echo " Запуск сервера"
echo "══════════════════════════════════════════════════════"
echo ""
echo "  UI:        http://localhost:8000"
echo "  Логин:     см. ADMIN_USERNAME / ADMIN_PASSWORD в .env"
echo "  Остановить: Ctrl+C"
echo ""
echo "══════════════════════════════════════════════════════"
echo ""

python dev.py
