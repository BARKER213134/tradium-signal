@echo off
REM ══════════════════════════════════════════════════════
REM  Tradium Signal — one-click start на новом компьютере
REM ══════════════════════════════════════════════════════
REM  Что делает:
REM   1. Проверяет Python
REM   2. Создаёт .venv если нет
REM   3. Ставит зависимости если нет
REM   4. Запускает dev-сервер на http://localhost:8000
REM ══════════════════════════════════════════════════════

setlocal
cd /d "%~dp0"
chcp 65001 >nul

echo.
echo ══════════════════════════════════════════════════════
echo  Tradium Signal
echo ══════════════════════════════════════════════════════
echo.

REM ── 1. Python ──
python --version >nul 2>&1
if errorlevel 1 (
    echo [X] Python 3.11+ не установлен.
    echo.
    echo Скачай отсюда: https://www.python.org/downloads/
    echo При установке ОБЯЗАТЕЛЬНО поставь галку "Add Python to PATH".
    echo.
    pause
    exit /b 1
)
for /f "tokens=2" %%v in ('python --version') do set PYVER=%%v
echo [✓] Python %PYVER%

REM ── 2. .env проверка ──
if not exist ".env" (
    echo [X] .env файл не найден в папке проекта.
    echo.
    echo Убедись что распаковал АРХИВ из pack-portable.ps1,
    echo а не склонировал с GitHub — там .env нет по security.
    echo.
    pause
    exit /b 1
)
echo [✓] .env найден

REM ── 3. venv ──
if not exist ".venv\Scripts\python.exe" (
    echo.
    echo Первый запуск — создаю виртуальное окружение...
    python -m venv .venv
    if errorlevel 1 (
        echo [X] Не удалось создать .venv
        pause
        exit /b 1
    )
    echo [✓] .venv создан
)

REM ── 4. dependencies ──
if not exist ".venv\.deps_installed" (
    echo.
    echo Устанавливаю зависимости ^(это занимает 2-3 минуты в первый раз^)...
    call .venv\Scripts\activate.bat
    python -m pip install --upgrade pip --quiet
    python -m pip install -r requirements.txt
    if errorlevel 1 (
        echo [X] pip install упал
        pause
        exit /b 1
    )
    echo. > .venv\.deps_installed
    echo [✓] Зависимости установлены
) else (
    call .venv\Scripts\activate.bat
    echo [✓] Зависимости уже установлены
)

REM ── 5. git config (один раз, если не настроено) ──
git rev-parse --git-dir >nul 2>&1
if not errorlevel 1 (
    git config user.name >nul 2>&1
    if errorlevel 1 (
        git config user.name "James Wood"
        git config user.email "jameswood@tradium.com"
        echo [✓] Git identity настроена
    )
)

echo.
echo ══════════════════════════════════════════════════════
echo  Запуск сервера
echo ══════════════════════════════════════════════════════
echo.
echo  UI:       http://localhost:8000
echo  Логин:    см. переменные ADMIN_USERNAME / ADMIN_PASSWORD в .env
echo  Остановить:  Ctrl+C
echo.
echo ══════════════════════════════════════════════════════
echo.

python dev.py

pause
