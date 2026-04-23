# pack-portable.ps1 — упаковывает весь проект с секретами в zip
# Запуск: правый клик → Run with PowerShell
# Или:    powershell -ExecutionPolicy Bypass -File pack-portable.ps1

$ErrorActionPreference = "Stop"
$ProjectDir = $PSScriptRoot
$Stamp = Get-Date -Format "yyyy-MM-dd-HHmm"
$OutName = "tradium-signal-portable-$Stamp.zip"
$OutPath = Join-Path $HOME\Desktop $OutName

Write-Host ""
Write-Host "══════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host " Упаковка tradium-signal в портативный архив" -ForegroundColor Cyan
Write-Host "══════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

# Проверяем что секреты на месте
$missing = @()
foreach ($f in @(".env")) {
    if (-not (Test-Path (Join-Path $ProjectDir $f))) {
        $missing += $f
    }
}
$sessions = Get-ChildItem -Path $ProjectDir -Filter "*.session" -ErrorAction SilentlyContinue
if ($sessions.Count -eq 0) {
    Write-Host "⚠️  Не найден *.session файл — на новом ПК будет нужна ре-авторизация Telegram" -ForegroundColor Yellow
}
if ($missing.Count -gt 0) {
    Write-Host "❌ Не найдены критические файлы: $($missing -join ', ')" -ForegroundColor Red
    Write-Host "Убедись что запускаешь скрипт из корня проекта" -ForegroundColor Red
    Read-Host "Нажми Enter для выхода"
    exit 1
}

# Что исключаем (мусор — не нужен на новом ПК)
$exclude = @(
    ".venv", "venv", "env", "__pycache__", ".pytest_cache",
    ".mypy_cache", ".ruff_cache", "node_modules",
    "charts", "logs", "data", "tmp",
    "_mongo_backup.json", "main_out.log",
    "*.pyc", "*.pyo", "*.log"
)

Write-Host "Проект: $ProjectDir"
Write-Host "Архив:  $OutPath"
Write-Host ""
Write-Host "Исключаю: $($exclude -join ', ')" -ForegroundColor DarkGray
Write-Host ""

# Создаём временную папку и копируем туда только нужное
$TempDir = Join-Path $env:TEMP "tradium-portable-$Stamp"
New-Item -ItemType Directory -Force -Path $TempDir | Out-Null

Write-Host "Копирую файлы (исключая мусор)..." -ForegroundColor Cyan
# Используем robocopy — быстро и умеет исключать
$roboArgs = @(
    "$ProjectDir",
    "$TempDir",
    "/E",                          # рекурсивно с пустыми папками
    "/XD", ".git", ".venv", "venv", "env", "__pycache__",
           ".pytest_cache", ".mypy_cache", ".ruff_cache", "node_modules",
           "charts", "logs", "data", "tmp",
    "/XF", "*.pyc", "*.pyo", "*.log", "_mongo_backup.json", "main_out.log",
    "/NFL", "/NDL", "/NJH", "/NJS", "/NC", "/NS"   # тише логи
)
robocopy @roboArgs | Out-Null
# robocopy exit 0/1/2/3 = success, 8+ = error
if ($LASTEXITCODE -ge 8) {
    Write-Host "❌ robocopy failed (exit $LASTEXITCODE)" -ForegroundColor Red
    exit 1
}

# Копируем .git ОТДЕЛЬНО (robocopy его исключает, но нам нужна история)
Write-Host "Копирую .git (история коммитов)..." -ForegroundColor Cyan
robocopy (Join-Path $ProjectDir ".git") (Join-Path $TempDir ".git") /E /NFL /NDL /NJH /NJS /NC /NS | Out-Null

# Создаём zip
Write-Host "Сжимаю в zip..." -ForegroundColor Cyan
if (Test-Path $OutPath) { Remove-Item $OutPath -Force }
Compress-Archive -Path "$TempDir\*" -DestinationPath $OutPath -CompressionLevel Optimal -Force

# Чистим temp
Remove-Item -Recurse -Force $TempDir

$Size = (Get-Item $OutPath).Length / 1MB
Write-Host ""
Write-Host "══════════════════════════════════════════════════════" -ForegroundColor Green
Write-Host " Готово!" -ForegroundColor Green
Write-Host "══════════════════════════════════════════════════════" -ForegroundColor Green
Write-Host ""
Write-Host "  Архив:  $OutPath"
Write-Host "  Размер: $([math]::Round($Size, 1)) MB"
Write-Host ""
Write-Host "Что делать дальше:" -ForegroundColor Yellow
Write-Host "  1. Скопируй архив на новый компьютер (флешка/облако)"
Write-Host "  2. Распакуй в удобное место"
Write-Host "  3. Двойной клик на START.bat (Windows) или START.sh (Mac/Linux)"
Write-Host ""
Read-Host "Нажми Enter для выхода"
