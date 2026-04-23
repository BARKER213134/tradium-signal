# pack-portable.ps1 — packs project (with secrets) into a portable zip
# Usage: right-click -> Run with PowerShell
#    or: powershell -ExecutionPolicy Bypass -File pack-portable.ps1

$ErrorActionPreference = "Stop"
$ProjectDir = $PSScriptRoot
$Stamp = Get-Date -Format "yyyy-MM-dd-HHmm"
$OutName = "tradium-signal-portable-$Stamp.zip"
$OutPath = Join-Path ([Environment]::GetFolderPath("Desktop")) $OutName

Write-Host ""
Write-Host "===================================================="
Write-Host " Packing tradium-signal -> portable zip"
Write-Host "===================================================="
Write-Host ""

# Required files check
$missing = @()
foreach ($f in @(".env")) {
    if (-not (Test-Path (Join-Path $ProjectDir $f))) {
        $missing += $f
    }
}
$sessions = Get-ChildItem -Path $ProjectDir -Filter "*.session" -ErrorAction SilentlyContinue
if ($sessions.Count -eq 0) {
    Write-Host "[!] No *.session file found - Telegram re-auth may be needed on new PC"
}
if ($missing.Count -gt 0) {
    Write-Host "[X] Missing critical files: $($missing -join ', ')"
    Write-Host "Run this script from the project root."
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host "Project: $ProjectDir"
Write-Host "Archive: $OutPath"
Write-Host ""

# Temp staging dir
$TempDir = Join-Path $env:TEMP "tradium-portable-$Stamp"
if (Test-Path $TempDir) { Remove-Item -Recurse -Force $TempDir }
New-Item -ItemType Directory -Force -Path $TempDir | Out-Null

Write-Host "Copying files (excluding junk)..."
$roboArgs = @(
    "$ProjectDir",
    "$TempDir",
    "/E",
    "/XD", ".venv", "venv", "env", "__pycache__",
           ".pytest_cache", ".mypy_cache", ".ruff_cache", "node_modules",
           "charts", "logs", "data", "tmp",
    "/XF", "*.pyc", "*.pyo", "*.log", "_mongo_backup.json", "main_out.log",
    "/NFL", "/NDL", "/NJH", "/NJS", "/NC", "/NS"
)
& robocopy @roboArgs | Out-Null
if ($LASTEXITCODE -ge 8) {
    Write-Host "[X] robocopy failed (exit $LASTEXITCODE)"
    exit 1
}

Write-Host "Zipping..."
if (Test-Path $OutPath) { Remove-Item $OutPath -Force }
Compress-Archive -Path "$TempDir\*" -DestinationPath $OutPath -CompressionLevel Optimal -Force

Remove-Item -Recurse -Force $TempDir

$Size = (Get-Item $OutPath).Length / 1MB
Write-Host ""
Write-Host "===================================================="
Write-Host " DONE"
Write-Host "===================================================="
Write-Host ""
Write-Host "  File:  $OutPath"
Write-Host "  Size:  $([math]::Round($Size, 1)) MB"
Write-Host ""
Write-Host "Next steps:"
Write-Host "  1. Copy this zip to the new PC (USB or cloud)"
Write-Host "  2. Unzip anywhere"
Write-Host "  3. Double-click START.bat (Win) or run ./START.sh (Mac/Linux)"
Write-Host ""
