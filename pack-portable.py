#!/usr/bin/env python3
"""pack-portable.py — собирает проект в portable zip на Рабочем столе.

Включает:
  - весь код + .git/
  - .env, *.session, _session_b64.txt (секреты)
  - START.bat / START.sh / SETUP-NEW-PC.md

Исключает:
  - .venv, __pycache__, .pytest_cache
  - charts/, logs/, data/, tmp/
  - *.pyc, *.log, _mongo_backup.json

Запуск: python pack-portable.py
"""
import os, sys, time, zipfile, fnmatch
from pathlib import Path

EXCLUDE_DIRS = {
    ".venv", "venv", "env",
    "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache",
    "node_modules", "charts", "logs", "data", "tmp",
    ".claude/preview",  # неактуальные снэпшоты
}
EXCLUDE_PATTERNS = [
    "*.pyc", "*.pyo", "*.log", "main_out.log", "_mongo_backup.json",
]


def should_skip_dir(name: str) -> bool:
    return name in EXCLUDE_DIRS


def should_skip_file(name: str) -> bool:
    return any(fnmatch.fnmatch(name, p) for p in EXCLUDE_PATTERNS)


def main():
    project = Path(__file__).resolve().parent
    desktop = Path(os.path.expanduser("~")) / "Desktop"
    if not desktop.exists():
        desktop = Path(os.path.expanduser("~"))

    stamp = time.strftime("%Y-%m-%d-%H%M")
    out = desktop / f"tradium-signal-portable-{stamp}.zip"

    print()
    print("=" * 58)
    print(" Packing tradium-signal -> portable zip")
    print("=" * 58)
    print()
    print(f"Project: {project}")
    print(f"Archive: {out}")
    print()

    # sanity check
    if not (project / ".env").exists():
        print("[X] .env NOT found — cannot continue without secrets")
        input("Press Enter to exit")
        return 1

    if not list(project.glob("*.session")):
        print("[!] No *.session file — Telegram re-auth will be required on new PC")

    files = []
    total_size = 0
    for root, dirs, filenames in os.walk(project):
        # фильтруем папки in-place (os.walk уважает)
        dirs[:] = [d for d in dirs if not should_skip_dir(d)]
        for fn in filenames:
            if should_skip_file(fn):
                continue
            src = Path(root) / fn
            try:
                sz = src.stat().st_size
            except OSError:
                continue
            arcname = src.relative_to(project).as_posix()
            files.append((src, arcname, sz))
            total_size += sz

    print(f"Files to pack: {len(files)}  (~{total_size/1024/1024:.1f} MB uncompressed)")
    print("Compressing...")

    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for i, (src, arcname, _) in enumerate(files):
            if i % 500 == 0 and i > 0:
                print(f"  {i}/{len(files)} files...")
            try:
                zf.write(src, arcname)
            except (PermissionError, OSError) as e:
                print(f"  [skip] {arcname}: {e}")

    out_size = out.stat().st_size / 1024 / 1024
    print()
    print("=" * 58)
    print(" DONE")
    print("=" * 58)
    print()
    print(f"  File:  {out}")
    print(f"  Size:  {out_size:.1f} MB  ({len(files)} files)")
    print()
    print("Next steps:")
    print("  1. Copy this zip to the new PC (USB or cloud)")
    print("  2. Unzip anywhere on new PC")
    print("  3. Double-click START.bat (Windows) or run ./START.sh (Mac/Linux)")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
