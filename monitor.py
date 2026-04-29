"""External uptime monitor for Tradium Signal production.

Checks critical public endpoints and sends a Telegram alert if anything
is broken. Designed to run from GitHub Actions every 30 minutes (or any
external scheduler) — completely outside the app, so it catches hangs
where the app itself can't self-report.

Required environment variables:
  PROD_URL          — https://tradium-signal-production.up.railway.app
  BOT2_BOT_TOKEN    — Telegram bot token (отдельный от userbot)
  ADMIN_CHAT_ID     — chat id куда слать алерты (число)

Optional:
  MONITOR_QUIET_OK  — '1' чтобы не слать "OK" пинги (только алерты)
"""
from __future__ import annotations
import json
import os
import sys
import time
import urllib.error
import urllib.request

# Гарантируем UTF-8 stdout (Windows cp1251 ломается на эмодзи)
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

PROD_URL = os.getenv("PROD_URL", "").rstrip("/")
BOT_TOKEN = os.getenv("BOT2_BOT_TOKEN", "")
CHAT_ID = os.getenv("ADMIN_CHAT_ID", "")
QUIET_OK = os.getenv("MONITOR_QUIET_OK", "1") == "1"

TIMEOUT_SEC = 25.0
ENDPOINTS = [
    # name, path, expected_keys (validation), max_time_warn, hard_fail_status
    ("healthz",     "/healthz",                 {"ok"},                10.0, True),
    ("health",      "/health",                  {"checks"},            15.0, False),
    ("live_snap",   "/api/live/snapshot",       {"ok", "open_count"},  20.0, True),
    ("paper_rej",   "/api/paper/rejections?limit=5", {"ok"},           20.0, False),
    ("live_rej",    "/api/live/rejections-all?limit=5", {"ok"},        20.0, False),
]


def _fetch(path: str) -> tuple[int, float, dict | str]:
    """Возвращает (http_code, elapsed_seconds, body_or_err)."""
    url = PROD_URL + path
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "tradium-monitor/1.0"})
        with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as r:
            raw = r.read()
            elapsed = time.time() - t0
            try:
                return (r.status, elapsed, json.loads(raw))
            except Exception:
                return (r.status, elapsed, raw.decode("utf-8", errors="replace")[:200])
    except urllib.error.HTTPError as e:
        return (e.code, time.time() - t0, f"HTTPError: {e.reason}")
    except urllib.error.URLError as e:
        return (0, time.time() - t0, f"URLError: {e.reason}")
    except Exception as e:
        return (0, time.time() - t0, f"Exception: {type(e).__name__}: {e}")


def _send_telegram(text: str) -> bool:
    if not (BOT_TOKEN and CHAT_ID):
        print(f"[monitor] cannot send Telegram — BOT_TOKEN/CHAT_ID missing")
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": int(CHAT_ID), "text": text,
        "disable_web_page_preview": True,
    }).encode("utf-8")
    req = urllib.request.Request(url, data=payload,
                                  headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status == 200
    except Exception as e:
        print(f"[monitor] Telegram send fail: {e}")
        return False


def main() -> int:
    if not PROD_URL:
        print("[monitor] PROD_URL not set", file=sys.stderr)
        return 2

    failures: list[str] = []
    warnings: list[str] = []
    rows: list[str] = []

    for name, path, must_keys, max_t, hard_fail in ENDPOINTS:
        code, elapsed, body = _fetch(path)
        line = f"{name:<10s} {path:<40s} HTTP {code:>3d} {elapsed:>5.1f}s"
        ok_status = (code == 200)
        ok_keys = isinstance(body, dict) and must_keys.issubset(body.keys())
        ok_speed = elapsed <= max_t

        if not ok_status:
            line += "  ❌ status"
            (failures if hard_fail else warnings).append(f"{name}: HTTP {code}")
        elif not ok_keys:
            preview = (str(body)[:120] + "...") if isinstance(body, str) else f"keys={list(body.keys())[:6]}"
            line += f"  ❌ keys ({preview})"
            (failures if hard_fail else warnings).append(f"{name}: bad response shape")
        elif not ok_speed:
            line += "  ⚠ slow"
            warnings.append(f"{name}: slow {elapsed:.1f}s")
        else:
            line += "  ✅"
        rows.append(line)

    report = "\n".join(rows)
    print(report)

    if failures:
        msg = (f"🚨 Tradium prod monitor — FAIL\n\n"
               f"URL: {PROD_URL}\n"
               f"Failed: {', '.join(failures)}\n"
               f"Warnings: {', '.join(warnings) if warnings else '—'}\n\n"
               f"Detail:\n{report}")
        _send_telegram(msg[:4000])
        return 1
    if warnings:
        msg = (f"⚠ Tradium prod monitor — WARN\n\n"
               f"URL: {PROD_URL}\n"
               f"Warnings: {', '.join(warnings)}\n\n"
               f"Detail:\n{report}")
        _send_telegram(msg[:4000])
        return 0
    if not QUIET_OK:
        _send_telegram(f"✅ Tradium prod OK\n\n{report}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
