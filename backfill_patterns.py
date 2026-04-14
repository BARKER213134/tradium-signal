"""Прогон pattern detection задним числом для backfilled CV сигналов.

Берёт все signals со source=cryptovizor, status=АРХИВ (или СЛЕЖУ если задано),
для каждого:
  1) Получает 1h свечи Binance с endTime = received_at + 24h, limit=60
     (так получаем 60 свечей вокруг момента сигнала и несколько часов после)
  2) Берёт свечи где open_time >= received_at и проверяет на паттерны
  3) Если паттерн найден на свече open_time T:
       - status='ПАТТЕРН'
       - pattern_triggered_at = T (момент свечи, не utcnow)
       - pattern_name = first detected pattern
       - pattern_price = close price свечи

Telegram алерты НЕ отправляются (это бэктест задним числом).

Usage:
    python backfill_patterns.py            # все АРХИВ source=cryptovizor
    python backfill_patterns.py --status СЛЕЖУ
    python backfill_patterns.py --limit 50
"""
import argparse
import asyncio
import logging
import sys
import time
from datetime import datetime, timedelta, timezone

import httpx

from database import _signals
from patterns import detect_patterns
from continuation_patterns import detect_continuation

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", force=True)
logger = logging.getLogger("backfill-patterns")
logger.setLevel(logging.INFO)

BINANCE_BASE = "https://data-api.binance.vision"
BINANCE_FUTURES = "https://fapi.binance.com"


def _normalize(pair: str) -> str:
    return (pair or "").replace("/", "").replace("-", "").replace(" ", "").upper()


def get_klines_around(pair: str, received_at: datetime, hours_after: int = 24, limit: int = 60) -> list[dict]:
    """Получает 1h свечи с end_time = received_at + hours_after."""
    symbol = _normalize(pair)
    end_dt = received_at + timedelta(hours=hours_after)
    end_ms = int(end_dt.timestamp() * 1000)
    # Spot
    for base in (BINANCE_BASE, BINANCE_FUTURES):
        try:
            url = f"{base}/api/v3/klines" if base == BINANCE_BASE else f"{base}/fapi/v1/klines"
            r = httpx.get(url, params={"symbol": symbol, "interval": "1h", "limit": limit, "endTime": end_ms}, timeout=10)
            if r.status_code != 200:
                continue
            data = r.json()
            if not data:
                continue
            return [{"t": int(k[0]), "o": float(k[1]), "h": float(k[2]), "l": float(k[3]), "c": float(k[4]), "v": float(k[5])} for k in data]
        except Exception:
            continue
    return []


def detect_pattern_on_candles(candles: list[dict], direction: str, signal_received_ms: int) -> tuple[str | None, dict | None]:
    """Идём по свечам после signal_received_ms, возвращаем (pattern_name, candle) первого совпадения."""
    if len(candles) < 10:
        return None, None
    # Найдём индекс первой свечи после received
    start_idx = None
    for i, c in enumerate(candles):
        if c["t"] >= signal_received_ms:
            start_idx = i
            break
    if start_idx is None:
        return None, None
    # Идём окнами, симулируя что watcher проверял после каждой новой свечи
    for end in range(start_idx + 5, len(candles) + 1):
        window = candles[:end]
        if len(window) < 10:
            continue
        reversal = detect_patterns(window, direction)
        cont = detect_continuation(window, direction)
        detected = reversal + cont
        if detected:
            return detected[0], candles[end - 1]
    return None, None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--status", default="АРХИВ", help="статус для обработки (АРХИВ/СЛЕЖУ)")
    ap.add_argument("--limit", type=int, default=200, help="макс сигналов за запуск")
    ap.add_argument("--source", default="cryptovizor")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    # Берём backfilled: status=АРХИВ + pattern_triggered=True + НЕТ pattern_name (только наши backfill)
    query = {
        "source": args.source,
        "status": args.status,
        "pattern_triggered": True,
        "$or": [{"pattern_name": None}, {"pattern_name": ""}, {"pattern_name": {"$exists": False}}],
        "pair": {"$ne": None},
        "direction": {"$ne": None},
    }

    docs = list(_signals().find(query).sort("received_at", -1).limit(args.limit))
    logger.info(f"Найдено {len(docs)} backfilled сигналов без pattern_name (status={args.status})")

    found = 0
    skipped = 0
    no_data = 0
    no_pattern = 0
    for i, s in enumerate(docs):
        pair = s.get("pair", "")
        direction = s.get("direction", "")
        received_at = s.get("received_at")
        if not received_at:
            skipped += 1
            continue
        candles = get_klines_around(pair, received_at, hours_after=24, limit=60)
        if not candles:
            no_data += 1
            logger.debug(f"[{i+1}/{len(docs)}] {pair}: нет свечей")
            continue
        signal_ms = int(received_at.timestamp() * 1000)
        pattern, candle = detect_pattern_on_candles(candles, direction, signal_ms)
        if not pattern:
            no_pattern += 1
            logger.debug(f"[{i+1}/{len(docs)}] {pair} {direction}: паттерн не найден")
            continue
        # Update DB
        pt_dt = datetime.fromtimestamp(candle["t"] / 1000, tz=timezone.utc).replace(tzinfo=None)
        update = {
            "status": "ПАТТЕРН",
            "pattern_triggered_at": pt_dt,
            "pattern_name": pattern,
            "pattern_price": candle["c"],
        }
        if not args.dry_run:
            _signals().update_one({"_id": s["_id"]}, {"$set": update})
        found += 1
        logger.info(f"[{i+1}/{len(docs)}] ✅ {pair} {direction} → {pattern} @ {pt_dt} price={candle['c']}")
        # Rate limit Binance
        time.sleep(0.05)

    logger.info(f"\n=== ИТОГ ===")
    logger.info(f"Найдено паттернов: {found}")
    logger.info(f"Без паттерна: {no_pattern}")
    logger.info(f"Без свечей: {no_data}")
    logger.info(f"Skipped: {skipped}")
    if args.dry_run:
        logger.info("(DRY-RUN: ничего не сохранено)")


if __name__ == "__main__":
    main()
