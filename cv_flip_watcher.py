"""CV + SuperTrend 30m Flip watcher — observation-only.

Для каждого CV сигнала (source=cryptovizor, pattern_triggered=True):
  1. Сразу создаётся дубль в cv_flip_signals со state=WAITING
     (виден в journal сразу).
  2. Periodic scan (30 сек) — по каждому WAITING-дублю:
     - Если прошло >TIMEOUT_H часов от cv_triggered_at без flip → state=TIMEOUT.
     - Если пришёл более новый CV на ту же пару → state=INVALIDATED.
     - Если на 30m свечах после cv_triggered_at был ≥MIN_BARS_UNDER_ST
       закрытых баров с ST-трендом против CV, и затем ST flip в сторону CV
       → state=FLIPPED, записываем flip_at, flip_price, шлём Telegram-алерт.

paper_trader НЕ получает эти сигналы — observation канал.
Запускается из admin lifespan (не в DEV_MODE), рядом с supertrend_tracker.
"""
from __future__ import annotations

import asyncio
import datetime as _dt
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

SCAN_INTERVAL_SEC = 30
TIMEOUT_H = 24
ST_TF = "30m"
ST_PERIOD = 7
ST_MULT = 2.5
MIN_BARS_UNDER_ST = 1
CANDLES_LIMIT = 300  # ~6.25 дня 30m — с запасом для timeout=24ч


def _to_utc_ts(d: datetime) -> float:
    """Mongo хранит naive UTC datetime. Возвращает epoch seconds."""
    if d.tzinfo is None:
        return d.replace(tzinfo=_dt.timezone.utc).timestamp()
    return d.timestamp()


async def _create_waiting_duplicates():
    """Для каждого CV сигнала за последние TIMEOUT_H часов без дубля — создать WAITING."""
    from database import _signals, _cv_flip_signals, utcnow
    since = utcnow() - timedelta(hours=TIMEOUT_H)
    cv_col = _signals()
    dup_col = _cv_flip_signals()

    created = 0
    for cv in cv_col.find(
        {
            "source": "cryptovizor",
            "pattern_triggered": True,
            "pattern_triggered_at": {"$gte": since},
        },
        {"_id": 1, "pair": 1, "direction": 1, "pattern_name": 1,
         "pattern_triggered_at": 1},
    ):
        sid = str(cv["_id"])
        if dup_col.find_one({"cv_signal_id": sid}, {"_id": 1}):
            continue
        direction = (cv.get("direction") or "").upper()
        if direction not in ("LONG", "SHORT"):
            continue
        pair = cv.get("pair") or ""
        if not pair:
            continue
        now = utcnow()
        doc = {
            "cv_signal_id": sid,
            "pair": pair,
            "direction": direction,
            "cv_triggered_at": cv.get("pattern_triggered_at"),
            "cv_pattern_name": cv.get("pattern_name") or "",
            "state": "WAITING",
            "flip_at": None,
            "flip_price": None,
            "bars_under_st": 0,
            "st_tf": ST_TF,
            "st_params": {"period": ST_PERIOD, "mult": ST_MULT},
            "timeout_h": TIMEOUT_H,
            "created_at": now,
            "updated_at": now,
            "source": "cv_flip",  # для унификации с journal
        }
        try:
            dup_col.insert_one(doc)
            created += 1
        except Exception as e:
            logger.debug(f"[cv-flip] skip insert {pair}: {e}")
    if created:
        logger.info(f"[cv-flip] created {created} WAITING duplicates")


async def _check_doc(doc, closed_candles, st_series, now_dt, dup_col):
    """Проверить один WAITING dup: timeout / invalidated / flipped."""
    from database import utcnow, _signals

    cv_at = doc.get("cv_triggered_at")
    if not isinstance(cv_at, datetime):
        return
    pair = doc.get("pair") or ""
    direction = doc.get("direction") or ""
    want_trend = 1 if direction == "LONG" else -1

    # 1) Timeout
    age_h = (now_dt - cv_at).total_seconds() / 3600.0
    if age_h > TIMEOUT_H:
        dup_col.update_one(
            {"_id": doc["_id"]},
            {"$set": {"state": "TIMEOUT", "updated_at": utcnow()}},
        )
        logger.info(f"[cv-flip] TIMEOUT {pair} {direction} ({doc.get('cv_pattern_name')})")
        return

    # 2) Invalidation — есть более новый CV на ту же пару
    newer = _signals().find_one(
        {
            "source": "cryptovizor",
            "pattern_triggered": True,
            "pair": pair,
            "pattern_triggered_at": {"$gt": cv_at},
        },
        {"_id": 1},
    )
    if newer:
        dup_col.update_one(
            {"_id": doc["_id"]},
            {"$set": {"state": "INVALIDATED", "updated_at": utcnow()}},
        )
        logger.info(f"[cv-flip] INVALIDATED {pair} {direction} (newer CV)")
        return

    # 3) Flip detection
    cv_ms = int(_to_utc_ts(cv_at) * 1000)
    # Первый закрытый 30m бар, открывшийся ПОСЛЕ cv_triggered_at
    first_idx = None
    for i, c in enumerate(closed_candles):
        if c["t"] >= cv_ms:
            first_idx = i
            break
    if first_idx is None or first_idx >= len(st_series):
        return  # нет закрытых баров после CV

    # Ищем первый flip после first_idx: trend[i]=want и trend[i-1]=-want
    # (именно противоположный, не 0 — trend=0 это seed-период ST).
    # Между first_idx и i должно быть ≥MIN_BARS_UNDER_ST баров с trend=-want.
    opp_trend = -want_trend
    flip_idx = None
    for i in range(max(first_idx + 1, 1), len(st_series)):
        curr = st_series[i].get("trend")
        prev = st_series[i - 1].get("trend")
        if curr == want_trend and prev == opp_trend:
            bars_before = sum(
                1 for j in range(first_idx, i)
                if st_series[j].get("trend") == opp_trend
            )
            if bars_before >= MIN_BARS_UNDER_ST:
                flip_idx = i
                break

    if flip_idx is None:
        bars_before = sum(
            1 for j in range(first_idx, len(st_series))
            if st_series[j].get("trend") == opp_trend
        )
        if bars_before != doc.get("bars_under_st", 0):
            dup_col.update_one(
                {"_id": doc["_id"]},
                {"$set": {"bars_under_st": bars_before, "updated_at": utcnow()}},
            )
        return

    # FLIPPED
    flip_bar = closed_candles[flip_idx]
    flip_price = float(flip_bar["c"])
    # Binance t = время открытия 30m бара (мс); close-time = +30 мин
    flip_at = datetime.utcfromtimestamp((int(flip_bar["t"]) + 30 * 60 * 1000) / 1000.0)
    dup_col.update_one(
        {"_id": doc["_id"]},
        {"$set": {
            "state": "FLIPPED",
            "flip_at": flip_at,
            "flip_price": flip_price,
            "updated_at": utcnow(),
        }},
    )
    logger.info(f"[cv-flip] FLIPPED {pair} {direction} @ {flip_price} "
                f"(cv {doc.get('cv_pattern_name')})")

    asyncio.create_task(_send_telegram(
        pair, direction, flip_price, flip_at, doc.get("cv_pattern_name", "")
    ))


async def _scan_waiting():
    """Проход по всем WAITING: проверить timeout / invalidated / flip."""
    from database import _cv_flip_signals, utcnow
    from exchange import get_klines_any
    from backtest_supertrend import compute_st_series

    now = utcnow()
    dup_col = _cv_flip_signals()
    waiting = list(dup_col.find({"state": "WAITING"}))
    if not waiting:
        return
    logger.debug(f"[cv-flip] scan: {len(waiting)} WAITING")

    # Группируем по паре чтобы не дёргать klines дважды
    by_pair: dict[str, list] = {}
    for d in waiting:
        by_pair.setdefault(d.get("pair", ""), []).append(d)

    for pair, docs in by_pair.items():
        if not pair:
            continue
        try:
            candles = await asyncio.to_thread(get_klines_any, pair, ST_TF, CANDLES_LIMIT)
        except Exception as e:
            logger.debug(f"[cv-flip] klines fail {pair}: {e}")
            continue
        if not candles or len(candles) < ST_PERIOD + 5:
            continue
        # Последний бар может быть ещё не закрыт — отбрасываем для честности
        closed = candles[:-1]
        try:
            st_series = compute_st_series(closed, ST_PERIOD, ST_MULT)
        except Exception as e:
            logger.warning(f"[cv-flip] ST compute fail {pair}: {e}")
            continue
        for doc in docs:
            try:
                await _check_doc(doc, closed, st_series, now, dup_col)
            except Exception as e:
                logger.warning(f"[cv-flip] check fail {pair} {doc.get('_id')}: {e}")


async def _send_telegram(pair: str, direction: str, price: float,
                         flip_at: datetime, pattern: str):
    """Telegram alert в BOT12_BOT_TOKEN — graceful skip если не настроен.
    Chat_id по умолчанию = ADMIN_CHAT_ID (см. config.CV_FLIP_CHAT_ID)."""
    from config import BOT12_BOT_TOKEN, CV_FLIP_CHAT_ID
    if not BOT12_BOT_TOKEN or not CV_FLIP_CHAT_ID:
        return
    try:
        import httpx
        dir_e = "🟢" if direction == "LONG" else "🔴"
        text = (
            f"💥 <b>CV+ST Flip confirmed</b>\n"
            f"{dir_e} <b>{pair}</b> {direction}\n"
            f"Pattern: <code>{pattern or '—'}</code>\n"
            f"Flip @ <b>{price}</b>\n"
            f"At: <code>{flip_at.strftime('%Y-%m-%d %H:%M UTC')}</code>"
        )
        url = f"https://api.telegram.org/bot{BOT12_BOT_TOKEN}/sendMessage"
        async with httpx.AsyncClient(timeout=10) as c:
            await c.post(
                url,
                json={
                    "chat_id": CV_FLIP_CHAT_ID,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
            )
    except Exception as e:
        logger.warning(f"[cv-flip] telegram send failed: {e}")


async def start_cv_flip_watcher():
    """Main loop. Запускается из admin lifespan (только в production,
    не в DEV_MODE). Safe restart: хранит всё в Mongo, stateless."""
    logger.info(f"[cv-flip] watcher started (scan={SCAN_INTERVAL_SEC}s, "
                f"timeout={TIMEOUT_H}h, ST={ST_TF} {ST_PERIOD}/{ST_MULT})")
    while True:
        try:
            await _create_waiting_duplicates()
            await _scan_waiting()
        except Exception as e:
            logger.exception(f"[cv-flip] loop error: {e}")
        await asyncio.sleep(SCAN_INTERVAL_SEC)
