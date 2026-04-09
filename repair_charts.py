"""Подтягивает графики для сигналов где has_chart=False, через поиск
соседних сообщений в группе по message_id."""
import asyncio
import logging
import os
from pathlib import Path

from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument

from config import API_ID, API_HASH, SOURCE_GROUP_ID, CHARTS_DIR
from database import SessionLocal, Signal, log_event
from ai_analyzer import analyze_chart, analyze_signal_quality

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("repair")


def _f(v):
    try: return float(v) if v is not None else None
    except Exception: return None


async def main():
    Path(CHARTS_DIR).mkdir(parents=True, exist_ok=True)

    session = os.path.join(os.path.dirname(os.path.abspath(__file__)), "session_userbot")
    client = TelegramClient(session, API_ID, API_HASH)
    await client.start()

    db = SessionLocal()
    try:
        targets = (
            db.query(Signal)
            .filter(Signal.has_chart == False)
            .filter(Signal.message_id != None)
            .all()
        )
        logger.info(f"Сигналов без графика: {len(targets)}")
        for s in targets:
            logger.info(f"#{s.id} {s.pair} msg_id={s.message_id}")
            # Получаем 5 сообщений после текста, ищем фото
            found = None
            async for m in client.iter_messages(SOURCE_GROUP_ID, min_id=s.message_id, limit=5, reverse=True):
                if m.id == s.message_id:
                    continue
                is_photo = isinstance(m.media, MessageMediaPhoto)
                is_doc = (
                    isinstance(m.media, MessageMediaDocument)
                    and m.media.document.mime_type.startswith("image/")
                ) if m.media else False
                if is_photo or is_doc:
                    found = m
                    break
            if not found:
                logger.warning(f"  фото не найдено")
                continue

            chart_path = os.path.join(CHARTS_DIR, f"{s.id}_{found.id}.jpg")
            try:
                await client.download_media(found, file=chart_path)
            except Exception as e:
                logger.error(f"  download fail: {e}")
                continue

            s.has_chart = True
            s.chart_message_id = found.id
            s.chart_path = chart_path
            db.commit()
            logger.info(f"  ✓ chart {chart_path}")

            # AI анализ + score
            try:
                chart_data = await analyze_chart(chart_path)
                s.chart_analyzed = True
                s.chart_ai_raw = chart_data.get("_raw", "")
                s.chart_pair = chart_data.get("pair")
                s.chart_direction = chart_data.get("direction")
                s.chart_entry = _f(chart_data.get("entry"))
                s.chart_sl = _f(chart_data.get("sl"))
                s.chart_tp1 = _f(chart_data.get("tp1"))
                s.chart_notes = chart_data.get("notes") or chart_data.get("pattern", "")
                s.dca1 = _f(chart_data.get("dca1"))
                s.dca2 = _f(chart_data.get("dca2"))
                s.dca3 = _f(chart_data.get("dca3"))
                s.dca4 = _f(chart_data.get("dca4"))
                if s.is_filtered:
                    s.is_filtered = False
                    s.filter_reason = None
                db.commit()
                logger.info(f"  ✓ AI dca4={s.dca4}")
                log_event(s.id, "chart_recovered", data={"dca4": s.dca4})

                # AI quality score
                quality = await analyze_signal_quality(chart_path, {
                    "pair": s.pair, "direction": s.direction, "timeframe": s.timeframe,
                    "entry": s.entry, "sl": s.sl, "tp1": s.tp1, "dca4": s.dca4,
                    "risk_reward": s.risk_reward, "trend": s.trend,
                })
                if quality and "score" in quality:
                    s.ai_score = quality["score"]
                    s.ai_confidence = quality.get("confidence")
                    s.ai_reasoning = quality.get("reasoning")
                    s.ai_risks = quality.get("risks") or []
                    s.ai_verdict = quality.get("verdict")
                    db.commit()
                    logger.info(f"  ✓ score={s.ai_score}")
            except Exception as e:
                logger.error(f"  AI fail: {e}")
    finally:
        db.close()
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
