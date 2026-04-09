import asyncio
import os
from database import SessionLocal, Signal
from ai_analyzer import analyze_signal_quality


async def main():
    db = SessionLocal()
    s = db.query(Signal).filter(Signal.pair == "UNI/USDT").first()
    if not s:
        print("UNI not found")
        return
    base = os.path.dirname(os.path.abspath(__file__))
    path = os.path.normpath(os.path.join(base, s.chart_path.lstrip("./\\")))
    print("chart:", path, "exists:", os.path.exists(path))
    q = await analyze_signal_quality(path, {
        "pair": s.pair, "direction": s.direction, "timeframe": s.timeframe,
        "entry": s.entry, "sl": s.sl, "tp1": s.tp1, "dca4": s.dca4,
        "risk_reward": s.risk_reward, "trend": s.trend,
    })
    print("score:", q.get("score"), "verdict:", q.get("verdict"))
    if q and "score" in q:
        s.ai_score = q["score"]
        s.ai_confidence = q.get("confidence")
        s.ai_reasoning = q.get("reasoning")
        s.ai_risks = q.get("risks") or []
        s.ai_verdict = q.get("verdict")
        db.commit()
        print("saved")
    db.close()


if __name__ == "__main__":
    asyncio.run(main())
