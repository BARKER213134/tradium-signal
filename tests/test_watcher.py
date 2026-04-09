import asyncio
from unittest.mock import AsyncMock, patch

import database
import watcher
from database import Signal, utcnow


def _make(db, **kwargs):
    defaults = dict(
        message_id=kwargs.pop("message_id", 1000),
        pair="BTC/USDT", direction="LONG", entry=60000,
        tp1=62000, sl=59000, dca4=58000,
        timeframe="1h", status="СЛЕЖУ",
        received_at=utcnow(),
    )
    defaults.update(kwargs)
    s = Signal(**defaults)
    db.add(s)
    db.commit()
    db.refresh(s)
    return s


def _reset():
    db = database.SessionLocal()
    try:
        db.query(Signal).delete()
        db.commit()
    finally:
        db.close()


def test_dca4_transition_to_otkryt(fake_bot):
    _reset()
    watcher.setup(fake_bot, admin_chat_id=12345)
    db = database.SessionLocal()
    try:
        s = _make(db, message_id=2001, status="СЛЕЖУ", dca4=58000, sl=50000)
        sid = s.id
    finally:
        db.close()

    async def fake_prices(pairs):
        return {"BTCUSDT": 57900.0}  # below dca4 → reached for LONG

    async def fake_klines(pair, tf, limit=50):
        return []  # не триггерить _check_patterns

    with patch.object(watcher, "get_prices", side_effect=fake_prices), \
         patch.object(watcher, "get_klines", side_effect=fake_klines):
        asyncio.run(watcher._check_once())

    db = database.SessionLocal()
    try:
        s = db.query(Signal).filter(Signal.id == sid).first()
        assert s.status == "ОТКРЫТ"
        assert s.dca4_triggered is True
    finally:
        db.close()
    # Alert was sent
    assert fake_bot.messages or fake_bot.photos


def test_pattern_transition(fake_bot):
    _reset()
    watcher.setup(fake_bot, admin_chat_id=12345)
    db = database.SessionLocal()
    try:
        s = _make(db, message_id=2002, status="ОТКРЫТ", dca4_triggered=True)
        sid = s.id
    finally:
        db.close()

    # Bullish engulfing candle sequence
    bull_engulf = [
        {"o": 10.0, "h": 10.1, "l": 9.0, "c": 9.2},
        {"o": 10.0, "h": 10.1, "l": 9.5, "c": 9.6},  # bearish
        {"o": 9.5, "h": 10.6, "l": 9.4, "c": 10.5},   # bullish engulfing
    ]

    async def fake_klines(pair, tf, limit=50):
        return bull_engulf

    async def fake_prices(pairs):
        return {}  # so _check_tp_sl does nothing

    with patch.object(watcher, "get_klines", side_effect=fake_klines), \
         patch.object(watcher, "get_prices", side_effect=fake_prices):
        asyncio.run(watcher._check_once())

    db = database.SessionLocal()
    try:
        s = db.query(Signal).filter(Signal.id == sid).first()
        assert s.status == "ПАТТЕРН"
        assert s.pattern_name is not None
    finally:
        db.close()


def test_tp_hit_long(fake_bot):
    _reset()
    watcher.setup(fake_bot, admin_chat_id=12345)
    db = database.SessionLocal()
    try:
        s = _make(db, message_id=2003, status="ПАТТЕРН",
                  direction="LONG", entry=100.0, tp1=110.0, sl=95.0,
                  dca4_triggered=True, pattern_triggered=True)
        sid = s.id
    finally:
        db.close()

    async def fake_prices(pairs):
        return {"BTCUSDT": 111.0}

    with patch.object(watcher, "get_prices", side_effect=fake_prices):
        asyncio.run(watcher._check_once())

    db = database.SessionLocal()
    try:
        s = db.query(Signal).filter(Signal.id == sid).first()
        assert s.status == "TP"
        assert s.result == "TP"
        assert s.exit_price == 110.0
        assert abs(s.pnl_percent - 10.0) < 0.001
    finally:
        db.close()


def test_sl_hit_short(fake_bot):
    _reset()
    watcher.setup(fake_bot, admin_chat_id=12345)
    db = database.SessionLocal()
    try:
        s = _make(db, message_id=2004, status="ОТКРЫТ",
                  direction="SHORT", entry=100.0, tp1=90.0, sl=105.0,
                  dca4_triggered=True, pattern_triggered=True)
        sid = s.id
    finally:
        db.close()

    async def fake_prices(pairs):
        return {"BTCUSDT": 106.0}

    # Also mock klines so _check_patterns does nothing
    async def fake_klines(pair, tf, limit=50):
        return []

    with patch.object(watcher, "get_prices", side_effect=fake_prices), \
         patch.object(watcher, "get_klines", side_effect=fake_klines):
        asyncio.run(watcher._check_once())

    db = database.SessionLocal()
    try:
        s = db.query(Signal).filter(Signal.id == sid).first()
        assert s.status == "SL"
        assert s.result == "SL"
        # SHORT entering 100 exiting 105 → -5%
        assert abs(s.pnl_percent + 5.0) < 0.001
    finally:
        db.close()
