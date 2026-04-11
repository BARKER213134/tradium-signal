"""Cryptovizor alert bot — reply-клавиатура с постоянно видимыми кнопками."""
import logging

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

from config import BOT2_BOT_TOKEN, ADMIN_CHAT_ID, BOT2_NAME
from database import SessionLocal, Signal, desc

logger = logging.getLogger(__name__)

bot2 = Bot(token=BOT2_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML)) if BOT2_BOT_TOKEN else None
dp2 = Dispatcher()

SOURCE = BOT2_NAME or "cryptovizor"

BTN_WATCHING = "👀 Следим"
BTN_ACTIVE = "🚀 Сигнал"


def _keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_WATCHING), KeyboardButton(text=BTN_ACTIVE)],
        ],
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Выбери раздел",
    )


def only_admin(func):
    async def wrapper(message: types.Message, **_kwargs):
        if message.from_user.id != ADMIN_CHAT_ID:
            return
        return await func(message)
    return wrapper


def _fmt_short(s: Signal) -> str:
    dir_emoji = "🟢" if s.direction == "LONG" else "🔴" if s.direction == "SHORT" else "⚪"
    pair = (s.pair or "—").replace("/USDT", "")
    ai = ""
    if getattr(s, "ai_score", None) is not None:
        em = "🟢" if s.ai_score >= 70 else "🟡" if s.ai_score >= 40 else "🔴"
        ai = f" {em}{s.ai_score}"
    trend = s.trend or ""
    trend_v = "".join("🟢" if c == "G" else "🔴" for c in trend) if trend else "—"
    pattern = f" · {s.pattern_name}" if s.pattern_name else ""
    return (
        f"{dir_emoji} <b>{pair}</b> · 30m · <code>{s.status}</code>{ai}{pattern}\n"
        f"   {trend_v} · entry <code>{s.entry or '—'}</code>"
    )


def _list_text(title: str, filter_fn):
    db = SessionLocal()
    try:
        q = db.query(Signal).filter(Signal.source == SOURCE)
        if filter_fn:
            q = filter_fn(q)
        signals = q.order_by(desc(Signal.received_at)).limit(10).all()
        if not signals:
            return f"{title}\n\n<i>Нет сигналов</i>"
        return f"{title}\n\n" + "\n\n".join(_fmt_short(s) for s in signals)
    finally:
        db.close()


# ─── Handlers ──────────────────────────────────────────────────────────

@dp2.message(Command("start", "menu"))
@only_admin
async def cmd_start(message: types.Message):
    await message.answer(
        "🤖 <b>Cryptovizor Signal Bot</b>\n\nКнопки внизу всегда доступны.",
        reply_markup=_keyboard(),
    )


@dp2.message(F.text == BTN_WATCHING)
@only_admin
async def msg_watching(message: types.Message):
    text = _list_text(
        "👀 <b>Следим</b>",
        lambda q: q.filter(Signal.status == "СЛЕЖУ"),
    )
    await message.answer(text, reply_markup=_keyboard())


@dp2.message(F.text == BTN_ACTIVE)
@only_admin
async def msg_active(message: types.Message):
    text = _list_text(
        "🚀 <b>Сигнал (паттерн найден)</b>",
        lambda q: q.filter(Signal.status == "ПАТТЕРН"),
    )
    await message.answer(text, reply_markup=_keyboard())


async def start_bot2():
    if not bot2:
        logger.warning("BOT2_BOT_TOKEN не задан — Cryptovizor bot не запущен")
        return
    import asyncio as _aio
    logger.info("🤖 Cryptovizor Bot запущен!")
    for attempt in range(5):
        try:
            await dp2.start_polling(bot2)
            break
        except Exception as e:
            if "Conflict" in str(e) and attempt < 4:
                wait = 10 * (attempt + 1)
                logger.warning(f"Bot2 conflict, retry in {wait}s (attempt {attempt+1}/5)")
                await _aio.sleep(wait)
            else:
                raise
