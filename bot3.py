"""Volume Alert bot — reply-клавиатура."""
import logging

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

from config import BOT3_BOT_TOKEN, ADMIN_CHAT_ID, BOT2_NAME
from database import SessionLocal, Signal, desc

logger = logging.getLogger(__name__)

bot3 = Bot(token=BOT3_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML)) if BOT3_BOT_TOKEN else None
dp3 = Dispatcher()

SOURCE = BOT2_NAME or "cryptovizor"

BTN_WATCHING = "👀 Следим"
BTN_VOLUME = "🔥 Volume Alert"


def _keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_WATCHING), KeyboardButton(text=BTN_VOLUME)]],
        resize_keyboard=True,
        is_persistent=True,
    )


def only_admin(func):
    async def wrapper(message: types.Message, **_kwargs):
        if message.from_user.id != ADMIN_CHAT_ID:
            return
        return await func(message)
    return wrapper


def _fmt(s: Signal) -> str:
    dir_emoji = "🟢" if s.direction == "LONG" else "🔴"
    pair = (s.pair or "—").replace("/USDT", "")
    ai = ""
    if getattr(s, "ai_score", None) is not None:
        em = "🟢" if s.ai_score >= 70 else "🟡" if s.ai_score >= 40 else "🔴"
        ai = f" {em}{s.ai_score}"
    vol_info = ""
    if getattr(s, "dca3", None) is not None:  # dca3 используем для RVOL
        vol_info = f" · ×{s.dca3}"
    return (
        f"{dir_emoji} <b>{pair}</b> · 1h · <code>{s.status}</code>{ai}{vol_info}\n"
        f"   entry <code>{s.entry or '—'}</code>"
    )


def _list_text(title, filter_fn):
    db = SessionLocal()
    try:
        q = db.query(Signal).filter(Signal.source == SOURCE)
        if filter_fn:
            q = filter_fn(q)
        signals = q.order_by(desc(Signal.received_at)).limit(10).all()
        if not signals:
            return f"{title}\n\n<i>Нет сигналов</i>"
        return f"{title}\n\n" + "\n\n".join(_fmt(s) for s in signals)
    finally:
        db.close()


@dp3.message(Command("start", "menu"))
@only_admin
async def cmd_start(message: types.Message):
    await message.answer(
        "🔥 <b>Volume Alert Bot</b>\n\nКнопки внизу.",
        reply_markup=_keyboard(),
    )


@dp3.message(F.text == BTN_WATCHING)
@only_admin
async def msg_watching(message: types.Message):
    text = _list_text("👀 <b>Следим</b>", lambda q: q.filter(Signal.status == "СЛЕЖУ"))
    await message.answer(text, reply_markup=_keyboard())


@dp3.message(F.text == BTN_VOLUME)
@only_admin
async def msg_volume(message: types.Message):
    text = _list_text("🔥 <b>Volume Alert</b>", lambda q: q.filter(Signal.status == "VOLUME"))
    await message.answer(text, reply_markup=_keyboard())


async def start_bot3():
    if not bot3:
        logger.warning("BOT3_BOT_TOKEN не задан — Volume bot не запущен")
        return
    logger.info("🔥 Volume Alert Bot запущен!")
    await dp3.start_polling(bot3)
