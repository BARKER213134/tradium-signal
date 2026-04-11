"""Tradium alert bot — reply-клавиатура с постоянно видимыми кнопками."""
import logging

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

from config import BOT_TOKEN, ADMIN_CHAT_ID
from database import SessionLocal, Signal, desc

logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

SOURCE = "tradium"

BTN_WATCHING = "👀 Слежу"
BTN_OPEN = "💠 Открытые"


def _keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_WATCHING), KeyboardButton(text=BTN_OPEN)],
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
    dir_emoji = "🟢" if s.direction in ("LONG", "BUY") else "🔴" if s.direction in ("SHORT", "SELL") else "⚪"
    pair = (s.pair or "—").replace("/USDT", "")
    ai = ""
    if getattr(s, "ai_score", None) is not None:
        em = "🟢" if s.ai_score >= 70 else "🟡" if s.ai_score >= 40 else "🔴"
        ai = f" {em}{s.ai_score}"
    return (
        f"{dir_emoji} <b>{pair}</b> · <code>{s.timeframe or '—'}</code> · "
        f"<code>{s.status or '—'}</code>{ai}\n"
        f"   E <code>{s.entry or '—'}</code> · DCA4 <code>{s.dca4 or '—'}</code> · "
        f"TP <code>{s.tp1 or '—'}</code> · SL <code>{s.sl or '—'}</code>"
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

@dp.message(Command("start", "menu"))
@only_admin
async def cmd_start(message: types.Message):
    await message.answer(
        "🤖 <b>Tradium Signal Bot</b>\n\nКнопки внизу всегда доступны.",
        reply_markup=_keyboard(),
    )


@dp.message(F.text == BTN_WATCHING)
@only_admin
async def msg_watching(message: types.Message):
    text = _list_text(
        "👀 <b>Слежу (ждут DCA #4)</b>",
        lambda q: q.filter(Signal.status == "СЛЕЖУ"),
    )
    await message.answer(text, reply_markup=_keyboard())


@dp.message(F.text == BTN_OPEN)
@only_admin
async def msg_open(message: types.Message):
    text = _list_text(
        "💠 <b>Открытые (DCA4 + Паттерн)</b>",
        lambda q: q.filter(Signal.status.in_(["ОТКРЫТ", "ПАТТЕРН"])),
    )
    await message.answer(text, reply_markup=_keyboard())


async def start_bot():
    import asyncio as _aio
    logger.info("🤖 Tradium Bot запущен!")
    for attempt in range(5):
        try:
            await dp.start_polling(bot)
            break
        except Exception as e:
            if "Conflict" in str(e) and attempt < 4:
                wait = 10 * (attempt + 1)
                logger.warning(f"Bot conflict, retry in {wait}s (attempt {attempt+1}/5)")
                await _aio.sleep(wait)
            else:
                raise
