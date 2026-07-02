"""userbot.py — DISABLED stub.

Ранее содержал Telethon userbot который читал сигналы из Cryptovizor и
Tradium каналов + BIG BUY DM. Пользователь отказался от этих подписок
(2026-07-01) → весь ingestion path выключен. Файл оставлен только как
placeholder чтобы старый импорт `from userbot import ...` не крашил
main.py и admin.py.

Все функции — no-op. Ничего не подключается к Telegram, session
не загружается.
"""
from __future__ import annotations
import logging

logger = logging.getLogger(__name__)

# Placeholders для старых импортов
_tg_client = None
_bot = None
_admin_chat_id = None
_last_setup_at = None
_last_disconnect_at = None
_last_setup_error = None
_reconnect_count = 0
_cryptovizor_id_resolved = None
_cryptovizor_resolve_method = None
_handlers_registered = {"tradium": False, "cryptovizor": False, "kl": False}


def set_bot(bot, admin_chat_id):
    """No-op. Оставлено для совместимости с main.py."""
    global _bot, _admin_chat_id
    _bot = bot
    _admin_chat_id = admin_chat_id


async def start_userbot():
    """No-op. Telethon userbot отключён — user не пользуется CV/Tradium
    подпиской. Возвращаем немедленно без подключения."""
    logger.info("[userbot] DISABLED — CV/Tradium ingestion удалён (2026-07-01)")
    return None


async def setup_userbot():
    """No-op — совместимость."""
    return None


async def handle_cryptovizor_message(text: str, message_id: int):
    """No-op — Cryptovizor ingestion удалён."""
    return None


async def peek_channel_history(chat_id, limit: int = 5):
    """No-op — Telethon клиент отсутствует."""
    return {"ok": False, "error": "userbot disabled (CV/Tradium ingestion removed)"}


def get_status_details() -> dict:
    """Возвращает статус для /api/userbot-status. Всегда 'disabled'."""
    return {
        "is_connected": False,
        "disabled": True,
        "reason": "CV/Tradium ingestion removed 2026-07-01",
        "last_setup_at": None,
        "last_disconnect_at": None,
        "last_setup_error": None,
        "reconnect_count": 0,
        "cryptovizor_id_resolved": None,
        "cryptovizor_resolve_method": None,
        "cryptovizor_env_set": False,
        "handlers_registered": _handlers_registered,
    }
