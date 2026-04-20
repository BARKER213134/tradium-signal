"""Singleton Anthropic клиент — переиспользуется из всех модулей.

Раньше `anthropic.Anthropic(api_key=...)` создавался на каждый AI-вызов
(11 мест: admin.py×3, paper_trader.py×4, watcher.py×2, ai_analyzer.py,
ai_signal_filter.py). Каждое создание = инициализация HTTP-клиента,
оверхед ~100-200мс + лишние TLS handshakes при параллельных вызовах.

Теперь: один общий клиент, внутренний HTTP pool переиспользуется.

Usage:
    from ai_client import get_ai_client
    client = get_ai_client()
    client.messages.create(...)
"""
from __future__ import annotations
import logging
import threading

logger = logging.getLogger(__name__)

_client = None
_lock = threading.Lock()


def get_ai_client():
    """Возвращает shared anthropic.Anthropic instance (lazy-init, thread-safe)."""
    global _client
    if _client is not None:
        return _client
    with _lock:
        if _client is not None:
            return _client
        import anthropic
        from config import ANTHROPIC_API_KEY
        _client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        logger.info("[ai_client] Anthropic singleton initialized")
        return _client
