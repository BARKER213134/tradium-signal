"""[DEPRECATED] Legacy wrapper для совместимости.

Используй exchange_symbols вместо этого модуля. Оставлено только чтобы
старые import'ы не сломались — все вызовы делегируются в новый модуль
с exchange='binance'.
"""
from __future__ import annotations
import logging
from typing import Set

logger = logging.getLogger(__name__)


def get_supported_symbols() -> Set[str]:
    """[DEPRECATED] Use exchange_symbols.get_supported_symbols(exchange='binance')"""
    from exchange_symbols import get_supported_symbols as _get
    return _get(exchange="binance")


def refresh_supported_symbols() -> dict:
    """[DEPRECATED] Use exchange_symbols.refresh_supported_symbols(exchange='binance')"""
    from exchange_symbols import refresh_supported_symbols as _ref
    return _ref(exchange="binance")


def is_symbol_supported(symbol: str) -> bool:
    """[DEPRECATED] Use exchange_symbols.is_symbol_supported(symbol, exchange='binance')

    ВАЖНО: эта функция теперь использует DEFAULT биржу (BingX), а НЕ Binance.
    Для явного фильтра по Binance используй exchange_symbols.is_symbol_supported(sym, 'binance').
    """
    from exchange_symbols import is_symbol_supported as _is
    return _is(symbol)  # default exchange


def get_meta() -> dict:
    """[DEPRECATED]"""
    from exchange_symbols import get_meta
    return get_meta()
