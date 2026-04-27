"""Multi-exchange symbol registry — единый источник истины какие пары торгуются.

Поддерживает: Binance Futures, BingX Perpetual Futures.
Используется для:
  • Paper фильтр: paper.on_signal пропускает только сигналы на торгуемых парах
  • Live whitelist: запрос только на пары которые есть на нужной бирже
  • UI: показать сколько пар поддерживается, обновлено когда

Default exchange = BingX (так как Binance Futures ограничен в РФ/СНГ).
Можно переопределить через MongoDB doc {"_id": "default_exchange"}.
"""
from __future__ import annotations
import logging
import time
from typing import Set
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# ════════════════════════════════════════════════════════════════
# Cache: in-memory + MongoDB persistence
# Структура: _cache[exchange_name] = {symbols: Set[str], updated: float}
# ════════════════════════════════════════════════════════════════
_cache: dict[str, dict] = {}
_CACHE_TTL_SEC = 3600  # 1 час

# Топ-30 на холодный старт если биржа недоступна
_FALLBACK_TOP_30 = {
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "AVAXUSDT", "DOGEUSDT", "DOTUSDT", "LINKUSDT",
    "MATICUSDT", "UNIUSDT", "ATOMUSDT", "LTCUSDT", "NEARUSDT",
    "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT", "FILUSDT",
    "SUIUSDT", "SEIUSDT", "TIAUSDT", "PEPEUSDT", "WIFUSDT",
    "ORDIUSDT", "RUNEUSDT", "LDOUSDT", "AAVEUSDT", "FETUSDT",
}


def _utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ════════════════════════════════════════════════════════════════
# Default exchange (для paper-фильтра и UI без явного указания)
# ════════════════════════════════════════════════════════════════
def get_default_exchange() -> str:
    """Какая биржа используется по умолчанию для paper-фильтра.
    Можно переопределить в Mongo: live_state коллекция, doc _id='default_exchange'."""
    try:
        from database import _get_db
        doc = _get_db().live_state.find_one({"_id": "default_exchange"})
        if doc and doc.get("exchange") in ("binance", "bingx"):
            return doc["exchange"]
    except Exception:
        pass
    return "bingx"  # default


def set_default_exchange(exchange: str) -> dict:
    """Сменить default exchange (только bingx или binance)."""
    if exchange not in ("binance", "bingx"):
        return {"ok": False, "error": "exchange must be 'binance' or 'bingx'"}
    try:
        from database import _get_db
        _get_db().live_state.update_one(
            {"_id": "default_exchange"},
            {"$set": {"exchange": exchange, "updated_at": _utcnow()}},
            upsert=True,
        )
        return {"ok": True, "default_exchange": exchange}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ════════════════════════════════════════════════════════════════
# Per-exchange fetcher: дёргает биржу через ccxt
# ════════════════════════════════════════════════════════════════
def _fetch_from_exchange(exchange: str) -> Set[str]:
    """Pull список USDT-perp пар через ccxt unified API.
    Защищено timeout'ом — если биржа медленная, прерываем через 20с."""
    try:
        import ccxt
    except ImportError:
        logger.error("[exchange-symbols] ccxt not installed")
        return set()

    try:
        if exchange == "binance":
            ex = ccxt.binance({"options": {"defaultType": "future"}, "timeout": 20000})
        elif exchange == "bingx":
            ex = ccxt.bingx({"options": {"defaultType": "swap"}, "timeout": 20000})  # BingX uses 'swap' for perpetuals
        else:
            logger.error(f"[exchange-symbols] unknown exchange: {exchange}")
            return set()

        markets = ex.load_markets()
        symbols = set()
        for canonical_sym, m in markets.items():
            if not m:
                continue
            # Нужны: active + swap/perpetual + USDT-quoted + linear (USDT-margin)
            if not m.get("active"):
                continue
            if not (m.get("swap") or m.get("type") == "swap"):
                continue
            if m.get("quote") != "USDT":
                continue
            # Linear (USDT-margin), не coin-margin
            if m.get("linear") is False:
                continue
            # Получаем XXXUSDT формат (id) для совместимости с paper.symbol
            sym_id = (m.get("id") or "").upper()
            if not sym_id:
                continue
            # BingX returns ids like 'BTC-USDT' или 'BTCUSDT' — нормализуем
            sym_id = sym_id.replace("-", "").replace("/", "").replace(":USDT", "")
            if sym_id.endswith("USDT"):
                symbols.add(sym_id)
        return symbols
    except Exception as e:
        logger.error(f"[exchange-symbols] fetch fail for {exchange}: {e}")
        return set()


# ════════════════════════════════════════════════════════════════
# Public API
# ════════════════════════════════════════════════════════════════
def _load_from_mongo(exchange: str) -> tuple[Set[str], float]:
    """Загрузить cached список из MongoDB (на холодный старт)."""
    try:
        from database import _get_db
        doc = _get_db().exchange_symbols.find_one({"_id": exchange})
        if doc and doc.get("symbols"):
            return set(doc["symbols"]), float(doc.get("updated_at_epoch") or 0)
    except Exception as e:
        logger.debug(f"[exchange-symbols] mongo load fail for {exchange}: {e}")
    return set(), 0.0


def get_supported_symbols(exchange: str = None) -> Set[str]:
    """Вернуть актуальный set символов биржи (НЕ блокирующая операция).

    Логика:
      • Если есть в in-memory cache → вернуть сразу
      • Иначе из MongoDB (быстро, ~5мс)
      • Если совсем пусто → fallback топ-30
      • Refresh из Binance/BingX API делается ТОЛЬКО в фоновом loop
        (никогда из hot path — иначе блокирует event loop)
    """
    exchange = exchange or get_default_exchange()

    state = _cache.get(exchange)
    if not state or not state.get("symbols"):
        # Холодный старт — Mongo (быстро)
        cached, ts = _load_from_mongo(exchange)
        state = {"symbols": cached, "updated": ts}
        _cache[exchange] = state

    if not state["symbols"]:
        # Совсем пусто (Mongo тоже пуст, watcher loop ещё не отработал)
        return _FALLBACK_TOP_30
    return state["symbols"]


def refresh_supported_symbols(exchange: str = None) -> dict:
    """Принудительно дёрнуть exchange API + persist в Mongo."""
    exchange = exchange or get_default_exchange()
    new_set = _fetch_from_exchange(exchange)
    if not new_set:
        return {"ok": False, "error": f"{exchange} fetch failed",
                "exchange": exchange,
                "stale_count": len(_cache.get(exchange, {}).get("symbols", set()))}

    state = _cache.get(exchange) or {"symbols": set(), "updated": 0}
    diff_added = sorted(list(new_set - state["symbols"]))
    diff_removed = sorted(list(state["symbols"] - new_set))
    state["symbols"] = new_set
    state["updated"] = time.time()
    _cache[exchange] = state

    try:
        from database import _get_db
        _get_db().exchange_symbols.update_one(
            {"_id": exchange},
            {"$set": {
                "exchange": exchange,
                "symbols": sorted(list(new_set)),
                "count": len(new_set),
                "updated_at": _utcnow(),
                "updated_at_epoch": state["updated"],
                "added_last_refresh": diff_added,
                "removed_last_refresh": diff_removed,
            }},
            upsert=True,
        )
    except Exception as e:
        logger.warning(f"[exchange-symbols] mongo persist fail: {e}")

    if diff_added or diff_removed:
        logger.warning(
            f"[exchange-symbols][{exchange}] {len(new_set)} pairs "
            f"(+{len(diff_added)}/-{len(diff_removed)}) | "
            f"+{diff_added[:3]} -{diff_removed[:3]}"
        )
    else:
        logger.info(f"[exchange-symbols][{exchange}] {len(new_set)} pairs (no changes)")

    return {
        "ok": True,
        "exchange": exchange,
        "count": len(new_set),
        "added": diff_added,
        "removed": diff_removed,
        "updated_at": _utcnow().isoformat(),
    }


def _multiplier_variants(symbol: str) -> list[str]:
    """Генерирует все вероятные варианты символа с/без множителей.
    Например для PEPEUSDT → [PEPEUSDT, 1000PEPEUSDT, 10000PEPEUSDT].
    Это потому что разные биржи используют разные prefixes для дешёвых монет:
      Binance: 1000PEPEUSDT
      BingX: 1000PEPEUSDT (часто) или PEPEUSDT (иногда)
      Bybit: 1000PEPEUSDT и т.д.
    """
    if not symbol:
        return []
    s = symbol.upper()
    variants = [s]
    # Если символ начинается с множителя — попробуем без и с другими
    multipliers = ["1000000", "10000", "1000", "100"]
    for m in multipliers:
        if s.startswith(m):
            base = s[len(m):]  # PEPEUSDT (после убирания 1000)
            variants.append(base)
            # Также попробуем с другими множителями
            for m2 in multipliers:
                if m2 != m:
                    variants.append(m2 + base)
            break
    else:
        # Не было множителя — добавим все возможные prefixes
        if s.endswith("USDT"):
            base = s[:-4]  # PEPE
            for m in multipliers:
                variants.append(m + base + "USDT")
    # Уникальные с сохранением порядка
    seen = set()
    return [v for v in variants if not (v in seen or seen.add(v))]


def is_symbol_supported(symbol: str, exchange: str = None) -> bool:
    """True если symbol торгуется на указанной бирже.
    Проверяет также варианты с множителями (PEPE ↔ 1000PEPE)."""
    if not symbol:
        return False
    available = get_supported_symbols(exchange)
    for variant in _multiplier_variants(symbol):
        if variant in available:
            return True
    return False


def resolve_symbol(symbol: str, exchange: str = None) -> str | None:
    """Возвращает КАНОНИЧЕСКОЕ имя символа на бирже (с правильным множителем).
    PEPEUSDT на BingX где есть 1000PEPEUSDT → возвращает '1000PEPEUSDT'.
    Возвращает None если символ не торгуется."""
    if not symbol:
        return None
    available = get_supported_symbols(exchange)
    for variant in _multiplier_variants(symbol):
        if variant in available:
            return variant
    return None


def get_meta(exchange: str = None) -> dict:
    """Метаинформация для UI."""
    exchange = exchange or get_default_exchange()
    syms = get_supported_symbols(exchange)
    state = _cache.get(exchange, {})
    ts = state.get("updated", 0)
    return {
        "exchange": exchange,
        "default_exchange": get_default_exchange(),
        "count": len(syms),
        "updated_at_epoch": ts,
        "updated_at": (datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None).isoformat()
                       if ts else None),
        "is_fallback": (syms == _FALLBACK_TOP_30),
        "ttl_sec": _CACHE_TTL_SEC,
        "stale": (time.time() - ts) > _CACHE_TTL_SEC if ts else True,
    }


def get_meta_all() -> dict:
    """Метаинфо по ВСЕМ биржам (для UI отображения двух колонок)."""
    return {
        "default": get_default_exchange(),
        "binance": get_meta("binance"),
        "bingx": get_meta("bingx"),
    }
