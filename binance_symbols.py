"""Binance Futures symbol registry — single source of truth для торгуемых пар.

Логика:
  • Раз в час подтягиваем GET fapi.binance.com/fapi/v1/exchangeInfo
  • Фильтруем USDT-perpetual со статусом TRADING
  • Кешируем in-memory + персистим в MongoDB (binance_symbols collection)
  • is_symbol_supported('HANAUSDT') → False (нет на Binance) → paper не трейдит

Все источники сигналов (CryptoVizor/Confluence/SuperTrend/Cluster/Anomaly)
могут трекать любые пары, но в paper попадают только те что на Binance —
чтобы paper-результаты были корректным прокси для реальной торговли.
"""
from __future__ import annotations
import logging
import time
from typing import Set
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# In-memory cache (быстрый доступ из horizons watcher / paper.on_signal)
_cache_symbols: Set[str] = set()
_cache_updated_at: float = 0.0  # epoch seconds
_CACHE_TTL_SEC = 3600  # 1 час

# Минимальный набор на холодный старт если Binance недоступен
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


def _fetch_from_binance() -> Set[str]:
    """GET /fapi/v1/exchangeInfo, фильтр TRADING + PERPETUAL + USDT-quoted."""
    import requests
    try:
        r = requests.get(
            "https://fapi.binance.com/fapi/v1/exchangeInfo",
            timeout=15,
            headers={"User-Agent": "tradium-signal/1.0"},
        )
        r.raise_for_status()
        data = r.json()
        symbols = set()
        for s in data.get("symbols", []):
            if (s.get("status") == "TRADING"
                and s.get("contractType") == "PERPETUAL"
                and s.get("quoteAsset") == "USDT"):
                sym = s.get("symbol", "")
                if sym:
                    symbols.add(sym)
        return symbols
    except Exception as e:
        logger.error(f"[binance-symbols] fetch fail: {e}")
        return set()


def _load_from_mongo() -> tuple[Set[str], float]:
    """Загрузить cached список из MongoDB (на холодный старт сервера)."""
    try:
        from database import _get_db
        doc = _get_db().binance_symbols.find_one({"_id": "state"})
        if doc and doc.get("symbols"):
            syms = set(doc["symbols"])
            ts = float(doc.get("updated_at_epoch") or 0)
            return syms, ts
    except Exception as e:
        logger.debug(f"[binance-symbols] mongo load fail: {e}")
    return set(), 0.0


def get_supported_symbols() -> Set[str]:
    """Вернуть актуальный set символов. Авто-рефреш если кеш стар.
    На холодный старт грузит из Mongo, потом дёргает Binance если нужно."""
    global _cache_symbols, _cache_updated_at
    now = time.time()

    # Холодный старт — пробуем Mongo
    if not _cache_symbols:
        cached, ts = _load_from_mongo()
        if cached:
            _cache_symbols = cached
            _cache_updated_at = ts

    # Если всё ещё пусто или кеш просрочен — рефрешим из Binance
    if not _cache_symbols or (now - _cache_updated_at) > _CACHE_TTL_SEC:
        try:
            refresh_supported_symbols()
        except Exception as e:
            logger.warning(f"[binance-symbols] auto-refresh fail: {e}")

    # Fallback: если совсем ничего нет — вернуть топ-30 (лучше чем None)
    if not _cache_symbols:
        return _FALLBACK_TOP_30
    return _cache_symbols


def refresh_supported_symbols() -> dict:
    """Принудительно дёрнуть Binance API + сохранить в Mongo.
    Возвращает {ok, count, added[], removed[]}."""
    global _cache_symbols, _cache_updated_at
    new_set = _fetch_from_binance()
    if not new_set:
        return {"ok": False, "error": "Binance fetch failed",
                "count": len(_cache_symbols),
                "stale_count": len(_cache_symbols)}

    diff_added = sorted(list(new_set - _cache_symbols))
    diff_removed = sorted(list(_cache_symbols - new_set))
    _cache_symbols = new_set
    _cache_updated_at = time.time()

    try:
        from database import _get_db
        _get_db().binance_symbols.update_one(
            {"_id": "state"},
            {"$set": {
                "symbols": sorted(list(new_set)),
                "count": len(new_set),
                "updated_at": _utcnow(),
                "updated_at_epoch": _cache_updated_at,
                "added_last_refresh": diff_added,
                "removed_last_refresh": diff_removed,
            }},
            upsert=True,
        )
    except Exception as e:
        logger.warning(f"[binance-symbols] mongo persist fail: {e}")

    if diff_added or diff_removed:
        logger.warning(
            f"[binance-symbols] refreshed: {len(new_set)} pairs "
            f"(+{len(diff_added)}/-{len(diff_removed)}) | "
            f"added={diff_added[:5]} removed={diff_removed[:5]}"
        )
    else:
        logger.info(f"[binance-symbols] refreshed: {len(new_set)} pairs (no changes)")

    return {
        "ok": True,
        "count": len(new_set),
        "added": diff_added,
        "removed": diff_removed,
        "updated_at": _utcnow().isoformat(),
    }


def is_symbol_supported(symbol: str) -> bool:
    """True если symbol торгуется на Binance Futures USDT-perp.
    symbol — формат XXXUSDT (как в paper_trader.symbol)."""
    if not symbol:
        return False
    sym = symbol.upper()
    # Снимаем префиксы 1000 для сравнения? Нет — Binance тоже
    # держит '1000PEPEUSDT' как отдельный символ.
    return sym in get_supported_symbols()


def get_meta() -> dict:
    """Метаинформация для UI (количество, время последнего апдейта)."""
    syms = get_supported_symbols()
    return {
        "count": len(syms),
        "updated_at_epoch": _cache_updated_at,
        "updated_at": (datetime.fromtimestamp(_cache_updated_at, tz=timezone.utc).replace(tzinfo=None).isoformat()
                       if _cache_updated_at else None),
        "is_fallback": (_cache_symbols == _FALLBACK_TOP_30),
        "ttl_sec": _CACHE_TTL_SEC,
        "stale": (time.time() - _cache_updated_at) > _CACHE_TTL_SEC if _cache_updated_at else True,
    }
