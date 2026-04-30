"""Async-safe in-memory TTL cache.

Использует asyncio.Lock для предотвращения race conditions — только один
запрос за раз пересчитывает ответ при cache miss, остальные ждут.

Future-ready: если перейдём на Redis — меняем внутренности здесь,
вызывающий код не трогаем. Структура:
  - CachedResult(key, data, expires_at)
  - AsyncTTLCache(ttl, lock_on_miss=True)
"""
from __future__ import annotations
import asyncio
import time
from typing import Any, Callable, Awaitable, Optional, Dict


class AsyncTTLCache:
    """Простой TTL-cache с per-key блокировкой.

    Использование:
        cache = AsyncTTLCache(ttl=60)
        result = await cache.get_or_compute("my_key", my_async_compute_fn)

    Если два запроса одновременно попадают в miss — только первый вызывает
    fn(), второй ждёт и получает тот же результат.
    """

    def __init__(self, ttl: float = 60.0):
        self.ttl = ttl
        self._store: Dict[str, tuple[float, Any]] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    def _lock(self, key: str) -> asyncio.Lock:
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    def get(self, key: str) -> Optional[Any]:
        entry = self._store.get(key)
        if not entry:
            return None
        expires_at, data = entry
        if time.time() >= expires_at:
            return None
        return data

    def set(self, key: str, data: Any, ttl: Optional[float] = None) -> None:
        self._store[key] = (time.time() + (ttl or self.ttl), data)

    def invalidate(self, key: Optional[str] = None) -> None:
        if key is None:
            self._store.clear()
        else:
            self._store.pop(key, None)

    async def get_or_compute(self, key: str, compute: Callable[[], Awaitable[Any]],
                             ttl: Optional[float] = None) -> Any:
        """Атомарно получить из кеша или вычислить + сохранить.
        compute — async callable без аргументов (wrap через partial/lambda если нужны).
        """
        # Fast path: cache hit
        cached = self.get(key)
        if cached is not None:
            return cached
        # Slow path: lock, recheck, compute
        async with self._lock(key):
            cached = self.get(key)
            if cached is not None:
                return cached
            data = await compute()
            self.set(key, data, ttl)
            return data


# ── Глобальные инстансы для admin.py ──
# TTL увеличены для разгрузки Atlas Free Tier M0 (30.04.2026 throttle issue).
# UI обновляется реже, но платформа стабильнее. После upgrade Atlas можно вернуть.
journal_cache = AsyncTTLCache(ttl=60)       # /api/journal — 60с (было 20)
kl_enrich_cache = AsyncTTLCache(ttl=120)    # /api/key-levels/enrich
journal_by_symbol_cache = AsyncTTLCache(ttl=180)  # /api/journal/by-symbol — 3 мин
top_picks_cache = AsyncTTLCache(ttl=120)    # /api/top-picks
pending_clusters_cache = AsyncTTLCache(ttl=180)  # /api/pending-clusters
confluence_cache = AsyncTTLCache(ttl=90)    # /api/confluence
anomalies_cache = AsyncTTLCache(ttl=90)     # /api/anomalies
smart_levels_cache = AsyncTTLCache(ttl=300)
fvg_signals_cache = AsyncTTLCache(ttl=90)   # /api/fvg-signals
fvg_journal_cache = AsyncTTLCache(ttl=90)   # /api/fvg-journal
paper_learnings_cache = AsyncTTLCache(ttl=120)
paper_rejections_cache = AsyncTTLCache(ttl=60)  # 30→60
paper_history_cache = AsyncTTLCache(ttl=60)     # 30→60
cv_flips_cache = AsyncTTLCache(ttl=120)         # 45→120
cv_flip_results_cache = AsyncTTLCache(ttl=180)  # 60→180
clusters_cache = AsyncTTLCache(ttl=90)          # 30→90
market_events_cache = AsyncTTLCache(ttl=120)
