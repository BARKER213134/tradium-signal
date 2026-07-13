"""Глобальный бюджет запросов к fapi.binance.com — защита от 418-банов.

2026-07-13: суммарный темп всех циклов (ST-трекер 5м, momentum 20м,
whale/shark 30м, accum 30м, rsi/trend-кэши, delta-снапшоты на сигнал)
в пике превышал лимиты Binance -> повторные баны IP: REST 418 + немой
вебсокет. Симптомы лечили весь день, это — причина.

Скользящее окно 60с. Потребители спрашивают allow() ПЕРЕД fapi-запросом;
False -> сразу фолбэк (Vision/спот/BingX/кэш) без траты попытки и таймаута.
Бюджет: env FAPI_BUDGET_PER_MIN (дефолт 240 — консервативно против
лимита Binance 2400 weight/мин; klines limit<=1500 весит 5-10).
"""
import os
import threading
import time

_LOCK = threading.Lock()
_WIN: list = []            # таймстампы разрешённых запросов за последние 60с
_DENIED = 0

BUDGET = int(os.getenv("FAPI_BUDGET_PER_MIN", "240"))


def allow(n: int = 1) -> bool:
    """True — можно сделать n запросов к fapi; False — уходи на фолбэк."""
    global _DENIED
    now = time.time()
    with _LOCK:
        while _WIN and now - _WIN[0] > 60:
            _WIN.pop(0)
        if len(_WIN) + n > BUDGET:
            _DENIED += 1
            return False
        _WIN.extend([now] * n)
        return True


def stats() -> dict:
    now = time.time()
    with _LOCK:
        used = sum(1 for t in _WIN if now - t <= 60)
    return {"used_per_min": used, "budget": BUDGET, "denied_total": _DENIED}
