"""🏄 RIDER SHORT — ВЫКЛЮЧЕН и удалён с платформы 2026-07-31.

Live-результаты убили стратегию: 442 закрытых сделки за июнь-июль,
WR 31%, EV −4.65/сделку, Σ −2053пп; хвосты до −92% (шорт без стопа
в рынке, где случается 87 ралли ≥+40% в месяц). Гейт «BTC ST4h DOWN»
от идиосинкратических мем-разгонов не защищал. Ресёрч-цифры
(PF 1.29 на 6 мес 4h Donchian 55/20) режим 06-07.2026 не пережили.
Сигналы вычищены из БД, входы/выходы/алерты удалены.

Файл оставлен ради get_btc_st4 — общей утилиты «BTC SuperTrend 4h»,
которую используют admin/watcher/paper_trader/supertrend_tracker.
"""
from __future__ import annotations
import logging
import time
from typing import Optional

from impulse_detector import _resample, _supertrend_state

logger = logging.getLogger(__name__)

_btc_st4_cache: dict = {"ts": 0.0, "st": None}


def get_btc_st4() -> Optional[str]:
    """BTC SuperTrend 4h. Кэш 10 мин."""
    now = time.time()
    if now - _btc_st4_cache["ts"] < 600 and _btc_st4_cache["st"] is not None:
        return _btc_st4_cache["st"]
    try:
        from exchange import get_klines_any
        kl = get_klines_any("BTC/USDT", "1h", 500)
        if kl and len(kl) >= 300:
            st = _supertrend_state(_resample(kl, 4))
            _btc_st4_cache.update(ts=now, st=st)
            return st
    except Exception:
        pass
    return _btc_st4_cache["st"]
