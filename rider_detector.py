"""🏄 RIDER SHORT — тренд-райдер «от начала движения до конца» (research 2026-07-09).

Запрос: сигнал, который отрабатывает всё движение, а не кусок до фикс-TP.
Исследование 6 мес × 209 пар × 4h показало жёсткую асимметрию:
  • LONG-райдер НЕ существует: пампы альтов короткие, любой трейлинг
    отдаёт прибыль (PF 0.62-0.78 на всех вариантах). Лонги забирает
    фикс-тейк — IMPULSE/IGNITION.
  • SHORT-райдер работает: падения длинные и упорные, их надо ехать
    до конца. Donchian 55/20 на 4h: PF 1.29, +1.34%/сделку (комиссия
    учтена), WR 45%, медианный выигрыш +8.8%, медиана удержания ~6 дней,
    топ-пара 1% (без концентрации).

Правило входа (на ЗАКРЫТОЙ 4h-свече):
  • close < min(low прошлых 55 баров 4h)  — слом 9-дневного лоу
  • ATR%(1h,14) в 0.7-6.0                 — живая пара
  • BTC SuperTrend(10,3) 4h DOWN          — рынок не против шорта
Выход (трейлинг, проверяется на каждой закрытой 4h-свече):
  • close > max(high прошлых 20 баров 4h) — даунтренд сломан, закрыть.
Без фикс-TP/SL — позиция живёт, пока падение продолжается (дни-недели).

Профиль: тренд-фолловинг — прибыль концентрируется в фазах дампа
(янв +13.6%/сдел, май +4.5%), в росте/чопе мелкие минусы. Это нормально
и ожидаемо: система платит малым за право забрать -30% целиком.

Хранение: new_strategy_signals (strategy='rider_short', без tp/sl —
generic-трекер их пропускает, выходы ведёт check_open_riders).
"""
from __future__ import annotations
import logging
import time
from typing import Optional

from impulse_detector import _resample, _atr_pct, _supertrend_state

logger = logging.getLogger(__name__)

ENTRY_N = 55        # 4h-баров для входного лоу (~9 дней)
EXIT_N = 20         # 4h-баров для выходного хая (~3.3 дня)
ATR_GATE = (0.7, 6.0)

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


def check_entry(pair: str, candles_1h: Optional[list[dict]] = None,
                btc_st4: Optional[str] = None) -> Optional[dict]:
    """Проверка входа RIDER SHORT на последней ЗАКРЫТОЙ 4h-свече."""
    try:
        if candles_1h is None:
            from exchange import get_klines_any
            candles_1h = get_klines_any(pair, "1h", 500)
        if not candles_1h or len(candles_1h) < 300:
            return None

        atr_pct = _atr_pct(candles_1h)
        if atr_pct is None or not (ATR_GATE[0] <= atr_pct <= ATR_GATE[1]):
            return None
        if btc_st4 is None:
            btc_st4 = get_btc_st4()
        if btc_st4 != "DOWN":
            return None

        c4 = _resample(candles_1h, 4)
        if len(c4) < ENTRY_N + 6:
            return None
        closed = c4[:-1]                     # последняя = частичная, откинуть
        bar = closed[-1]
        lo55 = min(c["l"] for c in closed[-1 - ENTRY_N:-1])
        if bar["c"] >= lo55:
            return None
        hi20 = max(c["h"] for c in closed[-EXIT_N:])
        return {
            "strategy": "rider_short", "direction": "SHORT",
            "pair": pair, "symbol": pair.replace("/", "").upper(),
            "entry": bar["c"],
            "entry_bar_t": bar["t"],         # дедуп: один вход с бара
            "trail_hi20": hi20,              # инфо: текущий уровень выхода
            "indicators": {"atr_pct": round(atr_pct, 2), "lo55": lo55,
                           "btc_st4": btc_st4},
        }
    except Exception:
        logger.debug(f"[rider] check fail {pair}", exc_info=True)
        return None


def store_entry(sig: dict) -> bool:
    """Одна открытая позиция на пару + один вход с одного 4h-бара."""
    try:
        from database import _get_db, utcnow
        col = _get_db().new_strategy_signals
        if col.find_one({"strategy": "rider_short", "pair": sig["pair"],
                         "state": "WAITING"}):
            return False
        if col.find_one({"strategy": "rider_short", "pair": sig["pair"],
                         "entry_bar_t": sig["entry_bar_t"]}):
            return False
        col.insert_one({**sig, "created_at": utcnow(), "state": "WAITING"})
        logger.info(f"[rider] SHORT entry {sig['pair']} @ {sig['entry']}")
        return True
    except Exception:
        logger.exception("[rider] store fail")
        return False


def scan_entries(max_pairs: int = 300) -> list[dict]:
    """Скан входов. Вызывается из watcher-лупа (thread)."""
    from futures_data import get_liquid_pairs
    fired = []
    btc = get_btc_st4()
    if btc != "DOWN":
        return fired                        # рынок не в шорт-режиме — тишина
    try:
        pairs = get_liquid_pairs(min_volume_usd=5_000_000)[:max_pairs]
    except Exception:
        return fired
    for sym in pairs:
        pair = sym.replace("USDT", "/USDT") if "/" not in sym else sym
        try:
            sig = check_entry(pair, btc_st4=btc)
            if sig and store_entry(sig):
                fired.append(sig)
        except Exception:
            continue
    return fired


def check_open_riders() -> list[dict]:
    """Трейлинг-выход: close(закрытой 4h) > hi20 → закрыть позицию.
    Возвращает закрытые доки (для алертов)."""
    closed_out = []
    try:
        from database import _get_db, utcnow
        from exchange import get_klines_any
        col = _get_db().new_strategy_signals
        open_docs = list(col.find({"strategy": "rider_short", "state": "WAITING"}))
        for d in open_docs:
            try:
                kl = get_klines_any(d["pair"], "1h", 400)
                if not kl or len(kl) < 100:
                    continue
                c4 = _resample(kl, 4)
                closed = c4[:-1]
                if len(closed) < EXIT_N + 2:
                    continue
                bar = closed[-1]
                if bar["t"] <= (d.get("entry_bar_t") or 0):
                    continue                # ещё нет новых закрытых баров
                hi20 = max(c["h"] for c in closed[-1 - EXIT_N:-1])
                if bar["c"] > hi20:
                    entry = d["entry"]
                    pnl = (entry - bar["c"]) / entry * 100
                    state = "TP" if pnl > 0 else "SL"
                    col.update_one({"_id": d["_id"]}, {"$set": {
                        "state": state, "exit_price": bar["c"],
                        "pnl_pct": round(pnl, 3), "exit_at": utcnow(),
                        "updated_at": utcnow(),
                    }})
                    closed_out.append({**d, "exit_price": bar["c"],
                                       "pnl_pct": round(pnl, 3), "state": state})
                    logger.info(f"[rider] EXIT {d['pair']} pnl={pnl:+.2f}%")
                else:
                    col.update_one({"_id": d["_id"]},
                                   {"$set": {"trail_hi20": hi20,
                                             "updated_at": utcnow()}})
            except Exception:
                continue
    except Exception:
        logger.exception("[rider] exit check fail")
    return closed_out
