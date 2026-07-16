# -*- coding: utf-8 -*-
"""🎓 Оценка сделки по сигналу: (источник × направление × фаза бейджа)
→ EV/WR из единого бэктеста всех сигналов базы (3 мес, 47.9k сигналов,
одна сетка: LONG +6/-3, SHORT -5/+3, 72ч, first-touch, оба=SL,
таймаут = факт. закрытие; вход = close 1h-бара сигнала).

Пересчёт: research/all_signals_unified_bt.py → unified_bt.json.
Грейды: A (🟩 EV>=+0.4%/сделку) · B (🟨 0..0.4) · C (🟥 <0).
Срез по фазе: 'по фазе' (dir совпал с бейджем) / 'против' / 'нейтраль';
если среза нет или n<30 — фолбэк на 'ВСЕ'.
"""
import logging
import time

logger = logging.getLogger(__name__)

# (src, dir, slice) -> (n, WR%, EV%/сделку)
EDGE = {
    ('combo', 'LONG', 'ВСЕ'): (488, 43.6, 0.377),
    ('combo', 'LONG', 'нейтраль'): (295, 41.7, 0.128),
    ('combo', 'LONG', 'по фазе'): (88, 61.4, 1.918),
    ('combo', 'LONG', 'против'): (105, 34.3, -0.215),
    ('combo', 'SHORT', 'ВСЕ'): (668, 37.0, -0.203),
    ('combo', 'SHORT', 'нейтраль'): (89, 28.1, -0.762),
    ('combo', 'SHORT', 'по фазе'): (407, 45.5, 0.436),
    ('combo', 'SHORT', 'против'): (172, 21.5, -1.425),
    ('confluence_5plus', 'LONG', 'ВСЕ'): (437, 42.8, 0.413),
    ('confluence_5plus', 'LONG', 'нейтраль'): (233, 42.9, 0.322),
    ('confluence_5plus', 'LONG', 'по фазе'): (44, 36.4, -0.015),
    ('confluence_5plus', 'LONG', 'против'): (160, 44.4, 0.663),
    ('confluence_5plus', 'SHORT', 'ВСЕ'): (494, 36.6, -0.301),
    ('confluence_5plus', 'SHORT', 'нейтраль'): (145, 29.7, -0.857),
    ('confluence_5plus', 'SHORT', 'по фазе'): (296, 41.6, 0.07),
    ('confluence_5plus', 'SHORT', 'против'): (53, 28.3, -0.859),
    ('confluence_lo', 'LONG', 'ВСЕ'): (3893, 36.6, -0.111),
    ('confluence_lo', 'LONG', 'нейтраль'): (2089, 39.1, 0.083),
    ('confluence_lo', 'LONG', 'по фазе'): (660, 41.5, 0.283),
    ('confluence_lo', 'LONG', 'против'): (1144, 29.0, -0.694),
    ('confluence_lo', 'SHORT', 'ВСЕ'): (4284, 37.0, -0.256),
    ('confluence_lo', 'SHORT', 'нейтраль'): (1197, 33.8, -0.634),
    ('confluence_lo', 'SHORT', 'по фазе'): (2612, 39.3, -0.017),
    ('confluence_lo', 'SHORT', 'против'): (475, 32.4, -0.621),
    ('delta_series', 'LONG', 'ВСЕ'): (206, 39.8, 0.429),
    ('delta_series', 'LONG', 'нейтраль'): (110, 45.5, 0.752),
    ('delta_series', 'LONG', 'против'): (90, 34.4, 0.163),
    ('delta_series', 'SHORT', 'ВСЕ'): (156, 39.7, 0.004),
    ('delta_series', 'SHORT', 'нейтраль'): (35, 45.7, 0.126),
    ('delta_series', 'SHORT', 'по фазе'): (113, 35.4, -0.235),
    ('fade', 'SHORT', 'ВСЕ'): (239, 43.9, 0.249),
    ('fade', 'SHORT', 'нейтраль'): (126, 46.0, 0.436),
    ('fade', 'SHORT', 'по фазе'): (113, 41.6, 0.04),
    ('ignition', 'LONG', 'ВСЕ'): (262, 29.8, -0.497),
    ('ignition', 'LONG', 'нейтраль'): (122, 29.5, -0.608),
    ('ignition', 'LONG', 'против'): (120, 30.8, -0.342),
    ('impulse', 'LONG', 'ВСЕ'): (232, 34.9, -0.05),
    ('impulse', 'LONG', 'нейтраль'): (113, 35.4, -0.139),
    ('impulse', 'LONG', 'против'): (103, 34.0, -0.017),
    ('rider_short', 'SHORT', 'ВСЕ'): (263, 37.3, -0.128),
    ('rider_short', 'SHORT', 'нейтраль'): (37, 37.8, 0.027),
    ('rider_short', 'SHORT', 'по фазе'): (226, 37.2, -0.154),
    ('second_flip', 'LONG', 'ВСЕ'): (791, 32.7, -0.187),
    ('second_flip', 'LONG', 'нейтраль'): (378, 35.4, 0.003),
    ('second_flip', 'LONG', 'по фазе'): (59, 25.4, -0.599),
    ('second_flip', 'LONG', 'против'): (354, 31.1, -0.322),
    ('second_flip', 'SHORT', 'ВСЕ'): (679, 40.5, 0.177),
    ('second_flip', 'SHORT', 'нейтраль'): (112, 29.5, -0.731),
    ('second_flip', 'SHORT', 'по фазе'): (516, 44.2, 0.487),
    ('second_flip', 'SHORT', 'против'): (51, 27.5, -0.966),
    ('shark', 'SHORT', 'ВСЕ'): (757, 52.6, 1.021),
    ('shark', 'SHORT', 'нейтраль'): (84, 42.9, 0.217),
    ('shark', 'SHORT', 'по фазе'): (634, 54.6, 1.187),
    ('shark', 'SHORT', 'против'): (39, 41.0, 0.057),
    ('st_break', 'LONG', 'ВСЕ'): (995, 36.7, 0.07),
    ('st_break', 'LONG', 'нейтраль'): (903, 38.8, 0.238),
    ('st_break', 'LONG', 'по фазе'): (34, 20.6, -1.454),
    ('st_break', 'LONG', 'против'): (58, 13.8, -1.658),
    ('st_break', 'SHORT', 'ВСЕ'): (1542, 39.2, 0.001),
    ('st_break', 'SHORT', 'по фазе'): (1529, 39.0, -0.017),
    ('st_break4h', 'LONG', 'ВСЕ'): (475, 38.7, 0.314),
    ('st_break4h', 'LONG', 'нейтраль'): (323, 42.7, 0.611),
    ('st_break4h', 'LONG', 'против'): (141, 30.5, -0.3),
    ('st_break4h', 'SHORT', 'ВСЕ'): (562, 29.2, -0.767),
    ('st_break4h', 'SHORT', 'нейтраль'): (65, 24.6, -0.973),
    ('st_break4h', 'SHORT', 'по фазе'): (490, 29.2, -0.79),
    ('st_daily', 'LONG', 'ВСЕ'): (7432, 35.8, -0.049),
    ('st_daily', 'LONG', 'нейтраль'): (4106, 38.6, 0.119),
    ('st_daily', 'LONG', 'по фазе'): (464, 44.6, 0.717),
    ('st_daily', 'LONG', 'против'): (2862, 30.4, -0.415),
    ('st_daily', 'SHORT', 'ВСЕ'): (3163, 39.8, 0.068),
    ('st_daily', 'SHORT', 'нейтраль'): (547, 32.4, -0.426),
    ('st_daily', 'SHORT', 'по фазе'): (2219, 43.6, 0.35),
    ('st_daily', 'SHORT', 'против'): (397, 29.0, -0.825),
    ('st_mtf', 'LONG', 'ВСЕ'): (5509, 40.1, 0.436),
    ('st_mtf', 'LONG', 'нейтраль'): (2464, 40.5, 0.416),
    ('st_mtf', 'LONG', 'по фазе'): (861, 44.1, 0.92),
    ('st_mtf', 'LONG', 'против'): (2184, 38.0, 0.268),
    ('st_mtf', 'SHORT', 'ВСЕ'): (4240, 36.0, -0.198),
    ('st_mtf', 'SHORT', 'нейтраль'): (849, 30.0, -0.679),
    ('st_mtf', 'SHORT', 'по фазе'): (2961, 40.1, 0.128),
    ('st_mtf', 'SHORT', 'против'): (430, 19.1, -1.496),
    ('st_vip', 'LONG', 'ВСЕ'): (1947, 39.1, -0.005),
    ('st_vip', 'LONG', 'нейтраль'): (1061, 40.0, -0.03),
    ('st_vip', 'LONG', 'по фазе'): (325, 47.7, 0.612),
    ('st_vip', 'LONG', 'против'): (561, 32.6, -0.316),
    ('st_vip', 'SHORT', 'ВСЕ'): (1921, 39.0, -0.094),
    ('st_vip', 'SHORT', 'нейтраль'): (269, 37.9, -0.152),
    ('st_vip', 'SHORT', 'по фазе'): (1345, 44.8, 0.344),
    ('st_vip', 'SHORT', 'против'): (307, 14.3, -1.962),
    ('ten', 'LONG', 'ВСЕ'): (157, 39.5, 0.355),
    ('ten', 'LONG', 'нейтраль'): (83, 44.6, 0.729),
    ('ten', 'LONG', 'против'): (67, 34.3, -0.027),
    ('triple_confluence', 'LONG', 'ВСЕ'): (586, 38.4, 0.009),
    ('triple_confluence', 'LONG', 'нейтраль'): (302, 42.4, 0.179),
    ('triple_confluence', 'LONG', 'по фазе'): (90, 37.8, 0.079),
    ('triple_confluence', 'LONG', 'против'): (194, 32.5, -0.288),
    ('triple_confluence', 'SHORT', 'ВСЕ'): (641, 41.7, 0.182),
    ('triple_confluence', 'SHORT', 'нейтраль'): (90, 40.0, -0.063),
    ('triple_confluence', 'SHORT', 'по фазе'): (438, 47.5, 0.665),
    ('triple_confluence', 'SHORT', 'против'): (113, 20.4, -1.492),
    ('verified', 'LONG', 'ВСЕ'): (250, 24.8, -0.827),
    ('verified', 'LONG', 'нейтраль'): (105, 16.2, -1.651),
    ('verified', 'LONG', 'по фазе'): (48, 35.4, 0.059),
    ('verified', 'LONG', 'против'): (97, 28.9, -0.374),
    ('verified', 'SHORT', 'ВСЕ'): (500, 37.2, -0.079),
    ('verified', 'SHORT', 'нейтраль'): (92, 21.7, -1.316),
    ('verified', 'SHORT', 'по фазе'): (404, 40.8, 0.212),
    ('vol_accum', 'LONG', 'ВСЕ'): (1330, 33.9, -0.154),
    ('vol_accum', 'LONG', 'нейтраль'): (649, 37.8, 0.13),
    ('vol_accum', 'LONG', 'по фазе'): (103, 32.0, -0.268),
    ('vol_accum', 'LONG', 'против'): (578, 29.9, -0.453),
    ('vol_accum', 'SHORT', 'ВСЕ'): (997, 37.0, -0.145),
    ('vol_accum', 'SHORT', 'нейтраль'): (169, 26.0, -1.029),
    ('vol_accum', 'SHORT', 'по фазе'): (715, 41.4, 0.208),
    ('vol_accum', 'SHORT', 'против'): (113, 25.7, -1.053),
    ('volcano', 'LONG', 'ВСЕ'): (131, 30.5, -0.41),
    ('volcano', 'LONG', 'нейтраль'): (53, 34.0, -0.173),
    ('volcano', 'LONG', 'против'): (71, 29.6, -0.458),
    ('volume_surge', 'LONG', 'ВСЕ'): (565, 35.6, 0.029),
    ('volume_surge', 'LONG', 'нейтраль'): (245, 40.0, 0.215),
    ('volume_surge', 'LONG', 'по фазе'): (49, 51.0, 1.332),
    ('volume_surge', 'LONG', 'против'): (271, 28.8, -0.375),
    ('volume_surge', 'SHORT', 'ВСЕ'): (371, 38.5, -0.016),
    ('volume_surge', 'SHORT', 'нейтраль'): (51, 45.1, 0.342),
    ('volume_surge', 'SHORT', 'по фазе'): (261, 41.4, 0.259),
    ('volume_surge', 'SHORT', 'против'): (59, 20.3, -1.543),
    ('whale', 'LONG', 'ВСЕ'): (703, 57.8, 1.814),
    ('whale', 'LONG', 'нейтраль'): (421, 58.2, 1.794),
    ('whale', 'LONG', 'по фазе'): (50, 54.0, 1.385),
    ('whale', 'LONG', 'против'): (232, 57.8, 1.943),
}

# источники без оценки (группы/записи, не сигналы)
_SKIP = {"stack", "paper", "accum", "cluster", "anomaly"}

_flips_cache = {"at": 0.0, "flips": []}


def _phase_flips():
    """Флипы фаз из market_side_history (кэш 5 мин). [(ts_ms, side)] asc."""
    now = time.time()
    if now - _flips_cache["at"] < 300 and _flips_cache["flips"]:
        return _flips_cache["flips"]
    try:
        from datetime import timezone
        from database import _get_db
        flips = [(f["at"].replace(tzinfo=timezone.utc).timestamp() * 1000, f["side"])
                 for f in _get_db().market_side_history.find(
                     {}, {"at": 1, "side": 1}).sort("at", 1)]
        if flips:
            _flips_cache.update(at=now, flips=flips)
        return flips
    except Exception:
        logger.debug("[trade-grade] flips load fail", exc_info=True)
        return _flips_cache["flips"]


def _phase_at(ts_ms, flips):
    if not flips or ts_ms < flips[0][0]:
        return None
    lo, hi = 0, len(flips) - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if flips[mid][0] <= ts_ms:
            lo = mid
        else:
            hi = mid - 1
    return flips[lo][1]


def _src_key(item: dict):
    src = item.get("source")
    if not src or src in _SKIP:
        return None
    if src == "confluence":
        return "confluence_5plus" if (item.get("score") or 0) >= 5 else "confluence_lo"
    if src == "supertrend":
        return f"st_{item.get('st_tier') or 'daily'}"
    return src


def annotate_items(items: list) -> None:
    """Проставляет trade_grade/trade_ev/trade_wr/trade_n/trade_slice
    каждому item журнала (in-place). Дёшево: бинпоиск по кэшу флипов."""
    flips = _phase_flips()
    for it in items:
        try:
            src = _src_key(it)
            d = it.get("direction")
            if not src or d not in ("LONG", "SHORT"):
                continue
            ph = _phase_at((it.get("at_ts") or 0) * 1000, flips) if it.get("at_ts") else None
            sl = ("по фазе" if (d == ph) else
                  "против" if (ph in ("LONG", "SHORT") and d != ph) else
                  "нейтраль" if ph == "NEUTRAL" else None)
            row = EDGE.get((src, d, sl)) if sl else None
            used = sl
            if row is None or row[0] < 30:
                row = EDGE.get((src, d, "ВСЕ"))
                used = "ВСЕ"
            if row is None:
                continue
            n, wr, ev = row
            it["trade_grade"] = "A" if ev >= 0.4 else "B" if ev >= 0 else "C"
            it["trade_ev"] = ev
            it["trade_wr"] = wr
            it["trade_n"] = n
            it["trade_slice"] = used
            it["trade_phase"] = ph
        except Exception:
            continue
