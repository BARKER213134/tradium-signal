# -*- coding: utf-8 -*-
"""🎓 Оценка сделки под ЦЕЛЬ +10%: (источник × направление × фаза бейджа)
→ EV/WR/hit10 из единого бэктеста всех сигналов базы (3 мес, ~48k сигналов).

Сетка: LONG TP +10% / SL −5%, SHORT TP −10% / SL +5% (RR 2:1, БУ 33.3%),
горизонт 96ч, first-touch по 1h high/low, оба в баре = SL (консервативно),
таймаут = фактическое закрытие. Вход = close 1h-бара сигнала.
hit10 = доля сделок, дошедших до полных +10%.

Пересчёт: research/all_signals_ten_bt.py → ten_bt.json.
Грейды: A (🟩 EV >= +0.75%/сделку) · B (🟨 0..0.75) · C (🟥 < 0).
Срез по фазе: 'по фазе' / 'против' / 'нейтраль'; нет среза или n<30 → 'ВСЕ'.
"""
import logging
import time

logger = logging.getLogger(__name__)

# (src, dir, slice) -> (n, WR%, EV%/сделку, hit10%)
EDGE = {
    ('combo', 'LONG', 'ВСЕ'): (488, 43.4, 0.501, 21.9),
    ('combo', 'LONG', 'нейтраль'): (295, 46.4, 0.601, 20.3),
    ('combo', 'LONG', 'по фазе'): (88, 53.4, 2.317, 33.0),
    ('combo', 'LONG', 'против'): (105, 26.7, -1.3, 17.1),
    ('combo', 'SHORT', 'ВСЕ'): (668, 43.6, 0.418, 22.3),
    ('combo', 'SHORT', 'нейтраль'): (89, 25.8, -1.805, 13.5),
    ('combo', 'SHORT', 'по фазе'): (407, 55.0, 1.934, 30.5),
    ('combo', 'SHORT', 'против'): (172, 25.6, -2.018, 7.6),
    ('confluence_5plus', 'LONG', 'ВСЕ'): (431, 46.9, 1.038, 22.5),
    ('confluence_5plus', 'LONG', 'нейтраль'): (227, 44.9, 0.654, 19.4),
    ('confluence_5plus', 'LONG', 'по фазе'): (44, 36.4, 0.219, 22.7),
    ('confluence_5plus', 'LONG', 'против'): (160, 52.5, 1.807, 26.9),
    ('confluence_5plus', 'SHORT', 'ВСЕ'): (484, 40.9, -0.502, 12.0),
    ('confluence_5plus', 'SHORT', 'нейтраль'): (135, 36.3, -1.536, 5.2),
    ('confluence_5plus', 'SHORT', 'по фазе'): (296, 44.3, 0.053, 15.2),
    ('confluence_5plus', 'SHORT', 'против'): (53, 34.0, -0.97, 11.3),
    ('confluence_lo', 'LONG', 'ВСЕ'): (3841, 39.1, -0.016, 19.5),
    ('confluence_lo', 'LONG', 'нейтраль'): (2037, 42.3, 0.279, 19.4),
    ('confluence_lo', 'LONG', 'по фазе'): (660, 42.1, 0.781, 25.6),
    ('confluence_lo', 'LONG', 'против'): (1144, 31.6, -1.001, 16.2),
    ('confluence_lo', 'SHORT', 'ВСЕ'): (4202, 40.6, -0.233, 15.6),
    ('confluence_lo', 'SHORT', 'нейтраль'): (1115, 37.0, -0.833, 11.9),
    ('confluence_lo', 'SHORT', 'по фазе'): (2612, 42.2, 0.122, 17.9),
    ('confluence_lo', 'SHORT', 'против'): (475, 40.2, -0.776, 12.0),
    ('delta_series', 'LONG', 'ВСЕ'): (198, 37.4, 0.16, 26.8),
    ('delta_series', 'LONG', 'нейтраль'): (102, 42.2, 0.434, 25.5),
    ('delta_series', 'LONG', 'против'): (90, 32.2, -0.141, 27.8),
    ('delta_series', 'SHORT', 'ВСЕ'): (154, 40.3, -0.341, 18.8),
    ('delta_series', 'SHORT', 'нейтраль'): (33, 39.4, -0.587, 21.2),
    ('delta_series', 'SHORT', 'по фазе'): (113, 38.1, -0.611, 16.8),
    ('fade', 'SHORT', 'ВСЕ'): (239, 43.9, -0.192, 13.4),
    ('fade', 'SHORT', 'нейтраль'): (126, 51.6, 0.379, 11.1),
    ('fade', 'SHORT', 'по фазе'): (113, 35.4, -0.828, 15.9),
    ('ignition', 'LONG', 'ВСЕ'): (250, 30.0, -1.059, 18.0),
    ('ignition', 'LONG', 'нейтраль'): (110, 30.9, -0.835, 17.3),
    ('ignition', 'LONG', 'против'): (120, 30.8, -0.998, 20.0),
    ('impulse', 'LONG', 'ВСЕ'): (224, 36.2, -0.007, 28.6),
    ('impulse', 'LONG', 'нейтраль'): (105, 37.1, -0.006, 26.7),
    ('impulse', 'LONG', 'против'): (103, 35.9, 0.039, 30.1),
    ('rider_short', 'SHORT', 'ВСЕ'): (263, 43.0, 0.349, 24.7),
    ('rider_short', 'SHORT', 'нейтраль'): (37, 32.4, -0.792, 24.3),
    ('rider_short', 'SHORT', 'по фазе'): (226, 44.7, 0.536, 24.8),
    ('second_flip', 'LONG', 'ВСЕ'): (789, 33.0, -0.66, 20.0),
    ('second_flip', 'LONG', 'нейтраль'): (376, 35.1, -0.667, 17.6),
    ('second_flip', 'LONG', 'по фазе'): (59, 27.1, -1.017, 22.0),
    ('second_flip', 'LONG', 'против'): (354, 31.6, -0.593, 22.3),
    ('second_flip', 'SHORT', 'ВСЕ'): (679, 47.4, 1.164, 29.7),
    ('second_flip', 'SHORT', 'нейтраль'): (112, 37.5, -0.071, 24.1),
    ('second_flip', 'SHORT', 'по фазе'): (516, 49.4, 1.494, 32.4),
    ('second_flip', 'SHORT', 'против'): (51, 49.0, 0.544, 15.7),
    ('shark', 'SHORT', 'ВСЕ'): (754, 51.6, 1.305, 26.1),
    ('shark', 'SHORT', 'нейтраль'): (81, 39.5, -0.148, 23.5),
    ('shark', 'SHORT', 'по фазе'): (634, 53.6, 1.532, 26.3),
    ('shark', 'SHORT', 'против'): (39, 43.6, 0.623, 28.2),
    ('st_break', 'LONG', 'ВСЕ'): (982, 37.5, -0.181, 18.8),
    ('st_break', 'LONG', 'нейтраль'): (890, 39.7, 0.101, 19.9),
    ('st_break', 'LONG', 'по фазе'): (34, 17.6, -2.906, 8.8),
    ('st_break', 'LONG', 'против'): (58, 15.5, -2.901, 8.6),
    ('st_break', 'SHORT', 'ВСЕ'): (1542, 39.5, -0.204, 18.0),
    ('st_break', 'SHORT', 'по фазе'): (1529, 39.4, -0.236, 17.8),
    ('st_break4h', 'LONG', 'ВСЕ'): (454, 38.8, 0.216, 24.0),
    ('st_break4h', 'LONG', 'нейтраль'): (302, 42.4, 0.686, 24.5),
    ('st_break4h', 'LONG', 'против'): (141, 33.3, -0.49, 24.1),
    ('st_break4h', 'SHORT', 'ВСЕ'): (560, 32.3, -1.28, 11.8),
    ('st_break4h', 'SHORT', 'нейтраль'): (63, 33.3, -0.348, 19.0),
    ('st_break4h', 'SHORT', 'по фазе'): (490, 31.8, -1.436, 10.8),
    ('st_daily', 'LONG', 'ВСЕ'): (7406, 40.5, 0.023, 20.5),
    ('st_daily', 'LONG', 'нейтраль'): (4080, 44.5, 0.358, 20.8),
    ('st_daily', 'LONG', 'по фазе'): (464, 37.5, 0.151, 23.1),
    ('st_daily', 'LONG', 'против'): (2862, 35.2, -0.475, 19.8),
    ('st_daily', 'SHORT', 'ВСЕ'): (3154, 47.4, 0.763, 24.6),
    ('st_daily', 'SHORT', 'нейтраль'): (538, 40.7, 0.328, 26.8),
    ('st_daily', 'SHORT', 'по фазе'): (2219, 49.6, 0.983, 25.5),
    ('st_daily', 'SHORT', 'против'): (397, 44.6, 0.12, 16.9),
    ('st_mtf', 'LONG', 'ВСЕ'): (5480, 41.4, 0.621, 28.5),
    ('st_mtf', 'LONG', 'нейтраль'): (2435, 45.3, 0.915, 28.2),
    ('st_mtf', 'LONG', 'по фазе'): (861, 43.9, 1.015, 29.5),
    ('st_mtf', 'LONG', 'против'): (2184, 35.9, 0.138, 28.5),
    ('st_mtf', 'SHORT', 'ВСЕ'): (4234, 40.7, 0.305, 25.2),
    ('st_mtf', 'SHORT', 'нейтраль'): (843, 31.1, -1.045, 17.8),
    ('st_mtf', 'SHORT', 'по фазе'): (2961, 43.3, 0.745, 28.3),
    ('st_mtf', 'SHORT', 'против'): (430, 41.9, -0.08, 17.9),
    ('st_vip', 'LONG', 'ВСЕ'): (1940, 37.8, -0.343, 16.9),
    ('st_vip', 'LONG', 'нейтраль'): (1054, 41.6, -0.132, 15.0),
    ('st_vip', 'LONG', 'по фазе'): (325, 37.5, -0.092, 20.0),
    ('st_vip', 'LONG', 'против'): (561, 30.8, -0.883, 18.5),
    ('st_vip', 'SHORT', 'ВСЕ'): (1897, 41.9, 0.164, 20.5),
    ('st_vip', 'SHORT', 'нейтраль'): (245, 34.3, -0.876, 17.6),
    ('st_vip', 'SHORT', 'по фазе'): (1345, 48.7, 1.007, 23.9),
    ('st_vip', 'SHORT', 'против'): (307, 17.9, -2.699, 7.5),
    ('ten', 'LONG', 'ВСЕ'): (151, 37.1, -0.101, 27.2),
    ('ten', 'LONG', 'нейтраль'): (77, 40.3, 0.318, 28.6),
    ('ten', 'LONG', 'против'): (67, 37.3, -0.07, 28.4),
    ('triple_confluence', 'LONG', 'ВСЕ'): (579, 39.4, -0.173, 19.3),
    ('triple_confluence', 'LONG', 'нейтраль'): (295, 45.4, 0.214, 18.3),
    ('triple_confluence', 'LONG', 'по фазе'): (90, 38.9, -0.079, 21.1),
    ('triple_confluence', 'LONG', 'против'): (194, 30.4, -0.804, 20.1),
    ('triple_confluence', 'SHORT', 'ВСЕ'): (634, 47.6, 0.94, 24.6),
    ('triple_confluence', 'SHORT', 'нейтраль'): (83, 41.0, -0.4, 16.9),
    ('triple_confluence', 'SHORT', 'по фазе'): (438, 55.3, 2.064, 31.1),
    ('triple_confluence', 'SHORT', 'против'): (113, 23.0, -2.434, 5.3),
    ('verified', 'LONG', 'ВСЕ'): (250, 22.4, -1.724, 17.2),
    ('verified', 'LONG', 'нейтраль'): (105, 18.1, -2.459, 12.4),
    ('verified', 'LONG', 'по фазе'): (48, 29.2, -0.815, 18.8),
    ('verified', 'LONG', 'против'): (97, 23.7, -1.378, 21.6),
    ('verified', 'SHORT', 'ВСЕ'): (500, 43.0, 0.637, 27.8),
    ('verified', 'SHORT', 'нейтраль'): (92, 25.0, -1.87, 12.0),
    ('verified', 'SHORT', 'по фазе'): (404, 47.3, 1.234, 31.7),
    ('vol_accum', 'LONG', 'ВСЕ'): (1324, 36.6, -0.305, 21.7),
    ('vol_accum', 'LONG', 'нейтраль'): (643, 41.8, 0.234, 23.0),
    ('vol_accum', 'LONG', 'по фазе'): (103, 35.0, -0.597, 16.5),
    ('vol_accum', 'LONG', 'против'): (578, 31.0, -0.853, 21.1),
    ('vol_accum', 'SHORT', 'ВСЕ'): (994, 42.8, 0.506, 24.8),
    ('vol_accum', 'SHORT', 'нейтраль'): (166, 33.1, -0.754, 18.7),
    ('vol_accum', 'SHORT', 'по фазе'): (715, 45.7, 0.947, 27.8),
    ('vol_accum', 'SHORT', 'против'): (113, 38.1, -0.43, 15.0),
    ('volcano', 'LONG', 'ВСЕ'): (130, 29.2, -1.021, 19.2),
    ('volcano', 'LONG', 'нейтраль'): (52, 28.8, -1.02, 17.3),
    ('volcano', 'LONG', 'против'): (71, 31.0, -0.84, 21.1),
    ('volume_surge', 'LONG', 'ВСЕ'): (562, 39.9, 0.277, 25.6),
    ('volume_surge', 'LONG', 'нейтраль'): (242, 46.3, 0.752, 26.0),
    ('volume_surge', 'LONG', 'по фазе'): (49, 49.0, 1.236, 24.5),
    ('volume_surge', 'LONG', 'против'): (271, 32.5, -0.32, 25.5),
    ('volume_surge', 'SHORT', 'ВСЕ'): (369, 45.3, 0.963, 28.2),
    ('volume_surge', 'SHORT', 'нейтраль'): (49, 46.9, 0.858, 28.6),
    ('volume_surge', 'SHORT', 'по фазе'): (261, 48.7, 1.553, 32.2),
    ('volume_surge', 'SHORT', 'против'): (59, 28.8, -1.559, 10.2),
    ('whale', 'LONG', 'ВСЕ'): (699, 58.4, 2.78, 39.6),
    ('whale', 'LONG', 'нейтраль'): (417, 60.0, 2.823, 37.9),
    ('whale', 'LONG', 'по фазе'): (50, 56.0, 1.845, 28.0),
    ('whale', 'LONG', 'против'): (232, 56.0, 2.905, 45.3),
}

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


_nsub_cache = {"at": 0.0, "val": None}


def neutral_subphase():
    """Подфаза нейтрали (год-бэктест 248k входов, сетка 10/5/96ч):
      DOWN (⚪🔻 шортовая): возраст >=24ч ИЛИ breadth падает >2пп/12ч —
        шорты EV +0.95..+1.66, лонги −0.71..−1.77 (запрет)
      PLATEAU (⚪▫ плато): пришла из 🟢 — единственная ячейка с random-лонгом
        не в минусе (+0.10); обе стороны только с триггером
      UP (⚪🔺 разгон): пришла из 🔴, моложе 24ч, breadth не падает —
        лонги только с триггером (random −0.4), шорты без приоритета (+0.49)
    Возвращает (sub, age_h, slope12) или (None, None, None) если фаза
    не NEUTRAL / данных нет. Кэш 120с."""
    now = time.time()
    if now - _nsub_cache["at"] < 120 and _nsub_cache["val"] is not None:
        return _nsub_cache["val"]
    val = (None, None, None)
    try:
        from datetime import timedelta
        from database import _get_db, utcnow
        db = _get_db()
        flips = _phase_flips()
        if flips and flips[-1][1] == "NEUTRAL":
            ms = db.system.find_one({"_id": "market_side_state"}) or {}
            age_h = None
            if ms.get("raw_side") == "NEUTRAL" and ms.get("raw_since"):
                age_h = (utcnow() - ms["raw_since"]).total_seconds() / 3600
            prev = flips[-2][1] if len(flips) >= 2 else None
            slope12 = None
            try:
                pts = list(db.breadth_history.find(
                    {"at": {"$gte": utcnow() - timedelta(hours=13)}},
                    {"at": 1, "pct": 1}).sort("at", 1))
                if len(pts) >= 2:
                    slope12 = pts[-1]["pct"] - pts[0]["pct"]
            except Exception:
                pass
            if (age_h is not None and age_h >= 24) or (slope12 is not None and slope12 < -2):
                sub = "DOWN"
            elif prev == "LONG":
                sub = "PLATEAU"
            else:
                sub = "UP"
            val = (sub, round(age_h, 1) if age_h is not None else None,
                   round(slope12, 1) if slope12 is not None else None)
    except Exception:
        logger.debug("[neutral-sub] fail", exc_info=True)
    _nsub_cache.update(at=now, val=val)
    return val


_ctx_cache = {"at": 0.0, "by_sym": {}}


def _pair_context():
    """pair_context из 30-мин скана (кэш 120с): symbol -> {pctl7d, mom24, rsi4h}."""
    now = time.time()
    if now - _ctx_cache["at"] < 120 and _ctx_cache["by_sym"]:
        return _ctx_cache["by_sym"]
    try:
        from database import _get_db
        by_sym = {d["_id"]: d for d in _get_db().pair_context.find({})}
        if by_sym:
            _ctx_cache.update(at=now, by_sym=by_sym)
        return by_sym
    except Exception:
        logger.debug("[pro] pair_context load fail", exc_info=True)
        return _ctx_cache["by_sym"]


def annotate_pro(items: list, extra_ctx: dict = None) -> None:
    """🚦 Серверный светофор для журнала: каждый свежий сигнал (<48ч)
    появляется сразу с вердиктом ДА/МОЖНО/НЕТ — та же логика, что
    /api/entry-check (реплей 30д: ДА WR 43.1% vs НЕТ 37.3%).
    Слагаемые: фаза-гейт (жёсткое НЕТ против фазы, кроме 💣-реверсала) ·
    догон >20%/24ч (жёсткое НЕТ) · RS-сила 7д · RSI4h-зоны · триггер
    (грейд сигнала: 🟩 +3 / 🟨 +1; ST-флипы 1h/4h из pair_context).
    Контекст живой (30-мин скан) — сигналы старше 48ч не оцениваются."""
    ctx_by_sym = dict(_pair_context())
    if extra_ctx:
        ctx_by_sym.update(extra_ctx)
    flips = _phase_flips()
    phase = flips[-1][1] if flips else None
    now_ts = time.time()
    # лучший грейд + число подтверждений (та же монета/направление, 48ч)
    best_by_key, cnt_by_key = {}, {}
    for it in items:
        g = it.get("trade_grade")
        d = it.get("direction")
        at = it.get("at_ts") or 0
        if d and at and now_ts - at <= 48 * 3600 and it.get("source") not in _SKIP:
            sym = (it.get("symbol") or (it.get("pair") or "").replace("/", "")).upper()
            k = (sym, d)
            cnt_by_key[k] = cnt_by_key.get(k, 0) + 1
            if g and (k not in best_by_key or g < best_by_key[k]):
                best_by_key[k] = g
    for it in items:
        try:
            d = it.get("direction")
            src = it.get("source")
            # stack — синтетическая группа, но раз участники прогрейжены
            # (annotate_items), светофор ей тоже положен
            if d not in ("LONG", "SHORT") or (src in _SKIP and src != "stack"):
                continue
            at_ts = it.get("at_ts") or 0
            if not at_ts or now_ts - at_ts > 48 * 3600:
                continue
            sym = (it.get("symbol") or (it.get("pair") or "").replace("/", "")).upper()
            ctx = ctx_by_sym.get(sym)
            if not ctx or phase is None:
                continue
            want = 1 if d == "LONG" else -1
            st4f = bool(ctx.get("st4_flip")) and ctx.get("st4_trend") == want
            st1f = bool(ctx.get("st1_flip")) and ctx.get("st1_trend") == want
            st4_against = (ctx.get("st4_trend") == -want and not st4f)
            score, checks, hard_no = 0, [], None
            # фаза-гейт (⚪ — по подфазе: год 248k входов, сетка 10/5/96ч)
            nsub, nage, _nsl = (neutral_subphase() if phase == "NEUTRAL"
                                else (None, None, None))
            if d == "LONG":
                if phase == "SHORT":
                    if st4f:
                        score += 1
                        checks.append("· 🔴, но флип 4h ST вверх — реверсал (агрессивно)")
                    else:
                        hard_no = "фаза 🔴 — НЕ ЛОНГОВАТЬ"
                elif phase == "LONG":
                    score += 2; checks.append("✓ фаза 🟢 (+2)")
                elif nsub == "DOWN":
                    hard_no = "⚪🔻 шортовая нейтраль — НЕ ЛОНГОВАТЬ (EV −0.7..−1.8)"
                else:
                    score += 1
                    checks.append("· фаза ⚪🔺 разгон (+1)" if nsub == "UP"
                                  else "· фаза ⚪▫ плато (+1)")
            else:
                if phase == "LONG":
                    hard_no = "фаза 🟢 — НЕ ШОРТИТЬ"
                elif phase == "SHORT":
                    score += 2; checks.append("✓ фаза 🔴 (+2)")
                elif nsub == "DOWN":
                    if nage is not None and nage >= 48:
                        score += 2; checks.append("✓ ⚪🔻 48ч+ — усиленный шорт (EV +1.66) (+2)")
                    else:
                        score += 1; checks.append("✓ ⚪🔻 шортовая нейтраль (+1)")
                else:
                    score -= 1
                    checks.append("✗ шорт в ⚪🔺/▫ — без приоритета (−1)")
            # догон (в обе стороны: LONG после рывка вверх, SHORT после
            # слива вниз И против свежего отскока — кейс LAB 22.07)
            m24 = ctx.get("mom24")
            if m24 is not None:
                if d == "LONG" and m24 > 20:
                    hard_no = hard_no or f"догон +{m24:.0f}%/24ч"
                elif d == "SHORT" and m24 < -20:
                    hard_no = hard_no or f"догон вниз {m24:.0f}%/24ч"
                elif d == "SHORT" and m24 > 20:
                    hard_no = hard_no or f"шорт против рывка +{m24:.0f}%/24ч"
                elif abs(m24) > 12:
                    score -= 1; checks.append(f"✗ ход {m24:+.0f}%/24ч — поздно (−1)")
            # объём: зона 1.5-3× — худшая для обеих сторон (бэктест 8k ДА:
            # LONG −2.01%, SHORT −0.46% — «движение уже прошло на объёме»)
            vr_ = ctx.get("vol_ratio")
            if vr_ is not None and 1.5 <= vr_ < 3:
                score -= 1
                checks.append(f"✗ объём {vr_:.1f}× нормы — движение уже прошло (−1)")
            # RS-сила 7д
            p7 = ctx.get("pctl7d")
            if p7 is not None:
                if d == "LONG" and p7 >= 0.85:
                    score += 2; checks.append("✓ лидер силы 7д (+2)")
                elif d == "SHORT" and p7 <= 0.15:
                    score += 2; checks.append("✓ лидер слабости 7д (+2)")
                elif d == "LONG" and p7 <= 0.15:
                    score -= 1; checks.append("✗ лонг аутсайдера (−1)")
                elif d == "SHORT" and p7 >= 0.85:
                    score -= 1; checks.append("✗ шорт лидера силы (−1)")
            # RSI4h
            r4 = ctx.get("rsi4h")
            if r4 is not None:
                if d == "LONG":
                    if r4 <= 20:
                        score += 2; checks.append(f"✓ капитуляция RSI4h {r4:.0f} (+2)")
                    elif r4 >= 80:
                        score -= 2; checks.append(f"✗ перегрев RSI4h {r4:.0f} (−2)")
                    elif 50 <= r4 <= 70:
                        score += 1; checks.append(f"✓ RSI4h {r4:.0f} зона 50-70 (+1)")
                else:
                    if r4 >= 80 and phase == "SHORT":
                        score += 1; checks.append("✓ фейд перегрева в 🔴 (+1)")
                    elif r4 <= 20:
                        score -= 2; checks.append(f"✗ шорт в яму RSI4h {r4:.0f} (−2)")
                    elif 30 <= r4 <= 50:
                        score += 1; checks.append(f"✓ RSI4h {r4:.0f} шорт-зона (+1)")
            # триггер: грейд (свой или соседний) + ST-флипы
            bg = best_by_key.get((sym, d))
            if bg == "A":
                score += 3; checks.append("✓ 🟩-сигнал (+3)")
            elif bg == "B":
                score += 1; checks.append("✓ 🟨-сигнал (+1)")
            if st4f:
                score += 2; checks.append("✓ флип 4h ST (+2)")
            elif st1f:
                score += 1; checks.append("✓ флип 1h ST (+1)")
            if bg is None and not st4f and not st1f:
                score -= 2; checks.append("✗ нет триггера (−2)")
            if st4_against:
                score -= 1; checks.append("✗ 4h ST против (−1)")
            if hard_no:
                verdict = "НЕТ"
                checks.insert(0, f"🚫 {hard_no}")
            elif score >= 5:
                verdict = "ДА"
            elif score >= 2:
                verdict = "МОЖНО"
            else:
                verdict = "НЕТ"
            # ⭐ ОТБОРНЫЙ вход — профиль победителей (анатомия TP-сделок
            # 20.07 + год-валидация компонент): недельный лидер + живой ATR +
            # RSI в рабочей зоне + без догона + 3+ подтверждений + счёт 6+
            star = False
            if verdict == "ДА":
                r7 = ctx.get("ret7d")
                atr_ = ctx.get("atr_pct")
                n_sigs = cnt_by_key.get((sym, d), 0)
                if d == "LONG":
                    star = (r7 is not None and r7 > 10 and atr_ and atr_ > 1.5
                            and r4 is not None and 55 <= r4 <= 72
                            and m24 is not None and m24 < 10
                            and score >= 6 and n_sigs >= 3)
                else:
                    star = (r7 is not None and r7 < -10 and atr_ and atr_ > 1.5
                            and r4 is not None and 28 <= r4 <= 45
                            and m24 is not None and -10 < m24 < 10
                            and score >= 6 and n_sigs >= 3)
                if star:
                    checks.insert(0, (f"⭐ ОТБОРНЫЙ ВХОД: лидер 7д {r7:+.0f}% · "
                                      f"ATR {atr_:.1f}% (ходит широко) · RSI4h {r4:.0f} "
                                      f"(зона, не перегрев) · 24ч {m24:+.1f}% (без догона) · "
                                      f"{n_sigs} подтверждений · счёт +{score}. "
                                      f"Профиль вчерашних TP+10%: 3 из 3 дошли до цели"))
            it["pro_star"] = star
            it["pro_score"] = score if not hard_no else -99
            it["pro_verdict"] = verdict
            it["pro_checks"] = checks
        except Exception:
            continue
    # фолбэк: живой контекст недоступен (пара вне универсума 30-мин скана)
    # или сигнал старше 48ч — показываем вердикт, зафиксированный при
    # появлении сигнала (штамп-луп / бэкфилл 3 мес)
    _sv_score = {"ДА": 5, "МОЖНО": 2, "НЕТ": -3}
    for it in items:
        if it.get("pro_verdict"):
            continue
        sv = it.get("svetofor")
        if sv in _sv_score:
            it["pro_verdict"] = sv
            it["pro_score"] = _sv_score[sv]
            it["pro_star"] = bool(it.get("svetofor_star"))
            it["pro_checks"] = (["⭐ ОТБОРНЫЙ (зафиксирован при появлении): профиль победителей"]
                                if it.get("svetofor_star") else []) + \
                ["вердикт зафиксирован в момент появления сигнала"]


def svetofor_stamp_recent(hours: float = 2) -> int:
    """Проставляет svetofor/svetofor_score свежим сигналам без вердикта
    (4 коллекции, последние `hours` ч) — та же логика, что annotate_pro,
    но персистентно: галочки ✅ на графиках и история для бэктестов.
    Вызывается из watcher каждые ~10 мин (sync, в thread)."""
    from datetime import timedelta
    from database import _get_db, utcnow
    db = _get_db()
    since = utcnow() - timedelta(hours=hours)
    cand = []  # (coll, _id, src_key, direction, sym, at)
    for n_ in db.new_strategy_signals.find(
            {"created_at": {"$gte": since}, "svetofor": {"$exists": False}},
            {"strategy": 1, "pair": 1, "symbol": 1, "direction": 1, "created_at": 1}).limit(300):
        if n_.get("direction"):
            sym = (n_.get("symbol") or (n_.get("pair") or "").replace("/", "")).upper()
            cand.append(("new_strategy_signals", n_["_id"], n_.get("strategy"),
                         n_["direction"], sym, n_["created_at"]))
    for s_ in db.supertrend_signals.find(
            {"flip_at": {"$gte": since}, "svetofor": {"$exists": False}},
            {"tier": 1, "pair": 1, "direction": 1, "flip_at": 1}).limit(300):
        if s_.get("direction"):
            sym = (s_.get("pair") or "").replace("/", "").upper()
            cand.append(("supertrend_signals", s_["_id"],
                         f"st_{s_.get('tier') or 'daily'}", s_["direction"],
                         sym, s_["flip_at"]))
    for c_ in db.confluence.find(
            {"detected_at": {"$gte": since}, "svetofor": {"$exists": False}},
            {"pair": 1, "symbol": 1, "direction": 1, "detected_at": 1, "score": 1}).limit(300):
        if c_.get("direction"):
            sym = (c_.get("symbol") or (c_.get("pair") or "").replace("/", "")).upper()
            key = "confluence_5plus" if (c_.get("score") or 0) >= 5 else "confluence_lo"
            cand.append(("confluence", c_["_id"], key, c_["direction"], sym, c_["detected_at"]))
    for v_ in db.verified_signals.find(
            {"created_at": {"$gte": since}, "svetofor": {"$exists": False}},
            {"pair": 1, "pair_norm": 1, "direction": 1, "created_at": 1}).limit(300):
        if v_.get("direction"):
            sym = (v_.get("pair_norm") or (v_.get("pair") or "").replace("/", "")).upper()
            cand.append(("verified_signals", v_["_id"], "verified",
                         v_["direction"], sym, v_["created_at"]))
    if not cand:
        return 0
    # пары вне универсума 30-мин скана (новые листинги, мелкий объём):
    # досчитываем контекст свечами на лету, до 8 пар за проход
    known = _pair_context()
    missing = [s for s in {c[4] for c in cand} if s not in known][:8]
    extra_ctx = {}
    for msym in missing:
        try:
            from exchange import get_klines_any
            from accum_detector import _rsi4h_value
            from backtest_supertrend import compute_st_series
            pair_slash = msym[:-4] + "/USDT"
            kd = get_klines_any(pair_slash, "1h", 320)
            if not kd or len(kd) < 60:
                continue
            closes = [x["c"] for x in kd]
            mom24 = (closes[-1] / closes[-25] - 1) * 100 if len(closes) > 25 else 0
            rv = _rsi4h_value(kd)
            s1 = compute_st_series(kd, 10, 3.0)
            st1t = s1[-1]["trend"] if s1 else None
            st1f = (len(s1) >= 6 and any(
                s1[j]["trend"] != s1[j-1]["trend"] and s1[j-1]["trend"]
                for j in range(len(s1) - 4, len(s1))))
            b4 = {}
            for x in kd:
                k_ = x["t"] // (4 * 3600_000)
                if k_ not in b4:
                    b4[k_] = dict(x)
                else:
                    b4[k_]["h"] = max(b4[k_]["h"], x["h"])
                    b4[k_]["l"] = min(b4[k_]["l"], x["l"])
                    b4[k_]["c"] = x["c"]
            kd4 = [b4[k_] for k_ in sorted(b4)][:-1]
            s4 = compute_st_series(kd4, 10, 3.0)
            extra_ctx[msym] = {
                "mom24": round(mom24, 2), "pctl7d": None,
                "rsi4h": round(rv[0], 1) if rv else None,
                "st1_trend": st1t, "st1_flip": bool(st1f),
                "st4_trend": s4[-1]["trend"] if len(s4) >= 3 else None,
                "st4_flip": (len(s4) >= 3 and s4[-1]["trend"] != s4[-2]["trend"]
                             and bool(s4[-2]["trend"])),
            }
        except Exception:
            logger.debug(f"[svetofor-stamp] ctx {msym} fail", exc_info=True)
    # аннотируем через ту же annotate_pro (единый скоринг с колонкой 🚦)
    items = []
    for coll, _id, src, d, sym, at in cand:
        pseudo_src = ("supertrend" if coll == "supertrend_signals" else
                      "confluence" if coll == "confluence" else
                      "verified" if coll == "verified_signals" else src)
        items.append({"source": pseudo_src, "st_tier": src[3:] if src.startswith("st_") else None,
                      "score": 5 if src == "confluence_5plus" else 0,
                      "strategy": src, "direction": d, "symbol": sym,
                      "pair": sym[:-4] + "/USDT" if sym.endswith("USDT") else sym,
                      "at_ts": int(at.timestamp())})
    annotate_items(items)
    annotate_pro(items, extra_ctx=extra_ctx)
    from pymongo import UpdateOne
    ops = {}
    n_done = 0
    for (coll, _id, src, d, sym, at), it in zip(cand, items):
        v = it.get("pro_verdict")
        if not v:
            continue
        ops.setdefault(coll, []).append(UpdateOne(
            {"_id": _id},
            {"$set": {"svetofor": v, "svetofor_score": it.get("pro_score", 0),
                      "svetofor_star": bool(it.get("pro_star"))}}))
    for coll, o in ops.items():
        try:
            r = db[coll].bulk_write(o, ordered=False)
            n_done += r.modified_count
        except Exception:
            logger.debug(f"[svetofor-stamp] bulk {coll} fail", exc_info=True)
    return n_done


def svetofor_star_upgrade(hours: float = 6) -> int:
    """⭐ Дозапись звёзд: штамп ставится через ~10 мин после рождения
    сигнала, когда кластер подтверждений ещё из 1-2 — звезда не фиксируется,
    хотя позже кластер дорастает до 3+ и лента её показывает (22.07: в БД
    7 звёзд, в ленте 28). Пересматривает свежие ДА без звезды и апгрейдит
    False→True (даунгрейда нет — заработанная звезда остаётся)."""
    from datetime import timedelta
    from database import _get_db, utcnow
    from pymongo import UpdateOne
    db = _get_db()
    since = utcnow() - timedelta(hours=hours)
    cand = []  # (coll, _id, src_key, direction, sym, at)
    for n_ in db.new_strategy_signals.find(
            {"created_at": {"$gte": since}, "svetofor": "ДА",
             "svetofor_star": {"$ne": True}},
            {"strategy": 1, "pair": 1, "symbol": 1, "direction": 1, "created_at": 1}).limit(200):
        sym = (n_.get("symbol") or (n_.get("pair") or "").replace("/", "")).upper()
        cand.append(("new_strategy_signals", n_["_id"], n_.get("strategy"),
                     n_["direction"], sym, n_["created_at"]))
    for s_ in db.supertrend_signals.find(
            {"flip_at": {"$gte": since}, "svetofor": "ДА",
             "svetofor_star": {"$ne": True}},
            {"tier": 1, "pair": 1, "direction": 1, "flip_at": 1}).limit(200):
        sym = (s_.get("pair") or "").replace("/", "").upper()
        cand.append(("supertrend_signals", s_["_id"],
                     f"st_{s_.get('tier') or 'daily'}", s_["direction"], sym, s_["flip_at"]))
    for c_ in db.confluence.find(
            {"detected_at": {"$gte": since}, "svetofor": "ДА",
             "svetofor_star": {"$ne": True}},
            {"pair": 1, "symbol": 1, "direction": 1, "detected_at": 1, "score": 1}).limit(200):
        sym = (c_.get("symbol") or (c_.get("pair") or "").replace("/", "")).upper()
        key = "confluence_5plus" if (c_.get("score") or 0) >= 5 else "confluence_lo"
        cand.append(("confluence", c_["_id"], key, c_["direction"], sym, c_["detected_at"]))
    for v_ in db.verified_signals.find(
            {"created_at": {"$gte": since}, "svetofor": "ДА",
             "svetofor_star": {"$ne": True}},
            {"pair": 1, "pair_norm": 1, "direction": 1, "created_at": 1}).limit(200):
        sym = (v_.get("pair_norm") or (v_.get("pair") or "").replace("/", "")).upper()
        cand.append(("verified_signals", v_["_id"], "verified", v_["direction"], sym, v_["created_at"]))
    if not cand:
        return 0
    # соседи кандидатов за 48ч — чтобы cnt_by_key считался честно
    syms = sorted({c[4] for c in cand})
    nb_since = utcnow() - timedelta(hours=48)
    nb_items = []
    pairs_slash = [s[:-4] + "/USDT" for s in syms if s.endswith("USDT")]
    for n_ in db.new_strategy_signals.find(
            {"created_at": {"$gte": nb_since},
             "$or": [{"symbol": {"$in": syms}}, {"pair": {"$in": pairs_slash}}]},
            {"strategy": 1, "symbol": 1, "pair": 1, "direction": 1, "created_at": 1}).limit(2000):
        sym = (n_.get("symbol") or (n_.get("pair") or "").replace("/", "")).upper()
        nb_items.append({"source": n_.get("strategy"), "strategy": n_.get("strategy"),
                         "direction": n_.get("direction"), "symbol": sym,
                         "pair": sym[:-4] + "/USDT",
                         "at_ts": int(n_["created_at"].timestamp())})
    for s_ in db.supertrend_signals.find(
            {"flip_at": {"$gte": nb_since}, "pair_norm": {"$in": syms}},
            {"tier": 1, "pair_norm": 1, "direction": 1, "flip_at": 1}).limit(2000):
        sym = s_.get("pair_norm")
        nb_items.append({"source": "supertrend", "st_tier": s_.get("tier"),
                         "direction": s_.get("direction"), "symbol": sym,
                         "pair": sym[:-4] + "/USDT",
                         "at_ts": int(s_["flip_at"].timestamp())})
    # кандидаты как псевдо-items (для annotate)
    c_items = []
    for coll, _id, src, d, sym, at in cand:
        pseudo_src = ("supertrend" if coll == "supertrend_signals" else
                      "confluence" if coll == "confluence" else
                      "verified" if coll == "verified_signals" else src)
        c_items.append({"source": pseudo_src,
                        "st_tier": src[3:] if src.startswith("st_") else None,
                        "score": 5 if src == "confluence_5plus" else 0,
                        "strategy": src, "direction": d, "symbol": sym,
                        "pair": sym[:-4] + "/USDT" if sym.endswith("USDT") else sym,
                        "at_ts": int(at.timestamp())})
    all_items = c_items + nb_items
    annotate_items(all_items)
    annotate_pro(all_items)
    ops = {}
    n_up = 0
    for (coll, _id, src, d, sym, at), it in zip(cand, c_items):
        if it.get("pro_star"):
            ops.setdefault(coll, []).append(UpdateOne(
                {"_id": _id}, {"$set": {"svetofor_star": True}}))
    for coll, o in ops.items():
        try:
            r = db[coll].bulk_write(o, ordered=False)
            n_up += r.modified_count
        except Exception:
            logger.debug(f"[star-upgrade] bulk {coll} fail", exc_info=True)
    return n_up


def annotate_items(items: list) -> None:
    """Проставляет trade_grade/trade_ev/trade_wr/trade_n/trade_hit10/
    trade_slice/trade_phase каждому item журнала (in-place)."""
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
            n, wr, ev, hit10 = row
            it["trade_grade"] = "A" if ev >= 0.75 else "B" if ev >= 0 else "C"
            it["trade_ev"] = ev
            it["trade_wr"] = wr
            it["trade_n"] = n
            it["trade_hit10"] = hit10
            it["trade_slice"] = used
            it["trade_phase"] = ph
        except Exception:
            continue
    # 🧩 STACK-группы: грейд по ЛУЧШЕМУ участнику (по EV его среза).
    # Сам stack — синтетическая строка, но участники — реальные сигналы.
    for it in items:
        try:
            if it.get("source") != "stack" or it.get("trade_grade"):
                continue
            d = it.get("direction")
            members = it.get("stack_members") or []
            if d not in ("LONG", "SHORT") or not members:
                continue
            ph = _phase_at((it.get("at_ts") or 0) * 1000, flips) if it.get("at_ts") else None
            sl = ("по фазе" if (d == ph) else
                  "против" if (ph in ("LONG", "SHORT") and d != ph) else
                  "нейтраль" if ph == "NEUTRAL" else None)
            best = None
            for m in members:
                msrc = m.get("source")
                if not msrc or msrc in _SKIP:
                    continue
                if msrc == "confluence":
                    key = ("confluence_5plus" if (m.get("score") or 0) >= 5
                           else "confluence_lo")
                elif msrc == "supertrend":
                    mp = m.get("pattern") or ""
                    key = ("st_vip" if "🏆" in mp else
                           "st_mtf" if "🔱" in mp else "st_daily")
                else:
                    key = msrc
                row = EDGE.get((key, d, sl)) if sl else None
                used = sl
                if row is None or row[0] < 30:
                    row = EDGE.get((key, d, "ВСЕ"))
                    used = "ВСЕ"
                if row and (best is None or row[2] > best[0][2]):
                    best = (row, key, used)
            if best:
                row, key, used = best
                n, wr, ev, hit10 = row
                it["trade_grade"] = "A" if ev >= 0.75 else "B" if ev >= 0 else "C"
                it["trade_ev"] = ev
                it["trade_wr"] = wr
                it["trade_n"] = n
                it["trade_hit10"] = hit10
                it["trade_slice"] = f"{used} · лучший в группе: {key}"
                it["trade_phase"] = ph
        except Exception:
            continue
