# -*- coding: utf-8 -*-
"""🧿 Валидатор сигналов: «вход у уровня И против режима рынка».

Ретро-бэктест 16.09.26 (12 515 живых сигналов с исходами за 45д, единая
сетка TP+10/SL−5/96ч, bt_validator_v2/v3):
  ✅ у уровня (ST-линия 4h/12h ≤4%) И против режима:
     n=1814 · WR 52.9 · avgR +1.43 · половины +2.29/+0.51
  🗑 остальное: WR 38.7 · avgR −0.02 · режется ~86% потока
  Валидатор-v1 «по здравому смыслу» (по тренду/объём/пространство)
  разделял В ОБРАТНУЮ сторону (−0.15 vs +0.25) — похоронен.

«Против режима» — 4-е воспроизведение контрарианского закона платформы
(climate-разворот, MSO против структуры, ширина): LONG при узкой ширине
(<45% пар в 4h-аптренде EMA20/50), SHORT при широкой (>55%). Серая зона
45-55% — не contra.

Вердикт пишется в сигнал при store_signal (validator_ok/validator);
TG-отправители гейтят: False → журнал есть, телега молчит. Fail-open:
нет данных (ширина/линии) → ok=None → шлём как раньше."""
from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)

LEVEL_MAX_PCT = 4.0
BREADTH_LONG_MAX = 0.45
BREADTH_SHORT_MIN = 0.55
_breadth_cache = {"v": None, "ts": 0.0}


def market_breadth_ema4h():
    """Доля пар с 4h-аптрендом (EMA20>EMA50) из signal_trend_cache.
    None — данных мало (кэш прогревается). Кэш 10 мин."""
    now = time.time()
    if _breadth_cache["ts"] and now - _breadth_cache["ts"] < 600:
        return _breadth_cache["v"]
    v = None
    try:
        from database import _get_db
        col = _get_db().signal_trend_cache
        cutoff_ms = int((now - 8 * 3600) * 1000)
        latest = {}
        for d in col.find({"tf": "4h", "open_ms": {"$gte": cutoff_ms}},
                          {"pair": 1, "open_ms": 1, "trend": 1}):
            p = d.get("pair")
            if p and (p not in latest or d["open_ms"] > latest[p][0]):
                latest[p] = (d["open_ms"], d.get("trend"))
        up = sum(1 for _, tr in latest.values() if tr == "UP")
        dn = sum(1 for _, tr in latest.values() if tr == "DOWN")
        if up + dn >= 80:
            v = up / (up + dn)
    except Exception:
        logger.debug("[validator] breadth fail", exc_info=True)
    _breadth_cache["v"] = v
    _breadth_cache["ts"] = now
    return v


def validate(symbol: str, direction: str) -> dict:
    """Синхронный вердикт (вызывать из тредов, не из event loop).
    -> {"ok": True|False|None, "dist_pct", "breadth_pct", "reasons"}."""
    out = {"ok": None, "dist_pct": None, "breadth_pct": None, "reasons": []}
    try:
        from exchange import get_klines_any
        from backtest_supertrend import compute_st_series
        from database import utcnow
        pair = symbol[:-4] + "/USDT" if symbol.endswith("USDT") else symbol
        now_ms = utcnow().timestamp() * 1000
        dmin = None
        px = None
        for tf, tf_ms in (("4h", 14_400_000), ("12h", 43_200_000)):
            try:
                c = get_klines_any(pair, tf, 200)
            except Exception:
                c = None
            if not c or len(c) < 60:
                continue
            idx = len(c) - 1
            if c[idx]["t"] + tf_ms > now_ms + 60_000:
                idx -= 1
            st = compute_st_series(c, 10, 3.0)
            if not st or idx >= len(st):
                continue
            line = st[idx].get("st")
            if px is None:
                px = c[idx]["c"]
            if line and px:
                d = abs(px / float(line) - 1) * 100
                dmin = d if dmin is None else min(dmin, d)
        br = market_breadth_ema4h()
        out["dist_pct"] = round(dmin, 2) if dmin is not None else None
        out["breadth_pct"] = round(br * 100, 1) if br is not None else None
        if dmin is None or br is None:
            out["reasons"].append("нет данных (уровни/ширина) — fail-open")
            return out
        near = dmin <= LEVEL_MAX_PCT
        contra = ((br <= BREADTH_LONG_MAX) if direction == "LONG"
                  else (br >= BREADTH_SHORT_MIN))
        out["ok"] = bool(near and contra)
        out["reasons"].append(
            f"уровень {dmin:.1f}% {'✓' if near else '✗ (>4%)'}")
        out["reasons"].append(
            f"ширина {br * 100:.0f}% "
            + ("✓ против режима" if contra else "✗ по режиму/серая зона"))
    except Exception:
        logger.debug("[validator] fail %s", symbol, exc_info=True)
        out["reasons"].append("ошибка расчёта — fail-open")
    return out
