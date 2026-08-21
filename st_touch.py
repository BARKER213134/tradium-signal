# -*- coding: utf-8 -*-
"""🏓 Касание линии SuperTrend (4h/12h) — сигнал отбоя в сторону тренда.

Бэктест 20.08 (год, 308 пар, вход от линии, тейк +2%, выход по ЗАКРЫТИЮ
за линией, комиссия −0.1%, обе половины года в плюсе на всех разрезах):
  4h : LONG +0.61% WR61 · SHORT +0.85% WR71 · отбой ≥1% в 69/77%
  12h: LONG +0.74% WR74 · SHORT +0.90% WR82 · отбой ≥1% в 76/83%
  1h слабее (WR 45-49) — не детектим.
Стоп: НЕ тач-стоп вплотную за линией (фитили прокалывают линию — это и
есть механика касаний; тач-стоп режет EV почти вдвое на всех разрезах),
а ЗАКРЫТИЕ бара ТФ за линией (=флип).

Событие: последний ЗАКРЫТЫЙ бар ТФ дотянулся фитилём до линии ST(10,3)
при живом тренде, закрытие тренд не сломало. Дедуп 3 бара ТФ на
пару+ТФ. Окно расчёта 200 баров ТФ — бэкфилл обязан использовать то же
(урок 🧗: короткое окно даёт другие ряды ST)."""
from __future__ import annotations
import asyncio
import logging
from datetime import timedelta

logger = logging.getLogger(__name__)

STABLE_BASES = {"USDC", "FDUSD", "TUSD", "DAI", "USD1", "USDP", "EURI",
                "AEUR", "XUSD", "PAXG", "XAUT", "WBTC", "BFUSD", "USDE",
                "BUSD", "EUR"}
TF_H = {"4h": 4, "12h": 12}
WINDOW = 200          # баров ТФ для расчёта ST (и в лайве, и в бэкфилле)
DEDUP_BARS = 3        # как в бэктесте
EV_TXT = {("4h", "LONG"): "+0.61%/вход · WR 61% · отбой ≥1% в 69%",
          ("4h", "SHORT"): "+0.85%/вход · WR 71% · отбой ≥1% в 77%",
          ("12h", "LONG"): "+0.74%/вход · WR 74% · отбой ≥1% в 76%",
          ("12h", "SHORT"): "+0.90%/вход · WR 82% · отбой ≥1% в 83%"}


def detect_touch(candles: list[dict], tf: str, now_ms: float):
    """Касание на последнем ЗАКРЫТОМ баре. Возвращает dict или None.
    candles — родные бары ТФ (≥60), расчёт по окну WINDOW."""
    from backtest_supertrend import compute_st_series
    if not candles or len(candles) < 60:
        return None
    c = candles[-WINDOW:]
    st = compute_st_series(c, 10, 3.0)
    if len(st) != len(c) or len(st) < 30:
        return None
    tf_ms = TF_H[tf] * 3600_000
    idx = len(st) - 1
    if st[idx]["t"] + tf_ms > now_ms + 60_000:
        idx -= 1          # [-1] ещё не закрыт
    if idx < 25:
        return None
    cur, prev = st[idx], st[idx - 1]
    if not cur.get("trend") or cur["trend"] != prev.get("trend"):
        return None       # флип или прогрев — не касание
    line = cur.get("st")
    if not line:
        return None
    bar = c[idx]
    sg = int(cur["trend"])
    touched = (bar["l"] <= line) if sg > 0 else (bar["h"] >= line)
    if not touched:
        return None
    # 💪/⚠ сила линии (бэктест 21.08, 12.4k касаний 4h): прилипание
    # (3+ бара из 5 с экстремумом в 0.5% от линии) → пробой 41% против
    # базовых 29%; тихий подход (объём <0.8×ср20) → 36%; громкое
    # касание (≥1.5×) — крепчайшее, пробой 24%. Номер касания/возраст
    # тренда/глубина прокола НЕ предсказывают — не используем.
    stick = 0
    for k in range(max(0, idx - 5), idx):
        ref = c[k]["l"] if sg > 0 else c[k]["h"]
        if line and abs(ref / line - 1) <= 0.005:
            stick += 1
    _vols = [x.get("v") or 0 for x in c[max(0, idx - 20):idx]]
    volr = ((bar.get("v") or 0)
            / max(1e-9, sum(_vols) / max(1, len(_vols))))
    strength = ("weak" if (stick >= 3 or volr < 0.8)
                else ("strong" if volr >= 1.5 else None))
    return {"sg": sg, "line": float(line), "bar": bar,
            "bar_close_ms": st[idx]["t"] + tf_ms,
            "stick": stick, "volr": round(volr, 2), "strength": strength}


async def _pair(pair_norm: str, tf: str) -> bool:
    if pair_norm[:-4] in STABLE_BASES:
        return False
    from database import _get_db, utcnow
    db = _get_db()
    # 💀-гейт: мёртвые монеты не сигналим
    try:
        pc = db.pair_context.find_one({"_id": pair_norm}, {"vitality": 1})
        if pc and pc.get("vitality") == "dead":
            return False
    except Exception:
        pass
    from exchange import get_klines_any
    pair_slash = pair_norm[:-4] + "/USDT"
    try:
        c = await asyncio.to_thread(get_klines_any, pair_slash, tf, WINDOW)
    except Exception:
        return False
    now = utcnow()
    ev = detect_touch(c, tf, now.timestamp() * 1000)
    if not ev:
        return False
    # свежесть: бар закрылся в последние 1.25 ТФ (одно окно скана)
    tf_ms = TF_H[tf] * 3600_000
    if now.timestamp() * 1000 - ev["bar_close_ms"] > 1.25 * tf_ms:
        return False
    sg, line, bar = ev["sg"], ev["line"], ev["bar"]
    direction = "LONG" if sg > 0 else "SHORT"
    # дедуп 3 бара ТФ на пару+ТФ (кулдаун store_signal общий на
    # strategy+pair — короткий, чтобы 4h не глушил 12h)
    dup = db.new_strategy_signals.find_one({
        "strategy": "st_touch", "symbol": pair_norm, "indicators.tf": tf,
        "created_at": {"$gte": now - timedelta(hours=DEDUP_BARS * TF_H[tf])}})
    if dup:
        return False
    depth = abs((line - (bar["l"] if sg > 0 else bar["h"])) / line) * 100
    sig = {
        "strategy": "st_touch", "direction": direction,
        "pair": pair_slash, "symbol": pair_norm,
        "entry": line,
        "tp": line * (1 + sg * 0.02),
        "sl": line * (1 - sg * 0.02),
        "horizon_h": 30 * TF_H[tf],
        "indicators": {"tf": tf, "st_line": round(line, 10),
                       "close": bar["c"],
                       "touch_depth_pct": round(depth, 2),
                       "stick": ev.get("stick"), "volr": ev.get("volr"),
                       "line_strength": ev.get("strength")},
    }
    from impulse_detector import store_signal
    stored = await asyncio.to_thread(store_signal, sig, 1)
    if not stored:
        return False
    try:
        from watcher import _bot16
        from config import WHALE_CHAT_ID
        if _bot16 and WHALE_CHAT_ID:
            d_e = "🟢 LONG" if sg > 0 else "🔴 SHORT"
            # 💪/⚠ сила линии (бэктест 21.08)
            _st_line = ""
            if ev.get("strength") == "weak":
                _why = ("цена прилипла к линии "
                        f"({ev['stick']} из 5 баров)"
                        if ev.get("stick", 0) >= 3
                        else f"тихий подход (объём ×{ev.get('volr')})")
                _st_line = (f"⚠ <b>линия слабая</b>: {_why} — пробой "
                            f"36-41% против базовых 29%\n")
            elif ev.get("strength") == "strong":
                _st_line = (f"💪 <b>линия крепкая</b>: объёмный выкуп "
                            f"×{ev.get('volr')} — пробой лишь 24%\n")
            txt = (f"🏓 <b>КАСАНИЕ ST {tf} · "
                   f"{pair_slash.replace('/USDT', '')}</b>\n"
                   f"{d_e} — отбой от линии супертренда {tf}\n"
                   f"вход от линии <b>{line:.6g}</b> "
                   f"(закрытие бара {bar['c']:.6g}, прокол {depth:.2f}%)\n"
                   + _st_line +
                   f"тейк +2% · стоп: ЗАКРЫТИЕ {tf}-бара за линией "
                   f"(тач-стоп вплотную режет EV вдвое)\n"
                   f"<i>бэктест год: {EV_TXT[(tf, direction)]}</i>")
            try:
                from setup_checker import signal_tg_context
                txt += await asyncio.to_thread(
                    signal_tg_context, pair_slash, direction)
            except Exception:
                pass
            await _bot16.send_message(WHALE_CHAT_ID, txt, parse_mode="HTML")
    except Exception:
        logger.debug(f"[st-touch] tg fail {pair_norm}", exc_info=True)
    return True


async def check_all(tf: str) -> int:
    """Скан всех tracked-пар на касание ТФ. Вызывать после закрытия
    бара ТФ."""
    from supertrend_tracker import get_tracked_pairs
    pairs = await asyncio.to_thread(get_tracked_pairs)
    fired = 0
    for i, p in enumerate(pairs):
        try:
            if await _pair(p, tf):
                fired += 1
        except Exception:
            logger.debug(f"[st-touch] {p} {tf} fail", exc_info=True)
        if i % 20 == 19:
            await asyncio.sleep(0.5)
    logger.info(f"[st-touch] {tf}: {fired} сигналов")
    return fired
