# -*- coding: utf-8 -*-
"""🧲 Ретест свечи смены структуры (MSO 4h) — вход от уровня смены.

MSO = LuxAlgo Market Structure Oscillator (реплика открытого Pine v5,
та же математика, что панель MSO на графиках и bt_mso_4h.py).

Бэктесты 13.09.26 (год, 308 пар):
  · после кросса MSO через 50 цена уходит ≥2% в сторону смены в 77-89%
    случаев и в 83-86% возвращается к close свечи смены (медиана ~24ч);
    сам ретест не уникален (бейзлайн любого бара ~85%) — ценность в
    ЦЕНЕ входа: уровень смены даёт вход на ≥2% лучше рынка;
  · сигнал+структура (15.3k живых сигналов): вход LONG в сторону СВЕЖЕЙ
    смены (на кроссе) +0.93%/вход против +0.46% при входе сразу — смена
    структуры ранняя, пока MSO не перегрет.

Событие: последний ЗАКРЫТЫЙ 4h-бар коснулся close свечи ПОСЛЕДНЕГО
кросса (low<=level<=high) ПОСЛЕ того, как цена уходила ≥2% в сторону
кросса. Направление = сторона смены. Окно цикла 42 бара (7д), MSO по
окну 300 баров — бэкфилл обязан использовать те же параметры (урок 🧗:
короткое окно даёт другие ряды)."""
from __future__ import annotations

import asyncio
import logging
import math

logger = logging.getLogger(__name__)

STABLE_BASES = {"USDC", "FDUSD", "TUSD", "DAI", "USD1", "USDP", "EURI",
                "AEUR", "XUSD", "PAXG", "XAUT", "WBTC", "BFUSD", "USDE",
                "BUSD", "EUR"}
TF = "4h"
TF_MS = 4 * 3600_000
WINDOW = 300          # баров 4h для расчёта MSO (лайв == бэкфилл == бэктест)
CYCLE_BARS = 42       # окно цикла смена→уход→ретест (7д)
ESC = 0.02            # уход ≥2% в сторону смены
NSMOOTH = 4
W1, W2, W3 = 1.0, 3.0, 2.0


def mso_series(candles: list[dict]) -> list[float]:
    """MSO 0..100 по закрытым барам; те же формулы, что bt_mso_4h.py
    и JS-панель MSO. candles: [{'o','h','l','c',...}]."""
    nan = float("nan")

    def mk_sw():
        return {"last": nan, "mid": nan, "prev": nan, "crossed": True}

    def pat(s, hi):
        if math.isnan(s["prev"]) or math.isnan(s["mid"]) or math.isnan(s["last"]):
            return False
        if hi:
            return s["prev"] < s["mid"] >= s["last"]
        return s["prev"] > s["mid"] <= s["last"]

    def upd(s, price):
        s["crossed"] = False
        s["prev"], s["mid"], s["last"] = s["mid"], s["last"], price

    def mk_n():
        return {"os": 0, "mx": nan, "mn": nan, "difs": []}

    def step(n, buy, sell, close):
        prev = n["os"]
        if buy:
            n["os"] = 1
        elif sell:
            n["os"] = -1
        if n["os"] > prev:
            n["mx"] = close
        elif not (n["os"] < prev):
            n["mx"] = close if math.isnan(n["mx"]) else max(close, n["mx"])
        if n["os"] < prev:
            n["mn"] = close
        elif not (n["os"] > prev):
            n["mn"] = close if math.isnan(n["mn"]) else min(close, n["mn"])
        rng = n["mx"] - n["mn"]
        dif = nan if (math.isnan(rng) or rng <= 0) else (close - n["mn"]) / rng
        n["difs"].append(dif)
        if len(n["difs"]) > NSMOOTH:
            n["difs"].pop(0)
        w = [x for x in n["difs"] if not math.isnan(x)]
        if len(w) < NSMOOTH:
            return nan
        return sum(w) / len(w) * 100.0

    stH, stL = mk_sw(), mk_sw()
    itH, itL = mk_sw(), mk_sw()
    ltH, ltL = mk_sw(), mk_sw()
    n1, n2, n3 = mk_n(), mk_n(), mk_n()
    p_it_h = p_it_l = p_lt_h = p_lt_l = False
    out = []
    for i, b in enumerate(candles):
        c, h, l = b["c"], b["h"], b["l"]
        bull = bear = False
        if i >= 2 and candles[i - 2]["h"] < candles[i - 1]["h"] >= h:
            upd(stH, candles[i - 1]["h"])
        if not stH["crossed"] and not math.isnan(stH["last"]) and c > stH["last"]:
            stH["crossed"] = True
            bull = True
        if i >= 2 and candles[i - 2]["l"] > candles[i - 1]["l"] <= l:
            upd(stL, candles[i - 1]["l"])
        if not stL["crossed"] and not math.isnan(stL["last"]) and c < stL["last"]:
            stL["crossed"] = True
            bear = True
        v1 = step(n1, bull, bear, c)
        bull = bear = False
        ph = pat(stH, True)
        if ph and not p_it_h:
            upd(itH, stH["mid"])
        p_it_h = ph
        if not itH["crossed"] and not math.isnan(itH["last"]) and c > itH["last"]:
            itH["crossed"] = True
            bull = True
        pl = pat(stL, False)
        if pl and not p_it_l:
            upd(itL, stL["mid"])
        p_it_l = pl
        if not itL["crossed"] and not math.isnan(itL["last"]) and c < itL["last"]:
            itL["crossed"] = True
            bear = True
        v2 = step(n2, bull, bear, c)
        bull = bear = False
        ph = pat(itH, True)
        if ph and not p_lt_h:
            upd(ltH, itH["mid"])
        p_lt_h = ph
        if not ltH["crossed"] and not math.isnan(ltH["last"]) and c > ltH["last"]:
            ltH["crossed"] = True
            bull = True
        pl = pat(itL, False)
        if pl and not p_lt_l:
            upd(ltL, itL["mid"])
        p_lt_l = pl
        if not ltL["crossed"] and not math.isnan(ltL["last"]) and c < ltL["last"]:
            ltL["crossed"] = True
            bear = True
        v3 = step(n3, bull, bear, c)
        num = den = 0.0
        for w, v in ((W1, v1), (W2, v2), (W3, v3)):
            if not math.isnan(v):
                num += w * v
                den += w
        out.append(num / den if den > 0 else nan)
    return out


def detect_retest(candles: list[dict], now_ms: float):
    """Ретест на последнем ЗАКРЫТОМ 4h-баре. Возвращает dict или None.

    Берётся только ПОСЛЕДНИЙ кросс (более свежая смена отменяет старую);
    если его ретест уже случился раньше или цикл не завершён — None."""
    if not candles or len(candles) < 120:
        return None
    c = candles[-WINDOW:]
    idx = len(c) - 1
    if c[idx]["t"] + TF_MS > now_ms + 60_000:
        idx -= 1          # [-1] ещё не закрыт
    if idx < 110:
        return None
    osc = mso_series(c[:idx + 1])
    # последний кросс в окне цикла
    cross_k = None
    sg = 0
    for k in range(idx, max(idx - CYCLE_BARS, 1), -1):
        p, q = osc[k - 1], osc[k]
        if math.isnan(p) or math.isnan(q):
            continue
        if p < 50 <= q:
            cross_k, sg = k, 1
            break
        if p > 50 >= q:
            cross_k, sg = k, -1
            break
    if cross_k is None or cross_k >= idx:
        return None
    level = c[cross_k]["c"]
    escaped = False
    esc_max = 0.0
    for j in range(cross_k + 1, idx + 1):
        hi, lo = c[j]["h"], c[j]["l"]
        if not escaped:
            if (sg > 0 and hi >= level * (1 + ESC)) or \
               (sg < 0 and lo <= level * (1 - ESC)):
                escaped = True
                esc_max = abs((hi if sg > 0 else lo) / level - 1) * 100
            continue
        esc_max = max(esc_max, abs((hi if sg > 0 else lo) / level - 1) * 100)
        if lo <= level <= hi:
            if j == idx:      # ретест завершился именно на последнем закрытом
                return {"sg": sg, "level": float(level),
                        "cross_t": int(c[cross_k]["t"]),
                        "bars_since": idx - cross_k,
                        "esc_max": round(esc_max, 2),
                        "mso_now": round(osc[idx], 1) if not math.isnan(osc[idx]) else None,
                        "bar": c[idx],
                        "bar_close_ms": c[idx]["t"] + TF_MS}
            return None       # ретест был раньше — событие уже отработано
    return None


async def _pair(pair_norm: str) -> bool:
    if pair_norm[:-4] in STABLE_BASES:
        return False
    from database import _get_db, utcnow
    db = _get_db()
    try:
        pc = db.pair_context.find_one({"_id": pair_norm}, {"vitality": 1})
        if pc and pc.get("vitality") == "dead":
            return False
    except Exception:
        pass
    from exchange import get_klines_any
    pair_slash = pair_norm[:-4] + "/USDT"
    try:
        c = await asyncio.to_thread(get_klines_any, pair_slash, TF, WINDOW)
    except Exception:
        return False
    now = utcnow()
    ev = detect_retest(c, now.timestamp() * 1000)
    if not ev:
        return False
    # свежесть: ретест-бар закрылся в последние 1.25 ТФ
    if now.timestamp() * 1000 - ev["bar_close_ms"] > 1.25 * TF_MS:
        return False
    sg, level = ev["sg"], ev["level"]
    direction = "LONG" if sg > 0 else "SHORT"
    # дедуп: один сигнал на конкретную смену (кросс)
    dup = db.new_strategy_signals.find_one({
        "strategy": "mso_retest", "symbol": pair_norm,
        "indicators.cross_t": ev["cross_t"]})
    if dup:
        return False
    sig = {
        "strategy": "mso_retest", "direction": direction,
        "pair": pair_slash, "symbol": pair_norm,
        "entry": level,
        "tp": level * (1 + sg * 0.10),
        "sl": level * (1 - sg * 0.05),
        "horizon_h": 96,
        "indicators": {"tf": TF, "level": round(level, 10),
                       "cross_t": ev["cross_t"],
                       "bars_since_cross": ev["bars_since"],
                       "esc_max_pct": ev["esc_max"],
                       "mso_now": ev["mso_now"],
                       "close": ev["bar"]["c"]},
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
            age_h = ev["bars_since"] * 4
            txt = (f"🧲 <b>РЕТЕСТ СМЕНЫ 4h · "
                   f"{pair_slash.replace('/USDT', '')}</b>\n"
                   f"{d_e} — цена вернулась к свече смены структуры\n"
                   f"смена (кросс MSO 50 {'вверх' if sg > 0 else 'вниз'}) "
                   f"{age_h}ч назад · уходила на {ev['esc_max']:.1f}% · "
                   f"MSO сейчас {ev['mso_now']}\n"
                   f"вход от уровня <b>{level:.6g}</b> "
                   f"(закрытие бара {ev['bar']['c']:.6g})\n"
                   f"<i>бэктест год: вход в сторону свежей смены — LONG "
                   f"+0.93%/вход против +0.46% сразу; ретест = тот же вход "
                   f"по цене лучше на ≥2%</i>")
            try:
                from setup_checker import signal_tg_context
                txt += await asyncio.to_thread(
                    signal_tg_context, pair_slash, direction)
            except Exception:
                pass
            await _bot16.send_message(WHALE_CHAT_ID, txt, parse_mode="HTML")
    except Exception:
        logger.debug(f"[mso-retest] tg fail {pair_norm}", exc_info=True)
    return True


async def check_all() -> int:
    """Скан всех tracked-пар. Вызывать после закрытия 4h-бара."""
    from supertrend_tracker import get_tracked_pairs
    pairs = await asyncio.to_thread(get_tracked_pairs)
    fired = 0
    for i, p in enumerate(pairs):
        try:
            if await _pair(p):
                fired += 1
        except Exception:
            logger.debug(f"[mso-retest] {p} fail", exc_info=True)
        if i % 20 == 19:
            await asyncio.sleep(0.5)
    logger.info(f"[mso-retest] 4h: {fired} сигналов")
    return fired
