# -*- coding: utf-8 -*-
"""🧲/🌡 MSO-сигналы: ретест свечи смены (2h SHORT) и снятие перегрева
(12h SHORT).

MSO = LuxAlgo Market Structure Oscillator (реплика открытого Pine v5,
та же математика, что панель MSO на графиках и bt_mso_4h.py).

Грид-бэктест 15.09.26 (год, 308 пар, 6 ТФ × 8 правил, эдж = avgR минус
бейзлайн случайного входа того же ТФ, исходы TP+10/SL−5/96ч):
  · MSO — шортовый индикатор: все LONG-правила на всех ТФ минусовые
    или в шуме → лонги не сигналим вовсе;
  · 🧲 retest_S 2h: avgR +0.38 · WR 43 · эдж +0.24 · половины +0.60/+0.15
    (первая версия на 4h имела эдж лишь +0.11 — удалена 15.09);
  · 🌡 obexit_S 12h (MSO был >=85 и кроссит вниз — перегрев кончился):
    avgR +0.49 · WR 41.7 · эдж +0.32 (лучшее правило грида) ·
    половины +0.10/+0.87.

Механика ретеста: на последнем ЗАКРЫТОМ 2h-баре цена коснулась close
свечи ПОСЛЕДНЕГО кросса MSO вниз (low<=level<=high) ПОСЛЕ ухода вниз
>=2%. Окно цикла 96 баров 2h (8д, как в гриде), MSO по окну 300 баров —
бэкфилл обязан использовать те же параметры (урок 🧗)."""
from __future__ import annotations

import asyncio
import logging
import math

logger = logging.getLogger(__name__)

STABLE_BASES = {"USDC", "FDUSD", "TUSD", "DAI", "USD1", "USDP", "EURI",
                "AEUR", "XUSD", "PAXG", "XAUT", "WBTC", "BFUSD", "USDE",
                "BUSD", "EUR"}
# 🧲 ретест: 2h
TF = "2h"
TF_MS = 2 * 3600_000
WINDOW = 300
CYCLE_BARS = 96       # окно цикла смена→уход→ретест (8д на 2h, как в гриде)
ESC = 0.02
# 🌡 перегрев: 12h
OB_TF = "12h"
OB_TF_MS = 12 * 3600_000
OB_LEVEL = 85.0
NSMOOTH = 4
W1, W2, W3 = 1.0, 3.0, 2.0


def mso_series(candles: list[dict]) -> list[float]:
    """MSO 0..100 по закрытым барам; формулы = bt_mso_4h.py = JS-панель."""
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


def _closed_idx(c: list[dict], tf_ms: int, now_ms: float) -> int:
    idx = len(c) - 1
    if c[idx]["t"] + tf_ms > now_ms + 60_000:
        idx -= 1
    return idx


def detect_retest(candles: list[dict], now_ms: float):
    """🧲 SHORT-ретест на последнем ЗАКРЫТОМ 2h-баре (только кросс ВНИЗ;
    более свежий кросс любой стороны отменяет старый уровень)."""
    if not candles or len(candles) < 120:
        return None
    c = candles[-WINDOW:]
    idx = _closed_idx(c, TF_MS, now_ms)
    if idx < 110:
        return None
    osc = mso_series(c[:idx + 1])
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
    if cross_k is None or cross_k >= idx or sg > 0:
        return None          # лонг-ретесты не сигналим (грид: минус на всех ТФ)
    level = c[cross_k]["c"]
    escaped = False
    esc_max = 0.0
    for j in range(cross_k + 1, idx + 1):
        hi, lo = c[j]["h"], c[j]["l"]
        if not escaped:
            if lo <= level * (1 - ESC):
                escaped = True
                esc_max = abs(lo / level - 1) * 100
            continue
        esc_max = max(esc_max, abs(lo / level - 1) * 100)
        if lo <= level <= hi:
            if j == idx:
                return {"sg": -1, "level": float(level),
                        "cross_t": int(c[cross_k]["t"]),
                        "bars_since": idx - cross_k,
                        "esc_max": round(esc_max, 2),
                        "mso_now": round(osc[idx], 1) if not math.isnan(osc[idx]) else None,
                        "bar": c[idx],
                        "bar_close_ms": c[idx]["t"] + TF_MS}
            return None
    return None


def detect_obexit(candles: list[dict], now_ms: float):
    """🌡 12h: MSO был >=85 и на последнем ЗАКРЫТОМ баре кроссит вниз."""
    if not candles or len(candles) < 120:
        return None
    c = candles[-WINDOW:]
    idx = _closed_idx(c, OB_TF_MS, now_ms)
    if idx < 110:
        return None
    osc = mso_series(c[:idx + 1])
    p, q = osc[idx - 1], osc[idx]
    if math.isnan(p) or math.isnan(q) or not (p >= OB_LEVEL > q):
        return None
    return {"bar": c[idx], "bar_close_ms": c[idx]["t"] + OB_TF_MS,
            "mso_prev": round(p, 1), "mso_now": round(q, 1)}


async def _pair_gate(pair_norm: str, db) -> bool:
    if pair_norm[:-4] in STABLE_BASES:
        return False
    try:
        pc = db.pair_context.find_one({"_id": pair_norm}, {"vitality": 1})
        if pc and pc.get("vitality") == "dead":
            return False
    except Exception:
        pass
    return True


async def _tg(txt: str) -> None:
    try:
        from watcher import _bot16
        from config import WHALE_CHAT_ID
        if _bot16 and WHALE_CHAT_ID:
            await _bot16.send_message(WHALE_CHAT_ID, txt, parse_mode="HTML")
    except Exception:
        logger.debug("[mso] tg fail", exc_info=True)


async def _pair_retest(pair_norm: str) -> bool:
    from database import _get_db, utcnow
    db = _get_db()
    if not await _pair_gate(pair_norm, db):
        return False
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
    if now.timestamp() * 1000 - ev["bar_close_ms"] > 1.25 * TF_MS:
        return False
    level = ev["level"]
    entry = ev["bar"]["c"]      # вход по close ретест-бара (как в гриде)
    dup = db.new_strategy_signals.find_one({
        "strategy": "mso_retest", "symbol": pair_norm,
        "indicators.cross_t": ev["cross_t"]})
    if dup:
        return False
    sig = {
        "strategy": "mso_retest", "direction": "SHORT",
        "pair": pair_slash, "symbol": pair_norm,
        "entry": entry,
        "tp": entry * 0.90,
        "sl": entry * 1.05,
        "horizon_h": 96,
        "indicators": {"tf": TF, "level": round(level, 10),
                       "cross_t": ev["cross_t"],
                       "bars_since_cross": ev["bars_since"],
                       "esc_max_pct": ev["esc_max"],
                       "mso_now": ev["mso_now"],
                       "close": entry},
    }
    from impulse_detector import store_signal
    stored = await asyncio.to_thread(store_signal, sig, 1)
    if not stored:
        return False
    age_h = ev["bars_since"] * 2
    txt = (f"🧲 <b>РЕТЕСТ СМЕНЫ 2h · {pair_slash.replace('/USDT', '')}</b>\n"
           f"🔴 SHORT — возврат к свече смены структуры вниз\n"
           f"смена (кросс MSO 50 вниз) {age_h}ч назад · "
           f"уходила на {ev['esc_max']:.1f}% вниз · MSO {ev['mso_now']}\n"
           f"уровень смены <b>{level:.6g}</b> · вход {entry:.6g}\n"
           f"<i>грид-бэктест год: +0.38%/вход · WR 43 · эдж +0.24 над "
           f"рынком · обе половины в плюсе (лонг-ретесты минус — не шлём)</i>")
    try:
        from setup_checker import signal_tg_context
        txt += await asyncio.to_thread(signal_tg_context, pair_slash, "SHORT")
    except Exception:
        pass
    await _tg(txt)
    return True


async def _pair_obexit(pair_norm: str) -> bool:
    from database import _get_db, utcnow
    db = _get_db()
    if not await _pair_gate(pair_norm, db):
        return False
    from exchange import get_klines_any
    pair_slash = pair_norm[:-4] + "/USDT"
    try:
        c = await asyncio.to_thread(get_klines_any, pair_slash, OB_TF, WINDOW)
    except Exception:
        return False
    now = utcnow()
    ev = detect_obexit(c, now.timestamp() * 1000)
    if not ev:
        return False
    if now.timestamp() * 1000 - ev["bar_close_ms"] > 1.25 * OB_TF_MS:
        return False
    entry = ev["bar"]["c"]
    bar_t = int(ev["bar"]["t"])
    dup = db.new_strategy_signals.find_one({
        "strategy": "mso_obexit", "symbol": pair_norm,
        "indicators.bar_t": bar_t})
    if dup:
        return False
    sig = {
        "strategy": "mso_obexit", "direction": "SHORT",
        "pair": pair_slash, "symbol": pair_norm,
        "entry": entry,
        "tp": entry * 0.90,
        "sl": entry * 1.05,
        "horizon_h": 96,
        "indicators": {"tf": OB_TF, "bar_t": bar_t,
                       "mso_prev": ev["mso_prev"], "mso_now": ev["mso_now"],
                       "close": entry},
    }
    from impulse_detector import store_signal
    stored = await asyncio.to_thread(store_signal, sig, 1)
    if not stored:
        return False
    txt = (f"🌡 <b>ПЕРЕГРЕВ СНЯТ 12h · {pair_slash.replace('/USDT', '')}</b>\n"
           f"🔴 SHORT — MSO вышел из перекупленности "
           f"({ev['mso_prev']} → {ev['mso_now']}, порог {OB_LEVEL:.0f})\n"
           f"вход {entry:.6g} по закрытию 12h-бара\n"
           f"<i>грид-бэктест год: +0.49%/вход · WR 42 · эдж +0.32 — "
           f"лучшее правило MSO · обе половины в плюсе</i>")
    try:
        from setup_checker import signal_tg_context
        txt += await asyncio.to_thread(signal_tg_context, pair_slash, "SHORT")
    except Exception:
        pass
    await _tg(txt)
    return True


async def check_all(kind: str = "retest") -> int:
    """Скан всех tracked-пар. kind: retest (после 2h-границ) |
    obexit (после 12h-границ)."""
    from supertrend_tracker import get_tracked_pairs
    pairs = await asyncio.to_thread(get_tracked_pairs)
    fn = _pair_retest if kind == "retest" else _pair_obexit
    fired = 0
    for i, p in enumerate(pairs):
        try:
            if await fn(p):
                fired += 1
        except Exception:
            logger.debug(f"[mso] {p} {kind} fail", exc_info=True)
        if i % 20 == 19:
            await asyncio.sleep(0.5)
    logger.info(f"[mso] {kind}: {fired} сигналов")
    return fired
