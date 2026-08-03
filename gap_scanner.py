# -*- coding: utf-8 -*-
"""🌉 Воскресный гэп-сканер FOREX (2026-08-03).
Единственная FX-модель, пережившая все стенды (M1 Dukascopy, год):
уикенд-гэп на КРОССАХ. Фейд с 30-й минуты: EV_R +0.41, WR 67%;
тест края (лимитка после отката >=30%): EV_R +0.61..+0.80.
Металлы и EURUSD/USDJPY в минусе — исключены. Подробности:
research bt_forex_m1_v2 / bt_forex_gaps_final.

Расписание (UTC): вс 21:26 скан (открытие FX-недели 21:00 летом;
зимой рынок открывается 22:00 — авторетрай через час), пн 21:15 итоги.
Данные Yahoo v8 (котировки =X живые с открытия недели).
Карточки в TG (BOT16 -> WHALE_CHAT_ID), журнал в Mongo gap_signals,
пульс hb 'gap_scan' каждые <=30 мин."""
from __future__ import annotations
import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

PAIRS = ["EURGBP", "EURJPY", "GBPJPY", "AUDJPY", "EURAUD",
         "EURCHF", "GBPCHF", "NZDUSD", "USDCAD"]
GAP_TH_V = 0.15          # порог |гэп| в долях дневной волы
GAP_TH_ABS = 0.02        # и не меньше 0.02% абсолютом
SL_K = 0.5               # SL = 0.5×гэп
_H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def _utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _hb():
    try:
        from database import _get_db
        _get_db().heartbeats.update_one(
            {"_id": "gap_scan"}, {"$set": {"at": _utcnow()}}, upsert=True)
    except Exception:
        pass


def _send_tg(txt: str):
    try:
        from config import BOT16_BOT_TOKEN, WHALE_CHAT_ID
        if BOT16_BOT_TOKEN and WHALE_CHAT_ID:
            import requests
            requests.post(
                f"https://api.telegram.org/bot{BOT16_BOT_TOKEN}/sendMessage",
                data={"chat_id": WHALE_CHAT_ID, "text": txt},
                timeout=15)
    except Exception:
        logger.warning("[gap] tg send fail", exc_info=True)


def _yahoo(sym: str, interval: str, range_: str):
    """(ts[], o[], h[], l[], c[]) списками; None при сбое."""
    import requests
    try:
        r = requests.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}=X",
            params={"interval": interval, "range": range_},
            headers=_H, timeout=20)
        j = r.json()["chart"]["result"][0]
        ts = j["timestamp"]
        q = j["indicators"]["quote"][0]
        rows = []
        for i in range(len(ts)):
            o, h, l, c = (q["open"][i], q["high"][i],
                          q["low"][i], q["close"][i])
            if None in (o, h, l, c):
                continue
            rows.append((int(ts[i]), float(o), float(h), float(l), float(c)))
        return rows
    except Exception:
        logger.warning(f"[gap] yahoo fail {sym} {interval}", exc_info=True)
        return None


def _week_context(sym: str):
    """Из часовиков 10д: (fri_close, week_open_ts, v_pct) либо None.
    Разрыв таймстампов >8ч = уикенд."""
    rows = _yahoo(sym, "1h", "10d")
    if not rows or len(rows) < 48:
        return None
    brk = None
    for i in range(len(rows) - 1, 0, -1):
        if rows[i][0] - rows[i - 1][0] > 8 * 3600:
            brk = i
            break
    if brk is None:
        return None
    fri_close = rows[brk - 1][4]
    week_open_ts = rows[brk][0]
    # дневная вола: медиана диапазонов fx-суток (сдвиг 21:00)
    byday = {}
    for ts, o, h, l, c in rows[:brk]:
        d = (ts - 75600) // 86400
        e = byday.setdefault(d, [h, l, c])
        e[0] = max(e[0], h); e[1] = min(e[1], l); e[2] = c
    rngs = [(hi - lo) / cl * 100 for hi, lo, cl in byday.values()
            if cl > 0 and hi > lo]
    if len(rngs) < 4:
        return None
    rngs.sort()
    v = rngs[len(rngs) // 2]
    return fri_close, week_open_ts, v


def _scan_sync() -> tuple[str | None, bool]:
    """Скан всех пар. Возвращает (текст карточки | None, need_retry).
    need_retry=True когда воскресные котировки ещё не идут (зима:
    открытие 22:00 UTC)."""
    from database import _get_db
    db = _get_db()
    lines = []
    n_sig = 0
    n_fresh = 0
    today = _utcnow().strftime("%Y%m%d")
    for sym in PAIRS:
        ctx = _week_context(sym)
        if not ctx:
            lines.append(f"{sym}: нет данных")
            continue
        fri_close, week_open_ts, v = ctx
        m1 = _yahoo(sym, "1m", "2d") or []
        sun = [r for r in m1 if r[0] >= week_open_ts]
        if not sun:
            continue
        n_fresh += 1
        sun_open = sun[0][1]
        cur = sun[-1][4]
        gap = (sun_open / fri_close - 1) * 100
        gap_r = abs(gap) / v if v > 0 else 0
        if abs(gap) < max(GAP_TH_V * v, GAP_TH_ABS):
            lines.append(f"{sym} гэп {gap:+.3f}% ({gap_r:.2f}v) — мелкий")
            continue
        short = gap > 0
        # закрыт ли гэп уже
        touched = any((r[3] <= fri_close) if short else (r[2] >= fri_close)
                      for r in sun)
        if touched:
            lines.append(f"{sym} гэп {gap:+.3f}% ({gap_r:.2f}v) — уже закрыт")
            continue
        sl = cur * (1 + SL_K * abs(gap) / 100 * (1 if short else -1))
        sl_pct = abs(sl / cur - 1) * 100
        tp_pct = abs(fri_close / cur - 1) * 100
        rr = tp_pct / sl_pct if sl_pct > 0 else 0
        side = "SHORT" if short else "LONG"
        arrow = "⬆️" if short else "⬇️"
        d = 5 if cur < 3 else (3 if cur < 100 else 2)
        lines.append(
            f"{sym} гэп {gap:+.3f}% ({gap_r:.2f}v) {arrow} НЕ ЗАКРЫТ\n"
            f"  ФЕЙД {side} @{cur:.{d}f}\n"
            f"  TP {fri_close:.{d}f} (закр. пт) · SL {sl:.{d}f} · RR {rr:.1f}\n"
            f"  Край {sun_open:.{d}f} — лимитка {side} после отката ≥30% к TP")
        n_sig += 1
        try:
            db.gap_signals.update_one(
                {"_id": f"{sym}:{today}"},
                {"$set": {"symbol": sym, "at": _utcnow(), "side": side,
                          "gap_pct": round(gap, 4), "v_pct": round(v, 3),
                          "fri_close": fri_close, "sun_open": sun_open,
                          "entry": cur, "sl": sl, "tp": fri_close,
                          "entry_ts": int(time.time()), "status": "OPEN"}},
                upsert=True)
        except Exception:
            logger.warning("[gap] mongo write fail", exc_info=True)
    if n_fresh == 0:
        return None, True          # рынок ещё не открылся (зимнее время)
    head = (f"🌉 УИКЕНД-ГЭП · {_utcnow().strftime('%d.%m %H:%M')} UTC\n"
            f"Порог {GAP_TH_V}×волы · вход = фейд к закрытию пятницы\n\n")
    tail = f"\nСигналов: {n_sig} из {len(PAIRS)} пар"
    return head + "\n".join(lines) + tail, False


def _outcomes_sync() -> str | None:
    """Пн 21:15: доиграть открытые сигналы по 5m-барам недели."""
    from database import _get_db
    db = _get_db()
    cut = _utcnow() - timedelta(days=2)
    docs = list(db.gap_signals.find({"status": "OPEN", "at": {"$gte": cut}}))
    if not docs:
        return None
    lines = []
    week_r = 0.0
    for doc in docs:
        sym = doc["symbol"]
        rows = _yahoo(sym, "5m", "5d") or []
        rows = [r for r in rows if r[0] > doc["entry_ts"]]
        if not rows:
            continue
        short = doc["side"] == "SHORT"
        e, tp, sl = doc["entry"], doc["tp"], doc["sl"]
        sl_pct = abs(sl / e - 1) * 100
        out, pnl = "CLOSE", None
        for ts, o, h, l, c in rows:
            if short:
                if h >= sl:
                    out, pnl = "SL", (sl / e - 1) * -100
                    break
                if l <= tp:
                    out, pnl = "TP", (tp / e - 1) * -100
                    break
            else:
                if l <= sl:
                    out, pnl = "SL", (sl / e - 1) * 100
                    break
                if h >= tp:
                    out, pnl = "TP", (tp / e - 1) * 100
                    break
        if pnl is None:
            last = rows[-1][4]
            pnl = (last / e - 1) * 100 * (-1 if short else 1)
        r_mult = pnl / sl_pct if sl_pct > 0 else 0
        week_r += r_mult
        emo = {"TP": "✅", "SL": "❌", "CLOSE": "⏱"}[out]
        lines.append(f"{emo} {sym} {doc['side']} гэп {doc['gap_pct']:+.2f}% "
                     f"→ {out} {pnl:+.2f}% ({r_mult:+.1f}R)")
        db.gap_signals.update_one(
            {"_id": doc["_id"]},
            {"$set": {"status": out, "pnl_pct": round(pnl, 3),
                      "r_mult": round(r_mult, 2), "closed_at": _utcnow()}})
    if not lines:
        return None
    return ("🌉 ИТОГИ ГЭПОВ НЕДЕЛИ\n\n" + "\n".join(lines)
            + f"\n\nНеделя: {week_r:+.1f}R")


def _next_at(weekday: int, hh: int, mm: int) -> datetime:
    """Ближайший момент weekday(пн=0)/hh:mm UTC строго в будущем."""
    now = _utcnow()
    d = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    delta = (weekday - d.weekday()) % 7
    d += timedelta(days=delta)
    if d <= now:
        d += timedelta(days=7)
    return d


async def gap_loop():
    """Планировщик: вс 21:26 скан (+ретрай через час зимой), пн 21:15
    итоги. Пульс каждые <=30 мин."""
    await asyncio.sleep(120)
    while True:
        try:
            ev_scan = _next_at(6, 21, 26)
            ev_out = _next_at(0, 21, 15)
            ev, kind = ((ev_scan, "scan") if ev_scan < ev_out
                        else (ev_out, "outcomes"))
            while True:
                left = (ev - _utcnow()).total_seconds()
                if left <= 0:
                    break
                _hb()
                await asyncio.sleep(min(1800, max(5, left)))
            _hb()
            if kind == "scan":
                txt, retry = await asyncio.to_thread(_scan_sync)
                if retry:
                    logger.info("[gap] рынок ещё закрыт — ретрай через час")
                    await asyncio.sleep(3600)
                    txt, _ = await asyncio.to_thread(_scan_sync)
                if txt:
                    _send_tg(txt)
                    logger.info("[gap] скан отправлен")
            else:
                txt = await asyncio.to_thread(_outcomes_sync)
                if txt:
                    _send_tg(txt)
                    logger.info("[gap] итоги отправлены")
            await asyncio.sleep(120)   # не дать событию сработать дважды
        except Exception:
            logger.exception("[gap] loop error")
            await asyncio.sleep(600)
