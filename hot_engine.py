# -*- coding: utf-8 -*-
"""🔥 Горячий список: монеты с завершённым ралли >=+40% за <=7 дней;
горячесть включается В МОМЕНТ достижения +40% (анти-lookahead) и живёт
30 дней. Хранение: Mongo hot_coins {_id=symbol, hot_from, hot_until,
gain_pct}. Детект — из 30-мин скана accum (400×1h баров хватает: любое
ралли внутри окна моложе 17 дней).

Бэктест 30.07 (реальные сигналы 60д): LONG на горячих EV +0.18 против
−0.21 на холодных; КОМБО: st_break4h LONG +2.73 (WR 54, n=113) ·
second_flip +0.85 (n=81) · impulse +0.91 (n=93) — в 2-8× выше обычных.
Кит на горячей — ЛОВУШКА: −1.08 против +0.05 (n=187).
Год-контроль: рандом-лонг на горячих ХУЖЕ рандома на холодных
(−0.50 против −0.22) — сама горячесть не сигнал, работает только
связка сигнал+горячая."""
from __future__ import annotations
import logging
import time

logger = logging.getLogger(__name__)

HOT_DAYS = 30
RALLY_PCT = 0.40
RALLY_WIN_H = 168
COMBO_STRATS = {"st_break4h", "second_flip", "impulse"}

_cache: dict[str, tuple[float, bool]] = {}   # sym -> (ts, hot)


def _norm(sym_or_pair: str) -> str:
    s = (sym_or_pair or "").replace("/", "").upper()
    return s if s.endswith("USDT") else s + "USDT"


def refresh_hot(pair: str, kd: list[dict]) -> None:
    """Ищет в kd (1h, ~400 баров) завершённые ралли +40%/<=7д и
    продлевает горячесть в hot_coins. Идемпотентно, зовётся из скана."""
    try:
        if not kd or len(kd) < 200:
            return
        sym = _norm(pair)
        n = len(kd)
        ls = [x["l"] for x in kd]
        hs = [x["h"] for x in kd]
        best = None   # (hot_from_ms, gain)
        i = 24
        while i < n - 2:
            w0, w1 = max(0, i - 24), min(n, i + 25)
            if ls[i] <= 0 or ls[i] > min(ls[w0:w1]) * 1.0001:
                i += 1
                continue
            base = ls[i]
            hit = None
            for j in range(i + 1, min(n, i + RALLY_WIN_H + 1)):
                if hs[j] >= base * (1 + RALLY_PCT):
                    hit = j
                    break
            if hit is None:
                i += 1
                continue
            peak = max(hs[i:min(n, i + RALLY_WIN_H + 1)])
            gain = (peak / base - 1) * 100
            if best is None or kd[hit]["t"] > best[0]:
                best = (kd[hit]["t"], gain)
            i = hit + 12
        if best is None:
            return
        from database import _get_db, utcnow
        from datetime import datetime, timedelta, timezone
        hot_from = datetime.fromtimestamp(best[0] / 1000, tz=timezone.utc) \
            .replace(tzinfo=None)
        hot_until = hot_from + timedelta(days=HOT_DAYS)
        if hot_until <= utcnow():
            return
        col = _get_db().hot_coins
        cur = col.find_one({"_id": sym})
        if cur and cur.get("hot_until") and cur["hot_until"] >= hot_until:
            return
        col.update_one({"_id": sym},
                       {"$set": {"pair": pair, "hot_from": hot_from,
                                 "hot_until": hot_until,
                                 "gain_pct": round(gain, 1),
                                 "updated_at": utcnow()}},
                       upsert=True)
        _cache.pop(sym, None)
    except Exception:
        logger.debug("[hot] refresh fail", exc_info=True)


def is_hot(sym_or_pair: str) -> bool:
    """Горячая ли монета сейчас (кэш 120с)."""
    try:
        sym = _norm(sym_or_pair)
        now = time.time()
        hit = _cache.get(sym)
        if hit and now - hit[0] < 120:
            return hit[1]
        from database import _get_db, utcnow
        d = _get_db().hot_coins.find_one({"_id": sym}, {"hot_until": 1})
        val = bool(d and d.get("hot_until") and d["hot_until"] > utcnow())
        _cache[sym] = (now, val)
        return val
    except Exception:
        return False


def combo_alert(sig: dict) -> None:
    """🔥 КОМБО в BOT16: st_break4h / second_flip / impulse LONG на
    горячей монете — концентрат для ручного выкупа (бэктест 30.07)."""
    try:
        if sig.get("strategy") not in COMBO_STRATS \
                or sig.get("direction") != "LONG":
            return
        from config import BOT16_BOT_TOKEN, WHALE_CHAT_ID
        if not (BOT16_BOT_TOKEN and WHALE_CHAT_ID):
            return
        stats = {"st_break4h": "EV +2.73/сделку · WR 54% (n=113)",
                 "second_flip": "EV +0.85 · WR 51% (n=81)",
                 "impulse": "EV +0.91 · WR 43% (n=93)"}
        pair = sig.get("pair") or sig.get("symbol") or "?"
        entry = sig.get("entry")
        txt = (f"🔥 <b>КОМБО · {sig['strategy']} LONG на ГОРЯЧЕЙ · "
               f"{str(pair).replace('/USDT', '').replace('USDT', '')}</b>\n")
        if entry:
            txt += f"🟢 LONG @ {entry:.6g}\n"
        txt += (f"монета из горячего списка (ралли +40% за ≤7д, окно 30д)\n"
                f"<i>бэктест 60д на горячих: {stats.get(sig['strategy'], '')} "
                f"— против EV +0.1..1.3 у обычных</i>")
        import requests
        requests.post(
            f"https://api.telegram.org/bot{BOT16_BOT_TOKEN}/sendMessage",
            data={"chat_id": WHALE_CHAT_ID, "parse_mode": "HTML", "text": txt},
            timeout=10)
    except Exception:
        logger.debug("[hot] combo tg fail", exc_info=True)
