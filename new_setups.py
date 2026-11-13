"""🆕 Два новых источника из годового поиска входов на +10% (13.11.26).

Юзер: «найди сигнал в лонг и шорт, который даёт минимум 10%» → «заведи».
Поиск по 1.17 млн 2h-баров (302 монеты, год): при стопе −5% высокого WR нет
(случайный бар доходит до +10% в 21%), но с ШИРОКИМ стопом два входа
устойчивы в обеих половинах года (горизонт 14 дней, +10% раньше −15%):

  range_bottom (LONG) «дно в диапазоне»: RSI(14) 2h опускался ниже 25 за
      последние сутки, монета 10 дней в диапазоне (дрейф канала ≤ 0.75
      ширины), цена на 40%+ ниже 30-дневного максимума.
      503 сигнала/год: WR 63%, +1.75% (половины +1.73 / +1.80).
  low_break (SHORT) «пробой 30-дневного минимума»: волатильная монета
      (ATR ≥ 3% цены), цена в пределах 3% от 30-дневного минимума.
      1494 сигнала/год: WR 64%, +2.13% (половины +2.33 / +1.81).
      Оговорка: на коротком горизонте 96ч вне выборки был в минусе.

Сигналы пишутся в academy_signals (журнал и Telegram их не видят): школа
учится на них по своему общему канону, книга ⚖ торгует по плану PLAN.
Считается из тех же 2h-свечей, что карта структуры (structure_engine).
"""
import logging
from datetime import timedelta

import numpy as np

logger = logging.getLogger(__name__)

# план сделки для книги: тейк, стоп, горизонт в часах
PLAN = {
    "range_bottom": {"dir": "LONG", "tp": 0.10, "sl": 0.15, "hz": 336},
    "low_break": {"dir": "SHORT", "tp": 0.10, "sl": 0.15, "hz": 336},
    # 13.11, поиск «80%» (отбор на первых 55% года, проверка на остальном):
    # бар с максимальным объёмом за 5 дней закрылся в нижних 10% суточного
    # диапазона → LONG +5% / −15% / 30 дней. Проверка: 624 сигнала, 97 дней,
    # WR 82.5%, +2.04% (любой бар того же периода: 77%, +0.78%).
    "vol_climax": {"dir": "LONG", "tp": 0.05, "sl": 0.15, "hz": 720},
}
DEDUP_H = 48        # один сигнал источника на монету за 48ч (как в поиске)
MIN_BARS = 375      # 360 баров (30 дней) + прогрев индикаторов


def _wilder(x, n=14):
    out = np.empty(len(x))
    a = x[0]
    for i, v in enumerate(x):
        a = v if i == 0 else (a * (n - 1) + v) / n
        out[i] = a
    return out


def detect(bars):
    """bars — ЗАКРЫТЫЕ 2h-бары (dict t/o/h/l/c) по возрастанию времени.
    → [(имя источника, показатели)] для последнего бара."""
    try:
        if not bars or len(bars) < MIN_BARS:
            return []
        import structure_engine as se
        c = np.array([b["c"] for b in bars], dtype=float)
        h = np.array([b["h"] for b in bars], dtype=float)
        l = np.array([b["l"] for b in bars], dtype=float)
        px = float(c[-1])
        if not px > 0:
            return []
        d = np.diff(c, prepend=c[0])
        up = _wilder(np.clip(d, 0, None))
        dn = _wilder(np.clip(-d, 0, None))
        rsi = 100 - 100 / (1 + up / np.where(dn > 0, dn, np.nan))
        pc = np.roll(c, 1)
        pc[0] = c[0]
        tr = np.maximum(h - l, np.maximum(np.abs(h - pc), np.abs(l - pc)))
        atrp = float(_wilder(tr)[-1] / px * 100)
        dd30 = px / float(h[-360:].max()) - 1
        up30 = px / float(l[-360:].min()) - 1
        rmin = float(np.nanmin(rsi[-12:]))
        ch = se.channel(c, h, l)
        out = []
        ind = {"rsi_min24h": round(rmin, 1), "dd30_pct": round(dd30 * 100, 1),
               "up30_pct": round(up30 * 100, 1), "atr_pct": round(atrp, 2),
               "ch_mv": (ch or {}).get("mv"), "tf": "2h"}
        if ch and abs(ch["mv"]) <= 0.75 and rmin < 25 and dd30 <= -0.40:
            out.append(("range_bottom", ind))
        if atrp >= 3.0 and up30 <= 0.03:
            out.append(("low_break", ind))
        try:
            v = np.array([b.get("v") or 0.0 for b in bars[-60:]], dtype=float)
            lo12 = float(l[-12:].min())
            hi12 = float(h[-12:].max())
            if v[-1] > 0 and v[-1] >= v.max() and hi12 > lo12:
                pos24 = (px - lo12) / (hi12 - lo12)
                if pos24 <= 0.10:
                    out.append(("vol_climax", dict(ind, pos24=round(pos24, 3),
                                                   vol_x=round(float(v[-1] / max(np.median(v), 1e-12)), 1))))
        except Exception:
            pass
        return out
    except Exception:
        logger.debug("[setups] detect fail", exc_info=True)
        return []


def store(db, sym, found, px, now):
    """Записать сигналы в academy_signals с дедупом. → число записанных."""
    n = 0
    for name, ind in found:
        try:
            p = PLAN[name]
            if db.academy_signals.find_one(
                    {"strategy": name, "symbol": sym,
                     "created_at": {"$gte": now - timedelta(hours=DEDUP_H)}}, {"_id": 1}):
                continue
            k = 1 if p["dir"] == "LONG" else -1
            db.academy_signals.insert_one({
                "strategy": name, "direction": p["dir"],
                "pair": sym[:-4] + "/USDT", "symbol": sym,
                "entry": px, "tp": px * (1 + k * p["tp"]), "sl": px * (1 - k * p["sl"]),
                "horizon_h": p["hz"], "state": "WAITING",
                "indicators": ind, "setup": True,
                "created_at": now, "updated_at": now})
            n += 1
        except Exception:
            logger.debug(f"[setups] store fail {sym} {name}", exc_info=True)
    return n
