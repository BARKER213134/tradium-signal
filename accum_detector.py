"""🧊 ACCUMULATION watchlist — детектор фаз накопления (research 2026-07-10).

НЕ торговый сигнал. Исследование (90д × 30m × 206 пар, 3 итерации:
пробой/объём/OBV/spring/ретест/базы 2-7 дней) показало: фаза накопления
распознаётся надёжно, но направленно-нейтральна — сторона разрешения не
предсказывается ни одним фильтром устойчиво (train/test разваливается).

Честное применение: watchlist. Монета стоит в базе -> наблюдаем, вход
берём по сигналам разрешения (💥 IGNITION / 💰 TEN стреляют на выходе).

Определение базы (1h-свечи, окно 48ч):
  • весь ход (hi-lo) за 48ч < 5% от цены
  • |ход за 24ч| < 2.5% (нет дрейфа)
  • состояние держится >= 12ч подряд
"""
from __future__ import annotations
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

W = 48            # окно базы, часов
RNG_MAX = 5.0     # ширина базы, %
ROC24_MAX = 2.5   # дрейф за сутки, %
MIN_HOURS = 12    # сколько часов состояние должно держаться
MAX_BACK = 240    # глубина поиска начала базы


_fapi_down_until = 0.0   # circuit breaker: fapi в бане — не жжём таймауты


def _fetch_klines_delta(symbol: str, limit: int = 320) -> Optional[list]:
    """1h свечи С тайкер-дельтой (поле 9 = taker buy volume). get_klines_any
    его отбрасывает, поэтому прямой REST: fapi (prod) -> Vision (локально).
    При отказе fapi (418-бан) — предохранитель на 10 мин, иначе скан 300 пар
    жёг по 10с таймаута на каждую и растягивался до ~50 мин (2026-07-13)."""
    global _fapi_down_until
    import requests
    sym = symbol.replace("/", "").upper()
    urls = ["https://fapi.binance.com/fapi/v1/klines",
            "https://data-api.binance.vision/api/v3/klines"]
    if time.time() < _fapi_down_until:
        urls = urls[1:]
    else:
        try:
            from fapi_budget import allow
            if not allow(tag='accum'):
                urls = urls[1:]   # бюджет исчерпан — сразу Vision
        except Exception:
            pass
    for url in urls:
        is_fapi = "fapi" in url
        try:
            r = requests.get(url, params=dict(symbol=sym, interval="1h",
                                              limit=limit), timeout=10)
            if r.status_code != 200:
                if is_fapi:
                    _fapi_down_until = time.time() + 600
                continue
            rows = r.json()
            if rows and len(rows) > 60:
                return [dict(t=int(x[0]), o=float(x[1]), h=float(x[2]),
                             l=float(x[3]), c=float(x[4]), v=float(x[5]),
                             tb=float(x[9])) for x in rows]
        except Exception:
            if is_fapi:
                _fapi_down_until = time.time() + 600
            continue
    return None


def _delta_z(candles: list[dict], start_idx: int) -> Optional[float]:
    """Кумулятивная тайкер-дельта с start_idx, нормированная random-walk:
    sum(delta) / (std(delta) * sqrt(n)). >2 = явный покупатель, <-2 продавец.
    Research 2026-07-10: базы с dz>2 выходят вверх в 77.5%, с dz<-2 — вниз
    в 77.3% (n=1264). Торговой обёртки нет (ход после разрешения мал) —
    используется как ИНФО в watchlist."""
    try:
        deltas = [2*c["tb"] - c["v"] for c in candles]
        hist = deltas[max(0, start_idx-200):start_idx]
        if len(hist) < 50:
            return None
        mean = sum(hist)/len(hist)
        var = sum((d-mean)**2 for d in hist)/len(hist)
        std = var ** 0.5
        if std <= 0:
            return None
        seg = deltas[start_idx:]
        if not seg:
            return None
        return round(sum(seg) / (std * len(seg) ** 0.5), 2)
    except Exception:
        return None


def _state_at(candles: list[dict], idx: int) -> bool:
    """Накопление ли на баре idx (окно W заканчивается на idx включительно)."""
    if idx < W + 25:
        return False
    win = candles[idx - W + 1: idx + 1]
    c = candles[idx]["c"]
    if not c:
        return False
    hi = max(x["h"] for x in win)
    lo = min(x["l"] for x in win)
    if (hi - lo) / c * 100 >= RNG_MAX:
        return False
    c24 = candles[idx - 24]["c"]
    if not c24 or abs(c / c24 - 1) * 100 >= ROC24_MAX:
        return False
    return True


def check_pair(pair: str, candles_1h: Optional[list[dict]] = None) -> Optional[dict]:
    try:
        if candles_1h is None:
            from exchange import get_klines_any
            candles_1h = get_klines_any(pair, "1h", 320)
        if not candles_1h or len(candles_1h) < W + 30:
            return None
        last = len(candles_1h) - 1
        if not _state_at(candles_1h, last):
            return None
        hours = 1
        while hours < MAX_BACK and last - hours > W + 25 and _state_at(candles_1h, last - hours):
            hours += 1
        if hours < MIN_HOURS:
            return None
        win = candles_1h[last - W + 1:]
        base_hi = max(x["h"] for x in win)
        base_lo = min(x["l"] for x in win)
        price = candles_1h[last]["c"]
        # 🟢/🔴 кто внутри базы — тайкер-дельта (info, research 07-10).
        # Если свечи пришли уже с полем tb (из scan_universe) — без дозапроса.
        dz = None
        try:
            kd = (candles_1h if candles_1h and "tb" in candles_1h[0]
                  else _fetch_klines_delta(pair))
            if kd and len(kd) > hours + 60:
                dz = _delta_z(kd, len(kd) - hours)
        except Exception:
            pass
        return {
            "pair": pair,
            "symbol": pair.replace("/", "").upper(),
            "price": price,
            "base_hi": base_hi,
            "base_lo": base_lo,
            "rng_pct": round((base_hi - base_lo) / price * 100, 2),
            "hours": hours,
            "dist_up_pct": round((base_hi - price) / price * 100, 2),
            "dist_dn_pct": round((price - base_lo) / price * 100, 2),
            "delta_dz": dz,
        }
    except Exception:
        logger.debug(f"[accum] check fail {pair}", exc_info=True)
        return None


_last_scan: dict = {}   # диагностика последнего скана (виден в /api/accumulation)


def _delta_series_sig(pair: str, kd: list[dict]) -> Optional[dict]:
    """💠 Серия дельт на 4h: 3 закрытых бара подряд с дельтой одного знака,
    |Σ| >= 2σ√3 (σ по 50 барам), объём серии >= 1.8×MA20, цена в сторону.
    Бэктест год (5894 соб.): направление НЕ предсказывает (WR 31-32% при
    БУ 33) — ИНФО-сигнал в журнал по запросу юзера; автоторговля не берёт
    (гейт PAPER_MOMENTUM_ONLY). Трекер меряет исход по TP/SL 6/3, 72ч."""
    try:
        buckets: dict = {}
        order: list = []
        for c in kd:
            k = c["t"] // (4 * 3600_000)
            if k not in buckets:
                buckets[k] = {"c": c["c"], "v": 0.0, "d": 0.0, "n": 0}
                order.append(k)
            b = buckets[k]
            b["c"] = c["c"]; b["v"] += c["v"]
            b["d"] += 2 * c["tb"] - c["v"]; b["n"] += 1
        if order and buckets[order[-1]]["n"] < 4:
            order = order[:-1]     # незакрытый 4h-бакет не считаем
        if len(order) < 60:
            return None
        arr = [buckets[k] for k in order]
        j = len(arr) - 1
        d0, d1, d2 = arr[j]["d"], arr[j - 1]["d"], arr[j - 2]["d"]
        sgn = 1 if d0 > 0 else -1
        if not (sgn * d1 > 0 and sgn * d2 > 0):
            return None
        if sgn * (arr[j]["c"] / arr[j - 3]["c"] - 1) <= 0:
            return None
        hist = [a["d"] for a in arr[max(0, j - 50):j]]
        if len(hist) < 30:
            return None
        import statistics
        std = statistics.pstdev(hist)
        if not std or abs(d0 + d1 + d2) < 2 * std * (3 ** 0.5):
            return None
        vh = [a["v"] for a in arr[max(0, j - 22):j - 2]]
        vma = sum(vh) / len(vh) if vh else 0.0
        if not vma:
            return None
        vr = (arr[j]["v"] + arr[j - 1]["v"] + arr[j - 2]["v"]) / 3 / vma
        if vr < 1.8:
            return None
        price = kd[-1]["c"]
        direction = "LONG" if sgn > 0 else "SHORT"
        sigma = abs(d0 + d1 + d2) / (std * (3 ** 0.5))
        return {"strategy": "delta_series", "direction": direction,
                "pair": pair, "symbol": pair.replace("/", "").upper(),
                "entry": price,
                "tp": price * (1 + 0.06 * sgn), "sl": price * (1 - 0.03 * sgn),
                "horizon_h": 72,
                "indicators": {"sigma": round(sigma, 1), "vol_ratio": round(vr, 2)}}
    except Exception:
        return None


def _blowoff_sig(pair: str, kd: list[dict]):
    """🌋 Blowoff-вершина → SHORT: последний ЗАКРЫТЫЙ 1h-бар ставит новый
    24ч-хай, но закрывается в нижней половине своего диапазона, при разгоне
    >+10%/24ч — покупателя вынесли на пике (фитиль сверху).
    Год-бэктест (3748 соб., сетка −10/+5/96ч): EV +0.59%, hit10 31.8%
    (🟢 +1.27/38.1%, 🔴 +0.80/36.6%) против случайного шорта +0.33/21.6%.
    RSI-перегрев как триггер проверен и ОТВЕРГНУТ (−0.21, хуже случайного)."""
    try:
        if len(kd) < 60:
            return None
        b = kd[-2]                       # последний закрытый бар
        prev = kd[-25:-2]                # 23 бара до него (окно 24 с ним)
        if len(prev) < 23:
            return None
        if b["h"] < max(x["h"] for x in prev):
            return None                  # не новый 24ч-хай
        if (b["h"] - b["l"]) <= 0 or b["c"] >= (b["h"] + b["l"]) / 2:
            return None                  # закрытие не в нижней половине
        c24 = kd[-26]["c"]
        mom24 = (b["c"] / c24 - 1) * 100 if c24 else 0
        if mom24 <= 10:
            return None
        phase = None
        try:
            from supertrend_tracker import _market_phase_now
            phase = _market_phase_now()
        except Exception:
            pass
        price = b["c"]
        return {"strategy": "blowoff", "direction": "SHORT",
                "pair": pair, "symbol": pair.replace("/", "").upper(),
                "entry": price, "tp": price * 0.90, "sl": price * 1.05,
                "horizon_h": 96,
                "indicators": {"mom24": round(mom24, 1),
                               "wick_pct": round((b["h"] - b["c"]) / price * 100, 2),
                               "phase": phase}}
    except Exception:
        return None


def _tg16(txt: str) -> None:
    """TG в BOT16 (whale-канал) из sync-треда скана — прямой REST."""
    try:
        from config import BOT16_BOT_TOKEN, WHALE_CHAT_ID
        if BOT16_BOT_TOKEN and WHALE_CHAT_ID:
            import requests
            requests.post(
                f"https://api.telegram.org/bot{BOT16_BOT_TOKEN}/sendMessage",
                data={"chat_id": WHALE_CHAT_ID, "text": txt,
                      "parse_mode": "HTML"}, timeout=10)
    except Exception:
        logger.debug("[tg16] send fail", exc_info=True)


def _thin_pump_sig(pair: str, kd: list[dict]):
    """💨 Тонкий памп → SHORT: зелёный 1h-бар с ходом >2.2× медианы-240ч
    на объёме НИЖЕ медианы-240ч — рост по пустому стакану без реального
    покупателя, фейдится. Бэктест 90д (581 соб., сетка −10/+5/96ч):
    WR 51.3%, EV +1.36%/сделку (случайный шорт: 39.8% / −0.22).
    Обратная сторона (тонкий слив → лонг) проверена и ОТВЕРГНУТА (−1.32).
    Аномальный объём сам по себе — тоже (все комбо ±0.2пп от рандома)."""
    try:
        if len(kd) < 300:
            return None
        b = kd[-2]                        # последний закрытый бар
        hist = kd[-242:-2]
        if len(hist) < 150 or not b.get("c") or not b.get("o"):
            return None
        rngs = sorted((x["h"] - x["l"]) / x["c"] * 100 for x in hist if x["c"])
        vols = sorted(x["v"] for x in hist)
        rng_med = rngs[len(rngs) // 2]
        v_med = vols[len(vols) // 2]
        if rng_med <= 0 or v_med <= 0:
            return None
        rng = (b["h"] - b["l"]) / b["c"] * 100
        # двухуровневый порог (сенситивити 25.07): strict vol<1.0×медианы —
        # 6.5/день, WR 51.3, EV +1.36 (идёт в TG); loose vol<1.25× —
        # 12.6/день, WR 49.0, EV +0.98 (только журнал). Дальше ослаблять
        # нельзя: 1.8×/1.5 → 62/день при EV +0.43 (спам)
        if not (b["c"] > b["o"] and rng > 2.2 * rng_med
                and b["v"] < 1.25 * v_med):
            return None
        phase = None
        try:
            from supertrend_tracker import _market_phase_now
            phase = _market_phase_now()
        except Exception:
            pass
        price = b["c"]
        return {"strategy": "thin_pump", "direction": "SHORT",
                "pair": pair, "symbol": pair.replace("/", "").upper(),
                "entry": price, "tp": price * 0.90, "sl": price * 1.05,
                "horizon_h": 96,
                "indicators": {"rng_x": round(rng / rng_med, 1),
                               "vol_x": round(b["v"] / v_med, 2),
                               "tier": "strict" if b["v"] < v_med else "loose",
                               "phase": phase}}
    except Exception:
        return None


def _floor_buy_sig(pair: str, kd: list[dict]):
    """💎 Дно+покупатель → LONG (инфо/вход с триггером): закрытый 1h-бар
    ставит новый 48ч-лоу, но >=65% его объёма — агрессивные ПОКУПКИ
    маркетом при объёме >1.5× медианы-240ч. Так покупают только те, кто
    спешит: жадный сбор дна. Бэктест 90д (216 соб., сетка +10/−5/96ч):
    WR 52.3%, EV +0.67 — единственная рабочая ячейка имбаланса; продажа
    на хае +0.18, всё вне экстремумов — ноль."""
    try:
        if len(kd) < 300:
            return None
        b = kd[-2]
        if not b.get("v") or b["v"] <= 0:
            return None
        hist = kd[-242:-2]
        if len(hist) < 150:
            return None
        vols = sorted(x["v"] for x in hist)
        v_med = vols[len(vols) // 2]
        lo48 = min(x["l"] for x in kd[-50:-2])
        d_bar = 2 * b["tb"] - b["v"]
        if not (b["l"] <= lo48 and b["v"] > 1.5 * v_med
                and d_bar / b["v"] >= 0.65):
            return None
        phase = None
        try:
            from supertrend_tracker import _market_phase_now
            phase = _market_phase_now()
        except Exception:
            pass
        price = b["c"]
        return {"strategy": "floor_buy", "direction": "LONG",
                "pair": pair, "symbol": pair.replace("/", "").upper(),
                "entry": price, "tp": price * 1.10, "sl": price * 0.95,
                "horizon_h": 96,
                "indicators": {"imb": round(d_bar / b["v"], 2),
                               "vol_x": round(b["v"] / v_med, 1),
                               "phase": phase}}
    except Exception:
        return None


def _vol_anomaly_sig(pair: str, kd: list[dict]):
    """⚡ Аномалия объёма ГДЕ УГОДНО (1h) → инфо-сигнал в журнал: закрытый
    бар с объёмом >8×SMA24 и дельтой >q90-240ч. Направление = знак дельты
    (инфо: бэктест 90д — направление не предсказывает, ±0.2пп от рандома).
    Порог 8× по частоте: 45/день на 300 пар (4× дало бы 116/день)."""
    try:
        if len(kd) < 300:
            return None
        b = kd[-2]
        v24 = [x["v"] for x in kd[-26:-2]]
        sma24 = sum(v24) / len(v24) if v24 else 0
        if not sma24 or b["v"] < 8 * sma24:
            return None
        hist = kd[-242:-2]
        d_hist = sorted(abs(2 * x["tb"] - x["v"]) for x in hist)
        q90 = d_hist[int(len(d_hist) * 0.90)]
        d_bar = 2 * b["tb"] - b["v"]
        if abs(d_bar) <= q90:
            return None
        hi48 = max(x["h"] for x in kd[-50:-2])
        lo48 = min(x["l"] for x in kd[-50:-2])
        loc = ("TOP" if b["h"] >= hi48 else
               "BOT" if b["l"] <= lo48 else "MID")
        phase = None
        try:
            from supertrend_tracker import _market_phase_now
            phase = _market_phase_now()
        except Exception:
            pass
        price = b["c"]
        want = 1 if d_bar >= 0 else -1
        return {"strategy": "vol_anomaly", "direction": "LONG" if want > 0 else "SHORT",
                "pair": pair, "symbol": pair.replace("/", "").upper(),
                "entry": price, "tp": price * (1 + want * 0.10),
                "sl": price * (1 - want * 0.05), "horizon_h": 96,
                "indicators": {"vol_x": round(b["v"] / sma24, 1),
                               "delta": round(d_bar, 0), "loc": loc,
                               "imb": round(d_bar / b["v"], 2) if b["v"] else None,
                               "phase": phase}}
    except Exception:
        return None


def _vol_anomaly4h_sig(pair: str, kd: list[dict]):
    """🌩 Аномалия объёма на ЗАКРЫТОМ 4h-баре → инфо-сигнал в журнал:
    объём 4h-бара >6× среднего последних 24 4h-баров и |дельта 4h| выше
    q75 последних 60 баров. Скан каждые 30 мин видит закрытый бар в
    течение получаса после закрытия; кулдаун 3.5ч = раз на бар.
    Порог 6× по частоте: 29/день (4× дало бы 55/день)."""
    try:
        buckets: dict = {}
        order: list = []
        for x in kd:
            k = x["t"] // 14_400_000
            if k not in buckets:
                buckets[k] = {"o": x["o"], "h": x["h"], "l": x["l"],
                              "c": x["c"], "v": 0.0, "d": 0.0, "n": 0}
                order.append(k)
            bb = buckets[k]
            bb["h"] = max(bb["h"], x["h"]); bb["l"] = min(bb["l"], x["l"])
            bb["c"] = x["c"]; bb["v"] += x["v"]
            bb["d"] += 2 * x["tb"] - x["v"]; bb["n"] += 1
        if order and buckets[order[-1]]["n"] < 4:
            order = order[:-1]              # незакрытый бакет не считаем
        if len(order) < 70:
            return None
        arr = [buckets[k] for k in order]
        b = arr[-1]
        prev24 = [a["v"] for a in arr[-25:-1]]
        ma24 = sum(prev24) / len(prev24) if prev24 else 0
        if not ma24 or b["v"] < 6 * ma24:
            return None
        d_hist = sorted(abs(a["d"]) for a in arr[-61:-1])
        q75 = d_hist[int(len(d_hist) * 0.75)]
        if abs(b["d"]) <= q75:
            return None
        phase = None
        try:
            from supertrend_tracker import _market_phase_now
            phase = _market_phase_now()
        except Exception:
            pass
        price = b["c"]
        want = 1 if b["d"] >= 0 else -1
        return {"strategy": "vol_anomaly4h", "direction": "LONG" if want > 0 else "SHORT",
                "pair": pair, "symbol": pair.replace("/", "").upper(),
                "entry": price, "tp": price * (1 + want * 0.10),
                "sl": price * (1 - want * 0.05), "horizon_h": 96,
                "indicators": {"vol_x": round(b["v"] / ma24, 1),
                               "delta": round(b["d"], 0),
                               "imb": round(b["d"] / b["v"], 2) if b["v"] else None,
                               "phase": phase}}
    except Exception:
        return None


def _vol_anomaly_event(pair: str, kd: list[dict]):
    """⚡ ИНФО-событие «аномальный объём у экстремума» (для вкладки-скринера,
    НЕ торговый сигнал): закрытый 1h-бар с объёмом >4×SMA24 и крупной
    дельтой (|d|>q75-240ч) на новом 48ч-хае или лоу. Бэктест 90д: направление
    НЕ предсказывает (все стороны ±0.2пп от рандома) — контекст смотрит юзер
    на кластерном графике. TG — только сверх-аномалии (объём >8×)."""
    try:
        if len(kd) < 300:
            return None
        b = kd[-2]
        hist = kd[-242:-2]
        if len(hist) < 150:
            return None
        v24 = [x["v"] for x in kd[-26:-2]]
        sma24 = sum(v24) / len(v24) if v24 else 0
        if not sma24 or b["v"] < 4 * sma24:
            return None
        d_hist = sorted(abs(2 * x["tb"] - x["v"]) for x in hist)
        q75 = d_hist[int(len(d_hist) * 0.75)]
        d_bar = 2 * b["tb"] - b["v"]
        if abs(d_bar) <= q75:
            return None
        hi48 = max(x["h"] for x in kd[-50:-2])
        lo48 = min(x["l"] for x in kd[-50:-2])
        if b["h"] >= hi48:
            loc = "TOP"
        elif b["l"] <= lo48:
            loc = "BOT"
        else:
            return None
        phase = None
        try:
            from supertrend_tracker import _market_phase_now
            phase = _market_phase_now()
        except Exception:
            pass
        return {"pair": pair, "symbol": pair.replace("/", "").upper(),
                "loc": loc, "vol_x": round(b["v"] / sma24, 1),
                "delta": round(d_bar, 0), "price": b["c"],
                "bar_t": int(b["t"]), "phase": phase}
    except Exception:
        return None


def _capitulation_sig(pair: str, kd: list[dict]):
    """🛟 Капитуляция → LONG: за последние 48ч была капитуляция-свеча
    (новый 24ч-лоу + закрытие в ВЕРХНЕЙ половине бара + слив <−10%/24ч)
    И RSI4h опустился <=25 и развернулся вверх (по ЗАКРЫТЫМ 4h-барам).
    Год-бэктест (1723 соб., сетка +10/−5/96ч): EV +0.81%, hit10 34.9%;
    в 🔴-фазе +0.82/35.7 — второй легальный лонг в красной фазе после
    💣-реверсала. Дно — процесс: свеча одна (−0.36) и RSI один (+0.21)
    не работают, работает последовательность. Вершина наоборот — событие
    (см. _blowoff_sig)."""
    try:
        if len(kd) < 340:
            return None
        # RSI14 по закрытым 4h-барам: разворот вверх из зоны <=25
        closes4, cur_key = [], None
        for c in kd:
            k = c["t"] // (4 * 3600_000)
            if k != cur_key:
                closes4.append(c["c"])
                cur_key = k
            else:
                closes4[-1] = c["c"]
        closes4 = closes4[:-1]           # незакрытый 4h-бакет не считаем
        if len(closes4) < 40:
            return None
        rsis = []
        ag = al = 0.0
        for i in range(1, len(closes4)):
            ch = closes4[i] - closes4[i - 1]
            g, lo = max(ch, 0.0), max(-ch, 0.0)
            if i <= 14:
                ag += g; al += lo
                if i == 14:
                    ag /= 14; al /= 14
                    rsis.append(100.0 if al == 0 else 100 - 100 / (1 + ag / al))
            else:
                ag = (ag * 13 + g) / 14
                al = (al * 13 + lo) / 14
                rsis.append(100.0 if al == 0 else 100 - 100 / (1 + ag / al))
        if len(rsis) < 2:
            return None
        r_prev, r_last = rsis[-2], rsis[-1]
        if not (r_prev <= 25 and r_last > r_prev):
            return None
        # капитуляция-свеча среди последних 48 закрытых 1h-баров
        cap_bar = None
        for j in range(len(kd) - 49, len(kd) - 1):
            if j < 50:
                continue
            b = kd[j]
            lo24 = min(x["l"] for x in kd[j - 23:j])
            if b["l"] > lo24:
                continue
            if (b["h"] - b["l"]) <= 0 or b["c"] <= (b["h"] + b["l"]) / 2:
                continue
            c24 = kd[j - 24]["c"]
            if not c24 or (b["c"] / c24 - 1) * 100 >= -10:
                continue
            cap_bar = b
        if cap_bar is None:
            return None
        phase = None
        try:
            from supertrend_tracker import _market_phase_now
            phase = _market_phase_now()
        except Exception:
            pass
        price = kd[-2]["c"]
        c24n = kd[-26]["c"]
        mom24 = (kd[-2]["c"] / c24n - 1) * 100 if c24n else 0
        return {"strategy": "capitulation", "direction": "LONG",
                "pair": pair, "symbol": pair.replace("/", "").upper(),
                "entry": price, "tp": price * 1.10, "sl": price * 0.95,
                "horizon_h": 96,
                "indicators": {"rsi4h_prev": round(r_prev, 1),
                               "rsi4h": round(r_last, 1),
                               "mom24": round(mom24, 1),
                               "phase": phase}}
    except Exception:
        return None


def _rsi4h_value(candles: list[dict], hours: int = 4):
    """(RSI14 последнего бара, SMA14 RSI) на ресемпле `hours` или None.
    Ресемпл из 1h свечей, Wilder RSI. hours=12 — для 12h-климата."""
    try:
        closes = []
        cur_key = None
        for c in candles:
            k = c["t"] // (hours * 3600_000)
            if k != cur_key:
                closes.append(c["c"])
                cur_key = k
            else:
                closes[-1] = c["c"]
        if len(closes) < 32:
            return None
        rsis = []
        ag = al = 0.0
        for i in range(1, len(closes)):
            ch = closes[i] - closes[i - 1]
            g, lo = max(ch, 0.0), max(-ch, 0.0)
            if i <= 14:
                ag += g; al += lo
                if i == 14:
                    ag /= 14; al /= 14
                    rsis.append(100.0 if al == 0 else 100 - 100 / (1 + ag / al))
            else:
                ag = (ag * 13 + g) / 14
                al = (al * 13 + lo) / 14
                rsis.append(100.0 if al == 0 else 100 - 100 / (1 + ag / al))
        if len(rsis) < 15:
            return None
        sma = sum(rsis[-14:]) / 14
        return rsis[-1], sma
    except Exception:
        return None


def _rsi4h_bull(candles: list[dict]) -> Optional[bool]:
    """RSI14(4h) выше своей SMA14? Для ширины рынка (/api/market-side)."""
    v = _rsi4h_value(candles)
    return None if v is None else v[0] > v[1]


def scan_universe(max_pairs: int = 300):
    """Скан ликвидных пар. Вызывается из watcher-лупа (thread).
    Возвращает list[dict] с базами или None если скан прерван (пустой
    универсум) — в этом случае снапшот трогать нельзя."""
    from futures_data import get_liquid_pairs
    from database import utcnow
    out = []
    _last_scan.clear()
    _last_scan["started"] = utcnow().isoformat()
    try:
        pairs = get_liquid_pairs(min_volume_usd=5_000_000)[:max_pairs]
        # На 6-й минуте после рестарта ticker-кэш бывает пуст -> 0 пар.
        # Сбрасываем TTL кэша и ретраим (2026-07-11: без сброса ретрай
        # бесполезен — TTL 2 мин держит пустоту).
        if not pairs:
            import futures_data as _fd
            _fd._batch_cache_ts = 0
            time.sleep(30)
            pairs = get_liquid_pairs(min_volume_usd=5_000_000)[:max_pairs]
    except Exception as e:
        _last_scan["error"] = f"pairs: {e}"
        return None
    _last_scan["pairs"] = len(pairs)
    if not pairs:
        # Скан по алфавитному списку-фолбэку НЕЛЬЗЯ: другой универсум стирает
        # базы пар вне топ-300 по алфавиту (2026-07-11: QTUM/WOO/XMR).
        # Пропускаем цикл — watcher ресолипнет через 3 мин.
        _last_scan["error"] = "universe empty — скан пропущен"
        return None
    checked = errors = 0
    first_err = None
    delta_map = []
    breadth_bull = breadth_tot = 0
    breadth12_bull = breadth12_tot = 0
    btc_kd = None
    ds_fired = 0
    cand_rows = []
    vol_anoms = []
    for sym in pairs:
        pair = sym.replace("USDT", "/USDT") if "/" not in sym else sym
        try:
            # один фьючерсный запрос на пару: и база, и текущая 24ч-дельта
            # (400 баров: 12h-климату нужно >=32 закрытых 12h-свечи)
            kd = _fetch_klines_delta(pair, 400)
            checked += 1
            if pair == "BTC/USDT" and kd:
                btc_kd = kd
            if kd and len(kd) >= 260:
                _rv = _rsi4h_value(kd)
                _bull = None if _rv is None else _rv[0] > _rv[1]
                if _bull is not None:
                    breadth_tot += 1
                    if _bull:
                        breadth_bull += 1
                # 🌡 ширина на 12h — для глобального климата
                _rv12 = _rsi4h_value(kd, 12)
                if _rv12 is not None:
                    breadth12_tot += 1
                    if _rv12[0] > _rv12[1]:
                        breadth12_bull += 1
                # 💠 серия дельт — инфо-сигнал в журнал (кулдаун 24ч на пару)
                _ds = _delta_series_sig(pair, kd)
                if _ds is not None:
                    try:
                        from impulse_detector import store_signal
                        if store_signal(_ds, cooldown_h=24):
                            ds_fired += 1
                    except Exception:
                        pass
                # 🌋 blowoff-вершина → SHORT (кулдаун 24ч на пару)
                _bo = _blowoff_sig(pair, kd)
                if _bo is not None:
                    try:
                        from impulse_detector import store_signal
                        if store_signal(_bo, cooldown_h=24):
                            ds_fired += 1
                    except Exception:
                        pass
                # 🛟 капитуляция-дно → LONG (кулдаун 24ч на пару)
                _cp = _capitulation_sig(pair, kd)
                if _cp is not None:
                    try:
                        from impulse_detector import store_signal
                        if store_signal(_cp, cooldown_h=24):
                            ds_fired += 1
                    except Exception:
                        pass
                # 💨 тонкий памп → SHORT (кулдаун 24ч, TG — боевой сигнал)
                _tp_ = _thin_pump_sig(pair, kd)
                if _tp_ is not None:
                    try:
                        from impulse_detector import store_signal
                        if store_signal(_tp_, cooldown_h=24):
                            ds_fired += 1
                            _i = _tp_["indicators"]
                            # TG — только строгий уровень (~6/день), loose
                            # (~ещё 6/день) остаётся в журнале/вкладке
                            if _i.get("tier") == "strict":
                                _tg16(f"💨 <b>ТОНКИЙ ПАМП · "
                                      f"{pair.replace('/USDT', '')}</b>\n"
                                      f"🔴 SHORT @ {_tp_['entry']:.6g}\n"
                                      f"ход ×{_i['rng_x']} от нормы на объёме "
                                      f"×{_i['vol_x']} медианы — рост по "
                                      f"пустому стакану\n"
                                      f"TP −10% · SL +5% · до 96ч\n"
                                      f"<i>бэктест 90д: WR 51.3%, "
                                      f"EV +1.36%/сделку</i>")
                    except Exception:
                        pass
                # 💎 дно+покупатель → LONG (кулдаун 24ч)
                _fb = _floor_buy_sig(pair, kd)
                if _fb is not None:
                    try:
                        from impulse_detector import store_signal
                        if store_signal(_fb, cooldown_h=24):
                            ds_fired += 1
                    except Exception:
                        pass
                # ⚡ инфо-событие «аномальный объём у экстремума» (скринер)
                _va = _vol_anomaly_event(pair, kd)
                if _va is not None:
                    vol_anoms.append(_va)
                # ⚡ аномалия 1h где угодно → журнал (кулдаун 12ч)
                _vs = _vol_anomaly_sig(pair, kd)
                if _vs is not None:
                    try:
                        from impulse_detector import store_signal
                        if store_signal(_vs, cooldown_h=12):
                            ds_fired += 1
                    except Exception:
                        pass
                # 🌩 аномалия на закрытом 4h-баре → журнал (раз на бар)
                _v4 = _vol_anomaly4h_sig(pair, kd)
                if _v4 is not None:
                    try:
                        from impulse_detector import store_signal
                        if store_signal(_v4, cooldown_h=3.5):
                            ds_fired += 1
                    except Exception:
                        pass
                dz24 = _delta_z(kd, len(kd) - 24)
                # 🏆 кандидаты фазы: ATR%, импульс 24ч, объём-ратио
                # (год-бэктест: ATR ловит 2.2-3.9 из 10 реальных лидеров
                # фазы против 0.5 у случайного выбора)
                try:
                    _c = [x["c"] for x in kd]
                    _v = [x["v"] for x in kd]
                    _tr = [max(kd[i]["h"] - kd[i]["l"],
                               abs(kd[i]["h"] - kd[i-1]["c"]),
                               abs(kd[i]["l"] - kd[i-1]["c"]))
                           for i in range(len(kd) - 14, len(kd))]
                    _atrp = (sum(_tr) / len(_tr)) / _c[-1] * 100 if _c[-1] else 0
                    _mom = (_c[-1] / _c[-25] - 1) * 100 if len(_c) > 25 and _c[-25] else 0
                    _vbase = sum(_v[-240:-24]) / 216 if len(_v) >= 240 else 0
                    _vr = (sum(_v[-24:]) / 24) / _vbase if _vbase else 1.0
                    _ret7 = ((_c[-1] / _c[-169] - 1) * 100
                             if len(_c) >= 169 and _c[-169] else None)
                    # ST-состояния 1h/4h для серверного светофора (журнал)
                    _st1t = _st1f = _st4t = _st4f = None
                    try:
                        from backtest_supertrend import compute_st_series
                        _s1 = compute_st_series(kd, 10, 3.0)
                        if len(_s1) >= 6:
                            _st1t = _s1[-1]["trend"]
                            _st1f = any(_s1[j]["trend"] != _s1[j-1]["trend"]
                                        and _s1[j-1]["trend"]
                                        for j in range(len(_s1) - 4, len(_s1)))
                        _b4 = {}
                        for _x in kd:
                            _k = _x["t"] // (4 * 3600_000)
                            if _k not in _b4:
                                _b4[_k] = dict(_x)
                            else:
                                _b4[_k]["h"] = max(_b4[_k]["h"], _x["h"])
                                _b4[_k]["l"] = min(_b4[_k]["l"], _x["l"])
                                _b4[_k]["c"] = _x["c"]
                        _kd4 = [_b4[_k] for _k in sorted(_b4)][:-1]  # закрытые
                        _s4 = compute_st_series(_kd4, 10, 3.0)
                        if len(_s4) >= 3:
                            _st4t = _s4[-1]["trend"]
                            _st4f = (_s4[-1]["trend"] != _s4[-2]["trend"]
                                     and bool(_s4[-2]["trend"]))
                    except Exception:
                        pass
                    # 🌊 ПОТОК: CVD24 + свежесть аномалий потока (закрытые бары)
                    _dl = [2 * x["tb"] - x["v"] for x in kd]
                    _cvd24 = sum(_dl[-25:-1])
                    _ab_ts = _as_ts = None
                    try:
                        _vh = sorted(x["v"] for x in kd[-241:-1])
                        _vm_ = _vh[len(_vh) // 2] if _vh else 0
                        _dh = sorted(abs(x_) for x_ in _dl[-241:-1])
                        _q75_ = _dh[int(len(_dh) * 0.75)] if _dh else 0
                        for _j in range(max(26, len(kd) - 14), len(kd) - 1):
                            _bv, _bd = kd[_j]["v"], _dl[_j]
                            _sma_ = sum(x["v"] for x in kd[_j - 24:_j]) / 24
                            _big = _sma_ and _bv > 4 * _sma_ and abs(_bd) > _q75_
                            _imb = (_vm_ and _bv > 1.5 * _vm_ and _bv > 0
                                    and abs(_bd) / _bv >= 0.65)
                            if _big or _imb:
                                if _bd > 0:
                                    _ab_ts = int(kd[_j]["t"]) + 3600_000
                                else:
                                    _as_ts = int(kd[_j]["t"]) + 3600_000
                    except Exception:
                        pass
                    cand_rows.append({"pair": pair,
                                      "symbol": pair.replace("/", "").upper(),
                                      "cvd24": round(_cvd24, 0),
                                      "anom_buy_ts": _ab_ts,
                                      "anom_sell_ts": _as_ts,
                                      "atr_pct": round(_atrp, 2),
                                      "mom24": round(_mom, 2),
                                      "vol_ratio": round(_vr, 2),
                                      "ret7d": round(_ret7, 2) if _ret7 is not None else None,
                                      "rsi4h": round(_rv[0], 1) if _rv else None,
                                      "st1_trend": _st1t, "st1_flip": _st1f,
                                      "st4_trend": _st4t, "st4_flip": _st4f,
                                      "dz24": round(dz24, 2) if dz24 is not None else None})
                except Exception:
                    pass
                if dz24 is not None:
                    delta_map.append({"pair": pair,
                                      "symbol": pair.replace("/", "").upper(),
                                      "dz24": dz24})
                st = check_pair(pair, candles_1h=kd)
            else:
                st = check_pair(pair)
            if st:
                out.append(st)
        except Exception as e:
            errors += 1
            if first_err is None:
                first_err = f"{pair}: {e}"
    try:
        store_delta_map(delta_map)
        _last_scan["delta_map"] = len(delta_map)
    except Exception:
        pass
    # 🏆 кандидаты фазы: композит 0.5*ATR + 0.3*импульс(по направлению) +
    # 0.2*объём (перцентильные ранги). Топ-15 на лонг и на шорт.
    try:
        if len(cand_rows) >= 50:
            def _ranks(vals):
                order = sorted(range(len(vals)), key=lambda i: vals[i])
                rk = [0.0] * len(vals)
                for pos, idx in enumerate(order):
                    rk[idx] = pos / max(len(vals) - 1, 1)
                return rk
            r_atr = _ranks([c["atr_pct"] for c in cand_rows])
            r_vol = _ranks([c["vol_ratio"] for c in cand_rows])
            r_mom = _ranks([c["mom24"] for c in cand_rows])
            for i, c in enumerate(cand_rows):
                c["score_long"] = round(0.5 * r_atr[i] + 0.3 * r_mom[i] + 0.2 * r_vol[i], 3)
                c["score_short"] = round(0.5 * r_atr[i] + 0.3 * (1 - r_mom[i]) + 0.2 * r_vol[i], 3)
            top_long = sorted(cand_rows, key=lambda c: -c["score_long"])[:15]
            top_short = sorted(cand_rows, key=lambda c: -c["score_short"])[:15]
            from database import _get_db, utcnow
            _get_db().market_state.update_one(
                {"_id": "phase_candidates"},
                {"$set": {"long": top_long, "short": top_short,
                          "n_universe": len(cand_rows),
                          "updated_at": utcnow()}}, upsert=True)
            _last_scan["candidates"] = len(cand_rows)
    except Exception:
        logger.exception("[accum] candidates store fail")
    # 🎩 pair_context для PRO-колонки журнала: RS-ранг 7д (кросс-секционный
    # перцентиль), импульс 24ч, RSI4h — контекст «как оценил бы трейдер»
    try:
        ctx_rows = [c for c in cand_rows if c.get("ret7d") is not None]
        if len(ctx_rows) >= 50:
            from database import _get_db, utcnow
            from pymongo import ReplaceOne
            order = sorted(range(len(ctx_rows)), key=lambda i: ctx_rows[i]["ret7d"])
            pctl = [0.0] * len(ctx_rows)
            for pos, idx in enumerate(order):
                pctl[idx] = round(pos / max(len(ctx_rows) - 1, 1), 3)
            now = utcnow()
            ops = [ReplaceOne(
                {"_id": c["symbol"]},
                {"_id": c["symbol"], "pair": c["pair"],
                 "ret7d": c["ret7d"], "pctl7d": pctl[i],
                 "cvd24": c.get("cvd24"),
                 "anom_buy_ts": c.get("anom_buy_ts"),
                 "anom_sell_ts": c.get("anom_sell_ts"),
                 "dz24": c.get("dz24"),
                 "price": c.get("price"),
                 "mom24": c["mom24"], "rsi4h": c.get("rsi4h"),
                 "st1_trend": c.get("st1_trend"), "st1_flip": c.get("st1_flip"),
                 "st4_trend": c.get("st4_trend"), "st4_flip": c.get("st4_flip"),
                 "vol_ratio": c.get("vol_ratio"),
                 "atr_pct": c["atr_pct"], "updated_at": now},
                upsert=True) for i, c in enumerate(ctx_rows)]
            _get_db().pair_context.bulk_write(ops, ordered=False)
            _last_scan["pair_context"] = len(ops)
    except Exception:
        logger.exception("[accum] pair_context store fail")
    # ширина рынка для /api/market-side (>=50 пар — защита от сбойного скана)
    try:
        if breadth_tot >= 50:
            from database import _get_db, utcnow
            _pct = round(breadth_bull / breadth_tot * 100, 1)
            _get_db().market_state.update_one(
                {"_id": "breadth"},
                {"$set": {"bull": breadth_bull, "total": breadth_tot,
                          "pct": _pct, "updated_at": utcnow()}}, upsert=True)
            # история breadth для прогноза фазы (/api/phase-forecast)
            _get_db().breadth_history.insert_one(
                {"at": utcnow(), "pct": _pct, "src": "scan"})
            _last_scan["breadth"] = f"{breadth_bull}/{breadth_tot}"
    except Exception:
        logger.exception("[accum] breadth store fail")
    # 🌡 12h-климат (глобальная фаза): та же методика на 12h-барах.
    # Год-бэктест (матрица 12h×4h, сетка 10/5/96ч): фильтр «оба этажа
    # согласны» вредит (EV +0.05 vs +0.21), эдж в расхождениях:
    # 🔴12h+🟢4h LONG +2.03 (WR 58) · 🟢12h SHORT +0.88..+2.73 ·
    # 🟢12h+⚪4h LONG −2.05 (худшая ячейка года)
    try:
        if breadth12_tot >= 50:
            _pct12 = round(breadth12_bull / breadth12_tot * 100, 1)
            _bst12 = None
            try:
                if btc_kd is None:
                    btc_kd = _fetch_klines_delta("BTC/USDT", 400)
                if btc_kd:
                    from backtest_supertrend import compute_st_series
                    _b12 = {}
                    for _x in btc_kd:
                        _k = _x["t"] // (12 * 3600_000)
                        if _k not in _b12:
                            _b12[_k] = dict(_x)
                        else:
                            _b12[_k]["h"] = max(_b12[_k]["h"], _x["h"])
                            _b12[_k]["l"] = min(_b12[_k]["l"], _x["l"])
                            _b12[_k]["c"] = _x["c"]
                    _kd12 = [_b12[_k] for _k in sorted(_b12)][:-1]  # закрытые
                    _s12 = compute_st_series(_kd12, 10, 3.0)
                    if len(_s12) >= 3:
                        _bst12 = _s12[-1]["trend"]
            except Exception:
                pass
            _c_state = ("NEUTRAL" if _pct12 > 60 else "SHORT" if _pct12 < 40
                        else "LONG" if _bst12 == 1 else
                        "SHORT" if _bst12 == -1 else "NEUTRAL")
            from database import _get_db, utcnow
            _get_db().market_state.update_one(
                {"_id": "climate12"},
                {"$set": {"pct": _pct12, "bull": breadth12_bull,
                          "total": breadth12_tot,
                          "btc_st12": ("UP" if _bst12 == 1 else
                                       "DOWN" if _bst12 == -1 else None),
                          "state": _c_state, "updated_at": utcnow()}},
                upsert=True)
            _last_scan["climate12"] = f"{_c_state} {_pct12}%"
    except Exception:
        logger.exception("[accum] climate12 store fail")
    # ⚡ события аномального объёма: запись с кулдауном 12ч/пару, TG >8×,
    # чистка старше 7д. НЕ сигнал — скринер (вкладка «Аномалии объёма»)
    try:
        if vol_anoms:
            from datetime import timedelta
            from database import _get_db, utcnow
            db_ = _get_db()
            now_ = utcnow()
            cut = now_ - timedelta(hours=12)
            fresh = {d_["symbol"] for d_ in db_.anomaly_events.find(
                {"at": {"$gte": cut}}, {"symbol": 1})}
            new_ev = [dict(e, at=now_) for e in vol_anoms
                      if e["symbol"] not in fresh]
            if new_ev:
                db_.anomaly_events.insert_many(new_ev)
                for e in new_ev:
                    if e["vol_x"] >= 8:
                        _d = e["delta"]
                        _tg16(f"⚡ <b>АНОМАЛЬНЫЙ ОБЪЁМ · "
                              f"{e['pair'].replace('/USDT', '')}</b>\n"
                              f"{'🔺 на вершине (новый 48ч-хай)' if e['loc'] == 'TOP' else '🔻 на дне (новый 48ч-лоу)'}\n"
                              f"объём ×{e['vol_x']} нормы · дельта "
                              f"{'+' if _d >= 0 else ''}{_d:,.0f} "
                              f"({'покупатель' if _d >= 0 else 'продавец'})\n"
                              f"<i>инфо-событие: направление не предсказывает — "
                              f"открой 🧮 Кластеры и смотри контекст</i>")
            db_.anomaly_events.delete_many(
                {"at": {"$lt": now_ - timedelta(days=7)}})
            _last_scan["vol_anoms"] = len(new_ev)
    except Exception:
        logger.exception("[accum] vol_anoms store fail")
    # sample: сколько пар вообще дали свечи (диагностика get_klines_any)
    try:
        if pairs:
            from exchange import get_klines_any
            p0 = pairs[0].replace("USDT", "/USDT") if "/" not in pairs[0] else pairs[0]
            kl = get_klines_any(p0, "1h", 320)
            _last_scan["sample_klines"] = len(kl) if kl else 0
    except Exception as e:
        _last_scan["sample_klines"] = f"err: {e}"
    _last_scan.update(checked=checked, errors=errors, found=len(out),
                      delta_series=ds_fired,
                      first_err=first_err, finished=utcnow().isoformat())
    out.sort(key=lambda x: -x["hours"])
    return out


def track_resolutions(new_items: list[dict]) -> list[dict]:
    """Пары, вышедшие из базы с пробоем границы, пишутся в accum_events
    (source='accum' в журнале: 🧊 база → ВВЕРХ/ВНИЗ). Вызывать ДО
    store_snapshot — сравнивает с прошлым снапшотом."""
    events = []
    try:
        from database import _get_db, utcnow
        from exchange import get_klines_any
        db = _get_db()
        prev = {d["pair"]: d for d in db.accum_state.find()}
        cur = {it["pair"] for it in new_items}
        for pair, d in prev.items():
            if pair in cur:
                continue
            try:
                kl = get_klines_any(pair, "1h", 3)
                price = kl[-1]["c"] if kl else None
            except Exception:
                price = None
            if not price or not d.get("base_hi") or not d.get("base_lo"):
                continue
            if price > d["base_hi"]:
                resolution, direction = "UP", "LONG"
            elif price < d["base_lo"]:
                resolution, direction = "DOWN", "SHORT"
            else:
                continue    # растворилась без пробоя — не событие
            events.append({
                "pair": pair, "symbol": d.get("symbol"),
                "direction": direction, "resolution": resolution,
                "base_hi": d["base_hi"], "base_lo": d["base_lo"],
                "rng_pct": d.get("rng_pct"), "hours": d.get("hours"),
                "res_price": price, "resolved_at": utcnow(),
                "delta_dz": d.get("delta_dz"),
            })
        if events:
            db.accum_events.insert_many(events)
            logger.info(f"[accum] разрешений: "
                        f"{[(e['pair'], e['resolution']) for e in events]}")
    except Exception:
        logger.exception("[accum] resolutions fail")
    return events


def store_delta_map(rows: list[dict]) -> None:
    """Текущая фьючерсная 24ч-дельта по всем сканируемым парам (для колонки
    Δ в скринере). Полная замена снапшота."""
    try:
        from database import _get_db, utcnow
        col = _get_db().delta_state
        col.delete_many({})
        now = utcnow()
        if rows:
            col.insert_many([{**r, "updated_at": now} for r in rows])
    except Exception:
        logger.exception("[accum] delta_map store fail")


def store_snapshot(items: list[dict]) -> None:
    """Полная замена снапшота (актуальный список баз)."""
    try:
        from database import _get_db, utcnow
        col = _get_db().accum_state
        col.delete_many({})
        now = utcnow()
        if items:
            col.insert_many([{**it, "updated_at": now} for it in items])
    except Exception:
        logger.exception("[accum] store fail")
