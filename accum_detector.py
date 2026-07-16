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


def _rsi4h_bull(candles: list[dict]) -> Optional[bool]:
    """RSI14(4h) выше своей SMA14? Для ширины рынка (/api/market-side).
    4h-ресемпл из 1h свечей, Wilder RSI."""
    try:
        closes = []
        cur_key = None
        for c in candles:
            k = c["t"] // (4 * 3600_000)
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
        return rsis[-1] > sma
    except Exception:
        return None


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
    ds_fired = 0
    cand_rows = []
    for sym in pairs:
        pair = sym.replace("USDT", "/USDT") if "/" not in sym else sym
        try:
            # один фьючерсный запрос на пару: и база, и текущая 24ч-дельта
            kd = _fetch_klines_delta(pair, 320)
            checked += 1
            if kd and len(kd) >= 260:
                _bull = _rsi4h_bull(kd)
                if _bull is not None:
                    breadth_tot += 1
                    if _bull:
                        breadth_bull += 1
                # 💠 серия дельт — инфо-сигнал в журнал (кулдаун 24ч на пару)
                _ds = _delta_series_sig(pair, kd)
                if _ds is not None:
                    try:
                        from impulse_detector import store_signal
                        if store_signal(_ds, cooldown_h=24):
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
                    cand_rows.append({"pair": pair,
                                      "symbol": pair.replace("/", "").upper(),
                                      "atr_pct": round(_atrp, 2),
                                      "mom24": round(_mom, 2),
                                      "vol_ratio": round(_vr, 2),
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
