"""Бэктест сигналов за последние 24 часа.

Для каждого сигнала (CV / Anomaly / Confluence / Tradium) за 24ч:
  1. Берём entry price и timestamp
  2. Запрашиваем свечи Binance с момента сигнала до сейчас
  3. Считаем:
     - max_up = (highest high - entry) / entry * 100
     - max_dn = (entry - lowest low) / entry * 100
     - pnl_tp_sl: если есть tp1/sl — достигал ли
     - pnl_current: текущая цена
  4. Оценка:
     LONG: win если max_up >= 1.5% и не достиг SL раньше
     SHORT: win если max_dn >= 1.5%
  5. Также считаем что было бы с Reversal Meter фильтром:
     - "С сигналом" vs "Против" vs "Нейтрально"

Отчёт: по источникам, по монетам, общий PnL.
"""
import logging
logging.basicConfig(level=logging.WARNING, force=True)
import sys
try: sys.stdout.reconfigure(encoding='utf-8')
except: pass

import asyncio
import httpx
from collections import defaultdict, Counter
from datetime import datetime, timezone, timedelta

from database import _signals, _anomalies, _confluence
from reversal_meter import compute_score, agreement

BINANCE_SPOT = "https://data-api.binance.vision"
BINANCE_FUT = "https://fapi.binance.com"

TARGET_WIN_PCT = 1.5
TARGET_LOSS_PCT = 1.5


def _sym(pair: str) -> str:
    return (pair or "").replace("/", "").replace("-", "").upper()


# Кэш: sym → list of 1h candles за последние 30 часов
_candles_cache: dict[str, list] = {}


async def fetch_candles_async(client: httpx.AsyncClient, symbol: str, start_ms: int):
    """Один асинхронный запрос в Binance (пробует Spot, потом Futures)."""
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    hours = max(1, (end_ms - start_ms) // (1000 * 3600) + 2)
    limit = min(max(hours, 6), 100)
    params = {"symbol": symbol, "interval": "1h", "startTime": start_ms, "endTime": end_ms, "limit": limit}
    for url in (f"{BINANCE_SPOT}/api/v3/klines", f"{BINANCE_FUT}/fapi/v1/klines"):
        try:
            r = await client.get(url, params=params, timeout=10)
            if r.status_code != 200:
                continue
            data = r.json()
            if not data:
                continue
            return [{"t": int(k[0]), "o": float(k[1]), "h": float(k[2]), "l": float(k[3]), "c": float(k[4])} for k in data]
        except Exception:
            continue
    return []


async def preload_candles(symbols: set, hours: int = 30):
    """Грузим свечи для всех пар ПАРАЛЛЕЛЬНО (batch 20)."""
    now = datetime.now(timezone.utc)
    start_ms = int((now - timedelta(hours=hours)).timestamp() * 1000)
    async with httpx.AsyncClient() as client:
        syms = list(symbols)
        for batch_start in range(0, len(syms), 20):
            batch = syms[batch_start: batch_start + 20]
            tasks = [fetch_candles_async(client, s, start_ms) for s in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for s, r in zip(batch, results):
                if isinstance(r, list):
                    _candles_cache[s] = r
            print(f"   {batch_start + len(batch)}/{len(syms)} pairs loaded", flush=True)


def get_candles_from_cache(pair: str, start_ms: int) -> list:
    sym = _sym(pair)
    cached = _candles_cache.get(sym, [])
    if not cached:
        return []
    # Отсекаем свечи до start_ms (start_ms — момент сигнала)
    return [c for c in cached if c["t"] + 3_600_000 >= start_ms]  # +1h чтобы захватить свечу в момент сигнала


def analyze_signal(sig: dict) -> dict | None:
    """sig: {pair, direction, entry, at, tp1, sl, source}
    Возвращает stats по сигналу либо None если нет свечей."""
    pair = sig.get("pair") or ""
    direction = sig.get("direction") or ""
    entry = sig.get("entry")
    at = sig.get("at")
    tp1 = sig.get("tp1")
    sl = sig.get("sl")
    if not pair or not direction or entry is None or at is None:
        return None
    try:
        entry = float(entry)
    except: return None
    if entry <= 0: return None

    start_ms = int(at.timestamp() * 1000) if hasattr(at, "timestamp") else int(at * 1000)
    candles = get_candles_from_cache(pair, start_ms)
    if not candles:
        return None

    # Проверяем достижение TP/SL в хронологическом порядке
    is_long = direction in ("LONG", "BUY")
    outcome = "OPEN"  # OPEN/TP/SL/WIN/LOSS
    exit_price = None
    exit_time = None

    high_max = entry
    low_min = entry
    for c in candles:
        if c["h"] > high_max: high_max = c["h"]
        if c["l"] < low_min: low_min = c["l"]

        if is_long:
            # TP1 достигнут?
            if tp1 is not None and c["h"] >= float(tp1):
                outcome = "TP"; exit_price = float(tp1); exit_time = c["t"]
                break
            if sl is not None and c["l"] <= float(sl):
                outcome = "SL"; exit_price = float(sl); exit_time = c["t"]
                break
        else:  # SHORT
            if tp1 is not None and c["l"] <= float(tp1):
                outcome = "TP"; exit_price = float(tp1); exit_time = c["t"]
                break
            if sl is not None and c["h"] >= float(sl):
                outcome = "SL"; exit_price = float(sl); exit_time = c["t"]
                break

    # Если нет TP/SL — оцениваем по max_up/max_dn vs TARGET
    if outcome == "OPEN":
        max_up_pct = (high_max - entry) / entry * 100
        max_dn_pct = (entry - low_min) / entry * 100
        if is_long:
            if max_up_pct >= TARGET_WIN_PCT:
                outcome = "WIN"
                exit_price = entry * (1 + TARGET_WIN_PCT / 100)
            elif max_dn_pct >= TARGET_LOSS_PCT:
                outcome = "LOSS"
                exit_price = entry * (1 - TARGET_LOSS_PCT / 100)
            else:
                # Текущая цена
                exit_price = candles[-1]["c"]
        else:
            if max_dn_pct >= TARGET_WIN_PCT:
                outcome = "WIN"
                exit_price = entry * (1 - TARGET_WIN_PCT / 100)
            elif max_up_pct >= TARGET_LOSS_PCT:
                outcome = "LOSS"
                exit_price = entry * (1 + TARGET_LOSS_PCT / 100)
            else:
                exit_price = candles[-1]["c"]

    pnl_pct = (exit_price - entry) / entry * 100
    if not is_long:
        pnl_pct = -pnl_pct

    return {
        "pair": pair, "direction": direction, "source": sig.get("source", ""),
        "at": at, "entry": entry, "exit": exit_price,
        "outcome": outcome, "pnl_pct": pnl_pct,
        "max_up_pct": (high_max - entry) / entry * 100,
        "max_dn_pct": (entry - low_min) / entry * 100,
        "reversal_agreement": sig.get("reversal_agreement", "neutral"),
        "reversal_score_at_signal": sig.get("reversal_score_at_signal", 0),
    }


def collect_signals_24h():
    """Возвращает список сигналов за последние 24 часа (unified format)."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    since = now - timedelta(hours=24)
    out = []

    # Tradium
    for s in _signals().find({"source": "tradium", "received_at": {"$gte": since}}):
        out.append({
            "source": "tradium", "pair": s.get("pair"), "direction": s.get("direction"),
            "entry": s.get("entry"), "tp1": s.get("tp1"), "sl": s.get("sl"),
            "at": s.get("received_at"),
        })
    # Cryptovizor (pattern_triggered)
    for s in _signals().find({"source": "cryptovizor", "pattern_triggered": True,
                              "pattern_triggered_at": {"$gte": since}}):
        out.append({
            "source": "cryptovizor", "pair": s.get("pair"), "direction": s.get("direction"),
            "entry": s.get("pattern_price") or s.get("entry"),
            "tp1": s.get("dca2"), "sl": s.get("dca1"),
            "at": s.get("pattern_triggered_at"),
        })
    # Anomaly
    for a in _anomalies().find({"detected_at": {"$gte": since}}):
        d = a.get("direction")
        if d not in ("LONG", "SHORT"): continue
        out.append({
            "source": "anomaly", "pair": a.get("pair") or a.get("symbol", "").replace("USDT", "/USDT"),
            "direction": d, "entry": a.get("price"), "tp1": None, "sl": None,
            "at": a.get("detected_at"),
        })
    # Confluence
    for c in _confluence().find({"detected_at": {"$gte": since}}):
        d = c.get("direction")
        if d not in ("LONG", "SHORT"): continue
        out.append({
            "source": "confluence", "pair": c.get("pair") or c.get("symbol", "").replace("USDT", "/USDT"),
            "direction": d, "entry": c.get("price"),
            "tp1": c.get("r1") if d == "LONG" else c.get("s1"),
            "sl": c.get("s1") if d == "LONG" else c.get("r1"),
            "at": c.get("detected_at"),
        })
    return out


def enrich_with_reversal(signals: list) -> None:
    """Для каждого сигнала считаем Reversal Meter в момент сигнала.
    Один раз загружаем историю CV/Conf/Anomaly за 30ч и фильтруем в памяти."""
    print(f"[1/3] Загрузка контекста для Reversal Meter...", flush=True)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    ctx_since = now - timedelta(hours=30)
    cv_all = list(_signals().find(
        {"source": "cryptovizor", "pattern_triggered": True,
         "pattern_triggered_at": {"$gte": ctx_since},
         "direction": {"$ne": None}},
        {"pair": 1, "direction": 1, "pattern_name": 1, "pattern_triggered_at": 1, "_id": 0}
    ))
    cf_all = list(_confluence().find(
        {"detected_at": {"$gte": ctx_since}, "direction": {"$ne": None}},
        {"symbol": 1, "direction": 1, "detected_at": 1, "_id": 0}
    ))
    an_all = list(_anomalies().find(
        {"detected_at": {"$gte": ctx_since}},
        {"symbol": 1, "anomalies": 1, "detected_at": 1, "_id": 0}
    ))
    print(f"   контекст: CV={len(cv_all)} Conf={len(cf_all)} An={len(an_all)}", flush=True)
    print(f"   обсчёт {len(signals)} сигналов...", flush=True)
    for i, sig in enumerate(signals):
        if (i+1) % 100 == 0:
            print(f"   ...{i+1}/{len(signals)}", flush=True)
        try:
            m = compute_score(sig["at"],
                              cv_preloaded=cv_all, cf_preloaded=cf_all, an_preloaded=an_all)
            sig["reversal_score_at_signal"] = m["score"]
            sig["reversal_agreement"] = agreement(sig["direction"], m["direction"])
        except Exception:
            sig["reversal_score_at_signal"] = 0
            sig["reversal_agreement"] = "neutral"


async def main():
    print("Загрузка сигналов за 24 часа...")
    signals = collect_signals_24h()
    # Фильтр: исключаем битые пары
    signals = [s for s in signals if s.get("pair") and s.get("entry") and s.get("direction") in ("LONG", "SHORT")]
    print(f"Найдено: {len(signals)} сигналов")
    by_src = Counter(s["source"] for s in signals)
    for src, cnt in by_src.most_common():
        print(f"   {src}: {cnt}")
    if not signals:
        return

    enrich_with_reversal(signals)

    # Preload свечей для всех уникальных пар
    unique_syms = {_sym(s["pair"]) for s in signals if s.get("pair")}
    print(f"\n[2/3] Параллельная загрузка свечей для {len(unique_syms)} монет...", flush=True)
    await preload_candles(unique_syms, hours=30)
    print(f"   кэш: {len(_candles_cache)} пар со свечами", flush=True)

    print(f"\n[3/3] Анализ PnL...", flush=True)
    results = []
    skipped = 0
    for sig in signals:
        r = analyze_signal(sig)
        if r is None:
            skipped += 1
            continue
        results.append(r)

    print(f"\nУспешно проанализировано: {len(results)}  (skipped: {skipped})", flush=True)
    print("=" * 100)

    # ── Общая сводка ─────────────────────────────────────
    total = len(results)
    if total == 0:
        print("Нет результатов"); return

    def winrate(arr):
        wins = sum(1 for r in arr if r["outcome"] in ("TP", "WIN"))
        losses = sum(1 for r in arr if r["outcome"] in ("SL", "LOSS"))
        opens = sum(1 for r in arr if r["outcome"] == "OPEN")
        total = wins + losses + opens
        wr = wins / max(wins + losses, 1) * 100
        avg_pnl = sum(r["pnl_pct"] for r in arr) / max(len(arr), 1)
        total_pnl = sum(r["pnl_pct"] for r in arr)
        return wr, avg_pnl, total_pnl, wins, losses, opens, total

    print("\n──────────── ОБЩАЯ СВОДКА ────────────")
    wr, avg, tot, w, l, o, n = winrate(results)
    print(f"Сигналов: {n} | Wins: {w} | Losses: {l} | Open: {o}")
    print(f"Win rate: {wr:.1f}% | Avg PnL: {avg:+.2f}% | Сумма: {tot:+.1f}%")

    # ── По источникам ────────────────────────────────────
    print("\n──────────── ПО ИСТОЧНИКАМ ────────────")
    print(f"{'Источник':<14} {'N':>4} {'W':>4} {'L':>4} {'O':>4} {'WR%':>6} {'AvgPnL':>8} {'Sum PnL':>10}")
    print("-" * 62)
    for src in ["tradium", "cryptovizor", "anomaly", "confluence"]:
        arr = [r for r in results if r["source"] == src]
        if not arr: continue
        wr, avg, tot, w, l, o, n = winrate(arr)
        print(f"{src:<14} {n:>4} {w:>4} {l:>4} {o:>4} {wr:>5.1f}% {avg:>+7.2f}% {tot:>+9.1f}%")

    # ── С Reversal Meter фильтром ────────────────────────
    print("\n──────────── REVERSAL METER ФИЛЬТР ────────────")
    with_rev = [r for r in results if r["reversal_agreement"] == "with"]
    against = [r for r in results if r["reversal_agreement"] == "against"]
    neutral = [r for r in results if r["reversal_agreement"] == "neutral"]
    for label, arr in [("✅ С сигналом (with)", with_rev),
                        ("⚠️  Против (against)", against),
                        ("⚪ Нейтрально (neutral)", neutral)]:
        if not arr:
            print(f"{label:<30} (нет данных)")
            continue
        wr, avg, tot, w, l, o, n = winrate(arr)
        print(f"{label:<30} N={n:>3} WR={wr:>5.1f}% AvgPnL={avg:>+6.2f}% Sum={tot:>+7.1f}%")

    # ── По направлению ───────────────────────────────────
    print("\n──────────── ПО НАПРАВЛЕНИЮ ────────────")
    for d in ["LONG", "SHORT"]:
        arr = [r for r in results if r["direction"] == d]
        if not arr: continue
        wr, avg, tot, w, l, o, n = winrate(arr)
        icon = "🟢" if d == "LONG" else "🔴"
        print(f"{icon} {d:<6} N={n:>3} W={w:>3} L={l:>3} WR={wr:>5.1f}% AvgPnL={avg:>+6.2f}% Sum={tot:>+7.1f}%")

    # ── По монетам ───────────────────────────────────────
    print("\n──────────── ТОП 20 МОНЕТ (по количеству сигналов) ────────────")
    by_pair = defaultdict(list)
    for r in results:
        by_pair[r["pair"]].append(r)
    pair_stats = []
    for pair, arr in by_pair.items():
        wr, avg, tot, w, l, o, n = winrate(arr)
        pair_stats.append((pair, n, w, l, o, wr, avg, tot))
    pair_stats.sort(key=lambda x: -x[1])
    print(f"{'Монета':<18} {'N':>3} {'W':>3} {'L':>3} {'O':>3} {'WR%':>6} {'AvgPnL':>8} {'Sum':>8}")
    print("-" * 64)
    for pair, n, w, l, o, wr, avg, tot in pair_stats[:20]:
        print(f"{pair.replace('/USDT',''):<18} {n:>3} {w:>3} {l:>3} {o:>3} {wr:>5.1f}% {avg:>+7.2f}% {tot:>+7.1f}%")

    # ── Лучшие/худшие монеты по Sum PnL (min 2 сигнала) ──
    qualified = [x for x in pair_stats if x[1] >= 2]
    if qualified:
        qualified.sort(key=lambda x: -x[7])
        print("\n──────────── ТОП 10 ПО PnL (мин 2 сигнала) ────────────")
        print(f"{'Монета':<18} {'N':>3} {'WR%':>6} {'AvgPnL':>8} {'Sum':>8}")
        print("-" * 52)
        for pair, n, w, l, o, wr, avg, tot in qualified[:10]:
            print(f"{pair.replace('/USDT',''):<18} {n:>3} {wr:>5.1f}% {avg:>+7.2f}% {tot:>+7.1f}%")

        print("\n──────────── АНТИ-ТОП 10 (убыточные) ────────────")
        for pair, n, w, l, o, wr, avg, tot in sorted(qualified, key=lambda x: x[7])[:10]:
            print(f"{pair.replace('/USDT',''):<18} {n:>3} {wr:>5.1f}% {avg:>+7.2f}% {tot:>+7.1f}%")


if __name__ == "__main__":
    asyncio.run(main())
