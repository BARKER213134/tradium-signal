"""Разбор Confluence по score — какой score лучше отрабатывает."""
import logging
logging.basicConfig(level=logging.WARNING, force=True)
import sys
try: sys.stdout.reconfigure(encoding='utf-8')
except: pass

import asyncio
import httpx
from collections import defaultdict
from datetime import datetime, timezone, timedelta

from database import _confluence

BINANCE_SPOT = "https://data-api.binance.vision"
BINANCE_FUT = "https://fapi.binance.com"

TARGET_WIN = 1.5
TARGET_LOSS = 1.5


def _sym(pair):
    return (pair or "").replace("/", "").replace("-", "").upper()


async def fetch_candles(client, symbol, start_ms):
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    hours = max(1, (end_ms - start_ms) // (1000 * 3600) + 2)
    limit = min(max(hours, 6), 100)
    params = {"symbol": symbol, "interval": "1h", "startTime": start_ms, "endTime": end_ms, "limit": limit}
    for url in (f"{BINANCE_SPOT}/api/v3/klines", f"{BINANCE_FUT}/fapi/v1/klines"):
        try:
            r = await client.get(url, params=params, timeout=10)
            if r.status_code != 200: continue
            data = r.json()
            if not data: continue
            return [{"t": int(k[0]), "h": float(k[2]), "l": float(k[3]), "c": float(k[4])} for k in data]
        except: continue
    return []


async def main():
    since = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=24)
    docs = list(_confluence().find({"detected_at": {"$gte": since}, "direction": {"$ne": None}}))
    print(f"Загружено Confluence за 24ч: {len(docs)}")

    syms = {_sym(d.get("pair") or d.get("symbol", "").replace("USDT", "/USDT")) for d in docs}
    cache = {}
    print(f"Грузим свечи {len(syms)} монет...")
    async with httpx.AsyncClient() as cli:
        syms_list = list(syms)
        start_ms_global = int((datetime.now(timezone.utc) - timedelta(hours=30)).timestamp() * 1000)
        for i in range(0, len(syms_list), 20):
            batch = syms_list[i:i+20]
            tasks = [fetch_candles(cli, s, start_ms_global) for s in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for s, r in zip(batch, results):
                if isinstance(r, list): cache[s] = r
        print(f"   кэш: {len(cache)} пар")

    results = []
    for d in docs:
        pair = d.get("pair") or d.get("symbol", "").replace("USDT", "/USDT")
        sym = _sym(pair)
        entry = d.get("price")
        direction = d.get("direction")
        at = d.get("detected_at")
        score = d.get("score")
        strength = d.get("strength", "")
        st_passed = d.get("st_passed")
        pump_score = d.get("pump_score", 0)
        tp1 = d.get("r1") if direction == "LONG" else d.get("s1")
        sl_  = d.get("s1") if direction == "LONG" else d.get("r1")
        if not entry or not direction or not at: continue
        entry = float(entry)
        start_ms = int(at.timestamp() * 1000)
        candles = [c for c in cache.get(sym, []) if c["t"] + 3_600_000 >= start_ms]
        if not candles: continue

        is_long = direction == "LONG"
        outcome = "OPEN"; exit_price = None
        high_max = low_min = entry
        for c in candles:
            if c["h"] > high_max: high_max = c["h"]
            if c["l"] < low_min: low_min = c["l"]
            if is_long:
                if tp1 and c["h"] >= float(tp1): outcome = "TP"; exit_price = float(tp1); break
                if sl_ and c["l"] <= float(sl_): outcome = "SL"; exit_price = float(sl_); break
            else:
                if tp1 and c["l"] <= float(tp1): outcome = "TP"; exit_price = float(tp1); break
                if sl_ and c["h"] >= float(sl_): outcome = "SL"; exit_price = float(sl_); break
        if outcome == "OPEN":
            max_up = (high_max - entry) / entry * 100
            max_dn = (entry - low_min) / entry * 100
            if is_long:
                if max_up >= TARGET_WIN: outcome = "WIN"; exit_price = entry * (1 + TARGET_WIN/100)
                elif max_dn >= TARGET_LOSS: outcome = "LOSS"; exit_price = entry * (1 - TARGET_LOSS/100)
                else: exit_price = candles[-1]["c"]
            else:
                if max_dn >= TARGET_WIN: outcome = "WIN"; exit_price = entry * (1 - TARGET_WIN/100)
                elif max_up >= TARGET_LOSS: outcome = "LOSS"; exit_price = entry * (1 + TARGET_LOSS/100)
                else: exit_price = candles[-1]["c"]
        pnl = (exit_price - entry) / entry * 100
        if not is_long: pnl = -pnl
        results.append({
            "pair": pair, "direction": direction, "score": score, "strength": strength,
            "st_passed": st_passed, "pump_score": pump_score,
            "outcome": outcome, "pnl": pnl,
        })

    print(f"Проанализировано: {len(results)}\n")

    def stats(arr):
        if not arr: return (0, 0, 0, 0, 0, 0)
        w = sum(1 for r in arr if r["outcome"] in ("TP", "WIN"))
        l = sum(1 for r in arr if r["outcome"] in ("SL", "LOSS"))
        o = sum(1 for r in arr if r["outcome"] == "OPEN")
        n = w + l + o
        wr = w / max(w + l, 1) * 100
        avg = sum(r["pnl"] for r in arr) / max(n, 1)
        total = sum(r["pnl"] for r in arr)
        return (n, w, l, o, wr, avg, total)

    print("═" * 78)
    print("                    CONFLUENCE BY SCORE (24h)")
    print("═" * 78)
    print(f"{'Score':<8} {'N':>5} {'W':>5} {'L':>5} {'O':>4} {'WR%':>7} {'AvgPnL':>9} {'SumPnL':>9}")
    print("-" * 78)
    for score in sorted({r["score"] for r in results if r["score"] is not None}):
        arr = [r for r in results if r["score"] == score]
        n, w, l, o, wr, avg, total = stats(arr)
        marker = "✅" if total > 0 else "❌" if total < -5 else "  "
        print(f"{marker} {score:<5} {n:>5} {w:>5} {l:>5} {o:>4} {wr:>6.1f}% {avg:>+8.2f}% {total:>+8.1f}%")

    print("\n══════ BY SCORE + DIRECTION ══════")
    print(f"{'Score':<5} {'Dir':<6} {'N':>5} {'W':>5} {'L':>5} {'WR%':>7} {'AvgPnL':>9} {'SumPnL':>9}")
    print("-" * 62)
    for score in sorted({r["score"] for r in results if r["score"] is not None}):
        for d in ("LONG", "SHORT"):
            arr = [r for r in results if r["score"] == score and r["direction"] == d]
            if not arr: continue
            n, w, l, o, wr, avg, total = stats(arr)
            emoji = "🟢" if d == "LONG" else "🔴"
            print(f"{score:<5} {emoji} {d:<4} {n:>5} {w:>5} {l:>5} {wr:>6.1f}% {avg:>+8.2f}% {total:>+8.1f}%")

    print("\n══════ BY STRENGTH ══════")
    print(f"{'Strength':<12} {'N':>5} {'WR%':>7} {'AvgPnL':>9} {'SumPnL':>9}")
    print("-" * 48)
    for strength in ("STRONG", "MEDIUM", "WEAK", ""):
        arr = [r for r in results if r["strength"] == strength]
        if not arr: continue
        n, w, l, o, wr, avg, total = stats(arr)
        print(f"{strength or '(none)':<12} {n:>5} {wr:>6.1f}% {avg:>+8.2f}% {total:>+8.1f}%")

    print("\n══════ BY KC (st_passed) ══════")
    print(f"{'KC':<12} {'N':>5} {'WR%':>7} {'AvgPnL':>9} {'SumPnL':>9}")
    print("-" * 48)
    for kc_val, label in [(True, "✅ Passed"), (False, "❌ Failed"), (None, "— N/A")]:
        arr = [r for r in results if r["st_passed"] == kc_val]
        if not arr: continue
        n, w, l, o, wr, avg, total = stats(arr)
        print(f"{label:<12} {n:>5} {wr:>6.1f}% {avg:>+8.2f}% {total:>+8.1f}%")

    print("\n══════ BY SCORE + KC ══════")
    print(f"{'Score':<5} {'KC':<11} {'N':>5} {'WR%':>7} {'AvgPnL':>9} {'SumPnL':>9}")
    print("-" * 52)
    for score in sorted({r["score"] for r in results if r["score"] is not None}):
        for kc_val, label in [(True, "✅ passed"), (False, "❌ failed")]:
            arr = [r for r in results if r["score"] == score and r["st_passed"] == kc_val]
            if not arr: continue
            n, w, l, o, wr, avg, total = stats(arr)
            print(f"{score:<5} {label:<11} {n:>5} {wr:>6.1f}% {avg:>+8.2f}% {total:>+8.1f}%")


if __name__ == "__main__":
    asyncio.run(main())
