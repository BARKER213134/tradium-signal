"""Бэктест "кластерных" сигналов — N+ сигналов на монете в одну сторону за окно T.

Матрица параметров:
  - window_h: 1, 2, 4    (окно сбора сигналов)
  - min_count: 2, 3, 4    (минимум сигналов для триггера)
  - src_mode: any, 2src, 3src  (требование разных источников)
  - horizon_h: 2, 4       (окно оценки исхода)
  - TP/SL: ±1.5% симметрично (baseline)

Дополнительно:
  - net_score вариант: (LONG - SHORT) >= N в окне
  - Разбор по комбинациям источников (какие тройки работают лучше)
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

BINANCE_SPOT = "https://data-api.binance.vision"
BINANCE_FUT = "https://fapi.binance.com"

TARGET_WIN = 1.5   # +/- % для win/loss по фикс порогу
TARGET_LOSS = 1.5

# Окно истории для сбора данных
HISTORY_HOURS = 96  # анализируем последние 96 часов

# Параметры матрицы
WINDOWS = [2, 4, 8]          # hours
MIN_COUNTS = [2, 3]
SRC_MODES = ["any", "2src"]
HORIZONS = [2, 4, 6]


def _sym(pair):
    return (pair or "").replace("/", "").replace("-", "").upper()


# ── Загрузка сигналов ────────────────────────────────────
def load_signals(hours_back: int):
    since = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=hours_back)
    sigs = []

    # Cryptovizor (pattern_triggered)
    for s in _signals().find({"source": "cryptovizor", "pattern_triggered": True,
                              "pattern_triggered_at": {"$gte": since},
                              "direction": {"$ne": None}}):
        pair = s.get("pair")
        price = s.get("pattern_price") or s.get("entry")
        if not pair or not price: continue
        sigs.append({"pair": pair, "sym": _sym(pair), "direction": s["direction"],
                     "price": float(price), "at": s["pattern_triggered_at"], "source": "cryptovizor"})

    # Tradium
    for s in _signals().find({"source": "tradium", "received_at": {"$gte": since},
                              "direction": {"$ne": None}}):
        pair = s.get("pair")
        price = s.get("entry")
        if not pair or not price: continue
        sigs.append({"pair": pair, "sym": _sym(pair), "direction": s["direction"],
                     "price": float(price), "at": s["received_at"], "source": "tradium"})

    # Anomaly
    for a in _anomalies().find({"detected_at": {"$gte": since}}):
        d = a.get("direction")
        if d not in ("LONG", "SHORT"): continue
        pair = a.get("pair") or a.get("symbol", "").replace("USDT", "/USDT")
        price = a.get("price")
        if not pair or not price: continue
        sigs.append({"pair": pair, "sym": _sym(pair), "direction": d,
                     "price": float(price), "at": a["detected_at"], "source": "anomaly"})

    # Confluence
    for c in _confluence().find({"detected_at": {"$gte": since}}):
        d = c.get("direction")
        if d not in ("LONG", "SHORT"): continue
        pair = c.get("pair") or c.get("symbol", "").replace("USDT", "/USDT")
        price = c.get("price")
        if not pair or not price: continue
        sigs.append({"pair": pair, "sym": _sym(pair), "direction": d,
                     "price": float(price), "at": c["detected_at"], "source": "confluence"})

    sigs.sort(key=lambda x: x["at"])
    return sigs


# ── Свечи ────────────────────────────────────────────────
_candles_cache = {}

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


async def preload_candles(symbols, hours_back):
    start_ms = int((datetime.now(timezone.utc) - timedelta(hours=hours_back + 12)).timestamp() * 1000)
    async with httpx.AsyncClient() as cli:
        syms = list(symbols)
        for i in range(0, len(syms), 20):
            batch = syms[i:i+20]
            tasks = [fetch_candles(cli, s, start_ms) for s in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for s, r in zip(batch, results):
                if isinstance(r, list): _candles_cache[s] = r
            if (i + 20) % 60 == 0 or i + 20 >= len(syms):
                print(f"   {min(i+20, len(syms))}/{len(syms)}", flush=True)


def evaluate_trade(sym: str, direction: str, entry_price: float, entry_ms: int, horizon_h: int):
    """Оценка trade: TP/SL с фиксированными ±1.5%.
    Возвращает 'WIN' / 'LOSS' / 'OPEN' и PnL%."""
    candles = _candles_cache.get(sym, [])
    if not candles: return None
    # Фильтр свечей от entry_ms до entry_ms + horizon
    end_ms = entry_ms + horizon_h * 3600 * 1000
    relevant = [c for c in candles if entry_ms <= c["t"] <= end_ms]
    if not relevant:
        # Может быть entry_ms попадает в середину свечи
        relevant = [c for c in candles if entry_ms - 3_600_000 <= c["t"] <= end_ms]
    if not relevant: return None

    is_long = direction == "LONG"
    tp_price = entry_price * (1 + TARGET_WIN/100 if is_long else 1 - TARGET_WIN/100)
    sl_price = entry_price * (1 - TARGET_LOSS/100 if is_long else 1 + TARGET_LOSS/100)

    for c in relevant:
        if is_long:
            if c["h"] >= tp_price: return "WIN", TARGET_WIN
            if c["l"] <= sl_price: return "LOSS", -TARGET_LOSS
        else:
            if c["l"] <= tp_price: return "WIN", TARGET_WIN
            if c["h"] >= sl_price: return "LOSS", -TARGET_LOSS
    # Не достигли — считаем PnL по последней close
    last_c = relevant[-1]["c"]
    pnl = (last_c - entry_price) / entry_price * 100
    if not is_long: pnl = -pnl
    return "OPEN", pnl


# ── Поиск кластеров ──────────────────────────────────────
def find_clusters(signals: list, window_h: int, min_count: int, src_mode: str):
    """Находит все триггеры (cluster): на какой-то паре в каком-то направлении
    в окне window_h накопилось min_count+ сигналов (с требованием по источникам).

    Возвращает: [{pair, sym, direction, trigger_at, trigger_price, window_sigs, sources_set}]
    """
    by_pair_dir = defaultdict(list)  # (sym, dir) -> [sig, ...]
    for s in signals:
        by_pair_dir[(s["sym"], s["direction"])].append(s)

    triggers = []
    for (sym, direction), arr in by_pair_dir.items():
        # Скользим по сигналам
        arr.sort(key=lambda x: x["at"])
        n = len(arr)
        last_trigger_ms = 0
        for i in range(n):
            end = arr[i]
            end_ms = end["at"].timestamp()
            # Собираем предыдущие сигналы в окне
            window_start_ms = end_ms - window_h * 3600
            window_sigs = [s for s in arr[:i+1] if s["at"].timestamp() >= window_start_ms]
            if len(window_sigs) < min_count:
                continue
            sources = {s["source"] for s in window_sigs}
            if src_mode == "2src" and len(sources) < 2:
                continue
            if src_mode == "3src" and len(sources) < 3:
                continue
            # Дедуп — не более одного триггера в 2 часа на паре
            if end_ms - last_trigger_ms < 2 * 3600:
                continue
            last_trigger_ms = end_ms
            pair = end["pair"]
            triggers.append({
                "pair": pair, "sym": sym, "direction": direction,
                "trigger_at": end["at"], "trigger_price": end["price"],
                "window_sigs_n": len(window_sigs),
                "sources": tuple(sorted(sources)),
            })
    return triggers


# ── Main ─────────────────────────────────────────────────
async def main():
    print(f"Загружаю сигналы за {HISTORY_HOURS}ч…", flush=True)
    signals = load_signals(HISTORY_HOURS)
    print(f"  Всего сигналов: {len(signals)}", flush=True)
    if not signals:
        return
    by_src = Counter(s["source"] for s in signals)
    for src, n in by_src.most_common():
        print(f"    {src}: {n}", flush=True)

    # Preload candles
    unique_syms = {s["sym"] for s in signals}
    print(f"\nЗагружаю свечи для {len(unique_syms)} монет…", flush=True)
    await preload_candles(unique_syms, HISTORY_HOURS)
    print(f"  Кэш: {len(_candles_cache)} пар", flush=True)

    # Baseline: одиночные сигналы (все, без кластеризации)
    print("\nBaseline: одиночные сигналы", flush=True)
    base_wins = base_loss = base_open = 0
    base_pnls = []
    for s in signals:
        entry_ms = int(s["at"].timestamp() * 1000)
        res = evaluate_trade(s["sym"], s["direction"], s["price"], entry_ms, 2)
        if res is None: continue
        out, pnl = res
        if out == "WIN": base_wins += 1
        elif out == "LOSS": base_loss += 1
        else: base_open += 1
        base_pnls.append(pnl)
    base_n = base_wins + base_loss + base_open
    base_wr = base_wins / max(base_wins + base_loss, 1) * 100
    base_avg = sum(base_pnls) / max(len(base_pnls), 1)
    base_sum = sum(base_pnls)
    print(f"  N={base_n} W={base_wins} L={base_loss} O={base_open} WR={base_wr:.1f}% AvgPnL={base_avg:+.2f}% SumPnL={base_sum:+.1f}%", flush=True)

    # Матрица
    print("\n" + "=" * 92, flush=True)
    print("МАТРИЦА КЛАСТЕРНЫХ СИГНАЛОВ", flush=True)
    print("=" * 92, flush=True)
    print(f"{'Win':<3} {'Min':<3} {'Sr':<4} {'Hor':<3}  {'Trig':>5} {'W':>4} {'L':>4} {'O':>4} {'WR%':>6} {'AvgPnL':>8} {'SumPnL':>8}", flush=True)
    print("-" * 92, flush=True)

    all_results = []
    for window_h in WINDOWS:
        for min_count in MIN_COUNTS:
            for src_mode in SRC_MODES:
                triggers = find_clusters(signals, window_h, min_count, src_mode)
                for horizon_h in HORIZONS:
                    wins = losses = opens = 0
                    pnls = []
                    source_combos = Counter()
                    dir_stats = Counter()
                    for t in triggers:
                        entry_ms = int(t["trigger_at"].timestamp() * 1000)
                        res = evaluate_trade(t["sym"], t["direction"], t["trigger_price"], entry_ms, horizon_h)
                        if res is None: continue
                        out, pnl = res
                        if out == "WIN": wins += 1
                        elif out == "LOSS": losses += 1
                        else: opens += 1
                        pnls.append(pnl)
                        source_combos[t["sources"]] += 1
                        dir_stats[t["direction"]] += 1
                    n_eval = wins + losses + opens
                    if n_eval == 0: continue
                    wr = wins / max(wins + losses, 1) * 100
                    avg = sum(pnls) / max(len(pnls), 1)
                    total = sum(pnls)
                    marker = "✅" if total > 0 and wr > base_wr else "  "
                    print(f"{marker}{window_h}h  {min_count:<3} {src_mode:<4} {horizon_h}h   {len(triggers):>5} {wins:>4} {losses:>4} {opens:>4} {wr:>5.1f}% {avg:>+7.2f}% {total:>+7.1f}%", flush=True)
                    all_results.append({
                        "window_h": window_h, "min_count": min_count, "src_mode": src_mode,
                        "horizon_h": horizon_h, "triggers": len(triggers),
                        "wins": wins, "losses": losses, "opens": opens,
                        "wr": wr, "avg": avg, "total": total,
                        "source_combos": dict(source_combos.most_common(5)),
                        "dir_stats": dict(dir_stats),
                    })

    # Топ комбинаций
    print("\n" + "=" * 92, flush=True)
    print("ТОП-15 ПО SUM PnL", flush=True)
    print("=" * 92, flush=True)
    print(f"{'#':<3} {'Win':<4} {'Min':<4} {'Src':<5} {'Hor':<4} {'Trig':>5} {'WR%':>6} {'AvgPnL':>8} {'SumPnL':>8}", flush=True)
    print("-" * 68, flush=True)
    all_results.sort(key=lambda x: -x["total"])
    for i, r in enumerate(all_results[:15]):
        print(f"{i+1:<3} {r['window_h']}h   {r['min_count']:<4} {r['src_mode']:<5} {r['horizon_h']}h   {r['triggers']:>5} {r['wr']:>5.1f}% {r['avg']:>+7.2f}% {r['total']:>+7.1f}%", flush=True)

    print("\n" + "=" * 92, flush=True)
    print("ТОП-15 ПО WIN RATE (N >= 5)", flush=True)
    print("=" * 92, flush=True)
    qualified = [r for r in all_results if r["triggers"] >= 5]
    qualified.sort(key=lambda x: -x["wr"])
    print(f"{'#':<3} {'Win':<4} {'Min':<4} {'Src':<5} {'Hor':<4} {'Trig':>5} {'WR%':>6} {'AvgPnL':>8} {'SumPnL':>8}", flush=True)
    print("-" * 68, flush=True)
    for i, r in enumerate(qualified[:15]):
        print(f"{i+1:<3} {r['window_h']}h   {r['min_count']:<4} {r['src_mode']:<5} {r['horizon_h']}h   {r['triggers']:>5} {r['wr']:>5.1f}% {r['avg']:>+7.2f}% {r['total']:>+7.1f}%", flush=True)

    # Лучшие source combos из топ комбинаций
    print("\n" + "=" * 92, flush=True)
    print("ТОП КОМБИНАЦИЙ ИСТОЧНИКОВ (из всех кластеров)", flush=True)
    print("=" * 92, flush=True)
    all_combos = Counter()
    for r in all_results:
        for combo, cnt in r["source_combos"].items():
            all_combos[combo] += cnt
    for combo, n in all_combos.most_common(15):
        print(f"  {n:>4}× {' + '.join(combo)}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
