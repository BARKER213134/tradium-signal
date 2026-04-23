"""Бэктест влияния Key Levels (Tradium topics 3086/3088/3091) на WR сигналов.

Не меняет live систему — только анализ исторических данных.

Логика:
1. Тянем все сигналы за 14 дней (CV/Cluster/Confluence/Anomaly)
2. Тянем Key Levels сообщения из 3 топиков через endpoint
3. Для каждого сигнала находим KL события на той же паре в окне ±2ч
4. Классифицируем: ALIGNED / CONFIRMING / NEUTRAL / CONFLICTING
5. Для каждой группы симуляция TP/SL (TP 3% / SL 2% / hold 48h)
6. Отчёт: матрица [источник × KL группа]
"""
import os
import sys
import io
import re
import time
import httpx
from datetime import datetime, timedelta
from collections import Counter, defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from dotenv import load_dotenv
load_dotenv()

from database import _signals, _confluence, _clusters, _anomalies, utcnow
from exchange import get_klines_any

BASE = "https://tradium-signal-production.up.railway.app"
TP_PCT = 3.0
SL_PCT = 2.0
DAYS = 14
KL_WINDOW_H = 2

print(f"[{time.strftime('%H:%M:%S')}] === BACKTEST KEY LEVELS ===")
print(f"Окно: {DAYS} дней  |  TP {TP_PCT}% / SL {SL_PCT}% / hold 48h  |  KL окно ±{KL_WINDOW_H}ч")
print()

since = utcnow() - timedelta(days=DAYS)

# ═══════════════════════════════════════════════
# ШАГ 1: Собираем все сигналы
# ═══════════════════════════════════════════════
t0 = time.time()
print(f"[{time.strftime('%H:%M:%S')}] Шаг 1: собираем сигналы из БД...")

signals = []

# CV
for s in _signals().find({
    "source": "cryptovizor", "pattern_triggered": True,
    "pattern_triggered_at": {"$gte": since},
}):
    entry = s.get("pattern_price") or s.get("entry")
    if not entry or not s.get("direction"):
        continue
    signals.append({
        "src": "CV",
        "pair": s.get("pair", ""),
        "direction": s.get("direction", "").upper(),
        "entry": float(entry),
        "at": s.get("pattern_triggered_at"),
        "is_top_pick": bool(s.get("is_top_pick")),
        "ai_score": s.get("ai_score"),
    })

# Cluster
for c in _clusters().find({"trigger_at": {"$gte": since}}):
    if not c.get("trigger_price") or not c.get("direction"):
        continue
    signals.append({
        "src": "Cluster",
        "pair": c.get("pair", ""),
        "direction": c.get("direction", "").upper(),
        "entry": float(c["trigger_price"]),
        "at": c.get("trigger_at"),
        "is_top_pick": bool(c.get("is_top_pick")),
        "strength": c.get("strength"),
    })

# Confluence
for cf in _confluence().find({"detected_at": {"$gte": since}}):
    if not cf.get("price") or not cf.get("direction"):
        continue
    signals.append({
        "src": "Confluence",
        "pair": cf.get("pair") or cf.get("symbol", ""),
        "direction": cf.get("direction", "").upper(),
        "entry": float(cf["price"]),
        "at": cf.get("detected_at"),
        "is_top_pick": bool(cf.get("is_top_pick")),
        "score": cf.get("score"),
    })

# Anomaly
for a in _anomalies().find({"detected_at": {"$gte": since}}):
    if not a.get("price") or not a.get("direction"):
        continue
    signals.append({
        "src": "Anomaly",
        "pair": a.get("pair") or a.get("symbol", ""),
        "direction": a.get("direction", "").upper(),
        "entry": float(a["price"]),
        "at": a.get("detected_at"),
        "score": a.get("score"),
    })

# Нормализуем pair (BNB/USDT или BNBUSDT → BNBUSDT)
for s in signals:
    p = s["pair"].replace("/", "").upper()
    if not p.endswith("USDT"):
        p = p + "USDT"
    s["pair_norm"] = p

by_src = Counter(s["src"] for s in signals)
print(f"  Всего сигналов: {len(signals)}")
for src, n in by_src.most_common():
    print(f"    {src}: {n}")
print(f"  [время: {time.time() - t0:.1f}с]")

# ═══════════════════════════════════════════════
# ШАГ 2: Тянем Key Levels из топиков через endpoint
# ═══════════════════════════════════════════════
t1 = time.time()
print(f"\n[{time.strftime('%H:%M:%S')}] Шаг 2: тянем Key Levels из Telegram...")

TOPICS = {3086: "SUPPORT", 3088: "RANGES", 3091: "RESISTANCE"}
key_levels = []  # list of dicts
LIMIT = 2000

for tid, name in TOPICS.items():
    try:
        r = httpx.get(f"{BASE}/api/peek-tradium-topic",
                       params={"topic_id": tid, "limit": LIMIT},
                       timeout=120)
        d = r.json()
        if not d.get("ok"):
            print(f"  ⚠ {name} (topic {tid}): error {d.get('error')}")
            continue
        msgs = d.get("messages", [])
        print(f"  {name} (topic {tid}): получено {len(msgs)} сообщений")
        for m in msgs:
            m["_topic_name"] = name
            key_levels.append(m)
    except Exception as e:
        print(f"  ⚠ {name} fetch fail: {e}")

print(f"  Всего KL сообщений: {len(key_levels)}")
print(f"  [время: {time.time() - t1:.1f}с]")

# ═══════════════════════════════════════════════
# ШАГ 3: Парсим KL сообщения
# ═══════════════════════════════════════════════
t2 = time.time()
print(f"\n[{time.strftime('%H:%M:%S')}] Шаг 3: парсим Key Levels...")

def parse_kl(msg):
    text = msg.get("text", "") or ""
    date_str = msg.get("date", "")
    topic = msg.get("_topic_name", "")
    if not text:
        return None
    try:
        at = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        at = at.replace(tzinfo=None)  # naive UTC
    except Exception:
        return None

    # Symbol
    sym_m = re.search(r"Symbol:\s*#?(\w+)", text)
    if not sym_m:
        return None
    pair_norm = sym_m.group(1).upper()
    if not pair_norm.endswith("USDT"):
        pair_norm = pair_norm + "USDT"

    # TF
    tf_m = re.search(r"Timeframe:\s*(\w+)", text)
    tf = tf_m.group(1) if tf_m else "?"

    # Price
    cp_m = re.search(r"Current Price:\s*([\d.]+)", text)
    current_price = float(cp_m.group(1)) if cp_m else None

    # Зона (для SUPPORT/RESISTANCE: "Zone: X - Y", для RANGES: отдельно)
    # Событие:
    event = "unknown"
    if "Price Entered SUPPORT" in text or "Price Entered Support" in text:
        event = "entered_support"
    elif "Price Entered RESISTANCE" in text or "Price Entered Resistance" in text:
        event = "entered_resistance"
    elif "New SUPPORT Level" in text or "New Support" in text:
        event = "new_support"
    elif "New RESISTANCE Level" in text or "New Resistance" in text:
        event = "new_resistance"
    elif "RANGE Detected" in text:
        event = "range"

    zone_low = zone_high = None
    if event in ("entered_support", "entered_resistance", "new_support", "new_resistance"):
        z_m = re.search(r"Zone:\s*([\d.]+)\s*-\s*([\d.]+)", text)
        if z_m:
            zone_low = float(z_m.group(1))
            zone_high = float(z_m.group(2))
    elif event == "range":
        sz_m = re.search(r"Support Zone:\s*([\d.]+)", text)
        rz_m = re.search(r"Resistance Zone:\s*([\d.]+)", text)
        if sz_m and rz_m:
            zone_low = float(sz_m.group(1))
            zone_high = float(rz_m.group(1))

    return {
        "pair": pair_norm,
        "tf": tf,
        "event": event,
        "topic": topic,
        "zone_low": zone_low,
        "zone_high": zone_high,
        "current_price": current_price,
        "at": at,
    }

parsed_kl = []
for m in key_levels:
    p = parse_kl(m)
    if p:
        parsed_kl.append(p)

by_event = Counter(k["event"] for k in parsed_kl)
print(f"  Распарсено: {len(parsed_kl)} из {len(key_levels)}")
for ev, n in by_event.most_common():
    print(f"    {ev}: {n}")

# Индекс: pair → list of KL (sorted by at)
kl_by_pair = defaultdict(list)
for k in parsed_kl:
    kl_by_pair[k["pair"]].append(k)
for pair in kl_by_pair:
    kl_by_pair[pair].sort(key=lambda x: x["at"])
print(f"  Уникальных пар с KL: {len(kl_by_pair)}")
print(f"  [время: {time.time() - t2:.1f}с]")

# ═══════════════════════════════════════════════
# ШАГ 4: Классифицируем каждый сигнал
# ═══════════════════════════════════════════════
t3 = time.time()
print(f"\n[{time.strftime('%H:%M:%S')}] Шаг 4: классифицируем сигналы по KL группам...")

def classify_signal(sig):
    """Возвращает ALIGNED / CONFIRMING / NEUTRAL / CONFLICTING."""
    pair = sig["pair_norm"]
    direction = sig["direction"]
    at = sig["at"]
    if not at or pair not in kl_by_pair:
        return "NEUTRAL"
    start = at - timedelta(hours=KL_WINDOW_H)
    end = at + timedelta(hours=KL_WINDOW_H)
    nearby = [k for k in kl_by_pair[pair] if start <= k["at"] <= end]
    if not nearby:
        return "NEUTRAL"
    # Приоритет: ALIGNED > CONFIRMING > CONFLICTING
    status = "NEUTRAL"
    for k in nearby:
        ev = k["event"]
        # ALIGNED: Price Entered в сторону сигнала
        if direction == "LONG" and ev == "entered_support":
            return "ALIGNED"
        if direction == "SHORT" and ev == "entered_resistance":
            return "ALIGNED"
        # CONFLICTING
        if direction == "LONG" and ev == "entered_resistance":
            status = "CONFLICTING"
        if direction == "SHORT" and ev == "entered_support":
            status = "CONFLICTING"
        # CONFIRMING: New Level в сторону сигнала
        if status != "CONFLICTING":
            if direction == "LONG" and ev == "new_support":
                status = "CONFIRMING"
            if direction == "SHORT" and ev == "new_resistance":
                status = "CONFIRMING"
    return status

for sig in signals:
    sig["kl_class"] = classify_signal(sig)

dist = Counter(s["kl_class"] for s in signals)
print(f"  Распределение:")
for k, n in dist.most_common():
    print(f"    {k}: {n}  ({n*100/len(signals):.1f}%)")
print(f"  [время: {time.time() - t3:.1f}с]")

# ═══════════════════════════════════════════════
# ШАГ 5: Backtest TP/SL
# ═══════════════════════════════════════════════
t4 = time.time()
print(f"\n[{time.strftime('%H:%M:%S')}] Шаг 5: fetch свечей Binance с кешем...")

# Кеш свечей по паре (один fetch на пару — 100 свечей 1h = ~4 дня покрывают все сигналы если недавние)
# Для 14-дневного бэктеста — нужно limit=400 (16 дней в 1h)
candle_cache = {}
unique_pairs = set(s["pair_norm"] for s in signals)
print(f"  Уникальных пар для fetch: {len(unique_pairs)}")

def fetch_pair_candles(sym):
    if sym in candle_cache:
        return candle_cache[sym]
    try:
        cs = get_klines_any(sym, "1h", limit=500)
        out = []
        for c in cs or []:
            ts = c.get("t") or c.get("time")
            if not ts:
                continue
            if ts > 10**12:
                ts = ts // 1000
            out.append({"t": ts, "h": c["h"], "l": c["l"], "c": c["c"]})
        candle_cache[sym] = out
        return out
    except Exception as e:
        candle_cache[sym] = []
        return []

done = 0
for pair in unique_pairs:
    fetch_pair_candles(pair)
    done += 1
    if done % 50 == 0:
        print(f"    {done}/{len(unique_pairs)} ({time.time() - t4:.1f}с)")

total_fetched = sum(1 for cs in candle_cache.values() if cs)
print(f"  Загружено {total_fetched} из {len(unique_pairs)} пар")
print(f"  [время: {time.time() - t4:.1f}с]")

# Теперь симуляция для каждого сигнала
t5 = time.time()
print(f"\n[{time.strftime('%H:%M:%S')}] Шаг 6: симуляция TP/SL для {len(signals)} сигналов...")

def simulate(sig):
    pair = sig["pair_norm"]
    direction = sig["direction"]
    entry = sig["entry"]
    at = sig["at"]
    if not at:
        return {"result": "NO_DATA", "pnl": None}
    candles = candle_cache.get(pair) or []
    if not candles:
        return {"result": "NO_DATA", "pnl": None}
    entry_ts = int(at.timestamp())
    # Только свечи после entry
    cs = [c for c in candles if c["t"] >= entry_ts][:48]
    if not cs:
        return {"result": "NO_DATA", "pnl": None}
    is_long = direction in ("LONG", "BUY", "BULLISH")
    if is_long:
        tp, sl = entry * (1 + TP_PCT / 100), entry * (1 - SL_PCT / 100)
    else:
        tp, sl = entry * (1 - TP_PCT / 100), entry * (1 + SL_PCT / 100)
    for c in cs:
        h, l = c["h"], c["l"]
        if is_long:
            if l <= sl:
                return {"result": "SL", "pnl": -SL_PCT}
            if h >= tp:
                return {"result": "TP", "pnl": TP_PCT}
        else:
            if h >= sl:
                return {"result": "SL", "pnl": -SL_PCT}
            if l <= tp:
                return {"result": "TP", "pnl": TP_PCT}
    last = cs[-1]["c"]
    p = (last - entry) / entry * 100
    if not is_long:
        p = -p
    return {"result": "HOLD", "pnl": round(p, 2)}

for sig in signals:
    sig["backtest"] = simulate(sig)
print(f"  [время: {time.time() - t5:.1f}с]")

# ═══════════════════════════════════════════════
# ШАГ 7: Отчёт
# ═══════════════════════════════════════════════
print(f"\n{'='*100}")
print(f"ИТОГОВЫЙ ОТЧЁТ (за {DAYS} дней)  |  TP {TP_PCT}% / SL {SL_PCT}% / hold 48h")
print("="*100)

def run_stats(items):
    tp = sum(1 for s in items if s["backtest"]["result"] == "TP")
    sl = sum(1 for s in items if s["backtest"]["result"] == "SL")
    hold = sum(1 for s in items if s["backtest"]["result"] == "HOLD")
    nd = sum(1 for s in items if s["backtest"]["result"] == "NO_DATA")
    closed = tp + sl
    wr = tp / closed * 100 if closed else 0
    pnls = [s["backtest"]["pnl"] for s in items if s["backtest"]["pnl"] is not None]
    return {
        "total": len(items), "tp": tp, "sl": sl, "hold": hold, "nd": nd,
        "wr": wr, "sum": sum(pnls), "avg": sum(pnls) / len(pnls) if pnls else 0,
    }

def print_row(label, items):
    r = run_stats(items)
    print(f"  {label:35s} total={r['total']:4d}  TP={r['tp']:3d} SL={r['sl']:3d} HOLD={r['hold']:3d} ND={r['nd']:3d}  "
          f"WR={r['wr']:5.1f}%  ΣPnL={r['sum']:+8.2f}%  avg={r['avg']:+.2f}%")

# Общая сводка
print("\n▸ ВСЕ СИГНАЛЫ (baseline):")
print_row("ALL", signals)

# По KL группам (overall)
print("\n▸ ПО KEY LEVELS ГРУППАМ:")
for cls in ("ALIGNED", "CONFIRMING", "NEUTRAL", "CONFLICTING"):
    print_row(cls, [s for s in signals if s["kl_class"] == cls])

# По источникам
print("\n▸ ПО ИСТОЧНИКАМ × KL ГРУППАМ:")
for src in ("CV", "Cluster", "Confluence", "Anomaly"):
    src_items = [s for s in signals if s["src"] == src]
    print(f"\n  [{src}] total={len(src_items)}")
    print_row(f"  {src} all", src_items)
    for cls in ("ALIGNED", "CONFIRMING", "NEUTRAL", "CONFLICTING"):
        items = [s for s in src_items if s["kl_class"] == cls]
        if items:
            print_row(f"  {src} / {cls}", items)

# Top Picks отдельно
print("\n▸ TOP PICKS × KL:")
top_picks = [s for s in signals if s.get("is_top_pick")]
print_row("Top Picks all", top_picks)
for cls in ("ALIGNED", "CONFIRMING", "NEUTRAL", "CONFLICTING"):
    items = [s for s in top_picks if s["kl_class"] == cls]
    if items:
        print_row(f"Top Picks / {cls}", items)

# Предложенные фильтры
print("\n▸ СТРАТЕГИИ ФИЛЬТРАЦИИ:")
baseline = run_stats(signals)
strict = [s for s in signals if s["kl_class"] == "ALIGNED"]
soft = [s for s in signals if s["kl_class"] in ("ALIGNED", "CONFIRMING", "NEUTRAL")]
rs = run_stats(strict)
so = run_stats(soft)
print(f"  Baseline                          total={baseline['total']:4d}  WR={baseline['wr']:5.1f}%  ΣPnL={baseline['sum']:+8.2f}%")
print(f"  A. Strict (только ALIGNED)        total={rs['total']:4d}  WR={rs['wr']:5.1f}%  ΣPnL={rs['sum']:+8.2f}%  "
      f"(Δ total={rs['total']-baseline['total']:+d}, Δ WR={rs['wr']-baseline['wr']:+.1f}пп)")
print(f"  B. Soft (без CONFLICTING)         total={so['total']:4d}  WR={so['wr']:5.1f}%  ΣPnL={so['sum']:+8.2f}%  "
      f"(Δ total={so['total']-baseline['total']:+d}, Δ WR={so['wr']-baseline['wr']:+.1f}пп)")

print(f"\n[{time.strftime('%H:%M:%S')}] === ГОТОВО ===")
print(f"Общее время: {time.time() - t0:.0f}с")
