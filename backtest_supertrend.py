"""Бектест ST-стратегий на всех монетах (14 дней).

Варианты:
  BASELINE:     все сигналы любого бота без ST-фильтра
  T1_FILTER:    signals with direction == ST(1h).state AND ST flipped ≤6 bars
  T2A_MTF:      ST 1h flip + ST 4h aligned (standalone, без сигнала бота)
  T2B_BARCONF:  ST 1h flip + next 2 bars confirm trend
  T2C_SIGCONF:  ST 1h flip + другой бот сигнал ±2h same direction
  T2D_HTF:      ST 1h flip + daily ST aligned
  T2E_COMBO:    MTF + BARCONF + HTF

Entry: close of trigger bar.
SL: min(ST value at entry, ±1.5 ATR).
TP: trailing via opposite ST flip.
Max hold: 48h.

Метрики: trades, WR, avg R, PF, DD, EV.
"""
from __future__ import annotations
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


# ── Вспомогательные структуры ──────────────────────────────────────
@dataclass
class Trade:
    pair: str
    direction: str            # 'LONG' | 'SHORT'
    entry_ts: int             # unix
    entry_price: float
    sl_price: float
    exit_ts: Optional[int] = None
    exit_price: Optional[float] = None
    exit_reason: str = ""      # 'sl' | 'trail_flip' | 'timeout'
    r_multiple: float = 0.0    # PnL в терминах риска (SL distance)
    pnl_pct: float = 0.0       # PnL % от entry


@dataclass
class VariantStats:
    name: str
    trades: list[Trade] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.trades)

    @property
    def closed(self) -> list[Trade]:
        return [t for t in self.trades if t.exit_ts is not None]

    @property
    def wins(self) -> int:
        return sum(1 for t in self.closed if t.r_multiple > 0)

    @property
    def losses(self) -> int:
        return sum(1 for t in self.closed if t.r_multiple <= 0)

    @property
    def wr(self) -> float:
        c = self.closed
        return (self.wins / len(c) * 100.0) if c else 0.0

    @property
    def avg_r(self) -> float:
        c = self.closed
        return (sum(t.r_multiple for t in c) / len(c)) if c else 0.0

    @property
    def profit_factor(self) -> float:
        wins = sum(t.r_multiple for t in self.closed if t.r_multiple > 0)
        losses = abs(sum(t.r_multiple for t in self.closed if t.r_multiple <= 0))
        return (wins / losses) if losses > 0 else (wins if wins > 0 else 0.0)

    @property
    def ev(self) -> float:
        """Expected value на сделку (в R)."""
        return self.avg_r

    def summary(self) -> dict:
        return {
            "name": self.name,
            "trades": self.total,
            "wins": self.wins,
            "losses": self.losses,
            "wr": round(self.wr, 1),
            "avg_r": round(self.avg_r, 3),
            "profit_factor": round(self.profit_factor, 2),
            "ev_per_trade": round(self.ev, 3),
        }


# ── Расчёт SuperTrend ──────────────────────────────────────────────
def compute_st_series(candles: list[dict], period: int, mult: float) -> list[dict]:
    """Returns list of dicts per candle: {time, close, trend(1/-1/0), st_value, atr}."""
    n = len(candles)
    if n < period + 2:
        return []
    closes = [c["c"] for c in candles]
    highs = [c["h"] for c in candles]
    lows = [c["l"] for c in candles]

    tr = [highs[0] - lows[0]]
    for i in range(1, n):
        tr.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i] - closes[i-1]),
        ))

    atr = [None] * n
    if n >= period:
        atr[period-1] = sum(tr[:period]) / period
        for i in range(period, n):
            atr[i] = (atr[i-1] * (period - 1) + tr[i]) / period

    hl2 = [(highs[i] + lows[i]) / 2 for i in range(n)]
    final_upper = [0.0] * n
    final_lower = [0.0] * n
    trend = [0] * n
    st = [None] * n

    for i in range(n):
        if atr[i] is None:
            continue
        bu = hl2[i] + mult * atr[i]
        bl = hl2[i] - mult * atr[i]
        if i == 0 or atr[i-1] is None:
            final_upper[i] = bu
            final_lower[i] = bl
            trend[i] = 1
            st[i] = bl
            continue
        final_upper[i] = bu if (bu < final_upper[i-1] or closes[i-1] > final_upper[i-1]) else final_upper[i-1]
        final_lower[i] = bl if (bl > final_lower[i-1] or closes[i-1] < final_lower[i-1]) else final_lower[i-1]
        if trend[i-1] == 1 and closes[i] < final_lower[i-1]:
            trend[i] = -1
        elif trend[i-1] == -1 and closes[i] > final_upper[i-1]:
            trend[i] = 1
        else:
            trend[i] = trend[i-1] if trend[i-1] != 0 else 1
        st[i] = final_lower[i] if trend[i] == 1 else final_upper[i]

    out = []
    for i in range(n):
        out.append({
            "t": candles[i]["t"],
            "close": closes[i],
            "high": highs[i],
            "low": lows[i],
            "trend": trend[i],
            "st": st[i],
            "atr": atr[i],
        })
    return out


def find_flips(st_series: list[dict]) -> list[int]:
    """Indices where trend changes (flip bar)."""
    flips = []
    for i in range(1, len(st_series)):
        if st_series[i]["trend"] != 0 and st_series[i-1]["trend"] != 0 \
                and st_series[i]["trend"] != st_series[i-1]["trend"]:
            flips.append(i)
    return flips


def age_since_last_flip(st_series: list[dict], idx: int) -> Optional[int]:
    """Сколько баров прошло с последнего флипа до бара idx (None если тренд не менялся)."""
    cur = st_series[idx]["trend"]
    if cur == 0:
        return None
    for j in range(idx - 1, -1, -1):
        if st_series[j]["trend"] != 0 and st_series[j]["trend"] != cur:
            return idx - j
    return None


# ── Симуляция сделки ───────────────────────────────────────────────
def simulate_trade(st_1h: list[dict], entry_idx: int, direction: str,
                   atr_mult_sl: float = 1.5, max_hold_bars: int = 48) -> Optional[Trade]:
    """Симулирует сделку с trailing ST exit + ATR-based SL.

    Returns None если entry бар слишком близко к концу (нет места для выхода).
    """
    if entry_idx >= len(st_1h) - 2:
        return None

    entry = st_1h[entry_idx]
    entry_price = entry["close"]
    atr = entry["atr"]
    st_val = entry["st"]
    if atr is None or st_val is None:
        return None

    is_long = direction == "LONG"
    # SL — ближе из двух: ST value или 1.5 ATR от entry
    st_sl = st_val
    atr_sl = entry_price - atr_mult_sl * atr if is_long else entry_price + atr_mult_sl * atr
    sl_price = max(st_sl, atr_sl) if is_long else min(st_sl, atr_sl)
    if is_long and sl_price >= entry_price:
        sl_price = entry_price - atr_mult_sl * atr
    if not is_long and sl_price <= entry_price:
        sl_price = entry_price + atr_mult_sl * atr

    risk_per_unit = abs(entry_price - sl_price)
    if risk_per_unit <= 0:
        return None

    trade = Trade(
        pair="",  # caller sets
        direction=direction,
        entry_ts=entry["t"],
        entry_price=entry_price,
        sl_price=sl_price,
    )

    max_idx = min(entry_idx + max_hold_bars, len(st_1h) - 1)
    for i in range(entry_idx + 1, max_idx + 1):
        bar = st_1h[i]
        hi, lo, cl = bar["high"], bar["low"], bar["close"]
        # 1. SL hit
        if is_long and lo <= sl_price:
            trade.exit_ts = bar["t"]
            trade.exit_price = sl_price
            trade.exit_reason = "sl"
            trade.r_multiple = -1.0
            trade.pnl_pct = (sl_price - entry_price) / entry_price * 100
            return trade
        if not is_long and hi >= sl_price:
            trade.exit_ts = bar["t"]
            trade.exit_price = sl_price
            trade.exit_reason = "sl"
            trade.r_multiple = -1.0
            trade.pnl_pct = (sl_price - entry_price) / entry_price * 100
            return trade
        # 2. Trail flip — exit at close if ST flipped opposite
        if bar["trend"] != 0 and (
            (is_long and bar["trend"] == -1) or (not is_long and bar["trend"] == 1)
        ):
            trade.exit_ts = bar["t"]
            trade.exit_price = cl
            trade.exit_reason = "trail_flip"
            trade.r_multiple = (cl - entry_price) / risk_per_unit if is_long else (entry_price - cl) / risk_per_unit
            trade.pnl_pct = (cl - entry_price) / entry_price * 100 * (1 if is_long else -1)
            return trade

    # 3. Timeout
    last = st_1h[max_idx]
    trade.exit_ts = last["t"]
    trade.exit_price = last["close"]
    trade.exit_reason = "timeout"
    trade.r_multiple = (last["close"] - entry_price) / risk_per_unit if is_long else (entry_price - last["close"]) / risk_per_unit
    trade.pnl_pct = (last["close"] - entry_price) / entry_price * 100 * (1 if is_long else -1)
    return trade


# ── Индексация ST по timestamp для MTF-проверок ───────────────────
def st_state_at_ts(st_series: list[dict], ts_ms: int) -> Optional[dict]:
    """Найти ближайший (не превышающий ts) бар и вернуть его состояние."""
    if not st_series:
        return None
    # Бинарный поиск
    lo, hi = 0, len(st_series) - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if st_series[mid]["t"] <= ts_ms:
            lo = mid
        else:
            hi = mid - 1
    return st_series[lo] if st_series[lo]["t"] <= ts_ms else None


# ── Основная функция бектеста ──────────────────────────────────────
def backtest_variants_for_pair(
    pair: str,
    candles_1h: list[dict],
    candles_4h: list[dict],
    candles_1d: list[dict],
    bot_signals: list[dict],  # [{at_ms, direction}] от других ботов за период
) -> dict[str, VariantStats]:
    """Прогоняет все варианты для одной пары. Возвращает dict name→stats."""
    st_1h = compute_st_series(candles_1h, period=10, mult=3.0)
    st_4h = compute_st_series(candles_4h, period=10, mult=3.0)
    st_1d = compute_st_series(candles_1d, period=14, mult=3.0)
    if not st_1h:
        return {}

    flips = find_flips(st_1h)

    results: dict[str, VariantStats] = {
        "BASELINE": VariantStats("BASELINE"),
        "T1_FILTER": VariantStats("T1_FILTER"),
        "T2A_MTF": VariantStats("T2A_MTF"),
        "T2B_BARCONF": VariantStats("T2B_BARCONF"),
        "T2C_SIGCONF": VariantStats("T2C_SIGCONF"),
        "T2D_HTF": VariantStats("T2D_HTF"),
        "T2E_COMBO": VariantStats("T2E_COMBO"),
    }

    def _make_trade(idx: int, direction: str) -> Optional[Trade]:
        t = simulate_trade(st_1h, idx, direction)
        if t:
            t.pair = pair
        return t

    # ── BASELINE: все сигналы ботов ────────────────────────────
    for sig in bot_signals:
        # find nearest 1h bar index to signal timestamp
        idx = _ts_to_bar_idx(st_1h, sig["at_ms"])
        if idx is None or idx < 2:
            continue
        t = _make_trade(idx, sig["direction"])
        if t:
            results["BASELINE"].trades.append(t)

    # ── T1_FILTER: bot signal aligned with fresh ST (<=6 bars) ──
    for sig in bot_signals:
        idx = _ts_to_bar_idx(st_1h, sig["at_ms"])
        if idx is None or idx < 2:
            continue
        st_bar = st_1h[idx]
        if st_bar["trend"] == 0:
            continue
        is_long = sig["direction"] == "LONG"
        aligned = (st_bar["trend"] == 1) == is_long
        if not aligned:
            continue
        age = age_since_last_flip(st_1h, idx)
        if age is None or age > 6:
            continue
        t = _make_trade(idx, sig["direction"])
        if t:
            results["T1_FILTER"].trades.append(t)

    # ── Для T2A..T2E — триггер это флип на 1h ──────────────────
    for flip_idx in flips:
        st_bar = st_1h[flip_idx]
        direction = "LONG" if st_bar["trend"] == 1 else "SHORT"
        is_long = direction == "LONG"

        # T2A: MTF — ST 4h aligned
        st4 = st_state_at_ts(st_4h, st_bar["t"])
        mtf_ok = bool(st4 and st4["trend"] == st_bar["trend"])

        # T2B: Bar-confirmation — next 2 bars confirmation (close on правильной стороне ST)
        bar_conf_ok = False
        if flip_idx + 2 < len(st_1h):
            b1, b2 = st_1h[flip_idx + 1], st_1h[flip_idx + 2]
            if is_long:
                bar_conf_ok = (b1["close"] > st_bar["st"]) and (b2["close"] > st_bar["st"])
            else:
                bar_conf_ok = (b1["close"] < st_bar["st"]) and (b2["close"] < st_bar["st"])

        # T2C: Signal-confirmation — other bot signal ±2h same direction
        sig_conf_ok = False
        window_ms = 2 * 3600 * 1000
        for sig in bot_signals:
            if abs(sig["at_ms"] - st_bar["t"]) <= window_ms and sig["direction"] == direction:
                sig_conf_ok = True
                break

        # T2D: HTF — daily ST aligned
        st_d = st_state_at_ts(st_1d, st_bar["t"])
        htf_ok = bool(st_d and st_d["trend"] == st_bar["trend"])

        # Делаем сделку ТОЛЬКО если вариант прошёл
        # T2A: entry on flip bar (not next), to be fair — actually better on close of flip
        if mtf_ok:
            t = _make_trade(flip_idx, direction)
            if t: results["T2A_MTF"].trades.append(t)
        if bar_conf_ok:
            # Вход через 2 бара после флипа (потому что 2 бара нужны для подтверждения)
            entry_idx = flip_idx + 2
            if entry_idx < len(st_1h):
                t = _make_trade(entry_idx, direction)
                if t: results["T2B_BARCONF"].trades.append(t)
        if sig_conf_ok:
            t = _make_trade(flip_idx, direction)
            if t: results["T2C_SIGCONF"].trades.append(t)
        if htf_ok:
            t = _make_trade(flip_idx, direction)
            if t: results["T2D_HTF"].trades.append(t)
        if mtf_ok and bar_conf_ok and htf_ok:
            entry_idx = flip_idx + 2
            if entry_idx < len(st_1h):
                t = _make_trade(entry_idx, direction)
                if t: results["T2E_COMBO"].trades.append(t)

    return results


def _ts_to_bar_idx(st_series: list[dict], ts_ms: int) -> Optional[int]:
    """Binary search: индекс бара который содержит ts_ms (bar.t <= ts < next_bar.t)."""
    if not st_series:
        return None
    if ts_ms < st_series[0]["t"]:
        return None
    if ts_ms > st_series[-1]["t"] + 86400 * 1000:  # слишком поздно
        return None
    lo, hi = 0, len(st_series) - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if st_series[mid]["t"] <= ts_ms:
            lo = mid
        else:
            hi = mid - 1
    return lo


# ── Глобальный оркестратор ─────────────────────────────────────────
def run_backtest(days: int = 14, top_n: int = 200,
                 on_progress=None) -> dict:
    """Запускает бектест на топ-N монетах за N дней.
    Возвращает агрегированную статистику по всем вариантам.

    on_progress: optional callback(pair_index, total_pairs, pair_name)
    """
    from exchange import get_klines_any
    from database import _signals, _anomalies, _confluence, _clusters, utcnow
    from collections import defaultdict

    since = utcnow() - timedelta(days=days)

    # 1. Собираем уникальные пары из всех signal collections
    pairs = set()
    # Сигналы с направлением и pattern_triggered/pair
    for s in _signals().find(
        {"received_at": {"$gte": since}, "direction": {"$in": ["LONG", "SHORT"]}},
        {"pair": 1},
    ):
        p = (s.get("pair") or "").upper().replace("/", "")
        if p:
            if not p.endswith("USDT"):
                p = p + "USDT"
            pairs.add(p)
    for a in _anomalies().find(
        {"detected_at": {"$gte": since}, "direction": {"$in": ["LONG", "SHORT"]}},
        {"symbol": 1, "pair": 1},
    ):
        p = (a.get("symbol") or a.get("pair") or "").upper().replace("/", "")
        if p:
            if not p.endswith("USDT"): p = p + "USDT"
            pairs.add(p)
    for c in _confluence().find(
        {"detected_at": {"$gte": since}, "direction": {"$in": ["LONG", "SHORT"]}},
        {"symbol": 1, "pair": 1},
    ):
        p = (c.get("symbol") or c.get("pair") or "").upper().replace("/", "")
        if p:
            if not p.endswith("USDT"): p = p + "USDT"
            pairs.add(p)

    pairs = sorted(pairs)[:top_n]
    total = len(pairs)
    logger.info(f"[backtest-st] {total} пар к обработке")

    # 2. Для каждой пары — сигналы всех ботов
    def _load_bot_signals(pair_norm: str) -> list[dict]:
        pair_slash = pair_norm[:-4] + "/USDT"
        pair_or = {"$or": [{"pair": pair_slash}, {"symbol": pair_norm}]}
        sigs = []
        for s in _signals().find({
            "received_at": {"$gte": since},
            "direction": {"$in": ["LONG", "SHORT"]},
            **pair_or,
        }, {"direction": 1, "received_at": 1, "pattern_triggered_at": 1}):
            t = s.get("pattern_triggered_at") or s.get("received_at")
            if t:
                sigs.append({"at_ms": int(t.timestamp() * 1000), "direction": s["direction"]})
        for a in _anomalies().find({
            "detected_at": {"$gte": since},
            "direction": {"$in": ["LONG", "SHORT"]},
            **pair_or,
        }, {"direction": 1, "detected_at": 1}):
            sigs.append({"at_ms": int(a["detected_at"].timestamp() * 1000), "direction": a["direction"]})
        for c in _confluence().find({
            "detected_at": {"$gte": since},
            "direction": {"$in": ["LONG", "SHORT"]},
            **pair_or,
        }, {"direction": 1, "detected_at": 1}):
            sigs.append({"at_ms": int(c["detected_at"].timestamp() * 1000), "direction": c["direction"]})
        return sigs

    # 3. Прогон
    all_stats: dict[str, VariantStats] = {}
    processed = 0
    skipped_no_candles = 0

    for pair_norm in pairs:
        processed += 1
        if on_progress:
            try: on_progress(processed, total, pair_norm)
            except Exception: pass

        # Свечи
        pair_slash = pair_norm[:-4] + "/USDT"
        try:
            # 14 дней * 24 = 336 часовых баров + запас
            c1h = get_klines_any(pair_slash, "1h", 400)
            if not c1h or len(c1h) < 50:
                skipped_no_candles += 1
                continue
            c4h = get_klines_any(pair_slash, "4h", 150)
            c1d = get_klines_any(pair_slash, "1d", 40)
        except Exception as e:
            logger.warning(f"[backtest-st] {pair_norm} candles fail: {e}")
            skipped_no_candles += 1
            continue

        # Сигналы ботов
        try:
            bot_sigs = _load_bot_signals(pair_norm)
        except Exception as e:
            logger.warning(f"[backtest-st] {pair_norm} signals fail: {e}")
            bot_sigs = []

        # Бектест
        try:
            per_pair = backtest_variants_for_pair(pair_norm, c1h, c4h or [], c1d or [], bot_sigs)
        except Exception as e:
            logger.warning(f"[backtest-st] {pair_norm} backtest fail: {e}")
            continue

        # Агрегация
        for name, vs in per_pair.items():
            if name not in all_stats:
                all_stats[name] = VariantStats(name)
            all_stats[name].trades.extend(vs.trades)

    # 4. Формируем ответ
    summary = []
    for name in ["BASELINE", "T1_FILTER", "T2A_MTF", "T2B_BARCONF",
                 "T2C_SIGCONF", "T2D_HTF", "T2E_COMBO"]:
        if name in all_stats:
            summary.append(all_stats[name].summary())
        else:
            summary.append({"name": name, "trades": 0})

    return {
        "days": days,
        "pairs_total": total,
        "pairs_processed": processed,
        "pairs_skipped": skipped_no_candles,
        "variants": summary,
    }
