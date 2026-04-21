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
    opens = [c.get("o", c["c"]) for c in candles]
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
            "open": opens[i],
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


# ── Retest + Reversal candle detector ─────────────────────────────────
def _is_reversal_bar(bar: dict, prev_bar: dict, direction: str,
                     wick_body_ratio: float = 1.5) -> bool:
    """Разворотная свеча в сторону direction.

    LONG (bullish reversal):
      — close > open (зелёная) И
      — (lower_wick >= wick_body_ratio × body (pin bar / hammer) ИЛИ
         close > prev.open AND open <= prev.close (bullish engulfing))

    SHORT (bearish reversal):
      — close < open И
      — (upper_wick >= wick_body_ratio × body (shooting star) ИЛИ
         close < prev.open AND open >= prev.close (bearish engulfing))
    """
    o = bar.get("open", bar["close"])
    h = bar["high"]
    l = bar["low"]
    c = bar["close"]
    po = prev_bar.get("open", prev_bar["close"])
    pc = prev_bar["close"]
    body = abs(c - o)
    prev_body = abs(pc - po)
    if body <= 0:
        return False
    # минимальный body reversal-бара — 0.3% от цены (фильтр шума/додж)
    min_body = c * 0.003
    if body < min_body:
        return False
    if direction == "LONG":
        if c <= o:
            return False
        lower_wick = min(o, c) - l
        is_pin = lower_wick >= wick_body_ratio * body
        # engulfing: reversal-бар поглощает prev body, т.е. body >= prev_body
        # prev bar должен иметь вменяемое тело (не doji) — иначе "engulf doji" это шум
        prev_not_doji = prev_body >= c * 0.002
        is_engulf = (c > po) and (o <= pc) and (c > pc) and (body >= prev_body) and prev_not_doji
        return is_pin or is_engulf
    else:  # SHORT
        if c >= o:
            return False
        upper_wick = h - max(o, c)
        is_pin = upper_wick >= wick_body_ratio * body
        prev_not_doji = prev_body >= c * 0.002
        is_engulf = (c < po) and (o >= pc) and (c < pc) and (body >= prev_body) and prev_not_doji
        return is_pin or is_engulf


def _find_retest_and_reversal(
    trig_st: list[dict],
    flip_idx: int,
    direction: str,
    max_wait_bars: int = 24,
    retest_tolerance_pct: float = 0.3,
) -> Optional[int]:
    """После ST flip на flip_idx ищем:
      1. Retest — бар где low (LONG) / high (SHORT) коснулся ST линии
         (± retest_tolerance_pct от ST value)
      2. На следующих 1-3 барах — reversal candle
    Возвращает индекс reversal-бара или None если не найдено за max_wait_bars.
    """
    n = len(trig_st)
    is_long = direction == "LONG"
    for i in range(flip_idx + 1, min(flip_idx + max_wait_bars + 1, n - 1)):
        bar = trig_st[i]
        st_val = bar.get("st")
        if st_val is None:
            continue
        # Retest: цена коснулась ST линии
        tol = st_val * retest_tolerance_pct / 100.0
        if is_long:
            touched = bar["low"] <= (st_val + tol)
        else:
            touched = bar["high"] >= (st_val - tol)
        if not touched:
            continue
        # Ищем reversal на 1-3 следующих барах
        for j in range(i + 1, min(i + 4, n)):
            if j - 1 < 0:
                continue
            if _is_reversal_bar(trig_st[j], trig_st[j - 1], direction):
                return j
    return None


def simulate_trade_st_sl(
    st_series: list[dict],
    entry_idx: int,
    direction: str,
    sl_buffer_pct: float = 0.3,
    max_hold_bars: int = 48,
) -> Optional[Trade]:
    """Симуляция для retest+reversal стратегии: SL выставляется за ST уровнем
    entry-бара (с буфером sl_buffer_pct). Trailing на flip."""
    if entry_idx >= len(st_series) - 2 or entry_idx < 0:
        return None
    entry = st_series[entry_idx]
    entry_price = entry["close"]
    st_val = entry.get("st")
    if st_val is None or entry_price <= 0:
        return None
    is_long = direction == "LONG"
    buf = entry_price * sl_buffer_pct / 100.0
    sl_price = (st_val - buf) if is_long else (st_val + buf)
    if is_long and sl_price >= entry_price:
        return None
    if (not is_long) and sl_price <= entry_price:
        return None
    risk = abs(entry_price - sl_price)
    if risk <= 0:
        return None
    trade = Trade(pair="", direction=direction,
                  entry_ts=entry["t"], entry_price=entry_price, sl_price=sl_price)
    max_idx = min(entry_idx + max_hold_bars, len(st_series) - 1)
    for i in range(entry_idx + 1, max_idx + 1):
        bar = st_series[i]
        if is_long and bar["low"] <= sl_price:
            trade.exit_ts = bar["t"]; trade.exit_price = sl_price
            trade.exit_reason = "sl"; trade.r_multiple = -1.0
            trade.pnl_pct = (sl_price - entry_price) / entry_price * 100
            return trade
        if (not is_long) and bar["high"] >= sl_price:
            trade.exit_ts = bar["t"]; trade.exit_price = sl_price
            trade.exit_reason = "sl"; trade.r_multiple = -1.0
            trade.pnl_pct = (sl_price - entry_price) / entry_price * 100
            return trade
        if bar["trend"] != 0 and (
            (is_long and bar["trend"] == -1) or (not is_long and bar["trend"] == 1)
        ):
            cl = bar["close"]
            trade.exit_ts = bar["t"]; trade.exit_price = cl
            trade.exit_reason = "trail_flip"
            trade.r_multiple = (cl - entry_price) / risk if is_long else (entry_price - cl) / risk
            trade.pnl_pct = (cl - entry_price) / entry_price * 100 * (1 if is_long else -1)
            return trade
    last = st_series[max_idx]
    trade.exit_ts = last["t"]; trade.exit_price = last["close"]
    trade.exit_reason = "timeout"
    trade.r_multiple = (last["close"] - entry_price) / risk if is_long else (entry_price - last["close"]) / risk
    trade.pnl_pct = (last["close"] - entry_price) / entry_price * 100 * (1 if is_long else -1)
    return trade


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
    candles_by_tf: dict[str, list[dict]],
    bot_signals: list[dict],  # [{at_ms, direction}] от других ботов за период
) -> dict[str, VariantStats]:
    """Прогоняет все варианты для одной пары. candles_by_tf = {'15m':[...], '30m':[...], '1h':[...], '4h':[...], '1d':[...]}"""
    # Параметры ST по TF (TV-совместимые)
    ST_PARAMS = {
        "15m": (7, 2.0),
        "30m": (7, 2.5),
        "1h":  (10, 3.0),
        "4h":  (10, 3.0),
        "1d":  (14, 3.0),
    }

    # Компилируем ST для каждого TF
    st_by_tf: dict[str, list[dict]] = {}
    for tf, candles in candles_by_tf.items():
        if candles and len(candles) > 20:
            p, m = ST_PARAMS.get(tf, (10, 3.0))
            st_by_tf[tf] = compute_st_series(candles, period=p, mult=m)
        else:
            st_by_tf[tf] = []

    st_1h = st_by_tf.get("1h") or []

    results: dict[str, VariantStats] = {}
    def _get(name: str) -> VariantStats:
        if name not in results:
            results[name] = VariantStats(name)
        return results[name]

    # ── BASELINE + T1_FILTER + T2C_SIGCONF — используют bot signals на 1h ──
    if st_1h:
        for sig in bot_signals:
            idx = _ts_to_bar_idx(st_1h, sig["at_ms"])
            if idx is None or idx < 2:
                continue
            # BASELINE: все сигналы
            t = simulate_trade(st_1h, idx, sig["direction"])
            if t:
                t.pair = pair
                _get("BASELINE").trades.append(t)
            # T1_FILTER: aligned + fresh flip ≤6 bars
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
            t = simulate_trade(st_1h, idx, sig["direction"])
            if t:
                t.pair = pair
                _get("T1_FILTER").trades.append(t)

    # ── Вариант: ST flip на trigger_tf + все align_tfs совпадают ──
    # Список MTF-стратегий (TF триггера + TF-подтверждения)
    MTF_VARIANTS = [
        # (name, trigger_tf, align_tfs)
        ("T2D_1H_D",       "1h", ["1d"]),
        ("T2D_4H_D",       "4h", ["1d"]),
        ("T2D_15M_1H",     "15m", ["1h"]),
        ("T2D_15M_1H_4H",  "15m", ["1h", "4h"]),
        ("T2D_30M_4H",     "30m", ["4h"]),
        ("T2D_30M_4H_D",   "30m", ["4h", "1d"]),
        ("T2D_1H_4H",      "1h", ["4h"]),  # повтор T2A_MTF (для сравнения)
        ("T2D_1H_4H_D",    "1h", ["4h", "1d"]),
    ]

    # ── VIP_AS_IS: ST 1h flip + bot signal ±2h same direction ──────
    # (это наш реальный VIP — повторяем как продакшн классифицирует)
    # + VIP_RETEST_REV — та же логика, но вход после retest + reversal bar
    # Также для сравнения стандартные варианты (simulate_trade vs simulate_trade_st_sl).
    if st_1h:
        flips_1h = find_flips(st_1h)
        window_ms = 2 * 3600 * 1000
        for flip_idx in flips_1h:
            st_bar = st_1h[flip_idx]
            direction = "LONG" if st_bar["trend"] == 1 else "SHORT"
            # VIP: есть сигнал другого бота в окне ±2ч
            vip_ok = any(
                abs(sig["at_ms"] - st_bar["t"]) <= window_ms and sig["direction"] == direction
                for sig in bot_signals
            )
            if vip_ok:
                # ── VIP_AS_IS — вход на close флипа, стоп на ST/ATR (как продакшн) ──
                t = simulate_trade(st_1h, flip_idx, direction)
                if t:
                    t.pair = pair
                    _get("VIP_AS_IS").trades.append(t)
                # T2C_SIGCONF — алиас (оставляем для обратной совместимости) ─
                t2 = simulate_trade(st_1h, flip_idx, direction)
                if t2:
                    t2.pair = pair
                    _get("T2C_SIGCONF").trades.append(t2)
                # ── VIP_RETEST_REV: ждём retest ST + reversal-свеча ──
                rev_idx = _find_retest_and_reversal(st_1h, flip_idx, direction,
                                                    max_wait_bars=24)
                if rev_idx is not None:
                    t3 = simulate_trade_st_sl(st_1h, rev_idx, direction)
                    if t3:
                        t3.pair = pair
                        _get("VIP_RETEST_REV").trades.append(t3)

    # ── MTF-варианты ──────────────────────────────────────────
    for name, trigger_tf, align_tfs in MTF_VARIANTS:
        trig_st = st_by_tf.get(trigger_tf) or []
        if not trig_st:
            continue
        # Все align TF должны быть загружены
        align_series = [st_by_tf.get(a) or [] for a in align_tfs]
        if not all(align_series):
            continue
        flips = find_flips(trig_st)
        for flip_idx in flips:
            st_bar = trig_st[flip_idx]
            direction = "LONG" if st_bar["trend"] == 1 else "SHORT"
            # Проверяем все align TFs на момент флипа
            all_aligned = True
            for aseries in align_series:
                astate = st_state_at_ts(aseries, st_bar["t"])
                if not astate or astate["trend"] != st_bar["trend"]:
                    all_aligned = False
                    break
            if not all_aligned:
                continue
            # Симулируем на trigger_tf
            t = simulate_trade(trig_st, flip_idx, direction,
                               max_hold_bars=_max_hold_for_tf(trigger_tf))
            if t:
                t.pair = pair
                _get(name).trades.append(t)

    # ── MTF_AS_IS + MTF_RETEST_REV — продакшн-версия "Triple MTF" ─────
    # Triple MTF в продакшне = ST 1h flip + 4h aligned + 1d aligned
    if st_1h:
        st_4h = st_by_tf.get("4h") or []
        st_1d = st_by_tf.get("1d") or []
        if st_4h and st_1d:
            flips_1h = find_flips(st_1h)
            for flip_idx in flips_1h:
                st_bar = st_1h[flip_idx]
                direction = "LONG" if st_bar["trend"] == 1 else "SHORT"
                # 4h + 1d aligned на момент флипа
                s4 = st_state_at_ts(st_4h, st_bar["t"])
                sd = st_state_at_ts(st_1d, st_bar["t"])
                if not s4 or not sd:
                    continue
                if s4["trend"] != st_bar["trend"] or sd["trend"] != st_bar["trend"]:
                    continue
                # ── MTF_AS_IS — вход на close флипа (как продакшн) ──
                t = simulate_trade(st_1h, flip_idx, direction)
                if t:
                    t.pair = pair
                    _get("MTF_AS_IS").trades.append(t)
                # ── MTF_RETEST_REV — вход после retest + reversal ──
                rev_idx = _find_retest_and_reversal(st_1h, flip_idx, direction,
                                                    max_wait_bars=24)
                if rev_idx is not None:
                    t2 = simulate_trade_st_sl(st_1h, rev_idx, direction)
                    if t2:
                        t2.pair = pair
                        _get("MTF_RETEST_REV").trades.append(t2)
                # ── VIP_AND_MTF — MTF aligned + bot signal ±2ч (двойное подтверждение) ──
                # Гипотеза: когда обе системы согласны — точность должна быть >50% WR
                vip_conf_window_ms = 2 * 3600 * 1000
                vip_conf = any(
                    abs(sig["at_ms"] - st_bar["t"]) <= vip_conf_window_ms
                    and sig["direction"] == direction
                    for sig in bot_signals
                )
                if vip_conf:
                    t3 = simulate_trade(st_1h, flip_idx, direction)
                    if t3:
                        t3.pair = pair
                        _get("VIP_AND_MTF").trades.append(t3)
                    # То же + retest+reversal — двойной фильтр + best entry
                    rev_idx2 = _find_retest_and_reversal(st_1h, flip_idx, direction,
                                                        max_wait_bars=24)
                    if rev_idx2 is not None:
                        t4 = simulate_trade_st_sl(st_1h, rev_idx2, direction)
                        if t4:
                            t4.pair = pair
                            _get("VIP_AND_MTF_RETEST").trades.append(t4)

    return results


def _max_hold_for_tf(tf: str) -> int:
    """Max hold bars для данного TF — нормируем чтобы было ~2 дня удержания."""
    return {"15m": 192, "30m": 96, "1h": 48, "4h": 12, "1d": 3}.get(tf, 48)


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

        # Свечи на всех TF
        pair_slash = pair_norm[:-4] + "/USDT"
        candles_by_tf: dict[str, list[dict]] = {}
        try:
            c1h = get_klines_any(pair_slash, "1h", 400)   # 14д * 24 + запас
            if not c1h or len(c1h) < 50:
                skipped_no_candles += 1
                continue
            candles_by_tf["1h"] = c1h
            candles_by_tf["15m"] = get_klines_any(pair_slash, "15m", 1500) or []  # 14д × 96 = 1344
            candles_by_tf["30m"] = get_klines_any(pair_slash, "30m", 800) or []   # 14д × 48 = 672
            candles_by_tf["4h"]  = get_klines_any(pair_slash, "4h", 150) or []
            candles_by_tf["1d"]  = get_klines_any(pair_slash, "1d", 40) or []
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
            per_pair = backtest_variants_for_pair(pair_norm, candles_by_tf, bot_sigs)
        except Exception as e:
            logger.warning(f"[backtest-st] {pair_norm} backtest fail: {e}")
            continue

        # Агрегация
        for name, vs in per_pair.items():
            if name not in all_stats:
                all_stats[name] = VariantStats(name)
            all_stats[name].trades.extend(vs.trades)

    # 4. Формируем ответ — все собранные варианты, отсортированные по EV
    summary = []
    for name, vs in all_stats.items():
        summary.append(vs.summary())
    # Сортировка: BASELINE → 4 "главных" варианта (VIP/MTF as-is + retest-rev) → остальные по EV
    PRIO = {
        "BASELINE": 0,
        "VIP_AS_IS": 1,
        "MTF_AS_IS": 2,
        "VIP_AND_MTF": 3,          # двойное подтверждение (VIP ∧ MTF)
        "VIP_AND_MTF_RETEST": 4,   # двойной фильтр + retest/reversal entry
        "VIP_RETEST_REV": 5,
        "MTF_RETEST_REV": 6,
    }
    def _key(x):
        name = x["name"]
        if name in PRIO:
            return (0, PRIO[name])
        return (1, -(x.get("ev_per_trade", 0) or 0))
    summary.sort(key=_key)

    return {
        "days": days,
        "pairs_total": total,
        "pairs_processed": processed,
        "pairs_skipped": skipped_no_candles,
        "variants": summary,
    }
