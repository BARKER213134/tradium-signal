"""Fair Value Gap (FVG) detector + backtest evaluators.

FVG = 3-свечной имбаланс. Определение:
  Bullish FVG:  low[i+2]  > high[i]      (gap между свечами i и i+2)
  Bearish FVG:  high[i+2] < low[i]

  Средняя свеча i+1 — "импульсная", её тело задаёт силу движения.

Зоны FVG:
  Bullish: top = low[i+2]  | bottom = high[i]
  Bearish: top = low[i]    | bottom = high[i+2]
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Literal, Optional

Direction = Literal["bullish", "bearish"]


@dataclass
class FVG:
    idx: int            # индекс 3-й свечи (момент формирования)
    direction: Direction
    top: float          # верхняя граница зоны
    bottom: float       # нижняя граница зоны
    impulse_body_ratio: float  # тело средней свечи / её range (0..1)
    size_rel: float     # (top-bottom)/close3 — относительный размер
    close3: float       # цена закрытия 3-й свечи (entry price для Aggressive)
    time: int = 0       # timestamp 3-й свечи


def detect_fvg(candles: list[dict], min_size_rel: float = 0.0003) -> list[FVG]:
    """candles: [{t, o, h, l, c, v}, ...] старые→новые.
    min_size_rel: минимальный относительный размер FVG (0.0003 = 0.03%).
    """
    fvgs = []
    for i in range(len(candles) - 2):
        c1 = candles[i]
        c2 = candles[i + 1]
        c3 = candles[i + 2]
        # Bullish: low3 > high1
        if c3["l"] > c1["h"]:
            top = c3["l"]
            bottom = c1["h"]
            close3 = c3["c"]
            size_rel = (top - bottom) / close3 if close3 > 0 else 0
            if size_rel < min_size_rel:
                continue
            rng = c2["h"] - c2["l"]
            body = abs(c2["c"] - c2["o"])
            body_ratio = body / rng if rng > 0 else 0
            fvgs.append(FVG(
                idx=i + 2, direction="bullish",
                top=top, bottom=bottom,
                impulse_body_ratio=body_ratio,
                size_rel=size_rel,
                close3=close3,
                time=int(c3.get("t", 0)),
            ))
        # Bearish: high3 < low1
        elif c3["h"] < c1["l"]:
            top = c1["l"]
            bottom = c3["h"]
            close3 = c3["c"]
            size_rel = (top - bottom) / close3 if close3 > 0 else 0
            if size_rel < min_size_rel:
                continue
            rng = c2["h"] - c2["l"]
            body = abs(c2["c"] - c2["o"])
            body_ratio = body / rng if rng > 0 else 0
            fvgs.append(FVG(
                idx=i + 2, direction="bearish",
                top=top, bottom=bottom,
                impulse_body_ratio=body_ratio,
                size_rel=size_rel,
                close3=close3,
                time=int(c3.get("t", 0)),
            ))
    return fvgs


# ── Backtest evaluation ────────────────────────────────────────

@dataclass
class TradeResult:
    entry_idx: int
    exit_idx: int
    entry_price: float
    exit_price: float
    direction: Direction
    R: float              # результат в R (risk units)
    status: Literal["WIN", "LOSS", "TIMEOUT", "SKIPPED"]
    bars_held: int


def _sl_buffer(bottom, top):
    """5% от размера FVG как буфер — не пересчитываем в пипсах, используем относительно."""
    return (top - bottom) * 0.05


def evaluate_aggressive(
    fvg: FVG, candles: list[dict],
    exit_mode: Literal["rr2", "rr3", "trailing"] = "rr2",
    max_bars: int = 50,
) -> TradeResult:
    """A. Aggressive — вход сразу на close 3-й свечи."""
    entry_idx = fvg.idx
    entry = fvg.close3
    is_long = fvg.direction == "bullish"

    # SL: за противоположной границей FVG + buffer
    buf = _sl_buffer(fvg.bottom, fvg.top)
    if is_long:
        sl = fvg.bottom - buf  # под FVG
    else:
        sl = fvg.top + buf  # над FVG

    risk = abs(entry - sl)
    if risk <= 0:
        return TradeResult(entry_idx, entry_idx, entry, entry, fvg.direction, 0, "SKIPPED", 0)

    # TP
    if exit_mode == "rr2":
        tp = entry + 2 * risk if is_long else entry - 2 * risk
    elif exit_mode == "rr3":
        tp = entry + 3 * risk if is_long else entry - 3 * risk
    else:
        tp = None  # trailing

    # Проходим свечи после entry
    end = min(len(candles), entry_idx + 1 + max_bars)
    max_fav = entry  # пик цены в нашу сторону (для trailing)
    trail_sl = sl
    for j in range(entry_idx + 1, end):
        c = candles[j]
        if is_long:
            # Обновить trailing SL
            if exit_mode == "trailing":
                if c["h"] > max_fav:
                    max_fav = c["h"]
                    # После достижения 1R трейлим за 50% отката
                    if max_fav >= entry + risk:
                        new_sl = max_fav - risk  # трейл 1R сзади
                        if new_sl > trail_sl:
                            trail_sl = new_sl
                effective_sl = trail_sl
            else:
                effective_sl = sl
            # Проверка TP
            if tp is not None and c["h"] >= tp:
                return TradeResult(entry_idx, j, entry, tp, fvg.direction,
                                   (tp - entry) / risk, "WIN", j - entry_idx)
            # Проверка SL
            if c["l"] <= effective_sl:
                r = (effective_sl - entry) / risk
                status = "WIN" if r > 0 else "LOSS"
                return TradeResult(entry_idx, j, entry, effective_sl, fvg.direction,
                                   r, status, j - entry_idx)
        else:  # SHORT
            if exit_mode == "trailing":
                if c["l"] < max_fav or max_fav == entry:
                    max_fav = c["l"]
                    if max_fav <= entry - risk:
                        new_sl = max_fav + risk
                        if new_sl < trail_sl:
                            trail_sl = new_sl
                effective_sl = trail_sl
            else:
                effective_sl = sl
            if tp is not None and c["l"] <= tp:
                return TradeResult(entry_idx, j, entry, tp, fvg.direction,
                                   (entry - tp) / risk, "WIN", j - entry_idx)
            if c["h"] >= effective_sl:
                r = (entry - effective_sl) / risk
                status = "WIN" if r > 0 else "LOSS"
                return TradeResult(entry_idx, j, entry, effective_sl, fvg.direction,
                                   r, status, j - entry_idx)

    # Timeout — считаем по последней close
    if end - 1 <= entry_idx:
        return TradeResult(entry_idx, entry_idx, entry, entry, fvg.direction, 0, "SKIPPED", 0)
    last = candles[end - 1]["c"]
    r = (last - entry) / risk if is_long else (entry - last) / risk
    return TradeResult(entry_idx, end - 1, entry, last, fvg.direction, r, "TIMEOUT", end - 1 - entry_idx)


def evaluate_conservative(
    fvg: FVG, candles: list[dict],
    exit_mode: Literal["rr2", "rr3", "trailing"] = "rr2",
    max_wait_bars: int = 30,
    max_bars: int = 50,
) -> TradeResult:
    """B. Conservative — ждём ретест FVG зоны, вход лимитом."""
    is_long = fvg.direction == "bullish"
    entry_zone_near = fvg.top if is_long else fvg.bottom  # ближняя граница = вход
    entry_zone_far = fvg.bottom if is_long else fvg.top   # дальняя = за SL
    buf = _sl_buffer(fvg.bottom, fvg.top)
    sl = entry_zone_far - buf if is_long else entry_zone_far + buf

    # Ждём retest
    retest_idx = None
    entry_price = None
    wait_end = min(len(candles), fvg.idx + 1 + max_wait_bars)
    for j in range(fvg.idx + 1, wait_end):
        c = candles[j]
        if is_long:
            # Касание top FVG зоны снизу вверх (цена возвращается вниз)
            if c["l"] <= entry_zone_near:
                retest_idx = j
                entry_price = entry_zone_near
                # Но если тут же пробили SL — отмена
                if c["l"] <= sl:
                    return TradeResult(fvg.idx, j, entry_price, sl, fvg.direction,
                                       -1.0, "LOSS", j - fvg.idx)
                break
        else:
            if c["h"] >= entry_zone_near:
                retest_idx = j
                entry_price = entry_zone_near
                if c["h"] >= sl:
                    return TradeResult(fvg.idx, j, entry_price, sl, fvg.direction,
                                       -1.0, "LOSS", j - fvg.idx)
                break

    if retest_idx is None:
        return TradeResult(fvg.idx, fvg.idx, 0, 0, fvg.direction, 0, "SKIPPED", 0)

    # Теперь симулируем trade от retest_idx
    risk = abs(entry_price - sl)
    if risk <= 0:
        return TradeResult(retest_idx, retest_idx, entry_price, entry_price, fvg.direction, 0, "SKIPPED", 0)
    if exit_mode == "rr2":
        tp = entry_price + 2 * risk if is_long else entry_price - 2 * risk
    elif exit_mode == "rr3":
        tp = entry_price + 3 * risk if is_long else entry_price - 3 * risk
    else:
        tp = None

    end = min(len(candles), retest_idx + 1 + max_bars)
    max_fav = entry_price
    trail_sl = sl
    for j in range(retest_idx + 1, end):
        c = candles[j]
        if is_long:
            if exit_mode == "trailing":
                if c["h"] > max_fav:
                    max_fav = c["h"]
                    if max_fav >= entry_price + risk:
                        new_sl = max_fav - risk
                        if new_sl > trail_sl: trail_sl = new_sl
                effective_sl = trail_sl
            else:
                effective_sl = sl
            if tp is not None and c["h"] >= tp:
                return TradeResult(retest_idx, j, entry_price, tp, fvg.direction,
                                   (tp - entry_price) / risk, "WIN", j - retest_idx)
            if c["l"] <= effective_sl:
                r = (effective_sl - entry_price) / risk
                return TradeResult(retest_idx, j, entry_price, effective_sl, fvg.direction,
                                   r, "WIN" if r > 0 else "LOSS", j - retest_idx)
        else:
            if exit_mode == "trailing":
                if max_fav == entry_price or c["l"] < max_fav:
                    max_fav = c["l"]
                    if max_fav <= entry_price - risk:
                        new_sl = max_fav + risk
                        if new_sl < trail_sl: trail_sl = new_sl
                effective_sl = trail_sl
            else:
                effective_sl = sl
            if tp is not None and c["l"] <= tp:
                return TradeResult(retest_idx, j, entry_price, tp, fvg.direction,
                                   (entry_price - tp) / risk, "WIN", j - retest_idx)
            if c["h"] >= effective_sl:
                r = (entry_price - effective_sl) / risk
                return TradeResult(retest_idx, j, entry_price, effective_sl, fvg.direction,
                                   r, "WIN" if r > 0 else "LOSS", j - retest_idx)

    last = candles[end - 1]["c"]
    r = (last - entry_price) / risk if is_long else (entry_price - last) / risk
    return TradeResult(retest_idx, end - 1, entry_price, last, fvg.direction, r, "TIMEOUT", end - 1 - retest_idx)


def evaluate_hybrid(
    fvg: FVG, candles: list[dict],
    exit_mode: Literal["rr2", "rr3", "trailing"] = "rr2",
    max_wait_bars: int = 30,
    max_bars: int = 50,
    min_body_ratio: float = 0.6,
    min_size_rel: float = 0.0010,
) -> TradeResult:
    """C. Hybrid — Conservative + фильтры качества."""
    if fvg.impulse_body_ratio < min_body_ratio or fvg.size_rel < min_size_rel:
        return TradeResult(fvg.idx, fvg.idx, 0, 0, fvg.direction, 0, "SKIPPED", 0)
    return evaluate_conservative(fvg, candles, exit_mode=exit_mode,
                                  max_wait_bars=max_wait_bars, max_bars=max_bars)
