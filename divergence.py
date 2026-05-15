"""RSI Bull/Bear Divergence detector.

Bullish divergence: price makes LOWER LOW but RSI makes HIGHER LOW
  → momentum is fading despite price down → reversal up signal

Bearish divergence: price makes HIGHER HIGH but RSI makes LOWER HIGH
  → momentum fading despite price up → reversal down signal

Использует Binance fapi klines с fallback на BingX. Возвращает signals
с strength 0-100 для use в Confluence Score.
"""
from __future__ import annotations
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)


# ─── RSI computation (Wilder) ──────────────────────────────────────
def _compute_rsi(closes: list[float], period: int = 14) -> list[Optional[float]]:
    if len(closes) < period + 1:
        return [None] * len(closes)
    rsi = [None] * len(closes)
    gains = [0.0]
    losses = [0.0]
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i - 1]
        gains.append(max(ch, 0.0))
        losses.append(max(-ch, 0.0))
    avg_g = sum(gains[1:period + 1]) / period
    avg_l = sum(losses[1:period + 1]) / period
    rsi[period] = 100.0 if avg_l == 0 else (100 - 100 / (1 + avg_g / avg_l))
    for i in range(period + 1, len(closes)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rsi[i] = 100.0 if avg_l == 0 else (100 - 100 / (1 + avg_g / avg_l))
    return rsi


# ─── Pivot detection ───────────────────────────────────────────────
def _find_pivots_low(values: list[float], window: int = 3) -> list[tuple[int, float]]:
    """Find local minima: value[i] < value[i±j] for j in [1..window].
    Returns list of (index, value) tuples, sorted by index ascending.
    """
    pivots = []
    n = len(values)
    for i in range(window, n - window):
        v = values[i]
        if v is None:
            continue
        is_pivot = True
        for j in range(1, window + 1):
            left = values[i - j]
            right = values[i + j]
            if left is None or right is None:
                is_pivot = False
                break
            if v >= left or v >= right:
                is_pivot = False
                break
        if is_pivot:
            pivots.append((i, v))
    return pivots


def _find_pivots_high(values: list[float], window: int = 3) -> list[tuple[int, float]]:
    pivots = []
    n = len(values)
    for i in range(window, n - window):
        v = values[i]
        if v is None:
            continue
        is_pivot = True
        for j in range(1, window + 1):
            left = values[i - j]
            right = values[i + j]
            if left is None or right is None:
                is_pivot = False
                break
            if v <= left or v <= right:
                is_pivot = False
                break
        if is_pivot:
            pivots.append((i, v))
    return pivots


# ─── Klines fetcher ─────────────────────────────────────────────────
def _fetch_klines_fapi(pair: str, tf: str = '1h', limit: int = 200) -> Optional[list]:
    """Binance fapi klines. Returns [(t, o, h, l, c, v), ...] or None.
    """
    try:
        import httpx
        sym = pair.replace('/', '').upper()
        r = httpx.get('https://fapi.binance.com/fapi/v1/klines',
                      params={'symbol': sym, 'interval': tf, 'limit': limit},
                      timeout=8.0)
        if r.status_code != 200:
            return None
        out = []
        for k in r.json():
            try:
                out.append((int(k[0]), float(k[1]), float(k[2]),
                            float(k[3]), float(k[4]), float(k[5])))
            except Exception:
                continue
        return out if out else None
    except Exception as e:
        logger.debug(f'[divergence] klines fail {pair}: {e}')
        return None


def _fetch_klines_bingx(pair: str, tf: str = '1h', limit: int = 200) -> Optional[list]:
    """BingX fallback via ccxt."""
    try:
        from exchange import get_bingx_klines
        kl = get_bingx_klines(pair, tf, limit)
        if not kl:
            return None
        return [(k['t'], k['o'], k['h'], k['l'], k['c'], k['v']) for k in kl]
    except Exception as e:
        logger.debug(f'[divergence] bingx fail {pair}: {e}')
        return None


def _get_klines(pair: str, tf: str = '1h', limit: int = 200) -> Optional[list]:
    """Try fapi first, fallback to bingx."""
    kl = _fetch_klines_fapi(pair, tf, limit)
    if kl and len(kl) >= 30:
        return kl
    return _fetch_klines_bingx(pair, tf, limit)


# ─── Divergence detection ──────────────────────────────────────────
# v2 tuning (после backtest 7d: 5/8297 div detected при PIVOT_WINDOW=3,
# MIN_DIST=5). Релаксируем для крипты — swings короче чем у equities.
PIVOT_WINDOW = 2            # 3→2: pivot если value < neighbors в ±2 барах
MAX_LOOKBACK_BARS = 50      # 60→50: фокус на recent action
MIN_PIVOT_DISTANCE = 3      # 5→3: разрешаем более частые swings


def detect_divergence(pair: str, tf: str = '1h', limit: int = 100) -> Optional[dict]:
    """Detect RSI bull/bear divergence on most recent 2 pivots.

    Returns:
      None if no divergence detected
      dict {
        'type': 'bullish' | 'bearish',
        'pair': str, 'tf': str,
        'price_pivot1_bar': int, 'price_pivot1_value': float,  # earlier
        'price_pivot2_bar': int, 'price_pivot2_value': float,  # later
        'rsi_pivot1': float, 'rsi_pivot2': float,
        'rsi_delta': float,         # signed difference, positive=stronger divergence
        'strength': int 0-100,      # composite strength
        'last_close': float, 'last_rsi': float,
        'detected_at_ms': int,
        'bars_since_pivot': int,    # how recent is the divergence
      }
    """
    kl = _get_klines(pair, tf, limit)
    if not kl or len(kl) < 30:
        return None
    closes = [k[4] for k in kl]
    highs = [k[2] for k in kl]
    lows = [k[3] for k in kl]
    rsi = _compute_rsi(closes, 14)
    if rsi[-1] is None:
        return None

    # Look at last N bars for divergence
    end = len(kl) - 1  # current bar
    start = max(0, end - MAX_LOOKBACK_BARS)
    rel_lows = lows[start:end + 1]
    rel_highs = highs[start:end + 1]
    rel_rsi = rsi[start:end + 1]

    # Bullish: 2 lower lows in price, 2 higher lows in RSI
    price_low_pivots = _find_pivots_low(rel_lows, PIVOT_WINDOW)
    if len(price_low_pivots) >= 2:
        p1_idx, p1_val = price_low_pivots[-2]
        p2_idx, p2_val = price_low_pivots[-1]
        if p2_idx - p1_idx >= MIN_PIVOT_DISTANCE and p2_val < p1_val:
            r1 = rel_rsi[p1_idx]
            r2 = rel_rsi[p2_idx]
            if r1 is not None and r2 is not None and r2 > r1:
                # Bullish divergence!
                rsi_delta = r2 - r1
                price_delta_pct = (p1_val - p2_val) / p1_val * 100
                # Strength: bigger price LL + bigger RSI HL = stronger
                strength = int(min(100, rsi_delta * 5 + price_delta_pct * 3 + 30))
                bars_since_pivot = (len(rel_rsi) - 1) - p2_idx
                return {
                    'type': 'bullish',
                    'pair': pair, 'tf': tf,
                    'price_pivot1_bar': start + p1_idx,
                    'price_pivot1_value': round(p1_val, 8),
                    'price_pivot2_bar': start + p2_idx,
                    'price_pivot2_value': round(p2_val, 8),
                    'rsi_pivot1': round(r1, 1),
                    'rsi_pivot2': round(r2, 1),
                    'rsi_delta': round(rsi_delta, 1),
                    'price_delta_pct': round(price_delta_pct, 2),
                    'strength': strength,
                    'bars_since_pivot': bars_since_pivot,
                    'last_close': closes[-1],
                    'last_rsi': round(rsi[-1], 1),
                    'detected_at_ms': kl[-1][0],
                }

    # Bearish: 2 higher highs in price, 2 lower highs in RSI
    price_high_pivots = _find_pivots_high(rel_highs, PIVOT_WINDOW)
    if len(price_high_pivots) >= 2:
        p1_idx, p1_val = price_high_pivots[-2]
        p2_idx, p2_val = price_high_pivots[-1]
        if p2_idx - p1_idx >= MIN_PIVOT_DISTANCE and p2_val > p1_val:
            r1 = rel_rsi[p1_idx]
            r2 = rel_rsi[p2_idx]
            if r1 is not None and r2 is not None and r2 < r1:
                rsi_delta = r1 - r2
                price_delta_pct = (p2_val - p1_val) / p1_val * 100
                strength = int(min(100, rsi_delta * 5 + price_delta_pct * 3 + 30))
                bars_since_pivot = (len(rel_rsi) - 1) - p2_idx
                return {
                    'type': 'bearish',
                    'pair': pair, 'tf': tf,
                    'price_pivot1_bar': start + p1_idx,
                    'price_pivot1_value': round(p1_val, 8),
                    'price_pivot2_bar': start + p2_idx,
                    'price_pivot2_value': round(p2_val, 8),
                    'rsi_pivot1': round(r1, 1),
                    'rsi_pivot2': round(r2, 1),
                    'rsi_delta': round(rsi_delta, 1),
                    'price_delta_pct': round(price_delta_pct, 2),
                    'strength': strength,
                    'bars_since_pivot': bars_since_pivot,
                    'last_close': closes[-1],
                    'last_rsi': round(rsi[-1], 1),
                    'detected_at_ms': kl[-1][0],
                }

    return None


def detect_divergence_at_bar(klines: list, target_bar_idx: int,
                              rsi: list = None) -> Optional[dict]:
    """Detect divergence ending at specific bar (for backtesting).
    klines: list of (t,o,h,l,c,v). target_bar_idx: index ≤ len(klines)-1.
    """
    if not klines or target_bar_idx < 30:
        return None
    closes = [k[4] for k in klines[:target_bar_idx + 1]]
    highs = [k[2] for k in klines[:target_bar_idx + 1]]
    lows = [k[3] for k in klines[:target_bar_idx + 1]]
    if rsi is None:
        rsi = _compute_rsi(closes, 14)
    else:
        rsi = rsi[:target_bar_idx + 1]
    if rsi[-1] is None:
        return None
    end = len(closes) - 1
    start = max(0, end - MAX_LOOKBACK_BARS)
    rel_lows = lows[start:end + 1]
    rel_highs = highs[start:end + 1]
    rel_rsi = rsi[start:end + 1]

    # Bullish
    pivots_l = _find_pivots_low(rel_lows, PIVOT_WINDOW)
    if len(pivots_l) >= 2:
        p1_idx, p1_val = pivots_l[-2]
        p2_idx, p2_val = pivots_l[-1]
        if p2_idx - p1_idx >= MIN_PIVOT_DISTANCE and p2_val < p1_val:
            r1 = rel_rsi[p1_idx]; r2 = rel_rsi[p2_idx]
            if r1 is not None and r2 is not None and r2 > r1:
                rsi_delta = r2 - r1
                price_delta_pct = (p1_val - p2_val) / p1_val * 100
                strength = int(min(100, rsi_delta * 5 + price_delta_pct * 3 + 30))
                return {'type': 'bullish', 'strength': strength,
                        'rsi_pivot1': r1, 'rsi_pivot2': r2,
                        'rsi_delta': round(rsi_delta, 1),
                        'price_delta_pct': round(price_delta_pct, 2)}

    pivots_h = _find_pivots_high(rel_highs, PIVOT_WINDOW)
    if len(pivots_h) >= 2:
        p1_idx, p1_val = pivots_h[-2]
        p2_idx, p2_val = pivots_h[-1]
        if p2_idx - p1_idx >= MIN_PIVOT_DISTANCE and p2_val > p1_val:
            r1 = rel_rsi[p1_idx]; r2 = rel_rsi[p2_idx]
            if r1 is not None and r2 is not None and r2 < r1:
                rsi_delta = r1 - r2
                price_delta_pct = (p2_val - p1_val) / p1_val * 100
                strength = int(min(100, rsi_delta * 5 + price_delta_pct * 3 + 30))
                return {'type': 'bearish', 'strength': strength,
                        'rsi_pivot1': r1, 'rsi_pivot2': r2,
                        'rsi_delta': round(rsi_delta, 1),
                        'price_delta_pct': round(price_delta_pct, 2)}
    return None
