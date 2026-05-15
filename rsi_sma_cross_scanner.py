"""RSI/SMA(RSI) crossover Scanner на 12h с Volume confirmation.

Логика:
  1. Каждый bar close (12h) — пересчёт для всех USDT-perp pairs
  2. На последнем баре проверяем crossover RSI(14) с SMA(14, RSI)
  3. Volume confirmation: vol_ratio[last] / MA20 ≥ min_vol_ratio
  4. Если bullish cross → LONG signal, bearish → SHORT
  5. Emit в rsi_sma_cross_signals collection + paper_on_signal

Backtest 14d (286 пар, 12h tf):
  LONG vol≥2.0× : N=208 WR_final=62% MFE_med=+9.83% Final_med=+2.45%
  Best combo (LONG + RSI<50 + vol≥2×): WR 66.7%, MFE +12%, Final +3.13%
"""
from __future__ import annotations
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


CONFIG = {
    'tf':                 '12h',
    'min_vol_ratio':      2.0,        # vol[i] / MA20 ≥ 2× confirmation
    'min_24h_vol_usdt': 5_000_000,    # baseline filter
    'max_24h_vol_usdt': 2_000_000_000,
    'scan_interval_s':   600,         # каждые 10 мин (12h candle закрывается 2 раза в день)
    'dedup_window_min':  720,         # 12h cooldown per pair (1 cross per candle)
    'klines_limit':      50,          # достаточно для RSI(14) + SMA(14) + MA20(vol)
    'parallel_workers':  16,
    # Optional Quality filter (RSI<50 для LONG = strongest by backtest)
    'long_rsi_max':      None,        # None = no filter; 50 = только oversold cross
}


def update_config(**kwargs):
    for k, v in kwargs.items():
        if k in CONFIG:
            CONFIG[k] = v
    return dict(CONFIG)


def _compute_rsi(closes, p=14):
    n = len(closes)
    if n < p+1: return [None]*n
    rsi=[None]*n; gains=[0.0]; losses=[0.0]
    for i in range(1,n):
        ch=closes[i]-closes[i-1]
        gains.append(max(ch,0.0)); losses.append(max(-ch,0.0))
    avg_g=sum(gains[1:p+1])/p; avg_l=sum(losses[1:p+1])/p
    rsi[p]=100.0 if avg_l==0 else (100-100/(1+avg_g/avg_l))
    for i in range(p+1,n):
        avg_g=(avg_g*(p-1)+gains[i])/p
        avg_l=(avg_l*(p-1)+losses[i])/p
        rsi[i]=100.0 if avg_l==0 else (100-100/(1+avg_g/avg_l))
    return rsi


def _compute_sma(values, p=14):
    n=len(values); out=[None]*n; buf=[]; s=0.0
    for i,v in enumerate(values):
        if v is None: continue
        buf.append(v); s+=v
        if len(buf)>p: s-=buf.pop(0)
        if len(buf)==p: out[i]=s/p
    return out


def _fetch_klines(pair: str) -> Optional[list]:
    """Binance fapi 12h klines."""
    try:
        from divergence import _fetch_klines_fapi
        return _fetch_klines_fapi(pair, CONFIG['tf'], CONFIG['klines_limit'])
    except Exception as e:
        logger.debug(f'[rsi-cross] klines fail {pair}: {e}')
        return None


def _check_pair(pair: str, vol_24h: float) -> Optional[dict]:
    """Check single pair for crossover at LAST closed bar."""
    if not (CONFIG['min_24h_vol_usdt'] <= vol_24h < CONFIG['max_24h_vol_usdt']):
        return None
    kl = _fetch_klines(pair)
    if not kl or len(kl) < 35:  # need warmup
        return None
    closes = [float(k[4]) for k in kl]
    volumes = [float(k[5]) for k in kl]
    rsi = _compute_rsi(closes, 14)
    sma = _compute_sma(rsi, 14)
    if rsi[-1] is None or rsi[-2] is None or sma[-1] is None or sma[-2] is None:
        return None
    # Crossover detection at last bar
    r_prev, r_cur = rsi[-2], rsi[-1]
    s_prev, s_cur = sma[-2], sma[-1]
    is_bullish = r_prev <= s_prev and r_cur > s_cur
    is_bearish = r_prev >= s_prev and r_cur < s_cur
    if not (is_bullish or is_bearish):
        return None
    # Volume MA20
    if len(volumes) < 21:
        return None
    vol_ma20 = sum(volumes[-21:-1]) / 20
    vol_ratio = volumes[-1] / vol_ma20 if vol_ma20 > 0 else 0
    if vol_ratio < CONFIG['min_vol_ratio']:
        return None
    # Direction
    direction = 'LONG' if is_bullish else 'SHORT'
    # Optional RSI filter for quality
    if direction == 'LONG' and CONFIG.get('long_rsi_max') is not None:
        if r_cur > CONFIG['long_rsi_max']:
            return None
    return {
        'pair': pair,
        'symbol': pair.replace('/','').upper(),
        'direction': direction,
        'cross_type': 'bullish' if is_bullish else 'bearish',
        'entry': closes[-1],
        'rsi': round(r_cur, 2),
        'sma_rsi': round(s_cur, 2),
        'rsi_distance': round(r_cur - s_cur, 2),
        'vol_ratio': round(vol_ratio, 2),
        'vol_24h_usdt': round(vol_24h, 0),
        'bar_open_ms': int(kl[-1][0]),
        'detected_at': datetime.now(timezone.utc),
    }


def scan() -> list[dict]:
    """Market-wide scan для текущего 12h close."""
    t0 = time.time()
    # Tickers from Binance fapi
    try:
        from volume_explosion import _fetch_fapi_24h_tickers
        tickers = _fetch_fapi_24h_tickers()
        if tickers is None:
            from volume_explosion import _get_ex
            ex = _get_ex()
            if not ex: return []
            bx = ex.fetch_tickers()
            tickers = []
            for sym, t in (bx or {}).items():
                if sym.endswith(':USDT'):
                    base = sym.split(':')[0]
                    if '/USDT' in base:
                        tickers.append({'symbol': base.replace('/',''),
                                        'quoteVolume': t.get('quoteVolume') or 0})
    except Exception as e:
        logger.warning(f'[rsi-cross] tickers fail: {e}')
        return []

    candidates = []
    for t in tickers:
        try:
            sym = str(t.get('symbol',''))
            if not sym.endswith('USDT') or len(sym) <= 4: continue
            base = sym[:-4]
            pair = f'{base}/USDT'
            qvol = float(t.get('quoteVolume') or 0)
            if qvol <= 0: continue
            if not (CONFIG['min_24h_vol_usdt'] <= qvol < CONFIG['max_24h_vol_usdt']): continue
            candidates.append((pair, qvol))
        except Exception:
            continue
    logger.info(f'[rsi-cross] scanning {len(candidates)} pairs')

    triggers = []
    n_workers = min(CONFIG['parallel_workers'], len(candidates))
    with ThreadPoolExecutor(max_workers=n_workers) as tp:
        futs = {tp.submit(_check_pair, p, v): p for p,v in candidates}
        for f in as_completed(futs, timeout=120):
            try:
                r = f.result(timeout=0.3)
                if r: triggers.append(r)
            except Exception:
                continue
    logger.info(f'[rsi-cross] {len(triggers)} triggers in {time.time()-t0:.1f}s')
    return triggers


def emit_signal(trigger: dict) -> bool:
    """Insert with dedup. 1 signal per (pair, bar_open_ms)."""
    try:
        from database import _get_db
        db = _get_db()
        col = db.rsi_sma_cross_signals
        try:
            col.create_index([('pair', 1), ('bar_open_ms', 1)], unique=True)
            col.create_index([('detected_at', -1)])
        except Exception:
            pass
        try:
            col.insert_one({**trigger, 'source': 'rsi_cross_12h'})
            logger.info(f"[rsi-cross] EMIT {trigger['pair']} {trigger['cross_type']} "
                        f"rsi={trigger['rsi']} vol_ratio={trigger['vol_ratio']}")
            return True
        except Exception:
            return False  # duplicate
    except Exception as e:
        logger.warning(f'[rsi-cross] emit fail: {e}')
        return False
