"""V-Bottom Scanner — market-wide detector.

Скан всех BingX swap pairs через Binance fapi klines 15m.
При обнаружении V-Bottom (drop≥3% + reversal candle) на свежей свече —
эмит signal в v_bottom_signals collection + route через _paper_on_signal.

Backtest 7d (8297 signals): 200 V-Bottom hits → WR 98%, MFE +4.37% median,
MFE p75 +12.24% (strongest edge from confluence analysis).
"""
from __future__ import annotations
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


CONFIG = {
    'min_drop_pct':           3.0,    # минимальный obval за 1-2 свечи
    'min_24h_vol_usdt':  10_000_000,  # ≥ $10M в день (избегаем micro-cap noise)
    'max_24h_vol_usdt':  900_000_000, # ≤ $900M (фокус на mid-cap + большие)
    'tf':                    '15m',
    'klines_lookback':         50,    # сколько баров фетчим
    'scan_interval_s':         300,   # 5 мин между сканами
    'dedup_window_min':         30,   # 30 мин cooldown per pair
    'detect_v_top':         True,    # ловить и v_top (SHORT signal)
    'parallel_workers':       16,
}


def update_config(**kwargs):
    for k, v in kwargs.items():
        if k in CONFIG:
            CONFIG[k] = v
    return dict(CONFIG)


def _check_pair(pair: str, vol_24h: float, price: float) -> Optional[dict]:
    """Скан одной пары через fapi klines + v_pattern detection."""
    if not (CONFIG['min_24h_vol_usdt'] <= vol_24h < CONFIG['max_24h_vol_usdt']):
        return None
    try:
        from divergence import _fetch_klines_fapi
        from v_pattern import detect_v_pattern_at_bar
        kl = _fetch_klines_fapi(pair, CONFIG['tf'], CONFIG['klines_lookback'])
        if not kl or len(kl) < 5:
            return None
        # Convert to list of tuples format expected by v_pattern
        kl_tuples = [(int(k[0]), float(k[1]), float(k[2]), float(k[3]),
                       float(k[4]), float(k[5])) for k in kl]
        # Check last bar (most recent close)
        result = detect_v_pattern_at_bar(kl_tuples, len(kl_tuples) - 1,
                                          CONFIG['min_drop_pct'])
        if not result:
            return None
        if result['type'] == 'v_top' and not CONFIG['detect_v_top']:
            return None
        # Determine direction
        direction = 'LONG' if result['type'] == 'v_bottom' else 'SHORT'
        return {
            'pair': pair,
            'symbol': pair.replace('/', '').upper(),
            'direction': direction,
            'entry': result['close'],
            'pattern_type': result['type'],
            'drop_pct': result.get('drop_pct') or result.get('rise_pct'),
            'reversal_pct': result['reversal_pct'],
            'vol_24h_usdt': round(vol_24h, 0),
            'price_24h_change': None,  # filled in scan
            'detected_at': datetime.now(timezone.utc),
            'detected_at_ms': result['detected_at_ms'],
        }
    except Exception as e:
        logger.debug(f'[v-bottom] pair {pair}: {e}')
        return None


def _fetch_tickers() -> list:
    """Binance fapi tickers с BingX fallback."""
    try:
        import httpx
        r = httpx.get('https://fapi.binance.com/fapi/v1/ticker/24hr', timeout=10)
        if r.status_code == 200:
            out = []
            for t in r.json():
                sym = str(t.get('symbol',''))
                if sym.endswith('USDT'):
                    out.append({
                        'symbol': sym,
                        'quoteVolume': t.get('quoteVolume',0),
                        'lastPrice': t.get('lastPrice',0),
                        'priceChangePercent': t.get('priceChangePercent'),
                    })
            return out
    except Exception:
        pass
    # BingX fallback
    try:
        import ccxt
        ex = ccxt.bingx({'options':{'defaultType':'swap'},'enableRateLimit':True})
        bx = ex.fetch_tickers()
        out = []
        for sym, t in (bx or {}).items():
            if sym.endswith(':USDT'):
                base = sym.split(':')[0]
                if '/USDT' in base:
                    out.append({
                        'symbol': base.replace('/',''),
                        'quoteVolume': t.get('quoteVolume') or 0,
                        'lastPrice': t.get('last') or 0,
                        'priceChangePercent': t.get('percentage'),
                    })
        return out
    except Exception as e:
        logger.warning(f'[v-bottom] all tickers sources failed: {e}')
        return []


def scan() -> list[dict]:
    """Market-wide scan. Returns list of trigger dicts."""
    triggers: list = []
    t0 = time.time()
    tickers_data = _fetch_tickers()
    if not tickers_data:
        return []

    # Filter mid-cap candidates
    candidates: list = []
    for t in (tickers_data or []):
        try:
            sym = str(t.get('symbol') or '')
            if not sym.endswith('USDT') or len(sym) <= 4:
                continue
            base = sym[:-4]
            pair_display = f'{base}/USDT'
            qvol = float(t.get('quoteVolume') or 0)
            price = float(t.get('lastPrice') or 0)
            ch24 = t.get('priceChangePercent')
            if ch24 is not None:
                try: ch24 = float(ch24)
                except Exception: ch24 = None
            if qvol <= 0 or price <= 0:
                continue
            if not (CONFIG['min_24h_vol_usdt'] <= qvol < CONFIG['max_24h_vol_usdt']):
                continue
            candidates.append((pair_display, qvol, price, ch24))
        except Exception:
            continue
    logger.info(f'[v-bottom] {len(candidates)} candidates (24h vol filter passed)')

    # Parallel check
    n_workers = min(CONFIG['parallel_workers'], len(candidates))
    with ThreadPoolExecutor(max_workers=n_workers) as tp:
        futs = {tp.submit(_check_pair, p, v, pr): (p, ch) for (p, v, pr, ch) in candidates}
        for f in as_completed(futs, timeout=120):
            try:
                r = f.result(timeout=0.3)
                if r:
                    p, ch = futs[f]
                    r['price_24h_change'] = ch
                    triggers.append(r)
            except Exception:
                continue
    elapsed = time.time() - t0
    logger.info(f'[v-bottom] {len(triggers)} triggers in {elapsed:.1f}s')
    return triggers


def emit_signal(trigger: dict) -> bool:
    """Insert в v_bottom_signals с dedup."""
    try:
        from database import _get_db
        db = _get_db()
        col = db.v_bottom_signals
        try:
            col.create_index([('pair', 1), ('detected_at', -1)])
            col.create_index([('detected_at', -1)])
        except Exception:
            pass
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=CONFIG['dedup_window_min'])
        exists = col.find_one({'pair': trigger['pair'],
                               'pattern_type': trigger['pattern_type'],
                               'detected_at': {'$gte': cutoff}})
        if exists:
            return False
        col.insert_one({**trigger, 'source': 'v_bottom'})
        logger.info(f"[v-bottom] EMIT {trigger['pair']} {trigger['pattern_type']} "
                    f"drop={trigger['drop_pct']:.1f}% rev={trigger['reversal_pct']:.1f}%")
        return True
    except Exception as e:
        logger.warning(f'[v-bottom] emit fail: {e}')
        return False
