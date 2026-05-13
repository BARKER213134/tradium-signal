"""Volume Explosion scanner — детектор всплеска положительного объёма
по ВСЕМУ рынку (BingX swap perpetuals).

Юзер-спецификация (13.05.26):
1. 24h Volume в USDT в окне [MIN, MAX] (mid-cap, default 20M-500M)
2. 1h positive delta volume > MIN_1H_POS_DELTA_USDT (default 1M USDT)
3. 30m positive delta volume вырос на ≥ MIN_30M_GROWTH_PCT% (default 1000% = 10×)

ВАЖНО: НЕ привязано к нашим сигналам / cluster_delta cache.
Качаем trades напрямую с BingX для всех mid-cap пар → market-wide coverage.
"""
from __future__ import annotations
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


# ─── Конфигурация (можно менять без рестарта через update_config) ────
CONFIG = {
    'min_24h_vol_usdt':       20_000_000,    # ≥ $20M в день
    'max_24h_vol_usdt':      500_000_000,    # < $500M
    'min_1h_pos_delta_usdt':   1_000_000,    # ≥ $1M positive delta за час
    'min_30m_growth_pct':           1000.0,  # +1000% на 30m (10×)
    'scan_interval_s':              300,     # 5 мин между сканами
    'dedup_window_min':              60,     # не дублировать на одну пару чаще раз в час
    'trades_lookback_hours':          1.5,   # сколько часов trades fetch'им per pair
    'parallel_workers':             16,      # параллельные fetch_trades
    'use_cluster_delta_fallback':   True,    # если trades fail — fallback на cache
}

# Cached ccxt instance (re-use across scans)
_ex_cached = None


def _get_ex():
    global _ex_cached
    if _ex_cached is None:
        try:
            import ccxt
            _ex_cached = ccxt.bingx({
                'options': {'defaultType': 'swap'},
                'enableRateLimit': True,
            })
        except Exception as e:
            logger.warning(f'[vol-explosion] ccxt init fail: {e}')
            _ex_cached = None
    return _ex_cached


def update_config(**kwargs):
    """Update CONFIG values on-the-fly. Возвращает новый CONFIG."""
    for k, v in kwargs.items():
        if k in CONFIG:
            CONFIG[k] = v
    return dict(CONFIG)


# ─── Binance fapi klines fetcher (market-wide, taker_buy_quote_volume) ──
# Binance kline возвращает 12 полей включая taker_buy_quote_volume (USDT купленный
# агрессивными покупателями). delta_usdt = 2 * taker_buy_quote - total_quote.
# Один call per pair = 8 свечей 15m (2h) — ровно что нужно для 1h + 30m growth.
import httpx

_http_cached = None

def _get_http():
    global _http_cached
    if _http_cached is None:
        # Pool size matches parallel_workers (16) с буфером для retry
        _http_cached = httpx.Client(
            timeout=8.0,
            limits=httpx.Limits(max_connections=40, max_keepalive_connections=30),
            headers={'User-Agent': 'tradium-signal-bot/1.0'},
        )
    return _http_cached


def _fetch_fapi_klines_with_delta(pair: str, limit: int = 8) -> Optional[list]:
    """Fetch Binance fapi 15m klines. Returns list of dicts:
    [{'open_ms', 'buy_quote_usdt', 'sell_quote_usdt', 'total_quote_usdt'}, ...]
    sorted desc by open_ms (newest first). Last item is current OPEN candle.
    """
    sym = pair.replace('/', '').upper()
    try:
        r = _get_http().get(
            'https://fapi.binance.com/fapi/v1/klines',
            params={'symbol': sym, 'interval': '15m', 'limit': limit},
        )
        if r.status_code != 200:
            return None
        out = []
        for k in r.json():
            try:
                open_ms = int(k[0])
                total_quote = float(k[7])               # quote_asset_volume USDT
                taker_buy_quote = float(k[10])          # taker_buy_quote_volume USDT
                sell_quote = max(0.0, total_quote - taker_buy_quote)
                out.append({
                    'open_ms': open_ms,
                    'buy_quote_usdt': taker_buy_quote,
                    'sell_quote_usdt': sell_quote,
                    'total_quote_usdt': total_quote,
                })
            except Exception:
                continue
        out.sort(key=lambda x: -x['open_ms'])  # newest first
        return out
    except Exception as e:
        logger.debug(f'[vol-explosion] fapi klines fail {pair}: {e}')
        return None


def _check_pair_via_fapi_klines(pair: str, vol_24h_usdt: float, current_price: float) -> Optional[dict]:
    """Проверка пары через Binance fapi klines (market-wide, без зависимости от cluster_delta).
    Использует taker_buy_quote_volume — прямой USDT-объём покупок per kline.
    """
    if not (CONFIG['min_24h_vol_usdt'] <= vol_24h_usdt < CONFIG['max_24h_vol_usdt']):
        return None
    bars = _fetch_fapi_klines_with_delta(pair, limit=8)
    if not bars or len(bars) < 4:
        return None
    # bars[0] = newest (current open). Take 4 most-recent для 1h, 2+2 для 30m growth.
    def pos_delta(b):
        return max(0.0, b['buy_quote_usdt'] - b['sell_quote_usdt'])

    last_4 = bars[:4]
    pos_1h_usdt = sum(pos_delta(b) for b in last_4)
    if pos_1h_usdt < CONFIG['min_1h_pos_delta_usdt']:
        return None
    last_2 = bars[:2]
    prev_2 = bars[2:4]
    last_30m_usdt = sum(pos_delta(b) for b in last_2)
    prev_30m_usdt = sum(pos_delta(b) for b in prev_2)
    if prev_30m_usdt < 1000:
        growth_pct = float('inf') if last_30m_usdt >= 100_000 else 0
    else:
        growth_pct = (last_30m_usdt - prev_30m_usdt) / prev_30m_usdt * 100
    if growth_pct < CONFIG['min_30m_growth_pct']:
        return None
    return {
        'pair': pair,
        'price': current_price,
        'vol_24h_usdt': round(vol_24h_usdt, 0),
        'pos_delta_usdt_1h': round(pos_1h_usdt, 0),
        'last_30m_pos_usdt': round(last_30m_usdt, 0),
        'prev_30m_pos_usdt': round(prev_30m_usdt, 0),
        'growth_30m_pct': round(growth_pct, 1) if growth_pct != float('inf') else 9999.0,
        'data_source': 'fapi_klines',
        'detected_at': datetime.now(timezone.utc),
    }


# Legacy/fallback функция через cluster_delta cache
def _check_pair_via_cluster_delta(pair: str, vol_24h_usdt: float, current_price: float) -> Optional[dict]:
    """Fallback: использовать cluster_delta cache если есть."""
    try:
        from database import _get_db
        col = _get_db().cluster_delta
        cutoff_ms = int(time.time() * 1000) - 4 * 15 * 60 * 1000
        deltas = list(col.find({
            'pair': pair, 'tf': '15m', 'open_ms': {'$gte': cutoff_ms},
        }, {'open_ms': 1, 'buy_vol': 1, 'sell_vol': 1, '_id': 0})
            .sort('open_ms', -1).limit(4))
        if len(deltas) < 4:
            return None
        last_2 = deltas[:2]
        prev_2 = deltas[2:4]
        last_30m = sum(max(0, float(d.get('buy_vol', 0) or 0) - float(d.get('sell_vol', 0) or 0)) for d in last_2) * current_price
        prev_30m = sum(max(0, float(d.get('buy_vol', 0) or 0) - float(d.get('sell_vol', 0) or 0)) for d in prev_2) * current_price
        pos_1h = sum(max(0, float(d.get('buy_vol', 0) or 0) - float(d.get('sell_vol', 0) or 0)) for d in deltas[:4]) * current_price
        if pos_1h < CONFIG['min_1h_pos_delta_usdt']:
            return None
        if prev_30m < 1000:
            growth = float('inf') if last_30m >= 100_000 else 0
        else:
            growth = (last_30m - prev_30m) / prev_30m * 100
        if growth < CONFIG['min_30m_growth_pct']:
            return None
        return {
            'pair': pair, 'price': current_price,
            'vol_24h_usdt': round(vol_24h_usdt, 0),
            'pos_delta_usdt_1h': round(pos_1h, 0),
            'last_30m_pos_usdt': round(last_30m, 0),
            'prev_30m_pos_usdt': round(prev_30m, 0),
            'growth_30m_pct': round(growth, 1) if growth != float('inf') else 9999.0,
            'data_source': 'cluster_delta_cache',
            'detected_at': datetime.now(timezone.utc),
        }
    except Exception:
        return None


def _check_pair(pair: str, vol_24h_usdt: float, current_price: float) -> Optional[dict]:
    """Главная проверка: fapi klines (market-wide, taker_buy_quote_volume),
    fallback на cluster_delta cache если включён."""
    result = _check_pair_via_fapi_klines(pair, vol_24h_usdt, current_price)
    if result:
        return result
    if CONFIG.get('use_cluster_delta_fallback'):
        return _check_pair_via_cluster_delta(pair, vol_24h_usdt, current_price)
    return None


def _fetch_fapi_24h_tickers() -> Optional[list]:
    """Binance fapi /ticker/24hr — все perpetual USDT pairs c 24h метриками.
    Это ОСНОВНОЙ источник (объёмы > BingX в 3-10×). Возвращает list of dicts:
    [{'symbol': 'BTCUSDT', 'quoteVolume': ..., 'lastPrice': ...}, ...]
    """
    try:
        r = _get_http().get('https://fapi.binance.com/fapi/v1/ticker/24hr', timeout=10)
        if r.status_code != 200:
            logger.warning(f'[vol-explosion] fapi tickers status {r.status_code}')
            return None
        return r.json()
    except Exception as e:
        logger.warning(f'[vol-explosion] fapi tickers fail: {e}')
        return None


def scan() -> list[dict]:
    """Market-wide скан Binance Futures USDT-perpetual pairs.

    Шаги:
      1. fapi /ticker/24hr → ~400 pairs, фильтр по 24h vol → 30-80 mid-cap pairs
      2. Параллельно (16 workers) — fapi /klines per pair (8 баров 15m)
      3. Compute USDT delta из taker_buy_quote_volume, проверка 4 условий

    Returns: list of trigger dicts.
    """
    t0 = time.time()
    # ОСНОВНОЙ источник — Binance fapi tickers (объёмы > BingX)
    tickers_data = _fetch_fapi_24h_tickers()
    use_bingx_fallback = False
    if tickers_data is None:
        # Fallback на BingX если fapi banned (locally)
        logger.warning('[vol-explosion] fapi tickers unavailable → fallback BingX')
        use_bingx_fallback = True
        ex = _get_ex()
        if ex is None:
            return []
        try:
            bx = ex.fetch_tickers()
        except Exception as e:
            logger.warning(f'[vol-explosion] BingX tickers also fail: {e}')
            return []
        tickers_data = []
        for sym, t in (bx or {}).items():
            if sym.endswith(':USDT'):
                base = sym.split(':')[0]
                if '/USDT' in base:
                    tickers_data.append({
                        'symbol': base.replace('/', ''),
                        'quoteVolume': t.get('quoteVolume') or 0,
                        'lastPrice': t.get('last') or 0,
                    })

    # Filter mid-cap pairs FIRST (cheap), THEN parallel deep-check
    candidates: list[tuple] = []  # (pair_display, vol_24h, price)
    for t in (tickers_data or []):
        try:
            sym = str(t.get('symbol') or '')
            if not sym.endswith('USDT') or len(sym) <= 4:
                continue
            base = sym[:-4]
            pair_display = f'{base}/USDT'
            qvol = float(t.get('quoteVolume') or 0)
            price = float(t.get('lastPrice') or 0)
            if qvol <= 0 or price <= 0:
                continue
            if not (CONFIG['min_24h_vol_usdt'] <= qvol < CONFIG['max_24h_vol_usdt']):
                continue
            candidates.append((pair_display, qvol, price))
        except Exception:
            continue

    source_lbl = 'BingX(fallback)' if use_bingx_fallback else 'Binance fapi'
    logger.info(f'[vol-explosion] scan from {source_lbl}: '
                f'{len(candidates)} mid-cap pairs to check')
    if not candidates:
        return []

    # Parallel deep-check (fapi klines per pair OR cluster_delta fallback)
    triggers: list = []
    n_workers = min(CONFIG.get('parallel_workers', 16), len(candidates))
    with ThreadPoolExecutor(max_workers=n_workers) as tp:
        futures = {tp.submit(_check_pair, p, v, pr): p for (p, v, pr) in candidates}
        from concurrent.futures import as_completed
        for f in as_completed(futures, timeout=90):
            try:
                r = f.result(timeout=0.5)
                if r:
                    triggers.append(r)
            except Exception:
                continue

    elapsed = time.time() - t0
    if triggers:
        logger.info(f'[vol-explosion] {len(triggers)} triggers '
                    f'(scanned {len(candidates)} pairs from {source_lbl} in {elapsed:.1f}s): '
                    f'{[t["pair"] for t in triggers[:10]]}')
    else:
        logger.info(f'[vol-explosion] 0 triggers ({len(candidates)} pairs '
                    f'from {source_lbl} scanned in {elapsed:.1f}s)')
    return triggers


def emit_signal(trigger: dict) -> bool:
    """Запись сигнала в Mongo + dedup check.
    Returns: True если эмитится новый, False если дубликат в окне dedup_window_min.
    """
    try:
        from database import _get_db
        db = _get_db()
        col = db.volume_explosion_signals
        # Indexes
        try:
            col.create_index([('pair', 1), ('detected_at', -1)])
            col.create_index([('detected_at', -1)])
        except Exception:
            pass
        # Dedup
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=CONFIG['dedup_window_min'])
        exists = col.find_one({'pair': trigger['pair'], 'detected_at': {'$gte': cutoff}})
        if exists:
            return False
        doc = {**trigger, 'source': 'vol_explosion'}
        col.insert_one(doc)
        logger.info(
            f"[vol-explosion] EMIT {trigger['pair']} "
            f"24h=${trigger['vol_24h_usdt']/1e6:.1f}M  "
            f"1h_pos=${trigger['pos_delta_usdt_1h']/1e6:.2f}M  "
            f"30m_grow={trigger['growth_30m_pct']:.0f}%"
        )
        return True
    except Exception as e:
        logger.warning(f'[vol-explosion] emit fail: {e}')
        return False
