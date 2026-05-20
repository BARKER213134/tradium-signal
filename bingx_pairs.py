"""BingX pairs filter — список USDT-perp пар которые торгуются на BingX.

Pre-Pump predictor сканит ВСЕ пары на Binance, но auto-trade и alerts
имеют смысл только для пар которые есть на BingX (где мы торгуем real).
Иначе сигнал придёт, но войти невозможно (F0 reject).

API: https://open-api.bingx.com/openApi/swap/v2/quote/contracts
"""
from __future__ import annotations
import logging
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

_pairs_cache: dict = {'pairs': None, 'ts': 0}
_CACHE_TTL_S = 3600  # 1h (списки меняются редко)

_http = httpx.Client(timeout=10.0)


def get_bingx_usdt_perp_pairs() -> set:
    """Returns set of pair strings: {'BTC/USDT', 'ETH/USDT', ...} on BingX."""
    now = int(time.time())
    if _pairs_cache['pairs'] is not None and (now - _pairs_cache['ts']) < _CACHE_TTL_S:
        return _pairs_cache['pairs']

    pairs = set()
    try:
        # BingX swap API
        r = _http.get('https://open-api.bingx.com/openApi/swap/v2/quote/contracts')
        if r.status_code == 200:
            data = r.json() or {}
            contracts = data.get('data', []) or []
            for c in contracts:
                sym = c.get('symbol', '')
                # BingX format: 'BTC-USDT'
                if '-USDT' in sym:
                    parts = sym.split('-')
                    if len(parts) == 2 and parts[1] == 'USDT':
                        pairs.add(f'{parts[0]}/USDT')
            logger.info(f'[bingx_pairs] fetched {len(pairs)} BingX USDT-perp pairs')
    except Exception as e:
        logger.debug(f'[bingx_pairs] fetch fail: {e}')

    if pairs:
        _pairs_cache['pairs'] = pairs
        _pairs_cache['ts'] = now
    elif _pairs_cache['pairs'] is None:
        # Empty result — return empty set but don't cache (try again next call)
        return set()

    return pairs


def is_on_bingx(pair: str) -> bool:
    """True if pair available on BingX USDT-perp."""
    pairs = get_bingx_usdt_perp_pairs()
    # Normalize: 'BTCUSDT' → 'BTC/USDT', 'BTC/USDT' kept
    p = pair.upper()
    if '/' not in p and p.endswith('USDT'):
        p = p[:-4] + '/USDT'
    return p in pairs


def filter_bingx_pairs(pair_list: list) -> list:
    """Returns only those pairs from pair_list which are on BingX."""
    bx = get_bingx_usdt_perp_pairs()
    if not bx:
        # BingX недоступен — возвращаем все, пусть F0 потом отрежет на live trade
        return pair_list
    out = []
    for p in pair_list:
        norm = p.upper()
        if '/' not in norm and norm.endswith('USDT'):
            norm = norm[:-4] + '/USDT'
        if norm in bx:
            out.append(p)
    return out


def clear_cache():
    _pairs_cache['pairs'] = None
    _pairs_cache['ts'] = 0
