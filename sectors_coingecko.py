"""CoinGecko sectors fetcher + sector rotation detector.

Тянет category tags для каждой монеты через CoinGecko API.
Cache 24h (categories редко меняются).

Sector rotation: 2+ пары одной темы с volume_score ≥ 1.5 → весь сектор
помечается как "active" — другие пары темы — high-probability candidates.

API: https://api.coingecko.com/api/v3/coins/markets — есть category tags
     https://api.coingecko.com/api/v3/coins/categories/list — full list

Бесплатный план: 10-30 calls/min. Кешируем агрессивно.
"""
from __future__ import annotations
import logging
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

_sector_cache: dict = {}   # pair → {'categories': [...], 'ts': ts}
_CACHE_TTL_S = 86400 * 7   # 7 дней (категории очень редко меняются)

# Manual fallback для основных пар если CoinGecko не отвечает
_FALLBACK_SECTORS = {
    'BTC': ['layer-1'],
    'ETH': ['layer-1', 'smart-contract-platform'],
    'SOL': ['layer-1', 'solana-ecosystem'],
    'AVAX': ['layer-1', 'smart-contract-platform'],
    'NEAR': ['layer-1'],
    'ATOM': ['layer-1'],
    'APT': ['layer-1'],
    'SUI': ['layer-1'],
    'ARB': ['layer-2', 'arbitrum-ecosystem'],
    'OP': ['layer-2'],
    'MATIC': ['layer-2'],
    'BASE': ['layer-2', 'base-ecosystem'],
    'ZEC': ['privacy-coins'],
    'DASH': ['privacy-coins'],
    'XMR': ['privacy-coins'],
    'DOGE': ['meme-token'],
    'WIF': ['meme-token', 'solana-ecosystem'],
    'PEPE': ['meme-token'],
    'BONK': ['meme-token', 'solana-ecosystem'],
    'SHIB': ['meme-token'],
    'FIDA': ['solana-ecosystem', 'oracle'],
    'JTO': ['solana-ecosystem'],
    'JUP': ['solana-ecosystem', 'dex'],
    'PYTH': ['solana-ecosystem', 'oracle'],
    'UNI': ['dex', 'defi'],
    'AAVE': ['lending-borrowing', 'defi'],
    'CRV': ['dex', 'defi'],
    'GMX': ['dex', 'defi', 'arbitrum-ecosystem'],
    'CAKE': ['dex', 'defi', 'binance-smart-chain-ecosystem'],
    'AXS': ['gaming', 'play-to-earn'],
    'GALA': ['gaming'],
    'ILV': ['gaming'],
    'BEAM': ['gaming'],
    'IMX': ['gaming', 'layer-2'],
    'FET': ['artificial-intelligence', 'ai-meme'],
    'AGIX': ['artificial-intelligence'],
    'RNDR': ['artificial-intelligence'],
    'ARKM': ['artificial-intelligence'],
    'TAO': ['artificial-intelligence'],
    'NMR': ['artificial-intelligence'],
}

_coin_id_cache: dict = {}  # ticker → CoinGecko ID
_categories_pair_cache: dict = {}  # full pair → categories list


def _http_get(url, params=None, timeout=10):
    try:
        with httpx.Client(timeout=timeout) as c:
            return c.get(url, params=params or {})
    except Exception:
        return None


def get_sectors(pair: str) -> list:
    """Returns list of category slugs e.g. ['layer-1', 'solana-ecosystem'].
    Empty list if not found / cache miss."""
    now = int(time.time())
    cached = _categories_pair_cache.get(pair)
    if cached and (now - cached['ts']) < _CACHE_TTL_S:
        return cached['categories']

    # Extract ticker
    ticker = pair.split('/')[0].upper().replace('USDT', '').strip()

    # Try fallback first (instant)
    if ticker in _FALLBACK_SECTORS:
        cats = _FALLBACK_SECTORS[ticker]
        _categories_pair_cache[pair] = {'categories': cats, 'ts': now}
        return cats

    # Try CoinGecko
    try:
        # Search coin id
        if ticker not in _coin_id_cache:
            r = _http_get('https://api.coingecko.com/api/v3/search',
                          params={'query': ticker})
            if r and r.status_code == 200:
                d = r.json()
                coins = d.get('coins', [])
                # Best match by exact symbol
                exact = next((c for c in coins if c.get('symbol', '').upper() == ticker), None)
                if exact:
                    _coin_id_cache[ticker] = exact['id']
                elif coins:
                    _coin_id_cache[ticker] = coins[0]['id']
        coin_id = _coin_id_cache.get(ticker)
        if not coin_id:
            _categories_pair_cache[pair] = {'categories': [], 'ts': now}
            return []

        # Fetch coin info
        r = _http_get(f'https://api.coingecko.com/api/v3/coins/{coin_id}',
                      params={'localization': 'false', 'tickers': 'false',
                              'market_data': 'false', 'community_data': 'false',
                              'developer_data': 'false', 'sparkline': 'false'})
        if r and r.status_code == 200:
            d = r.json()
            cats = d.get('categories', [])
            # Normalize to slugs
            cats = [c.lower().replace(' ', '-') if c else '' for c in cats if c]
            cats = [c for c in cats if c][:8]  # max 8 categories
            _categories_pair_cache[pair] = {'categories': cats, 'ts': now}
            return cats
    except Exception as e:
        logger.debug(f'[sectors_coingecko] {pair}: {e}')

    _categories_pair_cache[pair] = {'categories': [], 'ts': now}
    return []


def get_sectors_bulk(pairs: list) -> dict:
    """Returns {pair: [categories]}. Batch fetch with rate limiting."""
    out = {}
    for p in pairs:
        out[p] = get_sectors(p)
        time.sleep(0.5)  # CoinGecko rate limit (free tier ~10-30/min)
    return out


def detect_sector_rotation(pair_volume_scores: dict, pair_sectors: dict,
                            min_pairs: int = 2, min_vol_score: float = 1.5) -> dict:
    """Returns {sector: {'active': bool, 'pairs': [...], 'count': N}}.

    Sector активен если 2+ пары в нём имеют volume_score ≥ min_vol_score.
    """
    from collections import defaultdict
    sector_pairs: dict = defaultdict(list)
    for pair, sectors in pair_sectors.items():
        vol_score = pair_volume_scores.get(pair, 0.0)
        if vol_score < min_vol_score: continue
        for s in sectors:
            sector_pairs[s].append((pair, vol_score))

    result = {}
    for sector, pairs_list in sector_pairs.items():
        if len(pairs_list) >= min_pairs:
            result[sector] = {
                'active': True,
                'pairs': [p for p, _ in pairs_list],
                'count': len(pairs_list),
                'avg_vol_score': sum(s for _, s in pairs_list) / len(pairs_list),
            }
    return result


def clear_cache():
    _sector_cache.clear()
    _coin_id_cache.clear()
    _categories_pair_cache.clear()
