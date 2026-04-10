"""Polymarket API клиент — список рынков, odds, объёмы.

Gamma API (публичный, без аутентификации):
- GET /markets — список всех рынков
- GET /markets/{id} — детали рынка
"""
import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"


def get_active_markets(
    limit: int = 100,
    min_volume: float = 5000,
    active_only: bool = True,
) -> list[dict]:
    """Возвращает список активных рынков Polymarket."""
    try:
        r = httpx.get(
            f"{GAMMA_BASE}/markets",
            params={
                "limit": limit,
                "active": active_only,
                "closed": False,
                "order": "volume24hr",
                "ascending": False,
            },
            timeout=15,
        )
        if r.status_code != 200:
            logger.error(f"Polymarket API {r.status_code}: {r.text[:200]}")
            return []
        markets = r.json()
        # Фильтруем по минимальному объёму
        filtered = []
        for m in markets:
            vol = float(m.get("volume24hr", 0) or 0)
            if vol >= min_volume:
                filtered.append(_parse_market(m))
        return filtered
    except Exception as e:
        logger.error(f"Polymarket fetch error: {e}")
        return []


def get_market(market_id: str) -> Optional[dict]:
    """Детали одного рынка."""
    try:
        r = httpx.get(f"{GAMMA_BASE}/markets/{market_id}", timeout=10)
        if r.status_code != 200:
            return None
        return _parse_market(r.json())
    except Exception as e:
        logger.error(f"Polymarket market {market_id}: {e}")
        return None


def _parse_market(m: dict) -> dict:
    """Нормализует данные рынка в наш формат."""
    # Outcomes и цены
    outcomes = m.get("outcomes", [])
    outcome_prices = m.get("outcomePrices", [])

    # Парсим цены
    yes_price = None
    no_price = None
    if outcome_prices:
        try:
            if isinstance(outcome_prices, str):
                import json
                outcome_prices = json.loads(outcome_prices)
            if len(outcome_prices) >= 2:
                yes_price = float(outcome_prices[0])
                no_price = float(outcome_prices[1])
            elif len(outcome_prices) == 1:
                yes_price = float(outcome_prices[0])
                no_price = 1.0 - yes_price
        except Exception:
            pass

    # Категория
    tags = m.get("tags", [])
    category = tags[0].get("label", "Other") if tags and isinstance(tags, list) and isinstance(tags[0], dict) else "Other"
    if isinstance(tags, list) and tags and isinstance(tags[0], str):
        category = tags[0]

    return {
        "id": m.get("id", ""),
        "question": m.get("question", ""),
        "description": (m.get("description", "") or "")[:500],
        "category": category,
        "yes_price": yes_price,
        "no_price": no_price,
        "volume_24h": float(m.get("volume24hr", 0) or 0),
        "volume_total": float(m.get("volumeNum", 0) or 0),
        "liquidity": float(m.get("liquidityNum", 0) or 0),
        "end_date": m.get("endDate"),
        "image": m.get("image"),
        "active": m.get("active", True),
        "closed": m.get("closed", False),
        "outcomes": outcomes if isinstance(outcomes, list) else [],
    }
