"""Claude API budget monitoring.

Логирует каждый вызов Claude с token counts. Позволяет:
- видеть дневной расход ($)
- алертить когда подходим к бюджету
- дебажить аномальный расход
"""
from __future__ import annotations
import logging
from datetime import datetime, timedelta
from typing import Optional

from database import _get_db, utcnow

logger = logging.getLogger(__name__)

# Стоимость per-1M токенов для текущих моделей (USD)
# Claude Sonnet 4: $3 input / $15 output
# Claude Opus 3: $15 / $75
MODEL_PRICES = {
    "claude-sonnet-4-5-20250929": (3.0, 15.0),
    "claude-opus-4-20250514": (15.0, 75.0),
    "claude-3-5-sonnet-20241022": (3.0, 15.0),
    "claude-3-5-haiku-20241022": (0.8, 4.0),
}

# Default дневной бюджет (можно переопределить через env)
import os
DAILY_BUDGET_USD = float(os.getenv("CLAUDE_DAILY_BUDGET_USD", "5.0"))


def _col():
    return _get_db().claude_usage


def log_usage(model: str, input_tokens: int, output_tokens: int, op: str = "") -> dict:
    """Логирует один вызов Claude. Возвращает расчёт стоимости."""
    in_price, out_price = MODEL_PRICES.get(model, (3.0, 15.0))
    cost_usd = (input_tokens * in_price + output_tokens * out_price) / 1_000_000
    doc = {
        "at": utcnow(),
        "model": model,
        "op": op,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost_usd": round(cost_usd, 6),
    }
    try:
        _col().insert_one(doc)
    except Exception as e:
        logger.warning(f"[claude-budget] log failed: {e}")
    return doc


def get_daily_usage() -> dict:
    """Расход за текущие сутки (UTC)."""
    now = utcnow()
    day_start = datetime(now.year, now.month, now.day)
    try:
        pipeline = [
            {"$match": {"at": {"$gte": day_start}}},
            {"$group": {"_id": None,
                        "cost": {"$sum": "$cost_usd"},
                        "in_tokens": {"$sum": "$input_tokens"},
                        "out_tokens": {"$sum": "$output_tokens"},
                        "calls": {"$sum": 1}}},
        ]
        agg = list(_col().aggregate(pipeline))
        if agg:
            a = agg[0]
            cost = round(a.get("cost") or 0, 4)
            pct = (cost / DAILY_BUDGET_USD) * 100 if DAILY_BUDGET_USD else 0
            return {
                "day_cost_usd": cost,
                "daily_budget_usd": DAILY_BUDGET_USD,
                "pct_used": round(pct, 1),
                "remaining_usd": round(max(0, DAILY_BUDGET_USD - cost), 4),
                "calls": a.get("calls") or 0,
                "input_tokens": a.get("in_tokens") or 0,
                "output_tokens": a.get("out_tokens") or 0,
                "status": "critical" if pct >= 90 else "warning" if pct >= 70 else "ok",
            }
    except Exception as e:
        logger.warning(f"[claude-budget] stats fail: {e}")
    return {"day_cost_usd": 0, "daily_budget_usd": DAILY_BUDGET_USD, "pct_used": 0,
            "remaining_usd": DAILY_BUDGET_USD, "calls": 0, "status": "ok"}


def check_budget_allowed() -> bool:
    """Возвращает False если бюджет исчерпан — вызывающий должен отказать."""
    usage = get_daily_usage()
    return usage["day_cost_usd"] < DAILY_BUDGET_USD
