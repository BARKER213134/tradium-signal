"""Live Trading Safety Layer — защита капитала при реальной торговле.

Проверки перед открытием позиции:
  1. Kill switch (глобальный стоп)
  2. Daily loss limit (% от начала UTC дня)
  3. Max drawdown (% от исторического equity peak)
  4. Position cap (max N одновременно)
  5. Min interval между сделками (anti-flood)
  6. Whitelist пар
  7. Min balance

Все параметры берутся из live_state.safety_preset + limits.
При срабатывании любой защиты → запрет открытия + лог + уведомление.
"""
from __future__ import annotations
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


# ═════ Пресеты безопасности ═════
# Пользователь переключает через 3 кнопки в UI.
SAFETY_PRESETS = {
    "conservative": {
        "label": "🛡️ Консервативный",
        "max_positions": 1,
        "max_size_pct": 3.0,
        "max_leverage": 2,
        "daily_loss_limit_pct": -5.0,   # -5% за сутки → stop
        "max_drawdown_pct": -10.0,       # -10% от пика → kill switch
        "min_interval_minutes": 15,
        "max_position_usd": 500,
        "min_balance_usd": 50,
    },
    "moderate": {
        "label": "⚖️ Умеренный",
        "max_positions": 2,
        "max_size_pct": 5.0,
        "max_leverage": 3,
        "daily_loss_limit_pct": -10.0,
        "max_drawdown_pct": -15.0,
        "min_interval_minutes": 10,
        "max_position_usd": 1500,
        "min_balance_usd": 50,
    },
    "aggressive": {
        "label": "🔥 Агрессивный",
        "max_positions": 3,
        "max_size_pct": 10.0,
        "max_leverage": 5,
        "daily_loss_limit_pct": -15.0,
        "max_drawdown_pct": -25.0,
        "min_interval_minutes": 5,
        "max_position_usd": 3000,
        "min_balance_usd": 50,
    },
    # Зеркало paper-trader: ТОЧНЫЕ те же лимиты что в paper.
    # max_positions=10 (== paper_trader.MAX_POSITIONS), max_size_pct=15%
    # (== mode.aggressive), max_leverage=9 (== mode.aggressive maximum).
    # Любое изменение в paper-логике должно отражаться здесь.
    "paper_mirror": {
        "label": "🪞 Paper Mirror — точная копия paper",
        "max_positions": 10,         # = paper_trader.MAX_POSITIONS
        "max_size_pct": 15.0,        # = paper aggressive max
        "max_leverage": 9,           # = paper aggressive max
        "daily_loss_limit_pct": -20.0,
        "max_drawdown_pct": -30.0,
        "min_interval_minutes": 0,   # paper не имеет min interval
        "max_position_usd": 10000,
        "min_balance_usd": 50,
    },
    # Для реального капитала — половина рисков paper'а.
    # Если paper делает +5%/нед, real ~+2.5% (минус slippage/funding/whales).
    # При балансе $1000: max 5 позиций × 5% × 5x = 125% notional, безопасно.
    "real_safe": {
        "label": "🟢 Real Safe (рекоменд. для старта)",
        "max_positions": 5,
        "max_size_pct": 5.0,
        "max_leverage": 5,
        "daily_loss_limit_pct": -10.0,
        "max_drawdown_pct": -20.0,
        "min_interval_minutes": 3,
        "max_position_usd": 500,
        "min_balance_usd": 50,
    },
    # Для уверенных — те же риски что paper, на real-деньгах.
    # Только когда уже 50+ закрытых сделок на testnet/paper показали +EV.
    "real_aggressive": {
        "label": "🔴 Real Aggressive (после 50+ сделок!)",
        "max_positions": 7,
        "max_size_pct": 10.0,
        "max_leverage": 7,
        "daily_loss_limit_pct": -15.0,
        "max_drawdown_pct": -25.0,
        "min_interval_minutes": 1,
        "max_position_usd": 2000,
        "min_balance_usd": 100,
    },
}

# Whitelist — только ликвидные перпетуалы на Binance Futures
# Можно расширить через env LIVE_WHITELIST
DEFAULT_WHITELIST = {
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "AVAXUSDT", "DOGEUSDT", "DOTUSDT", "LINKUSDT",
    "MATICUSDT", "UNIUSDT", "ATOMUSDT", "LTCUSDT", "NEARUSDT",
    "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT", "FILUSDT",
    "SUIUSDT", "SEIUSDT", "TIAUSDT", "PEPEUSDT", "WIFUSDT",
    "ORDIUSDT", "RUNEUSDT", "LDOUSDT", "AAVEUSDT", "FETUSDT",
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def get_state() -> dict:
    """Текущее состояние live trading. Создаёт дефолтное если нет."""
    from database import _live_state
    state = _live_state().find_one({"_id": "state"})
    if state is None:
        state = _init_state()
    return state


def _init_state() -> dict:
    """Инициализация при первом запуске."""
    from database import _live_state
    doc = {
        "_id": "state",
        "mode": "paper",                  # paper | testnet | real
        "kill_switch": False,
        "safety_preset": "conservative",
        "balance_testnet": 1000.0,
        "balance_real": 0.0,               # надо установить через UI перед real
        "equity_peak": None,               # обновляется по факту
        "daily_start_balance": None,       # balance на начало UTC-дня
        "daily_reset_at": None,
        "last_trade_at": None,
        "enabled": False,                  # на старте выключено
        "confirmation_required": True,     # подтверждение в Telegram
        "created_at": _utcnow(),
        "updated_at": _utcnow(),
    }
    _live_state().insert_one(doc)
    logger.info("[live-safety] state initialized (paper mode, preset=conservative)")
    return doc


def set_mode(mode: str) -> dict:
    """Переключить режим: paper | testnet | real. Возвращает обновлённый state."""
    if mode not in ("paper", "testnet", "real"):
        raise ValueError(f"invalid mode: {mode}")
    from database import _live_state
    _live_state().update_one(
        {"_id": "state"},
        {"$set": {"mode": mode, "updated_at": _utcnow()}},
        upsert=True,
    )
    logger.warning(f"[live-safety] MODE CHANGED → {mode}")
    return get_state()


def set_preset(preset: str) -> dict:
    """Переключить safety preset."""
    if preset not in SAFETY_PRESETS:
        raise ValueError(f"unknown preset: {preset}")
    from database import _live_state
    _live_state().update_one(
        {"_id": "state"},
        {"$set": {"safety_preset": preset, "updated_at": _utcnow()}},
        upsert=True,
    )
    logger.info(f"[live-safety] preset → {preset}")
    return get_state()


def set_balance(env: str, amount: float) -> dict:
    """Установить баланс для testnet или real. env: 'testnet' | 'real'."""
    if env not in ("testnet", "real"):
        raise ValueError("env must be 'testnet' or 'real'")
    if amount < 10 or amount > 10_000_000:
        raise ValueError("amount out of range [10, 10M]")
    field = "balance_testnet" if env == "testnet" else "balance_real"
    from database import _live_state
    _live_state().update_one(
        {"_id": "state"},
        {"$set": {field: float(amount), "updated_at": _utcnow(),
                  "daily_start_balance": float(amount),
                  "daily_reset_at": _utcnow(),
                  "equity_peak": float(amount)}},
        upsert=True,
    )
    logger.info(f"[live-safety] {env} balance set to ${amount}")
    return get_state()


def set_enabled(enabled: bool) -> dict:
    """Включить/выключить автоторговлю (не путать с kill switch)."""
    from database import _live_state
    _live_state().update_one(
        {"_id": "state"},
        {"$set": {"enabled": bool(enabled), "updated_at": _utcnow()}},
        upsert=True,
    )
    logger.warning(f"[live-safety] enabled → {enabled}")
    return get_state()


def activate_kill_switch(reason: str = "manual") -> dict:
    """Kill switch — блокирует все новые сделки."""
    from database import _live_state
    _live_state().update_one(
        {"_id": "state"},
        {"$set": {"kill_switch": True, "kill_reason": reason,
                  "kill_at": _utcnow(), "updated_at": _utcnow()}},
        upsert=True,
    )
    logger.error(f"🚨 [live-safety] KILL SWITCH ACTIVATED: {reason}")
    return get_state()


def reset_kill_switch() -> dict:
    """Снять kill switch вручную (только после анализа причины)."""
    from database import _live_state
    _live_state().update_one(
        {"_id": "state"},
        {"$set": {"kill_switch": False, "kill_reason": None,
                  "updated_at": _utcnow()}},
        upsert=True,
    )
    logger.warning("[live-safety] kill switch reset")
    return get_state()


def get_preset_config() -> dict:
    """Конфиг safety preset — порты limits по текущему выбору."""
    state = get_state()
    preset_name = state.get("safety_preset", "conservative")
    return SAFETY_PRESETS.get(preset_name, SAFETY_PRESETS["conservative"])


def get_current_balance() -> float:
    """Текущий баланс для активного mode."""
    state = get_state()
    mode = state.get("mode", "paper")
    if mode == "testnet":
        return float(state.get("balance_testnet", 0.0))
    elif mode == "real":
        return float(state.get("balance_real", 0.0))
    return 0.0


def check_daily_reset(current_balance: float) -> None:
    """Проверяет нужно ли сбросить daily counters (новый UTC день)."""
    state = get_state()
    last_reset = state.get("daily_reset_at")
    now = _utcnow()
    if (last_reset is None or
            last_reset.date() < now.date()):
        from database import _live_state
        _live_state().update_one(
            {"_id": "state"},
            {"$set": {
                "daily_start_balance": current_balance,
                "daily_reset_at": now,
                "updated_at": now,
            }},
        )
        logger.info(f"[live-safety] daily reset — new start balance ${current_balance}")


def update_equity_peak(current_equity: float) -> None:
    """Обновляет исторический максимум equity (для drawdown)."""
    state = get_state()
    peak = state.get("equity_peak") or current_equity
    if current_equity > peak:
        from database import _live_state
        _live_state().update_one(
            {"_id": "state"},
            {"$set": {"equity_peak": current_equity, "updated_at": _utcnow()}},
        )


def can_open_position(symbol: str, size_usd: float) -> tuple[bool, str]:
    """Главная проверка перед открытием реальной позиции.
    Возвращает (allowed, reason). reason='' если можно."""
    state = get_state()

    # Live trading отключён
    if not state.get("enabled", False):
        return False, "live trading disabled"

    mode = state.get("mode", "paper")
    if mode == "paper":
        return False, "mode=paper, не должен идти через live safety"

    # Kill switch активен
    if state.get("kill_switch", False):
        return False, f"kill switch active: {state.get('kill_reason', '?')}"

    preset = get_preset_config()

    # Whitelist пар
    if symbol.upper() not in DEFAULT_WHITELIST:
        return False, f"{symbol} not in whitelist ({len(DEFAULT_WHITELIST)} allowed pairs)"

    # Min balance
    balance = get_current_balance()
    if balance < preset["min_balance_usd"]:
        return False, f"balance ${balance} < min ${preset['min_balance_usd']}"

    # Max position USD
    if size_usd > preset["max_position_usd"]:
        return False, f"size ${size_usd} > max ${preset['max_position_usd']} ({state.get('safety_preset')})"

    # Position count cap
    from database import _live_trades
    open_count = _live_trades().count_documents({
        "status": "OPEN",
        "env": mode,
    })
    if open_count >= preset["max_positions"]:
        return False, f"already {open_count}/{preset['max_positions']} positions open"

    # Min interval между сделками
    last_trade = state.get("last_trade_at")
    if last_trade:
        elapsed_min = (_utcnow() - last_trade).total_seconds() / 60
        if elapsed_min < preset["min_interval_minutes"]:
            return False, f"min interval {preset['min_interval_minutes']}min, прошло {elapsed_min:.1f}min"

    # Daily loss limit
    check_daily_reset(balance)
    state = get_state()  # refresh after reset
    daily_start = state.get("daily_start_balance") or balance
    if daily_start > 0:
        daily_pnl_pct = (balance - daily_start) / daily_start * 100
        if daily_pnl_pct <= preset["daily_loss_limit_pct"]:
            activate_kill_switch(f"daily loss {daily_pnl_pct:.1f}% ≤ limit {preset['daily_loss_limit_pct']}%")
            return False, f"daily loss {daily_pnl_pct:.1f}% hit limit → kill switch активирован"

    # Max drawdown
    update_equity_peak(balance)
    state = get_state()
    peak = state.get("equity_peak") or balance
    if peak > 0:
        dd_pct = (balance - peak) / peak * 100
        if dd_pct <= preset["max_drawdown_pct"]:
            activate_kill_switch(f"drawdown {dd_pct:.1f}% ≤ limit {preset['max_drawdown_pct']}%")
            return False, f"max drawdown {dd_pct:.1f}% hit limit → kill switch"

    return True, ""


def record_trade_opened() -> None:
    """Обновляет last_trade_at после открытия."""
    from database import _live_state
    _live_state().update_one(
        {"_id": "state"},
        {"$set": {"last_trade_at": _utcnow(), "updated_at": _utcnow()}},
    )


def get_status_summary() -> dict:
    """Полная сводка для UI (badges вверху вкладки)."""
    state = get_state()
    preset = get_preset_config()
    balance = get_current_balance()
    daily_start = state.get("daily_start_balance") or balance
    daily_pnl_pct = ((balance - daily_start) / daily_start * 100) if daily_start > 0 else 0
    peak = state.get("equity_peak") or balance
    dd_pct = ((balance - peak) / peak * 100) if peak > 0 else 0

    from database import _live_trades
    mode = state.get("mode", "paper")
    open_count = 0
    if mode in ("testnet", "real"):
        open_count = _live_trades().count_documents({"status": "OPEN", "env": mode})

    return {
        "mode": mode,
        "enabled": state.get("enabled", False),
        "kill_switch": state.get("kill_switch", False),
        "kill_reason": state.get("kill_reason"),
        "safety_preset": state.get("safety_preset", "conservative"),
        "preset_config": preset,
        "balance": balance,
        "balance_testnet": state.get("balance_testnet", 0),
        "balance_real": state.get("balance_real", 0),
        "equity_peak": peak,
        "daily_start_balance": daily_start,
        "daily_pnl_pct": round(daily_pnl_pct, 2),
        "drawdown_pct": round(dd_pct, 2),
        "open_positions": open_count,
        "max_positions": preset["max_positions"],
        "confirmation_required": state.get("confirmation_required", True),
        "last_trade_at": state.get("last_trade_at").isoformat() if state.get("last_trade_at") else None,
        "whitelist_size": len(DEFAULT_WHITELIST),
    }


# ════════════════════════════════════════════════════════════════
# Multi-account API (для семьи/нескольких ключей одновременно)
# ════════════════════════════════════════════════════════════════
# Старая модель: один глобальный live_state с одним mode и парой ключей из env.
# Новая модель: коллекция live_accounts — N независимых аккаунтов, каждый со
# своими ключами, режимом, пресетом и kill switch. Watcher обходит все
# enabled аккаунты при сигнале и открывает позицию в каждом.
# Старая live_state остаётся для UI summary "global" (paper).

def list_accounts() -> list[dict]:
    """Все аккаунты (включая disabled). Без api_secret в ответе."""
    from database import _live_accounts
    rows = list(_live_accounts().find({}))
    for r in rows:
        # Маскируем ключи в ответе
        if r.get("api_key"):
            k = r["api_key"]
            r["api_key_masked"] = (k[:6] + "…" + k[-4:]) if len(k) > 12 else "•••"
            r.pop("api_key", None)
        r.pop("api_secret", None)
    return rows


def get_account(account_id: str) -> dict | None:
    """Полный документ аккаунта (с ключами — для внутреннего использования)."""
    from database import _live_accounts
    return _live_accounts().find_one({"_id": account_id})


def get_enabled_accounts() -> list[dict]:
    """Все enabled аккаунты — для watcher iteration по сигналу."""
    from database import _live_accounts
    return list(_live_accounts().find({
        "enabled": True,
        "kill_switch": {"$ne": True},
    }))


def add_account(payload: dict) -> dict:
    """Создать новый аккаунт. Обязательные: id, owner, mode, api_key, api_secret.
    mode: 'testnet' или 'real'. preset по умолчанию paper_mirror."""
    from database import _live_accounts
    aid = (payload.get("id") or payload.get("_id") or "").strip().lower()
    if not aid or not aid.replace("_", "").replace("-", "").isalnum():
        return {"ok": False, "error": "id required (alphanum/_/-)"}
    if _live_accounts().find_one({"_id": aid}):
        return {"ok": False, "error": f"account '{aid}' уже существует"}
    mode = payload.get("mode", "real")
    if mode not in ("testnet", "real"):
        return {"ok": False, "error": "mode must be testnet or real"}
    exchange = (payload.get("exchange") or "bingx").lower()
    if exchange not in ("binance", "bingx"):
        return {"ok": False, "error": "exchange must be 'binance' or 'bingx'"}
    api_key = payload.get("api_key", "").strip()
    api_secret = payload.get("api_secret", "").strip()
    if not api_key or not api_secret:
        return {"ok": False, "error": "api_key и api_secret обязательны"}
    preset = payload.get("safety_preset", "paper_mirror")
    if preset not in SAFETY_PRESETS:
        return {"ok": False, "error": f"unknown preset: {preset}"}
    doc = {
        "_id": aid,
        "owner": payload.get("owner", aid),
        "label": payload.get("label", aid),
        "exchange": exchange,        # 'binance' или 'bingx'
        "mode": mode,
        "enabled": False,        # включается отдельно после test-connection
        "kill_switch": False,
        "api_key": api_key,
        "api_secret": api_secret,
        "safety_preset": preset,
        "balance": 0.0,
        "equity_peak": None,
        "daily_start_balance": None,
        "daily_reset_at": None,
        "confirmation_required": bool(payload.get("confirmation_required", False)),
        "last_trade_at": None,
        "created_at": _utcnow(),
        "updated_at": _utcnow(),
    }
    _live_accounts().insert_one(doc)
    logger.info(f"[live-accounts] created '{aid}' mode={mode} preset={preset}")
    return {"ok": True, "id": aid}


def update_account(account_id: str, update: dict) -> dict:
    """Обновить поля аккаунта. Поддерживает: enabled, kill_switch, mode, owner,
    label, safety_preset, confirmation_required, api_key, api_secret."""
    from database import _live_accounts
    allowed = {"enabled", "kill_switch", "mode", "owner", "label",
               "safety_preset", "confirmation_required", "api_key", "api_secret",
               "balance", "exchange"}
    upd = {k: v for k, v in update.items() if k in allowed}
    if "safety_preset" in upd and upd["safety_preset"] not in SAFETY_PRESETS:
        return {"ok": False, "error": f"unknown preset: {upd['safety_preset']}"}
    if "mode" in upd and upd["mode"] not in ("testnet", "real"):
        return {"ok": False, "error": "mode must be testnet or real"}
    upd["updated_at"] = _utcnow()
    res = _live_accounts().update_one({"_id": account_id}, {"$set": upd})
    if res.matched_count == 0:
        return {"ok": False, "error": f"account '{account_id}' не найден"}
    logger.info(f"[live-accounts] updated '{account_id}' fields={list(upd.keys())}")
    return {"ok": True}


def delete_account(account_id: str) -> dict:
    """Полное удаление аккаунта. Открытые позиции не закрываются — это надо сделать вручную."""
    from database import _live_accounts, _live_trades
    open_count = _live_trades().count_documents({"account_id": account_id, "status": "OPEN"})
    if open_count > 0:
        return {"ok": False, "error": f"есть {open_count} открытых позиций — закрой сначала"}
    res = _live_accounts().delete_one({"_id": account_id})
    if res.deleted_count == 0:
        return {"ok": False, "error": f"account '{account_id}' не найден"}
    logger.info(f"[live-accounts] deleted '{account_id}'")
    return {"ok": True}


def can_open_position_for_account(account: dict, symbol: str, size_usd: float) -> tuple[bool, str]:
    """Per-account safety check (вместо глобального can_open_position)."""
    if not account.get("enabled"):
        return False, "account disabled"
    if account.get("kill_switch"):
        return False, f"kill switch active: {account.get('kill_reason', '?')}"
    preset_name = account.get("safety_preset", "paper_mirror")
    preset = SAFETY_PRESETS.get(preset_name, SAFETY_PRESETS["paper_mirror"])

    # Whitelist убран — paper уже фильтрует по exchange_symbols (актуальный
    # список 599 BingX / 566 Binance пар). Двойная защита не нужна.
    # Если нужно вернуть строгий whitelist — раскомментировать ниже:
    # mode = account.get("mode", "testnet")
    # if mode == "real" and symbol.upper() not in DEFAULT_WHITELIST:
    #     return False, f"{symbol} not in whitelist (real-mode)"

    balance = account.get("balance", 0.0) or 0.0
    if balance < preset["min_balance_usd"]:
        return False, f"balance ${balance} < min ${preset['min_balance_usd']}"

    if size_usd > preset["max_position_usd"]:
        return False, f"size ${size_usd} > max ${preset['max_position_usd']} ({preset_name})"

    from database import _live_trades
    open_count = _live_trades().count_documents({
        "status": "OPEN", "account_id": account["_id"],
    })
    if open_count >= preset["max_positions"]:
        return False, f"уже {open_count}/{preset['max_positions']} открытых позиций"

    last_trade = account.get("last_trade_at")
    if last_trade and preset["min_interval_minutes"] > 0:
        elapsed_min = (_utcnow() - last_trade).total_seconds() / 60
        if elapsed_min < preset["min_interval_minutes"]:
            return False, f"min interval {preset['min_interval_minutes']}min, прошло {elapsed_min:.1f}min"

    daily_start = account.get("daily_start_balance") or balance
    if daily_start > 0:
        daily_pnl_pct = (balance - daily_start) / daily_start * 100
        if daily_pnl_pct <= preset["daily_loss_limit_pct"]:
            from database import _live_accounts
            _live_accounts().update_one(
                {"_id": account["_id"]},
                {"$set": {"kill_switch": True, "kill_reason": f"daily loss {daily_pnl_pct:.1f}%",
                          "kill_at": _utcnow()}},
            )
            return False, f"daily loss {daily_pnl_pct:.1f}% → kill switch"

    peak = account.get("equity_peak") or balance
    if peak > 0:
        dd_pct = (balance - peak) / peak * 100
        if dd_pct <= preset["max_drawdown_pct"]:
            from database import _live_accounts
            _live_accounts().update_one(
                {"_id": account["_id"]},
                {"$set": {"kill_switch": True, "kill_reason": f"drawdown {dd_pct:.1f}%",
                          "kill_at": _utcnow()}},
            )
            return False, f"drawdown {dd_pct:.1f}% → kill switch"

    return True, ""


def record_account_trade_opened(account_id: str) -> None:
    from database import _live_accounts
    _live_accounts().update_one(
        {"_id": account_id},
        {"$set": {"last_trade_at": _utcnow(), "updated_at": _utcnow()}},
    )
