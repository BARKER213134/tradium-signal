"""Live Trader — реальные ордера на Binance Futures через ccxt.

Режимы:
  testnet — Binance testnet (игровые деньги, но реальная API)
  real    — Binance mainnet (реальные деньги!)

Архитектура зеркалит paper_trader, но вместо MongoDB-симуляции — реальные
ордера на бирже. Открытие/закрытие/TP/SL/breakeven/trailing — всё в реале.

ВСЕ вызовы защищены live_safety.can_open_position() — даже если AI
скажет открыть, safety может заблокировать.

ВАЖНО: если BINANCE_API_KEY не задан — функции возвращают стуб с
error. Это позволяет деплоить UI/эндпоинты до ключей.
"""
from __future__ import annotations
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# Глобальный ccxt-клиент (ленивая инициализация)
_exchange_testnet = None
_exchange_real = None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _get_exchange(env: str):
    """Возвращает ccxt.binance instance для testnet или real.
    Если ключи не заданы — None."""
    global _exchange_testnet, _exchange_real
    from config import BINANCE_API_KEY, BINANCE_API_SECRET
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        return None
    try:
        import ccxt
    except ImportError:
        logger.error("[live-trader] ccxt library not installed — pip install ccxt")
        return None

    if env == "testnet":
        if _exchange_testnet is None:
            _exchange_testnet = ccxt.binance({
                "apiKey": BINANCE_API_KEY,
                "secret": BINANCE_API_SECRET,
                "options": {"defaultType": "future"},
                "enableRateLimit": True,
            })
            _exchange_testnet.set_sandbox_mode(True)
            logger.info("[live-trader] binance testnet client initialized")
        return _exchange_testnet

    if env == "real":
        if _exchange_real is None:
            _exchange_real = ccxt.binance({
                "apiKey": BINANCE_API_KEY,
                "secret": BINANCE_API_SECRET,
                "options": {"defaultType": "future"},
                "enableRateLimit": True,
            })
            logger.warning("[live-trader] binance REAL client initialized ⚠️")
        return _exchange_real

    return None


def test_connection(env: str = "testnet") -> dict:
    """Проверка API — вернёт баланс если OK или error."""
    ex = _get_exchange(env)
    if ex is None:
        return {"ok": False, "error": "BINANCE_API_KEY/SECRET not set or ccxt missing"}
    try:
        balance = ex.fetch_balance()
        usdt = balance.get("USDT", {}).get("free", 0) + balance.get("USDT", {}).get("used", 0)
        return {
            "ok": True,
            "env": env,
            "usdt_total": round(usdt, 2),
            "usdt_free": balance.get("USDT", {}).get("free", 0),
            "usdt_used": balance.get("USDT", {}).get("used", 0),
        }
    except Exception as e:
        logger.error(f"[live-trader] connection test fail ({env}): {e}")
        return {"ok": False, "error": str(e), "env": env}


async def open_position(signal_data: dict, decision: dict, env: str) -> Optional[dict]:
    """Открывает реальную позицию на бирже.
      signal_data — как в paper_trader.on_signal
      decision — {leverage, size_pct, tp1, sl, reasoning}
      env — 'testnet' или 'real'
    Возвращает dict с записью в live_trades или None при неудаче."""
    from database import _live_trades
    from live_safety import (
        can_open_position, get_current_balance, record_trade_opened,
    )

    symbol = signal_data.get("symbol", "")
    direction = signal_data.get("direction", "LONG")
    entry_price = signal_data.get("entry") or signal_data.get("price")
    size_pct = float(decision.get("size_pct", 3))
    leverage = int(decision.get("leverage", 2))
    tp1 = decision.get("tp1") or signal_data.get("tp1")
    sl = decision.get("sl") or signal_data.get("sl")

    if not symbol or not entry_price:
        return {"ok": False, "error": "missing symbol or entry"}

    balance = get_current_balance()
    size_usdt = round(balance * size_pct / 100, 2)

    # Safety check
    allowed, reason = can_open_position(symbol, size_usdt)
    if not allowed:
        logger.warning(f"[live-trader] SAFETY BLOCK: {symbol} — {reason}")
        return {"ok": False, "error": f"safety block: {reason}"}

    ex = _get_exchange(env)
    if ex is None:
        return {"ok": False, "error": "API not configured"}

    side = "buy" if direction == "LONG" else "sell"
    # Размер в базовой валюте (контрактах) = size_usdt * leverage / price
    notional = size_usdt * leverage
    amount = round(notional / entry_price, 6)

    try:
        # 1. Установить leverage
        await asyncio.to_thread(ex.set_leverage, leverage, symbol)

        # 2. Рыночный ордер на вход
        order = await asyncio.to_thread(
            ex.create_market_order,
            symbol, side, amount,
        )
        exchange_order_id = order.get("id")
        fill_price = order.get("average") or entry_price

        # 3. Ставим TP (take profit) как reduce-only limit
        tp_order_id = None
        if tp1:
            tp_side = "sell" if direction == "LONG" else "buy"
            try:
                tp_order = await asyncio.to_thread(
                    ex.create_order,
                    symbol, "TAKE_PROFIT_MARKET", tp_side, amount,
                    None,
                    {"stopPrice": float(tp1), "reduceOnly": True},
                )
                tp_order_id = tp_order.get("id")
            except Exception as e:
                logger.warning(f"[live-trader] TP order fail for {symbol}: {e}")

        # 4. Ставим SL
        sl_order_id = None
        if sl:
            sl_side = "sell" if direction == "LONG" else "buy"
            try:
                sl_order = await asyncio.to_thread(
                    ex.create_order,
                    symbol, "STOP_MARKET", sl_side, amount,
                    None,
                    {"stopPrice": float(sl), "reduceOnly": True},
                )
                sl_order_id = sl_order.get("id")
            except Exception as e:
                logger.warning(f"[live-trader] SL order fail for {symbol}: {e}")

        # 5. Next trade_id
        from database import _get_db
        counter = _get_db().counters.find_one_and_update(
            {"_id": "live_trades"},
            {"$inc": {"seq": 1}},
            upsert=True, return_document=True,
        )
        trade_id = (counter or {}).get("seq", 1)

        doc = {
            "trade_id": trade_id,
            "env": env,
            "symbol": symbol,
            "pair": symbol.replace("USDT", "/USDT"),
            "direction": direction,
            "entry": fill_price,
            "tp1": tp1,
            "sl": sl,
            "original_sl": sl,
            "leverage": leverage,
            "size_usdt": size_usdt,
            "size_pct": size_pct,
            "amount": amount,
            "status": "OPEN",
            "source": signal_data.get("source", "unknown"),
            "exchange_order_id": exchange_order_id,
            "tp_order_id": tp_order_id,
            "sl_order_id": sl_order_id,
            "ai_reasoning": decision.get("reasoning", ""),
            "max_favorable_pct": 0.0,
            "sl_moved_to_be": False,
            "sl_trailing": False,
            "exit_events": [],
            "opened_at": _utcnow(),
            "closed_at": None,
            "exit_price": None,
            "pnl_usdt": None,
            "pnl_pct": None,
        }
        _live_trades().insert_one(doc)
        record_trade_opened()
        logger.warning(
            f"🔴 LIVE OPEN [{env}] #{trade_id}: {symbol} {direction} "
            f"×{leverage} ${size_usdt} entry={fill_price} (order={exchange_order_id})"
        )
        return {"ok": True, "trade": doc}

    except Exception as e:
        import traceback
        logger.error(f"[live-trader] OPEN fail {symbol}: {e}\n{traceback.format_exc()[-500:]}")
        return {"ok": False, "error": str(e)}


async def close_position(trade_id: int, reason: str = "MANUAL") -> Optional[dict]:
    """Закрывает реальную позицию по рынку + отменяет TP/SL ордера."""
    from database import _live_trades
    trades = _live_trades()
    pos = trades.find_one({"trade_id": int(trade_id), "status": "OPEN"})
    if not pos:
        return {"ok": False, "error": "position not found or already closed"}

    env = pos.get("env", "testnet")
    ex = _get_exchange(env)
    if ex is None:
        return {"ok": False, "error": "API not configured"}

    symbol = pos["symbol"]
    direction = pos["direction"]
    amount = pos.get("amount", 0)
    close_side = "sell" if direction == "LONG" else "buy"

    try:
        # 1. Отменяем TP и SL ордера
        for order_id_key in ("tp_order_id", "sl_order_id"):
            order_id = pos.get(order_id_key)
            if order_id:
                try:
                    await asyncio.to_thread(ex.cancel_order, order_id, symbol)
                except Exception as e:
                    logger.debug(f"[live-trader] cancel {order_id_key} fail: {e}")

        # 2. Закрываем по рынку
        close_order = await asyncio.to_thread(
            ex.create_market_order, symbol, close_side, amount,
            None, {"reduceOnly": True},
        )
        exit_price = close_order.get("average") or 0

        # 3. Подсчёт PnL
        entry = pos["entry"]
        leverage = pos.get("leverage", 1)
        size_usdt = pos.get("size_usdt", 0)
        raw_pnl_pct = ((exit_price - entry) / entry) * 100
        if direction == "SHORT":
            raw_pnl_pct = -raw_pnl_pct
        pnl_pct = round(raw_pnl_pct * leverage, 2)
        pnl_usdt = round(size_usdt * pnl_pct / 100, 2)

        trades.update_one({"trade_id": int(trade_id)}, {"$set": {
            "status": reason,
            "exit_price": exit_price,
            "pnl_pct": pnl_pct,
            "pnl_usdt": pnl_usdt,
            "closed_at": _utcnow(),
            "close_order_id": close_order.get("id"),
        }})

        # Обновить баланс в live_state
        from live_safety import get_state
        state = get_state()
        field = "balance_testnet" if env == "testnet" else "balance_real"
        new_balance = float(state.get(field, 0)) + pnl_usdt
        from database import _live_state
        _live_state().update_one(
            {"_id": "state"},
            {"$set": {field: round(new_balance, 2), "updated_at": _utcnow()}},
        )

        logger.warning(
            f"🔴 LIVE CLOSE [{env}] #{trade_id}: {symbol} {reason} "
            f"PnL={pnl_pct:+.2f}% ${pnl_usdt:+.2f}"
        )
        return {"ok": True, "trade_id": trade_id, "pnl_pct": pnl_pct, "pnl_usdt": pnl_usdt}

    except Exception as e:
        import traceback
        logger.error(f"[live-trader] CLOSE fail #{trade_id}: {e}\n{traceback.format_exc()[-500:]}")
        return {"ok": False, "error": str(e)}


async def close_all_positions(env: str, reason: str = "KILL_SWITCH") -> list:
    """Закрывает ВСЕ открытые позиции в окружении. Для kill switch."""
    from database import _live_trades
    open_pos = list(_live_trades().find({"status": "OPEN", "env": env}))
    results = []
    for pos in open_pos:
        r = await close_position(pos["trade_id"], reason)
        results.append(r)
    return results


def get_open_positions(env: str) -> list:
    """Открытые позиции в заданном окружении."""
    from database import _live_trades
    return list(_live_trades().find({"status": "OPEN", "env": env}).sort("opened_at", -1))


def get_history(env: str, limit: int = 50) -> list:
    """История закрытых сделок."""
    from database import _live_trades
    return list(_live_trades().find({
        "status": {"$in": ["TP", "SL", "BE", "TRAIL", "MANUAL", "AI_CLOSE", "KILL_SWITCH"]},
        "env": env,
    }).sort("closed_at", -1).limit(limit))


async def on_signal_live(signal_data: dict, env: str) -> Optional[dict]:
    """Главная точка входа для live trading (testnet или real).
    Вызывается из watcher._execute_signal когда mode != 'paper'.
    Проходит через ai_decide + safety + подтверждение Telegram (если включено)."""
    from live_safety import get_state
    state = get_state()

    # Live должен быть enabled
    if not state.get("enabled", False):
        logger.debug(f"[live-trader] {signal_data.get('symbol')} — live disabled, skip")
        return None

    # Kill switch
    if state.get("kill_switch", False):
        logger.warning(f"[live-trader] {signal_data.get('symbol')} — kill switch active")
        return None

    # Spawn AI decision
    import paper_trader as pt
    decision = await pt.ai_decide(signal_data)
    if not decision.get("enter"):
        return None

    # Подтверждение через Telegram если включено
    if state.get("confirmation_required", True):
        # Создаём pending confirmation — исполнение по кнопке из BOT11
        from database import _live_pending_confirmations
        import secrets
        token = secrets.token_urlsafe(16)
        pending = {
            "confirmation_token": token,
            "signal_data": signal_data,
            "decision": decision,
            "env": env,
            "status": "PENDING",
            "created_at": _utcnow(),
        }
        _live_pending_confirmations().insert_one(pending)
        # Отправляем в BOT11 — TODO после того как токен задан
        try:
            from watcher import _send_live_confirmation_alert
            await _send_live_confirmation_alert(pending)
        except Exception as e:
            logger.debug(f"[live-trader] confirmation alert fail: {e}")
        logger.warning(f"[live-trader] {signal_data.get('symbol')} → PENDING confirmation ({token})")
        return {"ok": True, "status": "pending", "token": token}

    # Без подтверждения — открываем сразу
    return await open_position(signal_data, decision, env)


async def execute_confirmed(token: str) -> Optional[dict]:
    """Пользователь подтвердил pending — исполняем."""
    from database import _live_pending_confirmations
    pending = _live_pending_confirmations().find_one({
        "confirmation_token": token, "status": "PENDING",
    })
    if not pending:
        return {"ok": False, "error": "pending not found or expired"}

    result = await open_position(
        pending["signal_data"],
        pending["decision"],
        pending["env"],
    )
    _live_pending_confirmations().update_one(
        {"confirmation_token": token},
        {"$set": {"status": "EXECUTED" if result and result.get("ok") else "FAILED",
                  "executed_at": _utcnow(),
                  "result": {k: v for k, v in (result or {}).items() if k != "trade"}}},
    )
    return result


async def execute_rejected(token: str) -> dict:
    """Пользователь отказался — помечаем."""
    from database import _live_pending_confirmations
    _live_pending_confirmations().update_one(
        {"confirmation_token": token, "status": "PENDING"},
        {"$set": {"status": "REJECTED", "rejected_at": _utcnow()}},
    )
    return {"ok": True, "status": "rejected"}


async def sync_positions(env: str) -> dict:
    """Синхронизация с биржей — обновляет статусы закрытых TP/SL ордеров.
    Вызывается периодически из watcher (каждые 30с)."""
    from database import _live_trades
    ex = _get_exchange(env)
    if ex is None:
        return {"ok": False, "error": "API not configured"}

    synced = 0
    closed = 0
    try:
        positions = ex.fetch_positions()
        open_symbols = {p["symbol"] for p in positions if float(p.get("contracts", 0)) != 0}
    except Exception as e:
        return {"ok": False, "error": f"fetch_positions fail: {e}"}

    # Все OPEN в БД, но не в open_symbols на бирже → закрылись через TP/SL
    db_open = list(_live_trades().find({"status": "OPEN", "env": env}))
    for pos in db_open:
        if pos["symbol"] not in open_symbols:
            # Позиция закрылась на бирже — подтягиваем историю
            try:
                # Пытаемся вытащить последний trade
                trades_list = await asyncio.to_thread(ex.fetch_my_trades, pos["symbol"], None, 5)
                if trades_list:
                    last = trades_list[-1]
                    exit_price = last.get("price", pos["entry"])
                    entry = pos["entry"]
                    direction = pos["direction"]
                    leverage = pos.get("leverage", 1)
                    size_usdt = pos.get("size_usdt", 0)
                    raw = ((exit_price - entry) / entry) * 100
                    if direction == "SHORT": raw = -raw
                    pnl_pct = round(raw * leverage, 2)
                    pnl_usdt = round(size_usdt * pnl_pct / 100, 2)
                    reason = "TP" if pnl_usdt > 0 else "SL"
                    _live_trades().update_one(
                        {"trade_id": pos["trade_id"]},
                        {"$set": {"status": reason, "exit_price": exit_price,
                                  "pnl_pct": pnl_pct, "pnl_usdt": pnl_usdt,
                                  "closed_at": _utcnow(),
                                  "sync_detected": True}},
                    )
                    closed += 1
                    logger.warning(f"[live-trader] sync detected close #{pos['trade_id']}: {reason} {pnl_pct}%")
            except Exception as e:
                logger.debug(f"[live-trader] sync trade fetch fail: {e}")
        synced += 1
    return {"ok": True, "synced": synced, "auto_closed": closed}


# ════════════════════════════════════════════════════════════════
# Multi-account API (per-account exchange + open + signal)
# ════════════════════════════════════════════════════════════════

# Cache ccxt instances per account: account_id → exchange
_exchange_per_account: dict[str, object] = {}


def _get_exchange_for_account(account: dict):
    """Возвращает ccxt.binance instance для конкретного аккаунта.
    Каждый аккаунт = свои ключи + свой sandbox режим."""
    aid = account["_id"]
    if aid in _exchange_per_account:
        return _exchange_per_account[aid]
    api_key = account.get("api_key")
    api_secret = account.get("api_secret")
    if not api_key or not api_secret:
        return None
    try:
        import ccxt
    except ImportError:
        logger.error("[live-trader] ccxt not installed")
        return None
    ex = ccxt.binance({
        "apiKey": api_key,
        "secret": api_secret,
        "options": {"defaultType": "future"},
        "enableRateLimit": True,
    })
    if account.get("mode") == "testnet":
        ex.set_sandbox_mode(True)
    _exchange_per_account[aid] = ex
    logger.info(f"[live-trader] account '{aid}' ccxt initialized (mode={account.get('mode')})")
    return ex


def test_connection_for_account(account: dict) -> dict:
    """Проверка соединения для конкретного аккаунта."""
    ex = _get_exchange_for_account(account)
    if ex is None:
        return {"ok": False, "error": "ccxt not configured or keys missing"}
    try:
        balance = ex.fetch_balance()
        usdt_free = balance.get("USDT", {}).get("free", 0)
        usdt_used = balance.get("USDT", {}).get("used", 0)
        usdt_total = usdt_free + usdt_used
        return {
            "ok": True,
            "account_id": account["_id"],
            "mode": account.get("mode"),
            "usdt_total": round(usdt_total, 2),
            "usdt_free": usdt_free,
            "usdt_used": usdt_used,
        }
    except Exception as e:
        return {"ok": False, "account_id": account["_id"], "error": str(e)}


async def open_position_for_account(signal_data: dict, decision: dict, account: dict) -> Optional[dict]:
    """Открыть позицию для конкретного аккаунта."""
    from database import _live_trades
    from live_safety import can_open_position_for_account, record_account_trade_opened

    aid = account["_id"]
    symbol = signal_data.get("symbol", "")
    direction = signal_data.get("direction", "LONG")
    entry_price = signal_data.get("entry") or signal_data.get("price")
    size_pct = float(decision.get("size_pct", 3))
    leverage = int(decision.get("leverage", 2))
    tp1 = decision.get("tp1") or signal_data.get("tp1")
    sl = decision.get("sl") or signal_data.get("sl")

    if not symbol or not entry_price:
        return {"ok": False, "error": "missing symbol or entry"}

    balance = account.get("balance", 0) or 0
    size_usdt = round(balance * size_pct / 100, 2)

    allowed, reason = can_open_position_for_account(account, symbol, size_usdt)
    if not allowed:
        logger.warning(f"[live-{aid}] SAFETY BLOCK: {symbol} — {reason}")
        return {"ok": False, "error": f"safety: {reason}"}

    ex = _get_exchange_for_account(account)
    if ex is None:
        return {"ok": False, "error": "exchange not configured"}

    # ── Fix 2: Pre-check символа на бирже (без этого -1121 для не-листенных альтов) ──
    try:
        if not getattr(ex, "markets", None):
            await asyncio.to_thread(ex.load_markets)
    except Exception as _le:
        logger.warning(f"[live-{aid}] load_markets fail: {_le}")

    if ex.markets and symbol not in ex.markets:
        # Пробуем форматы XXX/USDT и XXXUSDT
        slash_form = symbol.replace("USDT", "/USDT") if not symbol.endswith("/USDT") else symbol
        if slash_form in ex.markets:
            symbol = slash_form
        else:
            return {"ok": False,
                    "error": f"symbol {symbol} not listed on {account.get('mode','testnet')} (skip: not-supported)"}

    # ── Fix 1: Динамический клампинг по доступной марже на бирже ──
    try:
        bal_data = await asyncio.to_thread(ex.fetch_balance)
        usdt_free = float((bal_data.get("USDT") or {}).get("free", 0) or 0)
    except Exception as _be:
        logger.debug(f"[live-{aid}] fetch_balance fail: {_be}")
        usdt_free = 0.0

    # На futures: required_margin ≈ size_usdt (то что paper считает margin = size_usdt)
    # Используем максимум 50% свободной маржи на одну позицию (буфер на slippage/fees)
    required_margin = size_usdt
    if usdt_free > 0 and required_margin > usdt_free * 0.5:
        new_size = round(usdt_free * 0.5, 2)
        if new_size < 5.0:
            return {"ok": False,
                    "error": f"insufficient margin: free=${usdt_free:.2f}, need=${required_margin:.2f}"}
        logger.warning(
            f"[live-{aid}] downscaled {symbol} size: ${size_usdt} → ${new_size} "
            f"(free=${usdt_free:.2f}, half-rule)"
        )
        size_usdt = new_size

    side = "buy" if direction == "LONG" else "sell"
    notional = size_usdt * leverage
    amount = notional / entry_price

    # ── Fix 3: Клампинг amount по exchange limits ──
    mkt = ex.markets.get(symbol) if ex.markets else None
    if mkt:
        limits = mkt.get("limits") or {}
        amt_min = (limits.get("amount") or {}).get("min")
        amt_max = (limits.get("amount") or {}).get("max")
        cost_min = (limits.get("cost") or {}).get("min")  # min notional
        if amt_max and amount > amt_max:
            logger.warning(f"[live-{aid}] {symbol} amount {amount:.6f} > max {amt_max}, clamping")
            amount = amt_max
            # Пересчёт size_usdt чтобы соответствовать клампленному amount
            size_usdt = round(amount * entry_price / leverage, 2)
        if amt_min and amount < amt_min:
            return {"ok": False,
                    "error": f"amount {amount:.6f} < min {amt_min} for {symbol} (size too small)"}
        if cost_min and (amount * entry_price) < cost_min:
            return {"ok": False,
                    "error": f"notional ${amount * entry_price:.2f} < min ${cost_min} for {symbol}"}

    # Точная precision через ccxt
    try:
        amount = float(await asyncio.to_thread(ex.amount_to_precision, symbol, amount))
    except Exception as _pe:
        logger.debug(f"[live-{aid}] amount_to_precision fail {symbol}: {_pe}")
        amount = round(amount, 6)

    try:
        await asyncio.to_thread(ex.set_leverage, leverage, symbol)
        order = await asyncio.to_thread(ex.create_market_order, symbol, side, amount)
        exchange_order_id = order.get("id")
        fill_price = order.get("average") or entry_price

        # Небольшая пауза чтобы позиция полностью осела на бирже
        await asyncio.sleep(0.5)

        tp_order_id = sl_order_id = None
        tp_error = sl_error = None
        tp_side = "sell" if direction == "LONG" else "buy"
        sl_side = tp_side  # same direction for reduce-only

        # Округление amount по precision биржи через ccxt — markets загружены при первом ордере
        try:
            amount_tp_sl = float(await asyncio.to_thread(ex.amount_to_precision, symbol, amount))
        except Exception as _pe:
            logger.debug(f"[live-{aid}] amount_to_precision fail {symbol}: {_pe}")
            amount_tp_sl = amount

        if tp1:
            try:
                # Минимальные params: stopPrice + reduceOnly. workingType triggered -4120.
                tp_params = {
                    "stopPrice": float(tp1),
                    "reduceOnly": True,
                }
                tp_order = await asyncio.to_thread(
                    ex.create_order, symbol, "TAKE_PROFIT_MARKET", tp_side,
                    amount_tp_sl, None, tp_params,
                )
                tp_order_id = tp_order.get("id")
                logger.info(f"[live-{aid}] TP placed {symbol} stopPrice={tp1} id={tp_order_id}")
            except Exception as e:
                import traceback as _tb
                tp_error = f"{type(e).__name__}: {str(e)[:300]}"
                logger.warning(f"[live-{aid}] TP fail {symbol}: {e}\n{_tb.format_exc()[-600:]}")

        if sl:
            try:
                sl_params = {
                    "stopPrice": float(sl),
                    "reduceOnly": True,
                }
                sl_order = await asyncio.to_thread(
                    ex.create_order, symbol, "STOP_MARKET", sl_side,
                    amount_tp_sl, None, sl_params,
                )
                sl_order_id = sl_order.get("id")
                logger.info(f"[live-{aid}] SL placed {symbol} stopPrice={sl} id={sl_order_id}")
            except Exception as e:
                import traceback as _tb
                sl_error = f"{type(e).__name__}: {str(e)[:300]}"
                logger.warning(f"[live-{aid}] SL fail {symbol}: {e}\n{_tb.format_exc()[-600:]}")

        from database import _get_db
        counter = _get_db().counters.find_one_and_update(
            {"_id": "live_trades"}, {"$inc": {"seq": 1}},
            upsert=True, return_document=True,
        )
        trade_id = (counter or {}).get("seq", 1)

        doc = {
            "trade_id": trade_id,
            "account_id": aid,
            "env": account.get("mode", "testnet"),
            "symbol": symbol,
            "pair": symbol.replace("USDT", "/USDT"),
            "direction": direction,
            "entry": fill_price,
            "tp1": tp1, "sl": sl, "original_sl": sl,
            "leverage": leverage, "size_usdt": size_usdt, "size_pct": size_pct,
            "amount": amount, "status": "OPEN",
            "source": signal_data.get("source", "unknown"),
            # paper_trade_id — для cross-reference в UI (показать "open в paper и testnet")
            "paper_trade_id": signal_data.get("paper_trade_id"),
            "exchange_order_id": exchange_order_id,
            "tp_order_id": tp_order_id, "sl_order_id": sl_order_id,
            "tp_error": tp_error, "sl_error": sl_error,
            "ai_reasoning": decision.get("reasoning", ""),
            "max_favorable_pct": 0.0, "sl_moved_to_be": False, "sl_trailing": False,
            "exit_events": [],
            "opened_at": _utcnow(), "closed_at": None,
            "exit_price": None, "pnl_usdt": None, "pnl_pct": None,
        }
        _live_trades().insert_one(doc)
        record_account_trade_opened(aid)
        logger.warning(
            f"🔴 LIVE OPEN [{aid}] #{trade_id}: {symbol} {direction} "
            f"×{leverage} ${size_usdt} entry={fill_price} "
            f"tp={tp_order_id} sl={sl_order_id}"
        )
        # Telegram-уведомление (не блокирует ответ)
        try:
            asyncio.create_task(_send_live_open_alert(doc, account))
        except Exception:
            pass
        return {"ok": True, "trade": doc}
    except Exception as e:
        import traceback
        logger.error(f"[live-{aid}] OPEN fail {symbol}: {e}\n{traceback.format_exc()[-500:]}")
        return {"ok": False, "error": str(e)}


async def on_signal_for_account(signal_data: dict, account: dict) -> Optional[dict]:
    """[DEPRECATED] Старая логика: каждый аккаунт независимо вызывает ai_decide.
    Заменена на mirror_paper_for_account чтобы testnet/real точно копировали
    paper. Оставлено для обратной совместимости."""
    if not account.get("enabled") or account.get("kill_switch"):
        return None
    import paper_trader as pt
    decision = await pt.ai_decide(signal_data)
    if not decision.get("enter"):
        return None
    if account.get("confirmation_required"):
        logger.debug(f"[live-{account['_id']}] confirmation_required — скип в legacy режиме")
        return None
    return await open_position_for_account(signal_data, decision, account)


async def mirror_paper_for_account(signal_data: dict, decision: dict, account: dict) -> Optional[dict]:
    """Точная копия paper-решения для конкретного live-аккаунта.

    Вызывается из watcher._paper_on_signal ПОСЛЕ того как paper_trader.on_signal
    вернул открытую позицию (т.е. paper уже принял решение). Live аккаунт
    использует те же параметры (leverage, size_pct, tp1, sl, source) — никаких
    собственных ai_decide / verified-checks. Применяются только account-level
    safety guards (enabled, kill_switch, max_positions, daily_loss).

    Если mirror не удался — пишет mirror-attempt в live_trades со статусом
    FAILED, чтобы UI показал что попытка была + шлёт алерт юзеру.
    """
    aid = account.get("_id", "?")
    if not account.get("enabled"):
        logger.debug(f"[live-{aid}] disabled — skip mirror")
        return None
    if account.get("kill_switch"):
        logger.warning(f"[live-{aid}] kill_switch active — skip mirror")
        return None
    result = await open_position_for_account(signal_data, decision, account)
    # Если open не удался — записать FAILED-attempt чтобы было видно в UI
    if result and not result.get("ok"):
        try:
            from database import _live_trades, _get_db
            error_msg = str(result.get("error", "unknown"))
            symbol = signal_data.get("symbol", "")
            counter = _get_db().counters.find_one_and_update(
                {"_id": "live_trades"},
                {"$inc": {"seq": 1}},
                upsert=True, return_document=True,
            )
            trade_id = (counter or {}).get("seq", 1)
            _live_trades().insert_one({
                "trade_id": trade_id,
                "account_id": aid,
                "env": account.get("mode", "testnet"),
                "symbol": symbol,
                "pair": symbol.replace("USDT", "/USDT"),
                "direction": signal_data.get("direction", "?"),
                "status": "FAILED_OPEN",
                "source": signal_data.get("source", "unknown"),
                "paper_trade_id": signal_data.get("paper_trade_id"),
                "fail_reason": error_msg[:500],
                "tp1": decision.get("tp1"),
                "sl": decision.get("sl"),
                "leverage": decision.get("leverage"),
                "size_pct": decision.get("size_pct"),
                "opened_at": _utcnow(),
                "closed_at": _utcnow(),
            })
            logger.warning(f"[live-{aid}] FAILED_OPEN logged for {symbol}: {error_msg[:200]}")
            # Telegram-уведомление об ошибке
            try:
                asyncio.create_task(_send_mirror_failed_alert(symbol,
                    signal_data.get("direction"), error_msg, account))
            except Exception:
                pass
        except Exception as logerr:
            logger.warning(f"[live-{aid}] FAILED_OPEN log fail: {logerr}")
    return result


async def _send_mirror_failed_alert(symbol: str, direction: str, error: str, account: dict):
    """Алерт когда mirror не удался."""
    try:
        from config import BOT6_BOT_TOKEN, ADMIN_CHAT_ID
        if not BOT6_BOT_TOKEN or not ADMIN_CHAT_ID:
            return
        from aiogram import Bot
        from aiogram.client.default import DefaultBotProperties
        from aiogram.enums import ParseMode
        bot = Bot(token=BOT6_BOT_TOKEN,
                  default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        mode = account.get("mode", "testnet")
        label = account.get("label") or str(account.get("_id", "?"))
        # Понятное объяснение для частых ошибок
        nice_err = error
        if "not listed on" in error or "not-supported" in error:
            nice_err = f"❌ Символ {symbol} не торгуется на Binance {mode}"
        elif "Invalid symbol" in error or "does not have market" in error.lower() or "-1121" in error:
            nice_err = f"❌ Символ {symbol} не существует на Binance"
        elif "-4164" in error or "< min" in error:
            nice_err = f"⚠️ Слишком маленький размер позиции (min notional)"
        elif "-2019" in error or "insufficient margin" in error.lower():
            nice_err = f"⚠️ Недостаточно свободной маржи на бирже"
        elif "-4005" in error or "max" in error.lower() and "amount" in error.lower():
            nice_err = f"⚠️ Объём превышает лимит на символе"
        elif "size too small" in error:
            nice_err = f"⚠️ Размер позиции меньше минимума на этом символе"
        elif "safety" in error:
            nice_err = f"⚠️ Заблокировано safety: {error}"
        text = (
            f"⚠️ <b>LIVE MIRROR FAIL [{mode.upper()}]</b>\n"
            f"👤 {label}\n"
            f"📊 {symbol} {direction}\n"
            f"💬 {nice_err[:300]}\n\n"
            f"<i>Paper-сделка открылась, но на бирже не получилось зеркалить</i>"
        )
        await bot.send_message(int(ADMIN_CHAT_ID), text, parse_mode="HTML")
        await bot.session.close()
    except Exception as e:
        logger.warning(f"[live-trader] mirror-fail alert error: {e}")


# ════════════════════════════════════════════════════════════════
# Telegram уведомления для live trades
# ════════════════════════════════════════════════════════════════

async def _send_live_open_alert(trade: dict, account: dict) -> None:
    """Алерт при открытии live (testnet/real) позиции — в BOT6 (тот же что paper)."""
    try:
        from config import BOT6_BOT_TOKEN, ADMIN_CHAT_ID
        if not BOT6_BOT_TOKEN or not ADMIN_CHAT_ID:
            return
        from aiogram import Bot
        from aiogram.client.default import DefaultBotProperties
        from aiogram.enums import ParseMode
        bot = Bot(token=BOT6_BOT_TOKEN,
                  default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        mode = account.get("mode", "testnet")
        label = account.get("label") or account.get("owner") or str(account.get("_id", "?"))
        mode_emoji = "🧪" if mode == "testnet" else "🔴"
        direction = trade.get("direction", "LONG")
        dir_emoji = "📈" if direction == "LONG" else "📉"
        tp_str = f"{float(trade['tp1']):.4f}" if trade.get("tp1") else "—"
        sl_str = f"{float(trade['sl']):.4f}" if trade.get("sl") else "—"
        tp_ok = "✅" if trade.get("tp_order_id") else "⚠️"
        sl_ok = "✅" if trade.get("sl_order_id") else "⚠️"
        text = (
            f"{mode_emoji} <b>LIVE ОТКРЫТО [{mode.upper()}]</b> #{trade.get('trade_id')}\n"
            f"👤 {label}\n"
            f"{dir_emoji} <b>{trade.get('symbol')} {direction}</b> ×{trade.get('leverage')}x\n"
            f"💵 Вход: {float(trade.get('entry', 0)):.4f}\n"
            f"{tp_ok} TP: {tp_str}\n"
            f"{sl_ok} SL: {sl_str}\n"
            f"💰 Размер: ${trade.get('size_usdt', 0):.2f} ({trade.get('size_pct', 0):.1f}%)\n"
        )
        if trade.get("ai_reasoning"):
            text += f"📝 {str(trade['ai_reasoning'])[:120]}"
        await bot.send_message(int(ADMIN_CHAT_ID), text, parse_mode="HTML")
        await bot.session.close()
    except Exception as e:
        logger.warning(f"[live-trader] open alert fail: {e}")


# ════════════════════════════════════════════════════════════════
# Синхронизация позиций (per-account)
# ════════════════════════════════════════════════════════════════

async def _close_position_market(ex, account: dict, pos: dict, reason: str) -> dict:
    """Закрыть позицию по рыночной цене + отметить в DB."""
    from database import _live_trades
    aid = account.get("_id", "?")
    symbol = pos["symbol"]
    direction = pos["direction"]
    amount = pos.get("amount", 0)
    close_side = "sell" if direction == "LONG" else "buy"
    try:
        try:
            amt = float(await asyncio.to_thread(ex.amount_to_precision, symbol, amount))
        except Exception:
            amt = amount
        order = await asyncio.to_thread(
            ex.create_market_order, symbol, close_side, amt,
            None, {"reduceOnly": True},
        )
        exit_price = order.get("average") or order.get("price") or pos.get("entry")
        entry = float(pos["entry"])
        leverage = pos.get("leverage", 1)
        size_usdt = pos.get("size_usdt", 0)
        raw = ((float(exit_price) - entry) / entry) * 100
        if direction == "SHORT":
            raw = -raw
        pnl_pct = round(raw * leverage, 2)
        pnl_usdt = round(size_usdt * pnl_pct / 100, 2)
        _live_trades().update_one(
            {"trade_id": pos["trade_id"]},
            {"$set": {
                "status": reason,
                "exit_price": exit_price,
                "pnl_pct": pnl_pct,
                "pnl_usdt": pnl_usdt,
                "closed_at": _utcnow(),
                "close_order_id": order.get("id"),
                "db_managed_tpsl": True,
            }},
        )
        logger.warning(
            f"[live-{aid}] DB-managed close #{pos['trade_id']}: "
            f"{symbol} {reason} {pnl_pct:+.2f}% ${pnl_usdt:+.2f}"
        )
        try:
            asyncio.create_task(
                _send_live_close_alert(pos, reason, exit_price, pnl_pct, pnl_usdt, account)
            )
        except Exception:
            pass
        return {"ok": True, "trade_id": pos["trade_id"], "reason": reason, "pnl_pct": pnl_pct}
    except Exception as e:
        logger.error(f"[live-{aid}] DB-managed close fail #{pos.get('trade_id')}: {e}", exc_info=True)
        return {"ok": False, "trade_id": pos.get("trade_id"), "error": str(e)}


async def sync_positions_for_account(account: dict) -> dict:
    """Синхронизация с биржей для конкретного аккаунта.
    Обнаруживает позиции закрытые через TP/SL на бирже + сам триггерит TP/SL
    для DB-managed позиций (где tp_order_id/sl_order_id null — Binance testnet
    отклоняет TAKE_PROFIT_MARKET/STOP_MARKET с -4120)."""
    from database import _live_trades
    aid = account["_id"]
    ex = _get_exchange_for_account(account)
    if ex is None:
        return {"ok": False, "error": "exchange not configured", "account_id": str(aid)}

    synced = 0
    closed = 0
    try:
        positions = await asyncio.to_thread(ex.fetch_positions)
        open_symbols = {
            p["symbol"] for p in positions
            if float(p.get("contracts", 0) or 0) != 0
        }
    except Exception as e:
        return {"ok": False, "error": f"fetch_positions fail: {e}", "account_id": str(aid)}

    db_open = list(_live_trades().find({"status": "OPEN", "account_id": str(aid)}))
    # Грейс-период 90с: не закрываем позиции которые только что открылись —
    # на Binance может быть задержка eventual consistency между fill и
    # появлением в fetch_positions. Без этого race condition закрывает свежие.
    from datetime import timedelta
    grace_cutoff = _utcnow() - timedelta(seconds=90)

    # Текущие цены для DB-managed TP/SL триггера
    cur_prices: dict = {}
    if db_open:
        try:
            from exchange import get_prices_any as _gpa
            unique_pairs = list({p["symbol"].replace("USDT", "/USDT") for p in db_open})
            cur_prices = await asyncio.to_thread(_gpa, unique_pairs) or {}
        except Exception as _pe:
            logger.debug(f"[live-{aid}] price fetch fail: {_pe}")

    for pos in db_open:
        synced += 1
        opened = pos.get("opened_at")
        in_grace = False
        if opened and hasattr(opened, "replace"):
            opened_naive = opened.replace(tzinfo=None) if getattr(opened, "tzinfo", None) else opened
            if opened_naive > grace_cutoff:
                in_grace = True

        # ── DB-managed TP/SL: если на бирже нет TP/SL ордеров, проверяем цену сами ──
        sym = pos["symbol"]
        if not pos.get("tp_order_id") and not pos.get("sl_order_id"):
            cur = cur_prices.get(sym)
            if cur and not in_grace:
                tp1 = pos.get("tp1")
                sl = pos.get("sl")
                direction = pos.get("direction", "LONG")
                tp_hit = sl_hit = False
                if direction == "LONG":
                    if tp1 and cur >= float(tp1): tp_hit = True
                    if sl and cur <= float(sl):   sl_hit = True
                else:
                    if tp1 and cur <= float(tp1): tp_hit = True
                    if sl and cur >= float(sl):   sl_hit = True
                if tp_hit:
                    await _close_position_market(ex, account, pos, "TP")
                    closed += 1
                    continue
                if sl_hit:
                    await _close_position_market(ex, account, pos, "SL")
                    closed += 1
                    continue

        if in_grace:
            continue
        if pos["symbol"] not in open_symbols:
            # Позиция закрылась на бирже (TP/SL сработал)
            try:
                trades_list = await asyncio.to_thread(
                    ex.fetch_my_trades, pos["symbol"], None, 5
                )
                exit_price = pos["entry"]
                if trades_list:
                    exit_price = trades_list[-1].get("price", pos["entry"])

                entry = float(pos["entry"])
                direction = pos["direction"]
                leverage = pos.get("leverage", 1)
                size_usdt = pos.get("size_usdt", 0)
                raw = ((float(exit_price) - entry) / entry) * 100
                if direction == "SHORT":
                    raw = -raw
                pnl_pct = round(raw * leverage, 2)
                pnl_usdt = round(size_usdt * pnl_pct / 100, 2)
                reason = "TP" if pnl_usdt > 0 else "SL"

                _live_trades().update_one(
                    {"trade_id": pos["trade_id"]},
                    {"$set": {
                        "status": reason,
                        "exit_price": exit_price,
                        "pnl_pct": pnl_pct,
                        "pnl_usdt": pnl_usdt,
                        "closed_at": _utcnow(),
                        "sync_detected": True,
                    }},
                )
                closed += 1
                logger.warning(
                    f"[live-{aid}] sync close #{pos['trade_id']}: "
                    f"{pos['symbol']} {reason} pnl={pnl_pct:+.2f}% ${pnl_usdt:+.2f}"
                )
                # Уведомление о закрытии
                try:
                    asyncio.create_task(
                        _send_live_close_alert(pos, reason, exit_price, pnl_pct, pnl_usdt, account)
                    )
                except Exception:
                    pass
            except Exception as e:
                logger.debug(
                    f"[live-{aid}] sync fetch fail #{pos.get('trade_id')}: {e}",
                    exc_info=True,
                )

    return {"ok": True, "account_id": str(aid), "synced": synced, "auto_closed": closed}


async def sync_all_accounts() -> list:
    """Синхронизация всех enabled аккаунтов. Вызывается из watcher каждые 30с."""
    try:
        from live_safety import get_enabled_accounts
        accounts = get_enabled_accounts()
    except Exception:
        return []
    results = []
    for acc in accounts:
        try:
            r = await sync_positions_for_account(acc)
            results.append(r)
        except Exception as e:
            results.append({
                "ok": False,
                "account_id": str(acc.get("_id", "?")),
                "error": str(e),
            })
    return results


async def _send_live_close_alert(
    trade: dict, reason: str, exit_price, pnl_pct: float, pnl_usdt: float, account: dict
) -> None:
    """Алерт при закрытии live позиции (обнаруженной через sync)."""
    try:
        from config import BOT6_BOT_TOKEN, ADMIN_CHAT_ID
        if not BOT6_BOT_TOKEN or not ADMIN_CHAT_ID:
            return
        from aiogram import Bot
        from aiogram.client.default import DefaultBotProperties
        from aiogram.enums import ParseMode
        bot = Bot(token=BOT6_BOT_TOKEN,
                  default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        mode = account.get("mode", "testnet")
        label = account.get("label") or account.get("owner") or str(account.get("_id", "?"))
        mode_emoji = "🧪" if mode == "testnet" else "🔴"
        pnl_emoji = "✅" if pnl_usdt >= 0 else "❌"
        direction = trade.get("direction", "LONG")
        dir_emoji = "📈" if direction == "LONG" else "📉"
        text = (
            f"{mode_emoji} <b>LIVE ЗАКРЫТО [{mode.upper()}] {reason}</b> #{trade.get('trade_id')}\n"
            f"👤 {label}\n"
            f"{dir_emoji} {trade.get('symbol')} {direction} ×{trade.get('leverage')}x\n"
            f"💵 Вход: {float(trade.get('entry', 0)):.4f} → {float(exit_price):.4f}\n"
            f"{pnl_emoji} PnL: {pnl_pct:+.2f}% (${pnl_usdt:+.2f})\n"
        )
        await bot.send_message(int(ADMIN_CHAT_ID), text, parse_mode="HTML")
        await bot.session.close()
    except Exception as e:
        logger.warning(f"[live-trader] close alert fail: {e}")
