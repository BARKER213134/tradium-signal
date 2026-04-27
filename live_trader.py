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
    """Возвращает ccxt instance для конкретного аккаунта.
    Поддерживает Binance Futures и BingX Perpetual.
    Каждый аккаунт = свои ключи + биржа + sandbox режим."""
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

    exchange_name = (account.get("exchange") or "binance").lower()
    mode = account.get("mode", "real")

    if exchange_name == "bingx":
        ex = ccxt.bingx({
            "apiKey": api_key,
            "secret": api_secret,
            # BingX 'swap' = perpetual futures (USDT-M)
            "options": {"defaultType": "swap"},
            "enableRateLimit": True,
        })
        # BingX testnet через demo mode (если нужно — отдельно настраивается)
    elif exchange_name == "binance":
        ex = ccxt.binance({
            "apiKey": api_key,
            "secret": api_secret,
            "options": {"defaultType": "future"},
            "enableRateLimit": True,
        })
        if mode == "testnet":
            ex.set_sandbox_mode(True)
    else:
        logger.error(f"[live-trader] unknown exchange '{exchange_name}' for account {aid}")
        return None

    _exchange_per_account[aid] = ex
    logger.info(
        f"[live-trader] account '{aid}' ccxt {exchange_name} initialized "
        f"(mode={mode})"
    )
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

    # ── Fix 2: Pre-check символа на бирже + нормализация множителей ──
    # PEPE-сигнал на BingX → 1000PEPE (или наоборот) — resolve_symbol находит
    # правильный canonical вариант
    try:
        from exchange_symbols import resolve_symbol
        resolved = resolve_symbol(symbol, account.get("exchange"))
        if resolved and resolved != symbol:
            logger.info(f"[live-{aid}] symbol resolved {symbol} → {resolved} for {account.get('exchange')}")
            symbol = resolved
            signal_data["symbol"] = symbol  # обновляем для consistency
    except Exception as _re:
        logger.debug(f"[live-{aid}] resolve_symbol fail: {_re}")

    # Загрузка markets — для precision/limits проверок ниже
    try:
        if not getattr(ex, "markets", None):
            await asyncio.wait_for(asyncio.to_thread(ex.load_markets), timeout=15.0)
    except asyncio.TimeoutError:
        logger.warning(f"[live-{aid}] load_markets timeout — proceeding without pre-check")
    except Exception as _le:
        logger.warning(f"[live-{aid}] load_markets fail: {_le}")

    # Сохраняем оригинальный символ (XXXUSDT) для DB consistency с paper, отдельно
    # держим canonical (XXX/USDT:USDT) только для ccxt-вызовов.
    original_symbol = symbol  # Например "ETHUSDT"
    canonical_symbol = symbol
    if getattr(ex, "markets", None):
        # ccxt для BingX использует unified format 'ETH/USDT:USDT'.
        # Сигнал symbol = 'ETHUSDT' — пробуем несколько форматов чтобы найти правильный.
        mkt_lookup = None
        # 1. Прямой lookup
        try:
            mkt_lookup = ex.market(symbol)
        except Exception:
            mkt_lookup = None
        # 2. По markets_by_id (BingX id = 'ETH-USDT', но иногда совпадает с XXXUSDT)
        if not mkt_lookup and getattr(ex, "markets_by_id", None):
            id_match = ex.markets_by_id.get(symbol)
            if id_match:
                if isinstance(id_match, list):
                    for m in id_match:
                        if m and (m.get("swap") or m.get("type") == "swap"):
                            mkt_lookup = m; break
                    if not mkt_lookup and id_match:
                        mkt_lookup = id_match[0]
                else:
                    mkt_lookup = id_match
        # 3. Перебор форматов: ETH/USDT:USDT, ETH/USDT, ETH-USDT
        if not mkt_lookup and symbol.endswith("USDT"):
            base = symbol[:-4]
            for try_sym in (f"{base}/USDT:USDT", f"{base}/USDT", f"{base}-USDT"):
                try:
                    mkt_lookup = ex.market(try_sym)
                    if mkt_lookup: break
                except Exception:
                    continue
        # 4. Полный sweep по markets — найти market с подходящим id
        if not mkt_lookup:
            for canon, m in (ex.markets or {}).items():
                if not m: continue
                mid = (m.get("id") or "").upper().replace("-", "").replace("/", "").replace(":USDT", "")
                if mid == symbol and (m.get("swap") or m.get("type") == "swap"):
                    mkt_lookup = m; break

        if mkt_lookup:
            if mkt_lookup.get("active") is False:
                return {"ok": False,
                        "error": f"symbol {symbol} is INACTIVE on {account.get('mode','real')} (delisted)"}
            canonical_symbol = mkt_lookup["symbol"]  # для ccxt API
        else:
            return {"ok": False,
                    "error": f"symbol {symbol} not listed on {account.get('mode','real')} (skip: not-supported)"}
    # symbol используется в ccxt-вызовах, original_symbol в DB
    symbol = canonical_symbol

    # ── Fix 1: Динамический клампинг по доступной марже на бирже ──
    try:
        bal_data = await asyncio.wait_for(asyncio.to_thread(ex.fetch_balance), timeout=15.0)
        usdt_free = float((bal_data.get("USDT") or {}).get("free", 0) or 0)
        usdt_total = float((bal_data.get("USDT") or {}).get("total", 0) or 0)
    except asyncio.TimeoutError:
        logger.warning(f"[live-{aid}] fetch_balance timeout — assuming free=0")
        usdt_free = 0.0
        usdt_total = 0.0
    except Exception as _be:
        logger.debug(f"[live-{aid}] fetch_balance fail: {_be}")
        usdt_free = 0.0
        usdt_total = 0.0

    # Hard skip: если free margin меньше $20 — открывать нечего
    MIN_FREE_FOR_OPEN = 20.0
    if usdt_free < MIN_FREE_FOR_OPEN:
        return {"ok": False,
                "error": f"insufficient margin: free=${usdt_free:.2f} < ${MIN_FREE_FOR_OPEN} "
                         f"(total=${usdt_total:.2f}, закрой старые позиции на бирже)"}

    # Используем максимум 50% свободной маржи на одну позицию (буфер на slippage/fees)
    # required_margin для futures ≈ size_usdt (margin = notional / leverage = size_usdt)
    required_margin = size_usdt
    if required_margin > usdt_free * 0.5:
        new_size = round(usdt_free * 0.5, 2)
        if new_size < 5.0:
            return {"ok": False,
                    "error": f"insufficient margin after downscale: free=${usdt_free:.2f}, "
                             f"50%=${new_size} < $5 min"}
        logger.warning(
            f"[live-{aid}] downscaled {symbol} size: ${size_usdt} → ${new_size} "
            f"(free=${usdt_free:.2f}, half-rule)"
        )
        size_usdt = new_size

    side = "buy" if direction == "LONG" else "sell"
    notional = size_usdt * leverage
    amount = notional / entry_price

    # ── Fix 3: Клампинг amount по exchange limits ──
    mkt = None
    try:
        mkt = ex.market(symbol) if ex.markets else None
    except Exception:
        mkt = None
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
            "symbol": original_symbol,   # XXXUSDT — совместимо с paper
            "pair": original_symbol.replace("USDT", "/USDT"),
            "ccxt_symbol": canonical_symbol,  # для отладки/debug
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
    вернул открытую позицию. Live аккаунт использует те же параметры
    (leverage, size_pct, tp1, sl, source) — никаких собственных ai_decide.

    🪞 АВТО-СИНК VBAL: account.balance автоматически = paper.balance перед
    каждой сделкой → размеры идентичны без ручного ввода Vbal.

    Если mirror не удался — пишет FAILED_OPEN в live_trades + шлёт алерт.
    """
    aid = account.get("_id", "?")
    if not account.get("enabled"):
        logger.debug(f"[live-{aid}] disabled — skip mirror")
        return None
    if account.get("kill_switch"):
        logger.warning(f"[live-{aid}] kill_switch active — skip mirror")
        return None

    # ── 🪞 АВТО-СИНК: account.balance ← paper.balance ──
    # Гарантирует что размер live-сделки = размеру paper-сделки.
    # При росте/падении paper-баланса live автоматически масштабируется.
    try:
        import paper_trader as pt
        paper_balance = pt.get_balance()
        if paper_balance and paper_balance > 0:
            from database import _live_accounts, utcnow
            old_bal = account.get("balance", 0) or 0
            if abs(paper_balance - old_bal) > 0.01:  # значимое изменение
                _live_accounts().update_one(
                    {"_id": aid},
                    {"$set": {"balance": float(paper_balance),
                              "balance_synced_from_paper": True,
                              "updated_at": utcnow()}},
                )
                account["balance"] = float(paper_balance)
                logger.info(
                    f"[live-{aid}] auto-sync Vbal: ${old_bal:.2f} → ${paper_balance:.2f} (from paper)"
                )
    except Exception as _se:
        logger.warning(f"[live-{aid}] paper balance sync fail: {_se}")

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
    """DESYNC alert когда paper открыл, а live зеркалить не получилось."""
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
        nice_err = _humanize_error(error, symbol, mode)
        text = (
            f"⚠️ <b>DESYNC: paper=OPEN, live=FAILED</b>\n"
            f"👤 {label} ({mode.upper()})\n"
            f"📊 {symbol} {direction}\n"
            f"💬 {nice_err[:300]}\n\n"
            f"<i>📄 Paper открыл, 🔴 Live на бирже не получилось.\n"
            f"PnL paper и live теперь будут расходиться по этой сделке.</i>"
        )
        await bot.send_message(int(ADMIN_CHAT_ID), text, parse_mode="HTML")
        await bot.session.close()
    except Exception as e:
        logger.warning(f"[live-trader] mirror-fail alert error: {e}")


def _humanize_error(error: str, symbol: str, mode: str = "real") -> str:
    """Перевод технических ошибок Binance/BingX/ccxt в понятные русские причины."""
    if not error:
        return "unknown"
    err_l = error.lower()
    # Symbol issues — общие
    if "not listed on" in error or "not-supported" in error:
        return f"❌ Символ {symbol} не торгуется на бирже ({mode})"
    if "invalid symbol" in err_l or "does not have market" in err_l or "-1121" in error:
        return f"❌ Символ {symbol} не существует на бирже"
    if "INACTIVE" in error or "inactive" in err_l:
        return f"❌ Символ {symbol} деактивирован (delisted)"
    # Notional / size
    if "-4164" in error or "min notional" in err_l or "< min $" in error:
        return "⚠️ Слишком маленький размер позиции (min notional)"
    if "size too small" in error:
        return f"⚠️ Размер позиции меньше минимума на {symbol}"
    # Margin
    if "-2019" in error or "insufficient" in err_l or "margin is insufficient" in err_l:
        return "⚠️ Недостаточно свободной маржи на бирже"
    # Quantity / max
    if "-4005" in error or ("max" in err_l and "amount" in err_l) or ("max" in err_l and "quantity" in err_l):
        return "⚠️ Объём превышает лимит на символе"
    # BingX specific
    if "100400" in error or "100410" in error:
        return "⚠️ BingX: invalid params (проверь leverage/symbol)"
    if "permission" in err_l:
        return "🔐 API не имеет нужных прав (futures trading)"
    if "ip" in err_l and ("white" in err_l or "restrict" in err_l):
        return "🌐 IP не в whitelist API ключа"
    # Algo/order type
    if "-4120" in error or "algo order" in err_l:
        return "⚠️ Биржа отклонила тип ордера (используем DB-managed TP/SL)"
    # Generic
    if "safety:" in error:
        return f"⚠️ Заблокировано safety: {error.replace('safety:', '').strip()}"
    if "kill switch" in err_l:
        return "🛑 Аккаунт остановлен (kill switch active)"
    return error[:200]


# ════════════════════════════════════════════════════════════════
# Telegram уведомления для live trades
# ════════════════════════════════════════════════════════════════

async def _send_live_open_alert(trade: dict, account: dict) -> None:
    """Алерт при открытии live позиции — синхронизирован с paper (показывает slippage)."""
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
        mode_label = "TESTNET" if mode == "testnet" else "REAL"
        direction = trade.get("direction", "LONG")
        dir_emoji = "📈" if direction == "LONG" else "📉"
        tp_str = f"{float(trade['tp1']):.4f}" if trade.get("tp1") else "—"
        sl_str = f"{float(trade['sl']):.4f}" if trade.get("sl") else "—"
        # Birden TP/SL ордера на бирже
        tp_status = "✅ на бирже" if trade.get("tp_order_id") else "⚠️ DB-managed"
        sl_status = "✅ на бирже" if trade.get("sl_order_id") else "⚠️ DB-managed"
        # Slippage paper vs live
        slippage_str = ""
        ptid = trade.get("paper_trade_id")
        if ptid:
            try:
                from database import _get_db
                paper = _get_db().paper_trades.find_one({"trade_id": ptid}, {"entry": 1})
                if paper and paper.get("entry"):
                    paper_entry = float(paper["entry"])
                    live_entry = float(trade.get("entry") or 0)
                    if paper_entry > 0:
                        slip_pct = (live_entry - paper_entry) / paper_entry * 100
                        emoji = "🟢" if abs(slip_pct) < 0.05 else "🟡" if abs(slip_pct) < 0.2 else "🔴"
                        slippage_str = f"\n📊 Slippage vs paper: {emoji} {slip_pct:+.3f}%"
            except Exception:
                pass
        text = (
            f"{mode_emoji} <b>LIVE OPEN [{mode_label}]</b> #{trade.get('trade_id')}\n"
            f"👤 {label}\n"
            f"{dir_emoji} <b>{trade.get('symbol')} {direction}</b> ×{trade.get('leverage')}x\n"
            f"💵 Вход: {float(trade.get('entry', 0)):.4f}\n"
            f"🎯 TP: {tp_str}  ({tp_status})\n"
            f"🛡 SL: {sl_str}  ({sl_status})\n"
            f"💰 Размер: ${trade.get('size_usdt', 0):.2f} ({trade.get('size_pct', 0):.1f}%)"
        )
        if ptid:
            text += f"\n🔗 Paper #{ptid}"
        text += slippage_str
        if trade.get("ai_reasoning"):
            text += f"\n📝 {str(trade['ai_reasoning'])[:120]}"
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


async def mirror_partial_close_for_account(
    paper_pos: dict, live_pos: dict, fraction_to_close: float, reason: str, account: dict
) -> dict:
    """Зеркалит partial close paper → live.
    fraction_to_close — какая ДОЛЯ от ОРИГИНАЛЬНОЙ позиции должна быть закрыта.
    """
    aid = account.get("_id", "?")
    ex = _get_exchange_for_account(account)
    if ex is None:
        return {"ok": False, "error": "no exchange"}
    symbol_db = live_pos.get("symbol")
    direction = live_pos.get("direction")
    original_amount = float(live_pos.get("amount") or 0)
    if original_amount <= 0:
        return {"ok": False, "error": "no original amount"}
    close_amount = original_amount * fraction_to_close
    # Точная precision
    try:
        ccxt_sym = live_pos.get("ccxt_symbol") or symbol_db
        close_amount = float(await asyncio.to_thread(ex.amount_to_precision, ccxt_sym, close_amount))
    except Exception:
        close_amount = round(close_amount, 6)
    if close_amount <= 0:
        return {"ok": False, "error": "amount=0 after precision"}
    side = "sell" if direction == "LONG" else "buy"
    try:
        ccxt_sym = live_pos.get("ccxt_symbol") or symbol_db
        order = await asyncio.to_thread(
            ex.create_market_order, ccxt_sym, side, close_amount,
            None, {"reduceOnly": True},
        )
        exit_price = order.get("average") or order.get("price") or live_pos.get("entry")
        # Compute partial PnL
        entry = float(live_pos.get("entry") or 0)
        leverage = live_pos.get("leverage", 1)
        original_size = float(live_pos.get("size_usdt") or 0)
        size_closed = original_size * fraction_to_close
        raw = ((float(exit_price) - entry) / entry) * 100
        if direction == "SHORT":
            raw = -raw
        pnl_pct = round(raw * leverage, 2)
        pnl_usdt = round(size_closed * pnl_pct / 100, 2)
        # Update live_pos with the new partial
        from database import _live_trades
        new_partial = {
            "at": _utcnow(), "fraction": round(fraction_to_close, 4),
            "exit_price": exit_price, "pnl_pct": pnl_pct, "pnl_usdt": pnl_usdt,
            "reason": reason, "order_id": order.get("id"),
        }
        partial_closes = list(live_pos.get("partial_closes") or []) + [new_partial]
        tp_hits = list(live_pos.get("tp_ladder_hits") or []) + [reason]
        new_realized = round(float(live_pos.get("realized_pnl_usdt") or 0) + pnl_usdt, 2)
        new_remaining = round(float(live_pos.get("remaining_fraction") or 1.0) - fraction_to_close, 4)
        _live_trades().update_one(
            {"trade_id": live_pos["trade_id"]},
            {"$set": {
                "partial_closes": partial_closes,
                "tp_ladder_hits": tp_hits,
                "realized_pnl_usdt": new_realized,
                "remaining_fraction": new_remaining,
            }},
        )
        logger.warning(
            f"[live-{aid}] PARTIAL #{live_pos['trade_id']}: {symbol_db} {reason} "
            f"{fraction_to_close*100:.0f}% → ${pnl_usdt:+.2f} (realized=${new_realized:+.2f})"
        )
        try:
            asyncio.create_task(
                _send_live_partial_alert(live_pos, reason, fraction_to_close, pnl_usdt, account)
            )
        except Exception:
            pass
        return {"ok": True, "trade_id": live_pos["trade_id"], "pnl_usdt": pnl_usdt}
    except Exception as e:
        logger.error(f"[live-{aid}] partial close fail #{live_pos.get('trade_id')}: {e}", exc_info=True)
        return {"ok": False, "error": str(e)}


async def mirror_sl_move_for_account(live_pos: dict, new_sl: float, account: dict) -> dict:
    """Двигает SL на бирже: отменяет старый SL ордер, ставит новый по new_sl."""
    aid = account.get("_id", "?")
    ex = _get_exchange_for_account(account)
    if ex is None:
        return {"ok": False, "error": "no exchange"}
    from database import _live_trades
    ccxt_sym = live_pos.get("ccxt_symbol") or live_pos.get("symbol")
    direction = live_pos.get("direction")
    sl_side = "sell" if direction == "LONG" else "buy"
    # Cancel старый SL ордер если был
    old_sl_id = live_pos.get("sl_order_id")
    if old_sl_id:
        try:
            await asyncio.to_thread(ex.cancel_order, old_sl_id, ccxt_sym)
        except Exception as ce:
            logger.debug(f"[live-{aid}] cancel old SL fail: {ce}")
    # Place новый SL (если эта биржа поддерживает; иначе DB-managed)
    new_sl_id = None
    try:
        amount = live_pos.get("amount") or 0
        try:
            amount = float(await asyncio.to_thread(ex.amount_to_precision, ccxt_sym, amount * (live_pos.get("remaining_fraction") or 1.0)))
        except Exception:
            pass
        if amount > 0:
            sl_order = await asyncio.to_thread(
                ex.create_order, ccxt_sym, "STOP_MARKET", sl_side, amount,
                None, {"stopPrice": float(new_sl), "reduceOnly": True},
            )
            new_sl_id = sl_order.get("id")
    except Exception as e:
        logger.debug(f"[live-{aid}] new SL place fail (will be DB-managed): {e}")
    _live_trades().update_one(
        {"trade_id": live_pos["trade_id"]},
        {"$set": {
            "sl": float(new_sl),
            "sl_order_id": new_sl_id,
            "sl_moved_to_be": True,
        }},
    )
    logger.warning(f"[live-{aid}] SL moved #{live_pos['trade_id']}: → {new_sl}")
    return {"ok": True, "new_sl": new_sl, "new_sl_order_id": new_sl_id}


async def mirror_full_close_for_account(live_pos: dict, reason: str, account: dict, exit_price_hint: float = None) -> dict:
    """Закрывает остаток позиции market reduceOnly когда paper закрылся."""
    aid = account.get("_id", "?")
    ex = _get_exchange_for_account(account)
    if ex is None:
        return {"ok": False, "error": "no exchange"}
    ccxt_sym = live_pos.get("ccxt_symbol") or live_pos.get("symbol")
    direction = live_pos.get("direction")
    close_side = "sell" if direction == "LONG" else "buy"
    remaining = float(live_pos.get("remaining_fraction") or 1.0)
    original_amount = float(live_pos.get("amount") or 0)
    close_amount = original_amount * remaining
    try:
        close_amount = float(await asyncio.to_thread(ex.amount_to_precision, ccxt_sym, close_amount))
    except Exception:
        close_amount = round(close_amount, 6)
    if close_amount <= 0:
        return {"ok": False, "error": "amount=0"}
    try:
        # Cancel TP/SL ордера если есть
        for fld in ("tp_order_id", "sl_order_id"):
            oid = live_pos.get(fld)
            if oid:
                try:
                    await asyncio.to_thread(ex.cancel_order, oid, ccxt_sym)
                except Exception:
                    pass
        order = await asyncio.to_thread(
            ex.create_market_order, ccxt_sym, close_side, close_amount,
            None, {"reduceOnly": True},
        )
        exit_price = order.get("average") or order.get("price") or exit_price_hint or live_pos.get("entry")
        entry = float(live_pos.get("entry") or 0)
        leverage = live_pos.get("leverage", 1)
        original_size = float(live_pos.get("size_usdt") or 0)
        size_remaining = original_size * remaining
        raw = ((float(exit_price) - entry) / entry) * 100
        if direction == "SHORT":
            raw = -raw
        pnl_pct_remaining = round(raw * leverage, 2)
        pnl_usdt_remaining = round(size_remaining * pnl_pct_remaining / 100, 2)
        total_pnl = round(float(live_pos.get("realized_pnl_usdt") or 0) + pnl_usdt_remaining, 2)
        total_pct = round(total_pnl / original_size * 100, 2) if original_size else 0
        from database import _live_trades
        _live_trades().update_one(
            {"trade_id": live_pos["trade_id"]},
            {"$set": {
                "status": reason,
                "exit_price": exit_price,
                "pnl_pct": total_pct,
                "pnl_usdt": total_pnl,
                "closed_at": _utcnow(),
                "close_order_id": order.get("id"),
                "remaining_fraction": 0,
                "paper_synced_close": True,
            }},
        )
        logger.warning(
            f"[live-{aid}] FULL CLOSE #{live_pos['trade_id']}: {ccxt_sym} {reason} "
            f"total=${total_pnl:+.2f} ({total_pct:+.2f}%)"
        )
        try:
            asyncio.create_task(
                _send_live_close_alert(live_pos, reason, exit_price, total_pct, total_pnl, account)
            )
        except Exception:
            pass
        return {"ok": True, "pnl_usdt": total_pnl, "pnl_pct": total_pct}
    except Exception as e:
        logger.error(f"[live-{aid}] full close fail #{live_pos.get('trade_id')}: {e}", exc_info=True)
        return {"ok": False, "error": str(e)}


async def paper_to_live_sync_check() -> dict:
    """Сверяет paper и live позиции:
    - Если paper закрыл partial (TP1/TP2) — live тоже закрывает ту же долю
    - Если paper подвинул SL (BE/BE+/TRAIL) — live двигает SL на бирже
    - Если paper закрылся полностью (status != OPEN) — live тоже закрывается

    Идемпотентно: запускается каждые 15с, ничего не дублирует.
    """
    from database import _get_db, _live_trades
    from live_safety import get_enabled_accounts
    db = _get_db()

    accounts = get_enabled_accounts()
    if not accounts:
        return {"ok": True, "synced": 0, "skipped_no_accounts": True}

    # Все live OPEN с paper_trade_id
    live_open = list(_live_trades().find({
        "status": "OPEN", "paper_trade_id": {"$ne": None},
    }))
    if not live_open:
        return {"ok": True, "synced": 0}

    # Все paper позиции (все статусы) для этих trade_ids
    paper_ids = list({lt.get("paper_trade_id") for lt in live_open if lt.get("paper_trade_id")})
    paper_docs = {p["trade_id"]: p for p in db.paper_trades.find({"trade_id": {"$in": paper_ids}})}

    actions = []
    accounts_by_id = {a["_id"]: a for a in accounts}

    for live in live_open:
        ptid = live.get("paper_trade_id")
        if ptid is None: continue
        paper = paper_docs.get(ptid)
        if not paper: continue

        acc = accounts_by_id.get(live.get("account_id"))
        if not acc: continue

        # === A. Paper закрылся полностью → закрыть live ===
        if paper.get("status") not in (None, "OPEN"):
            r = await mirror_full_close_for_account(
                live, paper.get("status", "MANUAL"), acc,
                exit_price_hint=paper.get("exit_price"),
            )
            actions.append({"action": "full_close", "trade_id": live["trade_id"], "result": r})
            continue

        # === B. Paper зафиксировал partials → закрыть те же доли в live ===
        paper_partials = paper.get("partial_closes") or []
        live_partials = live.get("partial_closes") or []
        # Сравниваем по reason (TP1_PARTIAL, TP2_PARTIAL, etc.)
        live_reasons = {pc.get("reason") for pc in live_partials}
        for pp in paper_partials:
            if pp.get("reason") in live_reasons:
                continue
            # Эту долю надо зеркалить
            r = await mirror_partial_close_for_account(
                paper, live, float(pp.get("fraction") or 0), pp.get("reason", "PARTIAL"), acc
            )
            actions.append({"action": "partial", "trade_id": live["trade_id"],
                            "reason": pp.get("reason"), "result": r})
            # Обновим live в памяти на случай нескольких partials в этом же loop
            if r.get("ok"):
                live["partial_closes"] = live.get("partial_closes", []) + [pp]
                live["realized_pnl_usdt"] = live.get("realized_pnl_usdt", 0) + r.get("pnl_usdt", 0)
                live["remaining_fraction"] = live.get("remaining_fraction", 1) - float(pp.get("fraction") or 0)

        # === C. Paper подвинул SL → подвинуть SL в live ===
        paper_sl = paper.get("sl")
        live_sl = live.get("sl")
        if (paper.get("sl_moved_to_be") or paper.get("sl_moved_to_be_plus") or paper.get("sl_trailing")):
            if paper_sl and abs(float(paper_sl) - float(live_sl or 0)) / float(paper_sl) > 0.001:
                r = await mirror_sl_move_for_account(live, float(paper_sl), acc)
                actions.append({"action": "sl_move", "trade_id": live["trade_id"],
                                "new_sl": paper_sl, "result": r})

    return {"ok": True, "synced": len(actions), "actions": actions[:30]}


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
    """Алерт при закрытии live позиции."""
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
        mode_label = "TESTNET" if mode == "testnet" else "REAL"
        pnl_emoji = "✅" if pnl_usdt >= 0 else "❌"
        reason_emoji = {"TP": "🎯", "SL": "🛑", "BE": "⚖️", "TRAIL": "📉",
                        "MANUAL": "👤", "AI_CLOSE": "🤖"}.get(reason, "✕")
        direction = trade.get("direction", "LONG")
        dir_emoji = "📈" if direction == "LONG" else "📉"
        # Paper PnL для сравнения
        paper_pnl_str = ""
        ptid = trade.get("paper_trade_id")
        if ptid:
            try:
                from database import _get_db
                paper = _get_db().paper_trades.find_one(
                    {"trade_id": ptid},
                    {"pnl_pct": 1, "pnl_usdt": 1, "status": 1}
                )
                if paper and paper.get("pnl_usdt") is not None:
                    pp = float(paper["pnl_pct"] or 0)
                    pu = float(paper["pnl_usdt"] or 0)
                    diff_usdt = pnl_usdt - pu
                    diff_emoji = "🟢" if abs(diff_usdt) < 1 else "🟡" if abs(diff_usdt) < 5 else "🔴"
                    paper_pnl_str = (
                        f"\n📄 Paper PnL: {pp:+.2f}% (${pu:+.2f})"
                        f"\n📊 Diff vs paper: {diff_emoji} ${diff_usdt:+.2f}"
                    )
            except Exception:
                pass
        text = (
            f"{mode_emoji} <b>LIVE CLOSE [{mode_label}] {reason_emoji} {reason}</b> #{trade.get('trade_id')}\n"
            f"👤 {label}\n"
            f"{dir_emoji} {trade.get('symbol')} {direction} ×{trade.get('leverage')}x\n"
            f"💵 {float(trade.get('entry', 0)):.4f} → {float(exit_price):.4f}\n"
            f"{pnl_emoji} <b>Live PnL: {pnl_pct:+.2f}% (${pnl_usdt:+.2f})</b>"
            f"{paper_pnl_str}"
        )
        await bot.send_message(int(ADMIN_CHAT_ID), text, parse_mode="HTML")
        await bot.session.close()
    except Exception as e:
        logger.warning(f"[live-trader] close alert fail: {e}")


async def _send_live_partial_alert(
    live_pos: dict, reason: str, fraction: float, pnl_usdt: float, account: dict
) -> None:
    """Алерт при partial close (TP1_PARTIAL, TP2_PARTIAL, etc.)"""
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
        mode_emoji = "🧪" if mode == "testnet" else "🔴"
        text = (
            f"🪜 <b>LIVE PARTIAL {reason}</b> #{live_pos.get('trade_id')}\n"
            f"{mode_emoji} {label}\n"
            f"📊 {live_pos.get('symbol')} {live_pos.get('direction')} ×{live_pos.get('leverage')}x\n"
            f"📦 Закрыто {fraction*100:.0f}% позиции\n"
            f"💰 Зафиксировано: ${pnl_usdt:+.2f}\n"
            f"📈 Realized cumulative: ${float(live_pos.get('realized_pnl_usdt') or 0) + pnl_usdt:+.2f}"
        )
        await bot.send_message(int(ADMIN_CHAT_ID), text, parse_mode="HTML")
        await bot.session.close()
    except Exception as e:
        logger.warning(f"[live-trader] partial alert fail: {e}")
