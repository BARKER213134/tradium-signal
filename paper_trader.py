"""Paper Trading — виртуальная торговля с AI решениями.

AI анализирует каждый сигнал, решает входить или нет,
выбирает плечо и размер. Мониторит TP/SL. Учится на результатах.
"""
import asyncio
import json
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

INITIAL_BALANCE = 1000.0
MAX_POSITIONS = 5


def _utcnow():
    return datetime.now(timezone.utc)


def _get_collections():
    from database import _get_db
    db = _get_db()
    return db.paper_trades, db.paper_stats


def get_balance() -> float:
    trades, _ = _get_collections()
    doc = trades.find_one({"_id": "state"})
    if doc:
        return doc.get("balance", INITIAL_BALANCE)
    trades.insert_one({"_id": "state", "balance": INITIAL_BALANCE, "started_at": _utcnow()})
    return INITIAL_BALANCE


def _update_balance(new_balance: float):
    trades, _ = _get_collections()
    trades.update_one({"_id": "state"}, {"$set": {"balance": round(new_balance, 2)}}, upsert=True)


def get_open_positions() -> list:
    trades, _ = _get_collections()
    return list(trades.find({"status": "OPEN"}).sort("opened_at", -1))


def get_history(limit: int = 50) -> list:
    trades, _ = _get_collections()
    return list(trades.find({"status": {"$in": ["TP", "SL", "MANUAL"]}}).sort("closed_at", -1).limit(limit))


def get_stats() -> dict:
    history = get_history(200)
    if not history:
        return {"total": 0, "wins": 0, "losses": 0, "win_rate": 0, "total_pnl": 0, "avg_pnl": 0}
    wins = sum(1 for h in history if (h.get("pnl_usdt") or 0) > 0)
    losses = len(history) - wins
    total_pnl = sum(h.get("pnl_usdt", 0) for h in history)
    return {
        "total": len(history),
        "wins": wins,
        "losses": losses,
        "win_rate": round(wins / len(history) * 100, 1),
        "total_pnl": round(total_pnl, 2),
        "avg_pnl": round(total_pnl / len(history), 2),
    }


def open_position(symbol: str, direction: str, entry: float, tp1: float, sl: float,
                   leverage: int, size_pct: float, source: str, reasoning: str) -> dict:
    """Открывает виртуальную позицию."""
    trades, _ = _get_collections()
    balance = get_balance()
    size_usdt = round(balance * size_pct / 100, 2)

    # Генерируем ID
    last = trades.find_one({"status": {"$exists": True}}, sort=[("trade_id", -1)])
    trade_id = (last.get("trade_id", 0) if last else 0) + 1

    doc = {
        "trade_id": trade_id,
        "symbol": symbol,
        "pair": symbol.replace("USDT", "/USDT"),
        "direction": direction,
        "entry": entry,
        "tp1": tp1,
        "sl": sl,
        "leverage": leverage,
        "size_usdt": size_usdt,
        "size_pct": size_pct,
        "status": "OPEN",
        "source": source,
        "ai_reasoning": reasoning,
        "ai_review": None,
        "opened_at": _utcnow(),
        "closed_at": None,
        "exit_price": None,
        "pnl_usdt": None,
        "pnl_pct": None,
    }
    trades.insert_one(doc)
    logger.info(f"Paper OPEN #{trade_id}: {symbol} {direction} ×{leverage} ${size_usdt} entry={entry}")
    return doc


def close_position(trade_id: int, exit_price: float, reason: str = "TP"):
    """Закрывает позицию и обновляет баланс."""
    trades, _ = _get_collections()
    pos = trades.find_one({"trade_id": trade_id, "status": "OPEN"})
    if not pos:
        return None

    entry = pos["entry"]
    direction = pos["direction"]
    leverage = pos.get("leverage", 1)
    size_usdt = pos.get("size_usdt", 0)

    raw_pnl_pct = ((exit_price - entry) / entry) * 100
    if direction == "SHORT":
        raw_pnl_pct = -raw_pnl_pct
    pnl_pct = round(raw_pnl_pct * leverage, 2)
    pnl_usdt = round(size_usdt * pnl_pct / 100, 2)

    trades.update_one({"trade_id": trade_id}, {"$set": {
        "status": reason,
        "exit_price": exit_price,
        "pnl_pct": pnl_pct,
        "pnl_usdt": pnl_usdt,
        "closed_at": _utcnow(),
    }})

    balance = get_balance()
    _update_balance(balance + pnl_usdt)
    logger.info(f"Paper CLOSE #{trade_id}: {pos['symbol']} {reason} PnL={pnl_pct:+.2f}% ${pnl_usdt:+.2f}")
    return {"trade_id": trade_id, "pnl_pct": pnl_pct, "pnl_usdt": pnl_usdt, "reason": reason}


def check_positions(prices: dict):
    """Проверяет открытые позиции на TP/SL."""
    closed = []
    for pos in get_open_positions():
        sym = pos.get("symbol", "")
        price = prices.get(sym)
        if not price:
            continue

        direction = pos["direction"]
        tp1 = pos.get("tp1")
        sl = pos.get("sl")

        if direction == "LONG":
            if tp1 and price >= tp1:
                r = close_position(pos["trade_id"], price, "TP")
                if r: closed.append(r)
            elif sl and price <= sl:
                r = close_position(pos["trade_id"], price, "SL")
                if r: closed.append(r)
        elif direction == "SHORT":
            if tp1 and price <= tp1:
                r = close_position(pos["trade_id"], price, "TP")
                if r: closed.append(r)
            elif sl and price >= sl:
                r = close_position(pos["trade_id"], price, "SL")
                if r: closed.append(r)

    return closed


async def ai_decide(signal_data: dict) -> dict:
    """AI решает: входить или нет. Возвращает решение."""
    import anthropic
    from config import ANTHROPIC_API_KEY, ANTHROPIC_MODEL_FAST
    from exchange import get_keltner_eth, get_eth_market_context

    balance = get_balance()
    open_pos = get_open_positions()
    stats = get_stats()
    kc = get_keltner_eth()
    eth = get_eth_market_context()

    if len(open_pos) >= MAX_POSITIONS:
        return {"enter": False, "reasoning": f"Максимум позиций ({MAX_POSITIONS}) уже открыто"}

    # Формируем контекст
    open_str = ""
    for p in open_pos:
        open_str += f"  - {p['symbol']} {p['direction']} ×{p.get('leverage',1)} entry={p['entry']}\n"

    history = get_history(10)
    hist_str = ""
    for h in history:
        r = "✅" if (h.get("pnl_usdt") or 0) > 0 else "❌"
        hist_str += f"  {r} {h['symbol']} {h['direction']} PnL={h.get('pnl_pct',0):+.1f}% src={h.get('source','')}\n"

    prompt = (
        f"Ты — AI трейдер на Paper Trading. Твоя задача — максимизировать прибыль.\n\n"
        f"ДЕПОЗИТ: ${balance:.2f}\n"
        f"ОТКРЫТО: {len(open_pos)}/{MAX_POSITIONS}\n"
        f"{open_str}"
        f"\nСТАТИСТИКА ({stats['total']} сделок):\n"
        f"  Win Rate: {stats['win_rate']}% | PnL: ${stats['total_pnl']:+.2f} | Avg: ${stats['avg_pnl']:+.2f}\n"
        f"\nПОСЛЕДНИЕ СДЕЛКИ:\n{hist_str or '  Нет истории'}\n"
        f"\nРЫНОК:\n"
        f"  Keltner ETH: {kc.get('direction','?')} ({'подтверждён' if kc.get('confirmed') else 'neutral'})\n"
        f"  ETH 1h: {eth.get('eth_1h',0):+.2f}% | BTC 1h: {eth.get('btc_1h',0):+.2f}%\n"
        f"\nНОВЫЙ СИГНАЛ:\n"
        f"  Пара: {signal_data.get('symbol','')}\n"
        f"  Направление: {signal_data.get('direction','')}\n"
        f"  Entry: {signal_data.get('entry','')}\n"
        f"  Источник: {signal_data.get('source','')}\n"
        f"  Score: {signal_data.get('score','')}\n"
        f"  Паттерн: {signal_data.get('pattern','')}\n"
        f"  KC: {signal_data.get('kc_dir',kc.get('direction',''))}\n"
        f"  Pump: Vol ×{signal_data.get('pump_vol',0)} | OI {signal_data.get('pump_oi',0):+.1f}%\n"
        f"\nПРАВИЛА:\n"
        f"  - Макс 5 позиций одновременно\n"
        f"  - Размер: 1-5% от депозита\n"
        f"  - Плечо: 1-10x\n"
        f"  - Ставь TP и SL на основе уровней\n"
        f"  - Не входи если сигнал против Keltner (когда Keltner подтверждён)\n"
        f"  - Учись на прошлых ошибках\n\n"
        f"Ответь ТОЛЬКО JSON без markdown:\n"
        f'{{"enter": true/false, "leverage": 1-10, "size_pct": 1-5, "tp1": цена, "sl": цена, "reasoning": "почему"}}'
    )

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        message = await asyncio.to_thread(
            client.messages.create,
            model=ANTHROPIC_MODEL_FAST,
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        text = message.content[0].text.strip()
        # Парсим JSON
        if text.startswith("```"):
            text = text.split("```")[1].strip()
            if text.startswith("json"):
                text = text[4:].strip()
        result = json.loads(text)
        logger.info(f"Paper AI: {signal_data.get('symbol','')} → enter={result.get('enter')} reason={result.get('reasoning','')[:50]}")
        return result
    except Exception as e:
        logger.error(f"Paper AI error: {e}")
        return {"enter": False, "reasoning": f"AI error: {e}"}


async def ai_review_trade(trade: dict) -> str:
    """AI анализирует закрытую сделку."""
    import anthropic
    from config import ANTHROPIC_API_KEY, ANTHROPIC_MODEL_FAST

    prompt = (
        f"Разбери закрытую Paper Trade сделку:\n"
        f"  {trade['symbol']} {trade['direction']} ×{trade.get('leverage',1)}\n"
        f"  Entry: {trade['entry']} → Exit: {trade.get('exit_price')}\n"
        f"  PnL: {trade.get('pnl_pct',0):+.1f}% (${trade.get('pnl_usdt',0):+.2f})\n"
        f"  Результат: {trade.get('status')}\n"
        f"  Причина входа: {trade.get('ai_reasoning','')}\n\n"
        f"Одно предложение: что можно улучшить? На русском."
    )
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        msg = await asyncio.to_thread(
            client.messages.create, model=ANTHROPIC_MODEL_FAST, max_tokens=100,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text.strip()
    except Exception:
        return ""


async def on_signal(signal_data: dict):
    """Вызывается при новом сигнале. AI решает и открывает позицию."""
    decision = await ai_decide(signal_data)
    if not decision.get("enter"):
        logger.info(f"Paper SKIP: {signal_data.get('symbol','')} — {decision.get('reasoning','')}")
        return None

    entry = signal_data.get("entry") or signal_data.get("price", 0)
    if not entry:
        return None

    tp1 = decision.get("tp1") or (entry * 1.03 if signal_data.get("direction") == "LONG" else entry * 0.97)
    sl = decision.get("sl") or (entry * 0.98 if signal_data.get("direction") == "LONG" else entry * 1.02)
    leverage = max(1, min(10, decision.get("leverage", 3)))
    size_pct = max(1, min(5, decision.get("size_pct", 2)))

    pos = open_position(
        symbol=signal_data.get("symbol", ""),
        direction=signal_data.get("direction", "LONG"),
        entry=entry,
        tp1=tp1,
        sl=sl,
        leverage=leverage,
        size_pct=size_pct,
        source=signal_data.get("source", ""),
        reasoning=decision.get("reasoning", ""),
    )
    # Алерт в Telegram
    await _send_open_alert(pos, decision)
    return pos


_bot6 = None

def _setup_bot6():
    global _bot6
    from config import BOT6_BOT_TOKEN
    if not BOT6_BOT_TOKEN:
        return
    try:
        from aiogram import Bot
        from aiogram.client.default import DefaultBotProperties
        from aiogram.enums import ParseMode
        _bot6 = Bot(token=BOT6_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    except Exception as e:
        logger.error(f"BOT6 init: {e}")


async def _send_open_alert(pos: dict, decision: dict):
    """Алерт при открытии позиции."""
    if not _bot6:
        _setup_bot6()
    if not _bot6:
        return
    from config import ADMIN_CHAT_ID

    pair = pos["symbol"].replace("USDT", "")
    dirE = "🟢" if pos["direction"] == "LONG" else "🔴"
    balance = get_balance()

    text = (
        f"📈 <b>PAPER TRADE · ОТКРЫТИЕ</b>\n"
        f"\n"
        f"<b>{pair}/USDT</b> · {dirE} <b>{pos['direction']}</b>\n"
        f"<code>{pos['symbol']}</code>\n"
        f"\n"
        f"─── Позиция ───\n"
        f"🎯 Entry: <code>{pos['entry']}</code>\n"
        f"🟢 TP1: <code>{pos['tp1']}</code>\n"
        f"🔴 SL: <code>{pos['sl']}</code>\n"
        f"⚡ Плечо: ×{pos['leverage']} | Размер: {pos['size_pct']}% (${pos['size_usdt']})\n"
        f"\n"
        f"─── AI решение ───\n"
        f"💡 {decision.get('reasoning', '')}\n"
        f"\n"
        f"💰 Баланс: ${balance:.2f} | Источник: {pos.get('source', '')}"
    )
    try:
        await _bot6.send_message(int(ADMIN_CHAT_ID), text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"BOT6 open alert: {e}")


async def _send_close_alert(trade: dict, review: str = ""):
    """Алерт при закрытии позиции."""
    if not _bot6:
        _setup_bot6()
    if not _bot6:
        return
    from config import ADMIN_CHAT_ID

    pair = trade["symbol"].replace("USDT", "")
    dirE = "🟢" if trade["direction"] == "LONG" else "🔴"
    pnl = trade.get("pnl_pct", 0)
    pnl_usd = trade.get("pnl_usdt", 0)
    status = trade.get("status", "")
    icon = "✅" if status == "TP" else "❌"
    balance = get_balance()

    text = (
        f"{icon} <b>PAPER TRADE · ЗАКРЫТИЕ · {status}</b>\n"
        f"\n"
        f"<b>{pair}/USDT</b> · {dirE} <b>{trade['direction']}</b>\n"
        f"<code>{trade['symbol']}</code>\n"
        f"\n"
        f"─── Результат ───\n"
        f"🎯 Entry: <code>{trade['entry']}</code> → Exit: <code>{trade.get('exit_price')}</code>\n"
        f"📊 PnL: <b>{pnl:+.2f}%</b> (${pnl_usd:+.2f})\n"
        f"⚡ Плечо: ×{trade.get('leverage',1)}\n"
        f"\n"
        f"─── AI анализ ───\n"
        f"📝 Причина входа: {trade.get('ai_reasoning', '')}\n"
    )
    if review:
        text += f"🔍 Разбор: {review}\n"
    text += f"\n💰 Баланс: ${balance:.2f}"

    try:
        await _bot6.send_message(int(ADMIN_CHAT_ID), text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"BOT6 close alert: {e}")


def reset_trading():
    """Сброс Paper Trading — новый депозит."""
    trades, stats = _get_collections()
    trades.delete_many({})
    stats.delete_many({})
    trades.insert_one({"_id": "state", "balance": INITIAL_BALANCE, "started_at": _utcnow()})
    logger.info("Paper Trading reset")
