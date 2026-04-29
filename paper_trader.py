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
MAX_POSITIONS = 10

# Режимы агрессивности — переключаются через UI (POST /api/paper/mode)
# Хранятся в paper_stats.mode. По умолчанию AGGRESSIVE (user request).
MODE_CONSERVATIVE = {
    "name": "conservative",
    "size_min": 2, "size_max": 5,
    "lev_min": 1,  "lev_max": 5,
    "cluster_size_bonus": 1, "top_pick_size_bonus": 1,
    # 10 позиций × max 5% = до 50% депозита в открытых позициях
}
MODE_AGGRESSIVE = {
    "name": "aggressive",
    "size_min": 3, "size_max": 15,
    "lev_min": 2,  "lev_max": 10,
    "cluster_size_bonus": 3, "top_pick_size_bonus": 2,
    # 10 позиций × max 15% = до 150%
}
MODE_HYPER = {
    "name": "hyper",
    "size_min": 5, "size_max": 25,
    "lev_min": 3,  "lev_max": 15,
    "cluster_size_bonus": 5, "top_pick_size_bonus": 3,
    # 10 позиций × max 25% = до 250%. Claude всё равно адаптивно снижает
    # размер когда уже есть открытые позиции (видит их в контексте).
}
MODE_TURBO = {
    "name": "turbo",
    "size_min": 8, "size_max": 35,
    "lev_min": 5,  "lev_max": 18,
    "cluster_size_bonus": 6, "top_pick_size_bonus": 4,
    # Размер ×2 от HYPER на типичной ST CAUTION сделке. Profit на TP1 partial
    # (+1% raw × 30%) растёт с $0.22 до ~$0.65, full TP с $2 до ~$5.
    # Риск: B3-style катастрофы потенциально глубже, V2 (low-cap lev cap 4)
    # всё ещё работает — защищает.
}

_MODES = {
    "conservative": MODE_CONSERVATIVE,
    "aggressive": MODE_AGGRESSIVE,
    "hyper": MODE_HYPER,
    "turbo": MODE_TURBO,
}


def get_mode() -> dict:
    """Текущий режим агрессивности. Дефолт — AGGRESSIVE."""
    _, stats = _get_collections()
    doc = stats.find_one({"_id": "mode"})
    name = (doc or {}).get("name", "aggressive")
    return _MODES.get(name, MODE_AGGRESSIVE)


def set_mode(name: str) -> dict:
    """Устанавливает режим: 'conservative' | 'aggressive' | 'hyper'."""
    if name not in _MODES:
        name = "aggressive"
    _, stats = _get_collections()
    stats.update_one({"_id": "mode"}, {"$set": {"name": name}}, upsert=True)
    return get_mode()


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


CLOSED_STATUSES = ["TP", "SL", "MANUAL", "BE", "BE_PLUS", "TRAIL", "AI_CLOSE", "KILL_SWITCH"]


def get_history(limit: int = 50) -> list:
    trades, _ = _get_collections()
    return list(trades.find({"status": {"$in": CLOSED_STATUSES}}).sort("closed_at", -1).limit(limit))


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


def get_rejections(limit: int = 50) -> list:
    """Последние N отказов AI от сделок (enter=false) — для UI-лога."""
    from database import _get_db
    db = _get_db()
    out = []
    try:
        for r in db.paper_rejections.find({}).sort("at", -1).limit(limit):
            at = r.get("at")
            out.append({
                "symbol": r.get("symbol"),
                "direction": r.get("direction"),
                "source": r.get("source"),
                "score": r.get("score"),
                "pattern": r.get("pattern"),
                "is_top_pick": r.get("is_top_pick"),
                "reasoning": r.get("reasoning", ""),
                "at": at.isoformat() if hasattr(at, "isoformat") else str(at or ""),
            })
    except Exception as e:
        logger.debug(f"get_rejections fail: {e}")
    return out


def get_learnings(limit: int = 100) -> list:
    """Последние ai_review уроки из закрытых сделок."""
    trades, _ = _get_collections()
    out = []
    q = {"status": {"$in": CLOSED_STATUSES}, "ai_review": {"$ne": None}}
    for t in trades.find(q).sort("closed_at", -1).limit(limit):
        out.append({
            "trade_id": t.get("trade_id"),
            "symbol": t.get("symbol"),
            "direction": t.get("direction"),
            "source": t.get("source"),
            "entry": t.get("entry"),
            "exit": t.get("exit_price"),
            "leverage": t.get("leverage"),
            "size_pct": t.get("size_pct"),
            "pnl_pct": t.get("pnl_pct"),
            "pnl_usdt": t.get("pnl_usdt"),
            "status": t.get("status"),
            "review": t.get("ai_review", ""),
            "reasoning": t.get("ai_reasoning", ""),
            "closed_at": t.get("closed_at").isoformat() if t.get("closed_at") and hasattr(t.get("closed_at"), "isoformat") else None,
        })
    return out


def get_ai_memory() -> dict:
    """Сводка уроков AI — агрегат сверху, для передачи в ai_decide() промпт.
    Заполняется refresh_ai_memory() раз в день."""
    _, stats = _get_collections()
    doc = stats.find_one({"_id": "ai_memory"})
    return doc or {"summary": "", "top_lessons": [], "updated_at": None, "based_on_trades": 0}


def clear_ai_memory() -> bool:
    """Полностью удаляет ai_memory из БД. Используется когда Claude застрял
    на неверном правиле (например, 'Vol×0 = стоп' на основе багованных данных).
    После reset AI решает без уроков до следующего refresh'а (раз в сутки или
    ручного). Новые сделки с корректными данными переформируют память."""
    _, stats = _get_collections()
    r = stats.delete_one({"_id": "ai_memory"})
    logger.info(f"[ai-memory] CLEARED (deleted={r.deleted_count})")
    return r.deleted_count > 0


async def refresh_ai_memory() -> dict:
    """Раз в день: берёт последние 50 ai_review и просит Claude сделать свод
    ключевых выводов — что AI выучил. Результат сохраняется в paper_stats.ai_memory
    и подаётся в каждый ai_decide() через промпт.
    Цель: дать пользователю прозрачность (что AI знает) + улучшить решения."""
    import anthropic
    from config import ANTHROPIC_API_KEY, ANTHROPIC_MODEL
    learnings = get_learnings(50)
    if not learnings:
        return {"summary": "Ещё нет закрытых сделок для анализа", "top_lessons": [], "based_on_trades": 0}
    reviews_text = "\n".join(
        f"  #{x['trade_id']} {x['symbol']} {x['direction']} PnL {x.get('pnl_pct',0):+.1f}% "
        f"({x.get('status','?')}) · src={x.get('source','?')} · lev×{x.get('leverage','?')} "
        f"size{x.get('size_pct','?')}% → {x.get('review','')[:180]}"
        for x in learnings
    )
    prompt = (
        f"Ты — AI трейдер, который должен извлечь ключевые уроки из истории своих "
        f"сделок на Paper Trading. Прочитай последние {len(learnings)} разборов:\n\n"
        f"{reviews_text}\n\n"
        f"Задача:\n"
        f"1. Сформулируй 3-5 ГЛАВНЫХ уроков (что работает / что нет)\n"
        f"2. Краткое резюме (1-2 предложения): 'AI выучил, что ...'\n\n"
        f"Ответь ТОЛЬКО JSON без markdown:\n"
        f'{{"summary": "текст", "top_lessons": ["урок1", "урок2", ...]}}'
    )
    try:
        from ai_client import get_ai_client
        client = get_ai_client()
        msg = await asyncio.to_thread(
            client.messages.create, model=ANTHROPIC_MODEL, max_tokens=2000,
            messages=[{"role": "user", "content": prompt}],
        )
        text = msg.content[0].text.strip()
        # Robust JSON extraction: Claude может добавить ```json fences, вступительный
        # текст или комментарии. Ищем первый { и его закрывающий }.
        if text.startswith("```"):
            text = text.split("```", 2)[1]
            if text.startswith("json"): text = text[4:]
            text = text.strip()
        # Находим JSON object даже если он в середине ответа
        start = text.find("{")
        if start != -1:
            # Ищем закрывающий } методом подсчёта скобок + игнорируем строки
            depth = 0
            in_str = False
            esc = False
            end = -1
            for i in range(start, len(text)):
                c = text[i]
                if esc:
                    esc = False
                    continue
                if c == "\\" and in_str:
                    esc = True
                    continue
                if c == '"':
                    in_str = not in_str
                    continue
                if in_str:
                    continue
                if c == "{":
                    depth += 1
                elif c == "}":
                    depth -= 1
                    if depth == 0:
                        end = i + 1
                        break
            if end != -1:
                text = text[start:end]

        try:
            data = json.loads(text)
        except json.JSONDecodeError as je:
            # Fallback: пробуем regex на summary + top_lessons
            logger.warning(f"[ai-memory] JSON parse fail: {je}. Raw: {text[:200]}")
            import re as _re
            summary_match = _re.search(r'"summary"\s*:\s*"([^"]*(?:\\.[^"]*)*)"', text, _re.S)
            lessons = _re.findall(r'"([^"]{20,})"', text)  # любые длинные строки
            data = {
                "summary": (summary_match.group(1) if summary_match else "Claude вернул неформат, см. логи")[:500],
                "top_lessons": lessons[:5] if lessons else [],
            }

        data.setdefault("summary", "")
        data.setdefault("top_lessons", [])
        data["updated_at"] = _utcnow().isoformat()
        data["based_on_trades"] = len(learnings)
        _, stats = _get_collections()
        stats.update_one({"_id": "ai_memory"}, {"$set": data}, upsert=True)
        logger.info(f"[paper.ai-memory] refreshed from {len(learnings)} trades, lessons={len(data.get('top_lessons', []))}")
        return data
    except Exception as e:
        logger.error(f"refresh_ai_memory fail: {e}")
        return {"summary": f"error: {e}", "top_lessons": [], "based_on_trades": len(learnings)}


def open_position(symbol: str, direction: str, entry: float, tp1: float, sl: float,
                   leverage: int, size_pct: float, source: str, reasoning: str) -> dict:
    """Открывает виртуальную позицию."""
    trades, _ = _get_collections()
    balance = get_balance()
    size_usdt = round(balance * size_pct / 100, 2)

    # Атомарный counter — без race при параллельном open_position.
    # Раньше find_one(sort=trade_id desc)+1 давал дубликаты при бурсте.
    from database import _get_db
    db = _get_db()
    # Инициализируем counter из max(trade_id) если ещё не существует
    if not db.counters.find_one({"_id": "paper_trades"}):
        last = trades.find_one({"trade_id": {"$exists": True}}, sort=[("trade_id", -1)])
        max_id = (last or {}).get("trade_id", 0) or 0
        db.counters.update_one(
            {"_id": "paper_trades"},
            {"$setOnInsert": {"seq": max_id}},
            upsert=True,
        )
    counter = db.counters.find_one_and_update(
        {"_id": "paper_trades"},
        {"$inc": {"seq": 1}},
        upsert=True, return_document=True,
    )
    trade_id = (counter or {}).get("seq", 1)

    doc = {
        "trade_id": trade_id,
        "symbol": symbol,
        "pair": symbol.replace("USDT", "/USDT"),
        "direction": direction,
        "entry": entry,
        "tp1": tp1,
        "sl": sl,
        "original_sl": sl,          # для истории (после breakeven/trail SL меняется)
        "leverage": leverage,
        "size_usdt": size_usdt,
        "original_size_usdt": size_usdt,   # для расчёта абсолютных долей (TP ladder)
        "size_pct": size_pct,
        "status": "OPEN",
        "source": source,
        "ai_reasoning": reasoning,
        "ai_review": None,
        # Exit management fields
        "max_favorable_pct": 0.0,   # лучший PnL % наблюдался за время позиции
        "sl_moved_to_be": False,    # SL подвинут в безубыток?
        "sl_moved_to_be_plus": False,  # SL подвинут в BE+ (плюсовую зону)
        "sl_trailing": False,       # включён ли trailing stop?
        "last_ai_review_at": None,  # timestamp последнего AI-ревью
        "exit_events": [],          # лог событий: [{at, type, old_sl, new_sl, note}]
        # TP ladder fields
        "remaining_fraction": 1.0,        # какая доля позиции ещё открыта
        "tp_ladder_hits": [],             # список сработавших ступеней ["TP1_PARTIAL", "TP2_PARTIAL"]
        "partial_closes": [],             # лог частичных: [{at, fraction, exit_price, pnl_pct, pnl_usdt, reason}]
        "realized_pnl_usdt": 0.0,         # суммарный PnL от уже закрытых частей
        "opened_at": _utcnow(),
        "closed_at": None,
        "exit_price": None,
        "pnl_usdt": None,
        "pnl_pct": None,
    }
    trades.insert_one(doc)
    logger.info(f"Paper OPEN #{trade_id}: {symbol} {direction} ×{leverage} ${size_usdt} entry={entry}")
    return doc


def _close_partial(trade_id: int, fraction: float, exit_price: float, reason: str) -> dict | None:
    """Частично закрывает позицию. fraction ∈ (0, remaining_fraction].
    Позиция остаётся OPEN, уменьшается remaining_fraction, накапливается realized_pnl_usdt.
    """
    trades, _ = _get_collections()
    pos = trades.find_one({"trade_id": trade_id, "status": "OPEN"})
    if not pos:
        return None
    current_rem = float(pos.get("remaining_fraction", 1.0))
    fraction = min(fraction, current_rem)
    if fraction <= 0.001:
        return None

    entry = pos["entry"]
    direction = pos["direction"]
    leverage = pos.get("leverage", 1)
    original_size = float(pos.get("original_size_usdt") or pos.get("size_usdt", 0))
    size_closed_usdt = original_size * fraction

    raw_pnl_pct = ((exit_price - entry) / entry) * 100
    if direction == "SHORT":
        raw_pnl_pct = -raw_pnl_pct
    pnl_pct = round(raw_pnl_pct * leverage, 2)
    pnl_usdt = round(size_closed_usdt * pnl_pct / 100, 2)

    new_rem = round(current_rem - fraction, 4)
    new_tp_hits = list(pos.get("tp_ladder_hits", [])) + [reason]
    new_partial = {
        "at": _utcnow(), "fraction": round(fraction, 4),
        "exit_price": exit_price,
        "pnl_pct": pnl_pct, "pnl_usdt": pnl_usdt,
        "reason": reason,
    }
    new_partials = list(pos.get("partial_closes", [])) + [new_partial]
    new_realized = round(float(pos.get("realized_pnl_usdt") or 0) + pnl_usdt, 2)

    trades.update_one({"trade_id": trade_id}, {"$set": {
        "remaining_fraction": new_rem,
        "tp_ladder_hits": new_tp_hits,
        "partial_closes": new_partials,
        "realized_pnl_usdt": new_realized,
    }})
    balance = get_balance()
    _update_balance(balance + pnl_usdt)
    logger.info(f"Paper PARTIAL #{trade_id}: {pos['symbol']} {reason} {fraction*100:.0f}% @ {exit_price} → ${pnl_usdt:+.2f} (cum realized=${new_realized:+.2f}, rem={new_rem:.2f})")
    return {"trade_id": trade_id, "fraction": fraction, "pnl_usdt": pnl_usdt, "reason": reason,
            "remaining": new_rem, "realized_pnl_usdt": new_realized}


def close_position(trade_id: int, exit_price: float, reason: str = "TP"):
    """Финально закрывает остаток позиции. Итоговый PnL = realized (от partial) + финальный остаток."""
    trades, _ = _get_collections()
    pos = trades.find_one({"trade_id": trade_id, "status": "OPEN"})
    if not pos:
        return None

    entry = pos["entry"]
    direction = pos["direction"]
    leverage = pos.get("leverage", 1)
    original_size = float(pos.get("original_size_usdt") or pos.get("size_usdt", 0))
    remaining = float(pos.get("remaining_fraction", 1.0))
    realized = float(pos.get("realized_pnl_usdt") or 0)

    # PnL оставшейся доли
    raw_pnl_pct = ((exit_price - entry) / entry) * 100
    if direction == "SHORT":
        raw_pnl_pct = -raw_pnl_pct
    pnl_pct_final = round(raw_pnl_pct * leverage, 2)
    pnl_usdt_remainder = round(original_size * remaining * pnl_pct_final / 100, 2)

    # Итого по всей позиции
    total_pnl_usdt = round(realized + pnl_usdt_remainder, 2)
    # pnl_pct показываем как % от оригинального размера (для совместимости с UI)
    total_pnl_pct = round(total_pnl_usdt / original_size * 100, 2) if original_size else 0

    trades.update_one({"trade_id": trade_id}, {"$set": {
        "status": reason,
        "exit_price": exit_price,
        "pnl_pct": total_pnl_pct,       # итоговый % от исходного размера
        "pnl_pct_final_leg": pnl_pct_final,  # % только по последней (оставшейся) части
        "pnl_usdt": total_pnl_usdt,
        "pnl_usdt_final_leg": pnl_usdt_remainder,
        "closed_at": _utcnow(),
        "remaining_fraction": 0,
    }})

    balance = get_balance()
    _update_balance(balance + pnl_usdt_remainder)  # только финальная часть (partial уже добавлены)
    logger.info(f"Paper CLOSE #{trade_id}: {pos['symbol']} {reason} final_leg={pnl_pct_final:+.2f}% "
                f"(rem={remaining:.2f}) total=${total_pnl_usdt:+.2f} (realized ${realized:+.2f} + leg ${pnl_usdt_remainder:+.2f})")
    try:
        from cache_utils import paper_learnings_cache
        paper_learnings_cache.invalidate()
    except Exception:
        pass
    return {"trade_id": trade_id, "pnl_pct": total_pnl_pct, "pnl_usdt": total_pnl_usdt, "reason": reason,
            "realized_pnl_usdt": realized, "final_leg_pnl_usdt": pnl_usdt_remainder}


async def close_manual(trade_id: int) -> dict | None:
    """Ручное закрытие позиции — берёт текущую цену с биржи, закрывает
    со статусом MANUAL. Возвращает {trade_id, pnl_pct, pnl_usdt} или None.
    Шлёт уведомление в BOT6 при успехе."""
    trades, _ = _get_collections()
    pos = trades.find_one({"trade_id": int(trade_id), "status": "OPEN"})
    if not pos:
        return None
    from exchange import get_prices_any
    pair = pos.get("pair") or pos["symbol"].replace("USDT", "/USDT")
    prices = await asyncio.to_thread(get_prices_any, [pair])
    cur = prices.get(pos["symbol"])
    if cur is None:
        cur = pos.get("entry", 0)
    result = close_position(int(trade_id), cur, "MANUAL")
    # ── Telegram-уведомление о ручном закрытии ──
    if result:
        try:
            updated = trades.find_one({"trade_id": int(trade_id)})
            if updated:
                await _send_close_alert(updated, "")
        except Exception as e:
            logger.warning(f"[paper-manual-close] alert fail #{trade_id}: {e}")
    return result


async def close_all_manual() -> dict:
    """Закрывает ВСЕ открытые позиции по текущим рыночным ценам.
    Возвращает {closed: [{trade_id, symbol, pnl_pct, pnl_usdt}, ...], total_pnl_usdt, total_count}.
    """
    from exchange import get_prices_any
    open_positions = get_open_positions()
    if not open_positions:
        return {"closed": [], "total_pnl_usdt": 0, "total_count": 0, "note": "no open positions"}

    # Batch запрос цен для всех уникальных пар
    pairs = list({p.get("pair") or p["symbol"].replace("USDT", "/USDT") for p in open_positions})
    prices = await asyncio.to_thread(get_prices_any, pairs)

    trades_coll, _ = _get_collections()
    results = []
    total_pnl = 0.0
    closed_docs = []  # для batch алертов после
    for pos in open_positions:
        trade_id = pos.get("trade_id")
        if not trade_id:
            continue
        sym = pos.get("symbol", "")
        cur = prices.get(sym)
        if cur is None:
            cur = pos.get("entry", 0)
        r = close_position(int(trade_id), cur, "MANUAL")
        if r:
            r["symbol"] = sym
            results.append(r)
            total_pnl += r.get("pnl_usdt", 0) or 0
            # Собираем закрытые docs для алертов
            try:
                updated = trades_coll.find_one({"trade_id": int(trade_id)})
                if updated:
                    closed_docs.append(updated)
            except Exception:
                pass
    logger.info(f"Paper CLOSE ALL: {len(results)} positions, total PnL=${total_pnl:+.2f}")
    # ── Telegram-уведомления по каждой закрытой позиции (batch) ──
    for doc in closed_docs:
        try:
            await _send_close_alert(doc, "")
        except Exception as e:
            logger.debug(f"[paper-close-all] alert fail: {e}")
    return {
        "closed": results,
        "total_pnl_usdt": round(total_pnl, 2),
        "total_count": len(results),
    }


# Параметры rule-based exit management (TP ladder + BE+ + trailing).
# Все пороги — в "raw" %, то есть без учёта leverage (процент движения цены).
#
# TP LADDER:
#   +1.0%  → закрыть 30% позиции (зафиксировать первую прибыль)
#   +2.0%  → закрыть ещё 30% (итого закрыто 60%, осталось 40%)
#   +3.0%  → SL в entry + 1% (защищаем уже большую часть прибыли)
#   +4.0%  → включаем trailing на 1.5% от максимума
#
# Это заменяет прежнюю простую логику (+1%→BE, +2%→trail). Теперь сделки
# фиксируют прибыль поэтапно — психологически устойчивее к откатам.
TP_LADDER = [
    {"at_pct": 1.0, "close_fraction": 0.30, "name": "TP1_PARTIAL"},
    {"at_pct": 2.0, "close_fraction": 0.30, "name": "TP2_PARTIAL"},
]
BE_PLUS_TRIGGER_PCT = 3.0       # при +3% raw-движения → двигаем SL вверх
BE_PLUS_OFFSET_PCT  = 1.0       # SL ставим на entry +1% (для LONG) / entry -1% (для SHORT)
TRAILING_TRIGGER_PCT = 4.0      # при +4% → trailing
TRAILING_DISTANCE_PCT = 1.5     # отступ trailing-SL от максимума


def check_positions(prices: dict):
    """Проверяет открытые позиции. На каждом тике:
      1. TP ladder: +1%/30%, +2%/30% — частичные закрытия
      2. BE+: при +3% двигаем SL на entry+1% (защита прибыли)
      3. Trailing: при +4% включаем (1.5% отступ)
      4. SL/TP hit → финальное закрытие оставшейся доли (remaining_fraction)
    """
    trades, _ = _get_collections()
    closed = []
    for pos in get_open_positions():
        sym = pos.get("symbol", "")
        price = prices.get(sym)
        if not price:
            continue

        entry = pos["entry"]
        direction = pos["direction"]
        tp1 = pos.get("tp1")
        sl = pos.get("sl")
        is_long = direction == "LONG"

        # Raw PnL % (без leverage — чистое движение цены)
        raw_pnl = ((price - entry) / entry) * 100
        if not is_long:
            raw_pnl = -raw_pnl
        max_fav = max(pos.get("max_favorable_pct", 0.0) or 0.0, raw_pnl)

        updates = {}
        events = []
        new_sl = sl
        tp_hits_done = set(pos.get("tp_ladder_hits", []))

        # ── 1. TP LADDER — частичные закрытия ──
        for step in TP_LADDER:
            if step["name"] in tp_hits_done:
                continue
            if max_fav >= step["at_pct"]:
                # используем текущую цену как exit для partial
                partial_result = _close_partial(
                    pos["trade_id"], step["close_fraction"], price, step["name"]
                )
                if partial_result:
                    closed.append({**partial_result, "symbol": sym, "partial": True})
                    tp_hits_done.add(step["name"])
                    events.append({"at": _utcnow(), "type": step["name"],
                                   "fraction": step["close_fraction"],
                                   "note": f"+{max_fav:.2f}% → closed {int(step['close_fraction']*100)}%"})

        # Перезагружаем pos после partial closes (remaining_fraction мог измениться)
        pos = trades.find_one({"trade_id": pos["trade_id"], "status": "OPEN"})
        if not pos:
            continue  # вдруг весь закрыт (не должно при 30%+30%=60%, но safe)

        # ── 2. BE+ : при +3% → SL на entry ± 1% (защищаем прибыль оставшейся доли) ──
        if (not pos.get("sl_moved_to_be_plus")) and max_fav >= BE_PLUS_TRIGGER_PCT:
            if is_long:
                be_plus_sl = entry * (1 + BE_PLUS_OFFSET_PCT / 100.0)
                if be_plus_sl > (new_sl or -1):
                    events.append({"at": _utcnow(), "type": "BE_PLUS", "old_sl": sl,
                                   "new_sl": be_plus_sl,
                                   "note": f"+{max_fav:.2f}% → SL to entry+{BE_PLUS_OFFSET_PCT}%"})
                    new_sl = be_plus_sl
                    updates["sl_moved_to_be_plus"] = True
                    updates["sl_moved_to_be"] = True  # совместимость со старой логикой
            else:
                be_plus_sl = entry * (1 - BE_PLUS_OFFSET_PCT / 100.0)
                if be_plus_sl < (new_sl or 9e12):
                    events.append({"at": _utcnow(), "type": "BE_PLUS", "old_sl": sl,
                                   "new_sl": be_plus_sl,
                                   "note": f"+{max_fav:.2f}% → SL to entry-{BE_PLUS_OFFSET_PCT}%"})
                    new_sl = be_plus_sl
                    updates["sl_moved_to_be_plus"] = True
                    updates["sl_moved_to_be"] = True

        # ── 3. TRAILING — при +4% включаем 1.5% trail ──
        if max_fav >= TRAILING_TRIGGER_PCT:
            if is_long:
                max_price = entry * (1 + max_fav / 100.0)
                trail_sl = max_price * (1 - TRAILING_DISTANCE_PCT / 100.0)
                if trail_sl > (new_sl or -1):
                    events.append({"at": _utcnow(), "type": "TRAIL", "old_sl": new_sl,
                                   "new_sl": trail_sl, "note": f"trail @ {max_price:.6g} (-{TRAILING_DISTANCE_PCT}%)"})
                    new_sl = trail_sl
                    updates["sl_trailing"] = True
            else:
                min_price = entry * (1 - max_fav / 100.0)
                trail_sl = min_price * (1 + TRAILING_DISTANCE_PCT / 100.0)
                if trail_sl < (new_sl or 9e12):
                    events.append({"at": _utcnow(), "type": "TRAIL", "old_sl": new_sl,
                                   "new_sl": trail_sl, "note": f"trail @ {min_price:.6g} (+{TRAILING_DISTANCE_PCT}%)"})
                    new_sl = trail_sl
                    updates["sl_trailing"] = True

        # Применяем updates в MongoDB
        if new_sl != sl or updates or max_fav != pos.get("max_favorable_pct", 0):
            upd = {"max_favorable_pct": max_fav, **updates}
            if new_sl != sl:
                upd["sl"] = new_sl
            if events:
                trades.update_one(
                    {"trade_id": pos["trade_id"]},
                    {"$set": upd, "$push": {"exit_events": {"$each": events}}},
                )
                for ev in events:
                    logger.info(f"Paper #{pos['trade_id']} {sym} {ev['type']}: {ev.get('note','')}")
            else:
                trades.update_one({"trade_id": pos["trade_id"]}, {"$set": upd})
            sl = new_sl

        # ── 4. SL/TP hit → финальное закрытие оставшегося ──
        # V1: при SL hit фиксируем exit_price = sl exact (без slippage за SL).
        # Это соответствует поведению live-trader на BingX с workingType=MARK_PRICE
        # (stop срабатывает по mark-цене ровно на уровне SL, не по market wick).
        # Бэктест 27-28 апр: спасает ~+11% margin loss на катастрофах вроде B3
        # (slippage 1.29% за SL → −33% pnl_pct вместо ожидаемых −22%).
        if is_long:
            if tp1 and price >= tp1:
                r = close_position(pos["trade_id"], price, "TP")
                if r: closed.append(r)
            elif sl and price <= sl:
                reason = "SL"
                if pos.get("sl_trailing"):
                    reason = "TRAIL"
                elif pos.get("sl_moved_to_be_plus"):
                    reason = "BE_PLUS"
                elif pos.get("sl_moved_to_be") and abs(price - entry) / entry * 100 < 0.2:
                    reason = "BE"
                # V1: используем sl а не price — убираем wick-slippage
                r = close_position(pos["trade_id"], sl, reason)
                if r: closed.append(r)
        else:
            if tp1 and price <= tp1:
                r = close_position(pos["trade_id"], price, "TP")
                if r: closed.append(r)
            elif sl and price >= sl:
                reason = "SL"
                if pos.get("sl_trailing"):
                    reason = "TRAIL"
                elif pos.get("sl_moved_to_be_plus"):
                    reason = "BE_PLUS"
                elif pos.get("sl_moved_to_be") and abs(price - entry) / entry * 100 < 0.2:
                    reason = "BE"
                # V1: используем sl а не price — убираем wick-slippage
                r = close_position(pos["trade_id"], sl, reason)
                if r: closed.append(r)

    return closed


async def ai_review_open_positions() -> list:
    """AI-ревью открытых позиций — вызывается раз в 30 мин из watcher.
    Claude оценивает каждую позицию и решает:
      — HOLD: держим
      — CLOSE: закрыть немедленно
      — MOVE_SL: сдвинуть SL к указанной цене
    Возвращает список применённых действий.
    """
    import anthropic
    from config import ANTHROPIC_API_KEY, ANTHROPIC_MODEL
    from exchange import get_prices_any
    from datetime import timedelta
    trades, _ = _get_collections()
    positions = get_open_positions()
    if not positions:
        return []

    pairs = [p.get("pair") or p["symbol"].replace("USDT","/USDT") for p in positions]
    try:
        prices = await asyncio.to_thread(get_prices_any, pairs)
    except Exception as e:
        logger.warning(f"ai_review prices fail: {e}")
        return []

    # Фильтр — ревью делаем не чаще чем раз в 25 мин на позицию (экономим Claude)
    actions = []
    for pos in positions:
        last_review = pos.get("last_ai_review_at")
        if last_review and (_utcnow() - last_review) < timedelta(minutes=25):
            continue
        sym = pos.get("symbol", "")
        cur = prices.get(sym)
        if not cur:
            continue
        entry = pos["entry"]
        direction = pos["direction"]
        raw_pnl = ((cur - entry) / entry) * 100
        if direction == "SHORT":
            raw_pnl = -raw_pnl
        lev = pos.get("leverage", 1)
        leveraged_pnl = raw_pnl * lev
        hours_open = (_utcnow() - pos["opened_at"]).total_seconds() / 3600

        events_str = ""
        for ev in (pos.get("exit_events") or [])[-3:]:
            events_str += f"  [{ev.get('type')}] {ev.get('note','')}\n"

        prompt = (
            f"Ты — AI трейдер. Проанализируй открытую позицию и реши что делать.\n\n"
            f"ПОЗИЦИЯ #{pos.get('trade_id')}:\n"
            f"  {sym} {direction} ×{lev}\n"
            f"  Entry: {entry} | Сейчас: {cur}\n"
            f"  PnL (raw): {raw_pnl:+.2f}% | С плечом: {leveraged_pnl:+.2f}%\n"
            f"  TP: {pos.get('tp1')} | SL: {pos.get('sl')} (original: {pos.get('original_sl')})\n"
            f"  Max favorable: +{pos.get('max_favorable_pct', 0):.2f}%\n"
            f"  SL moved to BE: {pos.get('sl_moved_to_be', False)} | Trailing: {pos.get('sl_trailing', False)}\n"
            f"  Открыта {hours_open:.1f}ч назад\n"
            f"  Source: {pos.get('source')}\n"
            f"  Reasoning (на входе): {(pos.get('ai_reasoning') or '')[:200]}\n"
            f"  Последние exit события:\n{events_str or '  (нет)'}\n"
            f"\nРЕШИ:\n"
            f"  HOLD    — держим, не трогаем\n"
            f"  CLOSE   — закрыть сейчас (если фиксируем прибыль или видим разворот)\n"
            f"  MOVE_SL — сдвинуть SL к цене X (защитить прибыль / дать буфер)\n"
            f"\nКонтекст: breakeven и trailing уже автоматически работают по правилам:\n"
            f"  +1% → SL в entry (авто)\n"
            f"  +2% → trailing -1% от max (авто)\n"
            f"Ты НУЖЕН когда: фиксировать прибыль при экстремальном движении,\n"
            f"закрыть застойную позицию, сдвинуть SL агрессивнее правил.\n"
            f"\nОтветь ТОЛЬКО JSON:\n"
            f'{{"action": "HOLD"|"CLOSE"|"MOVE_SL", "new_sl": число_если_MOVE_SL, "reasoning": "одно предложение"}}'
        )
        try:
            from ai_client import get_ai_client
            client = get_ai_client()
            msg = await asyncio.to_thread(
                client.messages.create, model=ANTHROPIC_MODEL, max_tokens=300,
                messages=[{"role": "user", "content": prompt}],
            )
            text = msg.content[0].text.strip()
            # Те же robust-парсер трюки
            if text.startswith("```"):
                text = text.split("```", 2)[1]
                if text.startswith("json"): text = text[4:]
                text = text.strip()
            s_i = text.find("{")
            e_i = text.rfind("}")
            if s_i >= 0 and e_i > s_i:
                text = text[s_i:e_i+1]
            decision = json.loads(text)
        except Exception as e:
            logger.warning(f"[ai-review] #{pos.get('trade_id')} fail: {e}")
            trades.update_one({"trade_id": pos["trade_id"]},
                              {"$set": {"last_ai_review_at": _utcnow()}})
            continue

        action = (decision.get("action") or "").upper()
        reasoning = decision.get("reasoning", "")
        logger.info(f"[ai-review] #{pos.get('trade_id')} {sym}: {action} — {reasoning[:80]}")

        if action == "CLOSE":
            r = close_position(pos["trade_id"], cur, "AI_CLOSE")
            if r: actions.append({"trade_id": pos["trade_id"], "action": "CLOSE", "reasoning": reasoning})
        elif action == "MOVE_SL":
            new_sl = decision.get("new_sl")
            if new_sl:
                is_long = direction == "LONG"
                old_sl = pos.get("sl")
                # Проверяем что новый SL разумный
                valid = False
                if is_long and new_sl < cur and new_sl > (old_sl or -1):
                    valid = True
                elif not is_long and new_sl > cur and new_sl < (old_sl or 9e12):
                    valid = True
                if valid:
                    trades.update_one(
                        {"trade_id": pos["trade_id"]},
                        {"$set": {"sl": new_sl, "last_ai_review_at": _utcnow()},
                         "$push": {"exit_events": {
                             "at": _utcnow(), "type": "AI_MOVE_SL",
                             "old_sl": old_sl, "new_sl": new_sl, "note": reasoning[:100],
                         }}},
                    )
                    actions.append({"trade_id": pos["trade_id"], "action": "MOVE_SL",
                                    "new_sl": new_sl, "reasoning": reasoning})

        trades.update_one({"trade_id": pos["trade_id"]},
                          {"$set": {"last_ai_review_at": _utcnow()}})
        # Пауза чтоб не долбить Claude API
        await asyncio.sleep(1.0)

    return actions


def get_prompt_preview() -> dict:
    """Возвращает текущий AI-промт разбитый на секции — для UI-гармошки.
    Промт динамический: чем больше закрытых сделок → тем больше уроков в
    блоке 'Память', тем умнее решения AI.

    Возвращает dict:
      {
        sections: [{title, body, type: static|dynamic}, ...],
        full_text: str,   # полный собранный промт
        memory: {summary, lessons, based_on_trades, updated_at},
        mode: {name, size, lev},
        balance: float,
      }
    """
    from exchange import get_keltner_eth, get_eth_market_context
    balance = get_balance()
    open_pos = get_open_positions()
    stats = get_stats()
    mode = get_mode()
    memory = get_ai_memory()
    try:
        kc = get_keltner_eth()
        eth = get_eth_market_context()
    except Exception:
        kc, eth = {}, {}

    # ── Секция 1: Роль + текущее состояние (dynamic — зависит от режима/баланса) ──
    section_role = (
        f"Ты — AI трейдер на Paper Trading. Твоя задача — максимизировать прибыль.\n"
        f"Режим: <b>{mode['name'].upper()}</b> (size {mode['size_min']}-{mode['size_max']}%, "
        f"lev {mode['lev_min']}-{mode['lev_max']}×)\n"
        f"\n"
        f"ДЕПОЗИТ: ${balance:.2f}\n"
        f"ОТКРЫТО: {len(open_pos)}/{MAX_POSITIONS}\n"
    )

    # ── Секция 2: Статистика (dynamic) ──
    section_stats = (
        f"СТАТИСТИКА ({stats['total']} сделок):\n"
        f"  Win Rate: {stats['win_rate']}% | "
        f"PnL: ${stats['total_pnl']:+.2f} | "
        f"Avg: ${stats['avg_pnl']:+.2f}\n"
    )

    # ── Секция 3: Память (dynamic, зависит от накопленных уроков) ──
    memory_text = ""
    if memory.get("summary"):
        memory_text = f"🧠 ТВОЯ ПАМЯТЬ (из {memory.get('based_on_trades',0)} сделок):\n"
        memory_text += f"  {memory.get('summary','')}\n"
        lessons = memory.get("top_lessons", [])
        if lessons:
            memory_text += "\n  Главные уроки:\n"
            for l in lessons[:5]:
                memory_text += f"    • {l}\n"
    else:
        memory_text = (
            "🧠 ТВОЯ ПАМЯТЬ: пока пуста.\n"
            "  (Накапливается когда Claude агрегирует closed trades — раз в сутки\n"
            "   или по кнопке 'Пересобрать сводку'. Нужно минимум 1 закрытая сделка.)"
        )

    # ── Секция 4: Знания о платформе (static — инварианты) ──
    section_knowledge = (
        "ЗНАНИЯ О ПЛАТФОРМЕ:\n"
        "  Источники сигналов:\n"
        "    📡 tradium     — DCA4 pattern_triggered\n"
        "    🚀 cryptovizor — pattern на 1h\n"
        "    ⚠️ anomaly     — многофакторная аномалия (score/15)\n"
        "    🎯 confluence  — 4-6 факторов совпали (STRONG=5+ 🔥=6)\n"
        "    💠 cluster     — 2+ источника ±8ч (NORMAL/STRONG/MEGA)\n"
        "    👑 top_pick    — double-confirm\n"
        "    🌀 supertrend  — VIP / MTF / Daily\n"
        "\n  Key Levels флаги в сигнале:\n"
        "    🌀🌀🌀 + 🔥 = ST aligned 3 TF + fresh flip → сильно\n"
        "    🏆 = VIP совпадение ±2ч → максимум\n"
        "    TP⚠️ = TP за R уровнем → риск\n"
        "    SL✅ = SL под S уровнем → защищён\n"
    )

    # ── Секция 5: Правила входа (static) ──
    section_rules = (
        f"ПРАВИЛА:\n"
        f"  - Макс {MAX_POSITIONS} позиций одновременно\n"
        f"  - Размер: {mode['size_min']}-{mode['size_max']}% от депозита\n"
        f"  - Плечо: {mode['lev_min']}-{mode['lev_max']}×\n"
        f"  - Cluster MEGA/STRONG → увеличенный size (+{mode['cluster_size_bonus']}%)\n"
        f"  - Top Pick → +{mode['top_pick_size_bonus']}% size, +30% lev\n"
        f"  - Не входи против Keltner когда он подтверждён\n"
        f"  - ПРИМЕНЯЙ уроки из своей памяти (она обновляется ежедневно\n"
        f"    на свежей статистике закрытых сделок)\n"
    )

    # ── Секция 6: Рынок (dynamic) ──
    section_market = (
        f"РЫНОК:\n"
        f"  Keltner ETH: {kc.get('direction','?')} "
        f"({'подтверждён' if kc.get('confirmed') else 'neutral'})\n"
        f"  ETH 1h: {eth.get('eth_1h',0):+.2f}% | "
        f"BTC 1h: {eth.get('btc_1h',0):+.2f}%\n"
    )

    # ── Формат ответа AI (static) ──
    section_format = (
        'Ответь ТОЛЬКО JSON без markdown:\n'
        f'{{"enter": true/false, "leverage": {mode["lev_min"]}-{mode["lev_max"]}, '
        f'"size_pct": {mode["size_min"]}-{mode["size_max"]}, '
        '"tp1": цена, "sl": цена, "reasoning": "почему"}}'
    )

    sections = [
        {"title": "🎯 Роль и текущее состояние", "body": section_role, "type": "dynamic"},
        {"title": "📊 Статистика", "body": section_stats, "type": "dynamic"},
        {"title": "🧠 Память AI", "body": memory_text, "type": "memory"},
        {"title": "📚 Знания о платформе", "body": section_knowledge, "type": "static"},
        {"title": "📏 Правила входа", "body": section_rules, "type": "rules"},
        {"title": "📈 Рыночный контекст", "body": section_market, "type": "dynamic"},
        {"title": "📤 Ожидаемый формат ответа", "body": section_format, "type": "static"},
    ]

    full_text = "\n\n".join(s["body"] for s in sections)
    return {
        "sections": sections,
        "full_text": full_text,
        "memory": {
            "summary": memory.get("summary", ""),
            "lessons": memory.get("top_lessons", []),
            "based_on_trades": memory.get("based_on_trades", 0),
            "updated_at": memory.get("updated_at"),
        },
        "mode": mode,
        "balance": balance,
        "open_positions": len(open_pos),
        "stats": stats,
    }


async def ai_decide(signal_data: dict) -> dict:
    """AI решает: входить или нет. Возвращает решение."""
    import anthropic
    from config import ANTHROPIC_API_KEY, ANTHROPIC_MODEL
    from exchange import get_keltner_eth, get_eth_market_context

    balance = get_balance()
    open_pos = get_open_positions()
    stats = get_stats()
    kc = get_keltner_eth()
    eth = get_eth_market_context()

    if len(open_pos) >= MAX_POSITIONS:
        # Тихий отказ БЕЗ Claude вызова (экономим токены) и БЕЗ записи в
        # rejections (code path до записи не доходит — early return)
        return {"enter": False, "reasoning": f"Максимум позиций ({MAX_POSITIONS}) уже открыто"}

    # ═══════════════════════════════════════════════════════════
    # HARD BLOCK: вход против SuperTrend
    # ═══════════════════════════════════════════════════════════
    # Причина: сделка ON SHORT 21.04 получила -26.5% — ST на 1h был UP
    # (направление LONG), но мы открыли SHORT и сразу получили гигантскую
    # зелёную свечу. Блокируем если:
    #   1) текущее ST-состояние на 1h противоположно направлению сделки
    #   2) был ST flip в противоположную сторону за последние 15 минут
    #      (на VIP/MTF TF-ах, т.е. 15m/1h/4h — из collection _supertrend_signals)
    symbol = signal_data.get("symbol", "")
    direction = (signal_data.get("direction") or "").upper()
    pair = signal_data.get("pair") or (symbol.replace("USDT", "/USDT") if "USDT" in symbol else symbol)

    if direction in ("LONG", "SHORT") and pair:
        # (1) Текущее ST-направление на 1h (свежий кеш, если нет — НЕ блокируем,
        # чтоб не упасть в HTTP на hot-path и не плодить ложные отказы.
        # Прогрев кеша делается в watcher._candles_prewarm_loop для топ-50 пар.)
        try:
            from supertrend import supertrend_state
            st = supertrend_state(pair, "1h", cache_only=True)
            if st and st.get("state"):
                st_dir = "LONG" if st["state"] == "UP" else "SHORT"
                if st_dir != direction:
                    return {"enter": False,
                            "reasoning": f"⛔ ST 1h = {st['state']} ({st_dir}), а сделка {direction} — против тренда"}
        except Exception as _e:
            logger.debug(f"[ST-block] supertrend_state fail for {pair}: {_e}")

        # (2) Недавний flip в противоположную сторону (за последние 15 мин)
        # по VIP/MTF/Daily — ищем в _supertrend_signals. Даже если ST-состояние
        # совпадает, свежий разворот = нестабильный тренд → блокируем.
        try:
            from database import _supertrend_signals
            from datetime import timedelta as _td
            pair_norm = pair.replace("/", "").upper()
            if pair_norm and not pair_norm.endswith("USDT"):
                pair_norm = pair_norm + "USDT"
            cutoff = _utcnow() - _td(minutes=15)
            opposite = "LONG" if direction == "SHORT" else "SHORT"
            recent = _supertrend_signals().find_one({
                "pair_norm": pair_norm,
                "direction": opposite,
                "flip_at": {"$gte": cutoff},
            })
            if recent:
                flip_tier = str(recent.get("tier", "?")).upper()
                flip_tf = recent.get("tf") or recent.get("aligned_tfs") or ""
                return {"enter": False,
                        "reasoning": f"⛔ ST {flip_tier} flip в {opposite} (TF {flip_tf}) был <15 мин назад — не открываем {direction}"}
        except Exception as _e:
            logger.debug(f"[ST-block] recent flip check fail for {pair}: {_e}")

    # ═══════════════════════════════════════════════════════════
    # MARKET PHASE — фазо-зависимые блокировки
    # ═══════════════════════════════════════════════════════════
    # Статистические блоки на основе 7-дневного окна НЕ применяем —
    # недельные цифры слишком шумны, на следующей неделе паттерны могут
    # перевернуться. Пусть AI сам оценивает каждый сигнал с учётом
    # свежей статистики (через ai_memory — обновляется ежедневно).
    #
    # Остаются только адаптивные (фазо-зависимые) блоки — они реагируют
    # на текущее состояние рынка, а не на исторические WR.
    source = (signal_data.get("source") or "").lower()

    # Фазо-зависимые — получаем текущую фазу (cached, 120с)
    try:
        import market_phase as _mp
        phase_data = _mp.get_market_phase()
        phase = phase_data.get("phase", "NEUTRAL")
    except Exception:
        phase = "NEUTRAL"

    if phase == "BEAR_TREND":
        if source == "supertrend":
            tier = (signal_data.get("tier") or signal_data.get("st_tier") or "").lower()
            if tier == "mtf" and direction == "LONG":
                # MTF LONG работает только при BTC=UP (бектест)
                return {"enter": False, "reasoning": "⛔ BEAR phase + MTF LONG: работает только при BTC=UP"}
        if source == "confluence" and direction == "LONG":
            # Confluence LONG в BEAR — оверфильтрация требуется
            score = signal_data.get("score") or 0
            if score < 5:
                return {"enter": False, "reasoning": f"⛔ BEAR phase + Confluence LONG score={score}<5: 7д-бектест avg_R -0.02"}

    if phase == "BULL_TREND":
        if source == "confluence" and direction == "SHORT":
            score = signal_data.get("score") or 0
            if score < 5:
                return {"enter": False, "reasoning": f"⛔ BULL phase + Confluence SHORT score={score}<5: против глобального тренда"}
        if source == "supertrend":
            tier = (signal_data.get("tier") or signal_data.get("st_tier") or "").lower()
            if tier in ("vip", "mtf") and direction == "SHORT":
                return {"enter": False, "reasoning": f"⛔ BULL phase + ST {tier.upper()} SHORT: против тренда"}

    if phase == "VOLATILE":
        # В whipsaw принимаем только VIP с двойным подтверждением или Cluster MEGA
        if source not in ("cluster",):
            if source == "supertrend":
                tier = (signal_data.get("tier") or signal_data.get("st_tier") or "").lower()
                aligned = signal_data.get("aligned_bots_count") or 0
                if tier != "vip" or aligned < 2:
                    return {"enter": False, "reasoning": "⛔ VOLATILE phase: только VIP с 2+ aligned_bots или Cluster MEGA"}
            elif source in ("cryptovizor", "anomaly"):
                return {"enter": False, "reasoning": f"⛔ VOLATILE phase: {source} блокируется (whipsaw)"}

    if phase == "CHOP":
        # Флет — только сильные сетапы
        if source == "supertrend":
            tier = (signal_data.get("tier") or signal_data.get("st_tier") or "").lower()
            if tier == "daily":
                return {"enter": False, "reasoning": "⛔ CHOP phase: Daily ST сигналы в диапазоне дают whipsaw"}
        if source == "confluence":
            factors = signal_data.get("factors_count") or signal_data.get("factors") or 0
            score = signal_data.get("score") or 0
            if score < 5 and factors < 5:
                return {"enter": False, "reasoning": "⛔ CHOP phase: Confluence без score≥5 или factors≥5"}

    # Формируем контекст
    open_str = ""
    for p in open_pos:
        open_str += f"  - {p['symbol']} {p['direction']} ×{p.get('leverage',1)} entry={p['entry']}\n"

    history = get_history(10)
    hist_str = ""
    for h in history:
        r = "✅" if (h.get("pnl_usdt") or 0) > 0 else "❌"
        hist_str += f"  {r} {h['symbol']} {h['direction']} PnL={h.get('pnl_pct',0):+.1f}% src={h.get('source','')}\n"

    # Cluster context — если сигнал из кластера, добавляем спец-блок
    cluster_block = ""
    is_cluster = signal_data.get("is_cluster") or signal_data.get("source") == "cluster"
    if is_cluster:
        strength = signal_data.get("cluster_strength", "NORMAL")
        srcs = signal_data.get("sources_count", 1)
        rev = signal_data.get("reversal_score", 0)
        cluster_block = (
            f"\n🔥 КЛАСТЕРНЫЙ СИГНАЛ ({strength}):\n"
            f"  {srcs} независимых источников согласны\n"
            f"  Reversal Meter: {rev:+d}\n"
            f"  Backtest WR на кластерах: 78.6%\n"
            f"  РЕКОМЕНДАЦИЯ: можно увеличить размер/плечо (×2-3 от обычного)\n"
        )
        if strength == "RISKY":
            cluster_block += "  ⚠️ НО кластер ПРОТИВ Reversal — опасно, снизь размер!\n"

    # Режим и диапазоны (aggressive vs conservative)
    mode = get_mode()
    size_min, size_max = mode["size_min"], mode["size_max"]
    lev_min, lev_max = mode["lev_min"], mode["lev_max"]

    # AI memory — сводка уроков (что AI выучил за все сделки)
    memory = get_ai_memory()
    memory_block = ""
    if memory.get("summary"):
        memory_block = (
            f"\n🧠 ТВОЯ ПАМЯТЬ (выводы из {memory.get('based_on_trades',0)} сделок):\n"
            f"  {memory.get('summary','')}\n"
        )
        lessons = memory.get("top_lessons", [])
        if lessons:
            memory_block += "  Главные уроки:\n"
            for l in lessons[:5]:
                memory_block += f"    • {l}\n"

    # Знания о системе (обновлено с учётом новых фич)
    system_knowledge = (
        "\nЗНАНИЯ О ПЛАТФОРМЕ (обновлено за последнюю неделю):\n"
        "  Источники сигналов:\n"
        "    📡 tradium     — DCA4 pattern_triggered (редко но сильно)\n"
        "    🚀 cryptovizor — pattern на 1h (молот, поглощение и т.д.)\n"
        "    ⚠️ anomaly     — многофакторная аномалия (score/15)\n"
        "    🎯 confluence  — 4-6 факторов совпали (STRONG=5+ 🔥=6)\n"
        "    💠 cluster     — 2+ источника ±8ч (NORMAL/STRONG/MEGA)\n"
        "    👑 top_pick    — double-confirm\n"
        "    🌀 supertrend  — ST flip: VIP / MTF / Daily\n"
        "\n  Key Levels (уровни S/R в описании сигнала):\n"
        "    🌀🌀🌀 + 🔥 = ST aligned на 3 TF + fresh flip → сильный вход\n"
        "    🏆 = VIP совпадение (ST flip + bot signal ±2ч) → максимальный score\n"
        "    TP за R уровнем ⚠️ → TP может не дойти\n"
        "    SL под S уровнем ✅ → защищён\n"
        "\n  Anti-cluster: если на паре конфликт LONG vs SHORT (противоречие\n"
        "    нескольких ботов) — система автоматически блокирует, ты не увидишь.\n"
        "\n  ВАЖНО: Оценивай каждый сигнал по свежим данным (ai_memory, текущая\n"
        "    фаза рынка, контекст ETH/BTC). НЕ опирайся на старые прошлонедельные\n"
        "    цифры — они могли измениться. Твоя память обновляется ежедневно.\n"
    )

    prompt = (
        f"Ты — AI трейдер на Paper Trading. Твоя задача — максимизировать прибыль.\n"
        f"Режим: <b>{mode['name'].upper()}</b> (size {size_min}-{size_max}%, lev {lev_min}-{lev_max}×)\n\n"
        f"ДЕПОЗИТ: ${balance:.2f}\n"
        f"ОТКРЫТО: {len(open_pos)}/{MAX_POSITIONS}\n"
        f"{open_str}"
        f"\nСТАТИСТИКА ({stats['total']} сделок):\n"
        f"  Win Rate: {stats['win_rate']}% | PnL: ${stats['total_pnl']:+.2f} | Avg: ${stats['avg_pnl']:+.2f}\n"
        f"\nПОСЛЕДНИЕ СДЕЛКИ:\n{hist_str or '  Нет истории'}\n"
        f"{memory_block}"
        f"{system_knowledge}"
        f"\nРЫНОК:\n"
        f"  Keltner ETH: {kc.get('direction','?')} ({'подтверждён' if kc.get('confirmed') else 'neutral'})\n"
        f"  ETH 1h: {eth.get('eth_1h',0):+.2f}% | BTC 1h: {eth.get('btc_1h',0):+.2f}%\n"
        f"{cluster_block}"
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
        f"  - Макс {MAX_POSITIONS} позиций одновременно (сейчас {len(open_pos)})\n"
        f"  - Размер: {size_min}-{size_max}% от депозита\n"
        f"  - Плечо: {lev_min}-{lev_max}×\n"
        f"  - Ставь TP и SL на основе уровней (из Key Levels / сигнала)\n"
        f"  - Не входи если сигнал против Keltner (когда Keltner подтверждён)\n"
        f"  - Cluster MEGA/STRONG — увеличенный размер\n"
        f"  - Опирайся на СВОЮ ПАМЯТЬ (обновляется ежедневно на свежих сделках),\n"
        f"    а не на статические правила — рынок меняется неделя к неделе.\n\n"
        f"Ответь ТОЛЬКО JSON без markdown:\n"
        f'{{"enter": true/false, "leverage": {lev_min}-{lev_max}, '
        f'"size_pct": {size_min}-{size_max}, "tp1": цена, "sl": цена, "reasoning": "почему"}}'
    )

    try:
        from ai_client import get_ai_client
        client = get_ai_client()
        message = await asyncio.to_thread(
            client.messages.create,
            model=ANTHROPIC_MODEL,
            max_tokens=600,  # было 300 — иногда Claude обрезал JSON на длинном reasoning
            messages=[{"role": "user", "content": prompt}],
        )
        text = message.content[0].text.strip()
        # ── Robust JSON parsing (такой же как в refresh_ai_memory) ──
        # Claude может вернуть: 1) markdown fences, 2) вступительный текст,
        # 3) JSON с кавычками внутри строк. Парсим устойчиво через подсчёт скобок.
        if text.startswith("```"):
            text = text.split("```", 2)[1]
            if text.startswith("json"): text = text[4:]
            text = text.strip()
        start = text.find("{")
        if start != -1:
            depth = 0
            in_str = False
            esc = False
            end = -1
            for i in range(start, len(text)):
                c = text[i]
                if esc:
                    esc = False
                    continue
                if c == "\\" and in_str:
                    esc = True
                    continue
                if c == '"':
                    in_str = not in_str
                    continue
                if in_str:
                    continue
                if c == "{":
                    depth += 1
                elif c == "}":
                    depth -= 1
                    if depth == 0:
                        end = i + 1
                        break
            if end != -1:
                text = text[start:end]
        try:
            result = json.loads(text)
        except json.JSONDecodeError as je:
            # Fallback: regex-парсим ключевые поля чтобы не терять решение
            logger.warning(f"[paper.ai_decide] JSON parse fail: {je}. Raw first 200: {text[:200]}")
            import re as _re
            enter_m = _re.search(r'"enter"\s*:\s*(true|false)', text, _re.I)
            lev_m = _re.search(r'"leverage"\s*:\s*(\d+)', text)
            size_m = _re.search(r'"size_pct"\s*:\s*(\d+(?:\.\d+)?)', text)
            tp_m = _re.search(r'"tp1"\s*:\s*([\d.]+)', text)
            sl_m = _re.search(r'"sl"\s*:\s*([\d.]+)', text)
            reas_m = _re.search(r'"reasoning"\s*:\s*"([^"]{0,500})', text)
            if enter_m:
                result = {
                    "enter": enter_m.group(1).lower() == "true",
                    "leverage": int(lev_m.group(1)) if lev_m else (lev_min + lev_max) // 2,
                    "size_pct": float(size_m.group(1)) if size_m else size_min,
                    "tp1": float(tp_m.group(1)) if tp_m else None,
                    "sl": float(sl_m.group(1)) if sl_m else None,
                    "reasoning": (reas_m.group(1) if reas_m else "regex-parsed") + " [regex-fallback]",
                }
            else:
                # совсем никак — отказ с объяснением
                raise je
        logger.info(f"Paper AI: {signal_data.get('symbol','')} → enter={result.get('enter')} reason={str(result.get('reasoning',''))[:60]}")
        # Записываем отказы в БД для UI-лога rejections
        if result.get("enter") is False:
            try:
                from database import _get_db
                db = _get_db()
                db.paper_rejections.insert_one({
                    "symbol": signal_data.get("symbol", ""),
                    "pair": signal_data.get("pair", ""),
                    "direction": signal_data.get("direction", ""),
                    "source": signal_data.get("source", ""),
                    "score": signal_data.get("score"),
                    "pattern": signal_data.get("pattern", ""),
                    "is_top_pick": bool(signal_data.get("is_top_pick")),
                    "is_cluster": bool(signal_data.get("is_cluster")),
                    "reasoning": str(result.get("reasoning", ""))[:800],
                    "at": _utcnow(),
                })
            except Exception as _e:
                logger.debug(f"rejection log fail: {_e}")
        return result
    except Exception as e:
        logger.error(f"Paper AI error: {e}")
        return {"enter": False, "reasoning": f"AI error: {e}"}


async def ai_review_trade(trade: dict) -> str:
    """AI анализирует закрытую сделку."""
    import anthropic
    from config import ANTHROPIC_API_KEY, ANTHROPIC_MODEL

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
        from ai_client import get_ai_client
        client = get_ai_client()
        msg = await asyncio.to_thread(
            client.messages.create, model=ANTHROPIC_MODEL, max_tokens=100,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text.strip()
    except Exception:
        return ""


def _log_rejection_sync(signal_data: dict, reason: str):
    """Пишет в paper_rejections БЕЗ AI. Sync-версия для не-async вызывающих."""
    try:
        from database import _get_db
        _get_db().paper_rejections.insert_one({
            "symbol": signal_data.get("symbol", ""),
            "pair": signal_data.get("pair", ""),
            "direction": signal_data.get("direction", ""),
            "source": signal_data.get("source", ""),
            "score": signal_data.get("score"),
            "pattern": signal_data.get("pattern", ""),
            "is_top_pick": bool(signal_data.get("is_top_pick")),
            "is_cluster": bool(signal_data.get("is_cluster")),
            "reasoning": str(reason)[:800],
            "at": _utcnow(),
        })
    except Exception:
        logger.debug("[rule-based] rejection log fail", exc_info=True)


# Tracker фоновых tasks чтобы GC не уничтожил их до завершения
_REJ_LOG_TASKS: set = set()


def _log_rejection(signal_data: dict, reason: str):
    """Async-friendly fire-and-forget вариант: не блокирует event loop при
    лаге Atlas. Если loop отсутствует (вызвано из синхронного кода) —
    падает на sync-вариант. Сохраняет ссылку на task в _REJ_LOG_TASKS до
    завершения, чтобы GC не убил task раньше времени."""
    try:
        loop = asyncio.get_running_loop()
        task = loop.create_task(asyncio.to_thread(_log_rejection_sync, signal_data, reason))
        _REJ_LOG_TASKS.add(task)
        task.add_done_callback(_REJ_LOG_TASKS.discard)
    except RuntimeError:
        _log_rejection_sync(signal_data, reason)


def _calc_position_params(signal_data: dict, phase: str, verdict: str,
                          mode_cfg: dict) -> tuple[int, int, str]:
    """Rule-based выбор size/leverage.
    Возвращает (size_pct, leverage, note).
    """
    size_min, size_max = mode_cfg["size_min"], mode_cfg["size_max"]
    lev_min, lev_max = mode_cfg["lev_min"], mode_cfg["lev_max"]
    notes = []

    # Старт — среднее
    size_pct = (size_min + size_max) / 2.0
    leverage = (lev_min + lev_max) // 2

    source = (signal_data.get("source") or "").lower()
    tier = (signal_data.get("tier") or signal_data.get("st_tier") or "").lower()

    # Бонусы за силу источника — НЕ применяем если verdict=caution
    # (каузус DYDX #58: top_pick × 1.3 plev + size+3 + CAUTION×0.5 size дал
    # плечо 12 при 1 red flag → сделка -37%. Лучше не усиливать подозрительные)
    is_cluster = signal_data.get("is_cluster") or source == "cluster"
    strength = signal_data.get("cluster_strength", "NORMAL")
    is_top_pick = bool(signal_data.get("is_top_pick"))
    is_vip = tier == "vip" or source == "supertrend_vip"
    apply_boosts = (verdict != "caution")  # на caution бустеры не даём

    if apply_boosts and is_cluster:
        try:
            from cluster_detector import get_config as _cc
            cfg = _cc()
            if strength == "MEGA":
                mult = cfg.get("strong_boost", 2.0)
                leverage = int(round(leverage * mult))
                size_pct += mode_cfg["cluster_size_bonus"]
                notes.append(f"MEGA ×{mult:.1f}")
            elif strength == "STRONG":
                mult = cfg.get("leverage_boost", 1.5)
                leverage = int(round(leverage * mult))
                size_pct += max(1, mode_cfg["cluster_size_bonus"] - 1)
                notes.append(f"STRONG ×{mult:.1f}")
            elif strength == "NORMAL":
                leverage = int(round(leverage * 1.2))
                notes.append("cluster")
        except Exception:
            pass
    elif apply_boosts and is_top_pick:
        leverage = int(round(leverage * 1.3))
        size_pct += mode_cfg["top_pick_size_bonus"]
        notes.append("TOP PICK")
    elif apply_boosts and is_vip:
        size_pct += 1
        notes.append("VIP")

    # Фазовые корректировки
    if phase == "CHOP":
        size_pct *= 0.6
        leverage = max(lev_min, int(leverage * 0.7))
        notes.append("CHOP×0.6")
    elif phase == "VOLATILE":
        size_pct *= 0.4
        leverage = max(lev_min, int(leverage * 0.5))
        notes.append("VOL×0.4")
    elif phase in ("EUPHORIA",):
        size_pct *= 0.5
        notes.append("EUPHORIA×0.5")
    elif phase == "CAPITULATION":
        # LONG на отскок — нормальный размер; SHORT мы и так блокируем ранее
        pass
    # BULL/BEAR — оставляем средний

    # CAUTION verdict — ×0.5 и по размеру, и по плечу
    # (раньше ×0.5 был только size — при top_pick boost до 12× плеча
    # это давало notional 108% депозита при 1 red flag)
    if verdict == "caution":
        size_pct *= 0.5
        leverage = max(lev_min, int(round(leverage * 0.5)))
        notes.append("CAUTION×0.5(size+lev)")

    # V2: low-cap leverage cap — на парах с 24h volume < $50M ставим leverage ≤ 4×.
    # Бэктест 27-28 апр: 21 ST-сделка на низколиквидных парах при 9× leverage давала
    # катастрофы (B3 −129, BR −62) из-за slippage 1-1.5% за SL × 9 = 13.5% доп. потерь.
    # Снижение до 4× уменьшает урон в 2.25× и даёт +160 USDT vs baseline_pure.
    try:
        from exchange import is_low_cap_pair
        pair = signal_data.get("pair") or ""
        if pair and is_low_cap_pair(pair):
            if leverage > 4:
                leverage = 4
                notes.append("LOW_CAP_LEV4")
    except Exception:
        # fail-open — не блокируем сделку из-за проблем с volume API
        pass

    # Clamp в диапазон mode
    size_pct = max(size_min, min(size_max, size_pct))
    leverage = max(lev_min, min(lev_max, leverage))

    note = " [" + " · ".join(notes) + "]" if notes else ""
    return int(round(size_pct)), int(leverage), note


async def on_signal(signal_data: dict):
    """Rule-based логика входа (без AI).

    Последовательность:
      1. Anti-cluster guard (детектор конфликтов)
      2. verified_entry.check_entry() — 8-пунктовый чек-лист
      3. Если verdict=SKIP → отказ с логом в rejections
      4. Если verdict=CAUTION и источник слабый → отказ
      5. Если GO или (CAUTION + cluster/VIP) → open_position с параметрами
         из mode + фазовых корректировок + бонусов за источник
    """
    import verified_entry as ve

    symbol = signal_data.get("symbol", "")
    pair = signal_data.get("pair") or (symbol.replace("USDT", "/USDT") if "USDT" in symbol else symbol)
    direction = (signal_data.get("direction") or "").upper()
    source = (signal_data.get("source") or "").lower()

    if not pair or direction not in ("LONG", "SHORT"):
        return None

    # ── 0. Supertrend 1h-RSI filter ──────────────────────────────
    # Исторически supertrend as-is давал PnL −172 USDT (WR 48%, 50 сделок)
    # из-за входов в "мёртвых" RSI-зонах. Бэктест 55 supertrend-сделок
    # по 1h-RSI(14, Wilder) на закрытой свече до входа:
    #   RSI 40-50: +238.25 USDT  WR 61.5%   ← best
    #   RSI 50-60:  −48.16 USDT  WR 57.7%
    #   RSI 60-70: −175.53 USDT  WR 36.4%   ← catastrophic (late LONG)
    # Отсекая LONG при RSI≥60 и SHORT при RSI≤40: PnL меняется
    # с −61 → +134 USDT (saved +195 USDT). Fail-closed: если фильтр
    # не посчитан (сеть/мало свечей) — сделку не открываем.
    if source == "supertrend":
        try:
            from exchange import get_klines_any
            # sync HTTP к Binance/BingX блокирует event loop на 200ms-2s
            # на каждый supertrend сигнал. При burst (5-10 сигналов одновременно)
            # event loop виснет → autotrading тормозит.
            _candles = await asyncio.to_thread(get_klines_any, pair, "1h", 50) or []
            _closes = [c["c"] for c in _candles[:-1]]  # пропустить незакрытую
            if len(_closes) < 16:
                raise ValueError(f"мало свечей для RSI: {len(_closes)}")
            _avg_g = sum(max(_closes[i] - _closes[i-1], 0.0) for i in range(1, 15)) / 14.0
            _avg_l = sum(max(_closes[i-1] - _closes[i], 0.0) for i in range(1, 15)) / 14.0
            for _i in range(15, len(_closes)):
                _ch = _closes[_i] - _closes[_i-1]
                _avg_g = (_avg_g * 13 + max(_ch, 0.0)) / 14.0
                _avg_l = (_avg_l * 13 + max(-_ch, 0.0)) / 14.0
            rsi_1h = 100.0 if _avg_l == 0 else 100.0 - 100.0 / (1 + _avg_g / _avg_l)
        except Exception as e:
            logger.warning(f"supertrend RSI filter error {symbol}: {e}")
            _log_rejection(signal_data, f"[RSI-FILTER ERROR] supertrend RSI calc failed: {e}")
            return None

        if direction == "LONG" and rsi_1h >= 60:
            logger.info(f"Paper SKIP (supertrend RSI): {symbol} LONG 1h-RSI={rsi_1h:.1f}≥60")
            _log_rejection(signal_data, f"[RSI-FILTER] supertrend LONG: 1h-RSI={rsi_1h:.1f}≥60 (late entry, бэктест: WR 36% / -175 USDT)")
            return None
        if direction == "SHORT" and rsi_1h <= 40:
            logger.info(f"Paper SKIP (supertrend RSI): {symbol} SHORT 1h-RSI={rsi_1h:.1f}≤40")
            _log_rejection(signal_data, f"[RSI-FILTER] supertrend SHORT: 1h-RSI={rsi_1h:.1f}≤40 (falling knife)")
            return None
        logger.info(f"Paper ACCEPT (supertrend RSI): {symbol} {direction} 1h-RSI={rsi_1h:.1f}")

    # ── 1. Anti-cluster guard ──
    try:
        from anti_cluster_detector import detect_conflict
        conflict = detect_conflict(pair, None, window_h=4)
        if conflict["has_conflict"] and conflict["severity"] in ("strong", "nuclear"):
            logger.info(f"Paper SKIP (anti-cluster): {symbol} {direction} — "
                        f"CONFLICT {conflict['severity']}")
            _log_rejection(signal_data,
                           f"[ANTI-CLUSTER] severity={conflict['severity']} · "
                           f"LONG-вес {conflict['long_weight']} vs SHORT-вес {conflict['short_weight']} · "
                           f"конфликт сигналов на паре")
            return None
    except Exception:
        logger.debug("[paper] anti-cluster check fail", exc_info=True)

    # ── 2. Max positions ──
    open_pos = get_open_positions()
    if len(open_pos) >= MAX_POSITIONS:
        logger.info(f"Paper SKIP (max): {symbol} — {MAX_POSITIONS} открыто")
        return None  # тихий отказ без rejections-лога

    # Дубль на этой паре?
    pair_norm = pair.replace("/", "").upper()
    if not pair_norm.endswith("USDT"):
        pair_norm += "USDT"
    if any((p.get("symbol") or "").upper() == pair_norm for p in open_pos):
        _log_rejection(signal_data, f"[DUPLICATE] уже есть открытая позиция на {pair_norm}")
        return None

    # ── 3. Entry Checker ──
    # Timeout 30s: внутри check_entry sync HTTP к Binance (ST 1h state).
    # Без таймаута зависал hot path on_signal на 60+ сек при лагах сети.
    try:
        check = await asyncio.wait_for(
            asyncio.to_thread(ve.check_entry, pair, direction, signal_data),
            timeout=30.0,
        )
    except asyncio.TimeoutError:
        _log_rejection(signal_data, "[ENTRY-CHECK TIMEOUT] check_entry >30s — пропуск")
        return None
    except Exception as _e:
        logger.exception(f"[paper on_signal] check_entry crashed: {_e}")
        _log_rejection(signal_data, f"[ENTRY-CHECK CRASH] {_e}")
        return None
    if not check.get("ok"):
        _log_rejection(signal_data, f"[ENTRY-CHECK ERROR] {check.get('error','?')}")
        return None

    verdict = check.get("verdict")
    counts = check.get("counts", {})
    summary = check.get("summary", "")

    # Готовим читаемый список bad/warn чеков с конкретикой
    bad_full = [f"{c['name']}: {c.get('comment', '')}".strip()
                for c in check.get("checks", []) if c.get("status") == "bad"]
    warn_full = [f"{c['name']}: {c.get('comment', '')}".strip()
                 for c in check.get("checks", []) if c.get("status") == "warn"]

    if verdict == "skip":
        bad_str = " | ".join(bad_full) if bad_full else "—"
        _log_rejection(signal_data,
                       f"[ENTRY-CHECK SKIP] src={source} {summary} → BAD: {bad_str}")
        logger.info(f"Paper SKIP (rule): {symbol} {direction} — {summary}")
        return None

    if verdict == "caution":
        # CAUTION — разрешаем для прибыльных источников (по бэктесту 48h):
        #   cryptovizor: 245 sig, WR 36.7%, +167R ✅
        #   confluence:  201 sig, WR 33.7%, +79R ✅
        #   cluster:     6 sig,   WR 60%,   +1.7R ✅
        #   tradium:     9 sig,   WR 100%,  +9R ✅
        # Бэктест cap=10 за 27-29 апр (725 сигналов): supertrend все tier'ы
        # дают +$153 на 44 сделках (avg +$3.48, WR 61%). Поэтому пропускаем
        # ВСЕ supertrend (vip/mtf/daily) в CAUTION — старый блок -83R
        # перекрывался плохой ST UNK при отвалившемся API.
        is_strong = (
            source in ("cluster", "cryptovizor", "confluence", "tradium",
                       "supertrend", "supertrend_vip", "supertrend_mtf",
                       "supertrend_daily") or
            bool(signal_data.get("is_top_pick"))
        )

        # ZERO_RED исключение — разрешаем CAUTION с 0 красных флагов
        # (только warnings, без блокеров).
        zero_red = (counts.get("bad", 1) == 0)

        if not (is_strong or zero_red):
            bad_str = " | ".join(bad_full) if bad_full else "—"
            warn_str = " | ".join(warn_full[:3]) if warn_full else "—"
            _log_rejection(signal_data,
                           f"[ENTRY-CHECK CAUTION] src={source} not in whitelist · "
                           f"{summary} · BAD: {bad_str} · WARN: {warn_str}")
            logger.info(f"Paper SKIP (caution, src={source}): {symbol} {direction}")
            return None
        if zero_red and not is_strong:
            logger.info(f"Paper ALLOW (CAUTION zero-red, src={source}): {symbol} {direction}")

    # ── 4. Параметры позиции ──
    sig = check.get("signal", {})
    market = check.get("market", {})
    phase = market.get("phase", "NEUTRAL")

    entry = sig.get("entry") or signal_data.get("entry") or signal_data.get("price")
    tp1 = sig.get("tp1")
    sl = sig.get("sl")
    if not (entry and tp1 and sl):
        _log_rejection(signal_data,
                       f"[NO-LEVELS] entry={entry} tp={tp1} sl={sl} — сигнал без полных уровней")
        return None

    mode_cfg = get_mode()
    size_pct, leverage, param_note = _calc_position_params(signal_data, phase, verdict, mode_cfg)

    # Reasoning для истории
    emoji = {"go": "✅", "caution": "⚠"}.get(verdict, "?")
    reasoning = (f"{emoji} Rule-based {verdict.upper()} · {phase} · "
                 f"{counts.get('ok',0)}OK/{counts.get('warn',0)}w/{counts.get('bad',0)}b · "
                 f"{summary}{param_note}")

    # ── 5. Открытие ──
    pos = open_position(
        symbol=symbol,
        direction=direction,
        entry=entry,
        tp1=tp1,
        sl=sl,
        leverage=leverage,
        size_pct=size_pct,
        source=source or "?",
        reasoning=reasoning,
    )
    # Alert в BOT6
    try:
        await _send_open_alert(pos, {"reasoning": reasoning})
    except Exception:
        logger.debug("[paper] open alert fail", exc_info=True)
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
    # Иконка по статусу + прибыльности
    status_icon = {
        "TP": "✅",           # Take Profit
        "SL": "❌",           # Stop Loss (убыток)
        "BE": "🛡️",          # Breakeven (около нуля)
        "TRAIL": "🎯",        # Trailing SL (зафиксировали прибыль)
        "AI_CLOSE": "🧠",     # AI решил закрыть
        "MANUAL": "✋",       # Закрыл руками
        "KILL_SWITCH": "⛔",  # Kill switch
    }
    # Для TRAIL/BE/AI_CLOSE — цвет по факту прибыли
    icon = status_icon.get(status, "❌")
    if status in ("TRAIL", "AI_CLOSE", "MANUAL") and pnl > 0:
        icon = status_icon.get(status, "✅")
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


def reset_trading(initial_balance: float = None):
    """Сброс Paper Trading. Если initial_balance задан — используем его,
    иначе INITIAL_BALANCE (1000). Удаляет историю и открытые позиции."""
    trades, stats = _get_collections()
    trades.delete_many({})
    stats.delete_many({})
    amount = float(initial_balance) if initial_balance is not None else INITIAL_BALANCE
    trades.insert_one({
        "_id": "state",
        "balance": amount,
        "initial_balance": amount,
        "started_at": _utcnow(),
    })
    logger.info(f"Paper Trading reset: balance = ${amount}")


def set_balance(new_balance: float):
    """Установить произвольный баланс БЕЗ сброса истории — для перехода
    на реальную торговлю с конкретной суммой. Сохраняет все сделки.
    """
    trades, _ = _get_collections()
    new_amount = float(new_balance)
    trades.update_one(
        {"_id": "state"},
        {"$set": {"balance": round(new_amount, 2),
                  "initial_balance": round(new_amount, 2),
                  "balance_set_at": _utcnow()}},
        upsert=True,
    )
    logger.info(f"Paper balance set to ${new_amount} (history preserved)")
    return new_amount
