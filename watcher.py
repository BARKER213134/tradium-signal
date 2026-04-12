"""Фоновый мониторинг цен: при достижении DCA #4 отправляет сигнал в бот."""
import asyncio
import logging
import os
from database import utcnow

from aiogram.types import FSInputFile

from database import SessionLocal, Signal, log_event


def _broadcast(event: str, data: dict | None = None):
    """Тонкая обёртка: публикуем в WebSocket если admin уже импортирован."""
    try:
        from admin import broadcast_event
        broadcast_event(event, data)
    except Exception:
        pass
from exchange import get_prices as _sync_get_prices, get_klines as _sync_get_klines, get_prices_any as _sync_get_prices_any, get_klines_any as _sync_get_klines_any
from patterns import detect_patterns


async def get_prices(pairs):
    return await asyncio.to_thread(_sync_get_prices, pairs)


async def get_klines(pair, timeframe, limit=50):
    return await asyncio.to_thread(_sync_get_klines, pair, timeframe, limit)


async def get_prices_any(pairs):
    return await asyncio.to_thread(_sync_get_prices_any, pairs)


async def get_klines_any(pair, timeframe, limit=50):
    return await asyncio.to_thread(_sync_get_klines_any, pair, timeframe, limit)

logger = logging.getLogger(__name__)

POLL_INTERVAL = 30  # секунд (было 15, снижаем нагрузку)

_bot = None
_bot2 = None
_bot4 = None
_admin_chat_id = None


def setup(bot, admin_chat_id: int, bot2=None, bot3=None, bot4=None):
    global _bot, _bot2, _bot4, _admin_chat_id
    _bot = bot
    _bot2 = bot2
    _bot4 = bot4
    _admin_chat_id = admin_chat_id


def _resolve_chart(p: str) -> str | None:
    if not p:
        return None
    if os.path.isabs(p) and os.path.exists(p):
        return p
    base = os.path.dirname(os.path.abspath(__file__))
    cand = os.path.normpath(os.path.join(base, p.lstrip("./\\")))
    return cand if os.path.exists(cand) else None


def _eth_line() -> str:
    """Одна строка с ETH/BTC контекстом для Telegram."""
    try:
        from exchange import get_eth_market_context
        ctx = get_eth_market_context()
        eth = ctx.get("eth_1h", 0)
        btc = ctx.get("btc_1h", 0)
        eb = ctx.get("eth_btc", "—")
        eth_e = "🟢" if eth >= 0 else "🔴"
        btc_e = "🟢" if btc >= 0 else "🔴"
        return f"\n📊 ETH {eth_e}{eth:+.2f}% · BTC {btc_e}{btc:+.2f}% · ETH/BTC {eb}"
    except Exception:
        return ""


def _fmt_trend(trend: str | None) -> str:
    """GRRRR → 🟢🔴🔴🔴🔴"""
    if not trend:
        return "—"
    return "".join("🟢" if c == "G" else "🔴" for c in trend[:5])


def _dca_reached(direction: str, current: float, dca4: float) -> bool:
    if direction in ("LONG", "BUY"):
        return current <= dca4
    if direction in ("SHORT", "SELL"):
        return current >= dca4
    return False


async def _send_dca4_alert(signal: Signal, current_price: float):
    if not _bot or not _admin_chat_id:
        return
    is_long = signal.direction in ("LONG", "BUY")
    dir_emoji = "🟢" if is_long else "🔴"
    dir_label = "LONG" if is_long else "SHORT"
    pair = (signal.pair or "—").replace("/USDT", "")

    tp_line = f"<code>{signal.tp1}</code>"
    if signal.tp_percent:
        tp_line += f"  <code>+{signal.tp_percent}%</code>"
    sl_line = f"<code>{signal.sl}</code>"
    if signal.sl_percent:
        sl_line += f"  <code>-{signal.sl_percent}%</code>"

    ai_block = ""
    if getattr(signal, "ai_score", None) is not None:
        score = signal.ai_score
        emoji_ai = "🟢" if score >= 70 else "🟡" if score >= 40 else "🔴"
        ai_block = (
            f"\n{emoji_ai} <b>AI:</b> {score}/100"
        )
        if signal.ai_verdict:
            ai_block += f" · {signal.ai_verdict}"

    tp_disp = f"{signal.tp1}"
    if signal.tp_percent:
        tp_disp += f" (+{signal.tp_percent}%)"
    sl_disp = f"{signal.sl}"
    if signal.sl_percent:
        sl_disp += f" (-{signal.sl_percent}%)"

    text = (
        f"{dir_emoji} <b>DCA #4 ДОСТИГНУТ</b>\n"
        f"\n"
        f"<b>{pair}/USDT</b> · {signal.timeframe or '—'} · {dir_label}\n"
        f"\n"
        f"Entry: <code>{signal.entry}</code>\n"
        f"DCA #4: <code>{signal.dca4}</code> ⚡\n"
        f"Сейчас: <code>{current_price}</code>\n"
        f"\n"
        f"🎯 TP: <code>{tp_disp}</code>\n"
        f"🛑 SL: <code>{sl_disp}</code>\n"
        f"⚖️ R:R: {signal.risk_reward or '—'}"
        f"{ai_block}\n"
        f"\n"
        f"⏳ Ждём паттерн подтверждения"
        f"{_eth_line()}"
    )
    chart_path = _resolve_chart(signal.chart_path)
    try:
        if chart_path:
            await _bot.send_photo(
                _admin_chat_id,
                photo=FSInputFile(chart_path),
                caption=text,
                parse_mode="HTML",
            )
        else:
            await _bot.send_message(_admin_chat_id, text, parse_mode="HTML")
        logger.info(f"Отправлен DCA4-алерт для #{signal.id} {signal.pair}")
    except Exception as e:
        logger.error(f"Ошибка отправки DCA4 #{signal.id}: {e}")


async def _send_pattern_alert(signal: Signal, pattern: str, current_price: float):
    if not _bot or not _admin_chat_id:
        return
    is_long = signal.direction in ("LONG", "BUY")
    dir_emoji = "🟢" if is_long else "🔴"
    dir_label = "LONG" if is_long else "SHORT"
    pair = (signal.pair or "—").replace("/USDT", "")

    tp_line = f"<code>{signal.tp1}</code>"
    if signal.tp_percent:
        tp_line += f"  <code>+{signal.tp_percent}%</code>"
    sl_line = f"<code>{signal.sl}</code>"
    if signal.sl_percent:
        sl_line += f"  <code>-{signal.sl_percent}%</code>"

    text = (
        f"🚀 <b>ВХОД ПОДТВЕРЖДЁН</b>\n"
        f"\n"
        f"<b>{pair}/USDT</b> · {signal.timeframe or '—'} · {dir_emoji} {dir_label}\n"
        f"\n"
        f"Паттерн: <b>{pattern}</b>\n"
        f"Вход: <code>{current_price}</code>\n"
        f"\n"
        f"🎯 TP: <code>{signal.tp1}</code>\n"
        f"🛑 SL: <code>{signal.sl}</code>\n"
        f"⚖️ R:R: {signal.risk_reward or '—'}\n"
        f"\n"
        f"⚡ Открывать позицию"
    )
    chart_path = _resolve_chart(signal.chart_path)
    try:
        if chart_path:
            await _bot.send_photo(
                _admin_chat_id,
                photo=FSInputFile(chart_path),
                caption=text,
                parse_mode="HTML",
            )
        else:
            await _bot.send_message(_admin_chat_id, text, parse_mode="HTML")
        logger.info(f"Отправлен pattern-алерт для #{signal.id} {signal.pair} ({pattern})")
    except Exception as e:
        logger.error(f"Ошибка отправки pattern #{signal.id}: {e}")


async def _check_dca4(db):
    """Этап 1: СЛЕЖУ → ОТКРЫТ при касании DCA #4."""
    signals = (
        db.query(Signal)
        .filter(Signal.status == "СЛЕЖУ")
        .filter(Signal.dca4_triggered == False)
        .filter(Signal.dca4 != None)
        .filter(Signal.pair != None)
        .all()
    )
    if not signals:
        return
    pairs = list({s.pair for s in signals})
    prices = await get_prices(pairs)
    for s in signals:
        norm = s.pair.replace("/", "").replace("-", "").upper()
        current = prices.get(norm)
        if current is None:
            continue
        if _dca_reached(s.direction, current, s.dca4):
            await _send_dca4_alert(s, current)
            s.dca4_triggered = True
            s.dca4_triggered_at = utcnow()
            s.status = "ОТКРЫТ"
            s.is_forwarded = True
            s.forwarded_at = utcnow()
            db.commit()
            log_event(
                s.id, "dca4_hit", price=current,
                data={"dca4": s.dca4, "direction": s.direction, "pair": s.pair},
                message=f"DCA #4 достигнут @ {current}",
            )
            _broadcast("signal_update", {"id": s.id, "status": "ОТКРЫТ"})


async def _check_patterns(db):
    """Этап 2: ОТКРЫТ → ПАТТЕРН при обнаружении подтверждающего паттерна."""
    signals = (
        db.query(Signal)
        .filter(Signal.status == "ОТКРЫТ")
        .filter(Signal.pattern_triggered == False)
        .filter(Signal.pair != None)
        .all()
    )
    for s in signals:
        candles = await get_klines(s.pair, s.timeframe or "1h", limit=30)
        if not candles or len(candles) < 3:
            continue
        detected = detect_patterns(candles, s.direction or "LONG")
        if not detected:
            continue
        pattern = detected[0]
        current = candles[-1]["c"]
        await _send_pattern_alert(s, pattern, current)
        s.pattern_triggered = True
        s.pattern_name = pattern
        s.pattern_triggered_at = utcnow()
        s.pattern_price = current
        s.status = "ПАТТЕРН"
        db.commit()
        log_event(
            s.id, "pattern_detected", price=current,
            data={"pattern": pattern, "pair": s.pair, "direction": s.direction},
            message=f"Паттерн {pattern} подтвердил вход",
        )
        _broadcast("signal_update", {"id": s.id, "status": "ПАТТЕРН"})


async def _send_close_alert(signal: Signal, result: str, exit_price: float, pnl: float):
    if not _bot or not _admin_chat_id:
        return
    is_tp = result == "TP"
    emoji = "🎯" if is_tp else "🛑"
    label = "TAKE PROFIT" if is_tp else "STOP LOSS"
    pnl_sign = "+" if pnl >= 0 else ""
    pair = (signal.pair or "—").replace("/USDT", "")
    outcome = "✅ ПРИБЫЛЬ" if is_tp else "❌ УБЫТОК"

    text = (
        f"{emoji} <b>{label}</b>\n"
        f"\n"
        f"<b>{pair}/USDT</b> · {signal.timeframe or '—'} · {signal.direction}\n"
        f"\n"
        f"Entry: <code>{signal.entry}</code>\n"
        f"Exit: <code>{exit_price}</code>\n"
        f"PnL: <b>{pnl_sign}{pnl:.2f}%</b>\n"
    )
    if signal.pattern_name:
        text += f"Паттерн: {signal.pattern_name}\n"
    text += f"\n{outcome}"
    try:
        await _bot.send_message(_admin_chat_id, text, parse_mode="HTML")
        logger.info(f"Отправлен {result}-алерт для #{signal.id} PnL={pnl:.2f}%")
    except Exception as e:
        logger.error(f"Ошибка отправки close #{signal.id}: {e}")


def _tp_sl_hit(direction: str, current: float, tp: float | None, sl: float | None):
    """Возвращает ('TP', price) / ('SL', price) / (None, None)."""
    if direction in ("LONG", "BUY"):
        if tp is not None and current >= tp:
            return "TP", tp
        if sl is not None and current <= sl:
            return "SL", sl
    elif direction in ("SHORT", "SELL"):
        if tp is not None and current <= tp:
            return "TP", tp
        if sl is not None and current >= sl:
            return "SL", sl
    return None, None


async def _check_tp_sl(db, allowed_ids: set[int] | None = None):
    """Этап 3: ОТКРЫТ/ПАТТЕРН → TP/SL при достижении цели или стопа.

    Если передан allowed_ids — проверяем только сигналы из этого множества
    (grace period для свежеоткрытых на текущем тике).
    """
    q = (
        db.query(Signal)
        .filter(Signal.status.in_(["ОТКРЫТ", "ПАТТЕРН"]))
        .filter(Signal.pair != None)
    )
    if allowed_ids is not None:
        if not allowed_ids:
            return
        q = q.filter(Signal.id.in_(allowed_ids))
    signals = q.all()
    if not signals:
        return
    pairs = list({s.pair for s in signals})
    prices = await get_prices(pairs)
    for s in signals:
        norm = s.pair.replace("/", "").replace("-", "").upper()
        current = prices.get(norm)
        if current is None or s.entry is None:
            continue
        result, exit_price = _tp_sl_hit(s.direction, current, s.tp1, s.sl)
        if not result:
            continue
        raw = ((exit_price - s.entry) / s.entry) * 100
        pnl = -raw if s.direction in ("SHORT", "SELL") else raw
        await _send_close_alert(s, result, exit_price, pnl)
        s.status = result
        s.result = result
        s.exit_price = exit_price
        s.pnl_percent = pnl
        s.closed_at = utcnow()
        db.commit()
        log_event(
            s.id, f"{result.lower()}_hit", price=exit_price,
            data={
                "result": result, "entry": s.entry, "exit": exit_price,
                "pnl_percent": pnl, "pair": s.pair, "direction": s.direction,
            },
            message=f"{result} @ {exit_price}, PnL {pnl:+.2f}%",
        )
        _broadcast("signal_update", {"id": s.id, "status": result, "pnl": pnl})


async def _retry_failed_ai(db):
    """Периодически:
    - перезапускает analyze_chart для сигналов с has_chart=True но dca4=None
    - считает ai_score для сигналов где dca4 есть, но ai_score=None
    Счётчик попыток хранится в events (type=ai_retry)."""
    from ai_analyzer import analyze_chart, analyze_signal_quality

    AI_RETRY_MAX = 3

    # 1) Сигналы без dca4 — повторный analyze_chart
    signals = (
        db.query(Signal)
        .filter(Signal.status == "СЛЕЖУ")
        .filter(Signal.is_filtered == False)
        .filter(Signal.has_chart == True)
        .filter(Signal.dca4 == None)
        .filter(Signal.chart_path != None)
        .limit(3)
        .all()
    )

    # 2) Сигналы с dca4 но без ai_score — досчитываем оценку
    need_score = (
        db.query(Signal)
        .filter(Signal.has_chart == True)
        .filter(Signal.chart_path != None)
        .filter(Signal.dca4 != None)
        .filter(Signal.ai_score == None)
        .limit(2)
        .all()
    )
    for s in need_score:
        chart_path = _resolve_chart(s.chart_path)
        if not chart_path:
            continue
        try:
            logger.info(f"AI score catch-up #{s.id}")
            q = await analyze_signal_quality(chart_path, {
                "pair": s.pair, "direction": s.direction, "timeframe": s.timeframe,
                "entry": s.entry, "sl": s.sl, "tp1": s.tp1, "dca4": s.dca4,
                "risk_reward": s.risk_reward, "trend": s.trend,
            })
            if q and "score" in q:
                s.ai_score = q["score"]
                s.ai_confidence = q.get("confidence")
                s.ai_reasoning = q.get("reasoning")
                s.ai_risks = q.get("risks") or []
                s.ai_verdict = q.get("verdict")
                db.commit()
                log_event(s.id, "ai_scored",
                    data={"score": s.ai_score, "verdict": s.ai_verdict},
                    message=f"AI {s.ai_score}/100 ({s.ai_verdict})")
        except Exception as e:
            logger.error(f"score catch-up #{s.id}: {e}")

    if not signals:
        return

    for s in signals:
        # Проверяем количество попыток через events
        from database import _events
        retry_count = _events().count_documents({
            "signal_id": s.id, "type": "ai_retry",
        })
        if retry_count >= AI_RETRY_MAX:
            continue

        logger.info(f"AI retry #{retry_count + 1}/{AI_RETRY_MAX} для сигнала #{s.id}")
        chart_path = _resolve_chart(s.chart_path)
        if not chart_path:
            continue

        chart_data = await analyze_chart(chart_path)
        log_event(
            s.id, "ai_retry",
            data={
                "attempt": retry_count + 1,
                "success": bool(chart_data.get("dca4")),
                "error": chart_data.get("_error"),
            },
            message=f"AI retry #{retry_count + 1}",
        )

        if chart_data.get("dca4"):
            def _to_float(v):
                try: return float(v) if v is not None else None
                except Exception: return None

            s.dca1 = _to_float(chart_data.get("dca1"))
            s.dca2 = _to_float(chart_data.get("dca2"))
            s.dca3 = _to_float(chart_data.get("dca3"))
            s.dca4 = _to_float(chart_data.get("dca4"))
            if not s.tp1:
                s.tp1 = _to_float(chart_data.get("tp1"))
            if not s.sl:
                s.sl = _to_float(chart_data.get("sl"))
            if not s.entry:
                s.entry = _to_float(chart_data.get("entry"))
            db.commit()
            log_event(
                s.id, "ai_recovered",
                data={"dca4": s.dca4},
                message=f"AI восстановил DCA4={s.dca4}",
            )


async def _filter_stuck(db):
    """Сигналы СЛЕЖУ без dca4/entry/tp1/sl никогда не смогут продвинуться —
    помечаем их как отфильтрованные, чтобы не висели в UI вечно."""
    stuck = (
        db.query(Signal)
        .filter(Signal.status == "СЛЕЖУ")
        .filter(Signal.is_filtered == False)
        .filter((Signal.dca4 == None) | (Signal.entry == None) | (Signal.tp1 == None) | (Signal.sl == None))
        .all()
    )
    for s in stuck:
        missing = []
        if s.dca4 is None: missing.append("dca4")
        if s.entry is None: missing.append("entry")
        if s.tp1 is None: missing.append("tp1")
        if s.sl is None: missing.append("sl")
        s.is_filtered = True
        s.filter_reason = "missing: " + ", ".join(missing)
    if stuck:
        db.commit()
        logger.info(f"Отфильтровано {len(stuck)} застрявших сигналов")


async def _check_once():
    """Порядок важен: фильтрация → DCA4 → TP/SL (для уже открытых ранее)
    → pattern → DCA4 заново не нужно. Grace period: только что открытые
    сигналы (в этом тике) не проверяются на TP/SL — отложим до следующего тика."""
    db = SessionLocal()
    try:
        print("[WATCHER] _check_once start", flush=True)
        try:
            await asyncio.wait_for(_retry_failed_ai(db), timeout=30)
        except asyncio.TimeoutError:
            print("[WATCHER] _retry_failed_ai TIMEOUT", flush=True)
        except Exception as e:
            print(f"[WATCHER] _retry_failed_ai ERROR: {e}", flush=True)
        try:
            await asyncio.wait_for(_filter_stuck(db), timeout=10)
        except (asyncio.TimeoutError, Exception) as e:
            print(f"[WATCHER] _filter_stuck: {e}", flush=True)

        # Снимок id до _check_dca4 — чтобы не проверять TP/SL на свежеоткрытых
        opened_before = {
            s.id for s in db.query(Signal.id, Signal.status)
            .filter(Signal.status.in_(["ОТКРЫТ", "ПАТТЕРН"]))
            .all()
        }

        for step_name, step_fn in [
            ("dca4", lambda: _check_dca4(db)),
            ("patterns", lambda: _check_patterns(db)),
            ("tp_sl", lambda: _check_tp_sl(db, allowed_ids=opened_before)),
            ("cryptovizor", lambda: _check_cryptovizor(db)),
            # ai_signals убран — AI filter теперь внутри _check_cryptovizor
            ("ai_analysis", lambda: _fill_missing_ai_analysis(db)),
        ]:
            try:
                print(f"[WATCHER] step: {step_name}", flush=True)
                await asyncio.wait_for(step_fn(), timeout=120)
            except asyncio.TimeoutError:
                print(f"[WATCHER] step '{step_name}' TIMEOUT (120s)", flush=True)
            except Exception as e:
                print(f"[WATCHER] step '{step_name}' FAILED: {e}", flush=True)
    finally:
        db.close()

    # Аномалии — вне db сессии (свой MongoDB доступ)
    try:
        await _check_anomalies()
    except Exception as e:
        print(f"[ANOMALY] ERROR: {e}", flush=True)

    # Confluence Scanner
    try:
        await _check_confluence()
    except Exception as e:
        print(f"[CONFLUENCE] ERROR: {e}", flush=True)


async def _send_cryptovizor_alert(signal: Signal, pattern: str, current_price: float,
                                   s1: float | None, r1: float | None,
                                   chart_png: bytes | None):
    """Алерт в отдельный бот BOT2 для Cryptovizor."""
    target_bot = _bot2 or _bot
    if not target_bot or not _admin_chat_id:
        return
    is_long = signal.direction in ("LONG", "BUY")
    dir_emoji = "🟢" if is_long else "🔴"
    dir_label = "LONG" if is_long else "SHORT"
    pair = (signal.pair or "—").replace("/USDT", "")

    ai_line = ""
    if getattr(signal, "ai_score", None) is not None:
        score = signal.ai_score
        emoji_ai = "🟢" if score >= 70 else "🟡" if score >= 40 else "🔴"
        ai_line = f"\n{emoji_ai} <b>AI:</b> {score}/100 · {signal.ai_verdict or '—'}"

    pnl_line = ""
    if signal.entry and current_price:
        raw = ((current_price - signal.entry) / signal.entry) * 100
        pnl = -raw if not is_long else raw
        pnl_line = f"\n📊 <b>PnL с сигнала:</b> {pnl:+.2f}%"

    s1_line = f"\n🟢 <b>S1:</b> <code>{s1}</code>" if s1 else ""
    r1_line = f"\n🔴 <b>R1:</b> <code>{r1}</code>" if r1 else ""

    text = (
        f"🚀 <b>CRYPTOVIZOR · ПАТТЕРН НАЙДЕН</b>\n"
        f"\n"
        f"<b>{pair}/USDT</b> · 1h · {dir_emoji} <b>{dir_label}</b>\n"
        f"\n"
        f"🎴 <b>Паттерн:</b> {pattern}\n"
        f"🎯 <b>Цена сигнала:</b> <code>{signal.entry}</code>\n"
        f"💰 <b>Сейчас:</b> <code>{current_price}</code>"
        f"{pnl_line}"
        f"{s1_line}{r1_line}"
        f"{ai_line}\n"
        f"\n"
        f"⚡ <i>Тренд: {_fmt_trend(signal.trend)}</i>"
        f"{_eth_line()}"
    )

    try:
        if chart_png:
            from aiogram.types import BufferedInputFile
            photo = BufferedInputFile(chart_png, filename=f"{pair}_1h.png")
            await target_bot.send_photo(_admin_chat_id, photo=photo, caption=text, parse_mode="HTML")
        else:
            await target_bot.send_message(_admin_chat_id, text, parse_mode="HTML")
        logger.info(f"Cryptovizor alert sent #{signal.id} {pair} {pattern}")
    except Exception as e:
        logger.error(f"Cryptovizor alert fail #{signal.id}: {e}")


async def _check_cryptovizor(db):
    """Watcher для cryptovizor сигналов: детект reversal+continuation на 1h,
    переход СЛЕЖУ → ПАТТЕРН, алерт в BOT2."""
    from continuation_patterns import detect_continuation
    from levels import nearest_levels
    from ai_analyzer import analyze_signal_quality
    from chart_renderer import render_chart

    signals = (
        db.query(Signal)
        .filter(Signal.source == "cryptovizor")
        .filter(Signal.status == "СЛЕЖУ")
        .filter(Signal.pair != None)
        .filter(Signal.direction != None)
        .limit(20)
        .all()
    )
    if not signals:
        return

    for s in signals:
        try:
            candles = await get_klines_any(s.pair, "1h", limit=60)
            if not candles or len(candles) < 10:
                continue

            # Ждём НОВУЮ свечу — паттерн должен появиться ПОСЛЕ создания сигнала
            last_candle_open_ms = candles[-1].get("t", 0)  # open_time в ms
            signal_created_ms = 0
            if s.received_at and hasattr(s.received_at, 'timestamp'):
                signal_created_ms = int(s.received_at.timestamp() * 1000)

            if signal_created_ms and last_candle_open_ms and last_candle_open_ms < signal_created_ms:
                continue  # свеча открылась до сигнала — ждём следующую

            # Reversal-паттерны в направлении главного тренда (вход в сделку)
            reversal = detect_patterns(candles, s.direction)
            # Continuation-паттерны
            continuation = detect_continuation(candles, s.direction)

            detected = reversal + continuation

            current_price = candles[-1]["c"]
            h1 = candles  # уже 1h
            s1, r1 = nearest_levels(h1, current_price, left=3, right=3) if h1 else (None, None)

            # ── Паттерн найден → AI решает: Сигнал AI или Сигнал ──
            if detected:
                strongest = reversal[0] if reversal else continuation[0]
                chart_png = render_chart(
                    candles[-30:], s.pair, s.direction,
                    entry=s.entry, s1=s1, r1=r1, pattern=strongest,
                )
                s.pattern_triggered = True
                s.pattern_name = strongest
                s.pattern_triggered_at = utcnow()
                s.pattern_price = current_price
                if s1 is not None: s.dca1 = s1
                if r1 is not None: s.dca2 = r1

                # AI score
                await _ai_score_and_alert_pattern(s, strongest, current_price, s1, r1, chart_png, candles, db)

                # AI filter решает: Сигнал AI или обычный Сигнал
                ai_passed = await _run_ai_filter(s, current_price, db)
                if ai_passed:
                    s.status = "ПАТТЕРН"
                    s.filter_reason = f"AI_SIGNAL:score={s.ai_score or 0}"
                    db.commit()
                    log_event(s.id, "ai_signal", price=current_price,
                        message=f"AI отобрал: {strongest}, score {s.ai_score}")
                    _broadcast("signal_new", {"id": s.id, "status": "AI_SIGNAL", "source": "cryptovizor"})
                    # Алерт в BOT4 + глубокий анализ
                    await _send_ai_signal_alert(s, {"score": s.ai_score or 0}, current_price)
                else:
                    s.status = "ПАТТЕРН"
                    s.result = f"AI:{s.ai_score or 0}"
                    db.commit()
                    log_event(s.id, "cryptovizor_pattern", price=current_price,
                        data={"pattern": strongest, "s1": s1, "r1": r1},
                        message=f"Cryptovizor: {strongest}")
                    _broadcast("signal_new", {"id": s.id, "status": "ПАТТЕРН", "source": "cryptovizor"})
                    # Обычный алерт в BOT2
                    await _send_cryptovizor_alert(s, strongest, current_price, s1, r1, chart_png)
                continue

        except Exception as e:
            logger.error(f"Cryptovizor check #{s.id}: {e}")


async def _ai_score_and_alert_pattern(s, pattern, price, s1, r1, chart_png, candles, db):
    """AI score только (без отправки алерта — алерт отправляется в _check_cryptovizor)."""
    import tempfile, os
    try:
        png_path = None
        if chart_png:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
            tmp.write(chart_png); tmp.close()
            png_path = tmp.name
        if png_path:
            from ai_analyzer import analyze_signal_quality
            q = await analyze_signal_quality(png_path, {
                "pair": s.pair, "direction": s.direction, "timeframe": "1h",
                "entry": s.entry, "tp1": r1, "sl": s1,
                "trend": s.trend, "pattern": pattern,
            })
            if q and "score" in q:
                s.ai_score = q["score"]
                s.ai_confidence = q.get("confidence")
                s.ai_reasoning = q.get("reasoning")
                s.ai_risks = q.get("risks") or []
                s.ai_verdict = q.get("verdict")
                db.commit()
            try: os.remove(png_path)
            except Exception: pass
    except Exception as e:
        logger.error(f"CV AI score #{s.id}: {e}")


async def _fill_missing_ai_analysis(db):
    """Дозаполняет анализ для AI сигналов у которых comment пустой."""
    from database import _signals as _sc

    docs = list(_sc().find({
        "source": "cryptovizor",
        "filter_reason": {"$regex": "^AI_SIGNAL"},
        "$or": [{"comment": None}, {"comment": ""}],
    }).limit(2))

    for doc in docs:
        sig_id = doc.get("id")
        pair = doc.get("pair", "")
        if not pair:
            continue

        prices = await get_prices_any([pair])
        norm = pair.replace("/", "").upper()
        current = prices.get(norm)

        s1 = doc.get("dca1")
        r1 = doc.get("dca2")

        # Создаём фейковый signal-like объект
        class _S:
            pass
        sig = _S()
        for k, v in doc.items():
            if k != "_id":
                setattr(sig, k, v)

        analysis = await _generate_ai_deep_analysis(sig, current or doc.get("entry"), s1, r1)
        if analysis:
            _sc().update_one({"id": sig_id}, {"$set": {"comment": analysis}})
            logger.info(f"AI analysis filled for #{sig_id} {pair}")

            # Отправляем в бот если ещё не отправлено
            target_bot = _bot4 or _bot2 or _bot
            if target_bot and _admin_chat_id:
                dir_emoji = "🟢" if doc.get("direction") in ("LONG", "BUY") else "🔴"
                p = pair.replace("/USDT", "")
                short = analysis[:800] + ("…" if len(analysis) > 800 else "")
                text = (
                    f"🤖 <b>AI SIGNAL · {p}/USDT</b>\n"
                    f"{dir_emoji} <b>{doc.get('direction')}</b> · {doc.get('pattern_name','')}\n\n"
                    f"{short}"
                )
                try:
                    await target_bot.send_message(_admin_chat_id, text, parse_mode="HTML")
                except Exception:
                    pass


async def _run_ai_filter(s, current_price, db) -> bool:
    """Проверяет сигнал по AI критериям. True = проходит в Сигнал AI."""
    try:
        from database import _get_db
        criteria_doc = _get_db().settings.find_one({"_id": "ai_criteria"})
        user_criteria = criteria_doc.get("criteria", []) if criteria_doc else []

        if not user_criteria:
            # Нет критериев — пропускаем всё с ai_score >= 60
            return (s.ai_score or 0) >= 60

        enabled_patterns = {c["label"] for c in user_criteria if c.get("type") == "pattern" and c.get("enabled")}
        enabled_directions = {c["label"] for c in user_criteria if c.get("type") == "direction" and c.get("enabled")}
        enabled_hours = {int(c["id"].split(":")[1]) for c in user_criteria if c.get("type") == "hour" and c.get("enabled")}
        min_ai_score_c = next((c for c in user_criteria if c.get("id") == "min_ai_score"), None)

        hour = s.pattern_triggered_at.hour if s.pattern_triggered_at and hasattr(s.pattern_triggered_at, 'hour') else 0

        pass_pattern = not enabled_patterns or (s.pattern_name in enabled_patterns)
        pass_direction = not enabled_directions or (s.direction in enabled_directions)
        pass_hour = not enabled_hours or (hour in enabled_hours)
        pass_ai = True
        if min_ai_score_c and min_ai_score_c.get("enabled") and s.ai_score is not None:
            pass_ai = s.ai_score >= min_ai_score_c.get("value", 0)

        return pass_pattern and pass_direction and pass_hour and pass_ai
    except Exception as e:
        logger.error(f"AI filter check #{s.id}: {e}")
        return False


async def _check_ai_signals(db):
    """Для сигналов Cryptovizor в ПАТТЕРН, ещё не проверенных AI filter →
    Claude решает на основе бектеста отправлять ли в @aitradiumbot."""
    # Только сигналы с ai_filter_checked != True
    signals = (
        db.query(Signal)
        .filter(Signal.source == "cryptovizor")
        .filter(Signal.status == "ПАТТЕРН")
        .filter(Signal.result == None)  # ещё не проверен AI filter (используем поле result)
        .limit(5)
        .all()
    )
    if not signals:
        return

    from backtest import backtest_summary_for_ai
    from ai_signal_filter import should_send_signal

    # Загружаем сохранённые критерии пользователя
    from database import _get_db
    criteria_doc = _get_db().settings.find_one({"_id": "ai_criteria"})
    user_criteria = criteria_doc.get("criteria", []) if criteria_doc else []

    # Enabled критерии
    enabled_patterns = {c["label"] for c in user_criteria if c.get("type") == "pattern" and c.get("enabled")}
    enabled_directions = {c["label"] for c in user_criteria if c.get("type") == "direction" and c.get("enabled")}
    enabled_hours = {int(c["id"].split(":")[1]) for c in user_criteria if c.get("type") == "hour" and c.get("enabled")}
    min_win_rate_c = next((c for c in user_criteria if c.get("id") == "min_win_rate"), None)
    min_ai_score_c = next((c for c in user_criteria if c.get("id") == "min_ai_score"), None)

    has_criteria = bool(user_criteria)

    # Кешируем summary для Claude fallback
    summary = ""
    try:
        summary = await asyncio.to_thread(backtest_summary_for_ai, "cryptovizor")
    except Exception as e:
        logger.error(f"Backtest summary error: {e}")

    for s in signals:
        try:
            norm = (s.pair or "").replace("/", "").upper()
            prices = await get_prices_any([s.pair])
            current = prices.get(norm)
            hour = s.pattern_triggered_at.hour if s.pattern_triggered_at and hasattr(s.pattern_triggered_at, 'hour') else 0

            # Если есть сохранённые критерии — используем правила
            if has_criteria:
                pass_pattern = not enabled_patterns or (s.pattern_name in enabled_patterns)
                pass_direction = not enabled_directions or (s.direction in enabled_directions)
                pass_hour = not enabled_hours or (hour in enabled_hours)
                pass_ai = True
                if min_ai_score_c and min_ai_score_c.get("enabled") and s.ai_score is not None:
                    pass_ai = s.ai_score >= min_ai_score_c.get("value", 0)

                send = pass_pattern and pass_direction and pass_hour and pass_ai
                result = {
                    "send": send,
                    "score": 8 if send else 3,
                    "reasoning": f"Criteria: pattern={'✓' if pass_pattern else '✗'} dir={'✓' if pass_direction else '✗'} hour={'✓' if pass_hour else '✗'} ai={'✓' if pass_ai else '✗'}",
                }
            else:
                # Нет критериев — Claude решает
                result = await should_send_signal({
                    "pair": s.pair,
                    "direction": s.direction,
                    "pattern": s.pattern_name,
                    "trend": s.trend,
                    "entry": s.entry,
                    "current_price": current,
                    "ai_score": s.ai_score,
                    "hour": hour,
                }, summary)

            # Помечаем как проверенный (используем поле result)
            s.result = f"AI:{result.get('score', 0)}"
            db.commit()

            if result.get("send"):
                # Не меняем status — сигнал остаётся в ПАТТЕРН (вкладка Сигнал)
                # Добавляем метку ai_filtered для вкладки Сигнал AI
                s.filter_reason = f"AI_SIGNAL:score={result['score']}"
                db.commit()
                log_event(s.id, "ai_signal", price=current,
                    data={"score": result["score"], "reasoning": result.get("reasoning")},
                    message=f"AI отобрал: score {result['score']}/10")
                _broadcast("signal_update", {"id": s.id, "status": "AI_SIGNAL", "source": "cryptovizor"})

                # Алерт в BOT4
                await _send_ai_signal_alert(s, result, current)
                logger.info(f"AI Signal #{s.id} {s.pair} score={result['score']}")
            else:
                log_event(s.id, "ai_skipped",
                    data={"score": result["score"], "reasoning": result.get("reasoning")},
                    message=f"AI пропустил: score {result['score']}/10")
        except Exception as e:
            logger.error(f"AI filter #{s.id}: {e}")


async def _generate_ai_full_analysis(signal, current_price, s1, r1):
    """Полный анализ для сайта (comment в БД)."""
    import anthropic
    from config import ANTHROPIC_API_KEY, ANTHROPIC_MODEL_FAST as ANTHROPIC_MODEL

    pair = (signal.pair or "").replace("/USDT", "")
    entry = signal.entry or current_price
    direction = signal.direction or "LONG"
    pattern = signal.pattern_name or "unknown"
    trend = signal.trend or ""

    from exchange import get_eth_market_context
    ectx = get_eth_market_context()
    eth_ctx_text = (
        f"ETH за 1h: {ectx.get('eth_1h', 0):+.2f}% | BTC за 1h: {ectx.get('btc_1h', 0):+.2f}% | "
        f"ETH/BTC тренд: {ectx.get('eth_btc', '—')}"
    )

    prompt = (
        f"Ты — профессиональный крипто-трейдер. Дай ПОЛНЫЙ анализ сделки.\n\n"
        f"Монета: {pair}/USDT\n"
        f"Направление: {direction}\n"
        f"Паттерн: {pattern}\n"
        f"Тренд (5 TF): {trend}\n"
        f"Entry: {entry}\n"
        f"Текущая цена: {current_price}\n"
        f"Support (S1): {s1 or '—'}\n"
        f"Resistance (R1): {r1 or '—'}\n"
        f"Рыночный контекст: {eth_ctx_text}\n\n"
        f"Ответь СТРОГО в таком формате:\n\n"
        f"О МОНЕТЕ:\n"
        f"Что это за проект, для чего используется, капитализация, ликвидность, "
        f"кто стоит за проектом. 2-3 предложения.\n\n"
        f"ВЕРДИКТ: ENTER или SKIP\n"
        f"УВЕРЕННОСТЬ: HIGH, MEDIUM или LOW\n"
        f"SCORE: X/10\n\n"
        f"TP1: цена\n"
        f"TP2: цена\n"
        f"SL: цена\n"
        f"R:R: соотношение\n\n"
        f"АНАЛИЗ:\n"
        f"Описание сетапа — почему паттерн {pattern} здесь работает или нет, "
        f"структура рынка, уровни, объёмы. "
        f"Как ETH и BTC влияют на эту монету — коррелирует ли она с рынком. 4-6 предложений.\n\n"
        f"РИСКИ:\n"
        f"⚠ Риск 1\n"
        f"⚠ Риск 2\n"
        f"⚠ Риск 3\n\n"
        f"Ответ на русском. БЕЗ markdown, без ## и **. Только plain text."
    )

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        message = await asyncio.to_thread(
            client.messages.create,
            model=ANTHROPIC_MODEL,
            max_tokens=800,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text
    except Exception as e:
        logger.error(f"AI full analysis error: {e}")
        return None


async def _generate_ai_tg_summary(signal, current_price, s1, r1):
    """Короткое саммари для Telegram (помещается в сообщение)."""
    import anthropic
    from config import ANTHROPIC_API_KEY, ANTHROPIC_MODEL_FAST as ANTHROPIC_MODEL

    pair = (signal.pair or "").replace("/USDT", "")
    entry = signal.entry or current_price
    direction = signal.direction or "LONG"
    pattern = signal.pattern_name or "unknown"
    trend = signal.trend or ""

    prompt = (
        f"Крипто-трейдер. Дай КРАТКИЙ анализ для Telegram.\n\n"
        f"{pair}/USDT | {direction} | {pattern} | Тренд: {trend}\n"
        f"Entry: {entry} | Цена: {current_price} | S1: {s1 or '—'} | R1: {r1 or '—'}\n\n"
        f"Формат строго:\n"
        f"TP1: цена | TP2: цена | SL: цена\n"
        f"R:R: X:X | Score: X/10\n"
        f"Одно предложение — вывод.\n\n"
        f"На русском. Максимум 200 символов."
    )

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        message = await asyncio.to_thread(
            client.messages.create,
            model=ANTHROPIC_MODEL,
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text
    except Exception as e:
        logger.error(f"AI TG summary error: {e}")
        return None


async def _send_ai_signal_alert(signal, ai_result, current_price):
    target_bot = _bot4 or _bot2 or _bot
    if not target_bot or not _admin_chat_id:
        return
    is_long = signal.direction in ("LONG", "BUY")
    dir_emoji = "🟢" if is_long else "🔴"
    pair = (signal.pair or "—").replace("/USDT", "")
    score = ai_result.get("score", 0)
    score_bar = "🟢" * score + "⚪" * (10 - score)

    # Получаем уровни
    s1 = getattr(signal, "dca1", None)
    r1 = getattr(signal, "dca2", None)

    # 1. Полный анализ → сохраняем в БД (для сайта)
    full_analysis = await _generate_ai_full_analysis(signal, current_price, s1, r1)
    if full_analysis:
        signal.comment = full_analysis
        from database import _signals
        _signals().update_one(
            {"id": signal.id},
            {"$set": {"comment": full_analysis}}
        )

    # 2. Короткое саммари → для Telegram
    tg_summary = await _generate_ai_tg_summary(signal, current_price, s1, r1)

    text = (
        f"🤖 <b>AI SIGNAL · TOP PICK</b>\n"
        f"\n"
        f"<b>{pair}/USDT</b> · 1h · {dir_emoji} <b>{signal.direction}</b>\n"
        f"\n"
        f"🎴 Паттерн: <b>{signal.pattern_name}</b>\n"
        f"🎯 Entry: <code>{signal.entry}</code>\n"
        f"💰 Сейчас: <code>{current_price}</code>\n"
    )
    if s1:
        text += f"🟢 S1: <code>{s1}</code>\n"
    if r1:
        text += f"🔴 R1: <code>{r1}</code>\n"

    text += f"\n⭐ Score: <b>{score}/10</b> {score_bar}\n"

    if tg_summary:
        text += f"\n📝 {tg_summary}"
    text += _eth_line()

    try:
        await target_bot.send_message(_admin_chat_id, text, parse_mode="HTML")
        logger.info(f"AI signal alert sent #{signal.id}")
    except Exception as e:
        logger.error(f"AI signal alert fail: {e}")


_anomaly_batch_idx = 0
_ANOMALY_INTERVAL = 10  # каждый 10-й тик (10×30с = 5 мин)
_anomaly_tick = _ANOMALY_INTERVAL - 1  # первый скан на первом тике

# Состояние скана — читается из admin API
anomaly_scan_state = {
    "running": False, "progress": 0, "total": 0,
    "found": 0, "batch": 0, "batches": 0, "current": "",
    "next_at": 0,  # unix timestamp следующего скана
}


async def _check_anomalies():
    """Сканирует фьючерсные пары батчами. Полный цикл за 4 тика (20 мин)."""
    import time as _time
    global _anomaly_batch_idx, _anomaly_tick
    _anomaly_tick += 1
    ticks_left = _ANOMALY_INTERVAL - (_anomaly_tick % _ANOMALY_INTERVAL)
    if ticks_left == _ANOMALY_INTERVAL:
        ticks_left = 0
    anomaly_scan_state["next_at"] = _time.time() + ticks_left * 30
    if _anomaly_tick % _ANOMALY_INTERVAL != 0:
        return

    from anomaly_scanner import get_liquid_pairs, scan_symbol, _refresh_batch_cache
    from database import _anomalies, utcnow

    # Сразу показываем что скан идёт
    anomaly_scan_state["running"] = True
    anomaly_scan_state["current"] = "загрузка..."
    anomaly_scan_state["found"] = 0
    anomaly_scan_state["progress"] = 0

    try:
        await asyncio.to_thread(_refresh_batch_cache)
    except Exception as e:
        logger.error(f"Batch cache refresh failed: {e}")

    pairs = await asyncio.to_thread(get_liquid_pairs, 5_000_000)
    if not pairs:
        anomaly_scan_state["running"] = False
        anomaly_scan_state["current"] = ""
        return

    batch = pairs
    anomaly_scan_state["total"] = len(pairs)
    anomaly_scan_state["batch"] = 1
    anomaly_scan_state["batches"] = 1

    print(f"[ANOMALY] Scanning {len(batch)} pairs", flush=True)

    now = utcnow()
    results = []
    for idx, symbol in enumerate(batch):
        anomaly_scan_state["current"] = symbol.replace("USDT", "")
        anomaly_scan_state["progress"] = int((idx / len(batch)) * 100)

        try:
            r = await asyncio.to_thread(scan_symbol, symbol)
        except Exception:
            continue
        if not r or r["score"] < 10:
            continue
        if not r.get("has_ftt") and not r.get("has_delta"):
            continue
        if r.get("raw_count", 0) < 3:
            continue

        # Дедупликация — та же пара за последние 4 часа
        import datetime
        existing = _anomalies().find_one({
            "symbol": r["symbol"],
            "detected_at": {"$gte": datetime.datetime.utcnow() - datetime.timedelta(hours=4)},
        })
        if existing:
            continue

        doc = {
            "symbol": r["symbol"], "pair": r["pair"], "price": r["price"],
            "score": r["score"], "direction": r["direction"],
            "anomalies": r["anomalies"], "detected_at": now,
        }
        _anomalies().insert_one(doc)
        anomaly_scan_state["found"] += 1
        results.append(r)
        logger.info(f"Anomaly: {r['symbol']} score={r['score']} dir={r['direction']}")

        # Алерт в бот (только сильные)
        if r["score"] >= 10:
            await _send_anomaly_alert(r)

    anomaly_scan_state["running"] = False
    anomaly_scan_state["progress"] = 100
    anomaly_scan_state["current"] = ""
    print(f"[ANOMALY] Done: {len(results)} found (score>=10) from {len(batch)} pairs", flush=True)
    if results:
        _broadcast("anomaly_update", {"count": len(results)})


_bot3 = None

def setup_bot3():
    """Инициализирует BOT3 для аномалий."""
    global _bot3
    from config import BOT3_BOT_TOKEN
    if not BOT3_BOT_TOKEN:
        return
    try:
        from aiogram import Bot
        from aiogram.client.default import DefaultBotProperties
        from aiogram.enums import ParseMode
        _bot3 = Bot(token=BOT3_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        logger.info("BOT3 (Anomaly) initialized")
    except Exception as e:
        logger.error(f"BOT3 init fail: {e}")


async def _send_anomaly_alert(r: dict):
    """Отправляет алерт об аномалии в BOT3."""
    if not _bot3:
        setup_bot3()
    if not _bot3 or not _admin_chat_id:
        logger.warning("Anomaly alert: BOT3 not available")
        return

    dir_emoji = "🟢" if r["direction"] == "LONG" else "🔴" if r["direction"] == "SHORT" else "⚪"
    pair = r["pair"].replace("/USDT", "")
    ws = r.get("score", 0)
    score_bar = "🔥" * min(int(ws / 2), 5)
    price = r.get("price", 0)

    # Собираем типы аномалий
    types = [a["type"] for a in r["anomalies"]]
    count = len(types)

    # FTT инфо
    ftt_info = ""
    for a in r["anomalies"]:
        if a["type"] == "ftt":
            ftt_dir = "🟢 LONG" if a["value"] == "LONG" else "🔴 SHORT"
            ftt_s = a.get("ftt_score", 0)
            tf = a.get("tf", "1h")
            ftt_info = f"\n🕯 <b>Разворот (FTT)</b>: {ftt_dir} · {ftt_s}/5 · {tf}"
            ftt_info += f"\n   Тень: {int(a.get('wick_ratio', 0) * 100)}% свечи · Объём: ×{a.get('vol_ratio', 0)}"

    # Delta инфо
    delta_info = ""
    for a in r["anomalies"]:
        if a["type"] == "delta_cluster":
            delta = a["value"]
            side = "покупки" if delta > 0 else "продажи"
            delta_info = f"\n📊 <b>Кластер</b>: {side} доминируют (delta {delta:+,.0f})"

    # Остальные индикаторы одной строкой
    indicators = []
    for a in r["anomalies"]:
        t, v = a["type"], a["value"]
        if t == "oi_spike":
            indicators.append(f"OI {v:+.1f}%")
        elif t == "funding_extreme":
            side = "лонги платят" if v > 0 else "шорты платят"
            indicators.append(f"Funding {v:.3f}% ({side})")
        elif t == "ls_extreme":
            bias = "перевес лонгов" if v > 1.5 else "перевес шортов"
            indicators.append(f"L/S {v:.1f} ({bias})")
        elif t == "taker_imbalance":
            bias = "агрессивные покупки" if v > 1 else "агрессивные продажи"
            indicators.append(f"Taker {v:.1f} ({bias})")
        elif t == "trade_speed":
            indicators.append(f"Скорость ×{v:.0f}")
        elif t == "wall":
            side = "поддержка" if v.get("side") == "bid" else "сопротивление"
            indicators.append(f"Стена {side} @ {v.get('price')}")

    # Вывод — что это значит
    if r["direction"] == "LONG":
        conclusion = "Накопление объёма, возможен рост"
    elif r["direction"] == "SHORT":
        conclusion = "Распределение объёма, возможно падение"
    else:
        conclusion = "Высокая активность, направление неясно"

    text = (
        f"⚠️ <b>АНОМАЛИЯ · {pair}/USDT</b>\n"
        f"\n"
        f"{dir_emoji} <b>{r['direction']}</b> · Цена: <code>{price}</code>\n"
        f"Score: <b>{ws}</b>/15 {score_bar} · {count} индикаторов\n"
    )
    text += ftt_info
    text += delta_info
    if indicators:
        text += "\n\n" + "\n".join(f"  · {ind}" for ind in indicators)
    text += f"\n\n💡 <i>{conclusion}</i>"
    text += _eth_line()
    try:
        await _bot3.send_message(_admin_chat_id, text, parse_mode="HTML")
        logger.info(f"Anomaly alert sent: {r.get('symbol')}")
    except Exception as e:
        logger.error(f"Anomaly alert fail: {e}")


# ── Confluence Scanner ────────────────────────────────────────────────
_CONFLUENCE_INTERVAL = 10  # каждый 10-й тик = 5 мин
_confluence_tick = _CONFLUENCE_INTERVAL - 1  # первый скан сразу

confluence_scan_state = {
    "running": False, "progress": 0, "total": 0,
    "found": 0, "current": "", "next_at": 0,
}


async def _check_confluence():
    """Сканирует ликвидные пары на confluence сетапы."""
    import time as _time
    global _confluence_tick
    _confluence_tick += 1
    ticks_left = _CONFLUENCE_INTERVAL - (_confluence_tick % _CONFLUENCE_INTERVAL)
    if ticks_left == _CONFLUENCE_INTERVAL:
        ticks_left = 0
    confluence_scan_state["next_at"] = _time.time() + ticks_left * 30
    if _confluence_tick % _CONFLUENCE_INTERVAL != 0:
        return

    from anomaly_scanner import get_liquid_pairs, _refresh_batch_cache
    from confluence_scanner import scan_confluence
    from database import _confluence, utcnow
    import datetime

    confluence_scan_state["running"] = True
    confluence_scan_state["current"] = "загрузка..."
    confluence_scan_state["found"] = 0
    confluence_scan_state["progress"] = 0

    try:
        await asyncio.to_thread(_refresh_batch_cache)
    except Exception:
        pass

    pairs = await asyncio.to_thread(get_liquid_pairs, 5_000_000)
    if not pairs:
        confluence_scan_state["running"] = False
        return

    confluence_scan_state["total"] = len(pairs)
    print(f"[CONFLUENCE] Scanning {len(pairs)} pairs", flush=True)

    now = utcnow()
    results = []
    for idx, symbol in enumerate(pairs):
        confluence_scan_state["current"] = symbol.replace("USDT", "")
        confluence_scan_state["progress"] = int((idx / len(pairs)) * 100)

        try:
            r = await asyncio.to_thread(scan_confluence, symbol)
        except Exception:
            continue
        if not r:
            continue

        # Дедупликация — та же пара + направление за 12 часов
        existing = _confluence().find_one({
            "symbol": r["symbol"],
            "direction": r["direction"],
            "detected_at": {"$gte": datetime.datetime.utcnow() - datetime.timedelta(hours=12)},
        })
        if existing:
            continue

        doc = {
            "symbol": r["symbol"], "pair": r["pair"], "price": r["price"],
            "score": r["score"], "strength": r["strength"],
            "direction": r["direction"], "factors": r["factors"],
            "s1": r.get("s1"), "r1": r.get("r1"),
            "pattern": r.get("pattern"), "trend_tf": r.get("trend_tf"),
            "detected_at": now,
        }
        _confluence().insert_one(doc)
        confluence_scan_state["found"] += 1
        results.append(r)
        logger.info(f"Confluence: {r['symbol']} score={r['score']} {r['strength']} {r['direction']}")

        # Алерт в BOT5
        if r["score"] >= 4:
            await _send_confluence_alert(r)

    confluence_scan_state["running"] = False
    confluence_scan_state["progress"] = 100
    confluence_scan_state["current"] = ""
    confluence_scan_state["last_scanned"] = len(pairs)
    confluence_scan_state["last_found"] = len(results)
    print(f"[CONFLUENCE] Done: {len(results)} new from {len(pairs)} pairs (total in DB: {_confluence().count_documents({})})", flush=True)
    if results:
        _broadcast("confluence_update", {"count": len(results)})


_bot5 = None

def _setup_bot5():
    global _bot5
    from config import BOT5_BOT_TOKEN
    if not BOT5_BOT_TOKEN:
        return
    try:
        from aiogram import Bot
        from aiogram.client.default import DefaultBotProperties
        from aiogram.enums import ParseMode
        _bot5 = Bot(token=BOT5_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        logger.info("BOT5 (Confluence) initialized")
    except Exception as e:
        logger.error(f"BOT5 init fail: {e}")


async def _send_confluence_alert(r: dict):
    """Отправляет confluence алерт в BOT5."""
    if not _bot5:
        _setup_bot5()
    if not _bot5 or not _admin_chat_id:
        return

    dir_emoji = "🟢" if r["direction"] == "LONG" else "🔴" if r["direction"] == "SHORT" else "⚪"
    pair = r["pair"].replace("/USDT", "")
    score = r["score"]
    strength = r["strength"]
    price = r.get("price", 0)

    # Визуал score
    filled = "🟢" * score + "⚪" * (6 - score)
    strength_label = "💪 STRONG" if strength == "STRONG" else "📊 MEDIUM"

    # Тренд TF
    tf_map = r.get("trend_tf", {})
    tf_line = " ".join(f"{tf}{emoji}" for tf, emoji in tf_map.items()) if tf_map else "—"

    # Собираем факторы с пояснениями
    checks = []
    has_level = has_volume = has_trend = has_pattern = has_eth = has_ftt = False

    for f in r.get("factors", []):
        t = f["type"]
        if t == "level":
            has_level = True
            lvl_type = "поддержке" if "S1" in f["value"] else "сопротивлении"
            strength_txt = "двойной (4h+1h)" if f.get("strength", 0) >= 2 else "одинарный"
            checks.append(f"✅ Цена на {lvl_type} — {strength_txt} уровень")
        elif t == "volume":
            has_volume = True
            checks.append(f"✅ Объём {f['value']} от среднего — подтверждает интерес")
        elif t == "trend":
            has_trend = True
            checks.append(f"✅ Тренд {f['value']} — большинство TF подтверждают")
        elif t == "pattern":
            has_pattern = True
            checks.append(f"✅ Паттерн «{f['value']}» на 1h")
        elif t == "eth_corr":
            has_eth = True
            checks.append(f"✅ ETH попутный ({f.get('eth_1h', 0):+.2f}%) — рынок помогает")
        elif t == "ftt":
            has_ftt = True
            checks.append(f"✅ FTT разворот: wick {int(f.get('wick_ratio', 0)*100)}%, объём ×{f.get('vol_ratio', 0)}")

    # Что НЕ совпало
    if not has_level: checks.append("❌ Нет уровня — цена в воздухе")
    if not has_volume: checks.append("❌ Объём обычный")
    if not has_trend: checks.append("❌ Тренд не подтверждён")
    if not has_pattern: checks.append("❌ Нет паттерна")
    if not has_eth: checks.append("❌ ETH не попутный")
    if not has_ftt: checks.append("❌ Нет FTT")

    # Вывод
    if score >= 5:
        conclusion = "Сильный сетап — высокая вероятность отработки"
    elif score == 4:
        conclusion = "Средний сетап — входить с осторожностью"
    else:
        conclusion = "Слабый сетап — лучше пропустить"

    text = (
        f"🎯 <b>CONFLUENCE SIGNAL</b>\n"
        f"\n"
        f"<b>{pair}/USDT</b> · {dir_emoji} <b>{r['direction']}</b> · {strength_label}\n"
        f"Цена: <code>{price}</code>\n"
        f"\n"
    )
    if r.get("s1"):
        text += f"🟢 Поддержка: <code>{r['s1']}</code>\n"
    if r.get("r1"):
        text += f"🔴 Сопротивление: <code>{r['r1']}</code>\n"
    text += (
        f"\n"
        f"<b>Score: {score}/6</b> {filled}\n"
        f"Тренд: {tf_line}\n"
        f"\n"
        f"<b>Чеклист факторов:</b>\n"
    )
    text += "\n".join(checks)
    text += f"\n\n💡 <i>{conclusion}</i>"
    text += _eth_line()

    try:
        await _bot5.send_message(_admin_chat_id, text, parse_mode="HTML")
        logger.info(f"Confluence alert: {r['symbol']}")
    except Exception as e:
        logger.error(f"Confluence alert fail: {e}")


_watcher_running = False

async def start_watcher():
    global _watcher_running
    _watcher_running = True
    print(f"[WATCHER] Started (interval={POLL_INTERVAL}s)", flush=True)
    logger.info(f"Price watcher запущен (интервал {POLL_INTERVAL}s)")
    tick = 0
    while True:
        tick += 1
        try:
            print(f"[WATCHER] tick {tick}", flush=True)
            await _check_once()
            print(f"[WATCHER] tick {tick} done", flush=True)
        except Exception as e:
            logger.error(f"Watcher ошибка: {e}")
        await asyncio.sleep(POLL_INTERVAL)
