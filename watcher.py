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

POLL_INTERVAL = 15  # секунд

_bot = None
_bot2 = None
_bot3 = None
_admin_chat_id = None


def setup(bot, admin_chat_id: int, bot2=None, bot3=None):
    global _bot, _bot2, _bot3, _admin_chat_id
    _bot = bot
    _bot2 = bot2
    _bot3 = bot3
    _admin_chat_id = admin_chat_id


def _resolve_chart(p: str) -> str | None:
    if not p:
        return None
    if os.path.isabs(p) and os.path.exists(p):
        return p
    base = os.path.dirname(os.path.abspath(__file__))
    cand = os.path.normpath(os.path.join(base, p.lstrip("./\\")))
    return cand if os.path.exists(cand) else None


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
        await _retry_failed_ai(db)
        await _filter_stuck(db)

        # Снимок id до _check_dca4 — чтобы не проверять TP/SL на свежеоткрытых
        opened_before = {
            s.id for s in db.query(Signal.id, Signal.status)
            .filter(Signal.status.in_(["ОТКРЫТ", "ПАТТЕРН"]))
            .all()
        }

        await _check_dca4(db)
        await _check_patterns(db)
        await _check_tp_sl(db, allowed_ids=opened_before)

        # Cryptovizor: отдельный поток паттернов на 1h
        await _check_cryptovizor(db)
    finally:
        db.close()


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
        f"⚡ <i>Тренд: {signal.trend}</i>"
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

            # Reversal-паттерны в направлении главного тренда (вход в сделку)
            reversal = detect_patterns(candles, s.direction)
            # Continuation-паттерны
            continuation = detect_continuation(candles, s.direction)

            detected = reversal + continuation

            current_price = candles[-1]["c"]
            h1 = candles  # уже 1h
            s1, r1 = nearest_levels(h1, current_price, left=3, right=3) if h1 else (None, None)

            # ── Путь 1: паттерн найден → BOT2 ──────────────────
            if detected:
                strongest = reversal[0] if reversal else continuation[0]
                chart_png = render_chart(
                    candles[-30:], s.pair, s.direction,
                    entry=s.entry, s1=s1, r1=r1, pattern=strongest,
                )
                s.status = "ПАТТЕРН"
                s.pattern_triggered = True
                s.pattern_name = strongest
                s.pattern_triggered_at = utcnow()
                s.pattern_price = current_price
                if s1 is not None: s.dca1 = s1
                if r1 is not None: s.dca2 = r1
                db.commit()
                log_event(s.id, "cryptovizor_pattern", price=current_price,
                    data={"pattern": strongest, "s1": s1, "r1": r1},
                    message=f"Cryptovizor: {strongest}")
                _broadcast("signal_update", {"id": s.id, "status": "ПАТТЕРН", "source": "cryptovizor"})
                await _ai_score_and_alert_pattern(s, strongest, current_price, s1, r1, chart_png, candles, db)
                continue

            # ── Путь 2: volume spike → BOT3 ────────────────────
            from volume_analysis import is_volume_spike, format_volume
            triggered, vol_info = is_volume_spike(candles, s.direction)
            if triggered:
                chart_png = render_chart(
                    candles[-30:], s.pair, s.direction,
                    entry=s.entry, s1=s1, r1=r1,
                    pattern=f"VOLUME ×{vol_info['rvol']}",
                )
                s.status = "VOLUME"
                s.pattern_triggered = True
                s.pattern_name = f"Volume ×{vol_info['rvol']}"
                s.pattern_triggered_at = utcnow()
                s.pattern_price = current_price
                # dca1=S1, dca2=R1, dca3=RVOL
                if s1 is not None: s.dca1 = s1
                if r1 is not None: s.dca2 = r1
                s.dca3 = vol_info["rvol"]
                db.commit()
                log_event(s.id, "volume_spike", price=current_price,
                    data=vol_info,
                    message=f"Volume spike ×{vol_info['rvol']} price {vol_info['price_change_pct']}%")
                _broadcast("signal_update", {"id": s.id, "status": "VOLUME", "source": "cryptovizor"})
                await _ai_score_and_alert_volume(s, vol_info, current_price, s1, r1, chart_png, candles, db)
        except Exception as e:
            logger.error(f"Cryptovizor check #{s.id}: {e}")


async def _ai_score_and_alert_pattern(s, pattern, price, s1, r1, chart_png, candles, db):
    """AI score + отправка паттерн-алерта в BOT2."""
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
        logger.error(f"CV AI pattern #{s.id}: {e}")
    await _send_cryptovizor_alert(s, pattern, price, s1, r1, chart_png)


async def _ai_score_and_alert_volume(s, vol_info, price, s1, r1, chart_png, candles, db):
    """AI score + отправка volume-алерта в BOT3."""
    import tempfile, os
    from volume_analysis import format_volume
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
                "trend": s.trend, "pattern": f"Volume spike ×{vol_info['rvol']}",
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
        logger.error(f"CV AI volume #{s.id}: {e}")

    # Алерт в BOT3
    target_bot = _bot3 or _bot2 or _bot
    if not target_bot or not _admin_chat_id:
        return
    is_long = s.direction in ("LONG", "BUY")
    dir_emoji = "🟢" if is_long else "🔴"
    dir_label = "LONG" if is_long else "SHORT"
    pair = (s.pair or "—").replace("/USDT", "")

    ai_line = ""
    if getattr(s, "ai_score", None) is not None:
        em = "🟢" if s.ai_score >= 70 else "🟡" if s.ai_score >= 40 else "🔴"
        ai_line = f"\n{em} <b>AI:</b> {s.ai_score}/100 · {s.ai_verdict or '—'}"

    text = (
        f"🔥 <b>VOLUME ALERT</b>\n"
        f"\n"
        f"<b>{pair}/USDT</b> · 1h · {dir_emoji} <b>{dir_label}</b>\n"
        f"\n"
        f"Volume: <code>{format_volume(vol_info['volume'])}</code> (×{vol_info['rvol']} от среднего)\n"
        f"Цена за свечу: <code>{vol_info['price_change_pct']:+.1f}%</code>\n"
        f"Entry: <code>{s.entry}</code>\n"
        f"Сейчас: <code>{price}</code>"
        f"{ai_line}\n"
        f"\n"
        f"⚡ Объёмный импульс в направлении тренда"
    )
    try:
        if chart_png:
            from aiogram.types import BufferedInputFile
            photo = BufferedInputFile(chart_png, filename=f"{pair}_vol_1h.png")
            await target_bot.send_photo(_admin_chat_id, photo=photo, caption=text, parse_mode="HTML")
        else:
            await target_bot.send_message(_admin_chat_id, text, parse_mode="HTML")
        logger.info(f"Volume alert sent #{s.id} {pair} ×{vol_info['rvol']}")
    except Exception as e:
        logger.error(f"Volume alert fail #{s.id}: {e}")


async def start_watcher():
    logger.info(f"Price watcher запущен (интервал {POLL_INTERVAL}s)")
    while True:
        try:
            await _check_once()
        except Exception as e:
            logger.error(f"Watcher ошибка: {e}")
        await asyncio.sleep(POLL_INTERVAL)
