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
_bot10 = None   # SuperTrend signals (VIP/MTF/Daily)
_admin_chat_id = None


def setup(bot, admin_chat_id: int, bot2=None, bot3=None, bot4=None, bot10=None):
    global _bot, _bot2, _bot4, _bot10, _admin_chat_id
    _bot = bot
    _bot2 = bot2
    _bot4 = bot4
    _bot10 = bot10
    _admin_chat_id = admin_chat_id


async def _st_tracker_loop():
    """Фоновая задача: каждые 5 мин запускает check_all_pairs у SuperTrend tracker.
    Генерирует 3 типа сигналов (VIP/MTF/Daily) и шлёт алерты в BOT10.
    """
    import asyncio as _asyncio
    # Ленивая инициализация BOT10
    if not _bot10:
        try: _setup_bot10()
        except Exception: pass
    while True:
        try:
            from supertrend_tracker import check_all_pairs
            await check_all_pairs(alert_enabled=True)
        except Exception:
            logger.exception("[st-tracker] loop crashed, retry in 5 min")
        await _asyncio.sleep(300)


# [ОТКЛЮЧЕНО] _paper_position_ai_review_loop и _ai_memory_refresh_loop
# удалены в рамках перехода на rule-based систему. Exit management работает
# через TP ladder (paper_trader.check_positions), решения о входе — через
# verified_entry.check_entry. AI не используется для торговых решений.


async def _market_phase_loop():
    """Раз в 3 мин пересчитываем фазу рынка. При смене — запись в историю + alert в BOT."""
    import asyncio as _asyncio
    prev_phase = None
    while True:
        try:
            import market_phase as mp
            result = await _asyncio.to_thread(mp.get_market_phase, True)
            phase = result.get("phase")
            confidence = result.get("confidence", 0)
            metrics = result.get("metrics", {})
            if phase and phase != prev_phase:
                # записываем смену
                changed = await _asyncio.to_thread(
                    mp.record_phase_change, phase, confidence, metrics
                )
                if changed:
                    logger.info(f"[market-phase] CHANGED: {prev_phase} → {phase} ({confidence}%)")
                    # Telegram alert (BOT — главный админ)
                    if prev_phase is not None:  # не алертим при первом старте
                        try:
                            global _bot, _admin_chat_id
                            if _bot and _admin_chat_id:
                                rec = result.get("recommended", [])
                                avoid = result.get("avoid", [])
                                msg = (
                                    f"{result.get('emoji','')} <b>РЫНОК: {result.get('label', phase)}</b>\n"
                                    f"confidence {confidence}%\n\n"
                                    f"Было: {prev_phase}\n"
                                    f"Стало: <b>{phase}</b>\n\n"
                                    f"✅ Что брать:\n" + "\n".join(f"  • {r}" for r in rec[:3]) + "\n\n"
                                    f"❌ Что НЕ брать:\n" + "\n".join(f"  • {a}" for a in avoid[:3])
                                )
                                await _bot.send_message(_admin_chat_id, msg, parse_mode="HTML")
                        except Exception:
                            logger.debug("[market-phase] alert fail", exc_info=True)
                prev_phase = phase
        except Exception:
            logger.exception("[market-phase] loop crashed")
        await _asyncio.sleep(180)  # 3 мин


async def _eth_kc_prewarm_loop():
    """Фоновый прогрев Keltner ETH + ETH/BTC контекста + ST 1h для топ-пар.
    Без прогрева первый Entry Checker или открытие графика висит ~7с на
    холодном HTTP к Binance (supertrend_state + check_pump_potential).
    Прогреваем каждые 4 мин.
    """
    import asyncio as _asyncio
    from exchange import get_keltner_eth, get_eth_market_context, check_pump_potential
    from supertrend import supertrend_state

    async def _warm_st_top_pairs():
        """Прогрев ST 1h + pump для топ-10 активных пар.
        Ограничено 10 парами с sleep между чтобы не забивать threadpool
        во время старта контейнера (иначе /api/journal-candles тормозит)."""
        try:
            from supertrend_tracker import get_tracked_pairs
            pairs = await _asyncio.to_thread(get_tracked_pairs)
            top = pairs[:10]  # меньше пар — быстрее прогрев, остальные закешируются при первом вызове
            for p_norm in top:
                try:
                    pair_slash = p_norm[:-4] + "/USDT" if p_norm.endswith("USDT") else p_norm
                    await _asyncio.to_thread(supertrend_state, pair_slash, "1h", None, None, False)
                    # pump — НЕ прогреваем заранее (TTL 120с, закешируется при первом use)
                except Exception:
                    pass
                # Даём threadpool'у слот для других запросов
                await _asyncio.sleep(0.2)
            logger.info(f"[prewarm] ST warmed for {len(top)} pairs (async-friendly)")
        except Exception:
            logger.exception("[prewarm] ST warm fail")

    # сразу на старте
    try:
        await _asyncio.to_thread(get_keltner_eth)
        await _asyncio.to_thread(get_eth_market_context)
        logger.info("[prewarm] ETH/KC warmed on start")
    except Exception:
        logger.exception("[prewarm] ETH/KC initial warm failed")
    # ST+pump — асинхронно чтобы не задерживать запуск watcher'а
    _asyncio.create_task(_warm_st_top_pairs())

    while True:
        try:
            await _asyncio.sleep(600)  # 10 минут (было 4)
            await _asyncio.to_thread(get_keltner_eth)
            await _asyncio.to_thread(get_eth_market_context)
            # ST+pump prewarm убран из периодического обновления — холодные
            # запросы при первом use ОК (TTL 120с в supertrend_state)
        except Exception:
            logger.exception("[prewarm] ETH/KC loop error")


async def _candles_prewarm_loop():
    """Фоновый прогрев candles cache. Снижено в ~5× для уменьшения CPU нагрузки:
      — топ-20 пар каждые 10 мин на 3 TF (1h/4h/1d)
      — cold пары полностью отключены (открыты будут чуть медленнее на холодную)
    """
    import asyncio as _asyncio
    HOT_TFS = ["1h", "4h", "1d"]
    COLD_TFS = []
    tick = 0
    while True:
        try:
            from supertrend_tracker import get_tracked_pairs
            from admin import warm_candles_cache
            pairs = await _asyncio.to_thread(get_tracked_pairs)
            hot = pairs[:20]  # top-20 only
            cold = []  # disabled
            warmed_hot = 0
            for p in hot:
                for tf in HOT_TFS:
                    try:
                        if await _asyncio.to_thread(warm_candles_cache, p, tf, 200):
                            warmed_hot += 1
                    except Exception:
                        pass
                    await _asyncio.sleep(0.03)
            # Cold пары — только раз в 3-й цикл (~10 мин)
            warmed_cold = 0
            if tick % 3 == 0:
                for p in cold:
                    for tf in COLD_TFS:
                        try:
                            if await _asyncio.to_thread(warm_candles_cache, p, tf, 200):
                                warmed_cold += 1
                        except Exception:
                            pass
                        await _asyncio.sleep(0.03)
            tick += 1
            logger.info(f"[prewarm] hot={warmed_hot} cold={warmed_cold} (hot {len(hot)} × {len(HOT_TFS)}TF + cold {len(cold)} × {len(COLD_TFS)}TF)")
        except Exception:
            logger.exception("[prewarm] loop crashed")
        await _asyncio.sleep(600)  # 10 минут вместо 3


def _resolve_chart(p: str) -> str | None:
    if not p:
        return None
    if os.path.isabs(p) and os.path.exists(p):
        return p
    base = os.path.dirname(os.path.abspath(__file__))
    cand = os.path.normpath(os.path.join(base, p.lstrip("./\\")))
    return cand if os.path.exists(cand) else None


def _kl_block(pair: str, direction: str, at=None,
              entry=None, tp=None, sl=None) -> str:
    """Возвращает КОМПАКТНЫЙ блок Key Levels (1-2 строки, ≤250 chars)
    чтобы не переполнять Telegram photo caption (лимит 1024 символа).

    Формат:
      📐 R 1h 0.03450 (+2.4%) · S 4h 0.03320 (-1.6%) · TP⚠️ SL✅

    Fallback к get_signal_emoji если build_levels_alert_block крашится
    или возвращает пустое.
    """
    try:
        from key_levels import build_levels_compact
        res = build_levels_compact(
            pair or "", direction or "",
            entry=entry, tp=tp, sl=sl,
            at=at or utcnow(),
        )
        if res:
            return res
    except Exception:
        pass
    # Fallback на старый однострочный блок при KL-событии ±2h
    try:
        from key_levels import get_signal_emoji, format_tg_block
        enrich = get_signal_emoji(pair or "", direction or "", at or utcnow())
        return format_tg_block(enrich) if enrich else ""
    except Exception:
        return ""


def _st_block(pair: str, direction: str, tf: str = "1h") -> str:
    """Возвращает строку SuperTrend для вставки в TG алерт ('' при любой ошибке).
    Использует cache_only=True чтобы НЕ блокировать async event loop
    синхронным HTTP запросом к Binance. Кеш прогревается фоном (prewarm_cache).
    """
    try:
        from supertrend import format_tg_block as _st_fmt
        return _st_fmt(pair or "", direction or "", tf or "1h", cache_only=True)
    except Exception:
        return ""


async def _st_prewarm_async(pair: str, tf: str = "1h") -> None:
    """Фоновый прогрев ST-кеша. Вызывается fire-and-forget перед алертом,
    чтобы к моменту build_message ST был в кеше. Не блокирует caller."""
    try:
        from supertrend import prewarm_cache
        await asyncio.to_thread(prewarm_cache, pair, tf)
    except Exception:
        pass


def _check_keltner_filter(direction: str) -> tuple[bool, dict]:
    """Проверяет Keltner Channel ETH фильтр. Возвращает (passed, kc_data).
    NEUTRAL = все сигналы проходят."""
    try:
        from exchange import get_keltner_eth, _kc_cache_ts
        import exchange
        exchange._kc_cache_ts = 0  # Сбрасываем кеш для свежих данных
        kc = get_keltner_eth()
        d = kc.get("direction", "NEUTRAL")
        if d == "NEUTRAL":
            return True, kc  # Флет — пропускаем всё
        passed = d == direction
        return passed, kc
    except Exception:
        return True, {"direction": "NEUTRAL", "confirmed": False}


async def _pump_check(symbol: str) -> dict:
    """Проверяет Volume+OI+Funding. Возвращает dict с score, factors, text."""
    try:
        from exchange import check_pump_potential
        p = await asyncio.to_thread(check_pump_potential, symbol)
        if not p:
            return {"score": 0, "factors": [], "text": ""}
        text = ""
        factors = p.get("factors", [])
        if factors:
            lines = "\n".join(f"  {f}" for f in factors)
            label = p.get("label", "")
            if label:
                text = f"\n\n{label}\n{lines}"
            else:
                text = f"\n\n{lines}"
        p["text"] = text
        return p
    except Exception:
        return {"score": 0, "factors": [], "text": ""}


_last_kc_direction = None
_last_reversal_zone = None  # 'STRONG_BULL' / 'BULL' / 'NEUTRAL' / 'BEAR' / 'STRONG_BEAR'


def _reversal_zone(score: int) -> str:
    if score >= 70: return "STRONG_BULL"
    if score >= 30: return "BULL"
    if score <= -70: return "STRONG_BEAR"
    if score <= -30: return "BEAR"
    return "NEUTRAL"


async def _check_reversal_flip():
    """Алертит во все боты когда Reversal Meter пересекает зону (±30 или ±70)."""
    global _last_reversal_zone
    try:
        from reversal_meter import compute_score
        meter = await asyncio.to_thread(compute_score)
        score = meter["score"]
        direction = meter["direction"]
        strength = meter["strength"]
        zone = _reversal_zone(score)

        if _last_reversal_zone is None:
            _last_reversal_zone = zone
            print(f"[REVERSAL] Initial zone: {zone} (score={score})", flush=True)
            return

        if zone == _last_reversal_zone:
            return

        old = _last_reversal_zone
        _last_reversal_zone = zone
        print(f"[REVERSAL] {old} → {zone} (score={score})", flush=True)
        logger.info(f"Reversal zone change: {old} → {zone} (score={score})")
        # Записываем event для маркера на графиках
        try:
            from database import _market_events
            _market_events().insert_one({
                "at": utcnow(),
                "type": "reversal",
                "from": old,
                "to": zone,
                "score": score,
                "direction": direction,
                "strength": strength,
            })
        except Exception as e:
            logger.warning(f"[REVERSAL] event save fail: {e}")
        # Уведомления в боты отключены (решение 2026-04-17). Маркеры на
        # графиках — через /api/market-events.
    except Exception as e:
        print(f"[REVERSAL] ERROR: {e}", flush=True)


async def _check_kc_change():
    """Проверяет смену Keltner и алертит во все боты."""
    global _last_kc_direction
    try:
        from exchange import get_keltner_eth
        kc = get_keltner_eth()
        d = kc.get("direction", "NEUTRAL")

        if _last_kc_direction is None:
            _last_kc_direction = d
            print(f"[KC] Initial: {d}", flush=True)
            return

        if d != _last_kc_direction:
            old = _last_kc_direction
            _last_kc_direction = d
            print(f"[KC] CHANGED: {old} → {d}", flush=True)
            logger.info(f"KC CHANGED: {old} → {d}")
            # Записываем event для маркера на графиках
            try:
                from database import _market_events
                _market_events().insert_one({
                    "at": utcnow(),
                    "type": "kc",
                    "from": old,
                    "to": d,
                    "price": kc.get("price"),
                    "upper": kc.get("upper"),
                    "lower": kc.get("lower"),
                })
            except Exception as e:
                logger.warning(f"[KC] event save fail: {e}")
            # Уведомления в боты отключены (решение 2026-04-17).
            # Маркеры на графиках — через /api/market-events.
    except Exception as e:
        print(f"[KC] ERROR: {e}", flush=True)


def _kc_line(passed: bool, kc: dict) -> str:
    """Строка Keltner для Telegram."""
    d = kc.get("direction", "NEUTRAL")
    if d == "NEUTRAL":
        return "⚪ KC: NEUTRAL"
    emoji = "🟢" if d == "LONG" else "🔴"
    mark = "✅" if passed else "⚠️"
    return f"{emoji} KC: {d} · {mark}"


def _eth_line() -> str:
    """ETH/BTC контекст."""
    try:
        from exchange import get_eth_market_context
        ctx = get_eth_market_context()
        eth = ctx.get("eth_1h", 0)
        btc = ctx.get("btc_1h", 0)
        eth_e = "🟢" if eth >= 0 else "🔴"
        btc_e = "🟢" if btc >= 0 else "🔴"
        return f"📊 ETH {eth_e}{eth:+.2f}% · BTC {btc_e}{btc:+.2f}%"
    except Exception:
        return ""


def _market_block(pump: dict, kc_passed: bool, kc_data: dict) -> str:
    """Блок Рынок + Фильтры для всех ботов."""
    lines = []
    # HIGH POTENTIAL заголовок
    hp = pump.get("label", "")

    # Рынок
    lines.append("\n─── Рынок ───")
    vol = pump.get("volume_spike", 0)
    oi = pump.get("oi_change", 0)
    fund = pump.get("funding", 0)
    lines.append(f"📊 Vol ×{vol} | 📈 OI {oi:+.1f}% | 💰 Fund {fund:.3f}%")
    if hp:
        lines.append(hp)

    # Фильтры
    lines.append("\n─── Фильтры ───")
    lines.append(_kc_line(kc_passed, kc_data))
    lines.append(_eth_line())

    return "\n".join(lines)


async def _reversal_block(signal_direction: str | None = None) -> str:
    """Возвращает блок Reversal Meter для Telegram сообщения.
    Информативно: показывает согласуется ли сигнал с направлением разворота."""
    try:
        from reversal_meter import compute_score, format_telegram_block
        meter = await asyncio.to_thread(compute_score)
        return "\n" + format_telegram_block(meter, signal_direction)
    except Exception as e:
        logger.warning(f"[reversal-block] {e}", exc_info=True)
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

    _kl = _kl_block(signal.pair, signal.direction, utcnow(),
                    entry=current_price or signal.entry, tp=signal.tp1, sl=signal.sl)
    _stb = _st_block(signal.pair, signal.direction, signal.timeframe or "1h")
    text = (
        f"{dir_emoji} <b>DCA #4 ДОСТИГНУТ</b>\n"
        f"\n"
        f"<b>{pair}/USDT</b> · {signal.timeframe or '—'} · {dir_label}\n"
        f"{_kl}{_stb}"
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

    _kl = _kl_block(signal.pair, signal.direction, utcnow(),
                    entry=current_price or signal.entry, tp=signal.tp1, sl=signal.sl)
    _stb = _st_block(signal.pair, signal.direction, signal.timeframe or "1h")
    text = (
        f"🚀 <b>ВХОД ПОДТВЕРЖДЁН</b>\n"
        f"\n"
        f"<b>{pair}/USDT</b> · {signal.timeframe or '—'} · {dir_emoji} {dir_label}\n"
        f"{_kl}{_stb}"
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
        # Top Pick check — pattern + STRONG Confluence ≤ 48h в том же направлении
        try:
            from top_picks import tag_signal
            if tag_signal(s.id, s.source):
                await _send_top_pick_alert({"type": s.source, "pair": s.pair, "direction": s.direction,
                                            "entry": current, "pattern": pattern, "signal_id": s.id,
                                            "tp": s.tp1, "sl": s.sl})
        except Exception as e:
            logger.warning(f"[top_pick] {e}", exc_info=True)


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
        await _check_kc_change()
        try:
            await _check_reversal_flip()
        except Exception as e:
            print(f"[WATCHER] _check_reversal_flip ERROR: {e}", flush=True)
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
            ("paper_positions", lambda: _check_paper_positions()),
            ("cluster_outcomes", lambda: _check_cluster_outcomes()),
            ("fvg_scan", lambda: _check_forex_fvg_scan()),
            ("fvg_monitor", lambda: _check_forex_fvg_monitor()),
        ]:
            try:
                print(f"[WATCHER] step: {step_name}", flush=True)
                # fvg_scan: 22 forex через TwelveData батчами по 7 с 65с throttle (~4 мин)
                #         + 10 yfinance (~30s). Поднимаем таймаут до 600с с запасом.
                step_timeout = 600 if step_name == "fvg_scan" else 120
                await asyncio.wait_for(step_fn(), timeout=step_timeout)
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
    # Маячок — видеть, дошёл ли вызов до функции (для диагностики зависаний)
    try:
        from database import _events
        _events().insert_one({
            "at": utcnow(),
            "type": "cv_alert_called",
            "data": {"signal_id": signal.id, "pair": signal.pair, "pattern": pattern,
                     "has_chart": bool(chart_png)},
        })
    except Exception:
        pass
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

    sym = (signal.pair or "").replace("/", "").upper()
    # Каждый хелпер с timeout — раньше без timeout виснули минутами на Binance/Mongo
    try:
        _stp, _std = await asyncio.wait_for(
            asyncio.to_thread(_check_keltner_filter, signal.direction),
            timeout=5.0,
        )
    except Exception:
        _stp, _std = True, {}
    try:
        _pump = await asyncio.wait_for(_pump_check(sym), timeout=5.0)
    except Exception:
        _pump = {"score": 0, "factors": [], "label": "", "volume_spike": 0, "oi_change": 0}
    hp = "🔥🔥🔥 <b>HIGH POTENTIAL</b> 🔥🔥🔥\n\n" if _pump.get("label") else ""

    lvl = ""
    if s1: lvl += f"🟢 S1: <code>{s1}</code> | "
    if r1: lvl += f"🔴 R1: <code>{r1}</code>"

    _kl = _kl_block(signal.pair, signal.direction,
                    getattr(signal, "pattern_triggered_at", None) or utcnow(),
                    entry=current_price or signal.entry, tp=signal.tp1, sl=signal.sl)
    _stb = _st_block(signal.pair, signal.direction, "1h")
    text = (
        f"{hp}"
        f"🚀 <b>CRYPTOVIZOR · ПАТТЕРН</b>\n"
        f"\n"
        f"<b>{pair}/USDT</b> · 1h · {dir_emoji} <b>{dir_label}</b>\n"
        f"<code>{sym}</code>\n"
        f"{_kl}{_stb}"
        f"\n"
        f"─── Сигнал ───\n"
        f"🕯 Паттерн: <b>{pattern}</b>\n"
        f"🎯 Entry: <code>{signal.entry}</code> → Сейчас: <code>{current_price}</code>"
        f"{pnl_line}\n"
        f"{lvl}\n"
        f"{ai_line}"
        f"⚡ Тренд: {_fmt_trend(signal.trend)}\n"
        f"\n"
        f"{_market_block(_pump, _stp, _std)}"
    )
    try:
        text += await asyncio.wait_for(_reversal_block(signal.direction), timeout=5.0)
    except Exception:
        pass
    try:
        text += await asyncio.wait_for(_pending_cluster_block(pair, signal.direction), timeout=5.0)
    except Exception:
        pass
    # Сохраняем pump в БД
    try:
        from database import _signals as _sc
        _sc().update_one({"id": signal.id}, {"$set": {"pump_score": _pump.get("score", 0), "pump_factors": _pump.get("factors", [])}})
    except Exception:
        pass

    # BOT2 (@trendscryptobot) — text-only, фото никогда не поддерживало
    # (send_photo висит). Принудительно игнорируем chart_png и шлём send_message.
    # Timeout 12с на случай если Telegram лагает.
    sent_ok = False
    tg_response = None
    try:
        tg_response = await asyncio.wait_for(
            target_bot.send_message(_admin_chat_id, text, parse_mode="HTML"),
            timeout=12.0,
        )
        sent_ok = True
        # Логируем успех с message_id
        try:
            from database import _events
            _events().insert_one({
                "at": utcnow(),
                "type": "cv_alert_sent",
                "data": {
                    "signal_id": signal.id, "pair": pair, "pattern": pattern,
                    "message_id": getattr(tg_response, "message_id", None),
                    "with_photo": bool(chart_png),
                },
            })
        except Exception:
            pass
        logger.info(f"[CV-ALERT] sent #{signal.id} {pair} {pattern} → msg_id={getattr(tg_response, 'message_id', '?')}")
    except asyncio.TimeoutError:
        logger.error(f"[CV-ALERT] TIMEOUT #{signal.id} {pair} — Telegram не ответил за 12с")
        try:
            from database import _events
            _events().insert_one({"at": utcnow(), "type": "cv_alert_timeout",
                "data": {"signal_id": signal.id, "pair": pair}})
        except Exception:
            pass
    except Exception as e:
        logger.error(f"[CV-ALERT] FAIL #{signal.id} {pair}: {e}")
        try:
            from database import _events
            _events().insert_one({"at": utcnow(), "type": "cv_alert_error",
                "data": {"signal_id": signal.id, "pair": pair,
                         "error": f"{type(e).__name__}: {str(e)[:200]}"}})
        except Exception:
            pass

    # Дополнительные действия после отправки — не блокируют hot path
    try:
        await _paper_on_signal({"symbol": sym, "direction": signal.direction, "entry": current_price,
                                 "source": "cryptovizor", "pattern": pattern,
                                 "score": getattr(signal, "ai_score", None),
                                 "pump_vol": _pump.get("volume_spike",0),
                                 "pump_oi": _pump.get("oi_change",0)})
    except Exception as e:
        logger.warning(f"[CV-ALERT] paper fail #{signal.id}: {e}")
    try:
        await _cluster_check_on_signal(pair, signal.direction)
    except Exception as e:
        logger.warning(f"[CV-ALERT] cluster_check fail #{signal.id}: {e}")


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

                # AI score — сохраняет ai_score в БД (не влияет на роутинг)
                try:
                    await _ai_score_and_alert_pattern(s, strongest, current_price, s1, r1, chart_png, candles, db)
                except Exception as e:
                    logger.warning(f"[CV] ai_score fail #{s.id}: {e}")

                # SuperTrend ETH фильтр — фильтрует контртренд
                try:
                    st_passed, st_data = _check_keltner_filter(s.direction)
                except Exception as e:
                    logger.warning(f"[CV] keltner fail #{s.id}: {e}")
                    st_passed, st_data = True, {}

                # Все CV с паттерном идут в BOT2 через _send_cryptovizor_alert.
                # AI_SIGNAL ветка удалена (2026-04-18) — был отдельный путь в
                # BOT4 который ломал отправку, сейчас всё унифицировано.
                s.status = "ПАТТЕРН"
                s.result = f"AI:{s.ai_score or 0}"
                s.st_passed = st_passed
                db.commit()
                log_event(s.id, "cryptovizor_pattern", price=current_price,
                    data={"pattern": strongest, "s1": s1, "r1": r1},
                    message=f"Cryptovizor: {strongest}, ST={'✅' if st_passed else '❌'}")
                _broadcast("signal_new", {"id": s.id, "status": "ПАТТЕРН", "source": "cryptovizor"})
                # Алерт только если ST подтверждён. Fire-and-forget — если
                # функция виснет, следующие сигналы не ждут. Каждый вызов
                # в отдельной задаче с глобальным timeout 45s.
                if st_passed:
                    async def _fire_and_forget_alert(sig_obj, pat, cp, _s1, _r1, _png):
                        try:
                            await asyncio.wait_for(
                                _send_cryptovizor_alert(sig_obj, pat, cp, _s1, _r1, _png),
                                timeout=45.0,
                            )
                        except asyncio.TimeoutError:
                            logger.error(f"[CV] alert TIMEOUT 45s #{sig_obj.id} {sig_obj.pair}")
                            try:
                                from database import _events
                                _events().insert_one({"at": utcnow(), "type": "cv_alert_timeout_global",
                                    "data": {"signal_id": sig_obj.id, "pair": sig_obj.pair}})
                            except Exception:
                                pass
                        except Exception as e:
                            logger.error(f"[CV] alert exception #{sig_obj.id}: {e}")
                    asyncio.create_task(_fire_and_forget_alert(s, strongest, current_price, s1, r1, chart_png))
                # Top Pick check — CV pattern + STRONG Confluence ≤ 48h
                try:
                    from top_picks import tag_signal
                    if tag_signal(s.id, "cryptovizor"):
                        await _send_top_pick_alert({"type": "cryptovizor", "pair": s.pair, "direction": s.direction,
                                                    "entry": current_price, "pattern": strongest, "signal_id": s.id})
                except Exception as e:
                    logger.warning(f"[top_pick-cv] {e}", exc_info=True)
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
            # BOT4 отключён — всё идёт в BOT2 (см. комментарий в _send_ai_signal_alert)
            target_bot = _bot2 or _bot
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
                # AI-filter passed — просто пометим в БД для UI статистики.
                # Отдельный алерт НЕ отправляем — сигнал уже пришёл в BOT2
                # через основной поток _check_cryptovizor → _send_cryptovizor_alert
                # (AI_SIGNAL ветка удалена 2026-04-18).
                s.filter_reason = f"AI_PASSED:score={result['score']}"
                db.commit()
                log_event(s.id, "ai_passed", price=current,
                    data={"score": result["score"], "reasoning": result.get("reasoning")},
                    message=f"AI отобрал: score {result['score']}/10 (без доп. алерта)")
                _broadcast("signal_update", {"id": s.id, "status": "AI_PASSED", "source": "cryptovizor"})
                logger.info(f"AI Passed #{s.id} {s.pair} score={result['score']} (no extra alert)")
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
        from ai_client import get_ai_client
        client = get_ai_client()
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
        from ai_client import get_ai_client
        client = get_ai_client()
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
    # BOT4 отключён (токен невалиден — решение пользователя 2026-04-17).
    # AI signals теперь шлются в BOT2 (@trendscryptobot), как и обычные
    # CV-алерты. Fallback на _bot (главный) если BOT2 упал.
    target_bot = _bot2 or _bot

    # ── Маячок СРАЗУ (раньше был в середине — до него не доходило если
    # Claude API падал на _generate_ai_full_analysis/tg_summary) ──
    try:
        from database import _events
        _events().insert_one({"at": utcnow(), "type": "ai_alert_called",
            "data": {"signal_id": signal.id, "pair": signal.pair,
                     "bot_ready": bool(target_bot), "chat_set": bool(_admin_chat_id)}})
    except Exception:
        pass

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

    # FAST PATH (2026-04-17): Claude-блоки вынесены в background task.
    # Основной алерт уходит за 2-5 сек с базовой инфой. AI анализ
    # сохраняется в signal.comment позже и доступен в UI + может быть
    # отправлен reply-сообщением если нужно.
    tg_summary = None  # Быстрый алерт без саммари
    sym = (signal.pair or "").replace("/", "").upper()
    _st = ai_result.get("st", {})
    _stp = _st.get("confirmed", False) and _st.get("direction") == signal.direction if _st else True

    try:
        _pump = await asyncio.wait_for(_pump_check(sym), timeout=5.0)
    except asyncio.TimeoutError:
        logger.warning(f"[AI-ALERT] pump_check TIMEOUT #{signal.id}")
        _pump = {"score": 0, "factors": [], "label": "", "volume_spike": 0, "oi_change": 0}
    except Exception as e:
        logger.warning(f"[AI-ALERT] pump_check fail #{signal.id}: {e}")
        _pump = {"score": 0, "factors": [], "label": "", "volume_spike": 0, "oi_change": 0}

    hp = "🔥🔥🔥 <b>HIGH POTENTIAL</b> 🔥🔥🔥\n\n" if _pump.get("label") else ""

    lvl = ""
    if s1: lvl += f"🟢 S1: <code>{s1}</code> | "
    if r1: lvl += f"🔴 R1: <code>{r1}</code>"

    _kl = _kl_block(signal.pair, signal.direction, utcnow(),
                    entry=current_price or signal.entry, tp=signal.tp1, sl=signal.sl)
    _stb = _st_block(signal.pair, signal.direction, "1h")
    text = (
        f"{hp}"
        f"🤖 <b>AI SIGNAL · TOP PICK</b>\n"
        f"\n"
        f"<b>{pair}/USDT</b> · 1h · {dir_emoji} <b>{signal.direction}</b>\n"
        f"<code>{sym}</code>\n"
        f"{_kl}{_stb}"
        f"\n"
        f"─── Сигнал ───\n"
        f"🕯 Паттерн: <b>{signal.pattern_name}</b>\n"
        f"🎯 Entry: <code>{signal.entry}</code> → Сейчас: <code>{current_price}</code>\n"
        f"{lvl}\n"
        f"⭐ Score: <b>{score}/10</b> {score_bar}\n"
    )
    if tg_summary:
        text += f"\n📝 {tg_summary}\n"
    try:
        text += f"\n{_market_block(_pump, _stp, _st or {})}"
    except Exception as e:
        logger.warning(f"[AI-ALERT] market_block fail #{signal.id}: {e}")
    try:
        text += await asyncio.wait_for(_reversal_block(signal.direction), timeout=5.0)
    except asyncio.TimeoutError:
        logger.warning(f"[AI-ALERT] reversal_block TIMEOUT #{signal.id}")
    except Exception as e:
        logger.warning(f"[AI-ALERT] reversal_block fail #{signal.id}: {e}")
    try:
        text += await asyncio.wait_for(_pending_cluster_block(signal.pair, signal.direction), timeout=5.0)
    except asyncio.TimeoutError:
        logger.warning(f"[AI-ALERT] cluster_block TIMEOUT #{signal.id}")
    except Exception as e:
        logger.warning(f"[AI-ALERT] cluster_block fail #{signal.id}: {e}")

    try:
        from database import _signals as _sc2
        _sc2().update_one({"id": signal.id}, {"$set": {"pump_score": _pump.get("score", 0), "pump_factors": _pump.get("factors", [])}})
    except Exception as e:
        logger.warning(f"[AI-ALERT] pump save fail #{signal.id}: {e}")

    # Главная отправка — с fallback на BOT2/BOT если основной бот упал
    # (типичный случай: BOT4_BOT_TOKEN невалиден, но Bot объект создан).
    bots_to_try = [b for b in [target_bot, _bot2, _bot] if b is not None]
    # Убираем дубли сохраняя порядок
    seen_ids = set()
    unique_bots = []
    for b in bots_to_try:
        if id(b) not in seen_ids:
            seen_ids.add(id(b))
            unique_bots.append(b)

    sent = False
    last_err = None
    for i, b in enumerate(unique_bots):
        try:
            await b.send_message(_admin_chat_id, text, parse_mode="HTML")
            sent = True
            name = "BOT4" if b is _bot4 else "BOT2" if b is _bot2 else "BOT"
            logger.info(f"[AI-ALERT] sent #{signal.id} via {name}" + (" (fallback)" if i > 0 else ""))
            break
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            logger.warning(f"[AI-ALERT] attempt {i+1} failed #{signal.id}: {last_err}")

    if not sent:
        logger.error(f"[AI-ALERT] ALL BOTS FAILED #{signal.id}: {last_err}")
        try:
            from database import _events
            import traceback as _tb
            _events().insert_one({"at": utcnow(), "type": "ai_alert_all_failed",
                "data": {"signal_id": signal.id, "pair": signal.pair,
                         "error": last_err, "attempts": len(unique_bots)}})
        except Exception:
            pass

    try:
        await _paper_on_signal({"symbol": sym, "direction": signal.direction, "entry": current_price, "source": "ai_signal", "pattern": signal.pattern_name, "score": score, "pump_vol": _pump.get("volume_spike",0), "pump_oi": _pump.get("oi_change",0)})
    except Exception as e:
        logger.warning(f"[AI-ALERT] paper fail #{signal.id}: {e}")
    try:
        await _cluster_check_on_signal(signal.pair, signal.direction)
    except Exception as e:
        logger.warning(f"[AI-ALERT] cluster fail #{signal.id}: {e}")

    # ── Fire-and-forget: AI-анализ в фоне (не блокирует отправку) ──
    # Алерт уже ушёл. Claude-анализ сохранится в signal.comment для UI.
    if sent:
        asyncio.create_task(_ai_background_analysis(signal, current_price, s1, r1))


async def _ai_background_analysis(signal, current_price, s1, r1):
    """Фоновый AI-анализ через Claude. Сохраняется в signal.comment для
    отображения в UI. Не блокирует отправку Telegram-алерта."""
    try:
        full_analysis = await asyncio.wait_for(
            _generate_ai_full_analysis(signal, current_price, s1, r1),
            timeout=30.0,
        )
        if full_analysis:
            from database import _signals as _sc_bg
            _sc_bg().update_one({"id": signal.id}, {"$set": {"comment": full_analysis}})
            logger.info(f"[AI-BG] analysis saved #{signal.id}")
    except asyncio.TimeoutError:
        logger.warning(f"[AI-BG] full_analysis TIMEOUT #{signal.id}")
    except Exception as e:
        logger.warning(f"[AI-BG] full_analysis fail #{signal.id}: {e}")


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

        # Дедупликация — та же пара за последние 4 часа (обновляем время если повторно)
        import datetime
        existing = _anomalies().find_one({
            "symbol": r["symbol"],
            "detected_at": {"$gte": utcnow() - datetime.timedelta(hours=4)},
        })
        if existing:
            _anomalies().update_one({"_id": existing["_id"]}, {"$set": {"detected_at": now, "price": r["price"], "score": r["score"]}})
            continue

        # SuperTrend фильтр
        st_passed, st_data = _check_keltner_filter(r["direction"])

        doc = {
            "symbol": r["symbol"], "pair": r["pair"], "price": r["price"],
            "score": r["score"], "direction": r["direction"],
            "anomalies": r["anomalies"], "detected_at": now,
            "st_passed": st_passed,
        }
        _anomalies().insert_one(doc)
        anomaly_scan_state["found"] += 1
        results.append(r)
        # Invalidate journal cache — anomaly сразу появляется в UI
        try:
            from cache_utils import journal_cache
            journal_cache.invalidate("journal_all")
        except Exception:
            pass
        logger.info(f"Anomaly: {r['symbol']} score={r['score']} dir={r['direction']} ST={'✅' if st_passed else '❌'}")

        # Алерт только если ST подтверждён
        if r["score"] >= 10 and st_passed:
            r["_st"] = st_data
            await _send_anomaly_alert(r)
            await asyncio.sleep(1.5)  # Telegram rate limit protection

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

    sym = r.get("symbol", "")
    _st = r.get("_st", {})
    _stp = True if _st else True
    _pump = await _pump_check(sym)
    hp = "🔥🔥🔥 <b>HIGH POTENTIAL</b> 🔥🔥🔥\n\n" if _pump.get("label") else ""

    _kl = _kl_block(r.get("pair") or r.get("symbol", ""), r.get("direction"), utcnow(),
                    entry=r.get("price") or r.get("entry"), tp=r.get("tp") or r.get("tp1"), sl=r.get("sl"))
    _stb = _st_block(r.get("pair") or r.get("symbol", ""), r.get("direction"), "1h")
    text = (
        f"{hp}"
        f"⚠️ <b>АНОМАЛИЯ · {pair}/USDT</b>\n"
        f"\n"
        f"{dir_emoji} <b>{r['direction']}</b> · Цена: <code>{price}</code>\n"
        f"<code>{sym}</code>\n"
        f"Score: <b>{ws}</b>/15 {score_bar} · {count} индикаторов\n"
        f"{_kl}{_stb}"
        f"\n"
        f"─── Аномалии ───"
    )
    text += ftt_info
    text += delta_info
    if indicators:
        text += "\n" + "\n".join(f"  · {ind}" for ind in indicators)
    text += f"\n\n💡 <i>{conclusion}</i>"
    text += f"\n\n{_market_block(_pump, _stp, _st or {})}"
    text += await _reversal_block(r.get("direction"))
    text += await _pending_cluster_block(r.get("pair") or r["symbol"].replace("USDT","/USDT"), r.get("direction"))

    from database import _anomalies as _anc
    _anc().update_one({"symbol": r["symbol"], "score": r["score"]}, {"$set": {"pump_score": _pump.get("score", 0), "pump_factors": _pump.get("factors", [])}})

    try:
        await _bot3.send_message(_admin_chat_id, text, parse_mode="HTML")
        logger.info(f"Anomaly alert sent: {r.get('symbol')}")
        await _paper_on_signal({"symbol": r["symbol"], "direction": r["direction"], "entry": r.get("price",0), "source": "anomaly", "score": r.get("score",0), "pump_vol": _pump.get("volume_spike",0), "pump_oi": _pump.get("oi_change",0)})
        await _cluster_check_on_signal(r.get("pair") or r["symbol"].replace("USDT","/USDT"), r.get("direction"))
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

        # Дедупликация — та же пара + направление за 4 часа (обновляем время если повторно)
        existing = _confluence().find_one({
            "symbol": r["symbol"],
            "direction": r["direction"],
            "detected_at": {"$gte": utcnow() - datetime.timedelta(hours=4)},
        })
        if existing:
            # Обновляем время последнего обнаружения
            _confluence().update_one({"_id": existing["_id"]}, {"$set": {"detected_at": now, "price": r["price"], "score": r["score"]}})
            continue

        st_passed, st_data = _check_keltner_filter(r["direction"])

        # Гейт для score=4: пропускаем только если st_passed=True (KC-подтверждение)
        # Score ≥ 5 записываем всегда (STRONG и выше).
        if r["score"] == 4 and not st_passed:
            logger.debug(f"Confluence {r['symbol']} score=4 skip: ST=❌")
            continue

        doc = {
            "symbol": r["symbol"], "pair": r["pair"], "price": r["price"],
            "score": r["score"], "strength": r["strength"],
            "direction": r["direction"], "factors": r["factors"],
            "s1": r.get("s1"), "r1": r.get("r1"),
            "pattern": r.get("pattern"), "trend_tf": r.get("trend_tf"),
            "detected_at": now,
            "st_passed": st_passed,
        }
        insert_res = _confluence().insert_one(doc)
        confluence_scan_state["found"] += 1
        results.append(r)
        # Invalidate journal cache — confluence сразу появляется в UI
        try:
            from cache_utils import journal_cache
            journal_cache.invalidate("journal_all")
        except Exception:
            pass
        logger.info(f"Confluence: {r['symbol']} score={r['score']} {r['strength']} {r['direction']} ST={'✅' if st_passed else '❌'}")

        # Алерт в BOT4 только если ST подтверждён и score >= 5 (минимум STRONG).
        # Score=4 живёт в БД и кормит кластеры, но не спамит бот.
        if r["score"] >= 5 and st_passed:
            r["_st"] = st_data
            await _send_confluence_alert(r)
            await asyncio.sleep(1.5)  # Telegram rate limit protection

        # Top Pick check — Confluence STRONG + cluster/tradium/CV ≤ 48h
        if r["score"] >= 5:
            try:
                from top_picks import tag_confluence
                if tag_confluence(insert_res.inserted_id):
                    await _send_top_pick_alert({
                        "type": "confluence", "pair": r["pair"], "direction": r["direction"],
                        "entry": r["price"], "tp": r.get("r1"), "sl": r.get("s1"),
                        "pattern": r.get("pattern"),
                    })
            except Exception as e:
                logger.warning(f"[top_pick-conf] {e}", exc_info=True)

    confluence_scan_state["running"] = False
    confluence_scan_state["progress"] = 100
    confluence_scan_state["current"] = ""
    confluence_scan_state["last_scanned"] = len(pairs)
    confluence_scan_state["last_found"] = len(results)
    print(f"[CONFLUENCE] Done: {len(results)} new from {len(pairs)} pairs (total in DB: {_confluence().count_documents({})})", flush=True)
    if results:
        _broadcast("confluence_update", {"count": len(results)})


_bot5 = None
_bot7 = None
_bot8 = None
_bot9 = None  # Top Picks alerts


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


def _setup_bot7():
    global _bot7
    from config import BOT7_BOT_TOKEN
    if not BOT7_BOT_TOKEN:
        return
    try:
        from aiogram import Bot
        from aiogram.client.default import DefaultBotProperties
        from aiogram.enums import ParseMode
        _bot7 = Bot(token=BOT7_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        logger.info("BOT7 (Cluster Alerts) initialized")
    except Exception as e:
        logger.error(f"BOT7 init fail: {e}")


def _setup_bot8():
    global _bot8
    from config import BOT8_BOT_TOKEN
    if not BOT8_BOT_TOKEN:
        return
    try:
        from aiogram import Bot
        from aiogram.client.default import DefaultBotProperties
        from aiogram.enums import ParseMode
        _bot8 = Bot(token=BOT8_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        logger.info("BOT8 (Forex FVG) initialized")
    except Exception as e:
        logger.error(f"BOT8 init fail: {e}")


def _setup_bot9():
    global _bot9
    from config import BOT9_BOT_TOKEN
    if not BOT9_BOT_TOKEN:
        return
    try:
        from aiogram import Bot
        from aiogram.client.default import DefaultBotProperties
        from aiogram.enums import ParseMode
        _bot9 = Bot(token=BOT9_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        logger.info("BOT9 (Top Picks) initialized")
    except Exception as e:
        logger.error(f"BOT9 init fail: {e}")


def _setup_bot10():
    """BOT10 — SuperTrend signals (VIP / Triple MTF / Daily)."""
    global _bot10
    from config import BOT10_BOT_TOKEN
    if not BOT10_BOT_TOKEN:
        logger.info("BOT10_BOT_TOKEN не задан — SuperTrend алерты отключены")
        return
    try:
        from aiogram import Bot
        from aiogram.client.default import DefaultBotProperties
        from aiogram.enums import ParseMode
        _bot10 = Bot(token=BOT10_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        logger.info("BOT10 (SuperTrend) initialized")
    except Exception as e:
        logger.error(f"BOT10 init fail: {e}")




def _fmt_price(v):
    """Округление цены под magnitude — без ужасных хвостов."""
    if v is None:
        return "—"
    try:
        v = float(v)
    except Exception:
        return str(v)
    if v == 0:
        return "0"
    abs_v = abs(v)
    if abs_v >= 1000:  decimals = 2
    elif abs_v >= 10:  decimals = 3
    elif abs_v >= 1:   decimals = 4
    elif abs_v >= 0.1: decimals = 5
    elif abs_v >= 0.01: decimals = 6
    elif abs_v >= 0.001: decimals = 7
    else: decimals = 8
    return f"{v:.{decimals}f}".rstrip("0").rstrip(".")


async def _send_top_pick_alert(sig: dict):
    """👑 Top Pick alert — сигнал подтверждён STRONG Confluence ≤ 48h."""
    if not _bot9:
        _setup_bot9()
    if not _bot9 or not _admin_chat_id:
        return
    try:
        from datetime import datetime, timezone as _tz

        t_type = sig.get("type", "signal")
        type_map = {
            "cluster":     ("💠", "Cluster"),
            "tradium":     ("📡", "Tradium"),
            "cryptovizor": ("🚀", "Cryptovizor"),
            "confluence":  ("🎯", "Confluence"),
        }
        type_emoji, type_label = type_map.get(t_type, ("📍", t_type.title()))
        pair = (sig.get("pair") or "").replace("/USDT", "")
        direction = sig.get("direction", "LONG")
        is_long = direction in ("LONG", "BUY")
        dir_e = "🟢" if is_long else "🔴"
        entry = sig.get("entry")
        tp = sig.get("tp")
        sl = sig.get("sl")

        # Расчёт TP%, SL%, R:R
        tp_pct = sl_pct = rr = None
        try:
            if entry and tp:
                tp_pct = (tp - entry) / entry * 100 if is_long else (entry - tp) / entry * 100
            if entry and sl:
                sl_pct = (sl - entry) / entry * 100 if is_long else (entry - sl) / entry * 100
            if entry and tp and sl:
                reward = abs(tp - entry)
                risk = abs(entry - sl)
                if risk > 0: rr = reward / risk
        except Exception:
            pass

        # (убрано по запросу: сессия, текущая цена, сноска про базу бектеста)

        # Подтверждения
        confirmations = sig.get("confirmations") or []
        n_conf = sig.get("confirmations_count") or len(confirmations) or 1
        confs_lines = []
        for c in confirmations[:5]:
            c_src = c.get("source", "")
            c_emoji = {"confluence":"🎯","cluster":"💠","tradium":"📡","cryptovizor":"🚀"}.get(c_src, "•")
            at = c.get("at", "")
            age_str = ""
            try:
                dt = datetime.fromisoformat(at.replace("Z", "+00:00")) if at else None
                if dt:
                    # naive сравнение — убираем tz
                    dt_naive = dt.replace(tzinfo=None) if dt.tzinfo else dt
                    now_naive = utcnow()
                    age_h = (now_naive - dt_naive).total_seconds() / 3600
                    if age_h < 1: age_str = f"{int(age_h*60)}м назад"
                    elif age_h < 24: age_str = f"{int(age_h)}ч назад"
                    else: age_str = f"{int(age_h/24)}д назад"
            except Exception:
                pass
            score = c.get("score")
            strength = c.get("strength")
            label = f"score <b>{score}</b>" if score else (f"<b>{strength}</b>" if strength else "")
            confs_lines.append(f"  {c_emoji} {label}  <i>{age_str}</i>")
        confs_block = "\n".join(confs_lines)
        if len(confirmations) > 5:
            confs_block += f"\n  ... +{len(confirmations)-5}"

        conf_header = (f"Подтверждён <b>{n_conf}</b> сигналом{'ами' if n_conf!=1 else ''} других типов"
                       if t_type == "confluence"
                       else f"Подтверждён <b>{n_conf}</b> STRONG Confluence ≤ 48ч")

        # Cluster-специфичные доп поля (если есть)
        cluster_info = ""
        if t_type == "cluster":
            sources_n = sig.get("sources_count")
            signals_n = sig.get("signals_count")
            strength = sig.get("strength")
            extras = []
            if strength: extras.append(f"<b>{strength}</b>")
            if signals_n and sources_n: extras.append(f"<b>{signals_n}</b> сигналов / <b>{sources_n}</b> источников")
            if extras: cluster_info = "\n⚡ " + " · ".join(extras)

        # Dist to TP/SL percentages
        def _pct_str(pct, good_sign):
            if pct is None: return ""
            sign = "+" if pct >= 0 else ""
            return f" <b>({sign}{pct:.2f}%)</b>"

        _kl = _kl_block(sig.get("pair") or (pair + "/USDT"), direction, utcnow(),
                        entry=entry, tp=tp, sl=sl)
        _stb = _st_block(sig.get("pair") or (pair + "/USDT"), direction, "1h")
        text = (
            f"👑 <b>TOP PICK</b> · {type_emoji} {type_label}\n"
            f"\n"
            f"{dir_e} <b>{pair}/USDT</b> · <b>{direction}</b>"
            + (f" · R:R <b>1:{rr:.1f}</b>" if rr else "")
            + cluster_info
            + _kl
            + _stb
            + "\n\n"
        )
        text += f"✨ {conf_header}:\n{confs_block}\n\n"
        text += (
            f"📍 Entry <code>{_fmt_price(entry)}</code>\n"
            f"🎯 TP    <code>{_fmt_price(tp)}</code>{_pct_str(tp_pct, 1)}\n"
            f"🛑 SL    <code>{_fmt_price(sl)}</code>{_pct_str(sl_pct, -1)}\n"
        )
        if sig.get("pattern"):
            text += f"📋 Pattern: <i>{sig.get('pattern')}</i>\n"

        # Компактная сводка по бектесту
        text += f"\n📊 Backtest: WR <b>75%</b> · Avg <b>+0.75%</b> · PF <b>3.00</b>"

        await _bot9.send_message(_admin_chat_id, text, parse_mode="HTML")
        await asyncio.sleep(0.5)
        logger.info(f"[TOP PICK] 👑 alert sent: {t_type} {pair} {direction}")
    except Exception as e:
        logger.exception(f"top_pick alert: {e}")


# ── BOT8: Forex FVG alerts ──────────────────────────────────
def _fvg_direction_emoji(d):
    return "🟢" if d == "bullish" else "🔴"


def _fvg_top_pick_block(sig) -> str:
    """Блок '⭐ TOP PICK' для FVG alerts. Возвращает '' если не top pick."""
    if not sig.get("is_top_pick"):
        return ""
    score = sig.get("confluence_score", 0)
    breakdown = sig.get("score_breakdown") or []
    # Форматируем bullets
    lines = []
    for b in breakdown:
        src = b.get("source", "")
        pts = b.get("points", 0)
        if src == "base_fvg":
            lines.append(f"  • Base FVG +{pts}")
        elif src == "multi_tf":
            lines.append(f"  • Multi-TF (1H↔4H) +{pts} 🔥")
        elif src == "smart_level":
            kind = b.get("kind", "")
            lines.append(f"  • Smart Level ({kind}) +{pts}")
        elif src == "multi_pair_correlation":
            n = b.get("correlated_pairs", 0)
            theme = b.get("theme", "")
            lines.append(f"  • Multi-Pair ({n} pairs, {theme}) +{pts}")
        elif src == "atr_strong":
            lines.append(f"  • ATR strong (≥1.5×) +{pts}")
    return (
        f"\n⭐⭐⭐ <b>TOP PICK · score {score}/7</b> ⭐⭐⭐\n"
        + "\n".join(lines) + "\n"
    )


async def _send_fvg_formed_alert(sig):
    """FVG DETECTED — формирование."""
    if not _bot8:
        _setup_bot8()
    if not _bot8 or not _admin_chat_id:
        return
    try:
        dir_e = _fvg_direction_emoji(sig["direction"])
        from datetime import datetime, timezone as _tz
        ts = sig.get("formed_ts") or 0
        hour = datetime.fromtimestamp(ts, tz=_tz.utc).hour if ts else 0
        session = "🇺🇸 NY" if 13 <= hour < 21 else "🇬🇧 London" if 8 <= hour < 16 else "—"
        size_pct = sig.get("fvg_size_rel", 0) * 100
        body_pct_val = sig.get("impulse_body_ratio")
        body_line = f"  • Impulse body: {body_pct_val*100:.0f}% ✅\n" if body_pct_val else ""
        source = sig.get("source", "scan")
        src_tag = "📡 TV" if source == "tv_webhook" else "🔎 Scan"
        top_pick_block = _fvg_top_pick_block(sig)
        text = (
            f"⚡⚡⚡ <b>FVG DETECTED</b> ⚡⚡⚡ {src_tag}\n"
            f"\n"
            f"<b>{sig['instrument']}</b> · {sig.get('timeframe','1H')} · {dir_e} <b>{sig['direction'].upper()}</b>\n"
            f"{top_pick_block}"
            f"\n"
            f"📍 Formed price: <code>{sig.get('formed_price','?')}</code>\n"
            f"🎯 Zone: <code>{sig['fvg_bottom']:.5f}</code> — <code>{sig['fvg_top']:.5f}</code>\n"
            f"\n"
            f"📊 Quality:\n"
            f"  • Size: {size_pct:.3f}%\n"
            f"{body_line}"
            f"  • Session: {session}\n"
            f"\n"
            f"⏳ <b>Entry limit:</b> <code>{sig['entry_price']:.5f}</code>\n"
            f"🛑 SL: <code>{sig['sl_price']:.5f}</code>\n"
        )
        await _bot8.send_message(_admin_chat_id, text, parse_mode="HTML")
        await asyncio.sleep(0.5)
    except Exception as e:
        logger.debug(f"fvg formed alert: {e}")


async def _send_fvg_entry_alert(sig):
    """RETEST ENTRY — ретест случился, сделка открыта."""
    if not _bot8:
        _setup_bot8()
    if not _bot8 or not _admin_chat_id:
        return
    try:
        dir_e = _fvg_direction_emoji(sig["direction"])
        top_pick_block = _fvg_top_pick_block(sig)
        text = (
            f"🎯🎯🎯 <b>RETEST ENTRY</b> 🎯🎯🎯\n"
            f"{top_pick_block}"
            f"\n"
            f"<b>{sig['instrument']}</b> · {dir_e} <b>{'LONG' if sig['direction']=='bullish' else 'SHORT'}</b>\n"
            f"\n"
            f"📍 Entry: <code>{sig.get('entered_price', sig['entry_price']):.5f}</code>\n"
            f"🛑 SL:    <code>{sig['sl_price']:.5f}</code>\n"
            f"📈 Trail: активируется после +1R\n"
            f"\n"
            f"🎯 Hybrid v2 · следим за trailing"
        )
        await _bot8.send_message(_admin_chat_id, text, parse_mode="HTML")
        await asyncio.sleep(0.5)
    except Exception as e:
        logger.debug(f"fvg entry alert: {e}")


async def _send_fvg_close_alert(sig, status):
    """TP or SL — сделка закрыта."""
    if not _bot8:
        _setup_bot8()
    if not _bot8 or not _admin_chat_id:
        return
    try:
        R = sig.get("outcome_R") or 0
        peak = sig.get("peak_R") or 0
        dir_e = _fvg_direction_emoji(sig["direction"])
        is_tp = status == "TP"
        header = "✅✅✅" if is_tp else "❌❌❌"
        emoji = "✅ TP" if is_tp else "❌ SL"
        outcome = "🚀 ПРИБЫЛЬ" if is_tp else "📉 УБЫТОК"
        tp_tag = "⭐ " if sig.get("is_top_pick") else ""
        text = (
            f"{header} <b>{tp_tag}{emoji} · {R:+.2f}R</b> {header}\n"
            f"\n"
            f"<b>{sig['instrument']}</b> · {dir_e} {'LONG' if sig['direction']=='bullish' else 'SHORT'} closed\n"
            f"\n"
            f"Entry: <code>{sig.get('entered_price', sig['entry_price']):.5f}</code>\n"
            f"Exit:  <code>{sig.get('exit_price', 0):.5f}</code>\n"
            f"PnL:   <b>{R:+.2f}R</b>\n"
            f"Peak:  +{peak:.2f}R\n"
            f"\n"
            f"{outcome}"
        )
        await _bot8.send_message(_admin_chat_id, text, parse_mode="HTML")
        await asyncio.sleep(0.5)
    except Exception as e:
        logger.debug(f"fvg close alert: {e}")


# ── Scan/monitor для watcher loop ──────────────────────────
_fvg_last_scan_ts = 0

async def _check_forex_fvg_scan():
    """Периодический скан (раз в scan_interval_min минут).

    По-умолчанию ОТКЛЮЧЁН — сигналы FVG теперь приходят через TradingView Webhook
    (см. /api/tv-webhook + tv_webhook.py). Чтобы включить legacy scan_all
    детекцию (через yfinance/TD), установи FVG_SCAN_ENABLED=1.
    """
    global _fvg_last_scan_ts
    if os.getenv("FVG_SCAN_ENABLED", "0").strip() not in ("1", "true", "yes", "on"):
        # webhook-only mode — scan_all отключён чтобы не плодить дубли
        return
    try:
        from fvg_scanner import scan_all, get_config
        cfg = get_config()
        interval = cfg.get("scan_interval_min", 10) * 60
        import time as _t
        now = _t.time()
        if now - _fvg_last_scan_ts < interval:
            return
        # Получаем snapshot waiting до скана, потом — после
        from database import _fvg_signals
        before = {(s["instrument"], s.get("formed_ts")) for s in _fvg_signals().find(
            {"status": "WAITING_RETEST"}, {"instrument": 1, "formed_ts": 1}
        )}
        print("[FVG-SCAN] starting scan_all...", flush=True)
        stats = await asyncio.to_thread(scan_all)
        # Ставим timestamp ПОСЛЕ успешного скана — защита от timeout-loop
        _fvg_last_scan_ts = _t.time()
        # FORMED alerts отключены — формирование FVG не значит торговать,
        # реальные сигналы = ENTRY / TP / SL (в _check_forex_fvg_monitor).
        # Состояние FORMED видно в UI вкладки Forex FVG (таблица WAITING).
        print(f"[FVG-SCAN] done: {stats['total_instruments']} instruments, {stats['new_fvgs']} new in DB", flush=True)
    except Exception as e:
        import traceback
        print(f"[FVG-SCAN] ERROR: {e}\n{traceback.format_exc()[-500:]}", flush=True)


async def _check_forex_fvg_monitor():
    """Monitor retest + TP/SL (каждый тик).
    КРИТИЧНО: alerts в отдельных try/except — если один alert падает
    (плохой chart_path, сетевая ошибка, и т.д.), остальные события всё
    равно обрабатываются. Сам monitor_signals отделён от alerts.
    """
    # 1. Запускаем monitor_signals (БД работа) — отдельный try/except
    try:
        from fvg_scanner import monitor_signals
        events = await asyncio.to_thread(monitor_signals)
    except Exception:
        logger.exception("[FVG-MON] monitor_signals crashed")
        return

    # 2. Алерты — каждый в своём try/except (ошибка одного не блокирует остальных)
    entered_count = 0
    tp_count = 0
    sl_count = 0
    for sig in events.get("entered", []):
        try:
            await _send_fvg_entry_alert(sig)
            entered_count += 1
        except Exception as e:
            logger.error(f"[FVG-MON] entry alert fail {sig.get('instrument','?')}: {e}")
    for sig in events.get("closed_tp", []):
        try:
            await _send_fvg_close_alert(sig, "TP")
            tp_count += 1
        except Exception as e:
            logger.error(f"[FVG-MON] TP alert fail {sig.get('instrument','?')}: {e}")
    for sig in events.get("closed_sl", []):
        try:
            await _send_fvg_close_alert(sig, "SL")
            sl_count += 1
        except Exception as e:
            logger.error(f"[FVG-MON] SL alert fail {sig.get('instrument','?')}: {e}")
    if entered_count or tp_count or sl_count:
        logger.info(f"[FVG-MON] entered={entered_count} tp={tp_count} sl={sl_count}")


async def _pending_cluster_block(pair: str, direction: str, triggered: bool = False) -> str:
    """Блок 'Cluster Status' для обычных алертов.
    triggered=True если этот сигнал только что триггернул кластер."""
    try:
        from cluster_detector import compute_pending_state
        st = compute_pending_state(pair, direction)
        if triggered or st["state"] == "ready":
            return (
                "\n─── ⚡⚡⚡ CLUSTER TRIGGERED ⚡⚡⚡ ───\n"
                f"🚀 {st['count']}/{st['min']} сигналов по {pair} {direction}\n"
                "→ Отправлено в BOT7 Cluster Alerts"
            )
        if st["state"] == "pending" and st["count"] >= 1:
            return (
                "\n─── ⚡ Cluster Status ───\n"
                f"🔔 {st['count']}/{st['min']} · ждём ещё {st['min'] - st['count']} {direction} "
                f"(окно {st['time_left_h']}ч)"
            )
    except Exception as e:
        logger.warning(f"[pending-block] {e}", exc_info=True)
    return ""


async def _send_cluster_alert(cl: dict):
    """Отправляет cluster алерт в BOT7."""
    if not _bot7:
        _setup_bot7()
    if not _bot7 or not _admin_chat_id:
        return
    direction = cl["direction"]
    dir_emoji = "🟢" if direction == "LONG" else "🔴"
    strength = cl.get("strength", "NORMAL")
    strength_emoji = {"MEGA": "🔥🔥🔥", "STRONG": "⚡⚡", "NORMAL": "⚡", "RISKY": "⚠️"}.get(strength, "⚡")
    pair = cl["pair"].replace("/USDT", "")
    tp_pct = abs((cl["tp_price"] - cl["trigger_price"]) / cl["trigger_price"] * 100)
    sl_pct = abs((cl["sl_price"] - cl["trigger_price"]) / cl["trigger_price"] * 100)

    # Участники
    src_icons = {"cryptovizor": "🚀", "tradium": "📡", "anomaly": "⚠️", "confluence": "🎯"}
    src_names = {"cryptovizor": "Молот/Паттерн", "tradium": "Tradium Setup",
                 "anomaly": "Anomaly", "confluence": "Confluence"}
    participants = []
    for s in cl["signals_in_cluster"]:
        icon = src_icons.get(s["source"], "•")
        t = s["at"].strftime("%H:%M") if hasattr(s["at"], "strftime") else str(s["at"])[11:16]
        price = s.get("price", "?")
        meta = s.get("meta", {}) or {}
        extra = ""
        if s["source"] == "confluence":
            extra = f" (score {meta.get('score','?')})"
        elif s["source"] == "cryptovizor" and meta.get("pattern_name"):
            extra = f" {meta['pattern_name']}"
        elif s["source"] == "anomaly" and meta.get("types"):
            extra = f" {','.join(meta['types'][:2])}"
        participants.append(f"{icon} {t} @ {price}{extra}")

    # Reversal combo
    rev_score = cl.get("reversal_score", 0)
    rev_dir = cl.get("reversal_direction", "NEUTRAL")
    if strength == "MEGA":
        combo_line = f"🔥 <b>MEGA</b> — Cluster + Strong Reversal ({rev_score:+d})"
    elif strength == "STRONG":
        combo_line = f"⚡ <b>STRONG</b> — Cluster + Reversal aligned ({rev_score:+d})"
    elif strength == "RISKY":
        combo_line = f"⚠️ <b>RISKY</b> — Cluster ПРОТИВ Reversal ({rev_score:+d}) — снизить размер"
    else:
        combo_line = f"⚡ NORMAL · Reversal {rev_score:+d} {rev_dir.lower()}"

    # Рекомендация leverage
    from cluster_detector import get_config
    cfg = get_config()
    if strength == "MEGA":
        lev_rec = f"Leverage ×{cfg['strong_boost']:.0f} recommended"
    elif strength == "STRONG":
        lev_rec = f"Leverage ×{cfg['leverage_boost']:.0f} recommended"
    else:
        lev_rec = "Leverage ×1-2 (обычный)"

    text = (
        f"{strength_emoji} <b>CLUSTER SIGNAL · {strength}</b> {strength_emoji}\n"
        f"\n"
        f"<b>{pair}/USDT</b> · {dir_emoji} <b>{direction}</b>\n"
        f"📍 Цена сейчас: <code>{cl['trigger_price']}</code>\n"
        f"\n"
        f"📊 <b>{cl['signals_count']} сигналов</b> из <b>{cl['sources_count']} источников</b>\n"
        f"\n"
        f"─── Участники ───\n"
        + "\n".join(participants) + "\n"
        + _kl_block(cl.get("pair") or cl.get("symbol", ""), cl.get("direction"), utcnow())
        + f"\n"
        f"─── Cluster + Reversal ───\n"
        f"{combo_line}\n"
        f"\n"
        f"💡 <i>Backtest WR: 78.6%</i>"
    )

    try:
        await _bot7.send_message(_admin_chat_id, text, parse_mode="HTML")
        logger.info(f"Cluster alert sent: {pair} {direction} strength={strength}")
        # Paper Trading pickup
        try:
            await _paper_on_signal({
                "symbol": cl["symbol"], "pair": cl["pair"],
                "direction": direction, "entry": cl["trigger_price"],
                "tp1": cl["tp_price"], "sl": cl["sl_price"],
                "source": "cluster",
                "is_cluster": True,
                "cluster_strength": strength,
                "sources_count": cl["sources_count"],
                "reversal_score": rev_score,
            })
        except Exception as e:
            logger.warning(f"[paper-cluster] {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Cluster alert fail: {e}")


async def _send_cluster_close_alert(cl: dict):
    """NO-OP: пользователь просил в BOT7 только сигналы, без TP/SL уведомлений.
    Статус закрытия виден в UI и в БД, но в Telegram не шлём."""
    return


async def _cluster_check_on_signal(pair: str, direction: str, at=None):
    """Вызывается после save каждого сигнала (CV/Anom/Conf/Tradium).
    Если сработал триггер — создаёт кластер и шлёт в BOT7."""
    try:
        from cluster_detector import should_trigger_cluster, create_cluster
        import datetime as _dt
        at = at or _dt.datetime.utcnow()
        trigger, sigs, count = should_trigger_cluster(pair, direction, at)
        if not trigger:
            return None
        cl = create_cluster(pair, direction, sigs, at)
        if cl:
            await _send_cluster_alert(cl)
            # Top Pick alert если кластер подтверждён STRONG Confluence
            if cl.get("is_top_pick"):
                await _send_top_pick_alert({
                    "type": "cluster", "pair": cl.get("pair"), "direction": direction,
                    "entry": cl.get("trigger_price"), "tp": cl.get("tp_price"), "sl": cl.get("sl_price"),
                    "confirmations": cl.get("top_pick_confirmations", []),
                    "confirmations_count": cl.get("top_pick_confirmations_count", 0),
                })
            # WebSocket broadcast
            try:
                _broadcast("cluster_new", {"id": cl["id"], "pair": cl["pair"], "direction": direction})
            except Exception:
                pass
        return cl
    except Exception as e:
        logger.warning(f"[cluster-check] {e}", exc_info=True)
        return None


async def _check_cluster_outcomes():
    """Периодическая проверка открытых кластеров на TP/SL."""
    try:
        from cluster_detector import check_cluster_outcomes
        from database import _clusters
        open_clusters = list(_clusters().find({"status": "OPEN"}))
        if not open_clusters:
            return
        pairs = list({c.get("pair") for c in open_clusters if c.get("pair")})
        prices = await get_prices_any(pairs)
        closed = check_cluster_outcomes(prices)
        for cl in closed:
            await _send_cluster_close_alert(cl)
            try:
                _broadcast("cluster_close", {"id": cl["id"], "status": cl["status"]})
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"[cluster-outcomes] {e}", exc_info=True)


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

    sym = r.get("symbol", "")
    _st = r.get("_st", {})
    _stp = True
    _pump = await _pump_check(sym)
    hp = "🔥🔥🔥 <b>HIGH POTENTIAL</b> 🔥🔥🔥\n\n" if _pump.get("label") else ""

    lvl = ""
    if r.get("s1"): lvl += f"🟢 S1: <code>{r['s1']}</code> | "
    if r.get("r1"): lvl += f"🔴 R1: <code>{r['r1']}</code>"

    # Яркий заголовок для score >= 5
    if score >= 5:
        strong_header = (
            f"⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐\n"
            f"🏆 <b>STRONG CONFLUENCE · {score}/6</b> 🏆\n"
            f"⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐\n\n"
        )
    else:
        strong_header = ""

    _kl = _kl_block(r.get("pair") or r.get("symbol", ""), r.get("direction"), utcnow(),
                    entry=r.get("price") or r.get("entry"), tp=r.get("tp") or r.get("tp1"), sl=r.get("sl"))
    _stb = _st_block(r.get("pair") or r.get("symbol", ""), r.get("direction"), "1h")
    text = (
        f"{hp}"
        f"{strong_header}"
        f"🎯 <b>CONFLUENCE · {strength_label}</b>\n"
        f"\n"
        f"<b>{pair}/USDT</b> · {dir_emoji} <b>{r['direction']}</b>\n"
        f"<code>{sym}</code>\n"
        f"Цена: <code>{price}</code>\n"
        f"{lvl}\n"
        f"Score: <b>{score}/6</b> {filled}\n"
        f"Тренд: {tf_line}\n"
        f"{_kl}{_stb}"
        f"\n"
        f"─── Факторы ───\n"
    )
    text += "\n".join(checks)
    text += f"\n\n💡 <i>{conclusion}</i>"
    text += f"\n\n{_market_block(_pump, _stp, _st or {})}"
    text += await _reversal_block(r.get("direction"))
    text += await _pending_cluster_block(r.get("pair") or r["symbol"].replace("USDT","/USDT"), r.get("direction"))

    from database import _confluence as _cfc
    _cfc().update_one({"symbol": r["symbol"], "score": r["score"]}, {"$set": {"pump_score": _pump.get("score", 0), "pump_factors": _pump.get("factors", [])}})

    try:
        await _bot5.send_message(_admin_chat_id, text, parse_mode="HTML")
        logger.info(f"Confluence alert: {r['symbol']}")
        await _paper_on_signal({"symbol": r["symbol"], "direction": r["direction"], "entry": r.get("price",0), "source": "confluence", "pattern": r.get("pattern",""), "score": r.get("score",0), "pump_vol": _pump.get("volume_spike",0), "pump_oi": _pump.get("oi_change",0)})
        await _cluster_check_on_signal(r.get("pair") or r["symbol"].replace("USDT","/USDT"), r.get("direction"))
    except Exception as e:
        logger.error(f"Confluence alert fail: {e}")


# ── Paper Trading positions check ─────────────────────────────────────

async def _check_paper_positions():
    """Проверяет открытые Paper Trading позиции на TP/SL.
    КРИТИЧНО: alert отправляется ДО ai_review_trade — если AI фейлит
    (Claude API down / JSON parse / timeout), сигнал всё равно дойдёт.
    Каждая сделка обработана в своём try/except, чтобы ошибка одной
    не блокировала остальные."""
    try:
        import paper_trader as pt
        positions = pt.get_open_positions()
        if not positions:
            return
        pairs = list({p.get("pair", p["symbol"].replace("USDT", "/USDT")) for p in positions})
        prices = await get_prices_any(pairs)
        closed = pt.check_positions(prices)
    except Exception as e:
        logger.warning(f"[paper-positions] fetch/check fail: {e}", exc_info=True)
        return

    # Обрабатываем каждое закрытие ИЗОЛИРОВАННО
    for c in closed:
        try:
            import paper_trader as pt
            trades, _ = pt._get_collections()
            trade = trades.find_one({"trade_id": c["trade_id"]})
            if not trade:
                continue
            # 1. СНАЧАЛА отправляем alert (критично — без review если AI упал)
            try:
                await pt._send_close_alert(trade, "")
            except Exception as ae:
                logger.error(f"[paper-positions] close alert fail #{c['trade_id']}: {ae}")
            # [ОТКЛЮЧЕНО] ai_review_trade — система rule-based, без AI-уроков.
        except Exception as e:
            logger.warning(f"[paper-positions] close handler fail #{c.get('trade_id')}: {e}", exc_info=True)


async def _send_live_confirmation_alert(pending: dict) -> None:
    """BOT11 подтверждение для live сделки. Когда confirmation_required=True
    AI решил войти — шлём сюда карту с inline кнопками ✅/❌.
    Токен хранится в pending.confirmation_token, callback приходит через
    /api/live/confirm."""
    try:
        from config import BOT11_BOT_TOKEN
        if not BOT11_BOT_TOKEN or not _admin_chat_id:
            logger.info("[live-confirm] BOT11 not configured")
            return
        from aiogram import Bot
        from aiogram.client.default import DefaultBotProperties
        from aiogram.enums import ParseMode
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

        bot11 = Bot(token=BOT11_BOT_TOKEN,
                    default=DefaultBotProperties(parse_mode=ParseMode.HTML))

        sig = pending.get("signal_data", {})
        dec = pending.get("decision", {})
        env = pending.get("env", "testnet")
        token = pending.get("confirmation_token", "")

        env_emoji = "🟡" if env == "testnet" else "🔴"
        dir_emoji = "🟢" if sig.get("direction") == "LONG" else "🔴"
        size_pct = dec.get("size_pct", 0)
        lev = dec.get("leverage", 0)

        text = (
            f"{env_emoji} <b>LIVE {env.upper()} — подтверждение</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{dir_emoji} <b>{sig.get('symbol','')}</b> · <b>{sig.get('direction','')}</b>\n"
            f"Entry: <code>{sig.get('entry')}</code>\n"
            f"TP1: <code>{dec.get('tp1')}</code> · SL: <code>{dec.get('sl')}</code>\n"
            f"Size: <b>{size_pct}%</b> · Lev: <b>×{lev}</b>\n"
            f"Source: {sig.get('source','?')}\n\n"
            f"<b>Reasoning:</b>\n<i>{str(dec.get('reasoning',''))[:500]}</i>\n\n"
            f"⏱ Токен истекает через 15 мин"
        )

        # Inline buttons с callback data содержащим токен
        kbd = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Исполнить", callback_data=f"live_approve:{token}"),
            InlineKeyboardButton(text="❌ Отказ", callback_data=f"live_reject:{token}"),
        ]])
        await bot11.send_message(_admin_chat_id, text, reply_markup=kbd, parse_mode="HTML")
    except Exception as e:
        logger.warning(f"[live-confirm] send fail: {e}")


async def _paper_on_signal(signal_data: dict):
    """Роутер сигнала по режиму торговли (paper | testnet | real).
    Paper → ai_decide + запись в MongoDB (симуляция).
    Testnet/Real → live_trader с обязательной safety-проверкой + (опционально)
                    подтверждение в BOT11.

    Плюс: параллельно запускает Verified Entry Checker (rule-based 8 проверок),
    и если verdict=GO шлёт в @topmonetabot с эмоджи ✨.
    """
    # ── ФИЛЬТР: только пары торгуемые на DEFAULT бирже (BingX) ──
    # Paper фильтрует по default exchange (по умолчанию BingX, можно сменить).
    # Сигналы для не-листенных пар пропускаются — paper не открывает.
    try:
        from exchange_symbols import is_symbol_supported, get_default_exchange
        sym_check = (signal_data.get("symbol") or "").upper()
        if sym_check and not is_symbol_supported(sym_check):
            ex_name = get_default_exchange()
            logger.info(f"[paper-signal] skip {sym_check} — not on {ex_name.upper()} Futures USDT-perp")
            return
    except Exception as _fe:
        # Если symbols registry глючит — НЕ блокируем сигнал, пропускаем дальше
        logger.debug(f"[paper-signal] symbol filter error: {_fe}")

    # [УБРАНО] Pump fallback — он был нужен когда использовался AI (Claude
    # отказывал при pump_vol=0). Теперь система rule-based, check_entry
    # сам делает Vol/OI fallback через check_pump_potential (с кешем 120с).

    # ── Verified Entry Checker (параллельно, не блокирует основной поток) ──
    # @topmonetabot = наш BOT9 (Top Picks) — verified ✨ сообщения идут в него.
    # Fallback: если BOT9 не инициализирован — в главный BOT (_bot).
    try:
        global _bot, _bot9, _admin_chat_id
        from config import VERIFIED_TOPIC_ID
        import verified_entry as ve
        if not _bot9:
            _setup_bot9()  # ленивая инициализация если ещё не было
        verified_bot = _bot9 or _bot
        asyncio.create_task(
            ve.run_verified_check(signal_data, bot=verified_bot,
                                  chat_id=_admin_chat_id,
                                  topic_id=VERIFIED_TOPIC_ID)
        )
    except Exception:
        logger.debug("[verified-check] schedule fail", exc_info=True)

    try:
        # Paper-trader делает ВСЕ проверки и принимает решение.
        # Если paper открыл позицию — testnet/real зеркалят её один-в-один
        # (та же пара, направление, leverage, size_pct, TP, SL).
        # Если paper отказал — live аккаунты тоже не открывают.
        # Кроме того, у каждого live-аккаунта есть свой kill_switch и
        # max_positions — они применяются ПОВЕРХ paper-решения.
        import paper_trader as pt
        paper_pos = await pt.on_signal(signal_data)

        if not paper_pos:
            # Paper отклонил сигнал → testnet/real не открываются (точное копирование).
            return

        # Прокидываем paper_trade_id чтобы UI мог показать "open в paper И в testnet"
        try:
            signal_data["paper_trade_id"] = paper_pos.get("trade_id")
        except Exception:
            pass

        # Multi-account live execution — копирование paper-решения
        try:
            import live_safety as ls
            accounts = ls.get_enabled_accounts()
        except Exception as e:
            logger.debug(f"[live-accounts] list fail: {e}")
            accounts = []

        if accounts:
            import live_trader as lt
            # Точная копия paper-параметров (decision)
            mirror_decision = {
                "enter": True,
                "leverage": paper_pos.get("leverage", 2),
                "size_pct": paper_pos.get("size_pct", 3),
                "tp1": paper_pos.get("tp1"),
                "sl": paper_pos.get("sl"),
                "reasoning": (paper_pos.get("ai_reasoning")
                              or paper_pos.get("reasoning")
                              or "mirror of paper"),
            }
            for acc in accounts:
                # Каждый аккаунт независим — не блокируем друг друга
                try:
                    asyncio.create_task(
                        lt.mirror_paper_for_account(signal_data, mirror_decision, acc),
                        name=f"live-mirror-{acc.get('_id','?')}",
                    )
                except Exception as le:
                    logger.warning(f"[live-{acc.get('_id','?')}] mirror schedule fail: {le}",
                                   exc_info=True)
    except Exception as e:
        logger.warning(f"[paper-signal] {e}", exc_info=True)


async def _live_sync_loop():
    """Фоновая синхронизация live позиций с биржей (каждые 30с).
    Защищена 30-секундным timeout — если биржа hang'нет, скип."""
    import asyncio as _asyncio
    # Стартовая задержка чтобы основной watcher подняться без нагрузки
    await _asyncio.sleep(45)
    while True:
        try:
            import live_trader as lt
            results = await _asyncio.wait_for(lt.sync_all_accounts(), timeout=30.0)
            auto = sum(r.get("auto_closed", 0) for r in results if isinstance(r, dict))
            if auto > 0:
                logger.info(f"[live-sync] auto-closed {auto} position(s)")
        except _asyncio.TimeoutError:
            logger.warning("[live-sync] sync timed out — exchange slow, will retry")
        except Exception:
            logger.debug("[live-sync] sync loop error", exc_info=True)
        await _asyncio.sleep(30)


async def _paper_to_live_mirror_loop():
    """Зеркало paper → live (каждые 15с). Timeout 25с."""
    import asyncio as _asyncio
    await _asyncio.sleep(50)
    while True:
        try:
            import live_trader as lt
            res = await _asyncio.wait_for(lt.paper_to_live_sync_check(), timeout=25.0)
            synced = res.get("synced", 0) if isinstance(res, dict) else 0
            if synced > 0:
                logger.info(f"[paper→live] synced {synced} action(s)")
        except _asyncio.TimeoutError:
            logger.warning("[paper→live] mirror sync timed out")
        except Exception:
            logger.debug("[paper→live] sync loop error", exc_info=True)
        await _asyncio.sleep(15)


async def _ui_prewarm_loop():
    """Прогреваем кеши endpoint'ов параллельно (gather) каждые 5 мин.
    Все taski wrapped в timeout — не блокируют worker.
    Tradium-setups убран т.к. compute = 23с (HTTP fetch from forum) и съедает
    ресурсы; пользователь увидит первый раз 23с при открытии Tradium вкладки."""
    import asyncio as _asyncio
    await _asyncio.sleep(60)  # стартовая задержка побольше

    async def _warm_one(name, coro_factory, timeout=20.0):
        try:
            await _asyncio.wait_for(coro_factory(), timeout=timeout)
        except _asyncio.TimeoutError:
            logger.debug(f"[ui-prewarm] {name} timeout")
        except Exception as e:
            logger.debug(f"[ui-prewarm] {name} fail: {e}")

    while True:
        try:
            # Все прогревы параллельно через gather — суммарное время = max
            tasks = []

            # market-phase (120s TTL)
            tasks.append(_warm_one("market-phase", lambda: _asyncio.to_thread(
                __import__("market_phase").get_market_phase, False
            )))

            # pending-clusters (90s TTL)
            async def _pc():
                from cluster_detector import get_pending_clusters
                await _asyncio.to_thread(get_pending_clusters, 50)
            tasks.append(_warm_one("pending-clusters", _pc))

            # reversal-meter (~60s TTL)
            try:
                async def _rm():
                    from reversal_meter import compute as _rm_compute
                    await _asyncio.to_thread(_rm_compute)
                tasks.append(_warm_one("reversal-meter", _rm))
            except ImportError:
                pass

            # top-picks (60s TTL)
            try:
                from admin import api_top_picks
                tasks.append(_warm_one("top-picks", lambda: api_top_picks(96, 200)))
            except Exception:
                pass

            # fvg-signals
            try:
                from admin import api_fvg_signals
                tasks.append(_warm_one("fvg-signals", lambda: api_fvg_signals("all", 200, "")))
            except Exception:
                pass

            # Запускаем всё параллельно — не последовательно (быстрее в 5 раз)
            await _asyncio.gather(*tasks, return_exceptions=True)

        except Exception:
            logger.debug("[ui-prewarm] loop error", exc_info=True)
        await _asyncio.sleep(300)  # 5 минут вместо 60с — снижаем нагрузку


async def _live_balance_refresh_loop():
    """Каждые 5 минут подтягивает реальный exchange balance в account.balance.
    Это гарантирует что UI показывает актуальный free на бирже, не paper-balance."""
    import asyncio as _asyncio
    await _asyncio.sleep(120)  # стартовая задержка
    while True:
        try:
            from live_safety import get_enabled_accounts, _live_accounts
            from database import utcnow
            import live_trader as lt
            for acc in get_enabled_accounts():
                aid = acc.get("_id")
                try:
                    res = await _asyncio.wait_for(
                        _asyncio.to_thread(lt.test_connection_for_account, acc),
                        timeout=20.0,
                    )
                    if res and res.get("ok"):
                        new_bal = res.get("usdt_total")
                        if new_bal is not None:
                            _live_accounts().update_one(
                                {"_id": aid},
                                {"$set": {"balance": float(new_bal),
                                          "balance_synced_from_exchange": True,
                                          "updated_at": utcnow()}},
                            )
                except _asyncio.TimeoutError:
                    pass
                except Exception as e:
                    logger.debug(f"[balance-refresh-{aid}] error: {e}")
        except Exception:
            pass
        await _asyncio.sleep(300)  # 5 минут


async def _exchange_symbols_refresh_loop():
    """Раз в час обновляем списки пар на ОБЕИХ биржах.
    Каждый refresh защищён 25-секундным timeout — если биржа не ответит,
    цикл идёт дальше с устаревшим кешем (paper использует fallback).

    На старте даём 60с задержки чтобы основной watcher успел подняться
    БЕЗ нагрузки от HTTP запросов к биржам."""
    import asyncio as _asyncio
    # Стартовая задержка чтобы дать подняться основному циклу
    await _asyncio.sleep(60)

    while True:
        try:
            from exchange_symbols import refresh_supported_symbols
            for ex_name in ("bingx", "binance"):
                try:
                    res = await _asyncio.wait_for(
                        _asyncio.to_thread(refresh_supported_symbols, ex_name),
                        timeout=25.0,
                    )
                    if isinstance(res, dict) and res.get("ok"):
                        added = len(res.get("added") or [])
                        removed = len(res.get("removed") or [])
                        if added or removed:
                            logger.warning(
                                f"[{ex_name}-symbols] +{added}/-{removed} → "
                                f"now {res.get('count')} pairs"
                            )
                except _asyncio.TimeoutError:
                    logger.warning(f"[{ex_name}-symbols] refresh timed out — using cached/fallback")
                except Exception:
                    logger.debug(f"[{ex_name}-symbols] refresh error", exc_info=True)
        except Exception:
            logger.debug("[exchange-symbols] refresh loop error", exc_info=True)
        await _asyncio.sleep(3600)  # 1 час до следующего refresh


_watcher_running = False

async def start_watcher():
    global _watcher_running
    _watcher_running = True
    print(f"[WATCHER] Started (interval={POLL_INTERVAL}s)", flush=True)
    logger.info(f"Price watcher запущен (интервал {POLL_INTERVAL}s)")
    # Прогрев ВСЕХ ботов сразу на старте (раньше была ленивая init только при первом алерте —
    # если init падал молча, алерты терялись тихо до рестарта)
    for name, fn in [("bot5", _setup_bot5), ("bot7", _setup_bot7),
                     ("bot8", _setup_bot8), ("bot9", _setup_bot9),
                     ("bot10", _setup_bot10)]:
        try:
            fn()
        except Exception as e:
            logger.error(f"[{name}] init fail: {e}")
    # Запускаем параллельный ST-tracker loop (check_all_pairs каждые 5 мин)
    try:
        asyncio.create_task(_st_tracker_loop())
        logger.info("[st-tracker] background loop started")
    except Exception:
        logger.exception("[st-tracker] failed to start loop")
    # Candles prewarm — каждые 3 мин для топ-80 активных пар
    try:
        asyncio.create_task(_candles_prewarm_loop())
        logger.info("[prewarm] candles loop started")
    except Exception:
        logger.exception("[prewarm] failed to start loop")
    # ETH/KC prewarm — каждые 4 мин, чтобы рендер /signals не упирался
    # в холодный HTTP к Binance (раньше 20+ сек на холодный старт страницы)
    try:
        asyncio.create_task(_eth_kc_prewarm_loop())
        logger.info("[prewarm] ETH/KC loop started")
    except Exception:
        logger.exception("[prewarm] ETH/KC loop failed to start")
    # Market phase loop — каждые 3 мин определяем фазу + alert при смене
    try:
        asyncio.create_task(_market_phase_loop())
        logger.info("[market-phase] loop started")
    except Exception:
        logger.exception("[market-phase] loop failed to start")
    # Live sync — каждые 30с синхронизируем позиции live-аккаунтов с биржей
    try:
        asyncio.create_task(_live_sync_loop())
        logger.info("[live-sync] background loop started")
    except Exception:
        logger.exception("[live-sync] failed to start loop")
    # Paper→Live mirror — каждые 15с зеркалит partials/SL moves/full close
    try:
        asyncio.create_task(_paper_to_live_mirror_loop())
        logger.info("[paper→live] background loop started")
    except Exception:
        logger.exception("[paper→live] failed to start loop")
    # Exchange symbols refresh — каждый час подтягиваем актуальные списки
    # пар по всем поддерживаемым биржам (Binance + BingX)
    try:
        asyncio.create_task(_exchange_symbols_refresh_loop())
        logger.info("[exchange-symbols] background loop started")
    except Exception:
        logger.exception("[exchange-symbols] failed to start loop")
    # Live balance refresh — каждые 5 мин обновляем account.balance из биржи
    try:
        asyncio.create_task(_live_balance_refresh_loop())
        logger.info("[live-balance-refresh] background loop started")
    except Exception:
        logger.exception("[live-balance-refresh] failed to start loop")
    # UI prewarm — прогрев тяжёлых endpoint'ов после cold-start
    try:
        asyncio.create_task(_ui_prewarm_loop())
        logger.info("[ui-prewarm] background loop started")
    except Exception:
        logger.exception("[ui-prewarm] failed to start loop")
    # [ОТКЛЮЧЕНО] AI memory refresh + AI review open positions —
    # система переведена на rule-based (Entry Checker 8 проверок + TP ladder).
    # AI больше не используется для торговых решений.
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
