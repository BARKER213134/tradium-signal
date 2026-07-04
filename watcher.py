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
_bot16 = None   # 🐋 WHALE signals (Range Breakout)
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
    # Ленивая инициализация BOT16 (WHALE)
    if not _bot16:
        try: _setup_bot16()
        except Exception: pass
    while True:
        try:
            from supertrend_tracker import check_all_pairs
            await check_all_pairs(alert_enabled=True)
        except Exception:
            logger.exception("[st-tracker] loop crashed, retry in 5 min")
        await _asyncio.sleep(300)


async def _whale_scanner_loop():
    """🐋 WHALE safety-net scanner — каждые 30 мин. TG dispatch напрямую
    из fired_docs (раньше был баг: query по created_at brought 0 because
    scanner uses flip_time как created_at, а cutoff искал 2-min window)."""
    import asyncio as _asyncio
    await _asyncio.sleep(60)
    while True:
        try:
            from whale_detector import scan_recent_flips_for_whale
            stats = await _asyncio.to_thread(scan_recent_flips_for_whale, None, 6)
            if stats.get('fired', 0) > 0:
                logger.info(f'[whale-scan-loop] fired {stats["fired"]} '
                             f'(tiers={stats["by_tier"]}) — sending TG')
                # FIX: шлём из fired_docs напрямую, не запрашиваем DB по дате.
                for doc in stats.get('fired_docs', []):
                    try:
                        await _whale_send_telegram(doc)
                    except Exception as _e:
                        logger.debug(f'[whale-scan-loop] tg send {doc.get("pair")}: {_e}')
        except Exception:
            logger.exception('[whale-scan-loop] crashed')
        await _asyncio.sleep(30 * 60)


async def _setup_verdict_loop():
    """🎰 Setup-Checker verdict loop — каждые 60s обновляет коллекцию
    pair_verdicts с одним verdict per pair. Journal endpoints джойнят
    эту коллекцию к ЛЮБОМУ signal (whale/shark/supertrend/cv_flip/conf/etc).

    Architecture:
      - Collect unique pairs из всех source collections (последние 48h)
      - Per pair: setup_checker.get_compact_verdict() с 5-мин кешем
      - Upsert в pair_verdicts collection {pair, verdict, updated_at}
      - Process max 30 unique pairs per cycle → ~6-15 sec runtime
      - Re-process pair если updated_at > 10 min (свежие данные)
    """
    import asyncio as _asyncio
    from datetime import datetime, timezone, timedelta
    await _asyncio.sleep(120)
    while True:
        try:
            from database import _get_db, utcnow
            from setup_checker import get_compact_verdict
            db = _get_db()
            cutoff = datetime.now(timezone.utc) - timedelta(hours=48)
            stale_cutoff = datetime.now(timezone.utc) - timedelta(minutes=10)

            # Собираем уникальные pairs из всех актуальных source collections
            unique_pairs: set = set()
            try:
                # new_strategy_signals (whale/shark/combo/etc)
                for s in db.new_strategy_signals.find(
                    {'created_at': {'$gte': cutoff}}, {'pair': 1}
                ).limit(500):
                    if s.get('pair'): unique_pairs.add(s['pair'])
                # supertrend (ST MTF / VIP)
                for s in db.supertrend_signals.find(
                    {'flip_at': {'$gte': cutoff}, 'tier': {'$in': ['vip', 'mtf']}},
                    {'pair': 1}
                ).limit(500):
                    if s.get('pair'): unique_pairs.add(s['pair'])
                # confluence
                for s in db.confluence.find(
                    {'detected_at': {'$gte': cutoff}}, {'pair': 1}
                ).limit(300):
                    if s.get('pair'): unique_pairs.add(s['pair'])
            except Exception as e:
                logger.debug(f'[verdict-loop] collect pairs: {e}')

            if not unique_pairs:
                await _asyncio.sleep(60)
                continue

            # Find pairs which need verdict refresh (no verdict OR stale > 10 min)
            need_refresh: list = []
            try:
                pv_col = db.pair_verdicts
                pv_col.create_index('pair', unique=True)
                existing = {p['pair']: p.get('updated_at')
                            for p in pv_col.find(
                                {'pair': {'$in': list(unique_pairs)}},
                                {'pair': 1, 'updated_at': 1})}
                for p in unique_pairs:
                    upd = existing.get(p)
                    if upd is None:
                        need_refresh.append(p)
                    elif upd and upd.tzinfo is None:
                        upd = upd.replace(tzinfo=timezone.utc)
                    if upd and upd < stale_cutoff:
                        need_refresh.append(p)
            except Exception:
                need_refresh = list(unique_pairs)

            logger.info(f'[verdict-loop] {len(unique_pairs)} unique pairs, '
                         f'{len(need_refresh)} need refresh')
            # Process max 30 pairs per cycle
            processed = 0
            for pair in need_refresh[:30]:
                if not pair: continue
                try:
                    verdict = await _asyncio.to_thread(get_compact_verdict, pair)
                    db.pair_verdicts.update_one(
                        {'pair': pair},
                        {'$set': {
                            'pair': pair, 'verdict': verdict,
                            'updated_at': utcnow(),
                        }},
                        upsert=True,
                    )
                    processed += 1
                except Exception as _e:
                    logger.debug(f'[verdict-loop] {pair}: {_e}')
            logger.info(f'[verdict-loop] processed {processed} verdicts')
        except Exception:
            logger.exception('[verdict-loop] crashed')
        await _asyncio.sleep(60)


async def _shark_scanner_loop():
    """🦈 SHARK safety-net scanner — каждые 30 мин (mirror WHALE).
    TG dispatch напрямую из fired_docs."""
    import asyncio as _asyncio
    await _asyncio.sleep(90)  # offset от WHALE
    while True:
        try:
            from shark_detector import scan_recent_flips_for_shark
            stats = await _asyncio.to_thread(scan_recent_flips_for_shark, None, 6)
            if stats.get('fired', 0) > 0:
                logger.info(f'[shark-scan-loop] fired {stats["fired"]} '
                             f'(tiers={stats["by_tier"]}) — sending TG')
                for doc in stats.get('fired_docs', []):
                    try:
                        await _shark_send_telegram(doc)
                    except Exception as _e:
                        logger.debug(f'[shark-scan-loop] tg send {doc.get("pair")}: {_e}')
        except Exception:
            logger.exception('[shark-scan-loop] crashed')
        await _asyncio.sleep(30 * 60)


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
    # Bounded task — если _warm_st_top_pairs зависнет, asyncio.wait_for cancel'ит
    # его через 60с. Без этого guard task мог висеть бесконечно → worker leak.
    async def _warm_bounded():
        try:
            await _asyncio.wait_for(_warm_st_top_pairs(), timeout=60.0)
        except _asyncio.TimeoutError:
            logger.warning("[prewarm] _warm_st_top_pairs timeout 60s — cancelled")
        except Exception:
            logger.debug("[prewarm] _warm_st_top_pairs error", exc_info=True)
    _asyncio.create_task(_warm_bounded())

    while True:
        try:
            await _asyncio.sleep(300)  # 5 минут (Pro plan: возвращаем активный refresh)
            await _asyncio.to_thread(get_keltner_eth)
            await _asyncio.to_thread(get_eth_market_context)
            # На Pro можем себе позволить ST+pump prewarm для топ-10
            # Bounded — переиспользуем _warm_bounded из start блока
            _asyncio.create_task(_warm_bounded())
        except Exception:
            logger.exception("[prewarm] ETH/KC loop error")


async def _candles_prewarm_loop():
    """Фоновый прогрев candles cache. Снижено с 40→25 hot пар + sleep
    0.03→0.08 чтобы НЕ забивать threadpool — он нужен для UI запросов.
      — топ-25 пар каждые 5 мин на 3 TF (15m/1h/4h) [было 40×4]
      — cold-50 каждые 15 мин на 2 TF (1h/4h) [было 100]
    Total tasks per cycle: 25×3=75 (было 250) → меньше CPU давление.
    """
    import asyncio as _asyncio
    HOT_TFS = ["15m", "1h", "4h"]
    COLD_TFS = ["1h", "4h"]
    tick = 0
    while True:
        try:
            from supertrend_tracker import get_tracked_pairs
            from admin import warm_candles_cache
            pairs = await _asyncio.to_thread(get_tracked_pairs)
            hot = pairs[:25]
            cold = pairs[25:75] if len(pairs) > 25 else []
            warmed_hot = 0
            for p in hot:
                for tf in HOT_TFS:
                    try:
                        if await _asyncio.to_thread(warm_candles_cache, p, tf, 200):
                            warmed_hot += 1
                    except Exception:
                        pass
                    await _asyncio.sleep(0.08)
            # Cold пары — только раз в 3-й цикл (~15 мин)
            warmed_cold = 0
            if tick % 3 == 0:
                for p in cold:
                    for tf in COLD_TFS:
                        try:
                            if await _asyncio.to_thread(warm_candles_cache, p, tf, 200):
                                warmed_cold += 1
                        except Exception:
                            pass
                        await _asyncio.sleep(0.08)
            tick += 1
            logger.info(f"[prewarm] hot={warmed_hot} cold={warmed_cold} (hot {len(hot)} × {len(HOT_TFS)}TF + cold {len(cold)} × {len(COLD_TFS)}TF)")
        except Exception:
            logger.exception("[prewarm] loop crashed")
        await _asyncio.sleep(300)  # 5 минут на Pro плане


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
    """Key Levels удалены (источник — Telegram топики — отключён 2026-07-02).
    Стаб чтобы не трогать 6 alert-билдеров, использующих _kl_block."""
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
    # SQLAlchemy query — sync. В async-функции блокирует event loop
    # на время роста таблицы Signal. Выносим в to_thread.
    def _query_dca():
        return list(
            db.query(Signal)
            .filter(Signal.status == "СЛЕЖУ")
            .filter(Signal.dca4_triggered == False)
            .filter(Signal.dca4 != None)
            .filter(Signal.pair != None)
            .all()
        )
    signals = await asyncio.to_thread(_query_dca)
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
            await asyncio.to_thread(db.commit)
            log_event(
                s.id, "dca4_hit", price=current,
                data={"dca4": s.dca4, "direction": s.direction, "pair": s.pair},
                message=f"DCA #4 достигнут @ {current}",
            )
            _broadcast("signal_update", {"id": s.id, "status": "ОТКРЫТ"})


async def _check_patterns(db):
    """Этап 2: ОТКРЫТ → ПАТТЕРН при обнаружении подтверждающего паттерна."""
    def _query_open():
        return list(
            db.query(Signal)
            .filter(Signal.status == "ОТКРЫТ")
            .filter(Signal.pattern_triggered == False)
            .filter(Signal.pair != None)
            .all()
        )
    signals = await asyncio.to_thread(_query_open)
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
        await asyncio.to_thread(db.commit)
        log_event(
            s.id, "pattern_detected", price=current,
            data={"pattern": pattern, "pair": s.pair, "direction": s.direction},
            message=f"Паттерн {pattern} подтвердил вход",
        )
        _broadcast("signal_update", {"id": s.id, "status": "ПАТТЕРН"})
        # Top Picks удалены (2026-07-02)


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
    def _query_tp_sl():
        q_ = (
            db.query(Signal)
            .filter(Signal.status.in_(["ОТКРЫТ", "ПАТТЕРН"]))
            .filter(Signal.pair != None)
        )
        if allowed_ids is not None:
            if not allowed_ids:
                return None
            q_ = q_.filter(Signal.id.in_(allowed_ids))
        return list(q_.all())
    signals = await asyncio.to_thread(_query_tp_sl)
    if signals is None or not signals:
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
        await asyncio.to_thread(db.commit)
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
    def _query_no_dca4():
        return list(
            db.query(Signal)
            .filter(Signal.status == "СЛЕЖУ")
            .filter(Signal.is_filtered == False)
            .filter(Signal.has_chart == True)
            .filter(Signal.dca4 == None)
            .filter(Signal.chart_path != None)
            .limit(3)
            .all()
        )
    signals = await asyncio.to_thread(_query_no_dca4)

    # 2) Сигналы с dca4 но без ai_score — досчитываем оценку
    def _query_need_score():
        return list(
            db.query(Signal)
            .filter(Signal.has_chart == True)
            .filter(Signal.chart_path != None)
            .filter(Signal.dca4 != None)
            .filter(Signal.ai_score == None)
            .limit(2)
            .all()
        )
    need_score = await asyncio.to_thread(_query_need_score)
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
                await asyncio.to_thread(db.commit)
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
        retry_count = await asyncio.to_thread(
            _events().count_documents,
            {"signal_id": s.id, "type": "ai_retry"},
        )
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
            await asyncio.to_thread(db.commit)
            log_event(
                s.id, "ai_recovered",
                data={"dca4": s.dca4},
                message=f"AI восстановил DCA4={s.dca4}",
            )


async def _filter_stuck(db):
    """Сигналы СЛЕЖУ без dca4/entry/tp1/sl никогда не смогут продвинуться —
    помечаем их как отфильтрованные, чтобы не висели в UI вечно."""
    def _query_stuck():
        return list(
            db.query(Signal)
            .filter(Signal.status == "СЛЕЖУ")
            .filter(Signal.is_filtered == False)
            .filter((Signal.dca4 == None) | (Signal.entry == None) | (Signal.tp1 == None) | (Signal.sl == None))
            .all()
        )
    stuck = await asyncio.to_thread(_query_stuck)
    for s in stuck:
        missing = []
        if s.dca4 is None: missing.append("dca4")
        if s.entry is None: missing.append("entry")
        if s.tp1 is None: missing.append("tp1")
        if s.sl is None: missing.append("sl")
        s.is_filtered = True
        s.filter_reason = "missing: " + ", ".join(missing)
    if stuck:
        await asyncio.to_thread(db.commit)
        logger.info(f"Отфильтровано {len(stuck)} застрявших сигналов")


async def _check_once():
    """Порядок важен: фильтрация → DCA4 → TP/SL (для уже открытых ранее)
    → pattern → DCA4 заново не нужно. Grace period: только что открытые
    сигналы (в этом тике) не проверяются на TP/SL — отложим до следующего тика."""
    db = SessionLocal()
    try:
        print("[WATCHER] _check_once start", flush=True)
        # KC + reversal_flip — оба с timeout 30с чтобы не блокировать tick
        try:
            await asyncio.wait_for(_check_kc_change(), timeout=30.0)
        except asyncio.TimeoutError:
            print("[WATCHER] _check_kc_change TIMEOUT", flush=True)
        except Exception as e:
            print(f"[WATCHER] _check_kc_change ERROR: {e}", flush=True)
        try:
            await asyncio.wait_for(_check_reversal_flip(), timeout=30.0)
        except asyncio.TimeoutError:
            print("[WATCHER] _check_reversal_flip TIMEOUT", flush=True)
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

        # Снимок id до _check_dca4 — чтобы не проверять TP/SL на свежеоткрытых.
        # SQLAlchemy .all() — sync, выносим в to_thread (без этого блокировал
        # event loop при росте таблицы Signal → весь _check_once застревал).
        def _snapshot_opened():
            return {
                s.id for s in db.query(Signal.id, Signal.status)
                .filter(Signal.status.in_(["ОТКРЫТ", "ПАТТЕРН"]))
                .all()
            }
        opened_before = await asyncio.to_thread(_snapshot_opened)

        for step_name, step_fn in [
            ("dca4", lambda: _check_dca4(db)),
            ("patterns", lambda: _check_patterns(db)),
            ("tp_sl", lambda: _check_tp_sl(db, allowed_ids=opened_before)),
            # cryptovizor вынесен в fire-and-forget bg task ниже — раньше
            # обрабатывал до 20 СЛЕЖУ signals sequentially с render_chart
            # (matplotlib sync) + Claude API call на каждый = 60-100с per
            # tick, блокировал event loop, /healthz timeout, графики
            # не грузились. Теперь tick проходит быстро, cryptovizor
            # работает фоном с собственной DB session.
            ("paper_positions", lambda: _check_paper_positions()),
            ("fvg_scan", lambda: _check_forex_fvg_scan()),
            ("fvg_monitor", lambda: _check_forex_fvg_monitor()),
        ]:
            try:
                print(f"[WATCHER] step: {step_name}", flush=True)
                # Heartbeat per step — диагностика какой step тормозит.
                # Раньше только tick_start/tick_done в heartbeat — невозможно
                # понять застрял ли в dca4/patterns/tp_sl/cryptovizor/etc.
                await _write_heartbeat(f"step_{step_name}")
                step_timeout = 90 if step_name == "fvg_scan" else 120
                await asyncio.wait_for(step_fn(), timeout=step_timeout)
            except asyncio.TimeoutError:
                print(f"[WATCHER] step '{step_name}' TIMEOUT", flush=True)
                await _write_heartbeat(f"step_{step_name}_timeout",
                                        {"timeout_s": step_timeout})
            except Exception as e:
                print(f"[WATCHER] step '{step_name}' FAILED: {e}", flush=True)
                await _write_heartbeat(f"step_{step_name}_error",
                                        {"error": str(e)[:100]})
    finally:
        db.close()

    # Аномалии + Confluence + Cryptovizor — fire-and-forget (раньше await блокировал tick).
    # Эти задачи тяжёлые (1000+ пар scan / 20 signals × render_chart × Claude AI),
    # 60-200с каждая. При await tick застревал, /healthz timeout, графики не грузились.
    # Теперь tick завершается за ~5-10с, фоновые задачи работают параллельно.
    # Guard "уже запущен" — новый tick не запустит второй task если предыдущий ещё крутится.
    global _confluence_bg_task
    # Anomaly scanner удалён (2026-07-02) — источник выпилен по решению юзера
    if _confluence_bg_task is None or _confluence_bg_task.done():
        async def _safe_confluence():
            try:
                await _check_confluence()
            except Exception as e:
                print(f"[CONFLUENCE] ERROR: {e}", flush=True)
        _confluence_bg_task = asyncio.create_task(_safe_confluence())

    # Cryptovizor bg task удалён вместе с CV ingestion (2026-07-01)


# Background tasks для тяжёлых scans/обработки — чтобы не блокировать tick.
# Раньше await блокировал _check_once на 60-200с (anomaly/confluence 1000+
# pairs sequential scan + cryptovizor 20 signals × render_chart × Claude AI);
# tick застревал → /healthz timeout → графики не грузились.
_confluence_bg_task: "asyncio.Task | None" = None








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

    from futures_data import get_liquid_pairs, _refresh_batch_cache
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
    print(f"[CONFLUENCE] Scanning {len(pairs)} pairs (chunked parallel)", flush=True)

    now = utcnow()
    results = []

    # ── Phase 1: parallel scan_confluence через gather в чанках ──
    # Раньше sequential await на 1000+ парах = 60-200с. Чанки по 10 параллельно
    # = ~12с. Безопасно для Binance rate limit (1200 weight/min).
    CHUNK_SIZE = 10
    scanned = []
    for chunk_start in range(0, len(pairs), CHUNK_SIZE):
        chunk = pairs[chunk_start:chunk_start + CHUNK_SIZE]
        confluence_scan_state["current"] = chunk[0].replace("USDT", "")
        confluence_scan_state["progress"] = int((chunk_start / len(pairs)) * 100)
        try:
            chunk_results = await asyncio.gather(
                *[asyncio.to_thread(scan_confluence, sym) for sym in chunk],
                return_exceptions=True,
            )
        except Exception as e:
            logger.warning(f"[CONFLUENCE] chunk gather fail at idx {chunk_start}: {e}")
            continue
        for sym, r in zip(chunk, chunk_results):
            if isinstance(r, Exception):
                continue
            scanned.append((sym, r))

    # ── Phase 2: sequential обработка ──
    for symbol, r in scanned:
        if not r:
            continue

        # Дедупликация — та же пара + направление за 4 часа (обновляем время если повторно)
        existing = await asyncio.to_thread(
            _confluence().find_one,
            {"symbol": r["symbol"], "direction": r["direction"],
             "detected_at": {"$gte": utcnow() - datetime.timedelta(hours=4)}},
        )
        if existing:
            # Обновляем время последнего обнаружения
            await asyncio.to_thread(
                _confluence().update_one,
                {"_id": existing["_id"]},
                {"$set": {"detected_at": now, "price": r["price"], "score": r["score"]}},
            )
            continue

        try:
            st_passed, st_data = await asyncio.wait_for(
                asyncio.to_thread(_check_keltner_filter, r["direction"]),
                timeout=10.0,
            )
        except (asyncio.TimeoutError, Exception):
            st_passed, st_data = True, {}

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
        insert_res = await asyncio.to_thread(_confluence().insert_one, doc)
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

        # Top Picks удалены (2026-07-02)

    confluence_scan_state["running"] = False
    confluence_scan_state["progress"] = 100
    confluence_scan_state["current"] = ""
    confluence_scan_state["last_scanned"] = len(pairs)
    confluence_scan_state["last_found"] = len(results)
    print(f"[CONFLUENCE] Done: {len(results)} new from {len(pairs)} pairs (total in DB: {_confluence().count_documents({})})", flush=True)
    if results:
        _broadcast("confluence_update", {"count": len(results)})


_bot5 = None
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


def _setup_bot16():
    """BOT16 — 🐋 WHALE Range Breakout signals."""
    global _bot16
    from config import BOT16_BOT_TOKEN
    if not BOT16_BOT_TOKEN:
        logger.info("BOT16_BOT_TOKEN не задан — WHALE alerts отключены")
        return
    try:
        from aiogram import Bot
        from aiogram.client.default import DefaultBotProperties
        from aiogram.enums import ParseMode
        _bot16 = Bot(token=BOT16_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        logger.info("BOT16 (🐋 WHALE) initialized")
    except Exception as e:
        logger.error(f"BOT16 init fail: {e}")


async def _momentum_send_telegram(sig: dict):
    """IMPULSE/FADE alert в BOT16 (общий канал сильных сигналов)."""
    global _bot16
    from config import WHALE_CHAT_ID
    st = sig.get('strategy')
    ind = sig.get('indicators') or {}
    pair = sig.get('pair', '?')
    if st == 'impulse':
        header = "\U0001F680 <b>IMPULSE - LONG momentum</b>\n"
        stats = "backtest 62d: WR 82%, EV +5.9%/trade, median MFE24 +17.7%"
        exit_line = "TP +8% / SL -4% / time-stop 12h"
    else:
        header = "\U0001F3A3 <b>FADE - SHORT rally</b>\n"
        stats = "backtest 62d: WR 54%, EV +1.4%/trade (BTC-filter)"
        exit_line = "TP -6% / SL +4% / horizon 24h"
    btc_line = f" | BTC-RSI4h {ind.get('btc_rsi_4h')}" if st == 'fade' else ''
    txt = (header +
           "----------------------\n" +
           f"<b>{pair}</b> | {sig.get('direction')}\n" +
           f"<b>Entry:</b> {sig.get('entry')}\n" +
           f"<b>Exit:</b> {exit_line}\n" +
           "----------------------\n" +
           f"RSI4h {ind.get('rsi_4h')} | RSI1d {ind.get('rsi_1d')} | " +
           f"ST4h {ind.get('st4')} | ATR {ind.get('atr_pct')}%" + btc_line + "\n" +
           f"{stats}\n" +
           f"<a href='https://www.binance.com/en/futures/{pair.replace('/', '')}'>Chart</a>")
    target_bot = _bot16 or _bot
    if not target_bot:
        return
    try:
        await target_bot.send_message(chat_id=WHALE_CHAT_ID, text=txt,
                                       disable_web_page_preview=True)
    except Exception as e:
        logger.warning(f'[momentum] send fail: {e}')


async def _momentum_scan_loop():
    """IMPULSE/FADE: скан ликвидных пар каждые 20 мин (кулдаун 12ч в
    store_signal). Правила из research 2026-07-02 (см. impulse_detector.py)."""
    import asyncio as _asyncio
    await _asyncio.sleep(240)
    while True:
        try:
            from impulse_detector import scan_universe
            fired = await _asyncio.to_thread(scan_universe, 300)
            if fired:
                logger.info(f"[momentum] fired {len(fired)}: "
                            f"{[(s['strategy'], s['pair']) for s in fired]}")
                for s in fired:
                    try:
                        await _momentum_send_telegram(s)
                        await _asyncio.sleep(1.2)
                    except Exception:
                        pass
        except Exception:
            logger.exception('[momentum] scan error')
        await _asyncio.sleep(1200)


async def _whale_send_telegram(doc: dict):
    """Шлёт WHALE alert в BOT16. Fallback на основной BOT если BOT16 нет.
    Шлёт BOTH STANDARD + PREMIUM (MARGINAL уже отрезан в maybe_fire_whale)."""
    global _bot16
    from config import WHALE_CHAT_ID, WHALE_MIN_TIER
    tier = doc.get('whale_tier', '?')
    # WHALE_MIN_TIER='PREMIUM' блокирует STANDARD (опциональный strict mode).
    # По умолчанию STANDARD — пропускаем всё что прошло maybe_fire (STANDARD+PREMIUM).
    if WHALE_MIN_TIER == 'PREMIUM' and tier != 'PREMIUM':
        logger.debug(f'[whale-tg] skip {tier} (WHALE_MIN_TIER=PREMIUM)')
        return
    tier_emoji = '👑' if tier == 'PREMIUM' else ('🥈' if tier == 'STANDARD' else '?')
    pair = doc.get('pair', '?')
    entry = doc.get('entry', '?')
    score = doc.get('whale_score', 0)
    ind = doc.get('whale_indicators') or {}
    brk = doc.get('whale_breakdown') or {}
    amps = [k for k in brk if not k.startswith('core_') and brk[k] > 0]

    # PREMIUM = ~4 per day, very rare. Distinct visual emphasis в сообщении.
    if tier == 'PREMIUM':
        header = f"👑🐋 <b>WHALE PREMIUM — RARE</b> ⭐\n"
    else:
        header = f"🐋 <b>WHALE {tier_emoji} {tier}</b>\n"
    txt = (header +
           f"━━━━━━━━━━━━━━━━━━\n"
           f"<b>{pair}</b> · LONG\n"
           f"<b>Entry:</b> {entry}\n"
           f"<b>Score:</b> {score}\n"
           f"<b>Amplifiers:</b> {', '.join(amps) if amps else '—'}\n"
           f"━━━━━━━━━━━━━━━━━━\n"
           f"📊 base {ind.get('base_days', 0)}d · vol {ind.get('vol_ratio_max', 0)}× · "
           f"DT {ind.get('prior_downtrend_pct', 0)}%\n"
           f"💡 Range Breakout setup (backtest WR 53.8% / MFE 5.56%)\n"
           f"<a href='https://www.binance.com/en/futures/{pair.replace('/', '')}'>📈 Chart</a>")
    target_bot = _bot16 or _bot
    if not target_bot:
        return
    try:
        await target_bot.send_message(chat_id=WHALE_CHAT_ID, text=txt,
                                       disable_web_page_preview=True)
    except Exception as e:
        logger.warning(f'[whale] send fail: {e}')


async def _shark_send_telegram(doc: dict):
    """Шлёт SHARK alert в BOT16 (общий с WHALE — same token, send-only mode).
    Использует тот же WHALE_CHAT_ID и WHALE_MIN_TIER threshold."""
    global _bot16
    from config import WHALE_CHAT_ID, WHALE_MIN_TIER
    tier = doc.get('shark_tier', '?')
    if WHALE_MIN_TIER == 'PREMIUM' and tier != 'PREMIUM':
        return
    tier_emoji = '👑' if tier == 'PREMIUM' else ('🥈' if tier == 'STANDARD' else '?')
    pair = doc.get('pair', '?')
    entry = doc.get('entry', '?')
    score = doc.get('shark_score', 0)
    ind = doc.get('shark_indicators') or {}
    brk = doc.get('shark_breakdown') or {}
    amps = [k for k in brk if not k.startswith('core_') and brk[k] > 0]

    if tier == 'PREMIUM':
        sh_header = f"👑🦈 <b>SHARK PREMIUM — RARE</b> ⭐\n"
    else:
        sh_header = f"🦈 <b>SHARK {tier_emoji} {tier}</b>\n"
    txt = (sh_header +
           f"━━━━━━━━━━━━━━━━━━\n"
           f"<b>{pair}</b> · SHORT 🔴\n"
           f"<b>Entry:</b> {entry}\n"
           f"<b>Score:</b> {score}\n"
           f"<b>Amplifiers:</b> {', '.join(amps) if amps else '—'}\n"
           f"━━━━━━━━━━━━━━━━━━\n"
           f"📊 distrib {ind.get('distribution_days', 0)}d · "
           f"multi-top×{ind.get('multi_top_count', 0)} · "
           f"vol {ind.get('vol_ratio_max', 0)}× · "
           f"UT {ind.get('prior_uptrend_pct', 0)}%\n"
           f"💡 Distribution Top breakdown (backtest WR 60.4% / MFE 4.96%)\n"
           f"<a href='https://www.binance.com/en/futures/{pair.replace('/', '')}'>📈 Chart</a>")
    target_bot = _bot16 or _bot
    if not target_bot:
        return
    try:
        await target_bot.send_message(chat_id=WHALE_CHAT_ID, text=txt,
                                       disable_web_page_preview=True)
    except Exception as e:
        logger.warning(f'[shark] send fail: {e}')




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
    """Кластеры удалены (2026-07-02) — стаб для alert-билдеров."""
    return ""





async def _cluster_check_on_signal(pair: str, direction: str, at=None):
    """Кластеры удалены (2026-07-02) — стаб (вызывается из alert-потоков)."""
    return None





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
        positions = await asyncio.to_thread(pt.get_open_positions)
        if not positions:
            return
        pairs = list({p.get("pair", p["symbol"].replace("USDT", "/USDT")) for p in positions})
        prices = await get_prices_any(pairs)
        closed = await asyncio.to_thread(pt.check_positions, prices)
    except Exception as e:
        logger.warning(f"[paper-positions] fetch/check fail: {e}", exc_info=True)
        return

    # Обрабатываем каждое закрытие ИЗОЛИРОВАННО
    for c in closed:
        try:
            import paper_trader as pt
            trades, _ = pt._get_collections()
            trade = await asyncio.to_thread(trades.find_one, {"trade_id": c["trade_id"]})
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


# Tracker фоновых live-mirror tasks. БЕЗ него Python GC может убить
# task до выполнения (event loop держит только weak references).
# Это была root cause "AZTEC paper opened но live mirror не создался"
# при одновременном открытии двух сделок (29 апр 2026, AZTEC vs ONDO).
_MIRROR_TASKS: set = set()

# Тот же AZTEC-style fix для verified_entry — раньше create_task без save
# reference, GC ел task при загруженном event loop. 30 апр 2026 verified
# просел до 0 за день когда были tick stalls (cryptovizor + anomaly/conf).
_VERIFIED_TASKS: set = set()


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
    # ВАЖНО: пишем в paper_rejections чтобы видеть сколько сигналов
    # теряется на этом фильтре (бэктест 28.04 показал ~70 ST/день и
    # ~106 CV/день уходят сюда тихо).
    try:
        from exchange_symbols import is_symbol_supported, get_default_exchange
        sym_check = (signal_data.get("symbol") or "").upper()
        if sym_check and not is_symbol_supported(sym_check):
            ex_name = get_default_exchange()
            logger.info(f"[paper-signal] skip {sym_check} — not on {ex_name.upper()} Futures USDT-perp")
            try:
                from paper_trader import _log_rejection_sync
                _log_rejection_sync(signal_data,
                                    f"⛔ F0: пара не на {ex_name.upper()} Futures USDT-perp")
            except Exception:
                pass
            return
    except Exception as _fe:
        # Если symbols registry глючит — НЕ блокируем сигнал, пропускаем дальше
        logger.debug(f"[paper-signal] symbol filter error: {_fe}")

    # ── СИНХРОННЫЙ fill RSI + Trend + Delta IMMEDIATELY на каждый новый сигнал ──
    # Юзер: "рсай сма не подгружается по приходу сигнала сразу".
    # Раньше fire-and-forget create_task() — задача могла быть GC'd под нагрузкой,
    # данные не попадали в кэш к моменту рендера журнала.
    # Сейчас: parallel gather с 5s budget — все 3 fill'a бегут одновременно,
    # ждём max 5s, возвращаемся. _paper_on_signal внутри 20s timeout — запас есть.
    try:
        _pair_fill = signal_data.get("pair") or ""
        if not _pair_fill and signal_data.get("symbol"):
            _s = signal_data["symbol"]
            _pair_fill = _s.replace("USDT", "/USDT") if "USDT" in _s else _s
        if _pair_fill:
            import time as _t_fill
            _at_ms = int(_t_fill.time() * 1000)

            async def _fill_rsi(_p):
                try:
                    from rsi_cache import fill_pair_rsi
                    await asyncio.to_thread(fill_pair_rsi, _p)
                except Exception as _e:
                    logger.debug(f"[rsi-cache] on-signal fill {_p}: {_e}")

            async def _fill_trend(_p):
                try:
                    from trend_cache import fill_pair_trend
                    await asyncio.to_thread(fill_pair_trend, _p)
                except Exception as _e:
                    logger.debug(f"[trend-cache] on-signal fill {_p}: {_e}")

            async def _fill_delta(_p, _ms):
                try:
                    from delta_calculator import get_delta_snapshot_fast as _gdsf
                    await asyncio.to_thread(_gdsf, _p, _ms)
                except Exception as _e:
                    logger.debug(f"[cluster-delta] on-signal fill {_p}: {_e}")

            async def _fill_rsi12h(_p):
                """Fill 12h state cache для UI колонки + paper_trader gate."""
                try:
                    import rsi12h_state as _r12
                    await asyncio.to_thread(_r12.get_state, _p)
                except Exception as _e:
                    logger.debug(f"[rsi12h] on-signal fill {_p}: {_e}")

            async def _fill_rsi4h(_p):
                """Fill 4h state cache для UI колонки."""
                try:
                    import rsi4h_state as _r4
                    await asyncio.to_thread(_r4.get_state, _p)
                except Exception as _e:
                    logger.debug(f"[rsi4h] on-signal fill {_p}: {_e}")

            async def _fill_ema_cross(_p, _ts_s):
                """Fill EMA50/200 cross state cache для UI колонки EMA×.
                _ts_s — момент сигнала в unix sec (определяет какой cross был ДО)."""
                try:
                    import ema_cross_state as _ec
                    await asyncio.to_thread(_ec.get_state, _p, _ts_s)
                except Exception as _e:
                    logger.debug(f"[ema-cross] on-signal fill {_p}: {_e}")

            _at_ts_s = int(_at_ms / 1000)
            # Parallel — max 10s total. fill_pair_rsi/trend делают 4 sequential
            # fapi calls each (~4s total на пару). С asyncio.gather все 6 fill'a
            # бегут параллельно (RSI, Trend, Delta, 12h-state, 4h-state, EMA-cross).
            try:
                await asyncio.wait_for(
                    asyncio.gather(
                        _fill_rsi(_pair_fill),
                        _fill_trend(_pair_fill),
                        _fill_delta(_pair_fill, _at_ms),
                        _fill_rsi12h(_pair_fill),
                        _fill_rsi4h(_pair_fill),
                        _fill_ema_cross(_pair_fill, _at_ts_s),
                        return_exceptions=True,
                    ),
                    timeout=10.0,
                )
            except asyncio.TimeoutError:
                logger.debug(f"[on-signal-fill] timeout 10s {_pair_fill}")
    except Exception as e:
        logger.debug(f"[on-signal-fill] schedule fail: {e}")

    # ── EARLY ENTRY trigger — DISABLED ──
    # Замена: 🧠 COMBO signal (combo_detector.maybe_fire_combo) который
    # использует weighted scoring и более узкие триггеры (st_vip + TC).
    _DISABLED_EARLY_ENTRY = True
    if False and _DISABLED_EARLY_ENTRY:  # blocker — execution to skip block
        _pair_ee = signal_data.get("pair") or ""
        if not _pair_ee and signal_data.get("symbol"):
            _s_ee = signal_data["symbol"]
            _pair_ee = _s_ee.replace("USDT", "/USDT") if "USDT" in _s_ee else _s_ee
        _src_ee = signal_data.get("source", "")
        _dir_ee = (signal_data.get("direction", "") or "").upper()
        # Только для информативных источников (CV/ST/anomaly/confluence + new strategies)
        _ee_eligible = _src_ee in ('supertrend', 'confluence',
                                    'vol_accum', 'triple_confluence', 'volume_surge',
                                    'second_flip', 'volcano', 'cluster')
        if _pair_ee and _ee_eligible:
            async def _check_early_entry():
                try:
                    from database import _get_db, utcnow
                    from datetime import timedelta
                    db_ee = _get_db()
                    # Pair recently в pre_pump_candidates (last 48h)?
                    cutoff_ee = utcnow() - timedelta(hours=48)
                    cand = db_ee.pre_pump_candidates.find_one(
                        {'pair': _pair_ee, 'detected_at': {'$gte': cutoff_ee},
                          'tier': {'$in': ['WATCH', 'STRONG', 'PRIME']}},
                        sort=[('detected_at', -1)]
                    )
                    if not cand:
                        return  # пара не в WATCH — early entry не применим
                    # Rate-limit: не более 1 early_entry per pair per 4h
                    last_ee = db_ee.pre_pump_early_entries.find_one(
                        {'pair': _pair_ee},
                        sort=[('at', -1)]
                    )
                    now_dt = utcnow()
                    if last_ee:
                        age_s = (now_dt - last_ee.get('at', now_dt)).total_seconds()
                        if age_s < 4 * 3600:
                            return  # rate limit
                    # Insert early entry
                    doc_ee = {
                        'pair': _pair_ee,
                        'direction': _dir_ee,
                        'source': _src_ee,
                        'pattern': signal_data.get('pattern', ''),
                        'entry_price': signal_data.get('entry') or signal_data.get('price'),
                        'at': now_dt,
                        'at_ts': int(now_dt.timestamp()),
                        # Snapshot pre_pump state at moment of trigger
                        'prepump_tier': cand.get('tier'),
                        'prepump_score': cand.get('composite_score'),
                        'prepump_components': cand.get('components', {}),
                        'sectors': cand.get('sectors', []),
                        'sector_active': cand.get('sector_active', False),
                    }
                    try:
                        db_ee.pre_pump_early_entries.insert_one(doc_ee)
                        try:
                            db_ee.pre_pump_early_entries.create_index('pair')
                            db_ee.pre_pump_early_entries.create_index([('at', -1)])
                        except Exception: pass
                    except Exception:
                        pass
                    # Send BOT13 alert
                    try:
                        from prepump_bot import send_early_entry_alert
                        await asyncio.to_thread(send_early_entry_alert, doc_ee)
                    except Exception as _bea:
                        logger.debug(f"[early-entry] bot13 send {_pair_ee}: {_bea}")
                    logger.info(f"[early-entry] 🎯 {_pair_ee} {_dir_ee} via {_src_ee} "
                                 f"(prepump {cand.get('tier')} score={cand.get('composite_score')})")
                except Exception as _ee_err:
                    logger.debug(f"[early-entry] {_pair_ee}: {_ee_err}")
            # Fire-and-forget (не блокируем _paper_on_signal)
            asyncio.create_task(_check_early_entry())

    # COMBO удалён (2026-07-02): бэктест EV -0.31%, дублировал ST-семейство

    # ── 🐋 WHALE detector (LONG) ──
    # Range Breakout pattern на основе 20-chart manual analysis.
    # Triggered при ST flip (vip/mtf) LONG. Backtest 30d STANDARD WR 53.8%.
    try:
        from whale_detector import maybe_fire_whale
        async def _whale_post():
            doc = await asyncio.to_thread(maybe_fire_whale, signal_data)
            if doc:
                try:
                    await _whale_send_telegram(doc)
                except Exception as _we:
                    logger.debug(f"[whale] telegram fail: {_we}")
        asyncio.create_task(_whale_post())
    except Exception as _we2:
        logger.debug(f"[whale] schedule fail: {_we2}")

    # ── 🦈 SHARK detector (SHORT) ──
    # Distribution Top Breakdown / Blow-Off Climax / Lower-High Continuation.
    # Mirror WHALE для SHORT direction. Backtest 30d STANDARD WR 60.4% MFE 4.96%.
    # Triggered при ST flip (vip/mtf) SHORT. Telegram alerts через BOT16 (общий с WHALE).
    try:
        from shark_detector import maybe_fire_shark
        async def _shark_post():
            doc = await asyncio.to_thread(maybe_fire_shark, signal_data)
            if doc:
                try:
                    await _shark_send_telegram(doc)
                except Exception as _se:
                    logger.debug(f"[shark] telegram fail: {_se}")
        asyncio.create_task(_shark_post())
    except Exception as _se2:
        logger.debug(f"[shark] schedule fail: {_se2}")

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
        # Save task reference в set чтобы GC не съел task под нагрузкой
        # (30 апр 2026 verified просел до 0 — задачи терялись когда event
        # loop был забит cryptovizor stalls). discard через done_callback.
        _ve_task = asyncio.create_task(
            ve.run_verified_check(signal_data, bot=verified_bot,
                                  chat_id=_admin_chat_id,
                                  topic_id=VERIFIED_TOPIC_ID),
            name=f"verified-{signal_data.get('symbol','?')}",
        )
        _VERIFIED_TASKS.add(_ve_task)
        _ve_task.add_done_callback(_VERIFIED_TASKS.discard)
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
                    aid = acc.get('_id','?')
                    task = asyncio.create_task(
                        lt.mirror_paper_for_account(signal_data, mirror_decision, acc),
                        name=f"live-mirror-{aid}",
                    )
                    # Сохраняем ссылку чтобы GC не убил task до выполнения.
                    # Логируем непойманные exception'ы — иначе тихий crash и
                    # paper-сделка остаётся без live-партнёра.
                    _MIRROR_TASKS.add(task)
                    def _on_done(t, _aid=aid, _sym=signal_data.get('symbol','?')):
                        _MIRROR_TASKS.discard(t)
                        if t.cancelled():
                            logger.error(f"[live-{_aid}] mirror task CANCELLED for {_sym}")
                            return
                        exc = t.exception()
                        if exc is not None:
                            logger.error(f"[live-{_aid}] mirror task EXCEPTION for {_sym}: {exc}",
                                         exc_info=exc)
                    task.add_done_callback(_on_done)
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

            # pending-clusters prewarm удалён (кластеры удалены 2026-07-02)

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

            # tradium-setups prewarm удалён вместе с Tradium ingestion (2026-07-01)

            # Запускаем всё параллельно — не последовательно (быстрее в 5 раз)
            await _asyncio.gather(*tasks, return_exceptions=True)

        except Exception:
            logger.debug("[ui-prewarm] loop error", exc_info=True)
        await _asyncio.sleep(120)  # 2 минуты на Pro (было 5 на Hobby)


async def _rsi12h_warmer_loop():
    """Background warmer для RSI/SMA 4h + 12h state cache.

    Каждые 3 мин:
    1. Берём все пары которые в недавних signals (last 24h)
    2. Для каждой пары прогреваем 4h И 12h state cache (parallel)
    3. Этим обеспечиваем что 4h и 12h колонки в журнале не пустые

    Cache TTL 30min (4h) / 1h (12h) — warmer держит данные fresh.

    Bumped 60→150 pairs cap, 5min→3min interval, 30s startup (was 180s)
    — юзер видел "4/12 сма нет данных" слишком долго после деплоя.
    """
    import asyncio as _asyncio
    await _asyncio.sleep(30)
    while True:
        try:
            from database import _get_db
            from datetime import datetime, timezone, timedelta
            import rsi12h_state as r12
            import rsi4h_state as r4
            db = _get_db()
            since = datetime.now(timezone.utc) - timedelta(hours=24)
            pairs = set()
            for col_name, time_field in [('signals','pattern_triggered_at'),
                                         ('new_strategy_signals','created_at'),
                                         ('supertrend_signals','flip_at')]:
                try:
                    for s in db[col_name].find({time_field:{'$gte':since}}, {'pair':1}).limit(800):
                        if s.get('pair'): pairs.add(s['pair'])
                except Exception:
                    pass
            pair_list = list(pairs)[:150]
            from concurrent.futures import ThreadPoolExecutor as _Exec, as_completed as _done
            if pair_list:
                ex = _Exec(max_workers=15)
                try:
                    # Parallel fill for BOTH 4h and 12h per pair
                    futs = []
                    for p in pair_list:
                        futs.append(ex.submit(r12.get_state, p))
                        futs.append(ex.submit(r4.get_state, p))
                    try:
                        for f in _done(futs, timeout=90.0):
                            try: f.result(timeout=0.2)
                            except Exception: pass
                    except Exception: pass
                finally:
                    ex.shutdown(wait=False, cancel_futures=True)
                logger.info(f"[rsi-state-warm] cached 4h+12h × {len(pair_list)} pairs")
        except Exception:
            logger.debug("[rsi-state-warm] error", exc_info=True)
        await _asyncio.sleep(180)


_PREPUMP_SCANNER_STATE = {
    'last_run_at': 0,
    'last_run_duration_s': 0,
    'next_run_at': 0,
    'cycle_interval_s': 1800,  # 30 min
    'last_cycle_pairs': 0,
    'last_cycle_results': 0,
    'last_cycle_active_sectors': 0,
    'total_cycles': 0,
}


def get_prepump_scanner_state() -> dict:
    """Returns текущее состояние scanner (для UI countdown)."""
    import time as _t
    s = dict(_PREPUMP_SCANNER_STATE)
    now = int(_t.time())
    s['now'] = now
    s['next_run_in_s'] = max(0, s['next_run_at'] - now) if s['next_run_at'] else 0
    s['last_run_ago_s'] = (now - s['last_run_at']) if s['last_run_at'] else None
    return s


async def _pre_pump_predictor_loop():
    """Pre-Pump Predictor scanner — каждые 30 мин.

    1. Получаем list of BingX USDT-perp pairs (filter)
    2. Detect active sectors (через volume scores)
    3. Compute predictive_score для каждой пары parallel
    4. Если PRIME (≥75) — записываем в pre_pump_candidates collection
    5. Если впервые qualified в 4h окне — sendalert через BOT13

    Tiers: PRIME ≥ 75, STRONG 60-74, WATCH 45-59
    Auto-trade DISABLED по умолчанию (variant B).
    """
    import asyncio as _asyncio
    import time as _time
    from concurrent.futures import ThreadPoolExecutor as _Exec, as_completed as _done
    await _asyncio.sleep(120)
    iteration = 0
    while True:
        try:
            from bingx_pairs import get_bingx_usdt_perp_pairs
            from volume_profile import get_volume_metrics
            from pre_pump_predictor import predict_pair
            from sectors_coingecko import detect_sector_rotation, get_sectors
            from database import _get_db, utcnow

            t_start = _time.time()
            pairs = list(get_bingx_usdt_perp_pairs())
            if not pairs:
                logger.debug("[pre-pump] no BingX pairs, skip cycle")
                await _asyncio.sleep(300)
                continue
            # Cap для не утопить ratelimit'ы
            pairs = pairs[:300]  # bump до 300 для большего coverage

            # Phase 1: Volume metrics для всех (parallel) — этого хватает для sector rotation
            vol_scores: dict = {}
            ex = _Exec(max_workers=20)
            try:
                futs = {ex.submit(get_volume_metrics, p): p for p in pairs}
                for f in _done(futs, timeout=90.0):
                    try:
                        p = futs[f]
                        m = f.result(timeout=0.2)
                        vol_scores[p] = m.get('volume_score', 0)
                    except Exception:
                        pass
            finally:
                ex.shutdown(wait=False, cancel_futures=True)

            # Phase 2: Sector classification (cached, fast for repeats)
            # Только для пар с volume_score >= 1.2 (lower threshold для больше coverage)
            interesting = [p for p, s in vol_scores.items() if s >= 1.2]
            pair_sectors: dict = {}
            for p in interesting[:80]:  # bump 50→80
                try:
                    pair_sectors[p] = get_sectors(p)
                except Exception:
                    pair_sectors[p] = []

            # Phase 3: Sector rotation detect
            sector_active = detect_sector_rotation(vol_scores, pair_sectors,
                                                     min_pairs=2, min_vol_score=1.5)
            active_pair_set: set = set()
            for sect, info in sector_active.items():
                for p in info['pairs']:
                    active_pair_set.add(p)

            # Phase 4: Predict per pair (только interesting + active)
            scan_pairs = list(set(interesting) | active_pair_set)
            results: list = []
            ex2 = _Exec(max_workers=10)
            try:
                futs2 = {ex2.submit(predict_pair, p, p in active_pair_set): p for p in scan_pairs}
                for f in _done(futs2, timeout=180.0):
                    try:
                        res = f.result(timeout=0.3)
                        # Save threshold 40 (WATCH tier после lowering) — больше candidates
                        if res and res.get('composite_score', 0) >= 40:
                            results.append(res)
                    except Exception:
                        pass
            finally:
                ex2.shutdown(wait=False, cancel_futures=True)

            # Phase 5: Store в Mongo + alert новых PRIME
            if results:
                db = _get_db()
                col = db.pre_pump_candidates
                try:
                    col.create_index('pair')
                    col.create_index([('detected_at', -1)])
                    col.create_index('tier')
                except Exception:
                    pass

                new_primes = []
                for r in results:
                    p = r['pair']; tier = r['tier']
                    # Проверяем — alert pop'ался ли last 4h
                    cutoff = utcnow().timestamp() - 4 * 3600
                    last = col.find_one(
                        {'pair': p, 'detected_at_ts': {'$gte': cutoff}, 'tier': 'PRIME'},
                        sort=[('detected_at_ts', -1)]
                    )
                    is_new_prime = (tier == 'PRIME' and not last)

                    doc = {
                        'pair': p, 'tier': tier,
                        'composite_score': r['composite_score'],
                        'direction': r['direction'],
                        'components': r['components'],
                        'volume_data': r.get('volume_data', {}),
                        'oi_data': r.get('oi_data', {}),
                        'funding_data': r.get('funding_data', {}),
                        'sector_active': p in active_pair_set,
                        'sectors': pair_sectors.get(p, []),
                        'detected_at': utcnow(),
                        'detected_at_ts': int(_time.time()),
                        'is_new_prime': is_new_prime,
                    }
                    try:
                        col.insert_one(doc)
                    except Exception:
                        pass
                    if is_new_prime:
                        new_primes.append(doc)

                # Alert новых PRIME через BOT13
                if new_primes:
                    try:
                        from prepump_bot import send_prepump_alert
                        for doc in new_primes:
                            try:
                                await _asyncio.to_thread(send_prepump_alert, doc)
                            except Exception as _be:
                                logger.debug(f"[pre-pump] alert {doc['pair']}: {_be}")
                    except ImportError:
                        pass  # prepump_bot not yet implemented

            elapsed = _time.time() - t_start
            iteration += 1
            # Update scanner state для UI countdown
            now_int = int(_time.time())
            _PREPUMP_SCANNER_STATE['last_run_at'] = now_int
            _PREPUMP_SCANNER_STATE['last_run_duration_s'] = round(elapsed, 1)
            _PREPUMP_SCANNER_STATE['next_run_at'] = now_int + 1800
            _PREPUMP_SCANNER_STATE['last_cycle_pairs'] = len(scan_pairs)
            _PREPUMP_SCANNER_STATE['last_cycle_results'] = len(results)
            _PREPUMP_SCANNER_STATE['last_cycle_active_sectors'] = len(sector_active)
            _PREPUMP_SCANNER_STATE['total_cycles'] = iteration
            if iteration <= 3 or iteration % 5 == 0:
                primes = sum(1 for r in results if r['tier'] == 'PRIME')
                strong = sum(1 for r in results if r['tier'] == 'STRONG')
                watch = sum(1 for r in results if r['tier'] == 'WATCH')
                logger.info(f"[pre-pump] iter={iteration} scanned={len(scan_pairs)} "
                             f"PRIME={primes} STRONG={strong} WATCH={watch} "
                             f"sectors={len(sector_active)} took={elapsed:.0f}s")
        except Exception:
            logger.exception("[pre-pump] cycle error")
        await _asyncio.sleep(1800)  # 30 min


async def _journal_cache_warmer_loop():
    """Background warmer для /api/journal — каждые 60с пересчитывает full
    compute и кладёт результат в journal_cache. Endpoint всегда читает из
    готового кэша и отвечает <100ms (вместо 30-60с cold compute).

    Запускается с задержкой 90с после старта (чтобы Mongo + warmer'ы успели
    наполнить state caches, иначе первый прогон даст пустые enrichments).
    """
    import asyncio as _asyncio
    import time as _time
    await _asyncio.sleep(90)
    iteration = 0
    while True:
        try:
            from cache_utils import journal_cache
            from admin import _compute_journal_sync
            t0 = _time.time()
            result = await _asyncio.to_thread(_compute_journal_sync)
            elapsed = _time.time() - t0
            # TTL 180s — больше чем 60с интервал warmer'а, чтобы между циклами
            # кэш не успевал инвалидироваться (особенно если compute >60с)
            journal_cache.set("journal_all", result, ttl=180.0)
            iteration += 1
            n_items = len((result or {}).get("items", []))
            if iteration <= 3 or iteration % 10 == 0:
                logger.info(f"[journal-warm] iter={iteration} items={n_items} compute={elapsed:.1f}s")
        except Exception:
            logger.debug("[journal-warm] error", exc_info=True)
        await _asyncio.sleep(60)


async def _rsi_cross_scanner_loop():
    """RSI/SMA(RSI) 12h crossover Scanner с Volume confirmation.
    Backtest 14d: LONG vol≥2× → WR 62%, MFE +9.83%, Final +2.45%."""
    import asyncio as _asyncio
    await _asyncio.sleep(260)
    while True:
        try:
            import rsi_sma_cross_scanner as rcs
            triggers = await _asyncio.wait_for(
                _asyncio.to_thread(rcs.scan), timeout=120.0,
            )
            for tr in triggers or []:
                emitted = await _asyncio.to_thread(rcs.emit_signal, tr)
                if not emitted:
                    continue
                try:
                    payload = {
                        'symbol': tr['symbol'], 'pair': tr['pair'],
                        'direction': tr['direction'],
                        'entry': tr['entry'],
                        'source': 'rsi_cross_12h',
                        'pattern': (f"RSI/SMA cross {tr['cross_type']} "
                                    f"rsi={tr['rsi']} vol={tr['vol_ratio']}×"),
                        'rsi_at_cross': tr['rsi'],
                        'vol_ratio': tr['vol_ratio'],
                    }
                    await _paper_on_signal(payload)
                except Exception as _pe:
                    logger.debug(f"[rsi-cross] paper route fail: {_pe}")
            if triggers:
                logger.info(f"[rsi-cross] emitted {sum(1 for _ in triggers)} signals")
        except _asyncio.TimeoutError:
            logger.warning("[rsi-cross] scan TIMEOUT 120s")
        except Exception:
            logger.debug("[rsi-cross] error", exc_info=True)
        await _asyncio.sleep(rcs.CONFIG.get('scan_interval_s', 600) if 'rcs' in dir() else 600)


async def _v_bottom_scanner_loop():
    """V-Bottom Scanner — каждые 5 мин ищет V-Bottom/V-Top setups market-wide
    (BingX swap mid-cap pairs, 10M-900M 24h vol). Triggers роутятся через
    _paper_on_signal с source='v_bottom'.

    Backtest 7d: V-Bottom = WR 98%, MFE +4.37% median, +12.24% p75."""
    import asyncio as _asyncio
    await _asyncio.sleep(220)
    while True:
        try:
            import v_bottom_scanner as vbs
            triggers = await _asyncio.wait_for(
                _asyncio.to_thread(vbs.scan), timeout=60.0,
            )
            for tr in triggers or []:
                emitted = await _asyncio.to_thread(vbs.emit_signal, tr)
                if not emitted:
                    continue
                try:
                    payload = {
                        'symbol': tr['symbol'],
                        'pair': tr['pair'],
                        'direction': tr['direction'],
                        'entry': tr['entry'],
                        'source': 'v_bottom',
                        'pattern': (f"{tr['pattern_type']} drop={tr.get('drop_pct',0):.1f}% "
                                    f"rev={tr.get('reversal_pct',0):.1f}%"),
                        'v_pattern': {
                            'type': tr['pattern_type'],
                            'drop_pct': tr.get('drop_pct'),
                            'reversal_pct': tr.get('reversal_pct'),
                        },
                    }
                    await _paper_on_signal(payload)
                except Exception as _pe:
                    logger.debug(f"[v-bottom] paper route fail: {_pe}")
            if triggers:
                logger.info(f"[v-bottom] cycle emitted {sum(1 for _ in triggers)} signals")
        except _asyncio.TimeoutError:
            logger.warning("[v-bottom] scan TIMEOUT 60s")
        except Exception:
            logger.debug("[v-bottom] error", exc_info=True)
        await _asyncio.sleep(300)


async def _new_strategies_updater_loop():
    """Каждые 5 минут проверяет WAITING сигналы из new_strategy_signals
    и закрывает их при достижении TP/SL/TIMEOUT (lookback 24h)."""
    import asyncio as _asyncio
    await _asyncio.sleep(180)  # стартовая задержка после prewarm
    while True:
        try:
            import new_strategies as ns
            res = await _asyncio.wait_for(ns.update_waiting_outcomes(), timeout=60.0)
            if res.get("updated", 0) > 0:
                logger.info(f"[new-strategies] updater synced {res['updated']} outcomes")
        except _asyncio.TimeoutError:
            logger.warning("[new-strategies] updater TIMEOUT 60s")
        except Exception:
            logger.debug("[new-strategies] updater error", exc_info=True)
        await _asyncio.sleep(300)  # каждые 5 минут



async def _levels_refresher_loop():
    """📐 Levels Engine: фоновый пересчёт S/R зон для активных пар.

    Каждые 10 мин: пары со свежими сигналами (24h) из supertrend_signals /
    new_strategy_signals / anomalies / confluence → refresh_pair_levels()
    (4 TF × klines + свинги + кластеризация) → Mongo computed_levels.
    UI (/api/levels) читает из Mongo мгновенно."""
    import asyncio as _asyncio
    await _asyncio.sleep(180)  # не мешаем стартовым прогревам
    while True:
        try:
            from database import _get_db
            from datetime import timedelta as _td, datetime as _dt, timezone as _tz
            from levels_engine import refresh_pair_levels
            db = _get_db()
            since = _dt.now(_tz.utc) - _td(hours=24)
            pairs = set()
            for col_name, ts_field in (('supertrend_signals', 'flip_at'),
                                        ('new_strategy_signals', 'created_at')):
                try:
                    for s in db[col_name].find({ts_field: {'$gte': since}},
                                                {'pair': 1}).limit(300):
                        if s.get('pair'):
                            pairs.add(s['pair'])
                except Exception:
                    pass
            for col_name in ('confluence',):
                try:
                    for s in db[col_name].find({'detected_at': {'$gte': since}},
                                                {'pair': 1}).limit(200):
                        if s.get('pair'):
                            pairs.add(s['pair'])
                except Exception:
                    pass
            pairs = list(pairs)[:400]  # cap
            if not pairs:
                await _asyncio.sleep(600)
                continue
            logger.info(f"[levels] refreshing {len(pairs)} pairs × 4 TF (12 workers)")
            from concurrent.futures import ThreadPoolExecutor, as_completed as _ac
            import time as _t
            _t0 = _t.time()
            ex_pool = ThreadPoolExecutor(max_workers=12)
            try:
                futs = [ex_pool.submit(refresh_pair_levels, p) for p in pairs]
                try:
                    for f in await _asyncio.to_thread(
                        lambda: list(_ac(futs, timeout=240.0))
                    ):
                        try:
                            f.result(timeout=0.05)
                        except Exception:
                            pass
                except Exception:
                    pass
                logger.info(f"[levels] done {len(pairs)} pairs in {_t.time()-_t0:.0f}s")
            finally:
                ex_pool.shutdown(wait=False, cancel_futures=True)
        except Exception:
            logger.exception('[levels] loop error')
        await _asyncio.sleep(600)  # 10 мин


async def _rsi_cache_loop():
    """Background task: периодически наполняет signal_rsi_cache (Mongo)
    для всех 4 TF: 15m, 1h, 4h, 1d. Persistent, cross-worker.
    Интервал 5 мин.
    """
    import asyncio as _asyncio
    await _asyncio.sleep(90)
    while True:
        try:
            from database import _get_db
            from datetime import timedelta as _td, datetime as _dt, timezone as _tz
            from rsi_cache import fill_pair_rsi
            db = _get_db()
            since = _dt.now(_tz.utc) - _td(hours=24)
            pairs = set()
            for col_name in ('supertrend_signals', 'new_strategy_signals'):
                ts_field = ('flip_at' if col_name == 'supertrend_signals'
                            else 'created_at')
                try:
                    for s in db[col_name].find({ts_field: {'$gte': since}},
                                                {'pair': 1}).limit(300):
                        if s.get('pair'):
                            pairs.add(s['pair'])
                except Exception:
                    pass
            # CV signals: фильтруем по pattern_triggered_at (когда паттерн сработал),
            # НЕ по received_at — потому что receive_at = когда CV bot добавил пару
            # (часто недели назад), а pattern fires сегодня. Раньше KAVA/SUN/etc
            # не попадали в pool → 16h stale rsi_cache.
            try:
                for s in db.signals.find({'pattern_triggered': True,
                                          'pattern_triggered_at': {'$gte': since}},
                                          {'pair': 1}).limit(300):
                    if s.get('pair'):
                        pairs.add(s['pair'])
            except Exception:
                pass
            # Legacy fallback: пары добавленные недавно (на всякий случай)
            try:
                for s in db.signals.find({'received_at': {'$gte': since}},
                                          {'pair': 1}).limit(100):
                    if s.get('pair'):
                        pairs.add(s['pair'])
            except Exception:
                pass
            pairs = list(pairs)[:500]  # cap 500 пар на цикл
            if not pairs:
                await _asyncio.sleep(300)
                continue
            logger.info(f"[rsi-cache] filling 4 TF for {len(pairs)} pairs (16 workers, FAPI)")
            from concurrent.futures import ThreadPoolExecutor
            # 16 workers с FAPI fetcher (single-call) — ~30-60s на 500 пар.
            # Раньше было 4 worker × CDN day-by-day = 10+ мин (не успевал).
            ex_pool = ThreadPoolExecutor(max_workers=16)
            try:
                fut_list = [ex_pool.submit(fill_pair_rsi, p) for p in pairs]
                # Timeout 180s — после этого новый цикл начинается, недокачанное доделается потом
                import time as _t_loop
                _t0 = _t_loop.time()
                from concurrent.futures import as_completed as _ac
                try:
                    for f in await _asyncio.to_thread(
                        lambda: list(_ac(fut_list, timeout=180.0))
                    ):
                        try:
                            f.result(timeout=0.05)
                        except Exception:
                            pass
                except Exception:
                    pass
                logger.info(f"[rsi-cache] done {len(pairs)} pairs in {_t_loop.time()-_t0:.0f}s")
            finally:
                ex_pool.shutdown(wait=False, cancel_futures=True)
        except Exception:
            logger.exception('[rsi-cache] loop error')
        await _asyncio.sleep(300)


async def _trend_cache_loop():
    """Background task: периодически наполняет signal_trend_cache (Mongo)
    для всех 4 TF: EMA20 vs EMA50. Cross-worker persistent. Интервал 6 мин."""
    import asyncio as _asyncio
    await _asyncio.sleep(150)
    while True:
        try:
            from database import _get_db
            from datetime import timedelta as _td, datetime as _dt, timezone as _tz
            from trend_cache import fill_pair_trend
            db = _get_db()
            since = _dt.now(_tz.utc) - _td(hours=24)
            pairs = set()
            for col_name in ('supertrend_signals', 'new_strategy_signals'):
                ts_field = ('flip_at' if col_name == 'supertrend_signals'
                            else 'created_at')
                try:
                    for s in db[col_name].find({ts_field: {'$gte': since}},
                                                {'pair': 1}).limit(300):
                        if s.get('pair'):
                            pairs.add(s['pair'])
                except Exception:
                    pass
            # Same fix как в _rsi_cache_loop: CV signals по pattern_triggered_at,
            # а не по received_at (received_at = когда добавили пару, не свежий
            # сигнал).
            try:
                for s in db.signals.find({'pattern_triggered': True,
                                          'pattern_triggered_at': {'$gte': since}},
                                          {'pair': 1}).limit(300):
                    if s.get('pair'):
                        pairs.add(s['pair'])
            except Exception:
                pass
            try:
                for s in db.signals.find({'received_at': {'$gte': since}},
                                          {'pair': 1}).limit(100):
                    if s.get('pair'):
                        pairs.add(s['pair'])
            except Exception:
                pass
            pairs = list(pairs)[:400]
            if not pairs:
                await _asyncio.sleep(360)
                continue
            logger.info(f"[trend-cache] filling 4 TF for {len(pairs)} pairs (16 workers, FAPI)")
            from concurrent.futures import ThreadPoolExecutor, as_completed as _ac_tr
            import time as _t_tr_loop
            ex_pool_tr = ThreadPoolExecutor(max_workers=16)
            _t0_tr = _t_tr_loop.time()
            try:
                fut_list_tr = [ex_pool_tr.submit(fill_pair_trend, p) for p in pairs]
                try:
                    for f in await _asyncio.to_thread(
                        lambda: list(_ac_tr(fut_list_tr, timeout=180.0))
                    ):
                        try:
                            f.result(timeout=0.05)
                        except Exception:
                            pass
                except Exception:
                    pass
                logger.info(f"[trend-cache] done {len(pairs)} pairs in {_t_tr_loop.time()-_t0_tr:.0f}s")
            finally:
                ex_pool_tr.shutdown(wait=False, cancel_futures=True)
            # Invalidate journal cache
            try:
                from cache_utils import journal_cache
                journal_cache.invalidate("journal_all")
            except Exception:
                pass
        except Exception:
            logger.exception('[trend-cache] loop error')
        await _asyncio.sleep(360)


async def _cluster_delta_cache_loop():
    """Background task: периодически наполняет cluster_delta cache для
    свежих сигналов всех источников (supertrend_signals, cv_flip_signals,
    new_strategy_signals, signals, anomalies, confluence_signals, clusters).
    Использует get_delta_snapshot_fast (klines, 1 запрос/TF/пара) — быстрый.
    Интервал 7 мин. Persistent через Mongo, cross-worker.

    Без этого loop'а delta+resonance колонки в журнале остаются пустыми
    для сигналов где не сработал хук on_signal (например, при перезапуске).
    """
    import asyncio as _asyncio
    await _asyncio.sleep(120)
    while True:
        try:
            from database import _get_db
            from datetime import timedelta as _td, datetime as _dt, timezone as _tz
            from delta_calculator import (
                get_delta_snapshot_fast, _candle_open_ms, TF_MINUTES,
            )
            db = _get_db()
            now_dt = _dt.now(_tz.utc)
            since = now_dt - _td(hours=24)
            # Собираем (pair, at_ts_ms) из всех источников
            wanted: set = set()  # set of (pair, at_ts_ms)
            # supertrend_signals
            try:
                for s in db.supertrend_signals.find(
                        {'flip_at': {'$gte': since}},
                        {'pair': 1, 'flip_at': 1}).limit(500):
                    p = s.get('pair')
                    fa = s.get('flip_at')
                    if p and fa:
                        wanted.add((p, int(fa.timestamp() * 1000)))
            except Exception:
                pass
            # cv_flip / new_strategy
            for col_name in ('new_strategy_signals',):
                try:
                    for s in db[col_name].find(
                            {'created_at': {'$gte': since}},
                            {'pair': 1, 'created_at': 1,
                             'st_flip_at': 1}).limit(500):
                        p = s.get('pair')
                        ts = s.get('st_flip_at') or s.get('created_at')
                        if p and ts:
                            wanted.add((p, int(ts.timestamp() * 1000)))
                except Exception:
                    pass
            # signals (tradium / cryptovizor)
            try:
                for s in db.signals.find(
                        {'received_at': {'$gte': since}},
                        {'pair': 1, 'received_at': 1,
                         'pattern_triggered_at': 1}).limit(500):
                    p = s.get('pair')
                    ts = s.get('pattern_triggered_at') or s.get('received_at')
                    if p and ts:
                        wanted.add((p, int(ts.timestamp() * 1000)))
            except Exception:
                pass
            # confluence
            try:
                for c in db.confluence_signals.find(
                        {'detected_at': {'$gte': since}},
                        {'pair': 1, 'detected_at': 1}).limit(500):
                    p = c.get('pair')
                    ts = c.get('detected_at')
                    if p and ts:
                        wanted.add((p, int(ts.timestamp() * 1000)))
            except Exception:
                pass
            if not wanted:
                await _asyncio.sleep(420)
                continue
            # Отфильтровываем те, для которых уже есть cluster_delta cache
            # (по обоим TF — 15m и 1h на signal candle).
            cd_col = db.cluster_delta
            todo: list = []
            wanted_list = list(wanted)[:600]  # cap 600 pairs/ts за один цикл
            # Чанк find для существующих
            existing: set = set()
            CHUNK = 300
            for i in range(0, len(wanted_list), CHUNK):
                conds = []
                for (p, ms) in wanted_list[i:i+CHUNK]:
                    for tf in ('15m', '1h', '4h'):
                        om = _candle_open_ms(ms, tf)
                        conds.append({'pair': p, 'tf': tf, 'open_ms': om})
                if not conds:
                    continue
                try:
                    for doc in cd_col.find({'$or': conds},
                                           {'pair':1,'tf':1,'open_ms':1,'_id':0}):
                        existing.add((doc['pair'], doc['tf'], doc['open_ms']))
                except Exception:
                    pass
            for (p, ms) in wanted_list:
                # Если хотя бы один TF отсутствует — пара в todo
                miss = False
                for tf in ('15m', '1h', '4h'):
                    om = _candle_open_ms(ms, tf)
                    if (p, tf, om) not in existing:
                        miss = True
                        break
                if miss:
                    todo.append((p, ms))
            todo = todo[:200]  # cap fetch за один цикл
            if not todo:
                logger.debug('[cluster-delta] cache complete')
                await _asyncio.sleep(420)
                continue
            logger.info(f"[cluster-delta] filling for {len(todo)} pair+ts pairs")
            from concurrent.futures import ThreadPoolExecutor
            def _one(p, ms):
                try:
                    return get_delta_snapshot_fast(p, ms)
                except Exception:
                    return None
            filled = 0
            with ThreadPoolExecutor(max_workers=8) as ex:
                futs = [ex.submit(_one, p, ms) for (p, ms) in todo]
                results = await _asyncio.to_thread(
                    lambda: [f.result() for f in futs])
                filled = sum(1 for r in results
                             if r and (r.get('15m') or r.get('1h')))
            logger.info(f"[cluster-delta] cache loop done: {filled}/{len(todo)} filled")
            # Invalidate journal cache → следующий рендер увидит новые поля
            try:
                from cache_utils import journal_cache
                journal_cache.invalidate("journal_all")
            except Exception:
                pass
        except Exception:
            logger.exception('[cluster-delta] cache loop error')
        await _asyncio.sleep(420)  # 7 min


async def _regime_warm_on_startup():
    """Warm BTC EMA cache в первую же секунду — чтобы первый signal не ждал."""
    import asyncio as _asyncio
    await _asyncio.sleep(15)  # дать watcher подняться
    try:
        from auto_strategy import detect_regime
        regime = await _asyncio.to_thread(detect_regime)
        logger.info(f"[regime-warm] startup regime cache: {regime}")
    except Exception as e:
        logger.debug(f"[regime-warm] fail: {e}")


async def _regime_monitor_loop():
    """Каждые 5 мин сканирует market regime (BTC 4h+1d EMAs).
    При смене регима:
      1. Логирует событие в auto_strategy_regime_log (Mongo)
      2. Сохраняет в system.auto_strategy_current_regime
      3. Закрывает open positions с direction ПРОТИВ нового regime:
         - BEAR detected → закрыть все LONG ALPHA-CV positions
         - BULL detected → закрыть все SHORT ALPHA-CV positions
         - CHOP detected → не трогаем (оба направления OK)

    Это защищает капитал от резкой смены тренда (например, BTC обвал → закрываем
    все LONGs не дожидаясь SL по каждому).
    """
    import asyncio as _asyncio
    await _asyncio.sleep(120)  # startup delay (после auto_strategy import)
    last_regime = None
    while True:
        try:
            from auto_strategy import detect_regime
            from database import _get_db
            from datetime import datetime as _dt, timezone as _tz
            db = _get_db()
            new_regime = await _asyncio.to_thread(detect_regime)
            now = _dt.now(_tz.utc)
            if last_regime is None:
                # First scan — load last known from Mongo
                cached = db.system.find_one({'_id': 'auto_strategy_current_regime'})
                last_regime = cached.get('regime') if cached else None
            if new_regime != last_regime:
                logger.warning(
                    f"[regime-monitor] REGIME CHANGED: {last_regime} → {new_regime}"
                )
                # Log event
                try:
                    db.auto_strategy_regime_log.insert_one({
                        'at': now,
                        'from_regime': last_regime,
                        'to_regime': new_regime,
                        'detected_by': 'auto',
                    })
                except Exception as e:
                    logger.debug(f'[regime-monitor] log fail: {e}')
                # Update current regime
                try:
                    db.system.update_one(
                        {'_id': 'auto_strategy_current_regime'},
                        {'$set': {
                            'regime': new_regime,
                            'changed_at': now,
                            'previous': last_regime,
                        }},
                        upsert=True,
                    )
                except Exception:
                    pass
                # ── PROTECT CAPITAL: close positions against new regime ──
                if new_regime in ('BULL', 'BEAR'):
                    bad_direction = 'SHORT' if new_regime == 'BULL' else 'LONG'
                    open_against = list(db.paper_trades.find({
                        'status': 'OPEN',
                        'auto_strategy_label': {'$exists': True},
                        'direction': bad_direction,
                    }, {'trade_id': 1, 'pair': 1, 'symbol': 1,
                        'direction': 1, 'entry': 1}))
                    if open_against:
                        logger.warning(
                            f"[regime-monitor] {new_regime}: закрываю "
                            f"{len(open_against)} {bad_direction} positions"
                        )
                        from exchange import get_prices_any
                        import paper_trader as pt
                        for pos in open_against:
                            try:
                                pair = pos.get('pair') or pos['symbol'].replace('USDT','/USDT')
                                prices = await _asyncio.to_thread(get_prices_any, [pair])
                                sym = pos['symbol']
                                if not sym.endswith('USDT'): sym += 'USDT'
                                cur = prices.get(sym)
                                if cur:
                                    await _asyncio.to_thread(
                                        pt.close_position,
                                        int(pos['trade_id']),
                                        float(cur),
                                        f'REGIME_CHANGE_{new_regime}',
                                    )
                            except Exception as e:
                                logger.debug(f'[regime-close] {pos.get("trade_id")}: {e}')
                last_regime = new_regime
        except Exception:
            logger.exception('[regime-monitor] cycle error')
        await _asyncio.sleep(300)  # 5 мин


async def _alpha_cv_exit_monitor():
    """Каждые 3 мин проверяет ALPHA-CV открытые позиции по trail_1R_0.5R logic.

    NEW v2.0 (победитель exhaustive backtest 32 exits × 10 entry filters):
      1. Trail SL активируется когда max_favorable - entry >= 1R (sl_dist)
      2. Trail SL = max_favorable - 0.5R (отступ от пика)
      3. SL signal — backup, не двигаем UP
      4. На каждом cycle (3 min): пересчитать max_favorable из 1h klines,
         обновить trail_sl, проверить current_price против trail_sl

    Backtest avg uplift vs предыдущий 1h SMA crossover exit:
      baseline: +0.18 → +0.29R (60% improvement)
      на filtered signals: +1.55 to +2.18R (3-9× vs current)
    """
    import asyncio as _asyncio
    await _asyncio.sleep(180)  # стартовая задержка
    while True:
        try:
            from database import _get_db
            from auto_strategy import compute_exit_state_v3
            import paper_trader as pt
            db = _get_db()
            open_positions = list(db.paper_trades.find({
                'status': 'OPEN',
                'auto_strategy_label': {'$exists': True}
            }, {'trade_id': 1, 'pair': 1, 'symbol': 1,
                'direction': 1, 'entry': 1, 'sl': 1, 'original_sl': 1,
                'opened_at': 1, 'auto_strategy_regime': 1}))
            if not open_positions:
                logger.debug("[alpha-cv-exit] no open positions")
                await _asyncio.sleep(180)
                continue
            logger.info(f"[alpha-cv-exit] checking {len(open_positions)} positions (trail_1R_0.5R)")
            for pos in open_positions:
                pair = pos.get('pair') or pos.get('symbol', '').replace('USDT', '/USDT')
                direction = pos.get('direction')
                trade_id = pos.get('trade_id')
                entry = pos.get('entry')
                # original_sl приоритетнее sl (sl мог быть подвинут BE/trailing
                # в paper_trader). Нужен ИСХОДНЫЙ для корректного 1R расчёта.
                sl = pos.get('original_sl') or pos.get('sl')
                opened_at = pos.get('opened_at')
                if not (pair and direction and trade_id and entry and sl and opened_at):
                    continue
                try:
                    opened_at_ms = int(opened_at.timestamp() * 1000) if hasattr(opened_at, 'timestamp') else int(opened_at)
                    regime = pos.get('auto_strategy_regime') or 'CHOP'
                    sig = await _asyncio.to_thread(
                        compute_exit_state_v3, pair, direction,
                        float(entry), float(sl), opened_at_ms, regime,
                    )
                    # Save state to trade doc для UI/monitoring
                    if sig.get('max_fav_r') is not None:
                        try:
                            await _asyncio.to_thread(
                                lambda: db.paper_trades.update_one(
                                    {'trade_id': trade_id},
                                    {'$set': {
                                        'alpha_cv_max_fav_r': sig.get('max_fav_r'),
                                        'alpha_cv_trail_sl': sig.get('trail_sl'),
                                        'alpha_cv_trail_active': bool(sig.get('trail_active')),
                                    }}
                                )
                            )
                        except Exception:
                            pass
                    if sig.get('should_exit'):
                        exit_price = sig.get('exit_price') or sig.get('current_price')
                        reason = sig.get('reason', 'trail')
                        if exit_price:
                            logger.info(
                                f"[alpha-cv-exit] CLOSE #{trade_id} {pair} {direction} "
                                f"@ {exit_price} | reason={reason} "
                                f"max_fav_r={sig.get('max_fav_r')} "
                                f"trail_sl={sig.get('trail_sl')}"
                            )
                            await _asyncio.to_thread(
                                pt.close_position, int(trade_id),
                                float(exit_price),
                                f"ALPHA_CV_TRAIL_{reason.upper()}"
                            )
                except Exception as e:
                    logger.debug(f"[alpha-cv-exit] pos #{trade_id}: {e}")
        except Exception:
            logger.exception("[alpha-cv-exit] cycle error")
        await _asyncio.sleep(180)  # каждые 3 мин


async def _cluster_delta_backfill_once():
    """Однократный backfill cluster_delta для сигналов последних 24 часов.
    Запускается один раз при старте watcher с задержкой 90с (даём время
    Mongo + HTTP клиентам прогреться).

    aggTrades живут только ~24h на Binance, поэтому backfill > 24h не нужен.
    Информативно — записывается в new_strategy_signals.delta_15m / delta_1h /
    resonance_15m / resonance_1h. На генерацию сигналов не влияет.
    """
    import asyncio as _asyncio
    await _asyncio.sleep(90)
    try:
        from database import _get_db, utcnow
        from datetime import timedelta as _td
        from delta_calculator import get_delta_snapshot_async
        col = _get_db().new_strategy_signals
        since = utcnow() - _td(hours=24)
        cursor = col.find({
            'created_at': {'$gte': since},
            'delta_15m': {'$exists': False},
        }).limit(200)
        docs = list(cursor)
        if not docs:
            logger.info("[cluster-delta] backfill: нечего дозаливать")
            return
        # Группируем по (pair, st_flip_at) — 1 fetch на пару+момент
        by_key: dict = {}
        for d in docs:
            flip_at = d.get('st_flip_at') or d.get('created_at')
            if not flip_at:
                continue
            ts_s = int(flip_at.timestamp()) if hasattr(flip_at, 'timestamp') else 0
            key = (d.get('pair', ''), ts_s)
            by_key.setdefault(key, []).append(d)
        filled = 0
        for (pair, ts_s), grp in by_key.items():
            if not pair or not ts_s:
                continue
            try:
                snap = await get_delta_snapshot_async(pair, ts_s * 1000)
                if not snap:
                    continue
                d15 = (snap.get('15m') or {}).get('delta_pct')
                d1h = (snap.get('1h')  or {}).get('delta_pct')
                r15 = (snap.get('15m') or {}).get('resonance')
                r1h = (snap.get('1h')  or {}).get('resonance')
                ids = [doc['_id'] for doc in grp]
                def _save():
                    col.update_many(
                        {'_id': {'$in': ids}},
                        {'$set': {
                            'cluster_delta': snap,
                            'delta_15m': d15, 'delta_1h': d1h,
                            'resonance_15m': r15, 'resonance_1h': r1h,
                        }},
                    )
                await _asyncio.to_thread(_save)
                filled += len(grp)
            except Exception as e:
                logger.debug(f'[cluster-delta] backfill {pair}: {e}')
                continue
            # Throttle между парами чтобы не задавить Binance rate-limit
            await _asyncio.sleep(0.3)
        logger.info(
            f"[cluster-delta] backfill done: {filled}/{len(docs)} filled "
            f"({len(by_key)} unique pair+ts)"
        )
        # Инвалидируем journal cache чтобы UI сразу показал новые поля
        try:
            from cache_utils import journal_cache
            journal_cache.invalidate("journal_all")
        except Exception:
            pass
    except Exception:
        logger.exception("[cluster-delta] backfill fail")


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


async def _cache_cleanup_loop():
    """Превентивная чистка кэшей раз в 30 мин — защита от memory leak.
    Прод 28.04 залип через 1.5ч стабильной работы → подозрение на рост
    cache dicts без TTL invalidation. Этот loop ограничивает размер
    долгоживущих кэшей.
    """
    import asyncio as _asyncio
    import time as _time
    await _asyncio.sleep(60)  # стартовая задержка
    while True:
        try:
            now = _time.time()
            # exchange._volume_cache: записи с TTL 1ч, удаляем старые + cap 200
            try:
                from exchange import _volume_cache, _VOLUME_TTL
                stale = [k for k, (_, ts) in _volume_cache.items() if now - ts > _VOLUME_TTL]
                for k in stale:
                    _volume_cache.pop(k, None)
                # Если всё ещё >200 — удаляем самые старые
                if len(_volume_cache) > 200:
                    items = sorted(_volume_cache.items(), key=lambda kv: kv[1][1])
                    for k, _ in items[:len(_volume_cache) - 200]:
                        _volume_cache.pop(k, None)
                logger.info(f"[cache-cleanup] volume_cache: removed {len(stale)} stale, "
                            f"total now {len(_volume_cache)}")
            except Exception as e:
                logger.debug(f"[cache-cleanup] volume err: {e}")

            # exchange._price_cache: TTL 5с, всё что старше — удаляем
            try:
                from exchange import _price_cache, _PRICE_TTL
                stale_p = [k for k, (_, ts) in _price_cache.items() if now - ts > _PRICE_TTL * 2]
                for k in stale_p:
                    _price_cache.pop(k, None)
                if len(_price_cache) > 500:
                    items = sorted(_price_cache.items(), key=lambda kv: kv[1][1])
                    for k, _ in items[:len(_price_cache) - 500]:
                        _price_cache.pop(k, None)
            except Exception:
                pass

            # exchange._futures_cache, _pump_cache, _kc_cache, _eth_ctx_cache —
            # они с TTL короткие но без cleanup. Cap 300.
            try:
                from exchange import _futures_cache, _pump_cache
                for cache in (_futures_cache, _pump_cache):
                    if len(cache) > 300:
                        keys = list(cache.keys())
                        for k in keys[:len(cache) - 300]:
                            cache.pop(k, None)
            except Exception:
                pass

            # admin caches cap'аются inline после write через _cap_admin_cache
            # (см. admin.py). Cleanup loop в watcher НЕ импортирует admin
            # чтобы избежать circular import при cold start.
        except Exception as e:
            logger.warning(f"[cache-cleanup] loop error: {e}")
        await _asyncio.sleep(300)  # 5 минут (было 30 — слишком редко)


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


async def _write_heartbeat(stage: str, data: dict | None = None):
    """Heartbeat в Mongo через to_thread с timeout 3с.
    Если Atlas тимаутит — heartbeat пропускается, НЕ блокирует watcher.
    Раньше sync write висел на 30с при медленном Atlas → весь tick застывал."""
    def _sync_write():
        try:
            from database import _get_db
            from datetime import datetime, timezone
            _get_db().system.update_one(
                {"_id": "watcher_heartbeat"},
                {"$set": {
                    "stage": stage,
                    "at": datetime.now(timezone.utc),
                    "data": data or {},
                }},
                upsert=True,
            )
        except Exception:
            pass
    try:
        await asyncio.wait_for(asyncio.to_thread(_sync_write), timeout=3.0)
    except (asyncio.TimeoutError, Exception):
        pass


async def start_watcher():
    global _watcher_running
    _watcher_running = True
    await _write_heartbeat("start_watcher_called")
    print(f"[WATCHER] Started (interval={POLL_INTERVAL}s)", flush=True)
    logger.info(f"Price watcher запущен (интервал {POLL_INTERVAL}s)")
    # Прогрев ВСЕХ ботов сразу на старте (раньше была ленивая init только при первом алерте —
    # если init падал молча, алерты терялись тихо до рестарта)
    for name, fn in [("bot5", _setup_bot5),
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
    # 🐋 WHALE safety-net scanner — каждые 30 мин на пропущенные flips
    try:
        asyncio.create_task(_whale_scanner_loop())
        logger.info("[whale-scanner] background loop started")
    except Exception:
        logger.exception("[whale-scanner] failed to start loop")
    # 🦈 SHARK safety-net scanner — каждые 30 мин на пропущенные SHORT flips
    try:
        asyncio.create_task(_shark_scanner_loop())
        logger.info("[shark-scanner] background loop started")
    except Exception:
        logger.exception("[shark-scanner] failed to start loop")
    # 🎰 Setup Checker verdict loop — каждые 60s заполняет setup_verdict
    # на новых signals
    try:
        asyncio.create_task(_setup_verdict_loop())
        logger.info("[verdict-loop] background loop started")
    except Exception:
        logger.exception("[verdict-loop] failed to start loop")
    # [DISABLED — Atlas Free M0 throttle protection]
    # Background prewarm loops отключены чтобы НЕ дополнительно нагружать
    # перегруженный Atlas Free Tier. UI справится через user-driven cache —
    # первый запрос холодный (~1с), следующие из cache (8-60с TTL).
    # При upgrade Atlas до M2+ можно включить обратно.
    if os.getenv("ENABLE_PREWARM", "0") == "1":
        try:
            asyncio.create_task(_candles_prewarm_loop())
            logger.info("[prewarm] candles loop started")
        except Exception:
            logger.exception("[prewarm] failed to start loop")
        try:
            asyncio.create_task(_eth_kc_prewarm_loop())
            logger.info("[prewarm] ETH/KC loop started")
        except Exception:
            logger.exception("[prewarm] ETH/KC loop failed to start")
    else:
        logger.warning("[prewarm] DISABLED (set ENABLE_PREWARM=1 to enable)")
    # Market phase loop — каждые 3 мин определяем фазу + alert при смене
    try:
        asyncio.create_task(_market_phase_loop())
        logger.info("[market-phase] loop started")
    except Exception:
        logger.exception("[market-phase] loop failed to start")
    # Cache cleanup loop — каждые 30 мин чистит долгоживущие dict-кэши
    # (защита от memory leak — прод залипал через 1.5ч стабильной работы)
    try:
        asyncio.create_task(_cache_cleanup_loop())
        logger.info("[cache-cleanup] background loop started")
    except Exception:
        logger.exception("[cache-cleanup] loop failed to start")
    # Live sync — каждые 30с синхронизируем позиции live-аккаунтов с биржей
    try:
        asyncio.create_task(_live_sync_loop())
        logger.info("[live-sync] background loop started")
    except Exception:
        logger.exception("[live-sync] failed to start loop")
    # New Strategies status updater — каждые 5 мин закрывает WAITING → TP/SL/TIMEOUT
    try:
        asyncio.create_task(_new_strategies_updater_loop())
        logger.info("[new-strategies] updater loop started")
    except Exception:
        logger.exception("[new-strategies] updater loop failed")
    # V-Bottom Scanner — DISABLED по запросу юзера (15.05.26).
    # Loop существует но не стартует. Module + collection сохранены на случай
    # future revival. Чтобы вернуть — раскомментировать ниже.
    # try:
    #     asyncio.create_task(_v_bottom_scanner_loop())
    #     logger.info("[v-bottom] scanner loop started")
    # except Exception:
    #     logger.exception("[v-bottom] scanner loop failed")
    # RSI/SMA(RSI) 12h crossover Scanner с Volume confirmation
    try:
        asyncio.create_task(_rsi_cross_scanner_loop())
        logger.info("[rsi-cross] 12h scanner loop started")
    except Exception:
        logger.exception("[rsi-cross] scanner loop failed")
    # RSI/SMA 12h state cache warmer — заполняет cache для активных pairs
    try:
        asyncio.create_task(_rsi12h_warmer_loop())
        logger.info("[rsi12h-warm] cache warmer loop started")
    except Exception:
        logger.exception("[rsi12h-warm] warmer loop failed")
    # Journal full compute warmer — каждые 60с фоном пересчитывает /api/journal
    # cache. Endpoint читает из готового кэша → отвечает <100ms.
    try:
        asyncio.create_task(_journal_cache_warmer_loop())
        logger.info("[journal-warm] background loop started")
    except Exception:
        logger.exception("[journal-warm] warmer loop failed")
    # Pre-Pump Predictor scanner — DISABLED (backtest показал что edge
    # только в triple_confluence/st_vip которые уже есть. Заменён на 🧠 COMBO.)
    # try:
    #     asyncio.create_task(_pre_pump_predictor_loop())
    #     logger.info("[pre-pump] predictor loop started")
    # except Exception:
    #     logger.exception("[pre-pump] predictor loop failed")
    # [DISABLED] Cluster Delta auto-backfill — был источник 418 banов от
    # Binance fapi/klines. Теперь backfill только вручную через
    # POST /api/cluster-delta/backfill-cdn (статический CDN, без rate limit).

    # Cluster Delta WebSocket — realtime klines stream от Binance Futures.
    # WS подключение использует ОТДЕЛЬНЫЙ rate-bucket от REST → не банится.
    # Покрывает today's signals что недоступны на CDN.
    try:
        import delta_websocket as dws
        asyncio.create_task(dws.run_kline_stream())
        logger.info("[delta-ws] kline stream task scheduled")
    except Exception:
        logger.exception("[delta-ws] failed to schedule")

    # ALPHA-CV Exit Monitor — каждые 3 мин проверяет открытые ALPHA-CV
    # позиции на 1h RSI < SMA(RSI) crossover. Закрывает momentum-loss signals.
    try:
        asyncio.create_task(_regime_warm_on_startup())  # warm BTC EMA cache <30с после старта
        asyncio.create_task(_regime_monitor_loop())
        asyncio.create_task(_alpha_cv_exit_monitor())
        logger.info("[alpha-cv-exit] monitor scheduled")
    except Exception:
        logger.exception("[alpha-cv-exit] failed to schedule")

    # RSI cache loop — каждые 5 мин наполняет admin._RSI_CACHE для журнала.
    # Без него journal API НЕ блокируется на klines fetch.
    try:
        asyncio.create_task(_rsi_cache_loop())
        logger.info("[rsi-cache] background loop scheduled")
    except Exception:
        logger.exception("[rsi-cache] failed to schedule")
    # Cluster Delta cache loop — каждые 7 мин наполняет cluster_delta для
    # всех источников сигналов (CV/ST/Anomaly/Confluence/NewStrategy).
    # Гарантирует что Δ% + Резонанс колонки в журнале не пустые.
    try:
        asyncio.create_task(_cluster_delta_cache_loop())
        logger.info("[cluster-delta] cache loop scheduled")
    except Exception:
        logger.exception("[cluster-delta] failed to schedule cache loop")
    # Trend cache loop — каждые 6 мин наполняет signal_trend_cache (EMA20 vs EMA50)
    try:
        asyncio.create_task(_trend_cache_loop())
        logger.info("[trend-cache] background loop scheduled")
    except Exception:
        logger.exception("[trend-cache] failed to schedule")
    # 📐 Levels Engine — собственные S/R зоны per-TF (замена Telegram KL)
    try:
        asyncio.create_task(_levels_refresher_loop())
        logger.info("[levels] background refresher scheduled")
    except Exception:
        logger.exception("[levels] failed to schedule")
    # 🚀🎣 IMPULSE/FADE momentum detector (research 2026-07-02)
    try:
        asyncio.create_task(_momentum_scan_loop())
        logger.info("[momentum] scan loop scheduled")
    except Exception:
        logger.exception("[momentum] failed to schedule")
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
    # UI prewarm — отключён по умолчанию (Atlas throttle)
    if os.getenv("ENABLE_PREWARM", "0") == "1":
        try:
            asyncio.create_task(_ui_prewarm_loop())
            logger.info("[ui-prewarm] background loop started")
        except Exception:
            logger.exception("[ui-prewarm] failed to start loop")
    # [ОТКЛЮЧЕНО] AI memory refresh + AI review open positions —
    # система переведена на rule-based (Entry Checker 8 проверок + TP ladder).
    # AI больше не используется для торговых решений.
    tick = 0
    await _write_heartbeat("entering_main_loop", {"tick": 0})
    # Hard limit на длину тика — если sub-task залип (Atlas slow / network),
    # сбросить через 3 мин и перейти к следующему тику. Раньше первый тик
    # на cold start мог зависнуть навсегда (heartbeat tick=1 stage=tick_start
    # 9+ минут), watcher не двигался вперёд.
    TICK_TIMEOUT_SEC = 180
    while True:
        tick += 1
        await _write_heartbeat("tick_start", {"tick": tick})
        try:
            print(f"[WATCHER] tick {tick}", flush=True)
            await asyncio.wait_for(_check_once(), timeout=TICK_TIMEOUT_SEC)
            await _write_heartbeat("tick_done", {"tick": tick})
            print(f"[WATCHER] tick {tick} done", flush=True)
        except asyncio.TimeoutError:
            logger.warning(f"[watcher] tick {tick} timed out after {TICK_TIMEOUT_SEC}s — skipping")
            await _write_heartbeat("tick_timeout", {"tick": tick,
                                                     "timeout_s": TICK_TIMEOUT_SEC})
        except Exception as e:
            import traceback as _tb
            err_str = f"{type(e).__name__}: {str(e)[:200]}"
            logger.error(f"Watcher ошибка: {e}\n{_tb.format_exc()[-1500:]}")
            await _write_heartbeat("tick_error", {"tick": tick, "error": err_str,
                                                   "traceback": _tb.format_exc()[-500:]})
        await asyncio.sleep(POLL_INTERVAL)
