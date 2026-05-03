"""SuperTrend tracker — генератор ST-сигналов 3 видов (VIP/MTF/Daily).

Работает в фоне через watcher loop каждые 5 минут:
  1. get_tracked_pairs() — берём все пары из сигналов ботов за 14д (~486)
  2. Для каждой пары fetch свечей (1h/4h/1d), compute ST, detect flips
  3. Для каждого нового флипа classify_tier():
       VIP   (T2C_SIGCONF) — флип на 1h + сигнал другого бота ±2ч same dir
       MTF   (T2D_1H_4H_D) — флип на 1h + 4h aligned + 1d aligned
       Daily (T2D_1H_D)    — флип на 1h + 1d aligned
     Приоритет VIP > MTF > Daily. Один флип = один сигнал.
  4. Save в MongoDB + alert через BOT10 (если alert_enabled)

SL = min(ST_value, 1.5 × ATR)
Статусы сделок НЕ трекаются (бектест отдельно по времени).
"""
from __future__ import annotations
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# TTL окно «недавнего флипа» — если флип произошёл на баре ≤N минут назад
# относительно текущего момента, считаем его «новым» для этой проверки.
# 70 мин даёт запас на 5-мин цикл + задержку Binance API (бар 1h закрывается
# на xx:00, следующая проверка может быть в xx:05-xx:10).
FRESH_FLIP_WINDOW_MIN = 70

# Окно для VIP — сигнал другого бота ±2 часа от флипа
VIP_SIG_WINDOW_MS = 2 * 3600 * 1000


def get_tracked_pairs() -> list[str]:
    """Возвращает нормализованные пары (BTCUSDT) из всех сигнальных коллекций
    за последние 14 дней + из supertrend_signals (для consistency)."""
    from database import _signals, _anomalies, _confluence, _clusters, utcnow
    since = utcnow() - timedelta(days=14)
    pairs: set[str] = set()
    try:
        for s in _signals().find(
            {"received_at": {"$gte": since}, "direction": {"$in": ["LONG", "SHORT"]}},
            {"pair": 1},
        ):
            p = (s.get("pair") or "").upper().replace("/", "")
            if p:
                if not p.endswith("USDT"): p = p + "USDT"
                pairs.add(p)
    except Exception as e:
        logger.warning(f"[st-tracker] signals query fail: {e}")
    try:
        for a in _anomalies().find({"detected_at": {"$gte": since}}, {"symbol": 1, "pair": 1}):
            p = (a.get("symbol") or a.get("pair") or "").upper().replace("/", "")
            if p:
                if not p.endswith("USDT"): p = p + "USDT"
                pairs.add(p)
    except Exception as e:
        logger.warning(f"[st-tracker] anomalies fail: {e}")
    try:
        for c in _confluence().find({"detected_at": {"$gte": since}}, {"symbol": 1, "pair": 1}):
            p = (c.get("symbol") or c.get("pair") or "").upper().replace("/", "")
            if p:
                if not p.endswith("USDT"): p = p + "USDT"
                pairs.add(p)
    except Exception as e:
        logger.warning(f"[st-tracker] confluence fail: {e}")
    try:
        for cl in _clusters().find({"trigger_at": {"$gte": since}}, {"pair": 1, "symbol": 1}):
            p = (cl.get("pair") or cl.get("symbol") or "").upper().replace("/", "")
            if p:
                if not p.endswith("USDT"): p = p + "USDT"
                pairs.add(p)
    except Exception:
        pass
    return sorted(pairs)


def _load_bot_signals_48h(pair_norm: str, flip_ts_ms: int) -> list[dict]:
    """Сигналы всех ботов по паре в окне ±2ч от flip_ts."""
    from database import _signals, _anomalies, _confluence
    lo_dt = datetime.utcfromtimestamp((flip_ts_ms - VIP_SIG_WINDOW_MS) / 1000)
    hi_dt = datetime.utcfromtimestamp((flip_ts_ms + VIP_SIG_WINDOW_MS) / 1000)
    pair_slash = pair_norm[:-4] + "/USDT"
    pair_or = {"$or": [{"pair": pair_slash}, {"symbol": pair_norm}]}
    out = []
    try:
        for s in _signals().find({
            "received_at": {"$gte": lo_dt, "$lte": hi_dt},
            "direction": {"$in": ["LONG", "SHORT"]},
            **pair_or,
        }, {"id": 1, "source": 1, "direction": 1, "received_at": 1, "pattern_triggered_at": 1}):
            at = s.get("pattern_triggered_at") or s.get("received_at")
            if at:
                out.append({
                    "source": s.get("source", "unknown"),
                    "direction": s["direction"],
                    "at": at,
                    "signal_id": s.get("id"),
                })
    except Exception as e:
        logger.warning(f"[st-tracker] _signals fetch fail {pair_norm}: {e}")
    try:
        for a in _anomalies().find({
            "detected_at": {"$gte": lo_dt, "$lte": hi_dt},
            "direction": {"$in": ["LONG", "SHORT"]},
            **pair_or,
        }, {"id": 1, "direction": 1, "detected_at": 1}):
            out.append({
                "source": "anomaly",
                "direction": a["direction"],
                "at": a["detected_at"],
                "signal_id": a.get("id"),
            })
    except Exception:
        pass
    try:
        for c in _confluence().find({
            "detected_at": {"$gte": lo_dt, "$lte": hi_dt},
            "direction": {"$in": ["LONG", "SHORT"]},
            **pair_or,
        }, {"id": 1, "direction": 1, "detected_at": 1}):
            out.append({
                "source": "confluence",
                "direction": c["direction"],
                "at": c["detected_at"],
                "signal_id": c.get("id"),
            })
    except Exception:
        pass
    return out


def _st_state_at_ts(st_series: list[dict], ts_ms: int) -> Optional[dict]:
    """Ближайший бар ST ≤ ts_ms."""
    if not st_series:
        return None
    lo, hi = 0, len(st_series) - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if st_series[mid]["t"] <= ts_ms:
            lo = mid
        else:
            hi = mid - 1
    return st_series[lo] if st_series[lo]["t"] <= ts_ms else None


def _classify_flip(
    pair_norm: str,
    flip_bar: dict,            # ST bar at flip
    st_4h: list[dict],
    st_1d: list[dict],
) -> tuple[Optional[str], dict]:
    """Возвращает (tier, extras). tier ∈ {vip, mtf, daily, None}.
    extras содержит aligned_tfs и aligned_bots."""
    flip_ts = flip_bar["t"]
    direction = "LONG" if flip_bar["trend"] == 1 else "SHORT"

    # 1. VIP — сигнал другого бота ±2ч в ту же сторону
    bot_sigs = _load_bot_signals_48h(pair_norm, flip_ts)
    aligned_bots = [
        {
            "source": s["source"],
            "direction": s["direction"],
            "at": s["at"].isoformat() if hasattr(s["at"], "isoformat") else str(s["at"]),
            "signal_id": s.get("signal_id"),
        }
        for s in bot_sigs
        if s["direction"] == direction
    ]
    if aligned_bots:
        return "vip", {"aligned_bots": aligned_bots, "aligned_tfs": ["1h"]}

    # 2. MTF (1h + 4h + 1d)
    st4 = _st_state_at_ts(st_4h, flip_ts)
    std = _st_state_at_ts(st_1d, flip_ts)
    aligned_tfs = ["1h"]
    if st4 and st4["trend"] == flip_bar["trend"]:
        aligned_tfs.append("4h")
    if std and std["trend"] == flip_bar["trend"]:
        aligned_tfs.append("1d")

    if "4h" in aligned_tfs and "1d" in aligned_tfs:
        return "mtf", {"aligned_bots": [], "aligned_tfs": aligned_tfs}

    # 3. Daily (1h + 1d)
    if "1d" in aligned_tfs:
        return "daily", {"aligned_bots": [], "aligned_tfs": aligned_tfs}

    return None, {}


def _compute_sl(flip_bar: dict, direction: str, atr_mult: float = 1.5) -> float:
    """SL = min(ST_value, 1.5 × ATR). Всегда на корректной стороне от entry."""
    entry = flip_bar["close"]
    st_val = flip_bar["st"]
    atr = flip_bar["atr"] or 0
    is_long = direction == "LONG"
    atr_sl = entry - atr_mult * atr if is_long else entry + atr_mult * atr
    if is_long:
        sl = max(st_val or atr_sl, atr_sl)
    else:
        sl = min(st_val or atr_sl, atr_sl)
    # Санити — если совпадает с entry, сдвигаем
    if abs(sl - entry) < entry * 0.0005:
        sl = entry - atr_mult * atr if is_long else entry + atr_mult * atr
    return sl


async def _save_signal(pair_norm: str, flip_bar: dict, tier: str, extras: dict,
                       st_period: int, st_mult: float) -> Optional[dict]:
    """Пишет в БД, возвращает созданный dict или None если уже есть (дубликат)."""
    from database import _supertrend_signals, utcnow
    from collections import namedtuple

    direction = "LONG" if flip_bar["trend"] == 1 else "SHORT"
    entry = flip_bar["close"]
    sl = _compute_sl(flip_bar, direction)
    flip_at = datetime.utcfromtimestamp(flip_bar["t"] / 1000)

    doc = {
        "pair": pair_norm[:-4] + "/USDT",
        "pair_norm": pair_norm,
        "direction": direction,
        "tier": tier,
        "entry_price": entry,
        "sl_price": sl,
        "st_tf": "1h",
        "st_period": st_period,
        "st_mult": st_mult,
        "st_value": flip_bar["st"],
        "aligned_tfs": extras.get("aligned_tfs", []),
        "aligned_bots": extras.get("aligned_bots", []),
        "flip_at": flip_at,
        "created_at": utcnow(),
    }
    # Next autoincrement id (через to_thread — иначе блокирует event loop)
    def _get_seq():
        from database import _get_db
        return _get_db().counters.find_one_and_update(
            {"_id": "supertrend_signals"},
            {"$inc": {"seq": 1}},
            upsert=True, return_document=True,
        )
    try:
        counter = await asyncio.wait_for(asyncio.to_thread(_get_seq), timeout=5.0)
        doc["id"] = (counter or {}).get("seq", 1)
    except (asyncio.TimeoutError, Exception):
        doc["id"] = int(utcnow().timestamp())

    # Insert через to_thread — в горячем 5-минутном цикле обрабатывается
    # ~486 пар, sync insert вешал event loop пока Atlas отвечает.
    def _do_insert():
        _supertrend_signals().insert_one(doc)
    try:
        await asyncio.wait_for(asyncio.to_thread(_do_insert), timeout=5.0)
        # Инвалидация journal cache чтоб новый ST-сигнал сразу появился в UI
        try:
            from cache_utils import journal_cache
            journal_cache.invalidate("journal_all")
        except Exception:
            pass
        # ── NEW STRATEGIES: запускаем 3 детектора (Volume Surge / Triple
        # Confluence / Volume Accumulation) после успешного сохранения ST flip.
        # Каждый детектор сам решает попадает ли flip под их критерии.
        # Backtest validated 14d, 11.5k signals: WR 67-72%, E +0.94...+1.46R.
        try:
            import new_strategies as ns
            asyncio.create_task(ns.run_detectors_on_flip(
                pair=doc["pair"], direction=direction,
                entry=entry, sl=sl, flip_ts=flip_at,
                signal_id=doc.get("id"), tier=tier,
            ))
        except Exception:
            logger.exception("[st-tracker] new_strategies detect fail")
        return doc
    except asyncio.TimeoutError:
        logger.warning(f"[st-tracker] save TIMEOUT {pair_norm}/{tier}")
        return None
    except Exception as e:
        # DuplicateKeyError (этот флип уже был записан) — тихо пропускаем
        if "duplicate" in str(e).lower() or "E11000" in str(e):
            return None
        logger.warning(f"[st-tracker] save fail {pair_norm}/{tier}: {e}")
        return None


def _format_alert_html(sig: dict) -> str:
    """HTML текст для BOT10."""
    tier = sig["tier"]
    tier_label = {"vip": "🏆 VIP", "mtf": "🔱 Triple MTF", "daily": "🧭 Daily Filter"}[tier]

    dir_emoji = "🟢" if sig["direction"] == "LONG" else "🔴"
    pair = sig["pair"]
    entry = sig["entry_price"]
    sl = sig["sl_price"]
    sl_pct = (sl - entry) / entry * 100
    sign = "+" if sl_pct > 0 else ""

    lines = [
        f"🌀 <b>ST SIGNAL · {tier_label}</b>",
        f"──────────────────────",
        f"{dir_emoji} <b>{pair}</b> · <b>{sig['direction']}</b> · 1h",
        f"Entry: <code>{entry}</code>",
        f"SL:    <code>{sl}</code> ({sign}{sl_pct:.2f}%, trail @ ST)",
    ]

    if tier == "vip" and sig.get("aligned_bots"):
        lines.append("")
        lines.append("✨ <b>Align (±2ч):</b>")
        src_emoji = {
            "tradium": "📡", "cryptovizor": "🚀", "anomaly": "⚠️",
            "confluence": "🎯", "cluster": "💠", "paper": "🤖",
        }
        for ab in sig["aligned_bots"][:5]:
            e = src_emoji.get(ab.get("source", ""), "•")
            # age in min
            try:
                at_dt = datetime.fromisoformat(ab["at"].replace("Z", "")) if isinstance(ab["at"], str) else ab["at"]
                age_min = int((sig["flip_at"] - at_dt).total_seconds() / 60)
                sign2 = "назад" if age_min >= 0 else "вперёд"
                lines.append(f"  {e} {ab.get('source','?').capitalize()} · {abs(age_min)} мин {sign2}")
            except Exception:
                lines.append(f"  {e} {ab.get('source','?').capitalize()}")
    elif tier in ("mtf", "daily"):
        tfs = " + ".join(sig.get("aligned_tfs", []))
        lines.append("")
        lines.append(f"📈 <b>Aligned:</b> {tfs}")

    # KL block
    try:
        from key_levels import build_levels_compact
        kl = build_levels_compact(sig["pair"], sig["direction"],
                                  entry=sig["entry_price"], sl=sig["sl_price"],
                                  at=sig["flip_at"])
        if kl:
            lines.append(kl.strip())
    except Exception:
        pass

    # ST state block
    try:
        from supertrend import format_tg_block as _st_fmt
        st_line = _st_fmt(sig["pair"], sig["direction"], "1h", cache_only=True)
        if st_line:
            lines.append(st_line.strip())
    except Exception:
        pass

    flip_str = sig["flip_at"].strftime("%H:%M UTC") if hasattr(sig["flip_at"], "strftime") else str(sig["flip_at"])
    lines.append("")
    lines.append(f"⏰ Flip {flip_str} · #{sig.get('id', '?')}")

    return "\n".join(lines)


async def _alert_signal(sig: dict) -> None:
    """1) Telegram BOT10 (только VIP и MTF — Daily по запросу пользователя
       не отправляется в бот, остаётся только эмоджи на графиках).
    2) Paper trader: ВСЕ tier'ы (vip/mtf/daily) передаются для AI-решения
       о входе. ST был добавлен позже основной интеграции — раньше его
       сигналы обходили paper, теперь исправлено."""
    tier = sig.get("tier", "daily")

    # === 1. Telegram (кроме daily) ===
    if tier != "daily":
        try:
            from watcher import _bot10, _admin_chat_id
            if _bot10 and _admin_chat_id:
                text = _format_alert_html(sig)
                try:
                    await _bot10.send_message(_admin_chat_id, text, parse_mode="HTML")
                except Exception as e:
                    logger.warning(f"[st-tracker] BOT10 send fail: {e}")
        except Exception:
            logger.debug("[st-tracker] watcher module not ready for BOT10")

    # === 2. Paper trader — AI решает входить или нет ===
    # Передаём все 3 tier'а. VIP и MTF имеют хорошую edge по бектесту,
    # Daily — слабее но тоже попадёт в ai_decide чтобы AI сам решил.
    try:
        from watcher import _paper_on_signal
        pair = sig.get("pair") or ""
        symbol = sig.get("pair_norm") or pair.replace("/", "").upper()
        # tier → score mapping (чтобы AI видел силу)
        tier_score = {"vip": 9, "mtf": 7, "daily": 5}.get(tier, 5)
        # Флаг is_top_pick для VIP — AI применит boost
        await _paper_on_signal({
            "symbol": symbol,
            "pair": pair,
            "direction": sig.get("direction"),
            "entry": sig.get("entry_price"),
            "tp1": None,                       # trailing via ST — AI сам поставит
            "sl": sig.get("sl_price"),
            "source": "supertrend",
            "score": tier_score,
            "pattern": f"ST {tier.upper()}",
            "st_tier": tier,
            "is_top_pick": tier == "vip",
            "aligned_bots": sig.get("aligned_bots", []),
            "aligned_tfs": sig.get("aligned_tfs", []),
        })
    except Exception as e:
        logger.debug(f"[st-tracker] paper route fail: {e}")


async def _process_pair(pair_norm: str, alert_enabled: bool = True) -> int:
    """Проверяет одну пару. Возвращает кол-во новых сигналов."""
    from exchange import get_klines_any
    from backtest_supertrend import compute_st_series, find_flips
    pair_slash = pair_norm[:-4] + "/USDT"

    # ST params для 1h
    ST_PERIOD, ST_MULT = 10, 3.0

    try:
        c1h = await asyncio.to_thread(get_klines_any, pair_slash, "1h", 400)
    except Exception as e:
        logger.debug(f"[st-tracker] {pair_norm} 1h fetch fail: {e}")
        return 0
    if not c1h or len(c1h) < 30:
        return 0

    st_1h = compute_st_series(c1h, ST_PERIOD, ST_MULT)
    if not st_1h:
        return 0

    # Находим флипы. Считаем «новым» тот где flip_at в окне последних N минут.
    from database import utcnow
    now_dt = utcnow()
    cutoff_ms = int((now_dt - timedelta(minutes=FRESH_FLIP_WINDOW_MIN)).timestamp() * 1000)
    flips = find_flips(st_1h)
    fresh_flip_indices = [i for i in flips if st_1h[i]["t"] >= cutoff_ms]
    if not fresh_flip_indices:
        return 0

    # Подтягиваем 4h и 1d только если есть свежие флипы (экономим API)
    c4h = c1d = None
    try:
        c4h = await asyncio.to_thread(get_klines_any, pair_slash, "4h", 150)
        c1d = await asyncio.to_thread(get_klines_any, pair_slash, "1d", 40)
    except Exception:
        pass
    st_4h = compute_st_series(c4h or [], 10, 3.0)
    st_1d = compute_st_series(c1d or [], 14, 3.0)

    created = 0
    for fidx in fresh_flip_indices:
        flip_bar = st_1h[fidx]
        # _classify_flip содержит 3× sync Mongo find — через to_thread.
        # Без этого 5-мин цикл по ~486 парам блокировал event loop надолго.
        try:
            tier, extras = await asyncio.wait_for(
                asyncio.to_thread(_classify_flip, pair_norm, flip_bar, st_4h, st_1d),
                timeout=10.0,
            )
        except (asyncio.TimeoutError, Exception):
            continue
        if tier is None:
            continue
        sig = await _save_signal(pair_norm, flip_bar, tier, extras, ST_PERIOD, ST_MULT)
        if sig:
            created += 1
            if alert_enabled:
                await _alert_signal(sig)
    return created


async def check_all_pairs(alert_enabled: bool = True) -> dict:
    """Проверка всех tracked pairs. Возвращает stats."""
    pairs = await asyncio.to_thread(get_tracked_pairs)
    stats = {"pairs_total": len(pairs), "pairs_processed": 0,
             "signals_created": 0, "errors": 0,
             "by_tier": {"vip": 0, "mtf": 0, "daily": 0}}
    for p in pairs:
        try:
            n = await _process_pair(p, alert_enabled=alert_enabled)
            stats["pairs_processed"] += 1
            stats["signals_created"] += n
        except Exception as e:
            logger.warning(f"[st-tracker] {p} fail: {e}")
            stats["errors"] += 1
        # Мягкая пауза чтоб не забить Binance rate limit
        if stats["pairs_processed"] % 20 == 0:
            await asyncio.sleep(0.5)
    logger.info(f"[st-tracker] cycle done: {stats}")
    return stats


async def backfill(days: int = 14, alert_enabled: bool = False,
                   on_progress=None) -> dict:
    """Тихий (по умолчанию) backfill исторических флипов за N дней.
    alert_enabled=False — не шлёт в Telegram, только заполняет БД."""
    from exchange import get_klines_any
    from backtest_supertrend import compute_st_series, find_flips
    from database import utcnow

    pairs = await asyncio.to_thread(get_tracked_pairs)
    total = len(pairs)
    since_ms = int((utcnow() - timedelta(days=days)).timestamp() * 1000)
    stats = {
        "pairs_total": total, "pairs_processed": 0, "pairs_skipped": 0,
        "signals_created": 0, "by_tier": {"vip": 0, "mtf": 0, "daily": 0},
        "errors": 0,
    }
    ST_PERIOD, ST_MULT = 10, 3.0

    for idx, pair_norm in enumerate(pairs, 1):
        if on_progress:
            try: on_progress(idx, total, pair_norm)
            except Exception: pass
        pair_slash = pair_norm[:-4] + "/USDT"
        try:
            c1h = await asyncio.to_thread(get_klines_any, pair_slash, "1h", 400)
            if not c1h or len(c1h) < 30:
                stats["pairs_skipped"] += 1
                continue
            c4h = await asyncio.to_thread(get_klines_any, pair_slash, "4h", 150)
            c1d = await asyncio.to_thread(get_klines_any, pair_slash, "1d", 40)
        except Exception as e:
            logger.debug(f"[backfill] {pair_norm} fetch fail: {e}")
            stats["errors"] += 1
            continue

        st_1h = compute_st_series(c1h, ST_PERIOD, ST_MULT)
        st_4h = compute_st_series(c4h or [], 10, 3.0)
        st_1d = compute_st_series(c1d or [], 14, 3.0)
        if not st_1h:
            stats["pairs_skipped"] += 1
            continue

        flips = find_flips(st_1h)
        # Только флипы в окне backfill
        window_flips = [i for i in flips if st_1h[i]["t"] >= since_ms]
        for fidx in window_flips:
            flip_bar = st_1h[fidx]
            try:
                tier, extras = await asyncio.wait_for(
                    asyncio.to_thread(_classify_flip, pair_norm, flip_bar, st_4h, st_1d),
                    timeout=10.0,
                )
            except (asyncio.TimeoutError, Exception):
                continue
            if tier is None:
                continue
            sig = await _save_signal(pair_norm, flip_bar, tier, extras, ST_PERIOD, ST_MULT)
            if sig:
                stats["signals_created"] += 1
                stats["by_tier"][tier] = stats["by_tier"].get(tier, 0) + 1
                if alert_enabled:
                    await _alert_signal(sig)
        stats["pairs_processed"] += 1
        # Паузы чтоб не ретлимитнуться
        if idx % 25 == 0:
            await asyncio.sleep(1.0)
    logger.info(f"[backfill] done: {stats}")
    return stats
