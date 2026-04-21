"""Verified Entry Checker — автоматическая rule-based проверка каждого
сигнала на 8 пунктах + отправка в @topmonetabot если verdict=GO.

Используется из watcher'а через asyncio.create_task — не блокирует основной
поток сигналов, работает параллельно (задержка ~3-5 сек).

Логика проверок копируется из endpoint'а /api/entry-checker (market_phase
+ ST 1h + Key Levels + R:R + Volume/OI + диверсификация + reco size/lev).

Эмоджи:
  ✨      — verdict=GO
  ⚠️✨    — verdict=CAUTION + source in (cluster, supertrend_vip)
"""
from __future__ import annotations
import logging
from datetime import timedelta

logger = logging.getLogger(__name__)


# ───────────────────────────────────────────────────────────────────
# CHECK ENTRY — rule-based проверка (без AI)
# ───────────────────────────────────────────────────────────────────
def check_entry(pair: str, direction: str,
                provided_signal: dict | None = None) -> dict:
    """Выполняет 8-пунктовый чек-лист. Возвращает тот же формат что и
    /api/entry-checker endpoint.

    Если provided_signal задан — используем его (сигнал только что пришёл,
    не надо искать в Mongo за 4ч).
    """
    from database import (_signals, _anomalies, _confluence, _clusters,
                          _supertrend_signals, _key_levels, utcnow)
    from supertrend import supertrend_state
    import market_phase as mp
    import paper_trader as pt

    direction = (direction or "LONG").upper()
    if direction not in ("LONG", "SHORT"):
        return {"ok": False, "error": "bad direction"}

    raw_pair = (pair or "").strip().upper()
    pair_slash = raw_pair.replace("USDT", "/USDT") if ("USDT" in raw_pair and "/" not in raw_pair) else raw_pair
    pair_norm = pair_slash.replace("/", "")
    if not pair_norm.endswith("USDT"):
        pair_norm = pair_norm + "USDT"

    # ── Сигнал: берём переданный или ищем свежий в Mongo ──
    if provided_signal:
        found = dict(provided_signal)
        sig_time = utcnow()
    else:
        since = utcnow() - timedelta(hours=4)
        found = None
        for col, q, tf, name in [
            (_signals(), {"source": "tradium", "pair": pair_slash, "received_at": {"$gte": since}}, "received_at", "tradium"),
            (_signals(), {"source": "cryptovizor", "pattern_triggered": True, "pair": pair_slash, "pattern_triggered_at": {"$gte": since}}, "pattern_triggered_at", "cryptovizor"),
            (_confluence(), {"$or": [{"pair": pair_slash}, {"symbol": pair_norm}], "detected_at": {"$gte": since}}, "detected_at", "confluence"),
            (_anomalies(), {"$or": [{"pair": pair_slash}, {"symbol": pair_norm}], "detected_at": {"$gte": since}}, "detected_at", "anomaly"),
            (_clusters(), {"pair": pair_slash, "trigger_at": {"$gte": since}}, "trigger_at", "cluster"),
            (_supertrend_signals(), {"pair_norm": pair_norm, "flip_at": {"$gte": since}, "tier": {"$in": ["vip", "mtf"]}}, "flip_at", "supertrend"),
        ]:
            try:
                doc = col.find_one(q, sort=[(tf, -1)])
                if doc:
                    doc["_source_name"] = name
                    doc["_time_field"] = tf
                    if found is None or doc.get(tf) > found.get(found["_time_field"], utcnow() - timedelta(days=999)):
                        found = doc
            except Exception:
                pass
        if not found:
            return {"ok": False, "error": f"Нет активного сигнала на {pair_slash}"}
        sig_time = found.get(found["_time_field"])

    sig_source = found.get("_source_name") or found.get("source") or "?"
    sig_dir = (found.get("direction") or direction).upper()
    sig_entry = found.get("entry") or found.get("price") or found.get("pattern_price") or found.get("trigger_price") or found.get("entry_price")
    sig_tp = found.get("tp1") or found.get("r1") or found.get("dca2") or found.get("tp_price")
    sig_sl = found.get("sl") or found.get("s1") or found.get("dca1") or found.get("sl_price")
    sig_score = found.get("ai_score") or found.get("score") or found.get("reversal_score")
    sig_pattern = found.get("pattern_name") or found.get("pattern")
    tier = found.get("tier")
    minutes_ago = int((utcnow() - sig_time).total_seconds() / 60) if sig_time else 0

    # ── ATR fallback для TP/SL если их нет в сигнале (anomaly/supertrend и т.п.) ──
    tp_sl_source = "signal"
    if sig_entry and (not sig_tp or not sig_sl):
        try:
            from exchange import get_klines_any
            candles_1h = get_klines_any(pair_slash, "1h", 30) or []
            if candles_1h and len(candles_1h) >= 15:
                trs = []
                for i in range(1, len(candles_1h)):
                    c, pc = candles_1h[i], candles_1h[i-1]["c"]
                    trs.append(max(c["h"] - c["l"], abs(c["h"] - pc), abs(c["l"] - pc)))
                atr = sum(trs[-14:]) / min(14, len(trs))
                if direction == "LONG":
                    if not sig_sl: sig_sl = sig_entry - atr * 1.5
                    if not sig_tp: sig_tp = sig_entry + atr * 2.5
                else:
                    if not sig_sl: sig_sl = sig_entry + atr * 1.5
                    if not sig_tp: sig_tp = sig_entry - atr * 2.5
                tp_sl_source = "ATR×1.5/2.5"
        except Exception:
            logger.debug("[verified] ATR TP/SL fallback fail", exc_info=True)

    checks = []

    # ── 1. Market Phase ──
    try:
        phase_data = mp.get_market_phase()
    except Exception:
        phase_data = {"phase": "NEUTRAL", "label": "Neutral", "emoji": "❓", "metrics": {}}
    phase = phase_data.get("phase", "NEUTRAL")
    phase_label = phase_data.get("label", phase)
    phase_emoji = phase_data.get("emoji", "❓")
    if phase == "BULL_TREND":
        status = "warn" if direction == "SHORT" else "ok"
        comment = (f"{phase_emoji} {phase_label}: SHORT против тренда" if status == "warn"
                   else f"{phase_emoji} {phase_label}: LONG совпадает с трендом")
    elif phase == "BEAR_TREND":
        status = "warn" if direction == "LONG" else "ok"
        comment = (f"{phase_emoji} {phase_label}: LONG против тренда (только контр-отскок)"
                   if status == "warn" else f"{phase_emoji} {phase_label}: SHORT совпадает с трендом")
    elif phase == "CHOP":
        status = "warn"
        comment = f"{phase_emoji} CHOP: малые сайзы, whipsaw risk"
    elif phase == "VOLATILE":
        status = "bad"
        comment = f"{phase_emoji} VOLATILE: только VIP с double-confirm или Cluster MEGA"
    elif phase == "EUPHORIA":
        status = "bad" if direction == "LONG" else "warn"
        comment = f"{phase_emoji} EUPHORIA: LONG опасно"
    elif phase == "CAPITULATION":
        status = "ok" if direction == "LONG" else "bad"
        comment = f"{phase_emoji} CAPITULATION: {'LONG на отскок' if direction == 'LONG' else 'SHORT опасно'}"
    else:
        status = "warn"
        comment = f"{phase_emoji} {phase_label}"
    checks.append({"name": "Market Phase", "status": status, "comment": comment, "value": f"phase={phase}"})

    # ── 2. Свежий сигнал ──
    tier_str = f" · tier={tier}" if tier else ""
    score_str = f" · score={sig_score}" if sig_score is not None else ""
    if sig_dir == direction:
        checks.append({"name": "Свежий сигнал", "status": "ok",
                       "comment": f"{sig_source}{tier_str}, {minutes_ago} мин назад{score_str}",
                       "value": f"pattern={sig_pattern}" if sig_pattern else ""})
    else:
        checks.append({"name": "Свежий сигнал", "status": "bad",
                       "comment": f"⚠ Сигнал {sig_dir}, вход {direction} — не совпадает",
                       "value": ""})

    # ── 3. ST 1h aligned ──
    st_state = "UNK"
    st_err = None
    # Пробуем 2 раза: 1) cache hit если прогрето, 2) с HTTP fetch если нет
    for attempt in range(2):
        try:
            st = supertrend_state(pair_slash, "1h", cache_only=(attempt == 0))
            if st and st.get("state"):
                st_state = st["state"]
                break
        except Exception as _e:
            st_err = str(_e)[:60]
    st_dir = "LONG" if st_state == "UP" else ("SHORT" if st_state == "DOWN" else "?")
    if st_state == "UNK":
        reason = f" ({st_err})" if st_err else " (пара не найдена на Binance или нет свечей)"
        checks.append({"name": "ST 1h aligned", "status": "warn",
                       "comment": f"Не удалось получить ST 1h{reason}", "value": ""})
    elif st_dir == direction:
        checks.append({"name": "ST 1h aligned", "status": "ok",
                       "comment": f"ST 1h = {st_state} — совпадает", "value": ""})
    else:
        checks.append({"name": "ST 1h aligned", "status": "bad",
                       "comment": f"⛔ ST 1h = {st_state} — вход против тренда 1h", "value": ""})

    # ── 4. Key Levels ──
    try:
        kl_since = utcnow() - timedelta(hours=168)
        kls = list(_key_levels().find({"pair_norm": pair_norm, "detected_at": {"$gte": kl_since}}))
        has_sl_protected = False
        tp_warning = False
        if kls and sig_entry and sig_sl and sig_tp:
            if direction == "LONG":
                supports = [k for k in kls if k.get("event") in ("new_support", "entered_support")]
                resistances = [k for k in kls if k.get("event") in ("new_resistance", "entered_resistance")]
                for k in supports:
                    lvl = k.get("zone_low") or k.get("current_price") or 0
                    if sig_sl < lvl < sig_entry + (sig_entry * 0.005):
                        has_sl_protected = True; break
                for k in resistances:
                    lvl = k.get("zone_high") or k.get("current_price") or 0
                    if sig_entry < lvl < sig_tp:
                        tp_warning = True; break
            else:
                resistances = [k for k in kls if k.get("event") in ("new_resistance", "entered_resistance")]
                supports = [k for k in kls if k.get("event") in ("new_support", "entered_support")]
                for k in resistances:
                    lvl = k.get("zone_high") or k.get("current_price") or 0
                    if sig_entry - (sig_entry * 0.005) < lvl < sig_sl:
                        has_sl_protected = True; break
                for k in supports:
                    lvl = k.get("zone_low") or k.get("current_price") or 0
                    if sig_tp < lvl < sig_entry:
                        tp_warning = True; break
        if not kls:
            checks.append({"name": "Key Levels", "status": "warn", "comment": "Нет KL данных", "value": ""})
        elif has_sl_protected and not tp_warning:
            checks.append({"name": "Key Levels", "status": "ok",
                           "comment": f"SL защищён ✅ TP до R ✅ ({len(kls)} ур.)", "value": ""})
        elif has_sl_protected and tp_warning:
            checks.append({"name": "Key Levels", "status": "warn",
                           "comment": "SL защищён ✅, но TP за R ⚠", "value": ""})
        elif not has_sl_protected and not tp_warning:
            checks.append({"name": "Key Levels", "status": "warn",
                           "comment": "SL без поддержки уровнем", "value": ""})
        else:
            checks.append({"name": "Key Levels", "status": "bad",
                           "comment": "SL не защищён И TP за уровнем", "value": ""})
    except Exception:
        checks.append({"name": "Key Levels", "status": "warn",
                       "comment": "Ошибка проверки KL", "value": ""})

    # ── 5. R:R ──
    rr = None
    if sig_entry and sig_tp and sig_sl:
        risk = abs(sig_entry - sig_sl)
        reward = abs(sig_tp - sig_entry)
        if risk > 0:
            rr = round(reward / risk, 2)
    src_note = f" (TP/SL: {tp_sl_source})"
    val_str = f"entry={sig_entry}, tp={round(sig_tp,6) if sig_tp else '?'}, sl={round(sig_sl,6) if sig_sl else '?'}"
    if rr is None:
        checks.append({"name": "R:R", "status": "warn", "comment": "Не удалось вычислить TP/SL", "value": ""})
    elif rr >= 2.0:
        checks.append({"name": "R:R", "status": "ok", "comment": f"R:R = 1:{rr} — отлично{src_note}", "value": val_str})
    elif rr >= 1.5:
        checks.append({"name": "R:R", "status": "ok", "comment": f"R:R = 1:{rr} — приемлемо{src_note}", "value": val_str})
    elif rr >= 1.0:
        checks.append({"name": "R:R", "status": "warn", "comment": f"R:R = 1:{rr} — слабое{src_note}", "value": val_str})
    else:
        checks.append({"name": "R:R", "status": "bad", "comment": f"R:R = 1:{rr} — плохое{src_note}", "value": val_str})

    # ── 6. Volume / OI ──
    try:
        from anomaly_scanner import _batch_cache
        vol_spike = (_batch_cache.get("volume_spike") or {}).get(pair_norm)
        oi_ch = (_batch_cache.get("oi_change") or {}).get(pair_norm)
        funding = (_batch_cache.get("funding") or {}).get(pair_norm)

        # Fallback: если нет в batch-кеше — вытягиваем напрямую с Binance Futures
        if vol_spike is None or oi_ch is None:
            try:
                from exchange import check_pump_potential
                pump = check_pump_potential(pair_norm)
                if vol_spike is None:
                    vol_spike = pump.get("volume_spike")
                if oi_ch is None:
                    oi_ch = pump.get("oi_change")
                if funding is None:
                    funding = pump.get("funding")
            except Exception:
                logger.debug("[verified] pump fallback fail", exc_info=True)

        vol_str = f"Vol×{vol_spike:.1f}" if vol_spike else "Vol×?"
        oi_str = f"OI {oi_ch:+.1f}%" if oi_ch is not None else "OI ?"
        fund_str = f"funding {funding:+.3f}%" if funding else ""
        detail = f"{vol_str} · {oi_str} {fund_str}".strip()
        oi_ok = (oi_ch is not None and abs(oi_ch) >= 1.0)
        vol_ok = (vol_spike is not None and vol_spike >= 1.5)
        if vol_ok and oi_ok:
            checks.append({"name": "Volume / OI", "status": "ok", "comment": "Движение с объёмом и OI", "value": detail})
        elif vol_ok or oi_ok:
            checks.append({"name": "Volume / OI", "status": "warn", "comment": "Частичное подтверждение", "value": detail})
        elif vol_spike is None and oi_ch is None:
            checks.append({"name": "Volume / OI", "status": "warn", "comment": "Нет данных (пара не на фьючах Binance?)", "value": detail})
        else:
            checks.append({"name": "Volume / OI", "status": "bad", "comment": "Без объёма/OI — слабое движение", "value": detail})
    except Exception:
        checks.append({"name": "Volume / OI", "status": "warn", "comment": "Err", "value": ""})

    # ── 7. Диверсификация ──
    try:
        open_pos = pt.get_open_positions()
        same_dir = sum(1 for p in open_pos if (p.get("direction") or "").upper() == direction)
        opp_dir = sum(1 for p in open_pos if (p.get("direction") or "").upper() != direction)
        on_this_pair = sum(1 for p in open_pos if (p.get("symbol") or "").upper() == pair_norm)
        if on_this_pair > 0:
            checks.append({"name": "Диверсификация", "status": "bad",
                           "comment": f"Уже открыта позиция на {pair_norm}", "value": ""})
        elif same_dir >= 4:
            checks.append({"name": "Диверсификация", "status": "bad",
                           "comment": f"Уже {same_dir} в {direction} — макс 3-4", "value": ""})
        elif same_dir >= 3:
            checks.append({"name": "Диверсификация", "status": "warn",
                           "comment": f"3 позиции в {direction}", "value": ""})
        else:
            checks.append({"name": "Диверсификация", "status": "ok",
                           "comment": f"{same_dir} в {direction} · {opp_dir} противоп.", "value": ""})
    except Exception:
        checks.append({"name": "Диверсификация", "status": "warn", "comment": "Err", "value": ""})

    # ── 8. Reco ──
    size_reco = {"BULL_TREND": "5-10%", "BEAR_TREND": "5-10%", "CHOP": "2-4%",
                 "VOLATILE": "1-3%", "EUPHORIA": "3-5%", "CAPITULATION": "3-5%"}.get(phase, "3-5%")
    lev_reco = {"BULL_TREND": "5-10×", "BEAR_TREND": "5-10×", "CHOP": "3-5×",
                "VOLATILE": "2-4×", "EUPHORIA": "3-5×", "CAPITULATION": "3-5×"}.get(phase, "3-5×")
    checks.append({"name": "Размер/плечо", "status": "ok",
                   "comment": f"Для {phase}: {size_reco} · {lev_reco}", "value": ""})

    # ── Вердикт ──
    n_bad = sum(1 for c in checks if c["status"] == "bad")
    n_warn = sum(1 for c in checks if c["status"] == "warn")
    n_ok = sum(1 for c in checks if c["status"] == "ok")
    if n_bad >= 2:
        verdict = "skip"
        summary = f"❌ {n_bad} красных флагов"
    elif n_bad == 1 and n_warn >= 3:
        verdict = "skip"
        summary = f"❌ 1 блокер + {n_warn} предупреждений"
    elif n_bad == 1:
        verdict = "caution"
        summary = f"⚠️ 1 красный флаг"
    elif n_warn >= 3:
        verdict = "caution"
        summary = f"⚠️ {n_warn} предупреждений"
    else:
        verdict = "go"
        summary = f"✅ {n_ok} OK, {n_warn} warn"

    return {
        "ok": True,
        "pair": pair_slash,
        "pair_norm": pair_norm,
        "direction": direction,
        "verdict": verdict,
        "summary": summary,
        "checks": checks,
        "counts": {"ok": n_ok, "warn": n_warn, "bad": n_bad},
        "signal": {
            "source": sig_source, "direction": sig_dir, "tier": tier,
            "entry": sig_entry, "tp1": sig_tp, "sl": sig_sl,
            "score": sig_score, "pattern": sig_pattern,
            "minutes_ago": minutes_ago,
            "rr": rr,
        },
        "market": {
            "phase": phase, "phase_label": phase_label, "phase_emoji": phase_emoji,
            "btc_st": phase_data.get("metrics", {}).get("btc_st", {}),
            "atr_1h_pct": phase_data.get("metrics", {}).get("atr_1h_pct"),
            "avg_funding": phase_data.get("metrics", {}).get("avg_funding"),
        },
    }


# ───────────────────────────────────────────────────────────────────
# DUPLICATE CHECK
# ───────────────────────────────────────────────────────────────────
def is_duplicate(pair_norm: str, direction: str, hours: int = 1) -> bool:
    """Был ли уже verified на эту пару + direction за последние N часов?"""
    try:
        from database import _get_db, utcnow
        since = utcnow() - timedelta(hours=hours)
        col = _get_db().verified_signals
        doc = col.find_one({
            "pair_norm": pair_norm,
            "direction": direction,
            "created_at": {"$gte": since},
        })
        return doc is not None
    except Exception:
        return False


# ───────────────────────────────────────────────────────────────────
# RECORD TO DB
# ───────────────────────────────────────────────────────────────────
def record_verified(result: dict) -> str | None:
    """Пишет в коллекцию verified_signals. Возвращает _id или None."""
    try:
        from database import _get_db, utcnow
        col = _get_db().verified_signals
        sig = result.get("signal", {})
        market = result.get("market", {})
        doc = {
            "created_at": utcnow(),
            "pair": result.get("pair"),
            "pair_norm": result.get("pair_norm"),
            "direction": result.get("direction"),
            "verdict": result.get("verdict"),
            "summary": result.get("summary"),
            "counts": result.get("counts", {}),
            "signal_source": sig.get("source"),
            "signal_tier": sig.get("tier"),
            "signal_score": sig.get("score"),
            "signal_pattern": sig.get("pattern"),
            "entry": sig.get("entry"),
            "tp1": sig.get("tp1"),
            "sl": sig.get("sl"),
            "rr": sig.get("rr"),
            "phase": market.get("phase"),
            "btc_st": market.get("btc_st"),
        }
        r = col.insert_one(doc)
        return str(r.inserted_id) if r else None
    except Exception:
        logger.exception("[verified] record fail")
        return None


# ───────────────────────────────────────────────────────────────────
# TELEGRAM ALERT
# ───────────────────────────────────────────────────────────────────
def _format_message(result: dict, emoji_prefix: str = "✨") -> str:
    """Форматирует сообщение для отправки в Telegram."""
    sig = result.get("signal", {})
    market = result.get("market", {})
    counts = result.get("counts", {})
    btc = market.get("btc_st", {})
    direction = result.get("direction", "?")
    pair = result.get("pair", "?")
    pair_display = pair.replace("/", "")

    entry = sig.get("entry")
    tp = sig.get("tp1")
    sl = sig.get("sl")
    rr = sig.get("rr")
    source = sig.get("source", "?")
    score = sig.get("score")
    tier = sig.get("tier")
    mins = sig.get("minutes_ago", 0)
    phase_label = market.get("phase_label", market.get("phase", "?"))
    atr = market.get("atr_1h_pct")
    funding = market.get("avg_funding")

    def _fmt_price(v):
        if v is None:
            return "?"
        if v >= 100: return f"{v:.2f}"
        if v >= 1:   return f"{v:.4f}"
        return f"{v:.6f}".rstrip("0").rstrip(".")

    def _fmt_st(s):
        return "↑" if s == "UP" else ("↓" if s == "DOWN" else "—")

    src_part = source
    if tier: src_part += f"/{tier}"
    if score is not None: src_part += f" (score {score})"

    rr_part = f"R:R 1:{rr}" if rr else "R:R ?"
    st1h = _fmt_st(btc.get("1h"))

    lines = [
        f"{emoji_prefix} <b>VERIFIED ENTRY — {direction} {pair_display}</b>",
        "",
        f"📊 Источник: {src_part}",
        f"🕐 {mins} мин назад",
        f"💰 Entry: {_fmt_price(entry)} · TP: {_fmt_price(tp)} · SL: {_fmt_price(sl)}",
        f"📈 {rr_part} · ST 1h {st1h}",
        "",
        f"✅ {counts.get('ok',0)} OK · ⚠️ {counts.get('warn',0)} warn · ❌ {counts.get('bad',0)} bad",
    ]
    footer_parts = [phase_label]
    if atr is not None: footer_parts.append(f"ATR {atr:.2f}%")
    if funding is not None: footer_parts.append(f"Funding {funding:+.3f}%")
    lines.append(" · ".join(footer_parts))
    return "\n".join(lines)


async def send_verified_alert(bot, chat_id, topic_id, result: dict,
                               emoji_prefix: str = "✨") -> bool:
    """Отправляет сообщение в Telegram. Возвращает True если успешно."""
    if not bot or not chat_id:
        logger.warning("[verified] нет bot/chat_id — скип отправки")
        return False
    try:
        text = _format_message(result, emoji_prefix)
        kwargs = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        if topic_id:
            kwargs["message_thread_id"] = topic_id
        await bot.send_message(**kwargs)
        return True
    except Exception:
        logger.exception("[verified] alert fail")
        return False


# ───────────────────────────────────────────────────────────────────
# HIGH-LEVEL ORCHESTRATOR — вызывается из watcher через create_task
# ───────────────────────────────────────────────────────────────────
async def run_verified_check(signal_data: dict, bot=None, chat_id=None, topic_id=None):
    """Главная функция. Вызывается как asyncio.create_task из watcher.

    signal_data: сигнал только что пришёл с полями {symbol/pair, direction, source, entry, tp1, sl, score, pattern}
    bot, chat_id, topic_id: Telegram bot + куда слать
    """
    import asyncio as _asyncio

    pair = signal_data.get("pair") or (signal_data.get("symbol", "").replace("USDT", "/USDT") if "USDT" in (signal_data.get("symbol") or "") else signal_data.get("symbol"))
    direction = (signal_data.get("direction") or "").upper()
    source = (signal_data.get("source") or "").lower()

    if not pair or direction not in ("LONG", "SHORT"):
        return

    # 1. Анти-дубль
    pair_norm = pair.replace("/", "").upper()
    if not pair_norm.endswith("USDT"):
        pair_norm = pair_norm + "USDT"
    if is_duplicate(pair_norm, direction, hours=1):
        logger.info(f"[verified] duplicate skip {pair_norm} {direction}")
        return

    # 2. Чек-лист
    try:
        result = await _asyncio.to_thread(check_entry, pair, direction, signal_data)
    except Exception:
        logger.exception("[verified] check_entry crashed")
        return
    if not result.get("ok"):
        logger.info(f"[verified] check failed: {result.get('error')}")
        return

    verdict = result.get("verdict")

    # 3. Применяем критерии
    emoji = None
    if verdict == "go":
        emoji = "✨"
    elif verdict == "caution" and source in ("cluster", "supertrend_vip", "supertrend"):
        # CAUTION только для сильных источников (cluster / supertrend VIP)
        tier = (signal_data.get("tier") or result.get("signal", {}).get("tier") or "").lower()
        if source == "cluster" or (source == "supertrend" and tier == "vip") or source == "supertrend_vip":
            emoji = "⚠️✨"

    if not emoji:
        logger.info(f"[verified] {pair_norm} {direction} verdict={verdict} src={source} — skip")
        return

    # 4. Запись в БД
    record_verified(result)

    # 5. Отправка
    sent = await send_verified_alert(bot, chat_id, topic_id, result, emoji_prefix=emoji)
    if sent:
        logger.info(f"[verified] SENT {emoji} {pair_norm} {direction} (verdict={verdict}, src={source})")
