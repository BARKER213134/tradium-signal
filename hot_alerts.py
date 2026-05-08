"""🔥 HOT alerts — отправка в BOT15 только сигналов score >= 60.

Дедупликация: одна (pair, direction, hour_bucket) — 1 alert/час.
Защита от спама: max 10 alerts/час глобально.
"""
from __future__ import annotations
import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Optional

from quality_score import compute_signal_score, score_label

logger = logging.getLogger(__name__)

HOT_MIN_SCORE = 60
GLOBAL_RATE_LIMIT_PER_HOUR = 10
DEDUP_WINDOW_MIN = 60

# in-memory dedup (pair_dir_hour) → ts; также пишем в Mongo для recovery
_recent_sent: dict = {}
_global_rate: list = []  # timestamps of recent sends


def _can_send(key: str, now_ts: float) -> bool:
    """Rate limiting + per-pair dedup."""
    # Global: max 10/hour
    cutoff = now_ts - 3600
    while _global_rate and _global_rate[0] < cutoff:
        _global_rate.pop(0)
    if len(_global_rate) >= GLOBAL_RATE_LIMIT_PER_HOUR:
        return False
    # Per-key: 60 min window
    last = _recent_sent.get(key)
    if last and (now_ts - last) < DEDUP_WINDOW_MIN * 60:
        return False
    return True


def _mark_sent(key: str, now_ts: float) -> None:
    _recent_sent[key] = now_ts
    _global_rate.append(now_ts)
    # Cleanup old keys (> 2h)
    cutoff = now_ts - 7200
    for k in list(_recent_sent.keys()):
        if _recent_sent[k] < cutoff:
            _recent_sent.pop(k, None)


def _format_alert(sig: dict, score: int, ctx: dict) -> str:
    """HTML format для Telegram."""
    pair = sig.get('pair') or sig.get('symbol', '')
    direction = (sig.get('direction') or '').upper()
    dir_emoji = '🟢' if direction == 'LONG' else '🔴'
    entry = sig.get('entry')
    sl = sig.get('sl')
    tp = sig.get('tp1') or sig.get('tp')
    src = sig.get('source', '?')
    src_emoji = {
        'volcano': '🌋', 'volume_surge': '🌊', 'triple_confluence': '🐉',
        'second_flip': '♻️', 'vol_accum': '🔋',
        'cluster': '💠', 'confluence': '🎯', 'cryptovizor': '🚀',
        'tradium': '📡', 'paper': '🤖', 'verified': '✨', 'supertrend': '🌀',
    }.get(src, '•')
    label = score_label(score)

    lines = [
        f"🔥 <b>HOT SIGNAL · Score {score}/100 · {label}</b>",
        f"",
        f"<b>{pair}</b> · {dir_emoji} <b>{direction}</b> · {src_emoji}",
    ]

    # KL info
    kl = sig.get('_kl') or {}
    if kl.get('near_level'):
        kl_emoji = kl.get('emoji', '🎯')
        kl_label = kl.get('label', '')
        lines.append(f"{kl_emoji} {kl_label}")

    # ETH/BTC
    eth = ctx.get('eth_chg')
    btc = ctx.get('btc_chg')
    if eth is not None:
        lines.append(f"ETH 1h: {eth:+.2f}%")
    if btc is not None:
        lines.append(f"BTC 1h: {btc:+.2f}%")

    # Tier + extra
    if sig.get('st_tier'):
        lines.append(f"ST tier: <b>{sig['st_tier'].upper()}</b>")
    if ctx.get('strategy_count', 0) >= 2:
        lines.append(f"🔀 Confluence: <b>{ctx['strategy_count']}</b> стратегий")

    lines.append("")
    if entry:
        lines.append(f"Entry: <code>{entry:.6f}</code>" if isinstance(entry, (int, float)) else f"Entry: <code>{entry}</code>")
    if sl:
        try:
            sl_pct = (sl - entry) / entry * 100 if entry else 0
            lines.append(f"SL:    <code>{sl:.6f}</code> ({sl_pct:+.2f}%)")
        except Exception:
            lines.append(f"SL:    <code>{sl}</code>")
    if tp:
        try:
            tp_pct = (tp - entry) / entry * 100 if entry else 0
            lines.append(f"TP:    <code>{tp:.6f}</code> ({tp_pct:+.2f}%)")
        except Exception:
            lines.append(f"TP:    <code>{tp}</code>")

    return "\n".join(lines)


async def send_hot_alert(sig: dict, ctx: Optional[dict] = None,
                          force: bool = False) -> Optional[int]:
    """Если score >= 60 — отправляет alert в BOT15.
    Возвращает score или None если не отправили (low score / dedup / rate)."""
    ctx = ctx or {}
    try:
        score = compute_signal_score(sig, ctx)
    except Exception as e:
        logger.warning(f"[hot] score compute fail: {e}")
        return None
    if score < HOT_MIN_SCORE and not force:
        return None

    # Rate / dedup key
    pair = sig.get('pair') or sig.get('symbol', '')
    direction = (sig.get('direction') or '').upper()
    now_ts = time.time()
    hour_bucket = int(now_ts // 3600)
    key = f"{pair}|{direction}|{hour_bucket}"
    if not force and not _can_send(key, now_ts):
        logger.debug(f"[hot] dedup/ratelimit {key}")
        return None

    # Send
    try:
        from config import BOT15_BOT_TOKEN, ADMIN_CHAT_ID, HOT_SIGNALS_CHAT_ID
        chat_id = HOT_SIGNALS_CHAT_ID or ADMIN_CHAT_ID
    except Exception:
        try:
            from config import ADMIN_CHAT_ID
            BOT15_BOT_TOKEN = None
            chat_id = ADMIN_CHAT_ID
        except Exception:
            return None
    if not BOT15_BOT_TOKEN or not chat_id:
        logger.warning("[hot] BOT15_BOT_TOKEN or chat_id not configured")
        return None

    text = _format_alert(sig, score, ctx)
    try:
        import httpx
        url = f"https://api.telegram.org/bot{BOT15_BOT_TOKEN}/sendMessage"
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(url, json={
                'chat_id': chat_id,
                'text': text,
                'parse_mode': 'HTML',
                'disable_web_page_preview': True,
            })
            if r.status_code != 200:
                logger.warning(f"[hot] BOT15 send fail HTTP {r.status_code}: {r.text[:200]}")
                return None
        _mark_sent(key, now_ts)
        # Persist в Mongo для diagnostics
        try:
            from database import _get_db, utcnow
            _get_db().hot_alerts_sent.insert_one({
                'at': utcnow(),
                'pair': pair, 'direction': direction,
                'source': sig.get('source'),
                'score': score,
                'sig_data': {k: v for k, v in sig.items()
                              if k in ('source','symbol','pair','direction','entry','sl','tp1','st_tier','at')},
            })
        except Exception:
            pass
        logger.info(f"[hot] sent score={score} {pair} {direction}")
        return score
    except Exception as e:
        logger.warning(f"[hot] send exception: {e}")
        return None
