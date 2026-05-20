"""BOT13 — Pre-Pump Predictor alerts.

Шлёт алерты в Telegram только для PRIME tier (composite_score ≥ 75).
Rate limit 4h per pair. Quiet hours support.

Auto-trade DISABLED по умолчанию (variant B) — алерты только информационные
до подтверждения backtest'ом на live данных.
"""
from __future__ import annotations
import logging
import time
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)


def _in_quiet_hours() -> bool:
    """Returns True если сейчас МСК-час входит в PREPUMP_QUIET_HOURS range."""
    try:
        from config import PREPUMP_QUIET_HOURS
        if not PREPUMP_QUIET_HOURS or '-' not in PREPUMP_QUIET_HOURS:
            return False
        a, b = PREPUMP_QUIET_HOURS.split('-')
        a, b = int(a), int(b)
        # МСК = UTC+3
        msk_hour = (datetime.now(timezone.utc).hour + 3) % 24
        if a <= b:
            return a <= msk_hour < b
        else:  # wrap (e.g. 22-07)
            return msk_hour >= a or msk_hour < b
    except Exception:
        return False


def _format_score_bar(score: float, max_score: float, length: int = 5) -> str:
    """Returns ▓▓▓▓░ visual bar for score/max ratio."""
    pct = score / max_score if max_score > 0 else 0
    filled = round(pct * length)
    return '▓' * filled + '░' * (length - filled)


def _format_alert(doc: dict) -> str:
    """Rich-format alert message для PRIME candidate."""
    pair = doc.get('pair', '?')
    score = doc.get('composite_score', 0)
    direction = doc.get('direction', 'LONG')
    components = doc.get('components', {})
    vol = doc.get('volume_data', {})
    oi = doc.get('oi_data', {})
    fr = doc.get('funding_data', {})
    sectors = doc.get('sectors', [])
    sector_active = doc.get('sector_active', False)

    # Direction emoji
    dir_emoji = '🟢 LONG' if direction == 'LONG' else '🔴 SHORT'
    msg = f"🔥 <b>PRIME · {pair}</b> · {dir_emoji}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📊 <b>Composite Score: {score:.0f}/100</b>\n\n"

    # Components breakdown
    msg += "<b>Leading indicators:</b>\n"
    vol_pts = components.get('volume', 0)
    oi_pts = components.get('oi', 0)
    fr_pts = components.get('funding', 0)
    bb_pts = components.get('bb_squeeze', 0)
    pf_pts = components.get('price_flat', 0)
    sec_pts = components.get('sector', 0)
    rsi_pts = components.get('rsi_comp', 0)

    msg += f"  Volume:  {_format_score_bar(vol_pts, 25)} {vol_pts:.0f}/25"
    if vol:
        vs = vol.get('volume_score', 0)
        msg += f"  (×{vs:.1f} vs 7d avg)"
    msg += "\n"

    msg += f"  OI:      {_format_score_bar(oi_pts, 20)} {oi_pts:.0f}/20"
    if oi:
        oig = oi.get('oi_change_24h_pct', 0)
        msg += f"  ({oig:+.1f}% 24h)"
    msg += "\n"

    msg += f"  Funding: {_format_score_bar(fr_pts, 15)} {fr_pts:.0f}/15"
    if fr:
        fra = fr.get('funding_avg_24h_pct', 0)
        msg += f"  ({fra:+.3f}%)"
    msg += "\n"

    msg += f"  BB Squeeze: {_format_score_bar(bb_pts, 15)} {bb_pts:.0f}/15\n"
    msg += f"  Price flat: {_format_score_bar(pf_pts, 10)} {pf_pts:.0f}/10\n"
    msg += f"  Sector:  {_format_score_bar(sec_pts, 10)} {sec_pts:.0f}/10"
    if sector_active:
        msg += "  🔥 ROTATION ACTIVE"
    msg += "\n"
    msg += f"  RSI comp: {_format_score_bar(rsi_pts, 5)} {rsi_pts:.1f}/5\n"

    # Sectors tag
    if sectors:
        cats = ', '.join(sectors[:3])
        msg += f"\n🏷 Sectors: <i>{cats}</i>\n"

    # Price + volume context
    if vol:
        v24 = vol.get('volume_24h_usd', 0)
        v_pct = vol.get('price_24h_change_pct', 0)
        msg += f"\n💵 Vol 24h: <b>${v24/1e6:.1f}M</b>"
        msg += f"  · Price: {v_pct:+.2f}%\n"

    msg += "\n<i>Этот сетап — leading indicator. Сигнал на наблюдение,\n"
    msg += "не автоматическая команда. Решение за тобой.</i>"
    return msg


def send_prepump_alert(doc: dict) -> bool:
    """Отправляет alert для PRIME candidate через BOT13 в PREPUMP_CHAT_ID.
    Returns True если успешно отправлено.

    Rate limiting не нужно тут — scanner уже filter'ит via is_new_prime.
    Quiet hours — пропускаем но всё равно записываем в БД (через scanner).
    """
    try:
        from config import BOT13_BOT_TOKEN, PREPUMP_CHAT_ID
        if not BOT13_BOT_TOKEN or not PREPUMP_CHAT_ID:
            logger.debug("[bot13] not configured (BOT13_BOT_TOKEN or PREPUMP_CHAT_ID missing)")
            return False
        if _in_quiet_hours():
            logger.info(f"[bot13] quiet hours, skip alert for {doc.get('pair','?')}")
            return False

        import httpx
        msg = _format_alert(doc)
        r = httpx.post(
            f'https://api.telegram.org/bot{BOT13_BOT_TOKEN}/sendMessage',
            json={
                'chat_id': PREPUMP_CHAT_ID,
                'text': msg,
                'parse_mode': 'HTML',
                'disable_web_page_preview': True,
            },
            timeout=8.0,
        )
        if r.status_code == 200:
            logger.info(f"[bot13] PRIME alert sent for {doc.get('pair','?')}")
            return True
        else:
            logger.warning(f"[bot13] send fail status={r.status_code}: {r.text[:200]}")
            return False
    except Exception as e:
        logger.exception(f"[bot13] send_prepump_alert error: {e}")
        return False
