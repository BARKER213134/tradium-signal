"""Парсер сигналов формата Tradium Setups [TERMINAL]."""
import re

QUOTE_CURRENCIES = ["USDT", "USDC", "USD", "BUSD", "BTC", "ETH"]
_QUOTE_RE = "|".join(QUOTE_CURRENCIES)

NUM = r'(-?[0-9]+(?:[.,][0-9]+)?)'


def _num(s):
    if s is None:
        return None
    try:
        return float(str(s).replace(",", "."))
    except Exception:
        return None


def parse_signal(text: str) -> dict:
    """Парсит сообщение Tradium Setups."""
    result = {
        "pair": None, "direction": None,
        "entry": None, "sl": None,
        "tp1": None, "tp2": None, "tp3": None,
        "timeframe": None, "risk_reward": None,
        "risk_percent": None, "amount": None,
        "tp_percent": None, "sl_percent": None,
        "trend": None, "comment": None,
        "setup_number": None,
    }
    if not text:
        return result

    # ── Номер сетапа ─────────────────────────────────
    m = re.search(r'№\s*(\d+)', text)
    if m:
        result["setup_number"] = int(m.group(1))

    # ── Пара + TF + биржа ────────────────────────────
    # $KAITO 1h Binance #Futures
    m = re.search(
        r'\$?([A-Z0-9]{2,15})\s+(\d+[mhHdDwW]|1D|4H|1H|15m)\b',
        text,
    )
    if m:
        base = m.group(1).upper()
        tf = m.group(2).lower().replace("d", "D").replace("w", "W")
        result["pair"] = f"{base}/USDT"
        result["timeframe"] = tf

    # Fallback: ищем любую криптопару
    if not result["pair"]:
        m = re.search(r'\$?([A-Z0-9]{2,10})[\s/\-]?(' + _QUOTE_RE + r')\b', text.upper())
        if m:
            result["pair"] = f"{m.group(1)}/{m.group(2)}"

    # ── Направление: Long / Short ────────────────────
    up = text.upper()
    if re.search(r'\b(LONG|ЛОНГ|BUY)\b', up):
        result["direction"] = "LONG"
    elif re.search(r'\b(SHORT|ШОРТ|SELL)\b', up):
        result["direction"] = "SHORT"

    # ── Entry ────────────────────────────────────────
    m = re.search(r'Entry[:\s]+' + NUM, text, re.IGNORECASE)
    if m: result["entry"] = _num(m.group(1))

    # ── TP + процент ─────────────────────────────────
    m = re.search(r'TP[:\s]+' + NUM + r'(?:\s+([0-9]+(?:[.,][0-9]+)?)%)?', text, re.IGNORECASE)
    if m:
        result["tp1"] = _num(m.group(1))
        result["tp_percent"] = _num(m.group(2))

    # ── SL + процент ─────────────────────────────────
    m = re.search(r'SL[:\s]+' + NUM + r'(?:\s+([0-9]+(?:[.,][0-9]+)?)%)?', text, re.IGNORECASE)
    if m:
        result["sl"] = _num(m.group(1))
        result["sl_percent"] = _num(m.group(2))

    # ── Risk-reward ──────────────────────────────────
    m = re.search(r'Risk[\s\-]?reward[:\s]+' + NUM, text, re.IGNORECASE)
    if m: result["risk_reward"] = _num(m.group(1))

    # ── Risk percent ─────────────────────────────────
    m = re.search(r'Risk[:\s]+([0-9]+(?:[.,][0-9]+)?)%', text, re.IGNORECASE)
    if m: result["risk_percent"] = _num(m.group(1))

    # ── Amount ───────────────────────────────────────
    m = re.search(r'Amount[:\s]+' + NUM, text, re.IGNORECASE)
    if m: result["amount"] = _num(m.group(1))

    # ── Тренд (5 кружков) ────────────────────────────
    # 🔴🟢🟢🟢🟢 или RRGGG
    circles = re.findall(r'[🔴🟢]', text)
    if len(circles) >= 5:
        trend_str = "".join("R" if c == "🔴" else "G" for c in circles[:5])
        result["trend"] = trend_str

    # ── Comment ──────────────────────────────────────
    m = re.search(r'Comment[:\s]+(.+?)(?:\n\n|\nWaiting|\n\|\s*Key|$)', text, re.IGNORECASE | re.DOTALL)
    if m:
        result["comment"] = m.group(1).strip()

    return result


def format_signal_message(signal_data: dict, raw_text: str) -> str:
    pair = signal_data.get("pair") or "—"
    direction = signal_data.get("direction") or "—"
    entry = signal_data.get("entry") or "—"
    sl = signal_data.get("sl") or "—"
    tp1 = signal_data.get("tp1") or "—"
    rr = signal_data.get("risk_reward") or "—"
    tf = signal_data.get("timeframe") or "—"

    emoji = "🟢" if direction == "LONG" else "🔴" if direction == "SHORT" else "⚪"

    return (
        f"{emoji} <b>НОВЫЙ СИГНАЛ</b>\n\n"
        f"📊 <b>Пара:</b> {pair} ({tf})\n"
        f"📈 <b>Направление:</b> {direction}\n"
        f"🎯 <b>Entry:</b> {entry}\n"
        f"🛑 <b>SL:</b> {sl}\n"
        f"✅ <b>TP:</b> {tp1}\n"
        f"⚖ <b>R:R:</b> {rr}\n"
    )
