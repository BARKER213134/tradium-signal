"""TradingView Webhook receiver for FVG signals.

Заменяет собственную детекцию FVG (scan_all в fvg_scanner) на приём
alert'ов от TradingView. TV шлёт POST на /api/tv-webhook с JSON:

    {
      "secret": "tv_f9c3a8b2d4e7f6a1b8c5d9e2f3a7b4c6",
      "ticker": "USDCHF",
      "tf": "60",
      "direction": "bullish",
      "price": 0.8874,
      "time": "2026-04-16T10:00:01Z"
    }

Создаёт запись в fvg_signals со status=WAITING_RETEST. Monitor_signals
дальше сам отслеживает retest → ENTERED → TP/SL.

FVG zone (top/bottom) эмулируется через ATR (LuxAlgo Free не даёт
точных уровней), но это всё равно работает — просто немного менее
точно чем настоящий LuxAlgo Premium.
"""
from __future__ import annotations
import logging
from datetime import datetime, timedelta
from typing import Optional

from database import _fvg_signals, utcnow
from fvg_scanner import INSTRUMENTS, fetch_candles, get_cached_candles

logger = logging.getLogger(__name__)


# Mapping TV ticker → наш INSTRUMENTS key
# TV обычно присылает без слеша: "USDCHF", "GBPJPY", "XAUUSD"
# Наш INSTRUMENTS key тоже без слеша — совпадает напрямую для форекса.
# Но для индексов и нефти возможны расхождения:
TV_TICKER_ALIASES = {
    "SPX": "SPX500",
    "SPX500USD": "SPX500",
    "US500": "SPX500",
    "NDX": "NAS100",
    "US100": "NAS100",
    "NAS100USD": "NAS100",
    "DJI": "US30",
    "US30USD": "US30",
    "DAX": "GER40",
    "DE40": "GER40",
    "FTSE": "UK100",
    "UK100USD": "UK100",
    "NIKKEI": "JPN225",
    "JP225": "JPN225",
    "WTI": "USOIL",
    "WTIUSD": "USOIL",
    "USOILUSD": "USOIL",
    "BRENT": "UKOIL",
    "UKOILUSD": "UKOIL",
    "GOLD": "XAUUSD",
    "XAU": "XAUUSD",
    "SILVER": "XAGUSD",
    "XAG": "XAGUSD",
}


def normalize_ticker(tv_ticker: str) -> Optional[str]:
    """TV ticker → наш INSTRUMENTS key. Возвращает None если неизвестен."""
    if not tv_ticker:
        return None
    t = tv_ticker.upper().replace("/", "").replace(":", "")
    # Убираем биржевой префикс если TV шлёт типа "FX:EURUSD" или "OANDA:EURUSD"
    if ":" in tv_ticker:
        t = tv_ticker.split(":", 1)[1].upper().replace("/", "")
    # Прямое совпадение?
    if t in INSTRUMENTS:
        return t
    # Через alias
    if t in TV_TICKER_ALIASES:
        return TV_TICKER_ALIASES[t]
    # Убрать USD суффикс (часто для metals/indices: XAUUSD → XAU → XAUUSD in our keys)
    if t.endswith("USD") and t[:-3] in TV_TICKER_ALIASES:
        return TV_TICKER_ALIASES[t[:-3]]
    return None


def _compute_atr(candles: list[dict], period: int = 14) -> Optional[float]:
    """Average True Range по последним period барам."""
    if not candles or len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        c = candles[i]
        p = candles[i - 1]
        h, l, pc = c["h"], c["l"], p["c"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


def _fvg_zone_from_atr(price: float, atr: float, direction: str,
                        zone_size_ratio: float = 0.5) -> tuple[float, float]:
    """Эмуляция FVG зоны через ATR. Возвращает (top, bottom)."""
    half_zone = atr * zone_size_ratio
    if direction == "bullish":
        top = price
        bottom = price - half_zone
    else:
        top = price + half_zone
        bottom = price
    return top, bottom


def process_tv_webhook(payload: dict) -> dict:
    """Обработка webhook'а от TradingView.
    Returns: {"ok": bool, "fvg_id": int, "reason": str} — для API ответа."""
    # 1. Парсинг
    tv_ticker = (payload.get("ticker") or "").strip()
    direction_raw = (payload.get("direction") or "").lower().strip()
    price_raw = payload.get("price")
    tf_raw = str(payload.get("tf") or "60")
    time_str = payload.get("time") or ""

    # 2. Валидация
    if direction_raw not in ("bullish", "bearish"):
        return {"ok": False, "reason": f"invalid direction: {direction_raw}"}
    if not isinstance(price_raw, (int, float)) or price_raw <= 0:
        return {"ok": False, "reason": f"invalid price: {price_raw}"}

    # 3. Маппинг ticker
    instrument = normalize_ticker(tv_ticker)
    if instrument is None:
        return {"ok": False, "reason": f"unknown instrument: {tv_ticker}"}
    ticker_yf, asset_class, td_symbol = INSTRUMENTS[instrument][0], INSTRUMENTS[instrument][1], (INSTRUMENTS[instrument][2] if len(INSTRUMENTS[instrument]) >= 3 else None)

    # 4. TF нормализация: TV может слать "60" / "1h" / "1H"
    tf_map = {"1": "1m", "5": "5m", "15": "15m", "30": "30m",
              "60": "1h", "240": "4h", "D": "1d", "1D": "1d", "W": "1w"}
    tf = tf_map.get(tf_raw, tf_raw.lower())

    # 5. Формируем время сигнала
    try:
        if time_str:
            formed_at = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            formed_at = formed_at.replace(tzinfo=None) if formed_at.tzinfo else formed_at
        else:
            formed_at = utcnow()
    except Exception:
        formed_at = utcnow()
    formed_ts = int(formed_at.replace(tzinfo=None).timestamp()) if formed_at.tzinfo is None else int(formed_at.timestamp())

    # 6. Дедупликация — тот же instrument+direction+time уже был?
    existing = _fvg_signals().find_one({
        "instrument": instrument,
        "direction": direction_raw,
        "formed_ts": formed_ts,
        "source": "tv_webhook",
    })
    if existing:
        return {"ok": True, "reason": "already exists", "fvg_id": existing.get("_id")}

    # 7. Получаем свечи для ATR — пробуем кеш → yfinance
    candles = get_cached_candles(instrument, "1h", max_age_min=90) or []
    if not candles:
        try:
            candles = fetch_candles(ticker_yf, period="7d", interval="1h")
        except Exception as e:
            logger.warning(f"[tv-webhook] fetch candles fail {ticker_yf}: {e}")
            candles = []

    # 8. Рассчитываем FVG zone
    price = float(price_raw)
    atr = _compute_atr(candles) if candles else None
    if atr:
        fvg_top, fvg_bottom = _fvg_zone_from_atr(price, atr, direction_raw, zone_size_ratio=0.5)
        fvg_size_rel = (fvg_top - fvg_bottom) / price
    else:
        # Fallback — фиксированный 0.15% как в конфиге
        half = price * 0.00075
        if direction_raw == "bullish":
            fvg_top, fvg_bottom = price, price - half
        else:
            fvg_top, fvg_bottom = price + half, price
        fvg_size_rel = (fvg_top - fvg_bottom) / price

    # 9. Entry / SL / TP (как в оригинальной scan_one_instrument)
    sl_buffer_ratio = 0.05
    buffer = (fvg_top - fvg_bottom) * sl_buffer_ratio
    if direction_raw == "bullish":
        entry_price = fvg_top       # входим на retest верхней границы
        sl_price = fvg_bottom - buffer
    else:
        entry_price = fvg_bottom
        sl_price = fvg_top + buffer
    risk_rel = abs(entry_price - sl_price) / entry_price if entry_price > 0 else 0

    # 10. Insert в fvg_signals — совместимо с существующей схемой
    doc = {
        "instrument": instrument,
        "ticker": ticker_yf,
        "asset_class": asset_class,
        "timeframe": tf.upper(),
        "direction": direction_raw,
        "source": "tv_webhook",  # новое поле — отличает от scan_all
        "fvg_top": fvg_top,
        "fvg_bottom": fvg_bottom,
        "fvg_size_rel": fvg_size_rel,
        "impulse_body_ratio": None,   # TV не даёт
        "formed_at": formed_at,
        "formed_ts": formed_ts,
        "formed_price": price,
        "expire_at": formed_at + timedelta(hours=30),
        "status": "WAITING_RETEST",
        "entry_price": entry_price,
        "sl_price": sl_price,
        "risk_rel": risk_rel,
        "entered_at": None,
        "entered_price": None,
        "trailing_active": False,
        "trailing_sl": None,
        "peak_R": 0.0,
        "closed_at": None,
        "exit_price": None,
        "outcome_R": None,
        "created_at": utcnow(),
        "updated_at": utcnow(),
    }
    res = _fvg_signals().insert_one(doc)
    logger.info(f"[tv-webhook] NEW {instrument} {direction_raw} @ {price} (ATR zone: {fvg_bottom:.5f}-{fvg_top:.5f})")
    return {
        "ok": True,
        "reason": "created",
        "instrument": instrument,
        "direction": direction_raw,
        "entry": entry_price,
        "sl": sl_price,
        "fvg_id": str(res.inserted_id),
    }
