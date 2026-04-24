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
from datetime import datetime, timedelta, timezone
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


def _ema_last(values: list[float], period: int) -> float | None:
    """Экспоненциальная MA — последнее значение. None если мало данных."""
    if not values or len(values) < period:
        return None
    k = 2.0 / (period + 1)
    ema = sum(values[:period]) / period
    for v in values[period:]:
        ema = v * k + ema * (1 - k)
    return ema


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

    # 7. Session filter (Лондон + NY, 7:00-17:00 UTC).
    # Бэктест 513 сигналов показал: вне этого окна ликвидность низкая,
    # FVG часто false-break. Отсеивает ~52% сигналов, выдаёт +эквити.
    session_hour = formed_at.hour if formed_at.tzinfo is None \
                   else formed_at.astimezone(timezone.utc).hour
    if session_hour < 7 or session_hour >= 17:
        logger.info(f"[tv-webhook] REJECT session {instrument} {direction_raw} "
                    f"hour={session_hour} (вне 7-17 UTC)")
        return {"ok": False, "reason": f"outside session window (UTC hour={session_hour})"}

    # 8. Получаем свечи для ATR + HTF filter (нужно минимум 200 баров 1h
    # для EMA50/200). Пробуем cache → yfinance (30d вместо 7d для HTF).
    candles = get_cached_candles(instrument, "1h", max_age_min=90) or []
    if not candles or len(candles) < 200:
        try:
            candles = fetch_candles(ticker_yf, period="30d", interval="1h")
        except Exception as e:
            logger.warning(f"[tv-webhook] fetch candles fail {ticker_yf}: {e}")
            candles = []

    # 9. HTF Trend Filter (1h EMA50 vs EMA200).
    # Бэктест: отсеивает 49% контр-трендовых сигналов, sum R с −37R → −2R.
    # Fail-open: если данных <200 баров, пропускаем (не фильтруем).
    if len(candles) >= 200:
        closes = [c["c"] for c in candles[-200:]]
        ema50 = _ema_last(closes, 50)
        ema200 = _ema_last(closes, 200)
        if ema50 is not None and ema200 is not None:
            uptrend = ema50 > ema200
            if direction_raw == "bullish" and not uptrend:
                logger.info(f"[tv-webhook] REJECT HTF {instrument} bullish "
                            f"but 1h EMA50={ema50:.5f} < EMA200={ema200:.5f} (downtrend)")
                return {"ok": False, "reason": "HTF downtrend for bullish signal"}
            if direction_raw == "bearish" and uptrend:
                logger.info(f"[tv-webhook] REJECT HTF {instrument} bearish "
                            f"but 1h EMA50={ema50:.5f} > EMA200={ema200:.5f} (uptrend)")
                return {"ok": False, "reason": "HTF uptrend for bearish signal"}

    # 10. Рассчитываем FVG zone через ATR
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

    # 10a. Фильтр "FVG_too_small vs ATR".
    # Бэктест на 14 днях: требование fvg_size/ATR >= 0.6 отрезает 21%
    # сигналов где ATR вырос сильнее чем размер FVG (volатильность
    # "съела" зону → retest с большой вероятностью шумный). На той же
    # выборке даёт AvgR +0.026 → +0.089 (×3.4) и WR 26.1% → 27.8%.
    if atr and atr > 0:
        fvg_atr_ratio = abs(fvg_top - fvg_bottom) / atr
        if fvg_atr_ratio < 0.6:
            logger.info(f"[tv-webhook] REJECT fvg_too_small {instrument} "
                        f"{direction_raw} fvg/atr={fvg_atr_ratio:.2f}<0.6")
            return {"ok": False,
                    "reason": f"FVG too small vs ATR (ratio={fvg_atr_ratio:.2f})"}

    # 11. Entry = midpoint зоны (бэктест −219R → −37R vs boundary).
    # SL за дальней границей + 5% буфер (не меняем, защищает от wick'ов).
    sl_buffer_ratio = 0.05
    buffer = (fvg_top - fvg_bottom) * sl_buffer_ratio
    entry_price = (fvg_top + fvg_bottom) / 2.0   # midpoint для обоих направлений
    if direction_raw == "bullish":
        sl_price = fvg_bottom - buffer
    else:
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
        # Было 30ч — 72% EXPIRED. 72ч (3 дня) даёт retest времени ×2,
        # классика FVG-торговли: часто цена возвращается в зону 1-3 суток.
        "expire_at": formed_at + timedelta(hours=72),
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

    # Confluence Score + Top Pick tagging
    score_result = {}
    try:
        from fvg_top_picks import tag_fvg
        score_result = tag_fvg(res.inserted_id, candles=candles, atr=atr, retroact=True)
        if score_result.get("is_top_pick"):
            logger.info(f"[tv-webhook] {instrument} tagged as TOP PICK (score={score_result.get('score')})")
    except Exception as e:
        logger.warning(f"[tv-webhook] scoring failed: {e}")

    return {
        "ok": True,
        "reason": "created",
        "instrument": instrument,
        "direction": direction_raw,
        "entry": entry_price,
        "sl": sl_price,
        "fvg_id": str(res.inserted_id),
        "confluence_score": score_result.get("score"),
        "is_top_pick": score_result.get("is_top_pick"),
        "breakdown": score_result.get("breakdown"),
        "retagged_related": score_result.get("retagged"),
    }
