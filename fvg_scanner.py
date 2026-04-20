"""Forex FVG Scanner — Hybrid v2 стратегия в live mode.

Источник: Yahoo Finance (yfinance) — 1H свечи.
Стратегия: Conservative entry на ретесте + Trailing stop после +1R.
Фильтры (Hybrid v2):
  - body ratio >= 0.5
  - FVG size >= 0.03% (forex), 0.05% (metals/energy), 0.1% (crypto)
  - Сессия London/NY (08-21 UTC)

Статусы FVG в БД:
  FORMED         — найден, ждём ретеста
  WAITING_RETEST — синоним FORMED
  ENTERED        — retest сработал, сделка открыта
  TP             — закрыт в плюс (trailing или fixed)
  SL             — закрыт в минус
  EXPIRED        — ретест не случился за max_wait_bars
  TIMEOUT        — сделка не закрылась за max_hold_bars
"""
from __future__ import annotations
import logging
import warnings
from datetime import datetime, timezone, timedelta
from typing import Optional

warnings.filterwarnings("ignore")

from database import _fvg_signals, _fvg_config, utcnow
from fvg_detector import detect_fvg, FVG
from fvg_hybrid_v2 import _is_session_active
from pymongo import DESCENDING, ASCENDING

logger = logging.getLogger(__name__)


# ── Инструменты (32) ────────────────────────────────────────
# tuple = (yfinance_ticker, asset_class, twelvedata_symbol_or_None)
# Форекс-символы в TwelveData формате "EUR/USD". Metals/Indices/Energy — остаются
# на yfinance (там данные хорошие и не стоит жечь TD квоту).
INSTRUMENTS = {
    # Forex Majors (7)
    "EURUSD":  ("EURUSD=X", "forex", "EUR/USD"),
    "GBPUSD":  ("GBPUSD=X", "forex", "GBP/USD"),
    "USDJPY":  ("USDJPY=X", "forex", "USD/JPY"),
    "USDCHF":  ("USDCHF=X", "forex", "USD/CHF"),
    "AUDUSD":  ("AUDUSD=X", "forex", "AUD/USD"),
    "NZDUSD":  ("NZDUSD=X", "forex", "NZD/USD"),
    "USDCAD":  ("USDCAD=X", "forex", "USD/CAD"),
    # Crosses (13)
    "EURGBP":  ("EURGBP=X", "forex", "EUR/GBP"),
    "EURJPY":  ("EURJPY=X", "forex", "EUR/JPY"),
    "EURCHF":  ("EURCHF=X", "forex", "EUR/CHF"),
    "EURAUD":  ("EURAUD=X", "forex", "EUR/AUD"),
    "EURCAD":  ("EURCAD=X", "forex", "EUR/CAD"),
    "GBPJPY":  ("GBPJPY=X", "forex", "GBP/JPY"),
    "GBPAUD":  ("GBPAUD=X", "forex", "GBP/AUD"),
    "GBPCAD":  ("GBPCAD=X", "forex", "GBP/CAD"),
    "AUDJPY":  ("AUDJPY=X", "forex", "AUD/JPY"),
    "AUDCAD":  ("AUDCAD=X", "forex", "AUD/CAD"),
    "AUDNZD":  ("AUDNZD=X", "forex", "AUD/NZD"),
    "CADJPY":  ("CADJPY=X", "forex", "CAD/JPY"),
    "CHFJPY":  ("CHFJPY=X", "forex", "CHF/JPY"),
    # Exotics (2)
    "USDMXN":  ("USDMXN=X", "forex", "USD/MXN"),
    "USDNOK":  ("USDNOK=X", "forex", "USD/NOK"),
    # Metals (2) — через yfinance (данные с NYMEX/COMEX хорошие)
    "XAUUSD":  ("GC=F", "metal", None),
    "XAGUSD":  ("SI=F", "metal", None),
    # Indices (6) — через yfinance
    "SPX500":  ("^GSPC", "index", None),
    "NAS100":  ("^IXIC", "index", None),
    "US30":    ("^DJI",  "index", None),
    "GER40":   ("^GDAXI", "index", None),
    "UK100":   ("^FTSE", "index", None),
    "JPN225":  ("^N225", "index", None),
    # Energy (2)
    "USOIL":   ("CL=F",  "energy", None),
    "UKOIL":   ("BZ=F",  "energy", None),
}


# ── Конфиг ──────────────────────────────────────────────────
DEFAULT_CONFIG = {
    "enabled_instruments": list(INSTRUMENTS.keys()),
    "timeframe": "1H",
    # 60 мин — оптимум под TwelveData free (8 credits/min, 800/day).
    # 22 forex × 24 часа = 528 credits/day. Остаётся запас 272 на UI клики.
    "scan_interval_min": 60,
    # Тюнинг: подняли чувствительность до уровня TradingView-индикатора FVG.
    #   body 0.5 → 0.4 (ловим моменты с менее трендовыми импульсами)
    #   size forex/index 0.03% → 0.015% (соответствует текущей волатильности)
    "min_body_ratio": 0.4,
    "min_size_rel_forex": 0.00015,
    "min_size_rel_metal": 0.0005,
    "min_size_rel_energy": 0.0005,
    "min_size_rel_index": 0.00015,
    "session_filter": "london_ny",     # london_ny | london | ny | any
    "max_wait_bars": 30,
    "max_hold_bars": 50,
    "sl_buffer_rel": 0.05,             # 5% от размера FVG
    "trailing_activate_at": 1.0,       # +1R
    "trailing_distance_R": 1.0,        # трейл 1R сзади
}


def get_config() -> dict:
    try:
        doc = _fvg_config().find_one({"_id": "fvg_config"})
        if not doc:
            return DEFAULT_CONFIG.copy()
        out = DEFAULT_CONFIG.copy()
        for k in DEFAULT_CONFIG:
            if k in doc:
                out[k] = doc[k]
        return out
    except Exception as e:
        logger.warning(f"fvg_config load: {e}")
        return DEFAULT_CONFIG.copy()


def save_config(cfg: dict) -> dict:
    safe = {}
    for k, v in cfg.items():
        if k not in DEFAULT_CONFIG:
            continue
        try:
            if k in ("min_body_ratio", "sl_buffer_rel", "trailing_activate_at",
                     "trailing_distance_R", "min_size_rel_forex", "min_size_rel_metal",
                     "min_size_rel_energy", "min_size_rel_index"):
                safe[k] = float(v)
            elif k in ("max_wait_bars", "max_hold_bars", "scan_interval_min"):
                safe[k] = int(v)
            elif k == "enabled_instruments":
                if isinstance(v, list):
                    safe[k] = [str(x) for x in v if x in INSTRUMENTS]
            else:
                safe[k] = str(v)
        except (TypeError, ValueError):
            continue
    if not safe:
        return get_config()
    _fvg_config().update_one({"_id": "fvg_config"}, {"$set": safe}, upsert=True)
    return get_config()


# ── TwelveData fetcher (для форекса) ────────────────────────
def _td_interval(tf: str) -> str:
    """Приводит нашу TF нотацию к формату TwelveData."""
    return {"15m":"15min","30m":"30min","1h":"1h","4h":"4h","1d":"1day"}.get(tf, tf)


def _td_log_usage(credits: int, op: str, symbols: list = None, success: bool = True) -> None:
    """Логирует трату credits в td_quota collection (для мониторинга квоты).
    Собирается только успешные запросы (429 = 0 credits)."""
    if credits <= 0:
        return
    try:
        from database import _td_quota
        _td_quota().insert_one({
            "at": utcnow(),
            "credits": credits,
            "op": op,
            "symbols": (symbols or [])[:20],
            "success": success,
        })
    except Exception:
        pass


def get_td_quota_stats() -> dict:
    """Статистика по квоте TwelveData: расход за день, скорость, прогноз."""
    from database import _td_quota
    from config import TWELVEDATA_API_KEY
    now = utcnow()
    day_start = datetime(now.year, now.month, now.day, tzinfo=None)
    hour_ago = now - timedelta(hours=1)
    min5_ago = now - timedelta(minutes=5)

    # Сколько credits за день (UTC)
    day_docs = list(_td_quota().find({"at": {"$gte": day_start}}, {"credits": 1, "op": 1, "at": 1}))
    day_credits = sum(d.get("credits", 0) for d in day_docs)
    # За час
    hour_credits = sum(d.get("credits", 0) for d in day_docs if d.get("at") and d["at"] >= hour_ago)
    # За 5 минут (rate check)
    min5_credits = sum(d.get("credits", 0) for d in day_docs if d.get("at") and d["at"] >= min5_ago)

    # Последний запрос
    last_doc = _td_quota().find_one({}, sort=[("at", -1)])
    last_at = last_doc["at"].isoformat() if last_doc and hasattr(last_doc.get("at"), "isoformat") else None

    # Разбивка по операциям
    by_op = {}
    for d in day_docs:
        op = d.get("op", "?")
        by_op[op] = by_op.get(op, 0) + d.get("credits", 0)

    # Статус
    daily_limit = 800
    min_limit = 8
    remaining = max(0, daily_limit - day_credits)
    pct = round(day_credits / daily_limit * 100, 1)
    if pct >= 95:
        status = "critical"
    elif pct >= 75:
        status = "warning"
    else:
        status = "ok"

    # Прогноз — экстраполяция по текущей скорости
    # Количество минут с начала суток
    minutes_elapsed = max(1, int((now - day_start).total_seconds() / 60))
    minutes_remaining = max(0, 1440 - minutes_elapsed)
    rate_per_min = day_credits / minutes_elapsed
    projected_eod = day_credits + rate_per_min * minutes_remaining

    return {
        "enabled": bool(TWELVEDATA_API_KEY),
        "day_credits": day_credits,
        "hour_credits": hour_credits,
        "min5_credits": min5_credits,
        "daily_limit": daily_limit,
        "min_limit": min_limit,
        "remaining": remaining,
        "pct_used": pct,
        "status": status,  # ok | warning | critical
        "projected_eod": round(projected_eod),
        "rate_per_min": round(rate_per_min, 2),
        "min_rate_pct": round(min5_credits / min_limit * 20, 1),  # % от minute limit за 5м * 20
        "by_op": by_op,
        "last_call_at": last_at,
    }


def cleanup_td_quota_old(days: int = 2) -> int:
    """Чистит старые записи td_quota (keep 2 days)."""
    try:
        from database import _td_quota
        cutoff = utcnow() - timedelta(days=days)
        r = _td_quota().delete_many({"at": {"$lt": cutoff}})
        return r.deleted_count
    except Exception:
        return 0


def fetch_candles_twelvedata(td_symbol: str, interval: str = "1h", outputsize: int = 200) -> list[dict]:
    """Один инструмент через TwelveData. Возвращает candles в нашем формате."""
    from config import TWELVEDATA_API_KEY
    if not TWELVEDATA_API_KEY:
        return []
    import httpx
    try:
        url = "https://api.twelvedata.com/time_series"
        params = {
            "symbol": td_symbol,
            "interval": _td_interval(interval),
            "outputsize": outputsize,
            "apikey": TWELVEDATA_API_KEY,
            "timezone": "UTC",
        }
        r = httpx.get(url, params=params, timeout=15)
        d = r.json()
        if d.get("status") == "error":
            # 429 не считаем в квоте (credits не потрачены)
            code = d.get("code")
            logger.warning(f"[TD] {td_symbol} {interval}: {d.get('message','error')} (code={code})")
            if code != 429:
                _td_log_usage(1, "single_err", [td_symbol], success=False)
            return []
        if not d.get("values"):
            return []
        _td_log_usage(1, "single", [td_symbol])
        out = []
        from datetime import datetime as _dt
        for v in reversed(d["values"]):
            try:
                ts = int(_dt.fromisoformat(v["datetime"].replace(" ","T")).replace(tzinfo=timezone.utc).timestamp())
                out.append({
                    "t": ts,
                    "o": float(v["open"]),
                    "h": float(v["high"]),
                    "l": float(v["low"]),
                    "c": float(v["close"]),
                    "v": float(v.get("volume", 0) or 0),
                })
            except Exception:
                continue
        return out
    except Exception as e:
        logger.warning(f"[TD] {td_symbol} failed: {e}")
        return []


def fetch_candles_twelvedata_batch(td_symbols: list, interval: str = "1h", outputsize: int = 200,
                                    chunk_size: int = 7, throttle_s: int = 65) -> dict:
    """Батч через TwelveData с учётом free tier rate limit: 8 credits/min.
    Каждый символ в батче = 1 credit. Используем chunk=7 (запас до лимита),
    между чанками ждём throttle_s секунд чтобы не словить 429.

    Возвращает {td_symbol: [candles...]}.
    """
    from config import TWELVEDATA_API_KEY
    if not TWELVEDATA_API_KEY or not td_symbols:
        return {}
    import httpx, time
    from datetime import datetime as _dt
    out = {}
    for i in range(0, len(td_symbols), chunk_size):
        chunk = td_symbols[i:i+chunk_size]
        # Пауза перед не-первым чанком (не перед первым чтоб не тормозить)
        if i > 0:
            time.sleep(throttle_s)
        try:
            r = httpx.get("https://api.twelvedata.com/time_series", params={
                "symbol": ",".join(chunk),
                "interval": _td_interval(interval),
                "outputsize": outputsize,
                "apikey": TWELVEDATA_API_KEY,
                "timezone": "UTC",
            }, timeout=20)
            d = r.json()
            # Глобальный rate-limit ответ
            if d.get("status") == "error" and d.get("code") == 429:
                logger.warning(f"[TD batch] 429 on chunk {chunk[:3]}... skipping rest (quota exhausted)")
                for sym in chunk:
                    out[sym] = []
                break
            # Учёт credits: по 1 за каждый валидный символ в ответе
            if len(chunk) == 1 and "values" in d:
                d = {chunk[0]: d}
            successful_syms = [s for s in chunk
                               if isinstance(d.get(s), dict)
                               and d[s].get("status") != "error"
                               and d[s].get("values")]
            if successful_syms:
                _td_log_usage(len(successful_syms), "batch", successful_syms)
            for sym in chunk:
                sym_data = d.get(sym) or {}
                if sym_data.get("status") == "error" or not sym_data.get("values"):
                    out[sym] = []
                    continue
                parsed = []
                for v in reversed(sym_data["values"]):
                    try:
                        ts = int(_dt.fromisoformat(v["datetime"].replace(" ","T")).replace(tzinfo=timezone.utc).timestamp())
                        parsed.append({
                            "t": ts,
                            "o": float(v["open"]),
                            "h": float(v["high"]),
                            "l": float(v["low"]),
                            "c": float(v["close"]),
                            "v": float(v.get("volume", 0) or 0),
                        })
                    except Exception:
                        continue
                out[sym] = parsed
        except Exception as e:
            logger.warning(f"[TD batch] chunk {chunk[:3]}... failed: {e}")
            for sym in chunk:
                out[sym] = []
    return out


# ── yfinance wrapper ────────────────────────────────────────
def fetch_candles(ticker: str, period: str = "7d", interval: str = "1h", retries: int = 2) -> list[dict]:
    """Скачивает свечи через yfinance с retry. Fallback — Ticker.history()."""
    import yfinance as yf

    def _parse_df(df):
        candles = []
        for ts, row in df.iterrows():
            try:
                def v(k):
                    x = row[k]
                    return float(x.iloc[0] if hasattr(x, 'iloc') else x)
                o, h, l, c = v("Open"), v("High"), v("Low"), v("Close")
                if not (o > 0 and h > 0 and l > 0 and c > 0):
                    continue
                ts_val = ts
                unix = int(ts_val.timestamp()) if hasattr(ts_val, "timestamp") else 0
                candles.append({"t": unix, "o": o, "h": h, "l": l, "c": c, "v": 0})
            except Exception:
                continue
        return candles

    last_err = None
    # Attempt 1-2: yf.download
    for attempt in range(retries):
        try:
            df = yf.download(ticker, period=period, interval=interval,
                             progress=False, auto_adjust=True, threads=False)
            if df is not None and not df.empty:
                out = _parse_df(df)
                if out:
                    return out
                last_err = "df.empty after parse"
            else:
                last_err = "df is empty/None"
        except Exception as e:
            last_err = f"download: {e}"
    # Attempt 3: Ticker.history (альтернативный путь yfinance — иногда работает когда download падает)
    try:
        tk = yf.Ticker(ticker)
        df = tk.history(period=period, interval=interval, auto_adjust=True)
        if df is not None and not df.empty:
            out = _parse_df(df)
            if out:
                return out
            last_err = "Ticker.history: empty after parse"
        else:
            last_err = "Ticker.history: empty"
    except Exception as e:
        last_err = f"Ticker.history: {e}"

    logger.warning(f"[fvg.fetch_candles] {ticker} {interval} {period} FAILED: {last_err}")
    return []


def cache_candles(instrument: str, tf: str, candles: list[dict]) -> None:
    """Сохраняет последние N свечей в БД для fallback'а когда yfinance недоступен."""
    try:
        from database import _get_db
        _get_db().fvg_candle_cache.update_one(
            {"instrument": instrument, "tf": tf},
            {"$set": {
                "instrument": instrument, "tf": tf,
                "candles": candles[-200:],  # последние 200
                "cached_at": utcnow(),
            }},
            upsert=True,
        )
    except Exception:
        pass


def get_cached_candles(instrument: str, tf: str, max_age_min: int = 60) -> list[dict]:
    """Возвращает кеш если он свежий (< max_age_min минут)."""
    try:
        from database import _get_db
        doc = _get_db().fvg_candle_cache.find_one({"instrument": instrument, "tf": tf})
        if not doc or not doc.get("candles"):
            return []
        cached_at = doc.get("cached_at")
        if cached_at and hasattr(cached_at, "timestamp"):
            age_min = (utcnow() - cached_at).total_seconds() / 60
            if age_min > max_age_min:
                return []
        return doc["candles"]
    except Exception:
        return []


def _min_size_for_class(asset_class: str, cfg: dict) -> float:
    return {
        "forex": cfg["min_size_rel_forex"],
        "metal": cfg["min_size_rel_metal"],
        "energy": cfg["min_size_rel_energy"],
        "index": cfg["min_size_rel_index"],
    }.get(asset_class, 0.0003)


def _passes_hybrid_v2(fvg: FVG, asset_class: str, cfg: dict) -> bool:
    """Проверяет фильтры Hybrid v2. Возвращает True если FVG прошёл."""
    if fvg.impulse_body_ratio < cfg["min_body_ratio"]:
        return False
    if fvg.size_rel < _min_size_for_class(asset_class, cfg):
        return False
    # Сессия (только для intraday, 1D имеет hour=0)
    hour = datetime.fromtimestamp(fvg.time, tz=timezone.utc).hour if fvg.time else 12
    session = cfg.get("session_filter", "london_ny")
    if session != "any" and hour != 0:
        if not _is_session_active(fvg.time, session):
            return False
    return True


# ── Scan: основная функция ──────────────────────────────────
def _process_candles_for_instrument(name: str, ticker: str, asset_class: str, candles: list, cfg: dict) -> int:
    """Общая логика: детект FVG + запись в БД. Выделено чтобы scan_one и batch могли переиспользовать."""
    if len(candles) < 20:
        return 0
    cache_candles(name, "1h", candles)
    fvgs = detect_fvg(candles, min_size_rel=0.0001)
    if not fvgs:
        return 0

    created = 0
    col = _fvg_signals()

    for fvg in fvgs:
        # Только FVG последних 48 часов (новые)
        age_h = (utcnow().timestamp() - fvg.time) / 3600 if fvg.time else 9999
        if age_h > 48:
            continue

        # Hybrid v2 фильтры
        if not _passes_hybrid_v2(fvg, asset_class, cfg):
            continue

        # Дедупликация — такой же FVG уже есть в БД?
        existing = col.find_one({
            "instrument": name,
            "formed_ts": fvg.time,
            "direction": fvg.direction,
        })
        if existing:
            continue

        # SL
        buffer = (fvg.top - fvg.bottom) * cfg["sl_buffer_rel"]
        if fvg.direction == "bullish":
            entry_price = fvg.top  # лимит на top FVG
            sl_price = fvg.bottom - buffer
        else:
            entry_price = fvg.bottom
            sl_price = fvg.top + buffer

        risk_rel = abs(entry_price - sl_price) / entry_price if entry_price > 0 else 0
        formed_at = datetime.fromtimestamp(fvg.time, tz=timezone.utc).replace(tzinfo=None) if fvg.time else utcnow()

        doc = {
            "instrument": name,
            "ticker": ticker,
            "asset_class": asset_class,
            "timeframe": cfg["timeframe"],
            "direction": fvg.direction,
            "fvg_top": fvg.top,
            "fvg_bottom": fvg.bottom,
            "fvg_size_rel": fvg.size_rel,
            "impulse_body_ratio": fvg.impulse_body_ratio,
            "formed_at": formed_at,
            "formed_ts": fvg.time,
            "formed_price": fvg.close3,
            "expire_at": formed_at + timedelta(hours=cfg["max_wait_bars"]),
            "status": "WAITING_RETEST",
            "entry_price": entry_price,
            "sl_price": sl_price,
            "risk_rel": risk_rel,
            # Фактические значения когда ретест сработает
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
        col.insert_one(doc)
        created += 1
        logger.info(f"[FVG] NEW {name} {fvg.direction} zone {fvg.bottom:.5f}-{fvg.top:.5f} size {fvg.size_rel*100:.3f}%")
    if created:
        try:
            from cache_utils import fvg_signals_cache, fvg_journal_cache
            fvg_signals_cache.invalidate()
            fvg_journal_cache.invalidate()
        except Exception:
            pass
    return created


def scan_one_instrument(name: str, ticker: str, asset_class: str, cfg: dict, td_symbol=None) -> int:
    """Сканирует один инструмент.
    Если td_symbol задан (форекс) — пробуем TwelveData → fallback yfinance.
    Иначе (metal/index/energy) — сразу yfinance.
    """
    candles = []
    if td_symbol:
        candles = fetch_candles_twelvedata(td_symbol, interval="1h", outputsize=200)
        if not candles:
            logger.info(f"[FVG] {name}: TD empty → yfinance fallback")
    if not candles:
        candles = fetch_candles(ticker, period="7d", interval="1h")
    return _process_candles_for_instrument(name, ticker, asset_class, candles, cfg)


def scan_all() -> dict:
    """Сканирует все enabled инструменты.
    Smart Hybrid: форекс через TwelveData batch (меньше запросов), остальное yfinance.
    Возвращает статистику.
    """
    cfg = get_config()
    enabled = cfg.get("enabled_instruments", list(INSTRUMENTS.keys()))
    stats = {"total_instruments": 0, "new_fvgs": 0, "errors": 0, "td_used": 0, "yf_used": 0}

    # Разделяем: форекс → TD batch; остальное → yfinance sequential
    forex_names = []
    other_names = []
    for name in enabled:
        if name not in INSTRUMENTS:
            continue
        entry = INSTRUMENTS[name]
        # INSTRUMENTS теперь (ticker, asset_class, td_symbol)
        if len(entry) >= 3 and entry[2]:
            forex_names.append(name)
        else:
            other_names.append(name)

    # 1. Форекс через TwelveData batch
    from config import TWELVEDATA_API_KEY
    if forex_names and TWELVEDATA_API_KEY:
        td_symbols = [INSTRUMENTS[n][2] for n in forex_names]
        name_by_td = {INSTRUMENTS[n][2]: n for n in forex_names}
        try:
            td_data = fetch_candles_twelvedata_batch(td_symbols, interval="1h", outputsize=200)
            for td_sym, candles in td_data.items():
                name = name_by_td.get(td_sym)
                if not name:
                    continue
                ticker, asset_class, _ = INSTRUMENTS[name]
                if candles:
                    try:
                        n = _process_candles_for_instrument(name, ticker, asset_class, candles, cfg)
                        stats["new_fvgs"] += n
                        stats["total_instruments"] += 1
                        stats["td_used"] += 1
                    except Exception as e:
                        logger.warning(f"[FVG] TD process {name}: {e}")
                        stats["errors"] += 1
                else:
                    # TD вернул пусто → fallback yfinance
                    try:
                        n = scan_one_instrument(name, ticker, asset_class, cfg, td_symbol=None)
                        stats["new_fvgs"] += n
                        stats["total_instruments"] += 1
                        stats["yf_used"] += 1
                    except Exception as e:
                        logger.warning(f"[FVG] YF fallback {name}: {e}")
                        stats["errors"] += 1
        except Exception as e:
            logger.exception(f"[FVG] TD batch fail: {e}")
            # Полный fallback на yfinance для всех форекс
            other_names = forex_names + other_names
            forex_names = []
    else:
        # Нет TD ключа — всё через yfinance
        other_names = forex_names + other_names
        forex_names = []

    # 2. Остальное через yfinance sequential
    for name in other_names:
        if name not in INSTRUMENTS:
            continue
        ticker, asset_class = INSTRUMENTS[name][0], INSTRUMENTS[name][1]
        try:
            n = scan_one_instrument(name, ticker, asset_class, cfg, td_symbol=None)
            stats["new_fvgs"] += n
            stats["total_instruments"] += 1
            stats["yf_used"] += 1
        except Exception as e:
            logger.debug(f"scan {name}: {e}")
            stats["errors"] += 1
    return stats


# ── Monitor open signals: retest + trailing + TP/SL ────────
def monitor_signals() -> dict:
    """Проверяет WAITING_RETEST и ENTERED сигналы по текущим ценам.
    Обновляет статусы и возвращает события (для алертов)."""
    cfg = get_config()
    col = _fvg_signals()
    now = utcnow()
    events = {"entered": [], "closed_tp": [], "closed_sl": [], "expired": []}

    # ── FIRST: Expiry check — не требует цен, только времени ──
    # Отдельный проход по всем WAITING_RETEST/ENTERED чтобы expire по времени
    # работал даже если yfinance/TD падают.
    exp_res = col.update_many(
        {"status": "WAITING_RETEST", "expire_at": {"$lt": now}},
        {"$set": {"status": "EXPIRED", "updated_at": now}},
    )
    if exp_res.modified_count:
        logger.info(f"[FVG] expired {exp_res.modified_count} stale WAITING")
    # Hold timeout для ENTERED — если не TP/SL за max_hold_bars часов от entered_at
    hold_cutoff = now - timedelta(hours=cfg.get("max_hold_bars", 50))
    th_res = col.update_many(
        {"status": "ENTERED", "entered_at": {"$lt": hold_cutoff}},
        {"$set": {"status": "TIMEOUT", "closed_at": now, "updated_at": now}},
    )
    if th_res.modified_count:
        logger.info(f"[FVG] timeout {th_res.modified_count} stale ENTERED")

    # Все активные (не закрытые) — уже после expiry чистки
    active = list(col.find({"status": {"$in": ["WAITING_RETEST", "ENTERED"]}}))
    if not active:
        return events

    # Группируем по ticker
    by_ticker: dict[str, list] = {}
    for s in active:
        by_ticker.setdefault(s["ticker"], []).append(s)

    for ticker, sigs in by_ticker.items():
        # CRITICAL: monitor_signals вызывается каждые 30с — НЕ дёргать TD live
        # (исчерпает 800/day за часы). TD обновляет cache только в scan_all
        # (раз в 60 мин). Monitor читает cache → yfinance fallback.
        first = sigs[0]
        instrument_name = first.get("instrument")

        candles = []
        # 1. Кеш из БД (обновляется сканером через TD раз в час)
        if instrument_name:
            candles = get_cached_candles(instrument_name, "1h", max_age_min=90)
        # 2. Fallback — yfinance (бесплатно, без лимитов; только для проверки цены)
        if not candles:
            try:
                candles = fetch_candles(ticker, period="2d", interval="1h")
            except Exception:
                continue
        if not candles:
            continue
        last_candle = candles[-1]
        cur_price = last_candle["c"]
        cur_high = last_candle["h"]
        cur_low = last_candle["l"]

        for s in sigs:
            is_long = s["direction"] == "bullish"
            entry_price = s["entry_price"]
            sl_price = s["sl_price"]
            risk = abs(entry_price - sl_price)

            if s["status"] == "WAITING_RETEST":
                # Expired?
                expire_at = s.get("expire_at")
                if expire_at and now >= expire_at:
                    col.update_one({"_id": s["_id"]}, {"$set": {
                        "status": "EXPIRED", "updated_at": now,
                    }})
                    events["expired"].append(s)
                    continue
                # Retest? (цена достигла зоны)
                retest = False
                if is_long:
                    retest = cur_low <= entry_price  # цена опустилась к top FVG
                else:
                    retest = cur_high >= entry_price  # цена поднялась к bottom FVG
                if retest:
                    # Прошли через SL сразу (gap)?
                    if is_long and cur_low <= sl_price:
                        # Считаем как loss
                        col.update_one({"_id": s["_id"]}, {"$set": {
                            "status": "SL",
                            "entered_at": now,
                            "entered_price": entry_price,
                            "closed_at": now,
                            "exit_price": sl_price,
                            "outcome_R": -1.0,
                            "updated_at": now,
                        }})
                        events["closed_sl"].append(s)
                    elif not is_long and cur_high >= sl_price:
                        col.update_one({"_id": s["_id"]}, {"$set": {
                            "status": "SL",
                            "entered_at": now,
                            "entered_price": entry_price,
                            "closed_at": now,
                            "exit_price": sl_price,
                            "outcome_R": -1.0,
                            "updated_at": now,
                        }})
                        events["closed_sl"].append(s)
                    else:
                        col.update_one({"_id": s["_id"]}, {"$set": {
                            "status": "ENTERED",
                            "entered_at": now,
                            "entered_price": entry_price,
                            "updated_at": now,
                        }})
                        # Обновим объект для алерта
                        s["entered_at"] = now
                        s["entered_price"] = entry_price
                        events["entered"].append(s)

            elif s["status"] == "ENTERED":
                # Обновляем peak_R и trailing
                peak_R = s.get("peak_R", 0.0)
                trailing_sl = s.get("trailing_sl") or sl_price
                trailing_active = s.get("trailing_active", False)

                if is_long:
                    # Считаем текущий R по high
                    cur_R_high = (cur_high - entry_price) / risk if risk > 0 else 0
                    if cur_R_high > peak_R:
                        peak_R = cur_R_high
                    # Активируем trailing после +1R
                    if peak_R >= cfg["trailing_activate_at"]:
                        trailing_active = True
                        new_sl = cur_high - cfg["trailing_distance_R"] * risk
                        if new_sl > trailing_sl:
                            trailing_sl = new_sl
                    effective_sl = trailing_sl if trailing_active else sl_price
                    # Проверка SL
                    if cur_low <= effective_sl:
                        final_R = (effective_sl - entry_price) / risk if risk > 0 else 0
                        status = "TP" if final_R > 0 else "SL"
                        col.update_one({"_id": s["_id"]}, {"$set": {
                            "status": status,
                            "closed_at": now,
                            "exit_price": effective_sl,
                            "outcome_R": round(final_R, 3),
                            "peak_R": round(peak_R, 3),
                            "trailing_active": trailing_active,
                            "trailing_sl": trailing_sl,
                            "updated_at": now,
                        }})
                        s["outcome_R"] = final_R
                        s["exit_price"] = effective_sl
                        s["peak_R"] = peak_R
                        if status == "TP":
                            events["closed_tp"].append(s)
                        else:
                            events["closed_sl"].append(s)
                    else:
                        col.update_one({"_id": s["_id"]}, {"$set": {
                            "peak_R": round(peak_R, 3),
                            "trailing_active": trailing_active,
                            "trailing_sl": trailing_sl,
                            "updated_at": now,
                        }})
                else:  # SHORT
                    cur_R_low = (entry_price - cur_low) / risk if risk > 0 else 0
                    if cur_R_low > peak_R:
                        peak_R = cur_R_low
                    if peak_R >= cfg["trailing_activate_at"]:
                        trailing_active = True
                        new_sl = cur_low + cfg["trailing_distance_R"] * risk
                        if trailing_sl is None or new_sl < trailing_sl:
                            trailing_sl = new_sl
                    effective_sl = trailing_sl if trailing_active else sl_price
                    if cur_high >= effective_sl:
                        final_R = (entry_price - effective_sl) / risk if risk > 0 else 0
                        status = "TP" if final_R > 0 else "SL"
                        col.update_one({"_id": s["_id"]}, {"$set": {
                            "status": status,
                            "closed_at": now,
                            "exit_price": effective_sl,
                            "outcome_R": round(final_R, 3),
                            "peak_R": round(peak_R, 3),
                            "trailing_active": trailing_active,
                            "trailing_sl": trailing_sl,
                            "updated_at": now,
                        }})
                        s["outcome_R"] = final_R
                        s["exit_price"] = effective_sl
                        s["peak_R"] = peak_R
                        if status == "TP":
                            events["closed_tp"].append(s)
                        else:
                            events["closed_sl"].append(s)
                    else:
                        col.update_one({"_id": s["_id"]}, {"$set": {
                            "peak_R": round(peak_R, 3),
                            "trailing_active": trailing_active,
                            "trailing_sl": trailing_sl,
                            "updated_at": now,
                        }})
    # Инвалидируем UI-кеши если что-то поменялось
    if any(events.values()):
        try:
            from cache_utils import fvg_signals_cache, fvg_journal_cache
            fvg_signals_cache.invalidate()
            fvg_journal_cache.invalidate()
        except Exception:
            pass
    return events


# ── API helpers ────────────────────────────────────────────
def get_pending_fvgs(limit: int = 50) -> list[dict]:
    """FVG ожидающие ретеста."""
    now = utcnow()
    docs = list(_fvg_signals().find({"status": "WAITING_RETEST"}).sort("formed_at", DESCENDING).limit(limit))
    for d in docs:
        d.pop("_id", None)
        expire = d.get("expire_at")
        if expire:
            time_left = (expire - now).total_seconds() / 3600
            d["time_left_h"] = round(max(0, time_left), 1)
        for k in ("formed_at", "expire_at", "entered_at", "closed_at", "created_at", "updated_at"):
            v = d.get(k)
            if hasattr(v, "isoformat"):
                d[k] = v.isoformat()
    return docs


def get_active_trades(limit: int = 50) -> list[dict]:
    """Открытые сделки (ENTERED)."""
    docs = list(_fvg_signals().find({"status": "ENTERED"}).sort("entered_at", DESCENDING).limit(limit))
    for d in docs:
        d.pop("_id", None)
        for k in ("formed_at", "expire_at", "entered_at", "closed_at", "created_at", "updated_at"):
            v = d.get(k)
            if hasattr(v, "isoformat"):
                d[k] = v.isoformat()
    return docs


def get_journal(hours: int = 168, status=None, instrument=None, direction=None, limit: int = 300) -> list[dict]:
    """Журнал — все сигналы за окно, с фильтрами."""
    since = utcnow() - timedelta(hours=hours) if hours else None
    q = {}
    if since:
        q["formed_at"] = {"$gte": since}
    if status and status != "all":
        q["status"] = status
    if instrument:
        q["instrument"] = instrument
    if direction:
        q["direction"] = direction
    docs = list(_fvg_signals().find(q).sort("formed_at", DESCENDING).limit(limit))
    for d in docs:
        d.pop("_id", None)
        for k in ("formed_at", "expire_at", "entered_at", "closed_at", "created_at", "updated_at"):
            v = d.get(k)
            if hasattr(v, "isoformat"):
                d[k] = v.isoformat()
    return docs


def get_stats() -> dict:
    """Агрегированная статистика."""
    col = _fvg_signals()
    total = col.count_documents({})
    waiting = col.count_documents({"status": "WAITING_RETEST"})
    entered = col.count_documents({"status": "ENTERED"})
    tp = col.count_documents({"status": "TP"})
    sl = col.count_documents({"status": "SL"})
    expired = col.count_documents({"status": "EXPIRED"})

    # Sum R и Avg R только по закрытым
    closed = list(col.find({"status": {"$in": ["TP", "SL"]}}, {"outcome_R": 1}))
    sum_r = sum((c.get("outcome_R") or 0) for c in closed)
    avg_r = sum_r / len(closed) if closed else 0
    wr = tp / (tp + sl) * 100 if (tp + sl) > 0 else 0
    return {
        "total": total,
        "waiting": waiting,
        "entered": entered,
        "tp": tp,
        "sl": sl,
        "expired": expired,
        "closed": tp + sl,
        "wr": round(wr, 1),
        "sum_r": round(sum_r, 2),
        "avg_r": round(avg_r, 3),
    }
