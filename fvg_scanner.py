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
INSTRUMENTS = {
    # Forex Majors (7)
    "EURUSD":  ("EURUSD=X", "forex"),
    "GBPUSD":  ("GBPUSD=X", "forex"),
    "USDJPY":  ("USDJPY=X", "forex"),
    "USDCHF":  ("USDCHF=X", "forex"),
    "AUDUSD":  ("AUDUSD=X", "forex"),
    "NZDUSD":  ("NZDUSD=X", "forex"),
    "USDCAD":  ("USDCAD=X", "forex"),
    # Crosses (13)
    "EURGBP":  ("EURGBP=X", "forex"),
    "EURJPY":  ("EURJPY=X", "forex"),
    "EURCHF":  ("EURCHF=X", "forex"),
    "EURAUD":  ("EURAUD=X", "forex"),
    "EURCAD":  ("EURCAD=X", "forex"),
    "GBPJPY":  ("GBPJPY=X", "forex"),
    "GBPAUD":  ("GBPAUD=X", "forex"),
    "GBPCAD":  ("GBPCAD=X", "forex"),
    "AUDJPY":  ("AUDJPY=X", "forex"),
    "AUDCAD":  ("AUDCAD=X", "forex"),
    "AUDNZD":  ("AUDNZD=X", "forex"),
    "CADJPY":  ("CADJPY=X", "forex"),
    "CHFJPY":  ("CHFJPY=X", "forex"),
    # Exotics (2)
    "USDMXN":  ("USDMXN=X", "forex"),
    "USDNOK":  ("USDNOK=X", "forex"),
    # Metals (2)
    "XAUUSD":  ("GC=F", "metal"),
    "XAGUSD":  ("SI=F", "metal"),
    # Indices (6)
    "SPX500":  ("^GSPC", "index"),
    "NAS100":  ("^IXIC", "index"),
    "US30":    ("^DJI",  "index"),
    "GER40":   ("^GDAXI", "index"),
    "UK100":   ("^FTSE", "index"),
    "JPN225":  ("^N225", "index"),
    # Energy (2)
    "USOIL":   ("CL=F",  "energy"),
    "UKOIL":   ("BZ=F",  "energy"),
}


# ── Конфиг ──────────────────────────────────────────────────
DEFAULT_CONFIG = {
    "enabled_instruments": list(INSTRUMENTS.keys()),
    "timeframe": "1H",
    "scan_interval_min": 10,
    "min_body_ratio": 0.5,
    "min_size_rel_forex": 0.0003,
    "min_size_rel_metal": 0.0005,
    "min_size_rel_energy": 0.0005,
    "min_size_rel_index": 0.0003,
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


# ── yfinance wrapper ────────────────────────────────────────
def fetch_candles(ticker: str, period: str = "7d", interval: str = "1h") -> list[dict]:
    """Скачивает свечи через yfinance."""
    try:
        import yfinance as yf
        df = yf.download(ticker, period=period, interval=interval,
                         progress=False, auto_adjust=True, threads=False)
        if df.empty:
            return []
        candles = []
        for ts, row in df.iterrows():
            try:
                def v(k):
                    x = row[k]
                    return float(x.iloc[0] if hasattr(x, 'iloc') else x)
                o, h, l, c = v("Open"), v("High"), v("Low"), v("Close")
                if not (o > 0 and h > 0 and l > 0 and c > 0):
                    continue
                # pandas timestamp → unix seconds
                ts_val = ts
                if hasattr(ts_val, "timestamp"):
                    unix = int(ts_val.timestamp())
                else:
                    unix = 0
                candles.append({"t": unix, "o": o, "h": h, "l": l, "c": c, "v": 0})
            except Exception:
                continue
        return candles
    except Exception as e:
        logger.debug(f"fetch {ticker}: {e}")
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
def scan_one_instrument(name: str, ticker: str, asset_class: str, cfg: dict) -> int:
    """Сканирует один инструмент. Возвращает сколько новых FVG создано."""
    candles = fetch_candles(ticker, period="7d", interval="1h")
    if len(candles) < 20:
        return 0

    fvgs = detect_fvg(candles, min_size_rel=0.0001)  # грубый порог — потом фильтруем v2
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
    return created


def scan_all() -> dict:
    """Сканирует все enabled инструменты. Возвращает статистику."""
    cfg = get_config()
    enabled = cfg.get("enabled_instruments", list(INSTRUMENTS.keys()))
    stats = {"total_instruments": 0, "new_fvgs": 0, "errors": 0}

    for name in enabled:
        if name not in INSTRUMENTS:
            continue
        ticker, asset_class = INSTRUMENTS[name]
        try:
            n = scan_one_instrument(name, ticker, asset_class, cfg)
            stats["new_fvgs"] += n
            stats["total_instruments"] += 1
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

    # Все активные (не закрытые)
    active = list(col.find({"status": {"$in": ["WAITING_RETEST", "ENTERED"]}}))
    if not active:
        return events

    # Группируем по ticker чтобы минимизировать yfinance запросы
    by_ticker: dict[str, list] = {}
    for s in active:
        by_ticker.setdefault(s["ticker"], []).append(s)

    import yfinance as yf

    for ticker, sigs in by_ticker.items():
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
