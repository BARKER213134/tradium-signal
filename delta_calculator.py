"""Cluster Delta + Resonance — buy/sell flow analysis per Binance Futures.

Использует aggTrades endpoint (без API key, бесплатный):
    GET /fapi/v1/aggTrades?symbol=BTCUSDT&startTime=&endTime=&limit=1000

Поля trade: m (isBuyerMaker), p (price), q (quantity base).
    isBuyerMaker=true  → market SELL (taker продал в bid)
    isBuyerMaker=false → market BUY  (taker купил в ask)

ВАЖНО: aggTrades живут только ~24h на Binance — старые сигналы не получится
дозалить. Поэтому считаем дельту в момент создания сигнала и кешируем в Mongo.

Резонанс = N подряд свечей с дельтой одного знака:
    +5 = 5 зелёных подряд (бычий поток непрерывный)
    -5 = 5 красных подряд (медведи продавливают)
     0 = чередуются

Кеш: collection cluster_delta с TTL 7 дней (expireAfterSeconds index).
Ключ: (pair, timeframe, candle_open_ms)
"""
from __future__ import annotations
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────
BINANCE_FAPI = "https://fapi.binance.com"
RESONANCE_BARS = 5      # окно резонанса (-5..+5)
TF_MINUTES = {'15m': 15, '1h': 60}
DELTA_NEUTRAL_THRESHOLD = 0.1  # |delta_pct| < 0.1% = нейтрально (для резонанса)

# HTTP client с keep-alive (отдельный от exchange.py чтобы не мешать)
_http_client = httpx.Client(
    timeout=10.0,
    limits=httpx.Limits(max_connections=10, max_keepalive_connections=5,
                        keepalive_expiry=30.0),
    headers={"Accept-Encoding": "gzip"},
)


def _normalize_symbol(pair: str) -> str:
    """BTC/USDT → BTCUSDT (на Binance Futures всё в формате без слэша)."""
    if not pair:
        return ""
    sym = pair.replace('/', '').replace('-', '').replace(' ', '').upper()
    if not sym.endswith('USDT'):
        sym = sym + 'USDT'
    return sym


def _candle_open_ms(ts_ms: int, tf: str) -> int:
    """Округляет timestamp вниз до начала свечи на TF."""
    minutes = TF_MINUTES.get(tf, 15)
    bucket_ms = minutes * 60 * 1000
    return (ts_ms // bucket_ms) * bucket_ms


# ─── Mongo cache ──────────────────────────────────────────────────────
def _ensure_cache_indexes_once():
    """Создаёт индексы на cluster_delta при первом обращении."""
    global _indexes_ready
    if _indexes_ready:
        return
    try:
        from database import _get_db
        col = _get_db().cluster_delta
        col.create_index([('pair', 1), ('tf', 1), ('open_ms', 1)], unique=True)
        # TTL 7 дней — старее не нужно (сигналы живут 24h в журнале)
        col.create_index('cached_at', expireAfterSeconds=7 * 24 * 3600)
        _indexes_ready = True
    except Exception as e:
        logger.debug(f'[delta] index ensure fail: {e}')


_indexes_ready = False


def _cache_get(pair: str, tf: str, open_ms: int) -> Optional[dict]:
    try:
        from database import _get_db
        return _get_db().cluster_delta.find_one(
            {'pair': pair, 'tf': tf, 'open_ms': open_ms},
            {'_id': 0},
        )
    except Exception:
        return None


def _cache_put(pair: str, tf: str, open_ms: int, data: dict):
    try:
        from database import _get_db
        _ensure_cache_indexes_once()
        doc = {**data, 'pair': pair, 'tf': tf, 'open_ms': open_ms,
               'cached_at': datetime.now(timezone.utc)}
        _get_db().cluster_delta.update_one(
            {'pair': pair, 'tf': tf, 'open_ms': open_ms},
            {'$set': doc},
            upsert=True,
        )
    except Exception as e:
        logger.debug(f'[delta] cache put fail: {e}')


# ─── Binance aggTrades fetcher ────────────────────────────────────────
def _fetch_agg_trades(symbol: str, start_ms: int, end_ms: int) -> list[dict]:
    """Выкачивает aggTrades в окне [start_ms, end_ms].
    Binance отдаёт max 1000 trades за вызов; для 1h на BTC это может быть
    10k+ trades — паджинируем по startTime последнего trade.
    """
    out: list[dict] = []
    cursor = start_ms
    # На altах (>95% сигналов) <5 страниц, моментально.
    # На BTC 1h может быть 30-50k trades — хватит ~20 страниц для proportional
    # delta% с точностью ±2%. Дальше diminishing returns vs latency.
    max_pages = 20
    for _ in range(max_pages):
        try:
            r = _http_client.get(
                f"{BINANCE_FAPI}/fapi/v1/aggTrades",
                params={
                    'symbol': symbol,
                    'startTime': cursor,
                    'endTime': end_ms,
                    'limit': 1000,
                },
            )
            if r.status_code != 200:
                logger.debug(f'[delta] aggTrades {symbol} {r.status_code}: {r.text[:120]}')
                break
            batch = r.json()
        except Exception as e:
            logger.debug(f'[delta] aggTrades fetch fail {symbol}: {e}')
            break
        if not batch:
            break
        out.extend(batch)
        if len(batch) < 1000:
            break  # последняя страница
        # Сдвигаем курсор на T последнего trade + 1ms чтобы не перекрытие
        last_t = batch[-1].get('T', 0)
        if last_t <= cursor:
            break  # защита от зацикливания
        cursor = last_t + 1
        if cursor >= end_ms:
            break
    return out


def _delta_from_trades(trades: list[dict]) -> dict:
    """Считает (buy_vol, sell_vol, delta_pct) по списку aggTrades.
    delta_pct = (buy - sell) / total * 100  → -100..+100
    """
    buy_vol = 0.0
    sell_vol = 0.0
    n_trades = 0
    for t in trades:
        try:
            qty = float(t.get('q', 0))
        except Exception:
            continue
        # 'm' = isBuyerMaker. true = market SELL, false = market BUY
        is_maker = t.get('m', False)
        if is_maker:
            sell_vol += qty
        else:
            buy_vol += qty
        n_trades += 1
    total = buy_vol + sell_vol
    delta_pct = ((buy_vol - sell_vol) / total * 100.0) if total > 0 else 0.0
    return {
        'buy_vol': round(buy_vol, 4),
        'sell_vol': round(sell_vol, 4),
        'delta_pct': round(delta_pct, 2),
        'n_trades': n_trades,
    }


def _candle_delta(pair: str, tf: str, open_ms: int,
                  use_cache: bool = True) -> Optional[dict]:
    """Возвращает delta-snapshot для одной свечи. Кеширует в Mongo."""
    if use_cache:
        cached = _cache_get(pair, tf, open_ms)
        if cached:
            return cached
    sym = _normalize_symbol(pair)
    if not sym:
        return None
    minutes = TF_MINUTES.get(tf, 15)
    end_ms = open_ms + minutes * 60 * 1000
    # Не кешируем ещё-открытую (текущую) свечу — она меняется
    now_ms = int(time.time() * 1000)
    is_closed = end_ms <= now_ms
    trades = _fetch_agg_trades(sym, open_ms, end_ms)
    if not trades:
        return None
    res = _delta_from_trades(trades)
    if is_closed and use_cache:
        _cache_put(pair, tf, open_ms, res)
    return res


# ─── Resonance ─────────────────────────────────────────────────────────
def _resonance_from_deltas(deltas: list[float]) -> int:
    """Считает резонанс — сколько подряд свечей с дельтой одного знака,
    начиная с самой свежей (правый конец списка).

    Возвращает -RESONANCE_BARS .. +RESONANCE_BARS:
        +N = N свежих свечей подряд BUY-дельта
        -N = N свежих свечей подряд SELL-дельта
         0 = последняя свеча нейтральная или один знак не повторился
    """
    if not deltas:
        return 0
    last = deltas[-1]
    if abs(last) < DELTA_NEUTRAL_THRESHOLD:
        return 0
    sign = 1 if last > 0 else -1
    count = 0
    for d in reversed(deltas):
        d_sign = 1 if d > 0 else (-1 if d < 0 else 0)
        if d_sign == sign:
            count += 1
        else:
            break
    return count * sign


def get_delta_snapshot(pair: str, at_ts_ms: Optional[int] = None,
                       timeframes: tuple = ('15m', '1h')) -> dict:
    """Главный entry-point: для пары на момент at_ts_ms возвращает
    delta_pct + resonance по каждому TF.

    at_ts_ms: миллисекунды UTC; если None → сейчас.
    Возвращает: {
        '15m': {'delta_pct': +12.4, 'resonance': +3, 'buy_vol': ..., 'sell_vol': ...},
        '1h':  {'delta_pct': +14.1, 'resonance': +4, ...},
    }
    """
    if at_ts_ms is None:
        at_ts_ms = int(time.time() * 1000)
    out = {}
    for tf in timeframes:
        try:
            minutes = TF_MINUTES.get(tf)
            if minutes is None:
                continue
            # Свеча, в которой попал at_ts_ms (signal candle)
            signal_open_ms = _candle_open_ms(at_ts_ms, tf)
            # Резонанс — N свежих свечей до сигнальной включительно
            deltas: list[float] = []
            snap_for_signal = None
            for i in range(RESONANCE_BARS - 1, -1, -1):
                bucket_open = signal_open_ms - i * minutes * 60 * 1000
                snap = _candle_delta(pair, tf, bucket_open)
                if snap is None:
                    continue
                deltas.append(snap.get('delta_pct') or 0.0)
                if i == 0:
                    snap_for_signal = snap
            if snap_for_signal is None:
                out[tf] = None
                continue
            out[tf] = {
                'delta_pct': snap_for_signal.get('delta_pct', 0),
                'buy_vol': snap_for_signal.get('buy_vol', 0),
                'sell_vol': snap_for_signal.get('sell_vol', 0),
                'n_trades': snap_for_signal.get('n_trades', 0),
                'resonance': _resonance_from_deltas(deltas),
                'resonance_window': len(deltas),
            }
        except Exception as e:
            logger.debug(f'[delta] snapshot fail {pair}/{tf}: {e}')
            out[tf] = None
    return out


# ─── Async wrapper для встраивания в watcher ──────────────────────────
async def get_delta_snapshot_async(pair: str, at_ts_ms: Optional[int] = None,
                                   timeframes: tuple = ('15m', '1h')) -> dict:
    """Async-обёртка — выполняет sync HTTP+Mongo в to_thread."""
    import asyncio
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(get_delta_snapshot, pair, at_ts_ms, timeframes),
            timeout=15.0,
        )
    except asyncio.TimeoutError:
        logger.debug(f'[delta] timeout {pair}')
        return {tf: None for tf in timeframes}
    except Exception as e:
        logger.debug(f'[delta] async fail {pair}: {e}')
        return {tf: None for tf in timeframes}


# ─── Helpers для UI ────────────────────────────────────────────────────
def delta_color(delta_pct: float) -> str:
    """Цвет для UI на основе знака дельты."""
    if delta_pct >= 5:
        return '#00e5a0'    # strong green
    if delta_pct >= 1:
        return '#88d8a3'    # light green
    if delta_pct >= -1:
        return '#aaa'       # neutral
    if delta_pct >= -5:
        return '#ff9d6e'    # light red
    return '#ff4d6d'        # strong red


def resonance_emoji(score: int) -> str:
    """Visual marker для резонанса."""
    if score >= 4: return '🔥'
    if score >= 2: return '🟢'
    if score >= 1: return '↑'
    if score == 0: return '⚪'
    if score >= -1: return '↓'
    if score >= -3: return '🔴'
    return '❄️'


def format_compact(snap: dict) -> str:
    """Компактный текстовый формат для Telegram/log:
    '15m: +12% / +3 🟢  1h: +14% / +4 🔥'
    """
    parts = []
    for tf in ('15m', '1h'):
        s = (snap or {}).get(tf)
        if not s:
            parts.append(f'{tf}: —')
            continue
        d = s.get('delta_pct', 0)
        r = s.get('resonance', 0)
        parts.append(f'{tf}: {d:+.1f}% / {r:+d} {resonance_emoji(r)}')
    return '  '.join(parts)
