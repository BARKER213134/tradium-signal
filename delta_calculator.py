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
TF_MINUTES = {'15m': 15, '1h': 60, '4h': 240}
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
        # TTL 21 день — z-score anomaly на 4h требует 30 свечей baseline = 5 дней,
        # для сигналов 14d journal окна нужно держать candles до 14+5 = 19 дней.
        # Жёсткий лимит сверху чтобы не разрастаться (1500 пар × 4 TF × ... candles).
        col.create_index('cached_at', expireAfterSeconds=21 * 24 * 3600)
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


# ─── Binance Vision CDN — статический хост, без rate limit ──────────
# https://data.binance.vision/data/futures/um/daily/klines/SYMBOL/INTERVAL/SYMBOL-INTERVAL-DATE.zip
# Содержит CSV: open_time,open,high,low,close,volume,close_time,quote_volume,
#               count,taker_buy_volume,taker_buy_quote_volume,ignore
# Обновляется ~24h после конца дня. Сегодняшний день недоступен — используй
# REST для today, CDN для вчера и старше. Не банится Binance'ом никогда.
BINANCE_VISION = "https://data.binance.vision"


def _fetch_klines_cdn(symbol: str, tf: str, date_str: str) -> list[dict]:
    """Скачивает daily klines ZIP с Binance Vision CDN.
    date_str: 'YYYY-MM-DD'. Возвращает список свечей (формат _delta_from_klines_batch).
    """
    import io, zipfile, csv
    url = (f"{BINANCE_VISION}/data/futures/um/daily/klines/"
           f"{symbol}/{tf}/{symbol}-{tf}-{date_str}.zip")
    out = []
    try:
        r = _http_client.get(url, timeout=20)
        if r.status_code != 200:
            return []
        z = zipfile.ZipFile(io.BytesIO(r.content))
        names = z.namelist()
        if not names:
            return []
        with z.open(names[0]) as f:
            rdr = csv.reader(io.TextIOWrapper(f, encoding='utf-8'))
            for row in rdr:
                # Skip header (если есть)
                if not row or not row[0].isdigit():
                    continue
                try:
                    open_ms = int(row[0])
                    volume = float(row[5])
                    n_trades = int(row[8]) if len(row) > 8 else 0
                    taker_buy = float(row[9]) if len(row) > 9 else 0.0
                    sell_vol = max(0.0, volume - taker_buy)
                    delta_pct = ((taker_buy - sell_vol) / volume * 100.0) if volume > 0 else 0.0
                    out.append({
                        'open_ms': open_ms,
                        'buy_vol': round(taker_buy, 4),
                        'sell_vol': round(sell_vol, 4),
                        'delta_pct': round(delta_pct, 2),
                        'n_trades': n_trades,
                    })
                except Exception:
                    continue
    except Exception as e:
        logger.debug(f'[delta-cdn] {symbol}/{tf}/{date_str}: {e}')
        return []
    return out


def bulk_fill_pair_history_cdn(pair: str, start_dt, end_dt,
                               timeframes: tuple = ('15m', '1h', '4h')) -> dict:
    """ПОЛНЫЙ backfill через CDN — без rate limit, без банов.

    start_dt, end_dt: datetime UTC объекты (или таймстемпы в секундах).
    Скачивает ZIP'ы по дням для каждой пары × timeframe, парсит и пишет
    в cluster_delta cache. Текущий день пропускается (CDN ещё не имеет).

    Возвращает {tf: count_candles_written}.
    """
    from datetime import datetime, timezone, timedelta as _td
    import time as _t
    sym = _normalize_symbol(pair)
    if not sym:
        return {}
    # Normalize dates
    if isinstance(start_dt, (int, float)):
        start_dt = datetime.fromtimestamp(start_dt, tz=timezone.utc)
    if isinstance(end_dt, (int, float)):
        end_dt = datetime.fromtimestamp(end_dt, tz=timezone.utc)
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)
    # Don't try today — CDN не имеет
    today = datetime.now(timezone.utc).date()
    end_date = min(end_dt.date(), today - _td(days=1))
    if start_dt.date() > end_date:
        return {}

    try:
        from database import _get_db
        col = _get_db().cluster_delta
        _ensure_cache_indexes_once()
        from pymongo import UpdateOne
    except Exception:
        return {}
    result = {}
    for tf in timeframes:
        ops = []
        cur_date = start_dt.date()
        while cur_date <= end_date:
            date_str = cur_date.strftime('%Y-%m-%d')
            candles = _fetch_klines_cdn(sym, tf, date_str)
            now_dt = datetime.now(timezone.utc)
            for c in candles:
                ops.append(UpdateOne(
                    {'pair': pair, 'tf': tf, 'open_ms': c['open_ms']},
                    {'$set': {
                        'pair': pair, 'tf': tf, 'open_ms': c['open_ms'],
                        'delta_pct': c['delta_pct'],
                        'buy_vol': c['buy_vol'],
                        'sell_vol': c['sell_vol'],
                        'n_trades': c['n_trades'],
                        'cached_at': now_dt,
                    }},
                    upsert=True,
                ))
            cur_date += _td(days=1)
        try:
            if ops:
                # Bulk write batches по 500 ops
                for i in range(0, len(ops), 500):
                    col.bulk_write(ops[i:i+500], ordered=False)
                result[tf] = len(ops)
            else:
                result[tf] = 0
        except Exception as e:
            logger.debug(f'[delta-cdn] bulk write {pair}/{tf}: {e}')
            result[tf] = 0
    return result


def fetch_klines_cdn(symbol: str, tf: str, start_ms: int, end_ms: int) -> list:
    """Klines через CDN (без банов) + REST для today.
    Returns list of [open_ms, open, high, low, close, volume, close_ms].
    """
    import io, zipfile, csv
    out = []
    today = datetime.now(timezone.utc).date()
    start_dt = datetime.fromtimestamp(start_ms/1000, tz=timezone.utc).date()
    end_dt = datetime.fromtimestamp(end_ms/1000, tz=timezone.utc).date()
    cur = start_dt
    while cur <= end_dt:
        if cur >= today:
            # REST для today
            try:
                day_start = int(datetime(cur.year, cur.month, cur.day,
                                          tzinfo=timezone.utc).timestamp() * 1000)
                day_end = day_start + 24 * 3600 * 1000
                r = _http_client.get(
                    f"{BINANCE_FAPI}/fapi/v1/klines",
                    params={'symbol': symbol, 'interval': tf,
                            'startTime': max(day_start, start_ms),
                            'endTime': min(day_end, end_ms),
                            'limit': 1500},
                    timeout=10)
                if r.status_code == 200:
                    for kr in r.json():
                        try:
                            out.append([int(kr[0]), float(kr[1]), float(kr[2]),
                                        float(kr[3]), float(kr[4]),
                                        float(kr[5]), int(kr[6])])
                        except Exception:
                            continue
            except Exception:
                pass
            cur += timedelta(days=1)
            continue
        url = (f'{BINANCE_VISION}/data/futures/um/daily/klines/'
               f'{symbol}/{tf}/{symbol}-{tf}-{cur.strftime("%Y-%m-%d")}.zip')
        try:
            r = _http_client.get(url, timeout=15)
            if r.status_code == 200:
                z = zipfile.ZipFile(io.BytesIO(r.content))
                with z.open(z.namelist()[0]) as f:
                    rdr = csv.reader(io.TextIOWrapper(f, encoding='utf-8'))
                    for row in rdr:
                        if not row or not row[0].isdigit():
                            continue
                        try:
                            o = int(row[0])
                            if o < start_ms or o > end_ms:
                                continue
                            out.append([o, float(row[1]), float(row[2]),
                                        float(row[3]), float(row[4]),
                                        float(row[5]), int(row[6])])
                        except Exception:
                            continue
        except Exception:
            pass
        cur += timedelta(days=1)
    return sorted(out, key=lambda k: int(k[0]))


def _compute_rsi_for_closes(closes: list[float], period: int = 14) -> list:
    """RSI(14) Wilder — возвращает список того же размера, None для warmup."""
    if len(closes) < period + 1:
        return [None] * len(closes)
    rsi = [None] * len(closes)
    gains = [0.0]; losses = [0.0]
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i-1]
        gains.append(max(ch, 0)); losses.append(max(-ch, 0))
    avg_g = sum(gains[1:period+1]) / period
    avg_l = sum(losses[1:period+1]) / period
    rsi[period] = 100.0 if avg_l == 0 else (100 - 100/(1 + avg_g/avg_l))
    for i in range(period+1, len(closes)):
        avg_g = (avg_g * (period-1) + gains[i]) / period
        avg_l = (avg_l * (period-1) + losses[i]) / period
        rsi[i] = 100.0 if avg_l == 0 else (100 - 100/(1 + avg_g/avg_l))
    return rsi


def _delta_from_klines_batch(symbol: str, tf: str,
                             start_ms: int, end_ms: int) -> list[dict]:
    """ПОЛНАЯ ИСТОРИЯ через /fapi/v1/klines (taker buy volume).

    Binance kline возвращает per-candle:
      [5] volume (base asset)
      [9] taker buy base volume  ← это BUY (taker купил в ask)
      sell_vol = volume - taker_buy

    Это арифметически идентично _delta_from_trades(aggTrades), но:
      - есть полная история (годы), не 24h как aggTrades
      - 1 запрос → до 1500 свечей (vs 1000 aggTrades)
      - в 100× быстрее для backfill

    Возвращает список свечей: [{open_ms, delta_pct, buy_vol, sell_vol, n_trades}]
    """
    out: list[dict] = []
    cursor = start_ms
    max_pages = 30  # 30 × 1500 = 45000 свечей max — для 14d на 15m с запасом
    for _ in range(max_pages):
        try:
            r = _http_client.get(
                f"{BINANCE_FAPI}/fapi/v1/klines",
                params={
                    'symbol': symbol, 'interval': tf,
                    'startTime': cursor, 'endTime': end_ms,
                    'limit': 1500,
                },
            )
            if r.status_code != 200:
                logger.debug(f'[delta] klines {symbol} {tf} {r.status_code}: {r.text[:100]}')
                break
            batch = r.json()
        except Exception as e:
            logger.debug(f'[delta] klines fetch fail {symbol}/{tf}: {e}')
            break
        if not batch:
            break
        for k in batch:
            try:
                open_ms = int(k[0])
                volume = float(k[5])
                taker_buy = float(k[9])
                n_trades = int(k[8]) if len(k) > 8 else 0
                buy_vol = taker_buy
                sell_vol = max(0.0, volume - taker_buy)
                total = volume
                delta_pct = ((buy_vol - sell_vol) / total * 100.0) if total > 0 else 0.0
                out.append({
                    'open_ms': open_ms,
                    'buy_vol': round(buy_vol, 4),
                    'sell_vol': round(sell_vol, 4),
                    'delta_pct': round(delta_pct, 2),
                    'n_trades': n_trades,
                })
            except Exception:
                continue
        last_close = int(batch[-1][6]) if len(batch[-1]) > 6 else 0
        if last_close <= cursor or len(batch) < 1500:
            break
        cursor = last_close + 1
        if cursor >= end_ms:
            break
    return out


def bulk_fill_pair_history(pair: str, start_ms: int, end_ms: int,
                           timeframes: tuple = ('15m', '1h', '4h')) -> dict:
    """Массовый backfill: фетчит klines диапазона и пишет cluster_delta cache
    для всех свечей. Используется для backfill всех signals из БД (год+ назад).

    Возвращает {tf: count} — сколько свечей записано в cache.
    """
    sym = _normalize_symbol(pair)
    if not sym:
        return {}
    _ensure_cache_indexes_once()
    try:
        from database import _get_db
        col = _get_db().cluster_delta
    except Exception:
        return {}
    result = {}
    for tf in timeframes:
        candles = _delta_from_klines_batch(sym, tf, start_ms, end_ms)
        if not candles:
            result[tf] = 0
            continue
        # bulk upsert
        try:
            from pymongo import UpdateOne
            now = datetime.now(timezone.utc)
            ops = [
                UpdateOne(
                    {'pair': pair, 'tf': tf, 'open_ms': c['open_ms']},
                    {'$set': {
                        'pair': pair, 'tf': tf, 'open_ms': c['open_ms'],
                        'delta_pct': c['delta_pct'],
                        'buy_vol': c['buy_vol'],
                        'sell_vol': c['sell_vol'],
                        'n_trades': c['n_trades'],
                        'cached_at': now,
                    }},
                    upsert=True,
                )
                for c in candles
            ]
            if ops:
                col.bulk_write(ops, ordered=False)
                result[tf] = len(ops)
            else:
                result[tf] = 0
        except Exception as e:
            logger.debug(f'[delta] bulk write fail {pair}/{tf}: {e}')
            result[tf] = 0
    return result


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


def get_signal_delta_only(pair: str, at_ts_ms: Optional[int] = None,
                          timeframes: tuple = ('15m', '1h', '4h')) -> dict:
    """СУПЕР-БЫСТРАЯ версия: только delta_pct сигнальной свечи (без резонанса).

    Делает 1 aggTrades call на TF (vs 5 в полной версии). Используется в
    inline fetch journal API чтобы успеть в budget 8с для 15+ пар.

    Резонанс заполняется потом фоном (background fill использует полную
    версию + cache hits растут).
    """
    if at_ts_ms is None:
        at_ts_ms = int(time.time() * 1000)
    out = {}
    for tf in timeframes:
        try:
            minutes = TF_MINUTES.get(tf)
            if minutes is None:
                out[tf] = None
                continue
            sig_open = _candle_open_ms(at_ts_ms, tf)
            snap = _candle_delta(pair, tf, sig_open)
            if not snap:
                out[tf] = None
                continue
            out[tf] = {
                'delta_pct': snap.get('delta_pct', 0),
                'buy_vol': snap.get('buy_vol', 0),
                'sell_vol': snap.get('sell_vol', 0),
                'n_trades': snap.get('n_trades', 0),
                'resonance': 0,  # заполнится в bg fill при следующем рендере
                'resonance_window': 1,
            }
        except Exception as e:
            logger.debug(f'[delta-signal-only] {pair}/{tf}: {e}')
            out[tf] = None
    return out


def get_delta_snapshot_fast(pair: str, at_ts_ms: Optional[int] = None,
                            timeframes: tuple = ('15m', '1h', '4h')) -> dict:
    """БЫСТРАЯ версия через klines (1 запрос на TF → 5 свечей резонанса).

    В ~10× быстрее `get_delta_snapshot` (aggTrades) на одиночных вызовах.
    Использовать для on-demand fill свежих сигналов в journal API.

    Также пишет результат в cluster_delta cache.
    """
    if at_ts_ms is None:
        at_ts_ms = int(time.time() * 1000)
    sym = _normalize_symbol(pair)
    if not sym:
        return {tf: None for tf in timeframes}
    out = {}
    try:
        from database import _get_db
        cd_col = _get_db().cluster_delta
        _ensure_cache_indexes_once()
    except Exception:
        cd_col = None
    # Окно baseline для anomaly z-score (30 свечей). Берём 35 чтоб иметь запас.
    # Раньше брали только RESONANCE_BARS=5, поэтому anomaly cell в журнале была
    # пустой - не хватало истории для расчёта std/mean baseline.
    BASELINE_BARS = 35
    for tf in timeframes:
        try:
            minutes = TF_MINUTES.get(tf)
            if minutes is None:
                continue
            sig_open = _candle_open_ms(at_ts_ms, tf)
            # Берём BASELINE_BARS свечей ДО signal candle (для z-score + резонанс)
            start_ms = sig_open - BASELINE_BARS * minutes * 60 * 1000
            end_ms = sig_open + minutes * 60 * 1000
            candles = _delta_from_klines_batch(sym, tf, start_ms, end_ms)
            if not candles:
                out[tf] = None
                continue
            by_open = {c['open_ms']: c for c in candles}
            sig_doc = by_open.get(sig_open)
            if not sig_doc:
                out[tf] = None
                continue
            # Резонанс: соберём N свечей до signal candle
            deltas: list[float] = []
            for j in range(RESONANCE_BARS - 1, -1, -1):
                bk = sig_open - j * minutes * 60 * 1000
                if bk in by_open:
                    deltas.append(by_open[bk].get('delta_pct') or 0.0)
            out[tf] = {
                'delta_pct': sig_doc.get('delta_pct', 0),
                'buy_vol': sig_doc.get('buy_vol', 0),
                'sell_vol': sig_doc.get('sell_vol', 0),
                'n_trades': sig_doc.get('n_trades', 0),
                'resonance': _resonance_from_deltas(deltas),
                'resonance_window': len(deltas),
            }
            # Записываем в cache (только закрытые свечи)
            if cd_col is not None:
                now_ms = int(time.time() * 1000)
                try:
                    from pymongo import UpdateOne
                    now_dt = datetime.now(timezone.utc)
                    ops = []
                    for c in candles:
                        # Только закрытые свечи
                        if c['open_ms'] + minutes * 60 * 1000 > now_ms:
                            continue
                        ops.append(UpdateOne(
                            {'pair': pair, 'tf': tf, 'open_ms': c['open_ms']},
                            {'$set': {
                                'pair': pair, 'tf': tf, 'open_ms': c['open_ms'],
                                'delta_pct': c['delta_pct'],
                                'buy_vol': c['buy_vol'],
                                'sell_vol': c['sell_vol'],
                                'n_trades': c['n_trades'],
                                'cached_at': now_dt,
                            }},
                            upsert=True,
                        ))
                    if ops:
                        cd_col.bulk_write(ops, ordered=False)
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f'[delta-fast] {pair}/{tf}: {e}')
            out[tf] = None
    return out


def get_delta_snapshot(pair: str, at_ts_ms: Optional[int] = None,
                       timeframes: tuple = ('15m', '1h', '4h')) -> dict:
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
                                   timeframes: tuple = ('15m', '1h', '4h')) -> dict:
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
