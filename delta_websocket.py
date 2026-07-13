"""WebSocket стрим Binance Futures klines → cluster_delta cache.

Подключение к wss://fstream.binance.com/stream — отдельный rate-bucket
от REST API, не банится. Подписываемся на @kline_15m + @kline_1h для всех
tracked pairs. На каждое kline update пишем дельту в cluster_delta.

Преимущества vs REST:
- Realtime (sub-second latency)
- Никогда не банится IP
- 1 connection покрывает до 1024 streams = 512 пар × 2 TF

Запускается из watcher.py через asyncio.create_task(run_kline_stream()).
"""
from __future__ import annotations
import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

WS_URL = "wss://fstream.binance.com/stream"
TIMEFRAMES = ('15m', '1h')


def _normalize_pair(symbol_upper: str) -> str:
    """BTCUSDT → BTC/USDT."""
    if symbol_upper.endswith('USDT'):
        return symbol_upper[:-4] + '/USDT'
    return symbol_upper


def _kline_doc(pair: str, tf: str, kline: dict) -> Optional[dict]:
    """Док cluster_delta из WS-сообщения (k.t=open, k.v=vol, k.V=taker buy)."""
    try:
        open_ms = int(kline.get('t', 0))
        volume = float(kline.get('v', 0))
        if not open_ms or volume <= 0:
            return None
        taker_buy = float(kline.get('V', 0))
        sell_vol = max(0.0, volume - taker_buy)
        return {
            'pair': pair, 'tf': tf, 'open_ms': open_ms,
            'delta_pct': round((taker_buy - sell_vol) / volume * 100.0, 2),
            'buy_vol': round(taker_buy, 4),
            'sell_vol': round(sell_vol, 4),
            'n_trades': int(kline.get('n', 0)),
            'cached_at': datetime.now(timezone.utc),
            'source': 'ws',
        }
    except Exception:
        return None


def _flush_docs(docs: list):
    """Пакетная запись (в thread!). Sync-запись per-message в event loop
    блокировала цикл на каждом тике — профиль «жил час и завис» (2026-07-13)."""
    try:
        from database import _get_db
        from pymongo import UpdateOne
        ops = [UpdateOne({'pair': d['pair'], 'tf': d['tf'], 'open_ms': d['open_ms']},
                         {'$set': d}, upsert=True) for d in docs]
        if ops:
            _get_db().cluster_delta.bulk_write(ops, ordered=False)
    except Exception as e:
        logger.debug(f'[delta-ws] flush: {e}')


def _get_active_pairs(hours: int = 24) -> list[str]:
    """Пары с сигналами за N часов, отсортированы по СВЕЖЕСТИ последнего
    сигнала (desc) — при обрезке под лимит URL отваливаются самые старые,
    а не конец алфавита."""
    try:
        from database import _get_db
        from datetime import timedelta as _td
        db = _get_db()
        since = datetime.now(timezone.utc) - _td(hours=hours)
        latest: dict = {}

        def _acc(cursor, ts_field):
            for s in cursor:
                p, ts = s.get('pair'), s.get(ts_field)
                if p and ts:
                    if p not in latest or ts > latest[p]:
                        latest[p] = ts

        _acc(db.supertrend_signals.find({'flip_at': {'$gte': since}},
                                        {'pair': 1, 'flip_at': 1}).limit(2000), 'flip_at')
        _acc(db.signals.find({'received_at': {'$gte': since}},
                             {'pair': 1, 'received_at': 1}).limit(3000), 'received_at')
        _acc(db.new_strategy_signals.find({'created_at': {'$gte': since}},
                                          {'pair': 1, 'created_at': 1}).limit(3000), 'created_at')
        return [p for p, _ in sorted(latest.items(), key=lambda kv: kv[1], reverse=True)]
    except Exception as e:
        logger.warning(f'[delta-ws] get_active_pairs fail: {e}')
        return []


async def run_kline_stream():
    """Главная WS петля. Переподключается при разрывах."""
    try:
        import websockets
    except ImportError:
        logger.error('[delta-ws] websockets package not installed')
        return
    while True:
        try:
            # пульс: цикл жив (даже если коннект не удаётся) — для /api/health
            try:
                from watcher import _hb
                await asyncio.to_thread(_hb, 'delta_ws_loop')
            except Exception:
                pass
            pairs = await asyncio.to_thread(_get_active_pairs, 24)
            # только реальные фьючерсные символы: в сигналах бывают спотовые
            # пары (Vision-фолбэк универсума) — fstream их не знает
            try:
                from futures_data import get_all_futures_pairs
                fut = set(await asyncio.to_thread(get_all_futures_pairs))
                if fut:
                    pairs = [p for p in pairs if p.replace('/', '').upper() in fut]
            except Exception:
                pass
            if not pairs:
                logger.info('[delta-ws] no active pairs, sleep 5min')
                await asyncio.sleep(300)
                continue
            # Binance permits max 1024 streams per connection, но гигантский
            # SUBSCRIBE (1000 params одним сообщением) молча отвергается —
            # 2026-07-13 стрим 2ч крутил connect->тишина->reconnect после
            # того как бэкфил раздул список пар и URL превысил 16k.
            # Поэтому: ТОЛЬКО ?streams= URL, режем самые старые пары под лимит.
            pairs = pairs[:400]
            while pairs:
                streams = []
                for pair in pairs:
                    sym = pair.replace('/', '').lower()
                    for tf in TIMEFRAMES:
                        streams.append(f'{sym}@kline_{tf}')
                url = f'{WS_URL}?streams={"/".join(streams)}'
                if len(url) <= 15000:
                    break
                pairs = pairs[:len(pairs) - 20]

            logger.info(f'[delta-ws] connecting: {len(pairs)} pairs, {len(streams)} streams')
            async with websockets.connect(url, ping_interval=180,
                                           ping_timeout=600,
                                           max_size=2**24,
                                           close_timeout=10) as ws:
                logger.info(f'[delta-ws] connected, listening...')
                import os as _os

                def _wr_status(**kw):
                    try:
                        from database import _get_db, utcnow
                        _get_db().system.update_one(
                            {'_id': 'delta_ws_status'},
                            {'$set': {'at': utcnow(), 'pid': _os.getpid(), **kw}},
                            upsert=True)
                    except Exception:
                        pass
                await asyncio.to_thread(_wr_status, state='connected',
                                        last_error=None,
                                        streams_n=len(streams),
                                        sample=streams[:3])
                last_persist_log = time.time()
                count = 0
                got_first = False
                pending: dict = {}
                last_flush = time.time()
                while True:
                    try:
                        # первое сообщение ждём 45с: если Binance молчит —
                        # подписка невалидна (не тот рынок/символы), фиксируем
                        raw = await asyncio.wait_for(ws.recv(),
                                                     timeout=45 if not got_first else 300)
                    except asyncio.TimeoutError:
                        if not got_first:
                            logger.warning('[delta-ws] no FIRST message 45s — bad subscription?')
                            await asyncio.to_thread(_wr_status, state='no_data',
                                                    last_error='connected но 0 сообщений за 45с')
                        else:
                            logger.warning('[delta-ws] no messages 5min, reconnect')
                        break
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    got_first = True
                    if 'data' not in msg:
                        continue  # ack/error
                    data = msg['data']
                    if data.get('e') != 'kline':
                        continue
                    k = data.get('k', {})
                    sym = data.get('s', '')
                    if not sym or not k:
                        continue
                    tf = k.get('i', '')
                    if tf not in TIMEFRAMES:
                        continue
                    pair = _normalize_pair(sym)
                    # копим и пишем пачкой раз в ~2с (bulk в thread)
                    d = _kline_doc(pair, tf, k)
                    if d:
                        pending[(pair, tf, d['open_ms'])] = d
                    if len(pending) >= 300 or (pending and time.time() - last_flush > 2.0):
                        docs = list(pending.values())
                        pending.clear()
                        last_flush = time.time()
                        await asyncio.to_thread(_flush_docs, docs)
                    count += 1
                    if time.time() - last_persist_log > 60:
                        logger.info(f'[delta-ws] persisted {count} klines/min')
                        last_persist_log = time.time()
                        count = 0
        except Exception as e:
            logger.warning(f'[delta-ws] connection error: {e}; reconnect in 10s')
            # диагноз в Mongo — виден в /api/health (логов Railway у нас нет)
            try:
                from database import _get_db, utcnow
                _err = f'{type(e).__name__}: {str(e)[:180]}'
                await asyncio.to_thread(lambda: _get_db().system.update_one(
                    {'_id': 'delta_ws_status'},
                    {'$set': {'state': 'error', 'last_error': _err,
                              'at': utcnow()}}, upsert=True))
            except Exception:
                pass
            await asyncio.sleep(10)
