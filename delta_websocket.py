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

logger = logging.getLogger(__name__)

WS_URL = "wss://fstream.binance.com/stream"
TIMEFRAMES = ('15m', '1h')


def _normalize_pair(symbol_upper: str) -> str:
    """BTCUSDT → BTC/USDT."""
    if symbol_upper.endswith('USDT'):
        return symbol_upper[:-4] + '/USDT'
    return symbol_upper


async def _persist_kline(pair: str, tf: str, kline: dict):
    """Пишет одну свечу из WS-сообщения в cluster_delta cache."""
    try:
        from database import _get_db
        # Binance kline event format:
        # k.t = open_ms, k.T = close_ms, k.v = volume base, k.V = taker buy base,
        # k.n = trades count, k.x = is_closed (true только если свеча закрылась)
        open_ms = int(kline.get('t', 0))
        if not open_ms:
            return
        volume = float(kline.get('v', 0))
        taker_buy = float(kline.get('V', 0))
        n_trades = int(kline.get('n', 0))
        if volume <= 0:
            return
        sell_vol = max(0.0, volume - taker_buy)
        delta_pct = (taker_buy - sell_vol) / volume * 100.0
        col = _get_db().cluster_delta
        col.update_one(
            {'pair': pair, 'tf': tf, 'open_ms': open_ms},
            {'$set': {
                'pair': pair, 'tf': tf, 'open_ms': open_ms,
                'delta_pct': round(delta_pct, 2),
                'buy_vol': round(taker_buy, 4),
                'sell_vol': round(sell_vol, 4),
                'n_trades': n_trades,
                'cached_at': datetime.now(timezone.utc),
                'source': 'ws',
            }},
            upsert=True,
        )
    except Exception as e:
        logger.debug(f'[delta-ws] persist {pair}/{tf}: {e}')


def _get_active_pairs(hours: int = 24) -> list[str]:
    """Возвращает список пар активных за last N часов из всех signal collections."""
    try:
        from database import _get_db
        from datetime import timedelta as _td
        db = _get_db()
        since = datetime.now(timezone.utc) - _td(hours=hours)
        pairs: set = set()
        for s in db.supertrend_signals.find({'flip_at': {'$gte': since}},
                                             {'pair': 1}).limit(2000):
            if s.get('pair'):
                pairs.add(s['pair'])
        for s in db.cv_flip_signals.find({'created_at': {'$gte': since}},
                                          {'pair': 1}).limit(2000):
            if s.get('pair'):
                pairs.add(s['pair'])
        for s in db.signals.find({'received_at': {'$gte': since}},
                                  {'pair': 1}).limit(3000):
            if s.get('pair'):
                pairs.add(s['pair'])
        for s in db.new_strategy_signals.find({'created_at': {'$gte': since}},
                                                {'pair': 1}).limit(2000):
            if s.get('pair'):
                pairs.add(s['pair'])
        return sorted(pairs)
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
            pairs = await asyncio.to_thread(_get_active_pairs, 24)
            if not pairs:
                logger.info('[delta-ws] no active pairs, sleep 5min')
                await asyncio.sleep(300)
                continue
            # Binance permits max 1024 streams per connection.
            # 500 pairs × 2 TF = 1000 streams — OK.
            # На всякий cap 500 пар.
            pairs = pairs[:500]
            streams = []
            for pair in pairs:
                sym = pair.replace('/', '').lower()
                for tf in TIMEFRAMES:
                    streams.append(f'{sym}@kline_{tf}')
            stream_param = '/'.join(streams)
            url = f'{WS_URL}?streams={stream_param}'
            if len(url) > 16000:  # URL length limit
                # Fallback на subscribe-after-connect
                url = WS_URL
                use_subscribe = True
            else:
                use_subscribe = False

            logger.info(f'[delta-ws] connecting: {len(pairs)} pairs, {len(streams)} streams')
            async with websockets.connect(url, ping_interval=180,
                                           ping_timeout=600,
                                           max_size=2**24,
                                           close_timeout=10) as ws:
                if use_subscribe:
                    msg = {
                        'method': 'SUBSCRIBE',
                        'params': streams,
                        'id': 1,
                    }
                    await ws.send(json.dumps(msg))
                logger.info(f'[delta-ws] connected, listening...')
                last_persist_log = time.time()
                count = 0
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=300)
                    except asyncio.TimeoutError:
                        logger.warning('[delta-ws] no messages 5min, reconnect')
                        break
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
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
                    # Persist every kline update (even unclosed) — даёт near-realtime delta
                    await _persist_kline(pair, tf, k)
                    count += 1
                    if time.time() - last_persist_log > 60:
                        logger.info(f'[delta-ws] persisted {count} klines/min')
                        last_persist_log = time.time()
                        count = 0
        except Exception as e:
            logger.warning(f'[delta-ws] connection error: {e}; reconnect in 10s')
            await asyncio.sleep(10)
