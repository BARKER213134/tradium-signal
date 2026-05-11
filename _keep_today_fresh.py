"""Keep cluster_delta cache hot для today's сигналов через локальный IP.

Запускается локально (НЕ на Railway). Цикл:
- Каждые 30с фетчит свежие сигналы из Mongo (last 1h)
- Для каждого (pair, signal_candle_open_ms) без delta — фетчит klines
- Пишет в cluster_delta
- Также top-up'ит свежие свечи всех активных пар (last 30min)
"""
import time
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
load_dotenv(override=True)

from database import _get_db
from delta_calculator import (
    bulk_fill_pair_history, _normalize_symbol, _candle_open_ms,
    _delta_from_klines_batch,
)


def fetch_fresh_signals(db, hours=1):
    """Возвращает уникальные (pair, at_ts_seconds) свежих сигналов."""
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    pairs_ts = set()
    # supertrend_signals
    for s in db.supertrend_signals.find({'flip_at': {'$gte': since}},
                                         {'pair': 1, 'flip_at': 1}).limit(500):
        if s.get('pair') and s.get('flip_at'):
            pairs_ts.add((s['pair'], int(s['flip_at'].timestamp())))
    # cv_flip_signals
    for s in db.cv_flip_signals.find({'created_at': {'$gte': since}},
                                      {'pair': 1, 'created_at': 1}).limit(500):
        if s.get('pair') and s.get('created_at'):
            pairs_ts.add((s['pair'], int(s['created_at'].timestamp())))
    # signals (CV/paper/cluster/etc) — used received_at AND pattern_triggered_at
    # Cryptovizor использует pattern_triggered_at (когда DCA триггернулся),
    # received_at — это когда монета добавлена в watch (может быть дни назад)
    for s in db.signals.find({'received_at': {'$gte': since}},
                              {'pair': 1, 'received_at': 1}).limit(1000):
        if s.get('pair') and s.get('received_at'):
            pairs_ts.add((s['pair'], int(s['received_at'].timestamp())))
    for s in db.signals.find({'pattern_triggered': True,
                                'pattern_triggered_at': {'$gte': since}},
                              {'pair': 1, 'pattern_triggered_at': 1}).limit(1000):
        if s.get('pair') and s.get('pattern_triggered_at'):
            pairs_ts.add((s['pair'], int(s['pattern_triggered_at'].timestamp())))
    # confluence
    for s in db.confluence.find({'detected_at': {'$gte': since}},
                                 {'symbol': 1, 'detected_at': 1}).limit(500):
        sym = s.get('symbol', '')
        if sym and sym.endswith('USDT') and not sym.endswith('/USDT'):
            pair = sym.replace('USDT', '/USDT')
            if s.get('detected_at'):
                pairs_ts.add((pair, int(s['detected_at'].timestamp())))
    # anomalies
    for s in db.anomalies.find({'detected_at': {'$gte': since}},
                                {'symbol': 1, 'detected_at': 1}).limit(500):
        sym = s.get('symbol', '')
        if sym and sym.endswith('USDT') and not sym.endswith('/USDT'):
            pair = sym.replace('USDT', '/USDT')
            if s.get('detected_at'):
                pairs_ts.add((pair, int(s['detected_at'].timestamp())))
    # new_strategy_signals
    for s in db.new_strategy_signals.find({'created_at': {'$gte': since}},
                                           {'pair': 1, 'st_flip_at': 1,
                                            'created_at': 1}).limit(500):
        flip = s.get('st_flip_at') or s.get('created_at')
        if s.get('pair') and flip:
            pairs_ts.add((s['pair'], int(flip.timestamp())))
    return pairs_ts


def fill_one_pair_today(pair, ts_seconds_list):
    """Для пары, фетчит klines покрывающие все signal candles + резонанс buffer.
    Возвращает количество записанных свечей."""
    if not ts_seconds_list:
        return 0
    sym = _normalize_symbol(pair)
    if not sym:
        return 0
    min_ts = min(ts_seconds_list)
    max_ts = max(ts_seconds_list)
    # Padding: 5h до min для резонанса, до now после max
    start_ms = (min_ts - 5 * 3600) * 1000
    end_ms = int(time.time() * 1000) + 60 * 1000  # +1min after now
    written = 0
    try:
        from database import _get_db
        col = _get_db().cluster_delta
        from pymongo import UpdateOne
        for tf in ('15m', '1h'):
            candles = _delta_from_klines_batch(sym, tf, start_ms, end_ms)
            if not candles:
                continue
            now_dt = datetime.now(timezone.utc)
            ops = [
                UpdateOne(
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
                )
                for c in candles
            ]
            if ops:
                col.bulk_write(ops, ordered=False)
                written += len(ops)
    except Exception as e:
        return -1
    return written


def main(loop=True, interval_sec=60):
    db = _get_db()
    print(f"Hot-fill started (loop={loop}, interval={interval_sec}s, throttled)")
    while True:
        try:
            t0 = time.time()
            # Только last 30 min (новые) + skip пары которые уже свежие в кэше
            pairs_ts = fetch_fresh_signals(db, hours=1)
            # Filter out pairs that already have FRESH cache (< 30 min old)
            from delta_calculator import _candle_open_ms
            cd = db.cluster_delta
            now_ms = int(time.time() * 1000)
            cutoff_ms = now_ms - 30 * 60 * 1000  # 30 min freshness threshold
            filtered = []
            for (pair, ts) in pairs_ts:
                # Check if signal candle (15m bucket) is already in cache
                sig_open = _candle_open_ms(ts * 1000, '15m')
                existing = cd.find_one(
                    {'pair': pair, 'tf': '15m', 'open_ms': sig_open},
                    {'_id': 1}
                )
                if not existing:
                    filtered.append((pair, ts))
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Fresh: {len(pairs_ts)}, "
                  f"need fetch: {len(filtered)}")
            by_pair = defaultdict(list)
            for (pair, ts) in filtered:
                by_pair[pair].append(ts)
            unique_pairs = list(by_pair.keys())
            if not unique_pairs:
                print(f"  all in cache, skip")
                if not loop:
                    break
                time.sleep(interval_sec)
                continue
            print(f"  unique pairs: {len(unique_pairs)}")
            written_total = 0
            errors = 0
            # ─── Throttled: 5 workers + sleep 0.5s каждые 10 пар ───
            with ThreadPoolExecutor(max_workers=5) as ex:
                futs = {ex.submit(fill_one_pair_today, p, by_pair[p]): p
                        for p in unique_pairs}
                done_count = 0
                for f in as_completed(futs):
                    pair = futs[f]
                    written = f.result()
                    if written < 0:
                        errors += 1
                    else:
                        written_total += written
                    done_count += 1
                    if done_count % 10 == 0:
                        time.sleep(0.5)  # throttle
            elapsed = time.time() - t0
            print(f"  candles written: {written_total} in {elapsed:.1f}s, errors={errors}")
        except Exception as e:
            print(f"  CYCLE ERROR: {e}")
        if not loop:
            break
        time.sleep(interval_sec)


if __name__ == '__main__':
    import sys
    loop = '--once' not in sys.argv
    main(loop=loop, interval_sec=30)
