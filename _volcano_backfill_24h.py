"""🌋 VOLCANO BACKFILL за 24h — пройти по всем ST flips и записать
сработавшие Volcano signals в new_strategy_signals.

Использует ту же логику что production (detect_volcano_breakout с новыми
константами lookback=8, RSI=999, accum_mult=0.8).

Получает klines пары из Binance Futures (как в exchange.py).
Дедупликация: пропускает если такой сигнал уже есть в БД (60 мин окно).
"""
import asyncio
import sys
from datetime import datetime, timedelta

sys.stdout.reconfigure(encoding='utf-8')


async def main():
    from pymongo import MongoClient, DESCENDING
    from new_strategies import (
        detect_volcano_breakout,
        VOLCANO_TIERS, VOLCANO_BAD_HOURS,
    )
    from exchange import get_klines_any

    uri = 'mongodb+srv://jameswood_db_user:P4bJfNu63sMVnRPG@cluster0.rawzyzc.mongodb.net/?appName=Cluster0'
    client = MongoClient(uri, serverSelectionTimeoutMS=15000)
    db = client['tradium']
    now = datetime.utcnow()

    # ST LONG MTF/Daily flips за 24ч
    since = now - timedelta(hours=24)
    print(f'Window: {since} to {now}')
    st_flips = list(db.supertrend_signals.find({
        'flip_at': {'$gte': since},
        'direction': 'LONG',
        'tier': {'$in': list(VOLCANO_TIERS)},
    }).sort('flip_at', 1))

    # Дедупликация по (pair, flip_at) — один flip может быть в разных tier'ах
    seen = set()
    unique_flips = []
    for st in st_flips:
        key = (st.get('pair_norm'), st['flip_at'])
        if key in seen:
            continue
        seen.add(key)
        unique_flips.append(st)
    print(f'Unique ST LONG MTF/Daily flips за 24h: {len(unique_flips)}')

    # Pre-check existing Volcano signals (для skip duplicates)
    existing_volcanoes = set()
    for v in db.new_strategy_signals.find({
        'strategy': 'volcano',
        'created_at': {'$gte': since - timedelta(hours=1)},
    }):
        existing_volcanoes.add((v.get('pair'), v.get('st_flip_at')))
    print(f'Existing Volcano signals: {len(existing_volcanoes)}')
    print()

    triggered = []
    skipped_no_klines = 0
    skipped_existing = 0

    for i, st in enumerate(unique_flips, 1):
        pair_norm = st.get('pair_norm')
        pair = pair_norm[:-4] + '/USDT' if pair_norm.endswith('USDT') else pair_norm
        flip_at = st['flip_at']
        flip_at_naive = flip_at.replace(tzinfo=None) if flip_at.tzinfo else flip_at

        # Skip duplicates
        if (pair, flip_at_naive) in existing_volcanoes:
            skipped_existing += 1
            continue

        # Fetch klines — нужно достаточно баров до flip_ts
        # flip_at = время открытия flip-свечи. Нужно minimum 31 бар ДО неё.
        # get_klines_any с limit=50 возвращает последние 50 свечей. Если flip
        # был 23h назад, нужно более старые. Используем endTime приём.
        try:
            # Просим больше баров чтобы захватить старые flips
            candles = await asyncio.to_thread(get_klines_any, pair, '1h', 200)
        except Exception as e:
            print(f'  [{i:>3}/{len(unique_flips)}] {pair_norm}: klines fail {e}')
            skipped_no_klines += 1
            continue

        if not candles or len(candles) < 31:
            skipped_no_klines += 1
            continue

        # Найти индекс flip candle (matches flip_at by open time)
        flip_ts_ms = int(flip_at_naive.timestamp() * 1000)
        flip_idx = None
        for idx, c in enumerate(candles):
            if c['t'] == flip_ts_ms:
                flip_idx = idx
                break
            if c['t'] > flip_ts_ms:
                # бар flip_at не нашли точно, берём предыдущий
                flip_idx = idx - 1 if idx > 0 else None
                break

        if flip_idx is None or flip_idx < 31:
            # Недостаточно истории до flip_at
            continue

        # Slice до flip candle включительно
        candles_at_flip = candles[:flip_idx + 1]

        # Запуск детектора с теми же параметрами что в production
        try:
            sig = detect_volcano_breakout(
                pair=pair,
                direction=st['direction'],
                entry=st.get('entry_price'),
                sl=st.get('sl_price'),
                candles_1h=candles_at_flip,
                tier=st.get('tier'),
                flip_hour_utc=flip_at.hour,
            )
        except Exception as e:
            print(f'  [{i:>3}/{len(unique_flips)}] {pair_norm}: detect fail {e}')
            continue

        if sig:
            triggered.append({
                'sig': sig,
                'flip_at': flip_at_naive,
                'st_signal_id': st.get('id'),
                'st_tier': st.get('tier'),
            })
            print(f'  [{i:>3}/{len(unique_flips)}] ✓ {pair_norm} '
                  f'@{flip_at} tier={st.get("tier")} '
                  f'vol={sig.get("vol_ratio")}× body={sig.get("body_atr")}× '
                  f'rsi={sig.get("rsi")}')
        else:
            # Quietly skip non-volcano flips
            pass

    print()
    print(f'=== Result: {len(triggered)} Volcano setups triggered ===')
    print(f'Skipped: existing={skipped_existing}, no_klines={skipped_no_klines}')

    if not triggered:
        print('No new Volcano signals to insert.')
        return

    # Save to new_strategy_signals
    print()
    print('Saving to new_strategy_signals collection...')
    inserted = 0
    for t in triggered:
        sig = t['sig']
        # Дедупликация (60 мин окно)
        cutoff = t['flip_at'] - timedelta(minutes=60)
        existing = db.new_strategy_signals.find_one({
            'pair': sig['pair'],
            'direction': sig['direction'],
            'strategy': sig['strategy'],
            'created_at': {'$gte': cutoff},
        })
        if existing:
            print(f'  SKIP dup: {sig["pair"]} @ {t["flip_at"]}')
            continue
        doc = {
            **sig,
            'state': 'WAITING',
            'st_flip_at': t['flip_at'],
            'st_signal_id': t['st_signal_id'],
            'st_tier': t['st_tier'],
            'created_at': t['flip_at'],   # backdate чтобы хронологически было правильно
            'updated_at': datetime.utcnow(),
            'backfilled': True,
        }
        try:
            db.new_strategy_signals.insert_one(doc)
            inserted += 1
            print(f'  ✓ INSERTED: {sig["pair"]} @ {t["flip_at"]}')
        except Exception as e:
            print(f'  ✗ FAIL {sig["pair"]}: {e}')

    print()
    print(f'=== {inserted} Volcano signals inserted to DB ===')
    print('Now they should appear in journal + dropdown filter "🌋 Volcano".')


if __name__ == '__main__':
    asyncio.run(main())
