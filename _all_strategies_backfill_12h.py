"""🔁 UNIFIED BACKFILL за 12h — проходит каждый ST flip через ВСЕ 5
детекторов (volume_surge / triple_confluence / vol_accum / volcano /
second_flip) и сохраняет пропущенные.

Использует production детекторы as-is (новые константы Volcano и т.д.).
Дедуп: skip если такой signal уже есть в new_strategy_signals.
"""
import asyncio
import sys
from datetime import datetime, timedelta
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')


async def main():
    from pymongo import MongoClient
    from new_strategies import (
        detect_volume_surge, detect_volume_accum, detect_triple_confluence,
        detect_volcano_breakout, detect_second_flip,
        STRATEGY_EMOJI, STRATEGY_LABEL,
    )
    from exchange import get_klines_any

    uri = 'mongodb+srv://jameswood_db_user:P4bJfNu63sMVnRPG@cluster0.rawzyzc.mongodb.net/?appName=Cluster0'
    client = MongoClient(uri, serverSelectionTimeoutMS=15000)
    db = client['tradium']
    now = datetime.utcnow()

    since = now - timedelta(hours=12)
    print(f'Window: {since} to {now}')

    # Все ST flips за 12h, дедуп по (pair, flip_at, direction)
    flips_raw = list(db.supertrend_signals.find({
        'flip_at': {'$gte': since}
    }).sort('flip_at', 1))
    seen = set()
    flips = []
    for st in flips_raw:
        k = (st.get('pair_norm'), st['flip_at'], st['direction'])
        if k in seen: continue
        seen.add(k)
        flips.append(st)
    print(f'Unique ST flips за 12h: {len(flips)}')

    # Existing signals для skip dups
    existing_keys = set()
    for v in db.new_strategy_signals.find({
        'created_at': {'$gte': since - timedelta(hours=2)},
    }):
        # key = (pair, st_flip_at, strategy)
        st_flip = v.get('st_flip_at')
        existing_keys.add((v.get('pair'), st_flip, v.get('strategy')))
    print(f'Existing signals (skip dup): {len(existing_keys)}')

    triggered_all = []
    skipped_no_klines = 0
    progress_n = 0
    batch_size = 50

    for i, st in enumerate(flips, 1):
        pair_norm = st.get('pair_norm')
        pair = pair_norm[:-4] + '/USDT' if pair_norm.endswith('USDT') else pair_norm
        flip_at = st['flip_at']
        flip_at_naive = flip_at.replace(tzinfo=None) if flip_at.tzinfo else flip_at
        direction = st['direction']
        entry = st.get('entry_price')
        sl = st.get('sl_price')
        tier = st.get('tier', 'mtf')
        signal_id = st.get('id')

        if not entry or not sl:
            continue

        # Fetch klines (50 баров достаточно для всех детекторов)
        try:
            candles = await asyncio.to_thread(get_klines_any, pair, '1h', 50)
        except Exception as e:
            skipped_no_klines += 1
            continue

        # Detect each strategy independently
        local_triggered = []

        # 🌊 Volume Surge
        try:
            vs = detect_volume_surge(pair, direction, entry, sl, candles)
            if vs and (pair, flip_at_naive, 'volume_surge') not in existing_keys:
                local_triggered.append(vs)
        except Exception: pass

        # 🔋 Volume Accum
        try:
            va = detect_volume_accum(pair, direction, entry, sl, candles)
            if va and (pair, flip_at_naive, 'vol_accum') not in existing_keys:
                local_triggered.append(va)
        except Exception: pass

        # 🌋 Volcano
        try:
            vc = detect_volcano_breakout(
                pair, direction, entry, sl, candles,
                tier=tier, flip_hour_utc=flip_at.hour,
            )
            if vc and (pair, flip_at_naive, 'volcano') not in existing_keys:
                local_triggered.append(vc)
        except Exception: pass

        # ♻️ Second Flip (DB query)
        try:
            sf = detect_second_flip(pair, direction, entry, sl, flip_at, tier)
            if sf and (pair, flip_at_naive, 'second_flip') not in existing_keys:
                local_triggered.append(sf)
        except Exception: pass

        # 🐉 Triple Confluence (DB query)
        try:
            tc = detect_triple_confluence(pair, direction, entry, sl, flip_at)
            if tc and (pair, flip_at_naive, 'triple_confluence') not in existing_keys:
                local_triggered.append(tc)
        except Exception: pass

        for sig in local_triggered:
            triggered_all.append({
                'sig': sig,
                'flip_at': flip_at_naive,
                'st_signal_id': signal_id,
                'st_tier': tier,
            })

        progress_n += 1
        if progress_n % batch_size == 0:
            print(f'  Progress: {progress_n}/{len(flips)} flips, '
                  f'{len(triggered_all)} signals so far')

    print(f'\\n=== Total triggered: {len(triggered_all)} ===')
    by_strat = defaultdict(int)
    for t in triggered_all:
        by_strat[t['sig']['strategy']] += 1
    for k, v in sorted(by_strat.items()):
        em = STRATEGY_EMOJI.get(k, '?')
        lbl = STRATEGY_LABEL.get(k, k)
        print(f'  {em} {lbl}: {v}')
    print(f'Skipped no_klines: {skipped_no_klines}')

    if not triggered_all:
        print('No new signals to insert.')
        return

    # Save to DB
    print()
    print('Inserting to new_strategy_signals...')
    inserted = 0
    failed = 0
    for t in triggered_all:
        sig = t['sig']
        # Final dedup check (60 min window) — на всякий случай
        cutoff = t['flip_at'] - timedelta(minutes=60)
        existing = db.new_strategy_signals.find_one({
            'pair': sig['pair'],
            'direction': sig['direction'],
            'strategy': sig['strategy'],
            'created_at': {'$gte': cutoff},
        })
        if existing:
            continue
        doc = {
            **sig,
            'state': 'WAITING',
            'st_flip_at': t['flip_at'],
            'st_signal_id': t['st_signal_id'],
            'st_tier': t['st_tier'],
            'created_at': t['flip_at'],
            'updated_at': datetime.utcnow(),
            'backfilled': True,
        }
        try:
            db.new_strategy_signals.insert_one(doc)
            inserted += 1
        except Exception as e:
            failed += 1

    print(f'\\n=== Inserted: {inserted}, failed: {failed} ===')

    # Final breakdown
    print()
    print('Final state — last 12h all strategies:')
    by_strat_final = defaultdict(int)
    for v in db.new_strategy_signals.find({'created_at': {'$gte': since}}):
        by_strat_final[v.get('strategy')] += 1
    for k, v in sorted(by_strat_final.items()):
        em = STRATEGY_EMOJI.get(k, '?')
        print(f'  {em} {k}: {v}')


if __name__ == '__main__':
    asyncio.run(main())
