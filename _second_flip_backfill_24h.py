"""♻️ SECOND FLIP backfill за 24h.

Идёт по всем ST flips за 24h, для каждого вызывает detect_second_flip,
сохраняет triggered в new_strategy_signals (как Volcano backfill).
"""
import asyncio
import sys
from datetime import datetime, timedelta

sys.stdout.reconfigure(encoding='utf-8')


async def main():
    from pymongo import MongoClient
    from new_strategies import detect_second_flip, SECOND_FLIP_TIERS

    uri = 'mongodb+srv://jameswood_db_user:P4bJfNu63sMVnRPG@cluster0.rawzyzc.mongodb.net/?appName=Cluster0'
    client = MongoClient(uri, serverSelectionTimeoutMS=15000)
    db = client['tradium']
    now = datetime.utcnow()

    since = now - timedelta(hours=24)
    print(f'Window: {since} to {now}')

    # ST flips за 24h, MTF/Daily, обе direction (LONG и SHORT —
    # Second Flip может быть для любого direction).
    st_flips = list(db.supertrend_signals.find({
        'flip_at': {'$gte': since},
        'tier': {'$in': list(SECOND_FLIP_TIERS)},
    }).sort('flip_at', 1))

    # Дедуп по (pair, flip_at, direction)
    seen = set()
    unique = []
    for st in st_flips:
        k = (st.get('pair_norm'), st['flip_at'], st['direction'])
        if k in seen: continue
        seen.add(k)
        unique.append(st)
    print(f'Unique ST flips MTF/Daily за 24h: {len(unique)}')

    # Existing second_flip signals (skip dups)
    existing = set()
    for v in db.new_strategy_signals.find({
        'strategy': 'second_flip',
        'created_at': {'$gte': since - timedelta(hours=1)},
    }):
        existing.add((v.get('pair'), v.get('st_flip_at')))
    print(f'Existing Second Flip signals: {len(existing)}')

    triggered = []
    for i, st in enumerate(unique, 1):
        pair_norm = st.get('pair_norm')
        pair = pair_norm[:-4] + '/USDT' if pair_norm.endswith('USDT') else pair_norm
        flip_at = st['flip_at']
        flip_at_naive = flip_at.replace(tzinfo=None) if flip_at.tzinfo else flip_at

        if (pair, flip_at_naive) in existing:
            continue

        try:
            sig = detect_second_flip(
                pair=pair,
                direction=st['direction'],
                entry=st.get('entry_price'),
                sl=st.get('sl_price'),
                flip_ts=flip_at,
                tier=st.get('tier'),
            )
        except Exception as e:
            print(f'  [{i}/{len(unique)}] {pair_norm}: detect fail {e}')
            continue

        if sig:
            triggered.append({
                'sig': sig,
                'flip_at': flip_at_naive,
                'st_signal_id': st.get('id'),
                'st_tier': st.get('tier'),
            })
            pattern_str = 'L→S→L (strict)' if sig['strict_pattern'] else 'consecutive'
            print(f'  [{i:>3}/{len(unique)}] ✓ {pair_norm:<14} '
                  f'@{flip_at} {st["direction"]} tier={st["tier"]} '
                  f'gap={sig["gap_h"]}h {pattern_str}')

    print(f'\n=== Result: {len(triggered)} Second Flip setups ===')

    if not triggered:
        print('No new signals.')
        return

    # Save to db
    inserted = 0
    for t in triggered:
        sig = t['sig']
        cutoff = t['flip_at'] - timedelta(minutes=60)
        existing_db = db.new_strategy_signals.find_one({
            'pair': sig['pair'],
            'direction': sig['direction'],
            'strategy': 'second_flip',
            'created_at': {'$gte': cutoff},
        })
        if existing_db:
            print(f'  SKIP dup: {sig["pair"]} @ {t["flip_at"]}')
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
            print(f'  ✓ INSERTED: {sig["pair"]} @ {t["flip_at"]}')
        except Exception as e:
            print(f'  ✗ FAIL {sig["pair"]}: {e}')

    print(f'\n=== {inserted} Second Flip signals inserted ===')


if __name__ == '__main__':
    asyncio.run(main())
