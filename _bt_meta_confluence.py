"""META-STRATEGY: confluence count across ALL signal sources.

Hypothesis: чем больше независимых источников согласились на направление
в коротком окне — тем выше WR и edge.

Подход:
1. Для каждого ST flip (VIP/MTF/Daily) собираем confluence count:
   - сколько других сигналов того же direction в окне [-6h, +1h]
2. Симулируем сделку: entry = ST entry, SL = ST band, TP = 2R fixed
3. Группируем по confluence count → смотрим WR vs count

Источники для confluence:
  - Cryptovizor (pattern_triggered)
  - Tradium (pattern_triggered)
  - Volume Surge / Triple Confluence / Vol Accum (new_strategy_signals)
  - Confluence engine (score >= 4)
  - Anomaly (pump_score >= 70)
"""
import os
import sys
from datetime import datetime, timedelta
from collections import defaultdict
from pymongo import MongoClient, DESCENDING

sys.stdout.reconfigure(encoding='utf-8')

uri = 'mongodb+srv://jameswood_db_user:P4bJfNu63sMVnRPG@cluster0.rawzyzc.mongodb.net/?appName=Cluster0'
client = MongoClient(uri, serverSelectionTimeoutMS=15000)
db = client['tradium']

# Window для confluence: -6 hours до ST flip + 1 hour after
CONFLUENCE_WINDOW_BEFORE = timedelta(hours=6)
CONFLUENCE_WINDOW_AFTER = timedelta(hours=1)


def load_all_signals(since):
    """Load all signal types into dict: {(pair, direction, dt): [sources]}"""
    print('Loading signals...')

    # ST flips — основа (entry points)
    st_signals = list(db.supertrend_signals.find({
        'flip_at': {'$gte': since},
    }).sort('flip_at', 1))
    print(f'  ST flips: {len(st_signals)}')

    # CV pattern triggered
    cv_pat = list(db.signals.find({
        'source': 'cryptovizor',
        'pattern_triggered': True,
        'pattern_triggered_at': {'$gte': since},
    }, {'pair':1,'direction':1,'pattern_triggered_at':1,'ai_score':1}))
    print(f'  CV patterns: {len(cv_pat)}')

    # Tradium pattern triggered
    tr_pat = list(db.signals.find({
        'source': 'tradium',
        'pattern_triggered': True,
    }, {'pair':1,'direction':1,'pattern_triggered_at':1,'received_at':1}))
    print(f'  Tradium patterns: {len(tr_pat)}')

    # New strategies
    ns_sig = list(db.new_strategy_signals.find({
        'created_at': {'$gte': since},
    }, {'pair':1,'direction':1,'created_at':1,'strategy':1}))
    print(f'  New strategies: {len(ns_sig)}')

    # Confluence (score >= 4)
    cf_sig = list(db.confluence.find({
        'detected_at': {'$gte': since},
        'score': {'$gte': 4},
    }, {'pair':1,'direction':1,'detected_at':1,'score':1,'strength':1}))
    print(f'  Confluence (score≥4): {len(cf_sig)}')

    # Anomaly (pump >= 70)
    an_sig = list(db.anomalies.find({
        'detected_at': {'$gte': since},
        'pump_score': {'$gte': 70},
    }, {'pair':1,'direction':1,'detected_at':1,'score':1}))
    print(f'  Anomalies (pump≥70): {len(an_sig)}')

    return {
        'st': st_signals,
        'cv': cv_pat,
        'tradium': tr_pat,
        'new_strat': ns_sig,
        'confluence': cf_sig,
        'anomaly': an_sig,
    }


def build_confluence(st_flip, all_sigs):
    """Для одного ST flip считаем confluence count + список источников.
    Окно: [flip_at - 6h, flip_at + 1h], same direction, same pair."""
    pair = st_flip.get('pair_norm', '').replace('USDT', '/USDT')
    direction = st_flip['direction']
    flip_at = st_flip['flip_at']
    window_start = flip_at - CONFLUENCE_WINDOW_BEFORE
    window_end = flip_at + CONFLUENCE_WINDOW_AFTER

    sources = set()
    sources.add('st')  # ST сам = базовый источник

    # CV
    for s in all_sigs['cv']:
        if (s.get('pair') == pair and s.get('direction') == direction
                and window_start <= s['pattern_triggered_at'] <= window_end):
            sources.add('cv')
            break

    # Tradium
    for s in all_sigs['tradium']:
        ts = s.get('pattern_triggered_at') or s.get('received_at')
        if (s.get('pair') == pair and s.get('direction') == direction
                and ts and window_start <= ts <= window_end):
            sources.add('tradium')
            break

    # New strategies (3 типа считаем отдельно)
    for s in all_sigs['new_strat']:
        if (s.get('pair') == pair and s.get('direction') == direction
                and window_start <= s['created_at'] <= window_end):
            strat = s.get('strategy', '')
            if strat == 'volume_surge':
                sources.add('vol_surge')
            elif strat == 'triple_confluence':
                sources.add('triple_conf')
            elif strat == 'vol_accum':
                sources.add('vol_accum')

    # Confluence
    for s in all_sigs['confluence']:
        if (s.get('pair') == pair and s.get('direction') == direction
                and window_start <= s['detected_at'] <= window_end):
            sources.add('conf')
            break

    # Anomaly
    for s in all_sigs['anomaly']:
        if (s.get('pair') == pair and s.get('direction') == direction
                and window_start <= s['detected_at'] <= window_end):
            sources.add('anom')
            break

    return sources


def fetch_klines_for_pairs(pairs):
    """Use cached klines from _bt_klines_cache.json."""
    import json
    with open('_bt_klines_cache.json') as f:
        cache = json.load(f)
    return cache


def simulate_trade(candles, entry_dt, entry_price, sl_price, tp_r=2.0,
                    direction='LONG', max_bars=72):
    """Find entry candle and simulate forward."""
    entry_ts_ms = int(entry_dt.timestamp() * 1000)
    entry_idx = None
    for i, c in enumerate(candles):
        if c['t'] >= entry_ts_ms:
            entry_idx = i
            break
    if entry_idx is None or entry_idx >= len(candles) - 5:
        return None

    if direction == 'LONG':
        risk = entry_price - sl_price
        if risk <= 0:
            return None
        tp_price = entry_price + risk * tp_r
    else:
        risk = sl_price - entry_price
        if risk <= 0:
            return None
        tp_price = entry_price - risk * tp_r

    end = min(len(candles), entry_idx + max_bars + 1)
    for j in range(entry_idx + 1, end):
        c = candles[j]
        if direction == 'LONG':
            if c['l'] <= sl_price:
                return ('LOSS', -1.0)
            if c['h'] >= tp_price:
                return ('WIN', tp_r)
        else:
            if c['h'] >= sl_price:
                return ('LOSS', -1.0)
            if c['l'] <= tp_price:
                return ('WIN', tp_r)
    last = candles[end - 1]
    if direction == 'LONG':
        r = (last['c'] - entry_price) / risk
    else:
        r = (entry_price - last['c']) / risk
    return ('OPEN', r)


def main():
    # Last 14 days
    since = datetime.utcnow() - timedelta(days=14)

    sigs = load_all_signals(since)
    print()
    print('Loading klines cache...')
    klines = fetch_klines_for_pairs([])
    print(f'Cached pairs: {len(klines)}')
    print()

    # Group by confluence count
    by_count = defaultdict(list)  # count → trades
    by_sources = defaultdict(list)  # frozenset(sources) → trades
    by_tier_count = defaultdict(list)  # (tier, count) → trades

    skipped_no_klines = 0
    skipped_invalid = 0
    processed = 0

    for st in sigs['st']:
        pair_norm = st.get('pair_norm', '')
        if pair_norm not in klines:
            skipped_no_klines += 1
            continue
        candles = klines[pair_norm]
        if len(candles) < 30:
            continue

        sources = build_confluence(st, sigs)
        count = len(sources)
        tier = st.get('tier', 'mtf')

        sl = st.get('sl_price')
        entry = st.get('entry_price')
        if not sl or not entry:
            skipped_invalid += 1
            continue

        result = simulate_trade(
            candles, st['flip_at'], entry, sl,
            tp_r=2.0, direction=st['direction'], max_bars=72,
        )
        if result is None:
            continue

        outcome, r = result
        trade = {
            'pair': pair_norm, 'direction': st['direction'],
            'tier': tier, 'count': count, 'sources': sources,
            'outcome': outcome, 'r': r,
        }
        by_count[count].append(trade)
        by_sources[frozenset(sources)].append(trade)
        by_tier_count[(tier, count)].append(trade)
        processed += 1

    print(f'Processed: {processed} | no_klines: {skipped_no_klines} | '
          f'invalid: {skipped_invalid}')

    # ── Print stats by confluence count ────
    print()
    print('=' * 60)
    print('CONFLUENCE COUNT vs WR / Expectancy')
    print('=' * 60)
    print(f'{"Count":>6} {"N":>5} {"WR":>7} {"E (R)":>7} {"BE":>5} {"Edge":>6}')
    print('-' * 50)
    BE = 1/3 * 100  # TP=2R → BE WR = 33%
    for cnt in sorted(by_count.keys()):
        trades = by_count[cnt]
        n = len(trades)
        wins = sum(1 for t in trades if t['outcome'] == 'WIN')
        wr = wins/n*100
        e = sum(t['r'] for t in trades)/n
        edge = wr - BE
        print(f'{cnt:>6} {n:>5} {wr:>6.1f}% {e:>+6.2f}R '
              f'{BE:>4.0f}% {edge:>+5.1f}%')

    # ── Stats by tier × count ────
    print()
    print('=' * 60)
    print('TIER × COUNT breakdown (top combos by N)')
    print('=' * 60)
    print(f'{"Tier":>6} {"Cnt":>4} {"N":>5} {"WR":>7} {"E (R)":>7}')
    rows = []
    for (tier, count), trades in by_tier_count.items():
        if len(trades) < 5:
            continue
        n = len(trades)
        wins = sum(1 for t in trades if t['outcome'] == 'WIN')
        wr = wins/n*100
        e = sum(t['r'] for t in trades)/n
        rows.append((tier, count, n, wr, e))
    rows.sort(key=lambda r: -r[2])  # sort by N desc
    for tier, count, n, wr, e in rows[:20]:
        print(f'{tier:>6} {count:>4} {n:>5} {wr:>6.1f}% {e:>+6.2f}R')

    # ── Top source combinations ────
    print()
    print('=' * 60)
    print('TOP source combinations (N >= 10)')
    print('=' * 60)
    sources_rows = []
    for sources_set, trades in by_sources.items():
        if len(trades) < 10:
            continue
        n = len(trades)
        wins = sum(1 for t in trades if t['outcome'] == 'WIN')
        wr = wins/n*100
        e = sum(t['r'] for t in trades)/n
        sources_rows.append((sources_set, n, wr, e))
    sources_rows.sort(key=lambda r: -r[3])  # by E desc
    for s, n, wr, e in sources_rows[:15]:
        sources_str = ','.join(sorted(s))
        print(f'  N={n:>4} WR={wr:>5.1f}% E={e:>+5.2f}R | {sources_str}')

    print('\n=== DONE ===')


if __name__ == '__main__':
    main()
