"""МАСТЕР-БЕКТЕСТ для дизайна автотрейдинговой стратегии.

Анализирует 14 дней сигналов со всеми features:
- source × direction × tier (alignment match/against/mixed)
- q_score buckets
- Hour-of-day × tier
- ST tier (VIP/MTF/Daily)
- Near level
- Combinations top-5

Outcomes считаются через Binance Vision CDN (полная OHLC, без банов).
Результаты пишутся в JSON для последующего использования в auto_strategy.py.
"""
from __future__ import annotations
import io
import json
import os
import time
import zipfile
import csv
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import httpx
from dotenv import load_dotenv
load_dotenv(override=True)

from database import _get_db, _signals
from delta_calculator import (
    _normalize_symbol, _candle_open_ms, RESONANCE_BARS, TF_MINUTES,
    _resonance_from_deltas,
)

# ─── Config ───────────────────────────────────────────────────────────
DAYS_LOOKBACK = 14
OUTCOME_HOURS = 24
http = httpx.Client(timeout=15.0,
                    limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
                    headers={"Accept-Encoding": "gzip"})

# ─── Klines via CDN (бан-устойчиво) ───
def fetch_klines_cdn(sym, tf, start_ms, end_ms):
    out = []
    today = datetime.now(timezone.utc).date()
    start_dt = datetime.fromtimestamp(start_ms/1000, tz=timezone.utc).date()
    end_dt = datetime.fromtimestamp(end_ms/1000, tz=timezone.utc).date()
    cur = start_dt
    while cur <= end_dt:
        if cur >= today:
            try:
                day_start = int(datetime(cur.year, cur.month, cur.day,
                                          tzinfo=timezone.utc).timestamp() * 1000)
                day_end = day_start + 24 * 3600 * 1000
                r = http.get("https://fapi.binance.com/fapi/v1/klines",
                             params={'symbol': sym, 'interval': tf,
                                     'startTime': max(day_start, start_ms),
                                     'endTime': min(day_end, end_ms),
                                     'limit': 1500}, timeout=10)
                if r.status_code == 200:
                    out.extend(r.json())
            except Exception:
                pass
            cur += timedelta(days=1)
            continue
        url = (f'https://data.binance.vision/data/futures/um/daily/klines/'
               f'{sym}/{tf}/{sym}-{tf}-{cur.strftime("%Y-%m-%d")}.zip')
        try:
            r = http.get(url, timeout=15)
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
                            out.append([o, row[1], row[2], row[3], row[4],
                                        row[5], int(row[6]), row[7]])
                        except Exception:
                            continue
        except Exception:
            pass
        cur += timedelta(days=1)
    return sorted(out, key=lambda k: int(k[0]))


def find_outcome(klines, entry, sl, tp, direction, start_ms):
    if not (entry and sl and tp):
        return ('unknown', None)
    is_long = (direction or '').upper() == 'LONG'
    for k in klines:
        try:
            open_ms = int(k[0])
            if open_ms < start_ms:
                continue
            high = float(k[2])
            low = float(k[3])
        except Exception:
            continue
        if is_long:
            if low <= sl:
                if high >= tp:
                    return ('tp', None)
                return ('sl', None)
            if high >= tp:
                return ('tp', None)
        else:
            if high >= sl:
                if low <= tp:
                    return ('tp', None)
                return ('sl', None)
            if low <= tp:
                return ('tp', None)
    return ('timeout', None)


# ─── Alignment tier ───
def compute_alignment(s):
    d15 = s.get('delta_15m')
    d1h = s.get('delta_1h')
    r15 = s.get('resonance_15m')
    r1h = s.get('resonance_1h')
    if all(v is None for v in (d15, d1h, r15, r1h)):
        return None
    direction = (s.get('direction') or '').upper()
    sgn = 1 if direction == 'LONG' else -1 if direction == 'SHORT' else 0
    if sgn == 0:
        return None
    aligned = against = total = 0
    for v in (d15, d1h, r15, r1h):
        if v is None or v == 0:
            continue
        total += 1
        if (v > 0) == (sgn > 0):
            aligned += 1
        else:
            against += 1
    if total < 2:
        return None
    if aligned >= total - 1 and aligned >= 2:
        tier = 'match'
    elif against >= total - 1 and against >= 2:
        tier = 'against'
    else:
        tier = 'mixed'
    return tier


# ─── Load signals ───
def load_signals(days):
    db = _get_db()
    since = datetime.now(timezone.utc) - timedelta(days=days)
    out = []
    seen_keys = set()
    def _add(item):
        # Dedup на уровне (pair, direction, source, at_ts // 60)
        key = (item.get('pair'), item.get('direction'),
               item.get('source'), item.get('at_ts', 0) // 60)
        if key in seen_keys:
            return
        seen_keys.add(key)
        out.append(item)

    # Tradium pattern triggered
    for s in _signals().find({'source': 'tradium', 'pattern_triggered': True,
                                'pattern_triggered_at': {'$gte': since}},
                              {'pair':1,'direction':1,'entry':1,'tp1':1,'sl':1,
                               'pattern_triggered_at':1,'is_top_pick':1,
                               'ai_score':1}).limit(2000):
        if s.get('entry') and s.get('sl') and s.get('tp1') and s.get('pair'):
            _add({
                'source': 'tradium', 'pair': s['pair'],
                'direction': s.get('direction'),
                'entry': float(s['entry']), 'sl': float(s['sl']),
                'tp': float(s['tp1']),
                'at_ts': int(s['pattern_triggered_at'].timestamp()),
                'is_top_pick': bool(s.get('is_top_pick')),
                'ai_score': s.get('ai_score') or 0,
            })

    # Cryptovizor pattern_triggered
    for s in _signals().find({'source': 'cryptovizor',
                                'pattern_triggered': True,
                                'pattern_triggered_at': {'$gte': since}},
                              {'pair':1,'direction':1,'pattern_price':1,
                               'entry':1,'dca1':1,'dca2':1,
                               'pattern_triggered_at':1,'pattern_name':1,
                               'is_top_pick':1,'ai_score':1}).limit(5000):
        entry = s.get('pattern_price') or s.get('entry')
        if entry and s.get('dca1') and s.get('dca2') and s.get('pair'):
            _add({
                'source': 'cryptovizor', 'pair': s['pair'],
                'direction': s.get('direction'),
                'entry': float(entry), 'sl': float(s['dca1']),
                'tp': float(s['dca2']),
                'at_ts': int(s['pattern_triggered_at'].timestamp()),
                'is_top_pick': bool(s.get('is_top_pick')),
                'ai_score': s.get('ai_score') or 0,
                'pattern_name': s.get('pattern_name', ''),
            })

    # Other signals collection
    for s in _signals().find({'source': {'$in': ['paper','cluster','verified','supertrend']},
                                'received_at': {'$gte': since}},
                              {'pair':1,'direction':1,'entry':1,'tp1':1,'sl':1,
                               'received_at':1,'source':1,'st_tier':1,
                               'is_top_pick':1}).limit(3000):
        if s.get('entry') and s.get('sl') and s.get('tp1') and s.get('pair'):
            _add({
                'source': s['source'], 'pair': s['pair'],
                'direction': s.get('direction'),
                'entry': float(s['entry']), 'sl': float(s['sl']),
                'tp': float(s['tp1']),
                'at_ts': int(s['received_at'].timestamp()),
                'st_tier': s.get('st_tier', ''),
                'is_top_pick': bool(s.get('is_top_pick')),
            })

    # SuperTrend signals (отдельная коллекция)
    sts = db.supertrend_signals
    for s in sts.find({'flip_at': {'$gte': since}},
                       {'pair':1,'direction':1,'entry_price':1,'sl_price':1,
                        'flip_at':1,'tier':1}).limit(15000):
        entry = s.get('entry_price'); sl = s.get('sl_price')
        if entry and sl and s.get('pair'):
            r_dist = abs(entry - sl)
            if (s.get('direction') or '').upper() == 'LONG':
                tp = entry + 1.5 * r_dist
            else:
                tp = entry - 1.5 * r_dist
            _add({
                'source': 'supertrend', 'st_tier': s.get('tier', ''),
                'pair': s['pair'], 'direction': s.get('direction'),
                'entry': float(entry), 'sl': float(sl), 'tp': float(tp),
                'at_ts': int(s['flip_at'].timestamp()),
            })

    # New strategy signals
    nss = db.new_strategy_signals
    for s in nss.find({'created_at': {'$gte': since}},
                       {'pair':1,'direction':1,'entry':1,'tp':1,'sl':1,
                        'created_at':1,'st_flip_at':1,'strategy':1,'st_tier':1,
                        'tp_R':1}).limit(10000):
        if s.get('entry') and s.get('sl') and s.get('tp') and s.get('pair'):
            flip = s.get('st_flip_at') or s.get('created_at')
            _add({
                'source': s.get('strategy', 'unknown'),
                'st_tier': s.get('st_tier', ''),
                'pair': s['pair'], 'direction': s.get('direction'),
                'entry': float(s['entry']), 'sl': float(s['sl']),
                'tp': float(s['tp']),
                'at_ts': int(flip.timestamp()),
                'tp_R': s.get('tp_R'),
            })

    # CV flip
    for s in db.cv_flip_signals.find({'created_at': {'$gte': since}},
                                      {'pair':1,'direction':1,'entry':1,
                                       'tp':1,'sl':1,'created_at':1}).limit(5000):
        if s.get('entry') and s.get('sl') and s.get('tp') and s.get('pair'):
            _add({
                'source': 'cv_flip', 'pair': s['pair'],
                'direction': s.get('direction'),
                'entry': float(s['entry']), 'sl': float(s['sl']),
                'tp': float(s['tp']),
                'at_ts': int(s['created_at'].timestamp()),
            })

    return out


# ─── Enrich with delta from cluster_delta cache ───
def enrich_with_delta(signals):
    db = _get_db()
    cd = db.cluster_delta
    keys_set = set()
    for s in signals:
        ats_ms = s['at_ts'] * 1000
        for tf in ('15m', '1h'):
            sig_open = _candle_open_ms(ats_ms, tf)
            for j in range(RESONANCE_BARS):
                bk = sig_open - j * TF_MINUTES[tf] * 60 * 1000
                keys_set.add((s['pair'], tf, bk))
    keys = list(keys_set)
    cached = {}
    CHUNK = 500
    for i in range(0, len(keys), CHUNK):
        conds = [{'pair':p,'tf':t,'open_ms':om} for (p,t,om) in keys[i:i+CHUNK]]
        for doc in cd.find({'$or': conds},
                            {'pair':1,'tf':1,'open_ms':1,'delta_pct':1,'_id':0}):
            cached[(doc['pair'], doc['tf'], doc['open_ms'])] = doc['delta_pct']
    for s in signals:
        ats_ms = s['at_ts'] * 1000
        for tf in ('15m', '1h'):
            sig_open = _candle_open_ms(ats_ms, tf)
            d = cached.get((s['pair'], tf, sig_open))
            if d is not None:
                s[f'delta_{tf}'] = d
            deltas = []
            for j in range(RESONANCE_BARS - 1, -1, -1):
                bk = sig_open - j * TF_MINUTES[tf] * 60 * 1000
                d2 = cached.get((s['pair'], tf, bk))
                if d2 is not None:
                    deltas.append(d2)
            if deltas:
                s[f'resonance_{tf}'] = _resonance_from_deltas(deltas)


# ─── Compute outcomes via CDN ───
def compute_outcomes(signals):
    by_pair = defaultdict(list)
    for s in signals:
        by_pair[s['pair']].append(s)
    print(f"  unique pairs: {len(by_pair)}")
    progress = [0]
    total = len(signals)

    def _process(pair):
        sym = _normalize_symbol(pair)
        if not sym:
            return
        sigs = by_pair[pair]
        ts_min = min(s['at_ts'] for s in sigs)
        ts_max = max(s['at_ts'] for s in sigs)
        start_ms = ts_min * 1000
        end_ms = (ts_max + (OUTCOME_HOURS + 1) * 3600) * 1000
        try:
            klines = fetch_klines_cdn(sym, '15m', start_ms, end_ms)
        except Exception:
            klines = []
        if not klines:
            return
        for s in sigs:
            ats_ms = s['at_ts'] * 1000
            outcome_start = _candle_open_ms(ats_ms, '15m') + 15 * 60 * 1000
            outcome_end = ats_ms + OUTCOME_HOURS * 3600 * 1000
            relevant = [k for k in klines
                        if outcome_start <= int(k[0]) <= outcome_end]
            outcome, _ = find_outcome(relevant, s['entry'], s['sl'], s['tp'],
                                       s.get('direction'), outcome_start)
            s['outcome'] = outcome
            try:
                if outcome == 'tp':
                    s['r'] = abs(s['tp'] - s['entry']) / abs(s['entry'] - s['sl'])
                elif outcome == 'sl':
                    s['r'] = -1.0
                else:
                    s['r'] = None
            except Exception:
                s['r'] = None
            progress[0] += 1
            if progress[0] % 200 == 0:
                print(f"  outcome processed: {progress[0]}/{total}", flush=True)

    with ThreadPoolExecutor(max_workers=12) as ex:
        list(ex.map(_process, list(by_pair.keys())))


# ─── Compute features (alignment, hour, ai_score_bucket, etc.) ───
def add_features(signals):
    for s in signals:
        s['_align'] = compute_alignment(s)
        if s.get('at_ts'):
            dt = datetime.fromtimestamp(s['at_ts'], tz=timezone.utc)
            s['_hour'] = dt.hour
            s['_weekday'] = dt.weekday()
        # AI score bucket
        ais = s.get('ai_score') or 0
        if ais >= 75: s['_ais_bucket'] = '75+'
        elif ais >= 60: s['_ais_bucket'] = '60-75'
        elif ais >= 40: s['_ais_bucket'] = '40-60'
        elif ais > 0: s['_ais_bucket'] = '<40'
        else: s['_ais_bucket'] = 'no_ai'


# ─── Stats helper ───
def stats(arr, label, min_n=5):
    n = len(arr)
    if n < min_n:
        return None
    wins = sum(1 for s in arr if s.get('outcome') == 'tp')
    losses = sum(1 for s in arr if s.get('outcome') == 'sl')
    timeouts = sum(1 for s in arr if s.get('outcome') == 'timeout')
    decided = wins + losses
    if decided < min_n:
        return None
    wr = wins / decided * 100
    avg_r = sum(s['r'] for s in arr if s.get('r') is not None) / decided
    expectancy = avg_r  # per decided trade
    return {
        'label': label, 'n': n, 'decided': decided,
        'wins': wins, 'losses': losses, 'timeouts': timeouts,
        'wr': round(wr, 1), 'avg_r': round(avg_r, 2),
        'expectancy': round(expectancy, 2),
    }


def print_stats(s):
    if not s: return
    print(f"  {s['label']:<48} N={s['n']:4d}  decided={s['decided']:4d}  "
          f"WR={s['wr']:5.1f}%  AvgR={s['avg_r']:+.2f}R")


# ─── Main analysis ───
def main():
    t0 = time.time()
    print(f"\n{'='*82}")
    print(f"МАСТЕР-БЕКТЕСТ для дизайна стратегии · {DAYS_LOOKBACK}d · outcome window {OUTCOME_HOURS}h")
    print(f"{'='*82}\n")

    print("STEP 1: Loading signals...")
    signals = load_signals(DAYS_LOOKBACK)
    print(f"  loaded: {len(signals)}")
    by_src = defaultdict(int)
    for s in signals:
        by_src[s['source']] += 1
    print(f"  by source: {dict(by_src)}\n")

    print("STEP 2: Enriching with cluster_delta cache...")
    enrich_with_delta(signals)
    with_d = sum(1 for s in signals if s.get('delta_15m') is not None)
    print(f"  delta filled: {with_d}/{len(signals)} ({with_d/len(signals)*100:.0f}%)\n")

    print("STEP 3: Computing alignment + features...")
    add_features(signals)
    align_counts = defaultdict(int)
    for s in signals:
        align_counts[s.get('_align') or 'no_data'] += 1
    print(f"  alignment: {dict(align_counts)}\n")

    print("STEP 4: Computing outcomes via CDN klines...")
    compute_outcomes(signals)
    decided = sum(1 for s in signals if s.get('outcome') in ('tp','sl'))
    print(f"  decided: {decided}/{len(signals)} ({decided/len(signals)*100:.0f}%)\n")

    print(f"\n{'='*82}")
    print(f"AGGREGATIONS")
    print(f"{'='*82}\n")

    # 1. Baseline
    print("── BASELINE (всё) ──")
    print_stats(stats(signals, 'all'))

    # 2. Per source
    print("\n── PER SOURCE ──")
    sources = sorted({s['source'] for s in signals})
    src_results = {}
    for src in sources:
        f = [s for s in signals if s['source'] == src]
        r = stats(f, f"src={src}")
        if r:
            src_results[src] = r
            print_stats(r)

    # 3. Per source × tier
    print("\n── SOURCE × TIER ──")
    bucket_results = []
    for src in sources:
        for tier in ('match', 'mixed', 'against', None):
            f = [s for s in signals if s['source'] == src and s.get('_align') == tier]
            r = stats(f, f"{src} | tier={tier or 'no_data'}")
            if r:
                r['source'] = src
                r['tier'] = tier or 'no_data'
                bucket_results.append(r)
                print_stats(r)

    # 4. Per source × tier × direction
    print("\n── SOURCE × TIER × DIRECTION ──")
    for src in sources:
        for direction in ('LONG', 'SHORT'):
            for tier in ('match', 'mixed', 'against'):
                f = [s for s in signals if s['source'] == src
                     and s.get('direction') == direction
                     and s.get('_align') == tier]
                r = stats(f, f"{src} | {direction} | tier={tier}")
                if r:
                    r['source'] = src; r['direction'] = direction; r['tier'] = tier
                    bucket_results.append(r)
                    print_stats(r)

    # 5. Hour analysis
    print("\n── HOUR OF DAY (UTC) — все сигналы ──")
    for h in range(24):
        f = [s for s in signals if s.get('_hour') == h]
        r = stats(f, f"hour={h:02d}", min_n=15)
        if r:
            r['hour'] = h
            print_stats(r)

    # 6. Weekday
    print("\n── WEEKDAY ──")
    days = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun']
    for d in range(7):
        f = [s for s in signals if s.get('_weekday') == d]
        r = stats(f, f"{days[d]}", min_n=15)
        if r: print_stats(r)

    # 7. ST tier (для supertrend)
    print("\n── SUPERTREND × TIER ──")
    for st_tier in ('vip','mtf','daily',''):
        for align_tier in ('match','mixed','against'):
            f = [s for s in signals if s['source'] == 'supertrend'
                 and (s.get('st_tier','') or '') == st_tier
                 and s.get('_align') == align_tier]
            r = stats(f, f"ST {st_tier or 'unknown'} | {align_tier}")
            if r: print_stats(r)

    # 8. Top edge buckets (sorted by expectancy, min decided=10)
    print(f"\n{'='*82}")
    print(f"TOP-15 EDGE BUCKETS (sorted by AvgR, min decided=15)")
    print(f"{'='*82}")
    eligible = [r for r in bucket_results if r.get('decided', 0) >= 15]
    eligible.sort(key=lambda x: -x['avg_r'])
    for r in eligible[:15]:
        print_stats(r)

    print(f"\n── BOTTOM-10 (avoid these) ──")
    for r in eligible[-10:]:
        print_stats(r)

    # Save results to JSON
    elapsed = time.time() - t0
    print(f"\n{'='*82}")
    print(f"DONE in {elapsed:.0f}s")
    print(f"{'='*82}")

    # Save raw + aggregated for strategy module
    raw_path = '_bt_master_results.json'
    summary = {
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'days_lookback': DAYS_LOOKBACK,
        'outcome_hours': OUTCOME_HOURS,
        'total_signals': len(signals),
        'decided': decided,
        'by_source': {src: src_results[src] for src in src_results},
        'top_edges': eligible[:30],
        'bottom_edges': eligible[-15:],
    }
    with open(raw_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"Saved summary to {raw_path}")

    # Также сохраним список сигналов с outcome для дальнейшего анализа
    detailed = []
    for s in signals:
        detailed.append({
            'source': s.get('source'), 'pair': s.get('pair'),
            'direction': s.get('direction'), 'at_ts': s.get('at_ts'),
            'entry': s.get('entry'), 'sl': s.get('sl'), 'tp': s.get('tp'),
            'st_tier': s.get('st_tier'), 'is_top_pick': s.get('is_top_pick'),
            'ai_score': s.get('ai_score'),
            'delta_15m': s.get('delta_15m'), 'delta_1h': s.get('delta_1h'),
            'resonance_15m': s.get('resonance_15m'),
            'resonance_1h': s.get('resonance_1h'),
            'align_tier': s.get('_align'), 'hour': s.get('_hour'),
            'weekday': s.get('_weekday'),
            'outcome': s.get('outcome'), 'r': s.get('r'),
        })
    with open('_bt_signals_detailed.json', 'w', encoding='utf-8') as f:
        json.dump(detailed, f, default=str)
    print(f"Saved detailed signals to _bt_signals_detailed.json (для дальнейшего анализа)")


if __name__ == '__main__':
    main()
