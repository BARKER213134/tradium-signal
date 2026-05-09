"""Бектест alignment tier за СЕГОДНЯ.

Для каждого сигнала today:
1. Берёт delta_15m, delta_1h, resonance_15m, resonance_1h из cluster_delta cache
2. Вычисляет alignment tier: match / against / mixed (как в UI)
3. Через klines определяет outcome (TP/SL/timeout) в окне 6h после at_ts
4. Агрегирует winrate + AvgR per tier
"""
from __future__ import annotations
import time
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import httpx
from dotenv import load_dotenv
load_dotenv(override=True)

from database import _get_db, _signals
from delta_calculator import _normalize_symbol, _candle_open_ms

DAYS_LOOKBACK = 2  # бектест за 2 дня
OUTCOME_HOURS = 24  # окно исхода — 24ч (для slow movers)
http = httpx.Client(timeout=12.0,
                    limits=httpx.Limits(max_connections=8, max_keepalive_connections=4),
                    headers={"Accept-Encoding": "gzip"})


def fetch_klines_cdn(sym, tf, start_ms, end_ms):
    """Klines через Binance Vision CDN — полная OHLC, без банов.
    CSV формат: open_time,open,high,low,close,volume,close_time,...
    """
    import io, zipfile, csv
    from datetime import datetime, timezone, timedelta
    out = []
    today = datetime.now(timezone.utc).date()
    start_dt = datetime.fromtimestamp(start_ms/1000, tz=timezone.utc).date()
    end_dt = datetime.fromtimestamp(end_ms/1000, tz=timezone.utc).date()
    cur = start_dt
    while cur <= end_dt:
        if cur >= today:
            # CDN не имеет today — fallback REST
            try:
                day_start_ms = int(datetime(cur.year, cur.month, cur.day,
                                             tzinfo=timezone.utc).timestamp() * 1000)
                day_end_ms = day_start_ms + 24 * 3600 * 1000
                r = http.get("https://fapi.binance.com/fapi/v1/klines",
                             params={'symbol': sym, 'interval': tf,
                                     'startTime': max(day_start_ms, start_ms),
                                     'endTime': min(day_end_ms, end_ms),
                                     'limit': 1500})
                if r.status_code == 200:
                    out.extend(r.json())
            except Exception:
                pass
            cur += timedelta(days=1)
            continue
        # CDN
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
                            open_ms = int(row[0])
                            if open_ms < start_ms or open_ms > end_ms:
                                continue
                            # Convert to klines REST format: [open, open_p, high, low, close, vol, close_t, ...]
                            out.append([open_ms, row[1], row[2], row[3], row[4], row[5],
                                        int(row[6]), row[7], int(row[8]) if len(row)>8 else 0])
                        except Exception:
                            continue
        except Exception:
            pass
        cur += timedelta(days=1)
    return sorted(out, key=lambda k: int(k[0]))


def fetch_klines(sym, tf, start_ms, end_ms):
    """Wrapper: пробуем CDN первым (без банов)."""
    return fetch_klines_cdn(sym, tf, start_ms, end_ms)


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


def compute_alignment(s):
    """Returns (tier, score, total) или None."""
    d15 = s.get('delta_15m')
    d1h = s.get('delta_1h')
    r15 = s.get('resonance_15m')
    r1h = s.get('resonance_1h')
    if d15 is None and d1h is None and r15 is None and r1h is None:
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
    return (tier, aligned, total)


def load_today_signals():
    """Все сигналы за last DAYS_LOOKBACK дней."""
    db = _get_db()
    now = datetime.now(timezone.utc)
    start_today = now - timedelta(days=DAYS_LOOKBACK)
    out = []

    # Tradium pattern triggered today
    for s in _signals().find({
        'source': 'tradium', 'pattern_triggered': True,
        'pattern_triggered_at': {'$gte': start_today},
    }, {'pair':1,'direction':1,'entry':1,'tp1':1,'sl':1,
        'pattern_triggered_at':1}).limit(500):
        if s.get('entry') and s.get('sl') and s.get('tp1') and s.get('pair'):
            out.append({
                'source': 'tradium', 'pair': s['pair'], 'direction': s.get('direction'),
                'entry': float(s['entry']), 'sl': float(s['sl']), 'tp': float(s['tp1']),
                'at_ts': int(s['pattern_triggered_at'].timestamp()),
            })

    # Cryptovizor pattern_triggered today
    for s in _signals().find({
        'source': 'cryptovizor', 'pattern_triggered': True,
        'pattern_triggered_at': {'$gte': start_today},
    }, {'pair':1,'direction':1,'pattern_price':1,'entry':1,'dca1':1,'dca2':1,
        'pattern_triggered_at':1}).limit(2000):
        entry = s.get('pattern_price') or s.get('entry')
        if entry and s.get('dca1') and s.get('dca2') and s.get('pair'):
            out.append({
                'source': 'cryptovizor', 'pair': s['pair'], 'direction': s.get('direction'),
                'entry': float(entry), 'sl': float(s['dca1']), 'tp': float(s['dca2']),
                'at_ts': int(s['pattern_triggered_at'].timestamp()),
            })

    # Other signals collection (paper/cluster/verified/supertrend) today
    for s in _signals().find({
        'source': {'$in': ['paper', 'cluster', 'verified', 'supertrend']},
        'received_at': {'$gte': start_today},
    }, {'pair':1,'direction':1,'entry':1,'tp1':1,'sl':1,'received_at':1,'source':1}).limit(2000):
        if s.get('entry') and s.get('sl') and s.get('tp1') and s.get('pair'):
            out.append({
                'source': s['source'], 'pair': s['pair'], 'direction': s.get('direction'),
                'entry': float(s['entry']), 'sl': float(s['sl']), 'tp': float(s['tp1']),
                'at_ts': int(s['received_at'].timestamp()),
            })

    # New strategy signals today
    nss = db.new_strategy_signals
    for s in nss.find({'created_at': {'$gte': start_today}},
                       {'pair':1,'direction':1,'entry':1,'tp':1,'sl':1,
                        'created_at':1,'st_flip_at':1,'strategy':1}).limit(3000):
        if s.get('entry') and s.get('sl') and s.get('tp') and s.get('pair'):
            flip = s.get('st_flip_at') or s.get('created_at')
            out.append({
                'source': s.get('strategy', 'unknown'), 'pair': s['pair'],
                'direction': s.get('direction'),
                'entry': float(s['entry']), 'sl': float(s['sl']), 'tp': float(s['tp']),
                'at_ts': int(flip.timestamp()),
            })

    # SuperTrend signals today
    sts = db.supertrend_signals
    for s in sts.find({'flip_at': {'$gte': start_today}},
                       {'pair':1,'direction':1,'entry_price':1,'sl_price':1,
                        'flip_at':1,'tier':1}).limit(3000):
        entry = s.get('entry_price')
        sl = s.get('sl_price')
        if entry and sl and s.get('pair'):
            # Default TP = 1.5R from entry
            r_dist = abs(entry - sl)
            if (s.get('direction') or '').upper() == 'LONG':
                tp = entry + 1.5 * r_dist
            else:
                tp = entry - 1.5 * r_dist
            out.append({
                'source': 'supertrend',
                'tier': s.get('tier', '?'),
                'pair': s['pair'], 'direction': s.get('direction'),
                'entry': float(entry), 'sl': float(sl), 'tp': float(tp),
                'at_ts': int(s['flip_at'].timestamp()),
            })

    # CV flip signals today (♻️)
    for s in db.cv_flip_signals.find({'created_at': {'$gte': start_today}},
                                      {'pair':1,'direction':1,'entry':1,'tp':1,'sl':1,
                                       'created_at':1}).limit(2000):
        if s.get('entry') and s.get('sl') and s.get('tp') and s.get('pair'):
            out.append({
                'source': 'cv_flip', 'pair': s['pair'], 'direction': s.get('direction'),
                'entry': float(s['entry']), 'sl': float(s['sl']), 'tp': float(s['tp']),
                'at_ts': int(s['created_at'].timestamp()),
            })

    return out


def enrich_with_delta(signals):
    """Загружает delta_15m, delta_1h, resonance из cluster_delta cache."""
    from delta_calculator import _resonance_from_deltas, RESONANCE_BARS, TF_MINUTES
    db = _get_db()
    cd = db.cluster_delta
    # Bulk lookup
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
    CHUNK = 200
    for i in range(0, len(keys), CHUNK):
        conds = [{'pair': p, 'tf': t, 'open_ms': om} for (p,t,om) in keys[i:i+CHUNK]]
        for doc in cd.find({'$or': conds}, {'pair':1,'tf':1,'open_ms':1,'delta_pct':1,'_id':0}):
            cached[(doc['pair'], doc['tf'], doc['open_ms'])] = doc['delta_pct']
    # Apply to signals
    for s in signals:
        ats_ms = s['at_ts'] * 1000
        for tf in ('15m', '1h'):
            sig_open = _candle_open_ms(ats_ms, tf)
            d = cached.get((s['pair'], tf, sig_open))
            if d is not None:
                s[f'delta_{tf}'] = d
            # Резонанс: 5 свечей до signal
            deltas = []
            for j in range(RESONANCE_BARS - 1, -1, -1):
                bk = sig_open - j * TF_MINUTES[tf] * 60 * 1000
                d2 = cached.get((s['pair'], tf, bk))
                if d2 is not None:
                    deltas.append(d2)
            if deltas:
                s[f'resonance_{tf}'] = _resonance_from_deltas(deltas)


def main():
    print("\n" + "=" * 78)
    print(f"BACKTEST: ALIGNMENT TIER за {DAYS_LOOKBACK}d (outcome window {OUTCOME_HOURS}h)")
    print("=" * 78)
    t0 = time.time()
    signals = load_today_signals()
    print(f"Loaded {len(signals)} signals last {DAYS_LOOKBACK}d")
    if not signals:
        print("Empty — exit")
        return
    by_src = defaultdict(int)
    for s in signals:
        by_src[s['source']] += 1
    print(f"By source: {dict(by_src)}\n")

    print("Enriching with delta from cache...")
    enrich_with_delta(signals)
    with_delta = sum(1 for s in signals if s.get('delta_15m') is not None)
    print(f"  with delta from cache: {with_delta}/{len(signals)}\n")

    # Compute alignment per signal
    for s in signals:
        a = compute_alignment(s)
        s['_align'] = a

    no_align = sum(1 for s in signals if s.get('_align') is None)
    print(f"  no alignment data: {no_align}\n")

    # Group signals by pair for klines fetch
    print("Fetching outcomes via klines...")
    by_pair = defaultdict(list)
    for s in signals:
        by_pair[s['pair']].append(s)
    print(f"  unique pairs: {len(by_pair)}")

    processed = 0
    for pair, sigs in by_pair.items():
        sym = _normalize_symbol(pair)
        if not sym:
            continue
        ts_min = min(s['at_ts'] for s in sigs)
        ts_max = max(s['at_ts'] for s in sigs)
        start_ms = ts_min * 1000
        end_ms = (ts_max + (OUTCOME_HOURS + 1) * 3600) * 1000
        klines_15m = fetch_klines(sym, '15m', start_ms, end_ms)
        if not klines_15m:
            continue
        for s in sigs:
            ats_ms = s['at_ts'] * 1000
            outcome_start = _candle_open_ms(ats_ms, '15m') + 15 * 60 * 1000
            outcome_end = ats_ms + OUTCOME_HOURS * 3600 * 1000
            relevant = [k for k in klines_15m
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
            processed += 1
        if processed % 100 == 0:
            print(f"  processed {processed}/{len(signals)}", flush=True)
    print(f"  total processed: {processed} in {time.time()-t0:.0f}s\n")

    # ─── Aggregate by tier ───
    def stats(filtered, label):
        n = len(filtered)
        if n == 0:
            print(f"  {label}: empty")
            return
        wins = sum(1 for s in filtered if s.get('outcome') == 'tp')
        losses = sum(1 for s in filtered if s.get('outcome') == 'sl')
        timeouts = sum(1 for s in filtered if s.get('outcome') == 'timeout')
        decided = wins + losses
        wr = (wins / decided * 100) if decided else 0
        avg_r = (sum(s['r'] for s in filtered if s.get('r') is not None) /
                 decided) if decided else 0
        print(f"  {label:<28} N={n:4d}  wins={wins:3d}  losses={losses:3d}  "
              f"timeouts={timeouts:3d}  WR={wr:5.1f}%  AvgR={avg_r:+.2f}")

    print(f"{'='*78}")
    print(f"RESULTS")
    print(f"{'='*78}")
    print("\n── ALL signals (no filter) ──")
    stats(signals, "all")

    # By alignment tier (overall)
    print("\n── By alignment tier (any direction) ──")
    for tier in ('match', 'against', 'mixed', None):
        f = [s for s in signals if (s.get('_align') or [None])[0] == tier]
        stats(f, str(tier or 'no_data'))

    # By tier × direction (the actual UI grouping)
    print("\n── tier × direction (как в UI) ──")
    for direction in ('LONG', 'SHORT'):
        for tier in ('match', 'against', 'mixed'):
            f = [s for s in signals
                 if s.get('direction') == direction
                 and (s.get('_align') or [None])[0] == tier]
            color = ('🟢' if direction == 'LONG' and tier == 'match' else
                     '🔴' if direction == 'SHORT' and tier == 'match' else
                     '🟡')
            stats(f, f"{color} {direction} {tier}")

    # Per source × tier
    print("\n── source × tier ──")
    sources = sorted({s['source'] for s in signals})
    for src in sources:
        for tier in ('match', 'against', 'mixed'):
            f = [s for s in signals if s['source'] == src
                 and (s.get('_align') or [None])[0] == tier]
            if not f: continue
            stats(f, f"{src[:18]:<18} {tier}")


if __name__ == '__main__':
    main()
