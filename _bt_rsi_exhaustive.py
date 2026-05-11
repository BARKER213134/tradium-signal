"""ИСЧЕРПЫВАЮЩИЙ grid search: лучшая RSI комбинация для LONG и SHORT.

Тестирует:
1. Single TF × bucket (15m / 1h / 4h / 1d × 12 buckets)
2. Pairwise (1h + 4h, 15m + 1h, 4h + 1d)
3. Triple (15m + 1h + 4h)
4. + align_tier filter (match/mixed/against/any)
5. + source-specific и source-agnostic

Сохраняет enriched data в JSON для повторных запусков.

Цель: найти ОДНУ best rule для LONG, ОДНУ для SHORT с max AvgR
при min decided=30 (статистически значимо).
"""
from __future__ import annotations
import io, json, time, zipfile, csv, os
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import httpx
from dotenv import load_dotenv
load_dotenv(override=True)

http = httpx.Client(timeout=15.0,
                    limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
                    headers={"Accept-Encoding": "gzip"})

TFS = {
    '15m': 15 * 60 * 1000,
    '1h':  60 * 60 * 1000,
    '4h':  4 * 60 * 60 * 1000,
    '1d':  24 * 60 * 60 * 1000,
}
RSI_PERIOD = 14
ENRICHED_CACHE = '_bt_signals_rsi_enriched.json'


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
                                        row[5], int(row[6])])
                        except Exception:
                            continue
        except Exception:
            pass
        cur += timedelta(days=1)
    return sorted(out, key=lambda k: int(k[0]))


def compute_rsi(closes, period=14):
    if len(closes) < period + 1:
        return []
    gains, losses = [], []
    for i in range(1, period + 1):
        ch = closes[i] - closes[i - 1]
        gains.append(max(ch, 0)); losses.append(max(-ch, 0))
    avg_g = sum(gains) / period
    avg_l = sum(losses) / period
    rsis = []
    for i in range(period, len(closes)):
        ch = closes[i] - closes[i - 1]
        avg_g = (avg_g * (period - 1) + max(ch, 0)) / period
        avg_l = (avg_l * (period - 1) + max(-ch, 0)) / period
        if avg_l == 0:
            rsis.append(100.0)
        else:
            rsis.append(100.0 - 100.0 / (1 + avg_g / avg_l))
    return rsis


def normalize_pair(pair):
    if not pair: return ''
    p = pair.replace('/', '').replace('-', '').upper()
    if not p.endswith('USDT'): p = p + 'USDT'
    return p


def enrich_with_rsi(signals):
    """Загружает klines для всех TF и считает RSI на сигнальной свече."""
    by_pair = defaultdict(list)
    for s in signals:
        if s.get('pair'):
            by_pair[s['pair']].append(s)
    print(f"Unique pairs: {len(by_pair)}")
    progress = [0]
    total_pairs = len(by_pair)

    def _process(pair):
        sym = normalize_pair(pair)
        if not sym: return
        sigs = by_pair[pair]
        ts_min = min(s['at_ts'] for s in sigs)
        ts_max = max(s['at_ts'] for s in sigs)
        for tf, bucket_ms in TFS.items():
            # warmup
            warmup_h = {'15m': 5, '1h': 20, '4h': 80, '1d': 30 * 24}[tf]
            start_ms = (ts_min - warmup_h * 3600) * 1000
            end_ms = (ts_max + 4 * 3600) * 1000
            try:
                kl = fetch_klines_cdn(sym, tf, start_ms, end_ms)
            except Exception:
                kl = []
            if not kl: continue
            opens_ms = [int(k[0]) for k in kl]
            closes = [float(k[4]) for k in kl]
            rsis = compute_rsi(closes, RSI_PERIOD)
            if not rsis: continue
            rsi_by = {opens_ms[RSI_PERIOD + i]: rsis[i]
                      for i in range(len(rsis))}
            for s in sigs:
                ats_ms = s['at_ts'] * 1000
                sig_open_ms = (ats_ms // bucket_ms) * bucket_ms
                rsi = rsi_by.get(sig_open_ms)
                if rsi is None:
                    candidates = [k for k in opens_ms if k <= sig_open_ms and k in rsi_by]
                    if candidates:
                        rsi = rsi_by[max(candidates)]
                if rsi is not None:
                    s[f'_rsi_{tf}'] = rsi
        progress[0] += 1
        if progress[0] % 25 == 0:
            print(f"  {progress[0]}/{total_pairs}", flush=True)

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(_process, list(by_pair.keys())))


# ─── Grid search ───
def stats(arr):
    n = len(arr)
    if n < 30: return None
    decided = [s for s in arr if s.get('outcome') in ('tp', 'sl')]
    if len(decided) < 30: return None
    wins = sum(1 for s in decided if s.get('outcome') == 'tp')
    wr = wins / len(decided) * 100
    avg_r = sum(s.get('r', 0) for s in decided) / len(decided)
    return {'n': n, 'decided': len(decided), 'wins': wins,
            'wr': round(wr, 1), 'avg_r': round(avg_r, 2)}


BUCKETS = [
    ('any',   lambda r: True),
    ('<30',   lambda r: r is not None and r < 30),
    ('30-40', lambda r: r is not None and 30 <= r < 40),
    ('40-50', lambda r: r is not None and 40 <= r < 50),
    ('50-60', lambda r: r is not None and 50 <= r < 60),
    ('60-70', lambda r: r is not None and 60 <= r < 70),
    ('70-80', lambda r: r is not None and 70 <= r < 80),
    ('>80',   lambda r: r is not None and r >= 80),
    ('30-50', lambda r: r is not None and 30 <= r < 50),
    ('40-60', lambda r: r is not None and 40 <= r < 60),
    ('50-70', lambda r: r is not None and 50 <= r < 70),
    ('60-80', lambda r: r is not None and 60 <= r < 80),
]


def filter_signal(s, conditions):
    """conditions: list of (tf, bucket_fn) или [('tier', value), ('source', 'cv')]."""
    for cond_type, *args in conditions:
        if cond_type == 'rsi':
            tf, fn = args
            rsi = s.get(f'_rsi_{tf}')
            if not fn(rsi): return False
        elif cond_type == 'tier':
            if s.get('align_tier') != args[0]: return False
        elif cond_type == 'source':
            if s.get('source') != args[0]: return False
        elif cond_type == 'sources':
            if s.get('source') not in args[0]: return False
        elif cond_type == 'direction':
            if s.get('direction') != args[0]: return False
    return True


def main():
    t0 = time.time()
    print("Loading signals...")
    with open('_bt_signals_detailed.json') as f:
        signals = json.load(f)
    print(f"Total: {len(signals)}")

    # Try load cached enriched
    if os.path.exists(ENRICHED_CACHE):
        print(f"Loading cached enriched RSI from {ENRICHED_CACHE}...")
        with open(ENRICHED_CACHE) as f:
            cached = json.load(f)
        # Reattach RSI fields by matching (pair, at_ts)
        rsi_map = {(c['pair'], c['at_ts']): {
            f'_rsi_{tf}': c.get(f'_rsi_{tf}') for tf in TFS
        } for c in cached if c.get('pair') and c.get('at_ts')}
        for s in signals:
            key = (s.get('pair'), s.get('at_ts'))
            if key in rsi_map:
                for k, v in rsi_map[key].items():
                    if v is not None:
                        s[k] = v
        n_with = sum(1 for s in signals if s.get('_rsi_1h') is not None)
        print(f"  attached RSI to {n_with}/{len(signals)} signals from cache")
        if n_with < len(signals) * 0.95:
            print("  cache stale, re-enriching...")
            enrich_with_rsi(signals)
            # Save cache
            save_enriched_cache(signals)
    else:
        print("No cache, computing RSI for all TFs...")
        enrich_with_rsi(signals)
        save_enriched_cache(signals)

    for tf in TFS:
        n = sum(1 for s in signals if s.get(f'_rsi_{tf}') is not None)
        print(f"  RSI {tf}: {n}/{len(signals)} ({n/len(signals)*100:.0f}%)")
    print(f"Enrich done in {time.time()-t0:.0f}s")

    # ─── EXHAUSTIVE GRID SEARCH ───
    print("\nRunning exhaustive grid search...")
    t_grid = time.time()
    all_results = []

    for direction in ('LONG', 'SHORT'):
        dir_sigs = [s for s in signals if s.get('direction') == direction]

        # 1. Single TF × bucket (без других фильтров)
        for tf, bucket_ms in TFS.items():
            for bucket_label, bucket_fn in BUCKETS:
                arr = [s for s in dir_sigs if bucket_fn(s.get(f'_rsi_{tf}'))]
                st = stats(arr)
                if st:
                    all_results.append({
                        'direction': direction,
                        'rule': f'RSI_{tf}={bucket_label}',
                        'conditions': [('rsi', tf, bucket_label)],
                        **st,
                    })

        # 2. Pairwise: TF1 × bucket1 AND TF2 × bucket2 (только разные TF)
        tf_list = list(TFS.keys())
        for i in range(len(tf_list)):
            for j in range(i + 1, len(tf_list)):
                tf1, tf2 = tf_list[i], tf_list[j]
                for b1_lbl, b1_fn in BUCKETS:
                    if b1_lbl == 'any': continue
                    for b2_lbl, b2_fn in BUCKETS:
                        if b2_lbl == 'any': continue
                        arr = [s for s in dir_sigs
                               if b1_fn(s.get(f'_rsi_{tf1}'))
                               and b2_fn(s.get(f'_rsi_{tf2}'))]
                        st = stats(arr)
                        if st:
                            all_results.append({
                                'direction': direction,
                                'rule': f'RSI_{tf1}={b1_lbl} AND RSI_{tf2}={b2_lbl}',
                                'conditions': [('rsi', tf1, b1_lbl),
                                               ('rsi', tf2, b2_lbl)],
                                **st,
                            })

        # 3. + tier filter (для уточнения)
        for tier in ('match', 'mixed', 'against'):
            tier_sigs = [s for s in dir_sigs if s.get('align_tier') == tier]
            for tf, _ in TFS.items():
                for bucket_label, bucket_fn in BUCKETS:
                    arr = [s for s in tier_sigs if bucket_fn(s.get(f'_rsi_{tf}'))]
                    st = stats(arr)
                    if st:
                        all_results.append({
                            'direction': direction,
                            'rule': f'tier={tier} AND RSI_{tf}={bucket_label}',
                            'conditions': [('tier', tier), ('rsi', tf, bucket_label)],
                            **st,
                        })

    # Source-agnostic так что не добавляем source. Top combos автоматически
    # покажут наиболее доходные правила для LONG / SHORT.

    print(f"Tested {len(all_results)} combinations in {time.time()-t_grid:.0f}s")

    # ─── REPORT ───
    print(f"\n{'='*100}")
    print("ТОП-25 правил для LONG (sorted by AvgR, min decided=30)")
    print(f"{'='*100}")
    long_results = sorted([r for r in all_results if r['direction'] == 'LONG'],
                           key=lambda x: -x['avg_r'])
    print(f"{'#':>3} {'Rule':<60} {'N':>5} {'Dec':>5} {'WR':>7} {'AvgR':>8}")
    print('-' * 100)
    for i, r in enumerate(long_results[:25], 1):
        print(f"{i:>3} {r['rule']:<60} {r['n']:>5} {r['decided']:>5} {r['wr']:>6.1f}% {r['avg_r']:>+7.2f}R")

    print(f"\n{'='*100}")
    print("ТОП-25 правил для SHORT (sorted by AvgR, min decided=30)")
    print(f"{'='*100}")
    short_results = sorted([r for r in all_results if r['direction'] == 'SHORT'],
                            key=lambda x: -x['avg_r'])
    print(f"{'#':>3} {'Rule':<60} {'N':>5} {'Dec':>5} {'WR':>7} {'AvgR':>8}")
    print('-' * 100)
    for i, r in enumerate(short_results[:25], 1):
        print(f"{i:>3} {r['rule']:<60} {r['n']:>5} {r['decided']:>5} {r['wr']:>6.1f}% {r['avg_r']:>+7.2f}R")

    # ─── BEST для каждого direction с min sample 100 (более robust) ───
    print(f"\n{'='*100}")
    print("BEST RULE — min decided=100 (robust sample)")
    print(f"{'='*100}")
    long_robust = sorted([r for r in long_results if r['decided'] >= 100],
                          key=lambda x: -x['avg_r'])
    short_robust = sorted([r for r in short_results if r['decided'] >= 100],
                           key=lambda x: -x['avg_r'])
    print('\nLONG (min 100 decided):')
    for i, r in enumerate(long_robust[:5], 1):
        print(f"  {i}. {r['rule']:<60} N={r['n']:>4} dec={r['decided']:>4} WR={r['wr']:.1f}% AvgR={r['avg_r']:+.2f}R")
    print('\nSHORT (min 100 decided):')
    for i, r in enumerate(short_robust[:5], 1):
        print(f"  {i}. {r['rule']:<60} N={r['n']:>4} dec={r['decided']:>4} WR={r['wr']:.1f}% AvgR={r['avg_r']:+.2f}R")

    # Save
    with open('_bt_exhaustive_results.json', 'w', encoding='utf-8') as f:
        json.dump({
            'long_top_50': long_results[:50],
            'long_robust_top_20': long_robust[:20],
            'short_top_50': short_results[:50],
            'short_robust_top_20': short_robust[:20],
            'total_tested': len(all_results),
            'time_sec': round(time.time() - t0),
        }, f, indent=2, ensure_ascii=False, default=str)
    print(f"\nSaved to _bt_exhaustive_results.json")
    print(f"Total time: {time.time()-t0:.0f}s")


def save_enriched_cache(signals):
    """Сохраняет signals с RSI полями для повторных запусков."""
    cached = []
    for s in signals:
        item = {
            'pair': s.get('pair'),
            'at_ts': s.get('at_ts'),
        }
        for tf in TFS:
            v = s.get(f'_rsi_{tf}')
            if v is not None:
                item[f'_rsi_{tf}'] = v
        if any(f'_rsi_{tf}' in item for tf in TFS):
            cached.append(item)
    with open(ENRICHED_CACHE, 'w', encoding='utf-8') as f:
        json.dump(cached, f)
    print(f"Saved enriched cache: {len(cached)} entries → {ENRICHED_CACHE}")


if __name__ == '__main__':
    main()
