"""GRID-SEARCH бектест: RSI на всех TF (15m, 1h, 4h, 1d) × buckets × source.

Цель: найти ТОП-20 комбинаций с самым высоким edge.

Output: sorted by AvgR descending, min decided=20.
"""
from __future__ import annotations
import io, json, time, zipfile, csv
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
    # Только 1h и 4h — главные actionable TF. 15m шум, 1d мало bars в 14d
    # window. Сократили с 4 TF до 2 для скорости (15-30 мин вместо часов).
    '1h':  60 * 60 * 1000,
    '4h':  4 * 60 * 60 * 1000,
}
RSI_PERIOD = 14


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
    gains = []
    losses = []
    for i in range(1, period + 1):
        ch = closes[i] - closes[i - 1]
        gains.append(max(ch, 0))
        losses.append(max(-ch, 0))
    avg_g = sum(gains) / period
    avg_l = sum(losses) / period
    rsis = []
    for i in range(period, len(closes)):
        ch = closes[i] - closes[i - 1]
        g = max(ch, 0); l = max(-ch, 0)
        avg_g = (avg_g * (period - 1) + g) / period
        avg_l = (avg_l * (period - 1) + l) / period
        if avg_l == 0:
            rsis.append(100.0)
        else:
            rs = avg_g / avg_l
            rsis.append(100.0 - 100.0 / (1 + rs))
    return rsis


def normalize_pair(pair):
    if not pair:
        return ''
    p = pair.replace('/', '').replace('-', '').upper()
    if not p.endswith('USDT'):
        p = p + 'USDT'
    return p


def main():
    t0 = time.time()
    with open('_bt_signals_detailed.json') as f:
        signals = json.load(f)
    print(f"Loaded {len(signals)} signals")

    by_pair = defaultdict(list)
    for s in signals:
        if s.get('pair'):
            by_pair[s['pair']].append(s)
    print(f"Unique pairs: {len(by_pair)}")

    print("Computing RSI на всех TF...")
    progress = [0]

    def _process(pair):
        sym = normalize_pair(pair)
        if not sym: return
        sigs = by_pair[pair]
        ts_min = min(s['at_ts'] for s in sigs)
        ts_max = max(s['at_ts'] for s in sigs)
        for tf, bucket_ms in TFS.items():
            # Padding по TF (RSI warmup × bucket size)
            warmup_h = 30 if tf == '15m' else 30 if tf == '1h' else 80 if tf == '4h' else 30 * 24
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
            rsi_by = {opens_ms[RSI_PERIOD + i]: rsis[i] for i in range(len(rsis))}
            for s in sigs:
                ats_ms = s['at_ts'] * 1000
                sig_open_ms = (ats_ms // bucket_ms) * bucket_ms
                rsi = rsi_by.get(sig_open_ms)
                if rsi is None:
                    # Fallback: ближайшая свеча до at_ts
                    candidates = [k for k in opens_ms if k <= sig_open_ms and k in rsi_by]
                    if candidates:
                        rsi = rsi_by[max(candidates)]
                if rsi is not None:
                    s[f'_rsi_{tf}'] = rsi
        progress[0] += 1
        if progress[0] % 50 == 0:
            print(f"  {progress[0]}/{len(by_pair)}", flush=True)

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(_process, list(by_pair.keys())))
    print(f"  done in {time.time()-t0:.0f}s")
    for tf in TFS:
        n = sum(1 for s in signals if s.get(f'_rsi_{tf}') is not None)
        print(f"  RSI {tf}: {n}/{len(signals)}")
    print()

    # ── GRID SEARCH ──
    print("Running grid search...")
    sources = sorted({s['source'] for s in signals if s.get('source')})
    directions = ['LONG', 'SHORT']
    tiers = ['match', 'mixed', 'against', None]
    rsi_buckets = [
        ('any',         lambda r: True),
        ('<30',         lambda r: r is not None and r < 30),
        ('30-40',       lambda r: r is not None and 30 <= r < 40),
        ('40-50',       lambda r: r is not None and 40 <= r < 50),
        ('50-60',       lambda r: r is not None and 50 <= r < 60),
        ('60-70',       lambda r: r is not None and 60 <= r < 70),
        ('70-80',       lambda r: r is not None and 70 <= r < 80),
        ('>80',         lambda r: r is not None and r >= 80),
        ('30-50',       lambda r: r is not None and 30 <= r < 50),
        ('40-60',       lambda r: r is not None and 40 <= r < 60),
        ('50-70',       lambda r: r is not None and 50 <= r < 70),
        ('60-80',       lambda r: r is not None and 60 <= r < 80),
    ]

    results = []
    for src in sources:
        src_sigs = [s for s in signals if s.get('source') == src]
        for direction in directions:
            dir_sigs = [s for s in src_sigs if s.get('direction') == direction]
            for tier in tiers:
                if tier is None:
                    tier_sigs = dir_sigs
                    tier_label = 'any-tier'
                else:
                    tier_sigs = [s for s in dir_sigs if s.get('align_tier') == tier]
                    tier_label = tier
                for rsi_tf in list(TFS.keys()) + ['none']:
                    for bucket_label, bucket_fn in rsi_buckets:
                        if rsi_tf == 'none' and bucket_label != 'any':
                            continue
                        if rsi_tf == 'none':
                            arr = tier_sigs
                        else:
                            arr = [s for s in tier_sigs
                                   if bucket_fn(s.get(f'_rsi_{rsi_tf}'))]
                        decided = [s for s in arr if s.get('outcome') in ('tp', 'sl')]
                        if len(decided) < 20:
                            continue
                        wins = sum(1 for s in decided if s.get('outcome') == 'tp')
                        wr = wins / len(decided) * 100
                        avg_r = sum(s.get('r', 0) for s in decided) / len(decided)
                        results.append({
                            'src': src, 'direction': direction,
                            'tier': tier_label,
                            'rsi_tf': rsi_tf, 'rsi_bucket': bucket_label,
                            'n': len(arr), 'decided': len(decided),
                            'wins': wins, 'wr': wr, 'avg_r': avg_r,
                        })

    # Sort by avg_r descending
    results.sort(key=lambda x: -x['avg_r'])

    print(f"\n{'='*100}")
    print(f"TOP-30 EDGE COMBINATIONS (sorted by AvgR, min decided=20)")
    print(f"{'='*100}")
    print(f"{'#':>3} {'Source':<20} {'Dir':<6} {'Tier':<10} {'RSI TF':<7} {'Bucket':<10} {'N':>5} {'Dec':>5} {'WR':>7} {'AvgR':>8}")
    print('-' * 100)
    for i, r in enumerate(results[:30], 1):
        print(f"{i:>3} {r['src'][:20]:<20} {r['direction']:<6} {r['tier']:<10} "
              f"{r['rsi_tf']:<7} {r['rsi_bucket']:<10} "
              f"{r['n']:>5} {r['decided']:>5} {r['wr']:>6.1f}% {r['avg_r']:>+7.2f}R")

    # Best combos for CV (наш main edge)
    print(f"\n{'='*100}")
    print("TOP-15 для CRYPTOVIZOR (наш main edge)")
    print(f"{'='*100}")
    cv_results = [r for r in results if r['src'] == 'cryptovizor']
    print(f"{'#':>3} {'Dir':<6} {'Tier':<10} {'RSI TF':<7} {'Bucket':<10} {'N':>5} {'Dec':>5} {'WR':>7} {'AvgR':>8}")
    print('-' * 100)
    for i, r in enumerate(cv_results[:15], 1):
        print(f"{i:>3} {r['direction']:<6} {r['tier']:<10} {r['rsi_tf']:<7} "
              f"{r['rsi_bucket']:<10} {r['n']:>5} {r['decided']:>5} "
              f"{r['wr']:>6.1f}% {r['avg_r']:>+7.2f}R")

    # Лучшие combos для each source (top 1 per source)
    print(f"\n{'='*100}")
    print("BEST COMBO PER SOURCE")
    print(f"{'='*100}")
    print(f"{'Source':<20} {'Dir':<6} {'Tier':<10} {'RSI TF':<7} {'Bucket':<10} {'N':>5} {'Dec':>5} {'WR':>7} {'AvgR':>8}")
    print('-' * 100)
    seen_src = set()
    for r in results:
        if r['src'] in seen_src: continue
        if r['decided'] < 30: continue
        seen_src.add(r['src'])
        print(f"{r['src']:<20} {r['direction']:<6} {r['tier']:<10} "
              f"{r['rsi_tf']:<7} {r['rsi_bucket']:<10} "
              f"{r['n']:>5} {r['decided']:>5} {r['wr']:>6.1f}% {r['avg_r']:>+7.2f}R")

    # Save
    with open('_bt_rsi_grid_results.json', 'w', encoding='utf-8') as f:
        json.dump({'top_50': results[:50], 'all': results}, f, indent=2,
                  ensure_ascii=False)
    print(f"\nSaved to _bt_rsi_grid_results.json")
    print(f"Total combinations tested: {len(results)}")
    print(f"Total time: {time.time()-t0:.0f}s")


if __name__ == '__main__':
    main()
