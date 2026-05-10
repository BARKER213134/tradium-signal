"""Бектест: RSI buckets, особое внимание зоне 50-70.

Загружает 5500 сигналов из _bt_signals_detailed.json, для каждого считает
1h RSI(14) на свече сигнала через klines (CDN), затем агрегирует
WR/AvgR по бакетам RSI.

Зона 50-70: "neutral-bullish" (нет ни перепроданности ни перекупленности).
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
                            # [open_ms, open, high, low, close, volume, close_ms]
                            out.append([o, row[1], row[2], row[3], row[4],
                                        row[5], int(row[6])])
                        except Exception:
                            continue
        except Exception:
            pass
        cur += timedelta(days=1)
    return sorted(out, key=lambda k: int(k[0]))


def compute_rsi(closes, period=14):
    """Возвращает список RSI значений (длиной = len(closes) - period).
    Wilder's smoothing."""
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
        g = max(ch, 0)
        l = max(-ch, 0)
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


def load_signals():
    with open('_bt_signals_detailed.json') as f:
        return json.load(f)


def main():
    t0 = time.time()
    signals = load_signals()
    print(f"Loaded {len(signals)} signals")
    decided = [s for s in signals if s.get('outcome') in ('tp', 'sl')]
    print(f"Decided (TP/SL): {len(decided)}\n")

    # Group by pair
    by_pair = defaultdict(list)
    for s in signals:
        if s.get('pair'):
            by_pair[s['pair']].append(s)
    print(f"Unique pairs: {len(by_pair)}")

    # Fetch 1h klines per pair, compute RSI series, attach RSI to each signal
    print("Fetching klines (1h) + computing RSI...")
    progress = [0]

    BUCKET_MS = 4 * 3600 * 1000  # 4h
    def _process(pair):
        sym = normalize_pair(pair)
        if not sym:
            return
        sigs = by_pair[pair]
        ts_min = min(s['at_ts'] for s in sigs)
        ts_max = max(s['at_ts'] for s in sigs)
        # RSI(14) на 4h = warmup ~14 баров × 4h = 56 часов запаса
        start_ms = (ts_min - 80 * 3600) * 1000
        end_ms = (ts_max + 4 * 3600) * 1000
        try:
            kl = fetch_klines_cdn(sym, '4h', start_ms, end_ms)
        except Exception:
            kl = []
        if not kl:
            return
        opens_ms = [int(k[0]) for k in kl]
        closes = [float(k[4]) for k in kl]
        rsis = compute_rsi(closes, 14)
        rsi_by_open_ms = {opens_ms[14 + i]: rsis[i] for i in range(len(rsis))}
        # Для каждого сигнала найдём свечу 4h на которой он висит
        for s in sigs:
            ats_ms = s['at_ts'] * 1000
            sig_open_ms = (ats_ms // BUCKET_MS) * BUCKET_MS
            rsi = rsi_by_open_ms.get(sig_open_ms)
            if rsi is None:
                # Берём последнюю доступную до at_ts
                for k in reversed(opens_ms):
                    if k <= sig_open_ms and k in rsi_by_open_ms:
                        rsi = rsi_by_open_ms[k]
                        break
            s['_rsi_4h'] = rsi
            s['_rsi_1h'] = rsi  # alias чтобы остальной код работал
            progress[0] += 1
            if progress[0] % 500 == 0:
                print(f"  {progress[0]}/{len(signals)}", flush=True)

    with ThreadPoolExecutor(max_workers=15) as ex:
        list(ex.map(_process, list(by_pair.keys())))
    with_rsi = sum(1 for s in signals if s.get('_rsi_1h') is not None)
    print(f"  RSI computed: {with_rsi}/{len(signals)} ({with_rsi/len(signals)*100:.0f}%)")
    print(f"  time: {time.time()-t0:.0f}s\n")

    # ── Aggregations ──
    def stats(arr, label, min_n=10):
        n = len(arr)
        if n < min_n:
            return None
        wins = sum(1 for s in arr if s.get('outcome') == 'tp')
        losses = sum(1 for s in arr if s.get('outcome') == 'sl')
        decided = wins + losses
        if decided < min_n:
            return None
        wr = wins / decided * 100
        avg_r = sum(s.get('r', 0) for s in arr if s.get('r') is not None) / decided
        return {'label': label, 'n': n, 'wins': wins, 'losses': losses,
                'decided': decided, 'wr': wr, 'avg_r': avg_r}

    def show(s):
        if not s: return
        print(f"  {s['label']:<48} N={s['n']:5d} dec={s['decided']:5d} "
              f"WR={s['wr']:5.1f}% AvgR={s['avg_r']:+.2f}R")

    print("=" * 84)
    print("RSI(14) на 4h — BUCKETS — все источники")
    print("=" * 84)
    buckets = [
        ('< 30 (oversold)', lambda r: r < 30),
        ('30-40', lambda r: 30 <= r < 40),
        ('40-50', lambda r: 40 <= r < 50),
        ('50-55', lambda r: 50 <= r < 55),
        ('55-60', lambda r: 55 <= r < 60),
        ('60-65', lambda r: 60 <= r < 65),
        ('65-70', lambda r: 65 <= r < 70),
        ('50-70 (target zone)', lambda r: 50 <= r < 70),
        ('70-80', lambda r: 70 <= r < 80),
        ('> 80 (overbought)', lambda r: r >= 80),
    ]
    for lbl, fn in buckets:
        arr = [s for s in signals if s.get('_rsi_1h') is not None and fn(s['_rsi_1h'])]
        show(stats(arr, lbl))

    print("\n=" * 1 + "=" * 83)
    print("RSI 50-70 × DIRECTION (зона нашего интереса)")
    print("=" * 84)
    target = [s for s in signals if s.get('_rsi_1h') is not None and 50 <= s['_rsi_1h'] < 70]
    print(f"\nTotal в RSI 50-70: {len(target)}")
    for direction in ('LONG', 'SHORT'):
        arr = [s for s in target if s.get('direction') == direction]
        show(stats(arr, f'{direction} (RSI 50-70)'))

    print("\n=" * 1 + "=" * 83)
    print("RSI 50-70 × SOURCE")
    print("=" * 84)
    sources = sorted({s['source'] for s in target if s.get('source')})
    for src in sources:
        arr = [s for s in target if s.get('source') == src]
        show(stats(arr, f'{src} (RSI 50-70)'))

    print("\n=" * 1 + "=" * 83)
    print("RSI 50-70 × SOURCE × DIRECTION × TIER")
    print("=" * 84)
    for src in sources:
        for d in ('LONG', 'SHORT'):
            for tier in ('match', 'mixed', 'against'):
                arr = [s for s in target if s.get('source') == src
                       and s.get('direction') == d
                       and s.get('align_tier') == tier]
                show(stats(arr, f'{src} | {d} | {tier} (RSI 50-70)'))

    print("\n=" * 1 + "=" * 83)
    print("RSI 50-70 vs ВНЕ зоны — сравнение по source")
    print("=" * 84)
    for src in sources:
        in_zone = [s for s in signals if s.get('source') == src
                   and s.get('_rsi_1h') is not None and 50 <= s['_rsi_1h'] < 70]
        out_zone = [s for s in signals if s.get('source') == src
                    and s.get('_rsi_1h') is not None
                    and not (50 <= s['_rsi_1h'] < 70)]
        s_in = stats(in_zone, f'{src} IN  RSI 50-70')
        s_out = stats(out_zone, f'{src} OUT RSI 50-70')
        if s_in and s_out:
            print()
            show(s_in)
            show(s_out)
            diff_wr = s_in['wr'] - s_out['wr']
            diff_r = s_in['avg_r'] - s_out['avg_r']
            print(f"    Δ: WR{diff_wr:+.1f}pp  AvgR{diff_r:+.2f}R")

    # Save
    out_data = {
        'rsi_buckets': [(lbl, stats([s for s in signals if s.get('_rsi_1h') is not None and fn(s['_rsi_1h'])], lbl)) for lbl, fn in buckets],
        'time_sec': round(time.time() - t0),
    }
    with open('_bt_rsi_results.json', 'w', encoding='utf-8') as f:
        json.dump(out_data, f, indent=2, ensure_ascii=False, default=str)
    print(f"\nSaved to _bt_rsi_results.json")


if __name__ == '__main__':
    main()
