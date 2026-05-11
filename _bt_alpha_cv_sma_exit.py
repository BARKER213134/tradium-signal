"""Бектест ALPHA-CV стратегии с EXIT по 1h SMA(RSI) crossover.

Entry: ALPHA-CV whitelist (cryptovizor / second_flip LONG / triple_confluence LONG)
Exit:
  - LONG: 1h RSI < 1h SMA14(RSI) на закрытом баре → close
  - SHORT: 1h RSI > 1h SMA14(RSI) → close
  - Backup SL: signal.sl hit
  - Max hold: 48h

Сравнение с baseline (signal TP/SL) — оценим improvement от dynamic exit.
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
                    limits=httpx.Limits(max_connections=15, max_keepalive_connections=10),
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
                    for kr in r.json():
                        try:
                            out.append([int(kr[0]), float(kr[1]), float(kr[2]),
                                        float(kr[3]), float(kr[4]),
                                        float(kr[5]), int(kr[6])])
                        except Exception:
                            continue
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
                            out.append([o, float(row[1]), float(row[2]),
                                        float(row[3]), float(row[4]),
                                        float(row[5]), int(row[6])])
                        except Exception:
                            continue
        except Exception:
            pass
        cur += timedelta(days=1)
    return sorted(out, key=lambda k: int(k[0]))


def compute_rsi(closes, period=14):
    if len(closes) < period + 1:
        return [None] * len(closes)
    rsi = [None] * len(closes)
    gains = [0.0]; losses = [0.0]
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i-1]
        gains.append(max(ch, 0)); losses.append(max(-ch, 0))
    avg_g = sum(gains[1:period+1]) / period
    avg_l = sum(losses[1:period+1]) / period
    rsi[period] = 100.0 if avg_l == 0 else (100 - 100/(1 + avg_g/avg_l))
    for i in range(period+1, len(closes)):
        avg_g = (avg_g * (period-1) + gains[i]) / period
        avg_l = (avg_l * (period-1) + losses[i]) / period
        rsi[i] = 100.0 if avg_l == 0 else (100 - 100/(1 + avg_g/avg_l))
    return rsi


def compute_sma(values, period=14):
    sma = [None] * len(values)
    buf = []; s = 0
    for i, v in enumerate(values):
        if v is None: continue
        buf.append(v); s += v
        if len(buf) > period: s -= buf.pop(0)
        if len(buf) == period: sma[i] = s / period
    return sma


def normalize_pair(pair):
    if not pair: return ''
    p = pair.replace('/', '').replace('-', '').upper()
    if not p.endswith('USDT'): p = p + 'USDT'
    return p


# ─── ALPHA-CV entry filter ───
def alpha_cv_filter(s):
    """True если сигнал проходит ALPHA-CV entry rules."""
    src = s.get('source')
    direction = (s.get('direction') or '').upper()
    tier = s.get('align_tier')
    if direction not in ('LONG', 'SHORT'):
        return False, 'invalid_direction'
    if src == 'cryptovizor':
        # tier=match/mixed/None → accept (CV сильнейший edge)
        if tier in (None, '', 'match', 'mixed'):
            return True, f'cv_{tier or "no_tier"}'
        return False, f'cv_tier_against'
    if src == 'second_flip' and direction == 'LONG':
        if tier in (None, '', 'match'):
            return True, 'sf_long_ok'
        return False, 'sf_tier_bad'
    if src == 'triple_confluence' and direction == 'LONG':
        if tier in (None, '', 'mixed'):
            return True, 'tc_long_ok'
        return False, 'tc_tier_bad'
    return False, f'src_not_whitelist={src}'


def simulate_signal(s, klines_1h):
    """ENTRY by alpha_cv_filter, EXIT by 1h SMA(RSI) crossover."""
    ok, reason = alpha_cv_filter(s)
    if not ok:
        return {'filter_pass': False, 'reason': reason}
    direction = (s.get('direction') or '').upper()
    entry = s.get('entry')
    sl = s.get('sl')
    at_ts = s.get('at_ts')
    if not (entry and sl and at_ts):
        return {'filter_pass': True, 'outcome': 'invalid_data', 'r': None}
    is_long = direction == 'LONG'
    ats_ms = at_ts * 1000

    if not klines_1h or len(klines_1h) < 30:
        return {'filter_pass': True, 'outcome': 'no_klines', 'r': None}
    closes = [k[4] for k in klines_1h]
    highs = [k[2] for k in klines_1h]
    lows = [k[3] for k in klines_1h]
    opens_ms = [k[0] for k in klines_1h]
    rsi = compute_rsi(closes, 14)
    sma = compute_sma(rsi, 14)

    # Найти первый бар ПОСЛЕ at_ts
    start_idx = None
    for i, om in enumerate(opens_ms):
        if om > ats_ms:
            start_idx = i
            break
    if start_idx is None:
        return {'filter_pass': True, 'outcome': 'no_future_data', 'r': None}

    max_hold = 48
    end_idx = min(start_idx + max_hold, len(opens_ms))

    exit_price = None
    exit_reason = None
    for i in range(start_idx, end_idx):
        h = highs[i]; l = lows[i]
        # SL hit?
        if is_long and l <= sl:
            exit_price = sl
            exit_reason = 'sl_hit'
            break
        if not is_long and h >= sl:
            exit_price = sl
            exit_reason = 'sl_hit'
            break
        # RSI-SMA crossover?
        r1 = rsi[i]; s1 = sma[i]
        if r1 is None or s1 is None:
            continue
        if is_long and r1 < s1:
            exit_price = closes[i]
            exit_reason = 'rsi_cross_exit'
            break
        if not is_long and r1 > s1:
            exit_price = closes[i]
            exit_reason = 'rsi_cross_exit'
            break

    if exit_price is None:
        last_i = end_idx - 1
        if last_i >= start_idx:
            exit_price = closes[last_i]
            exit_reason = 'timeout'
        else:
            return {'filter_pass': True, 'outcome': 'no_exit', 'r': None}

    # R-multiple
    sl_dist = abs(entry - sl)
    if sl_dist <= 0:
        return {'filter_pass': True, 'outcome': 'invalid_sl', 'r': None}
    if is_long:
        pnl = exit_price - entry
    else:
        pnl = entry - exit_price
    r = pnl / sl_dist
    outcome = 'tp' if r > 0.5 else ('sl' if r <= -0.9 else ('be' if abs(r) < 0.1 else ('partial_win' if r > 0 else 'partial_loss')))
    return {
        'filter_pass': True,
        'outcome': outcome,
        'r': r,
        'exit_reason': exit_reason,
        'exit_price': exit_price,
        'entry_reason': reason,
    }


def main():
    t0 = time.time()
    with open('_bt_signals_detailed.json') as f:
        signals = json.load(f)
    print(f"Loaded {len(signals)} signals")

    by_pair = defaultdict(list)
    for s in signals:
        if s.get('pair') and s.get('at_ts'):
            by_pair[s['pair']].append(s)
    print(f"Unique pairs: {len(by_pair)}")

    progress = [0]
    total_pairs = len(by_pair)
    all_results = []

    def _process(pair):
        sym = normalize_pair(pair)
        if not sym: return []
        sigs = by_pair[pair]
        ts_min = min(s['at_ts'] for s in sigs)
        ts_max = max(s['at_ts'] for s in sigs)
        start_ms = (ts_min - 30 * 3600) * 1000
        end_ms = (ts_max + 50 * 3600) * 1000
        try:
            kl = fetch_klines_cdn(sym, '1h', start_ms, end_ms)
        except Exception:
            return []
        results = []
        for s in sigs:
            res = simulate_signal(s, kl)
            if res:
                res['source'] = s.get('source')
                res['direction'] = s.get('direction')
                res['pair'] = s.get('pair')
                res['align_tier'] = s.get('align_tier')
                results.append(res)
        progress[0] += 1
        if progress[0] % 25 == 0:
            print(f"  {progress[0]}/{total_pairs}", flush=True)
        return results

    with ThreadPoolExecutor(max_workers=8) as ex:
        for r_list in ex.map(_process, list(by_pair.keys())):
            all_results.extend(r_list)

    print(f"\nProcessed {len(all_results)} signals in {time.time()-t0:.0f}s")

    passed = [r for r in all_results if r.get('filter_pass')]
    rejected = [r for r in all_results if not r.get('filter_pass')]
    print(f"Entry filter: {len(passed)} passed, {len(rejected)} rejected ({len(passed)/len(all_results)*100:.1f}%)")

    def stats(arr, label):
        n = len(arr)
        with_r = [r for r in arr if r.get('r') is not None]
        if len(with_r) == 0:
            return None
        wins = sum(1 for r in with_r if r['r'] > 0)
        losses = sum(1 for r in with_r if r['r'] < 0)
        wr = wins / len(with_r) * 100
        avg_r = sum(r['r'] for r in with_r) / len(with_r)
        sum_w = sum(r['r'] for r in with_r if r['r'] > 0)
        sum_l = abs(sum(r['r'] for r in with_r if r['r'] < 0))
        pf = sum_w / sum_l if sum_l > 0 else 0
        print(f"  {label:<40} N={n:5d} wins={wins:4d} losses={losses:4d} "
              f"WR={wr:5.1f}% AvgR={avg_r:+.2f}R PF={pf:.2f}")
        return {'n': n, 'wins': wins, 'losses': losses, 'wr': wr,
                'avg_r': avg_r, 'pf': pf}

    print(f"\n{'='*88}")
    print("OVERALL — ALPHA-CV + SMA EXIT")
    print(f"{'='*88}")
    stats(passed, 'all (after ALPHA-CV filter)')
    stats([r for r in passed if r.get('direction') == 'LONG'], 'LONG')
    stats([r for r in passed if r.get('direction') == 'SHORT'], 'SHORT')

    print(f"\n{'='*88}")
    print("PER SOURCE × DIRECTION")
    print(f"{'='*88}")
    sources = sorted({r.get('source') for r in passed if r.get('source')})
    for src in sources:
        for d in ('LONG', 'SHORT'):
            arr = [r for r in passed if r.get('source') == src
                   and r.get('direction') == d]
            stats(arr, f'{src} {d}')

    print(f"\n{'='*88}")
    print("EXIT REASONS")
    print(f"{'='*88}")
    by_reason = defaultdict(list)
    for r in passed:
        if r.get('exit_reason'):
            by_reason[r['exit_reason']].append(r)
    for reason, arr in sorted(by_reason.items(), key=lambda x: -len(x[1])):
        stats(arr, f'exit={reason}')

    print(f"\n{'='*88}")
    print("FILTER REJECT REASONS")
    print(f"{'='*88}")
    rej_count = defaultdict(int)
    for r in rejected:
        rej_count[r.get('reason', '?')] += 1
    for reason, n in sorted(rej_count.items(), key=lambda x: -x[1]):
        print(f"  {reason:<40} {n}")

    # Save
    with open('_bt_alpha_cv_sma_results.json', 'w', encoding='utf-8') as f:
        json.dump({
            'total': len(all_results),
            'passed': len(passed),
            'rejected': len(rejected),
            'time_sec': round(time.time() - t0),
        }, f, indent=2, ensure_ascii=False)
    print(f"\nDone in {time.time()-t0:.0f}s")


if __name__ == '__main__':
    main()
