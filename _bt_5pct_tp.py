"""Бектест с TP=±5% от entry (override signal.tp).

Цель: понять как изменится WR/AvgR если требуем минимум 5% движения,
а не маленький TP из сигнала (DCA2 у CV ~1.5-2%).

Тестируем 3 варианта:
1. **TP=5%, SL=signal.sl** (override TP, оставляем родной SL)
2. **TP=5%, SL=2%** (override обоих — фиксированный 2.5:1 RR)
3. **Multi-step**: 30% at signal.tp1, 40% at +5%, 30% at +10%
"""
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
                            out.append([o, row[1], row[2], row[3], row[4],
                                        row[5], int(row[6])])
                        except Exception:
                            continue
        except Exception:
            pass
        cur += timedelta(days=1)
    return sorted(out, key=lambda k: int(k[0]))


def find_outcome_5pct(klines, entry, sl_price, direction, start_ms, tp_pct=5.0):
    """Тест: TP = entry × (1 ± tp_pct/100), SL = signal.sl."""
    if not entry or not sl_price:
        return ('unknown', None)
    is_long = (direction or '').upper() == 'LONG'
    if is_long:
        tp_price = entry * (1 + tp_pct/100)
    else:
        tp_price = entry * (1 - tp_pct/100)
    sl_dist = abs(entry - sl_price)
    tp_dist = abs(tp_price - entry)
    rr = tp_dist / sl_dist if sl_dist > 0 else 0
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
            if low <= sl_price:
                if high >= tp_price:
                    return ('tp', rr)
                return ('sl', rr)
            if high >= tp_price:
                return ('tp', rr)
        else:
            if high >= sl_price:
                if low <= tp_price:
                    return ('tp', rr)
                return ('sl', rr)
            if low <= tp_price:
                return ('tp', rr)
    return ('timeout', rr)


def find_outcome_5pct_2pct_sl(klines, entry, direction, start_ms,
                               tp_pct=5.0, sl_pct=2.0):
    """Override обоих: TP=5%, SL=2% — фиксированный RR 2.5:1."""
    is_long = (direction or '').upper() == 'LONG'
    if is_long:
        tp_price = entry * (1 + tp_pct/100)
        sl_price = entry * (1 - sl_pct/100)
    else:
        tp_price = entry * (1 - tp_pct/100)
        sl_price = entry * (1 + sl_pct/100)
    rr = tp_pct / sl_pct  # фиксированно
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
            if low <= sl_price:
                if high >= tp_price:
                    return ('tp', rr)
                return ('sl', rr)
            if high >= tp_price:
                return ('tp', rr)
        else:
            if high >= sl_price:
                if low <= tp_price:
                    return ('tp', rr)
                return ('sl', rr)
            if low <= tp_price:
                return ('tp', rr)
    return ('timeout', rr)


def find_outcome_multistep(klines, entry, sl_price, direction, start_ms,
                            tp1_pct, tp2_pct=5.0, tp3_pct=10.0):
    """Multi-step: 30% at tp1, 40% at tp2, 30% at tp3.
    Возвращает суммарный R-multiple (взвешенный)."""
    if not entry or not sl_price:
        return ('unknown', 0)
    is_long = (direction or '').upper() == 'LONG'
    sl_dist = abs(entry - sl_price)
    if is_long:
        tp1 = entry * (1 + tp1_pct/100)
        tp2 = entry * (1 + tp2_pct/100)
        tp3 = entry * (1 + tp3_pct/100)
    else:
        tp1 = entry * (1 - tp1_pct/100)
        tp2 = entry * (1 - tp2_pct/100)
        tp3 = entry * (1 - tp3_pct/100)
    portion_filled = 0  # 0 → нет, 1 → tp1, 2 → tp2, 3 → tp3
    realized_r = 0.0  # суммарный R закрытых частей
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
            # SL hit?
            if low <= sl_price:
                # Закрываем оставшееся по SL
                remaining = 1.0 - (0.3 if portion_filled >= 1 else 0) - \
                            (0.4 if portion_filled >= 2 else 0)
                realized_r += remaining * (-1.0)
                if portion_filled == 0:
                    return ('sl', realized_r)
                else:
                    return ('partial_sl', realized_r)
            # Проверяем TP по-этапно
            if portion_filled < 1 and high >= tp1:
                realized_r += 0.3 * (tp1_pct / (sl_dist/entry*100))
                portion_filled = 1
            if portion_filled < 2 and high >= tp2:
                realized_r += 0.4 * (tp2_pct / (sl_dist/entry*100))
                portion_filled = 2
            if portion_filled < 3 and high >= tp3:
                realized_r += 0.3 * (tp3_pct / (sl_dist/entry*100))
                return ('tp3', realized_r)
        else:  # SHORT
            if high >= sl_price:
                remaining = 1.0 - (0.3 if portion_filled >= 1 else 0) - \
                            (0.4 if portion_filled >= 2 else 0)
                realized_r += remaining * (-1.0)
                if portion_filled == 0:
                    return ('sl', realized_r)
                else:
                    return ('partial_sl', realized_r)
            if portion_filled < 1 and low <= tp1:
                realized_r += 0.3 * (tp1_pct / (sl_dist/entry*100))
                portion_filled = 1
            if portion_filled < 2 and low <= tp2:
                realized_r += 0.4 * (tp2_pct / (sl_dist/entry*100))
                portion_filled = 2
            if portion_filled < 3 and low <= tp3:
                realized_r += 0.3 * (tp3_pct / (sl_dist/entry*100))
                return ('tp3', realized_r)
    # Timeout — закрываем остаток по timeout (нейтральная цена ~entry)
    if portion_filled == 0:
        return ('timeout', 0)
    return ('partial_timeout', realized_r)


def normalize_pair_to_symbol(pair):
    if not pair: return ''
    p = pair.replace('/', '').replace('-', '').upper()
    if not p.endswith('USDT'):
        p = p + 'USDT'
    return p


def run_backtest(signals_filtered, klines_cache, label, outcome_fn):
    """Запускает outcome_fn на отфильтрованных сигналах."""
    results = []
    for s in signals_filtered:
        pair = s.get('pair')
        sym = normalize_pair_to_symbol(pair)
        klines = klines_cache.get(sym)
        if not klines:
            continue
        ats_ms = (s.get('at_ts') or 0) * 1000
        outcome_start = ats_ms + 15 * 60 * 1000  # next 15m candle
        outcome_end = ats_ms + 24 * 3600 * 1000
        relevant = [k for k in klines if outcome_start <= int(k[0]) <= outcome_end]
        outcome, rr = outcome_fn(s, relevant, outcome_start)
        s_copy = dict(s)
        s_copy['_outcome'] = outcome
        s_copy['_rr'] = rr
        results.append(s_copy)
    # Stats
    n = len(results)
    if n == 0:
        return None
    wins = sum(1 for r in results if r['_outcome'] in ('tp', 'tp3', 'partial_sl', 'partial_timeout'))
    losses = sum(1 for r in results if r['_outcome'] == 'sl')
    timeouts = sum(1 for r in results if r['_outcome'] in ('timeout', 'unknown'))
    decided = wins + losses
    if decided == 0:
        return None
    wr = wins / decided * 100
    # Compute AvgR depending on outcome type
    avg_r_total = 0.0
    decided_with_r = 0
    for r in results:
        oc = r['_outcome']
        rr = r.get('_rr', 0)
        if oc == 'tp':
            avg_r_total += rr
            decided_with_r += 1
        elif oc == 'tp3':
            avg_r_total += rr
            decided_with_r += 1
        elif oc == 'partial_sl' or oc == 'partial_timeout':
            avg_r_total += rr
            decided_with_r += 1
        elif oc == 'sl':
            avg_r_total -= 1.0
            decided_with_r += 1
    avg_r = avg_r_total / decided_with_r if decided_with_r else 0
    return {
        'label': label, 'n': n, 'wins': wins, 'losses': losses,
        'timeouts': timeouts, 'decided': decided, 'wr': wr,
        'avg_r': avg_r,
    }


def main():
    # Load signals
    with open('_bt_signals_detailed.json') as f:
        signals = json.load(f)
    print(f"Loaded {len(signals)} signals")

    # Применяем ALPHA-CV фильтр (как в стратегии)
    BAD_HOURS = {1, 2, 10, 12, 13}
    BAD_WEEKDAYS = {0, 5}
    def alpha_cv(s):
        src = s.get('source')
        tier = s.get('align_tier')
        direction = s.get('direction')
        h = s.get('hour'); wd = s.get('weekday')
        if h in BAD_HOURS or wd in BAD_WEEKDAYS:
            return False
        if src == 'cryptovizor' and tier in ('match', 'mixed'):
            return True
        if src == 'second_flip' and direction == 'LONG' and tier == 'match':
            return True
        if src == 'triple_confluence' and direction == 'LONG' and tier == 'mixed':
            return True
        return False
    filtered = [s for s in signals if alpha_cv(s)]
    print(f"After ALPHA-CV filter: {len(filtered)} signals")
    # Group by pair for klines fetch
    by_pair = defaultdict(list)
    for s in filtered:
        if s.get('pair'):
            by_pair[s['pair']].append(s)
    print(f"Unique pairs: {len(by_pair)}")

    print("\nFetching klines via CDN...")
    klines_cache = {}
    progress = [0]
    def _fetch(pair):
        sym = normalize_pair_to_symbol(pair)
        if not sym: return
        sigs = by_pair[pair]
        ts_min = min(s['at_ts'] for s in sigs)
        ts_max = max(s['at_ts'] for s in sigs)
        start_ms = ts_min * 1000
        end_ms = (ts_max + 25 * 3600) * 1000
        try:
            kl = fetch_klines_cdn(sym, '15m', start_ms, end_ms)
            if kl:
                klines_cache[sym] = kl
        except Exception:
            pass
        progress[0] += 1
        if progress[0] % 25 == 0:
            print(f"  {progress[0]}/{len(by_pair)} pairs", flush=True)
    with ThreadPoolExecutor(max_workers=15) as ex:
        list(ex.map(_fetch, list(by_pair.keys())))
    print(f"  klines fetched: {len(klines_cache)} pairs\n")

    # Variants
    print("="*78)
    print("BACKTEST RESULTS — TP variants")
    print("="*78)

    # 1. Original signal TP/SL (baseline)
    def fn_baseline(s, klines, start_ms):
        # SL = signal.sl, TP = signal.tp (что уже было в DB)
        if not s.get('entry') or not s.get('sl') or not s.get('tp'):
            return ('unknown', 0)
        sl_dist = abs(s['entry'] - s['sl'])
        tp_dist = abs(s['tp'] - s['entry'])
        rr = tp_dist / sl_dist if sl_dist else 0
        is_long = (s.get('direction') or '').upper() == 'LONG'
        for k in klines:
            try:
                open_ms = int(k[0])
                if open_ms < start_ms: continue
                high = float(k[2]); low = float(k[3])
            except: continue
            if is_long:
                if low <= s['sl']:
                    if high >= s['tp']: return ('tp', rr)
                    return ('sl', rr)
                if high >= s['tp']: return ('tp', rr)
            else:
                if high >= s['sl']:
                    if low <= s['tp']: return ('tp', rr)
                    return ('sl', rr)
                if low <= s['tp']: return ('tp', rr)
        return ('timeout', rr)

    # 2. TP=5%, SL=signal.sl
    def fn_5pct(s, klines, start_ms):
        return find_outcome_5pct(klines, s.get('entry'), s.get('sl'),
                                  s.get('direction'), start_ms, tp_pct=5.0)

    # 3. TP=5%, SL=2% (override обоих)
    def fn_5_2(s, klines, start_ms):
        return find_outcome_5pct_2pct_sl(klines, s.get('entry'),
                                          s.get('direction'), start_ms,
                                          tp_pct=5.0, sl_pct=2.0)

    # 4. Multi-step TP1=signal.tp%, TP2=5%, TP3=10%
    def fn_multi(s, klines, start_ms):
        if not s.get('entry') or not s.get('sl') or not s.get('tp'):
            return ('unknown', 0)
        tp1_pct = abs(s['tp'] - s['entry']) / s['entry'] * 100
        return find_outcome_multistep(klines, s['entry'], s['sl'],
                                       s.get('direction'), start_ms,
                                       tp1_pct, tp2_pct=5.0, tp3_pct=10.0)

    # 5. TP=10%, SL=signal.sl (более амбициозно)
    def fn_10pct(s, klines, start_ms):
        return find_outcome_5pct(klines, s.get('entry'), s.get('sl'),
                                  s.get('direction'), start_ms, tp_pct=10.0)

    variants = [
        ('Baseline (signal TP/SL)', fn_baseline),
        ('TP=5%, SL=signal.sl', fn_5pct),
        ('TP=5%, SL=2% fixed', fn_5_2),
        ('Multi-step (30%@TP1 / 40%@5% / 30%@10%)', fn_multi),
        ('TP=10%, SL=signal.sl', fn_10pct),
    ]

    print(f"{'Variant':<46} {'N':>5} {'WR':>7} {'AvgR':>8} {'Wins':>5} {'SL':>4} {'TO':>5}")
    print('-'*78)
    for label, fn in variants:
        r = run_backtest(filtered, klines_cache, label, fn)
        if r:
            print(f"{r['label']:<46} {r['n']:>5} {r['wr']:>6.1f}% {r['avg_r']:>+7.2f}R "
                  f"{r['wins']:>5} {r['losses']:>4} {r['timeouts']:>5}")

    # Per-source breakdown for TP=5% variant
    print("\n\n── BREAKDOWN: TP=5%, SL=signal.sl per source ──")
    by_src_label = defaultdict(list)
    for s in filtered:
        src = s.get('source', '?')
        tier = s.get('align_tier', '?')
        direction = s.get('direction', '?')
        sym = normalize_pair_to_symbol(s.get('pair', ''))
        klines = klines_cache.get(sym, [])
        ats_ms = (s.get('at_ts') or 0) * 1000
        out_start = ats_ms + 15 * 60 * 1000
        out_end = ats_ms + 24 * 3600 * 1000
        rel = [k for k in klines if out_start <= int(k[0]) <= out_end]
        oc, rr = find_outcome_5pct(rel, s.get('entry'), s.get('sl'),
                                     direction, out_start, tp_pct=5.0)
        key = f"{src} | {direction} | {tier}"
        by_src_label[key].append((oc, rr))

    print(f"{'Bucket':<48} {'N':>5} {'WR':>7} {'AvgR':>8}")
    print('-'*78)
    for key in sorted(by_src_label.keys()):
        arr = by_src_label[key]
        wins = sum(1 for o,_ in arr if o == 'tp')
        losses = sum(1 for o,_ in arr if o == 'sl')
        decided = wins + losses
        if decided < 5: continue
        wr = wins / decided * 100
        rr_sum = 0.0
        for o, rr in arr:
            if o == 'tp': rr_sum += rr
            elif o == 'sl': rr_sum -= 1
        avg_r = rr_sum / decided if decided else 0
        print(f"{key:<48} {len(arr):>5} {wr:>6.1f}% {avg_r:>+7.2f}R")


if __name__ == '__main__':
    t0 = time.time()
    main()
    print(f"\nDone in {time.time()-t0:.0f}s")
