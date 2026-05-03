"""New Strategy Detectors — 3 backtest-validated strategies.

Triggered AFTER each ST flip (called from supertrend_tracker._save_signal).
For each ST flip, we run 3 detectors and save matching signals to
new_strategy_signals collection.

Strategies (validated 14d, 11.5k signals, OOS holdout):
  🌊 Volume Surge   — ST flip + vol >= 3× MA20(1h)         WR 67-72%, E=+1.18R
  🐉 Triple Confluence — 2+ sources same dir same pair 4h  WR 67%,    E=+0.61R
  🔋 Volume Accum   — ST flip + 3 rising vol bars before   WR 54%,    E=+0.31R

SL/TP per strategy:
  🌊 SL=ST line (1R), TP=2.5R
  🐉 SL=ST line (1R), TP=2R
  🔋 SL=ST line (1R), TP=1.5R
"""
from __future__ import annotations
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# Strategy configs
VOLUME_SURGE_THRESHOLD = 3.0   # 3× MA20
VOL_ACCUM_BARS = 3              # 3 consecutive rising volume bars
CROSS_SOURCE_WINDOW_H = 4       # 4-hour window for triple confluence
TRIPLE_CONFLUENCE_MIN_SOURCES = 2  # at least 2 different sources

STRATEGY_TP_R = {
    'volume_surge': 2.5,
    'triple_confluence': 2.0,
    'vol_accum': 1.5,
}

STRATEGY_EMOJI = {
    'volume_surge': '🌊',
    'triple_confluence': '🐉',
    'vol_accum': '🔋',
}

STRATEGY_LABEL = {
    'volume_surge': 'Volume Surge',
    'triple_confluence': 'Triple Confluence',
    'vol_accum': 'Volume Accumulation',
}


def compute_volume_ratio(candles: list[dict], n_ma: int = 20) -> float:
    """Returns last_bar_volume / MA(n_ma) of preceding bars.
    Returns 0 if not enough data."""
    if not candles or len(candles) < n_ma + 1:
        return 0.0
    last = candles[-1]
    prev = candles[-n_ma-1:-1]
    if not prev:
        return 0.0
    avg = sum(c.get('v', 0) for c in prev) / len(prev)
    if avg <= 0:
        return 0.0
    return float(last.get('v', 0)) / avg


def has_volume_accumulation(candles: list[dict], n_bars: int = VOL_ACCUM_BARS) -> bool:
    """Last n_bars (excluding current bar) had each higher volume than prev."""
    if len(candles) < n_bars + 2:
        return False
    # Check the last n_bars BEFORE current — i.e. candles[-n_bars-1:-1]
    window = candles[-n_bars-1:-1]
    for i in range(1, len(window)):
        if window[i].get('v', 0) <= window[i-1].get('v', 0):
            return False
    return True


def detect_volume_surge(pair: str, direction: str, entry: float, sl: float,
                       candles_1h: list[dict]) -> Optional[dict]:
    """🌊 Volume Surge: ST flip + last 1h bar volume >= 3× MA20.
    Returns signal dict or None.
    """
    vr = compute_volume_ratio(candles_1h, n_ma=20)
    if vr < VOLUME_SURGE_THRESHOLD:
        return None
    # SL/TP
    r_pct = abs((entry - sl) / entry) if entry and sl else 0
    if r_pct == 0:
        return None
    tp_R = STRATEGY_TP_R['volume_surge']
    if direction == 'LONG':
        tp = entry * (1 + tp_R * r_pct)
    else:
        tp = entry * (1 - tp_R * r_pct)
    return {
        'strategy': 'volume_surge',
        'pair': pair,
        'direction': direction,
        'entry': entry,
        'sl': sl,
        'tp': tp,
        'tp_R': tp_R,
        'vol_ratio': round(vr, 2),
        'risk_pct': round(r_pct * 100, 3),
    }


def detect_volume_accum(pair: str, direction: str, entry: float, sl: float,
                       candles_1h: list[dict]) -> Optional[dict]:
    """🔋 Volume Accumulation: 3 consecutive rising volume bars BEFORE current."""
    if not has_volume_accumulation(candles_1h, VOL_ACCUM_BARS):
        return None
    r_pct = abs((entry - sl) / entry) if entry and sl else 0
    if r_pct == 0:
        return None
    tp_R = STRATEGY_TP_R['vol_accum']
    if direction == 'LONG':
        tp = entry * (1 + tp_R * r_pct)
    else:
        tp = entry * (1 - tp_R * r_pct)
    # Bonus: include current vol_ratio for diagnostic
    vr = compute_volume_ratio(candles_1h, n_ma=20)
    return {
        'strategy': 'vol_accum',
        'pair': pair,
        'direction': direction,
        'entry': entry,
        'sl': sl,
        'tp': tp,
        'tp_R': tp_R,
        'bars_rising': VOL_ACCUM_BARS,
        'vol_ratio': round(vr, 2),
        'risk_pct': round(r_pct * 100, 3),
    }


def detect_triple_confluence(pair: str, direction: str, entry: float, sl: float,
                             flip_ts: datetime) -> Optional[dict]:
    """🐉 Triple Confluence: 2+ different source signals same pair + same dir
    within ±4h of ST flip timestamp.

    Sources counted: cryptovizor, anomaly, confluence, cluster, supertrend (current).
    """
    from database import _signals, _anomalies, _confluence, _clusters
    pair_norm = pair.replace('/', '').upper()
    pair_slash = pair_norm[:-4] + '/USDT' if pair_norm.endswith('USDT') else pair
    window_lo = flip_ts - timedelta(hours=CROSS_SOURCE_WINDOW_H)
    window_hi = flip_ts + timedelta(hours=CROSS_SOURCE_WINDOW_H)

    sources_found = set(['supertrend'])  # ST is always present (we're called from ST)
    aligned_signals = []

    # Check cryptovizor signals
    try:
        for s in _signals().find({
            'source': 'cryptovizor',
            'received_at': {'$gte': window_lo.replace(tzinfo=None),
                           '$lte': window_hi.replace(tzinfo=None)},
            '$or': [{'pair': pair_slash}, {'pair': pair_norm}],
            'direction': direction,
        }, {'pair': 1, 'received_at': 1}).limit(3):
            sources_found.add('cryptovizor')
            aligned_signals.append({
                'source': 'cryptovizor',
                'at': s.get('received_at').isoformat() if s.get('received_at') else None,
            })
            break  # one is enough for this source
    except Exception as e:
        logger.debug(f'[triple] CV check fail: {e}')

    # Check anomalies
    try:
        for a in _anomalies().find({
            'detected_at': {'$gte': window_lo.replace(tzinfo=None),
                           '$lte': window_hi.replace(tzinfo=None)},
            '$or': [{'symbol': pair_norm}, {'pair': pair_slash}],
            'direction': direction,
        }, {'detected_at': 1}).limit(1):
            sources_found.add('anomaly')
            aligned_signals.append({
                'source': 'anomaly',
                'at': a.get('detected_at').isoformat() if a.get('detected_at') else None,
            })
    except Exception as e:
        logger.debug(f'[triple] anomaly check fail: {e}')

    # Confluence
    try:
        for c in _confluence().find({
            'detected_at': {'$gte': window_lo.replace(tzinfo=None),
                           '$lte': window_hi.replace(tzinfo=None)},
            '$or': [{'symbol': pair_norm}, {'pair': pair_slash}],
            'direction': direction,
        }, {'detected_at': 1}).limit(1):
            sources_found.add('confluence')
            aligned_signals.append({
                'source': 'confluence',
                'at': c.get('detected_at').isoformat() if c.get('detected_at') else None,
            })
    except Exception as e:
        logger.debug(f'[triple] confluence check fail: {e}')

    # Cluster
    try:
        for cl in _clusters().find({
            'trigger_at': {'$gte': window_lo.replace(tzinfo=None),
                          '$lte': window_hi.replace(tzinfo=None)},
            'pair': pair_slash,
            'direction': direction,
        }, {'trigger_at': 1}).limit(1):
            sources_found.add('cluster')
            aligned_signals.append({
                'source': 'cluster',
                'at': cl.get('trigger_at').isoformat() if cl.get('trigger_at') else None,
            })
    except Exception as e:
        logger.debug(f'[triple] cluster check fail: {e}')

    # Need at least 2 different sources (including supertrend current)
    if len(sources_found) < TRIPLE_CONFLUENCE_MIN_SOURCES:
        return None

    r_pct = abs((entry - sl) / entry) if entry and sl else 0
    if r_pct == 0:
        return None
    tp_R = STRATEGY_TP_R['triple_confluence']
    if direction == 'LONG':
        tp = entry * (1 + tp_R * r_pct)
    else:
        tp = entry * (1 - tp_R * r_pct)
    return {
        'strategy': 'triple_confluence',
        'pair': pair,
        'direction': direction,
        'entry': entry,
        'sl': sl,
        'tp': tp,
        'tp_R': tp_R,
        'source_count': len(sources_found),
        'sources': sorted(sources_found),
        'aligned_signals': aligned_signals,
        'risk_pct': round(r_pct * 100, 3),
    }


async def run_detectors_on_flip(pair: str, direction: str, entry: float,
                                sl: float, flip_ts: datetime,
                                signal_id: Optional[int] = None,
                                tier: Optional[str] = None) -> list[dict]:
    """Called from supertrend_tracker after each ST flip.
    Returns list of strategy signals that triggered (0-3 entries).
    Saves to new_strategy_signals collection.
    """
    from exchange import get_klines_any

    # Fetch 1h klines for vol_ratio + accum checks
    try:
        candles_1h = await asyncio.to_thread(get_klines_any, pair, '1h', 25)
    except Exception as e:
        logger.warning(f'[new-strategies] klines fetch fail {pair}: {e}')
        candles_1h = []

    if not candles_1h or len(candles_1h) < 22:
        logger.debug(f'[new-strategies] insufficient klines for {pair}: {len(candles_1h)}')
        return []

    triggered = []

    # 🌊 Volume Surge
    try:
        vs = detect_volume_surge(pair, direction, entry, sl, candles_1h)
        if vs:
            triggered.append(vs)
    except Exception:
        logger.exception('[new-strategies] volume_surge fail')

    # 🔋 Volume Accumulation
    try:
        va = detect_volume_accum(pair, direction, entry, sl, candles_1h)
        if va:
            triggered.append(va)
    except Exception:
        logger.exception('[new-strategies] vol_accum fail')

    # 🐉 Triple Confluence (DB queries via to_thread)
    try:
        tc = await asyncio.to_thread(
            detect_triple_confluence, pair, direction, entry, sl, flip_ts,
        )
        if tc:
            triggered.append(tc)
    except Exception:
        logger.exception('[new-strategies] triple_confluence fail')

    # Persist all triggered to Mongo
    if triggered:
        await _save_strategy_signals(triggered, flip_ts, signal_id, tier)
        # Send Telegram alerts (BOT13)
        for sig in triggered:
            asyncio.create_task(_send_strategy_alert(sig))
        # Auto-paper trade — для каждой сработавшей стратегии открываем
        # позицию через paper_trader (если на этой паре ещё нет открытой,
        # paper сам делает duplicate detection). Backtest validated edge.
        asyncio.create_task(_auto_paper_for_strategies(triggered, pair, direction, entry, sl))

    return triggered


async def _auto_paper_for_strategies(triggered: list[dict], pair: str,
                                     direction: str, entry: float, sl: float) -> None:
    """Авто-открытие paper позиции по сильнейшей сработавшей стратегии.
    Использует strategy-specific TP target из детектора. Если на паре уже
    открыта позиция (от ST signal-а параллельно или раньше) — paper_trader
    сам отклонит как DUPLICATE (это корректно)."""
    if not triggered:
        return
    # Берём стратегию с самым большим TP_R (сильнейший edge):
    # 🌊 Volume Surge (TP=2.5R) > 🐉 Triple Confluence (TP=2R) > 🔋 Vol Accum (TP=1.5R)
    best = max(triggered, key=lambda t: t.get('tp_R', 0))
    sym = pair.replace('/', '').upper()
    if not sym.endswith('USDT'):
        sym = sym + 'USDT'
    score = best.get('source_count') or best.get('vol_ratio') or best.get('bars_rising') or 0
    extra_label = STRATEGY_LABEL.get(best['strategy'], best['strategy'])
    emoji = STRATEGY_EMOJI.get(best['strategy'], '✨')
    signal_data = {
        'symbol': sym,
        'pair': pair,
        'direction': direction,
        'entry': entry,
        'sl': sl,
        'tp1': best.get('tp'),
        'source': best['strategy'],  # 'volume_surge' / 'triple_confluence' / 'vol_accum'
        'score': score,
        'pattern': f'{emoji} {extra_label} (after ST flip)',
        'is_top_pick': False,
        # Метаданные для UI / journal
        'ns_strategy': best['strategy'],
        'ns_tp_R': best.get('tp_R'),
        'ns_vol_ratio': best.get('vol_ratio'),
        'ns_sources': best.get('sources'),
    }
    try:
        import paper_trader as pt
        # Timeout 30s — paper_trader.on_signal делает много проверок (RSI, anti-cluster,
        # entry checker via to_thread). Если не успело за 30с — skip.
        await asyncio.wait_for(pt.on_signal(signal_data), timeout=30.0)
    except asyncio.TimeoutError:
        logger.warning(f'[new-strategies] auto-paper TIMEOUT {best["strategy"]}/{pair}')
    except Exception as e:
        logger.warning(f'[new-strategies] auto-paper fail {best["strategy"]}/{pair}: {e}')


async def _save_strategy_signals(triggered: list[dict], flip_ts: datetime,
                                 signal_id: Optional[int], tier: Optional[str]) -> None:
    """Save strategy signals to new_strategy_signals collection (via to_thread).
    Дедупликация: для каждой (pair, direction, strategy) разрешён только 1
    сигнал в окне 60 мин. Это защищает от стакания эмодзи когда ST flip
    срабатывает на VIP/MTF/Daily tiers одновременно."""
    def _sync():
        try:
            from database import _get_db, utcnow
            from datetime import timedelta
            col = _get_db().new_strategy_signals
            dedup_window = timedelta(minutes=60)
            cutoff = utcnow() - dedup_window
            for sig in triggered:
                # Проверяем нет ли дубля same pair+direction+strategy в окне
                existing = col.find_one({
                    'pair': sig['pair'],
                    'direction': sig['direction'],
                    'strategy': sig['strategy'],
                    'created_at': {'$gte': cutoff},
                })
                if existing:
                    logger.debug(
                        f"[new-strategies] dedup skip {sig['strategy']}/{sig['pair']} "
                        f"— existing within 60min"
                    )
                    continue
                doc = {
                    **sig,
                    'state': 'WAITING',
                    'st_flip_at': flip_ts.replace(tzinfo=None) if flip_ts.tzinfo else flip_ts,
                    'st_signal_id': signal_id,
                    'st_tier': tier,
                    'created_at': utcnow(),
                    'updated_at': utcnow(),
                }
                try:
                    col.insert_one(doc)
                except Exception as e:
                    logger.debug(f'[new-strategies] insert fail {sig["strategy"]}/{sig["pair"]}: {e}')
        except Exception:
            logger.exception('[new-strategies] save fail')
    try:
        await asyncio.wait_for(asyncio.to_thread(_sync), timeout=5.0)
    except (asyncio.TimeoutError, Exception):
        pass


async def update_waiting_outcomes() -> dict:
    """Background updater: проверяет WAITING сигналы — попала ли цена в TP/SL.
    Вызывается периодически из watcher loop. Lookback 24h max — старее = TIMEOUT.
    """
    from database import _get_db, utcnow
    from datetime import timedelta
    from exchange import get_klines_any

    def _load_waiting():
        col = _get_db().new_strategy_signals
        cutoff = utcnow() - timedelta(hours=24)
        return list(col.find({
            'state': 'WAITING',
            'created_at': {'$gte': cutoff},
        }).limit(100))

    waiting = await asyncio.to_thread(_load_waiting)
    if not waiting:
        return {'checked': 0, 'updated': 0}

    # Group by pair to share klines fetch
    by_pair: dict[str, list] = {}
    for w in waiting:
        by_pair.setdefault(w['pair'], []).append(w)

    updated = 0
    timeouts = 0
    for pair, sigs in by_pair.items():
        try:
            candles = await asyncio.to_thread(get_klines_any, pair, '1h', 30)
        except Exception:
            continue
        if not candles or len(candles) < 5:
            continue
        for sig in sigs:
            entry = sig.get('entry')
            sl = sig.get('sl')
            tp = sig.get('tp')
            direction = sig.get('direction')
            created_at = sig.get('created_at')
            if not (entry and sl and tp and direction and created_at):
                continue
            # Time of signal in ms
            sig_ts = int(created_at.replace(tzinfo=timezone.utc).timestamp() * 1000) \
                if created_at.tzinfo is None else int(created_at.timestamp() * 1000)
            # Find first candle at or after sig_ts and walk forward
            outcome = None
            outcome_price = None
            outcome_at = None
            for c in candles:
                if c['t'] < sig_ts:
                    continue
                if direction == 'LONG':
                    if c['l'] <= sl:
                        outcome = 'SL'; outcome_price = sl
                        outcome_at = datetime.fromtimestamp(c['t']/1000, tz=timezone.utc); break
                    if c['h'] >= tp:
                        outcome = 'TP'; outcome_price = tp
                        outcome_at = datetime.fromtimestamp(c['t']/1000, tz=timezone.utc); break
                else:
                    if c['h'] >= sl:
                        outcome = 'SL'; outcome_price = sl
                        outcome_at = datetime.fromtimestamp(c['t']/1000, tz=timezone.utc); break
                    if c['l'] <= tp:
                        outcome = 'TP'; outcome_price = tp
                        outcome_at = datetime.fromtimestamp(c['t']/1000, tz=timezone.utc); break
            # Timeout: 24h passed without outcome
            age_h = (utcnow() - (created_at if created_at.tzinfo else
                                 created_at.replace(tzinfo=timezone.utc))).total_seconds() / 3600
            if outcome is None and age_h >= 24:
                outcome = 'TIMEOUT'
                outcome_price = candles[-1]['c'] if candles else None
                outcome_at = utcnow()
            if outcome is None:
                continue
            # Compute pnl_pct
            pnl_pct = 0
            if outcome_price and entry:
                if direction == 'LONG':
                    pnl_pct = (outcome_price - entry) / entry * 100
                else:
                    pnl_pct = (entry - outcome_price) / entry * 100
            def _save(sig_id=sig['_id'], outcome=outcome, outcome_price=outcome_price,
                     outcome_at=outcome_at, pnl_pct=pnl_pct):
                _get_db().new_strategy_signals.update_one(
                    {'_id': sig_id},
                    {'$set': {
                        'state': outcome,
                        'exit_price': outcome_price,
                        'exit_at': (outcome_at.replace(tzinfo=None) if outcome_at and outcome_at.tzinfo
                                    else outcome_at),
                        'pnl_pct': round(pnl_pct, 3),
                        'updated_at': utcnow(),
                    }}
                )
            try:
                await asyncio.wait_for(asyncio.to_thread(_save), timeout=3.0)
                updated += 1
                if outcome == 'TIMEOUT':
                    timeouts += 1
            except (asyncio.TimeoutError, Exception):
                pass

    if updated:
        logger.info(f'[new-strategies] updated {updated} outcomes ({timeouts} timeouts)')
    return {'checked': len(waiting), 'updated': updated, 'timeouts': timeouts}


async def _send_strategy_alert(sig: dict) -> None:
    """Send Telegram alert via BOT13. Strategy emoji + pair + dir + entry/sl/tp."""
    try:
        from config import BOT13_BOT_TOKEN, NEW_STRATEGY_CHAT_ID
    except Exception:
        return
    if not BOT13_BOT_TOKEN or not NEW_STRATEGY_CHAT_ID:
        return
    chat_id = NEW_STRATEGY_CHAT_ID
    emoji = STRATEGY_EMOJI.get(sig['strategy'], '✨')
    label = STRATEGY_LABEL.get(sig['strategy'], sig['strategy'])
    pair = sig['pair']
    direction = sig['direction']
    dir_emoji = '🟢' if direction == 'LONG' else '🔴'
    entry = sig['entry']
    sl = sig['sl']
    tp = sig['tp']
    tp_R = sig.get('tp_R', 1.5)
    risk_pct = sig.get('risk_pct', 0)
    extra = ''
    if sig['strategy'] == 'volume_surge':
        extra = f"\n📊 Vol ratio: <b>{sig.get('vol_ratio',0):.1f}×</b> MA20"
    elif sig['strategy'] == 'triple_confluence':
        srcs = ', '.join(sig.get('sources', []))
        extra = f"\n🔀 Sources: <b>{sig.get('source_count',0)}</b> — {srcs}"
    elif sig['strategy'] == 'vol_accum':
        extra = f"\n📈 Rising vol bars: <b>{sig.get('bars_rising',3)}</b>"

    text = (
        f"{emoji} <b>{label}</b>\n\n"
        f"<b>{pair}</b> · {dir_emoji} <b>{direction}</b>\n"
        f"Entry: <code>{entry:.6f}</code>\n"
        f"SL:    <code>{sl:.6f}</code> ({risk_pct:.2f}%)\n"
        f"TP:    <code>{tp:.6f}</code> ({tp_R}R)"
        f"{extra}\n\n"
        f"<i>Backtest validated · observation-only</i>"
    )
    try:
        import httpx
        url = f'https://api.telegram.org/bot{BOT13_BOT_TOKEN}/sendMessage'
        async with httpx.AsyncClient(timeout=10) as c:
            await c.post(url, json={
                'chat_id': chat_id,
                'text': text,
                'parse_mode': 'HTML',
                'disable_web_page_preview': True,
            })
    except Exception as e:
        logger.warning(f'[new-strategies] telegram send fail {sig["strategy"]}/{sig["pair"]}: {e}')
