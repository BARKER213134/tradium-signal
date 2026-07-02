"""COMBO signal detector — основан на backtest precondition analysis.

Fired when:
- Target signal приходит: st_vip OR triple_confluence
- В 24h preceding window same direction same pair собираем preceding signals
- Compute combo_score = positive weights минус anti-marker weights
- Если score ≥ 30 → COMBO signal fires

Backtest 14d showed:
- WIN markers: st_vip (+12.9pp), triple_confluence (+7.5pp), confluence (neutral)
- ANTI markers: st_mtf (-13.0pp), cv_flip (-11.7pp), cryptovizor (-10.2pp),
  vol_accum (-7.2pp), st_daily (-6.4pp)

Stored как new_strategy_signals with strategy='combo'.
Emoji: 🧠 (composite intelligence).
"""
from __future__ import annotations
import logging
import time
from datetime import datetime, timezone, timedelta
from collections import defaultdict

logger = logging.getLogger(__name__)

# Weights (calibrated via backtest)
WEIGHTS = {
    'st_vip':            +30,
    'triple_confluence': +25,
    'confluence':        +15,
    'tradium':           +5,
    'rsi_cross_12h':     +5,
    # Anti-markers (subtract)
    'st_mtf':            -10,
    'cv_flip':           -10,
    'cryptovizor':       -8,
    'st_daily':          -5,
    'vol_accum':         -5,
}

PRECEDING_WINDOW_S = 24 * 3600  # 24h
COMBO_THRESHOLD = 30
# Cooldown — не более 1 COMBO per pair per 6h
COMBO_COOLDOWN_S = 6 * 3600


def collect_preceding_signals(db, pair: str, ts: int, direction: str) -> list:
    """Returns list of (ts, source) for same pair, same direction в окне 24h до ts."""
    cutoff_dt = datetime.fromtimestamp(ts - PRECEDING_WINDOW_S, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    out = []
    # cryptovizor
    try:
        for s in db.signals.find({
            'source': 'cryptovizor', 'pair': pair,
            'pattern_triggered': True,
            'pattern_triggered_at': {'$gte': cutoff_dt, '$lt': end_dt},
        }, {'direction':1, 'pattern_triggered_at':1}):
            if (s.get('direction','') or '').upper() == direction:
                out.append((int(s['pattern_triggered_at'].timestamp()), 'cryptovizor'))
    except Exception: pass
    # tradium
    try:
        for s in db.signals.find({
            'source': 'tradium', 'pair': pair,
            'received_at': {'$gte': cutoff_dt, '$lt': end_dt},
        }, {'direction':1, 'received_at':1}):
            if (s.get('direction','') or '').upper() == direction:
                out.append((int(s['received_at'].timestamp()), 'tradium'))
    except Exception: pass
    # supertrend per tier
    try:
        for s in db.supertrend_signals.find({
            'pair': pair, 'flip_at': {'$gte': cutoff_dt, '$lt': end_dt},
        }, {'direction':1, 'flip_at':1, 'tier':1}):
            if (s.get('direction','') or '').upper() == direction:
                tier = s.get('tier', 'mtf')
                out.append((int(s['flip_at'].timestamp()), f'st_{tier}'))
    except Exception: pass
    # anomaly удалён (2026-07-02)
    # confluence
    try:
        for s in db.confluence.find({
            'pair': pair, 'detected_at': {'$gte': cutoff_dt, '$lt': end_dt},
        }, {'direction':1, 'detected_at':1}):
            if (s.get('direction','') or '').upper() == direction:
                out.append((int(s['detected_at'].timestamp()), 'confluence'))
    except Exception: pass
    # new_strategies
    try:
        for s in db.new_strategy_signals.find({
            'pair': pair, 'created_at': {'$gte': cutoff_dt, '$lt': end_dt},
        }, {'direction':1, 'created_at':1, 'strategy':1}):
            if (s.get('direction','') or '').upper() == direction:
                out.append((int(s['created_at'].timestamp()), s.get('strategy', '?')))
    except Exception: pass
    # cv_flip
    try:
        for s in db.cv_flip_signals.find({
            'pair': pair, 'cv_triggered_at': {'$gte': cutoff_dt, '$lt': end_dt},
        }, {'direction':1, 'cv_triggered_at':1}):
            if (s.get('direction','') or '').upper() == direction:
                out.append((int(s['cv_triggered_at'].timestamp()), 'cv_flip'))
    except Exception: pass
    # rsi_cross_12h
    try:
        for s in db.rsi_sma_cross_signals.find({
            'pair': pair, 'detected_at': {'$gte': cutoff_dt, '$lt': end_dt},
        }, {'direction':1, 'detected_at':1}):
            if (s.get('direction','') or '').upper() == direction:
                out.append((int(s['detected_at'].timestamp()), 'rsi_cross_12h'))
    except Exception: pass
    out.sort()
    return out


def compute_combo_score(preceding: list, target_src: str) -> tuple[int, dict]:
    """Returns (score, breakdown). Counts unique sources in preceding window.
    target_src — текущий источник который триггерит combo (st_vip / triple_confluence).
    """
    unique_srcs = set(s for _, s in preceding)
    breakdown = {}
    score = 0
    # Add target as positive (it's the trigger)
    if target_src in WEIGHTS:
        breakdown[target_src] = WEIGHTS[target_src]
        score += WEIGHTS[target_src]
    # Add preceding sources
    for src in unique_srcs:
        if src == target_src: continue  # уже учтён
        w = WEIGHTS.get(src, 0)
        if w != 0:
            breakdown[src] = w
            score += w
    return score, breakdown


def check_recent_combo(db, pair: str, ts: int) -> bool:
    """Returns True если есть combo for pair в last COMBO_COOLDOWN_S секунд."""
    try:
        cutoff_dt = datetime.fromtimestamp(ts - COMBO_COOLDOWN_S, tz=timezone.utc)
        recent = db.new_strategy_signals.find_one({
            'pair': pair, 'strategy': 'combo',
            'created_at': {'$gte': cutoff_dt},
        })
        return recent is not None
    except Exception:
        return False


def maybe_fire_combo(signal_data: dict) -> dict | None:
    """Called when target signal (st_vip / triple_confluence) arrives.
    Returns COMBO doc if fired, else None.
    """
    try:
        from database import _get_db, utcnow
        db = _get_db()

        pair = signal_data.get('pair', '')
        if not pair: return None
        # Normalize source — st_vip может приходить как 'supertrend' с tier='vip'
        source = signal_data.get('source', '')
        st_tier = signal_data.get('st_tier', '')
        if source == 'supertrend':
            if st_tier == 'vip': target_src = 'st_vip'
            elif st_tier == 'mtf': target_src = 'st_mtf'
            elif st_tier == 'daily': target_src = 'st_daily'
            else: return None  # ignore unknown ST tier
        elif source == 'triple_confluence':
            target_src = 'triple_confluence'
        else:
            return None  # only st_vip & triple_confluence trigger combo

        # COMBO fires ONLY от st_vip / triple_confluence triggers
        if target_src not in ('st_vip', 'triple_confluence'):
            return None

        direction = (signal_data.get('direction', '') or '').upper()
        entry = signal_data.get('entry') or signal_data.get('price')
        if not entry: return None
        try: entry = float(entry)
        except Exception: return None

        ts = int(time.time())
        if check_recent_combo(db, pair, ts):
            logger.debug(f'[combo] cooldown active for {pair}')
            return None

        # Collect 24h preceding
        preceding = collect_preceding_signals(db, pair, ts, direction)
        score, breakdown = compute_combo_score(preceding, target_src)

        if score < COMBO_THRESHOLD:
            logger.debug(f'[combo] {pair} score {score} < {COMBO_THRESHOLD}, skip')
            return None

        # Fire COMBO!
        unique_srcs = list(set(s for _, s in preceding))
        doc = {
            'pair': pair,
            'symbol': pair.replace('/', '').upper(),
            'direction': direction,
            'entry': entry,
            'strategy': 'combo',
            'combo_score': score,
            'combo_breakdown': breakdown,
            'trigger_source': target_src,
            'preceding_sources': unique_srcs,
            'preceding_count': len(preceding),
            'created_at': utcnow(),
            'state': 'NEW',
            # TP/SL ATR-based — оставим базовые
            'tp_R': 2.0,
        }
        try:
            db.new_strategy_signals.insert_one(doc)
            logger.info(f'[combo] 🧠 FIRED {pair} {direction} '
                         f'score={score} trigger={target_src} preceding={unique_srcs}')
        except Exception as e:
            logger.warning(f'[combo] insert fail: {e}')
        return doc
    except Exception:
        logger.exception('[combo] maybe_fire fail')
        return None


_BACKFILL_STATE = {
    'running': False, 'days': 0, 'targets_total': 0, 'targets_done': 0,
    'fired': 0, 'skipped_score': 0, 'skipped_cooldown': 0,
    'started_at': None, 'finished_at': None, 'error': None,
}


def get_backfill_state():
    return dict(_BACKFILL_STATE)


# ─── BACKFILL за N дней ─────────────────────────────────────────────────
def backfill_combo(days: int = 30) -> dict:
    """Сканит исторические target signals (st_vip + triple_confluence)
    за last N days. Для каждого считает combo score и инсертит quallified
    в new_strategy_signals.
    """
    global _BACKFILL_STATE
    _BACKFILL_STATE = {
        'running': True, 'days': days, 'targets_total': 0, 'targets_done': 0,
        'fired': 0, 'skipped_score': 0, 'skipped_cooldown': 0,
        'started_at': datetime.now(timezone.utc).isoformat(),
        'finished_at': None, 'error': None,
    }
    from database import _get_db
    db = _get_db()
    since = datetime.now(timezone.utc) - timedelta(days=days)

    # Load target signals
    targets = []
    # st_vip
    for s in db.supertrend_signals.find({
        'flip_at': {'$gte': since}, 'tier': 'vip',
    }, {'pair':1,'direction':1,'entry_price':1,'flip_at':1}):
        if s.get('entry_price') and s.get('pair'):
            targets.append({
                'pair': s['pair'],
                'direction': (s.get('direction','') or '').upper(),
                'entry': float(s['entry_price']),
                'at': s['flip_at'],
                'at_ts': int(s['flip_at'].timestamp()),
                'target_src': 'st_vip',
            })
    # triple_confluence
    for s in db.new_strategy_signals.find({
        'created_at': {'$gte': since}, 'strategy': 'triple_confluence',
    }, {'pair':1,'direction':1,'entry':1,'created_at':1}):
        if s.get('entry') and s.get('pair'):
            targets.append({
                'pair': s['pair'],
                'direction': (s.get('direction','') or '').upper(),
                'entry': float(s['entry']),
                'at': s['created_at'],
                'at_ts': int(s['created_at'].timestamp()),
                'target_src': 'triple_confluence',
            })

    # Sort by time (для применения cooldown chronologically)
    targets.sort(key=lambda x: x['at_ts'])
    _BACKFILL_STATE['targets_total'] = len(targets)
    logger.info(f'[combo-backfill] {len(targets)} target signals за {days}d')

    fired = 0
    skipped_score = 0
    skipped_cooldown = 0
    last_combo_per_pair: dict = {}  # in-memory cooldown tracker

    try:
        for idx, tgt in enumerate(targets):
            _BACKFILL_STATE['targets_done'] = idx + 1
            # Cooldown check (in-memory)
            last_ts = last_combo_per_pair.get(tgt['pair'], 0)
            if tgt['at_ts'] - last_ts < COMBO_COOLDOWN_S:
                skipped_cooldown += 1
                _BACKFILL_STATE['skipped_cooldown'] = skipped_cooldown
                continue
            preceding = collect_preceding_signals(db, tgt['pair'], tgt['at_ts'], tgt['direction'])
            score, breakdown = compute_combo_score(preceding, tgt['target_src'])
            if score < COMBO_THRESHOLD:
                skipped_score += 1
                _BACKFILL_STATE['skipped_score'] = skipped_score
                continue
            unique_srcs = list(set(s for _, s in preceding))
            doc = {
                'pair': tgt['pair'],
                'symbol': tgt['pair'].replace('/', '').upper(),
                'direction': tgt['direction'],
                'entry': tgt['entry'],
                'strategy': 'combo',
                'combo_score': score,
                'combo_breakdown': breakdown,
                'trigger_source': tgt['target_src'],
                'preceding_sources': unique_srcs,
                'preceding_count': len(preceding),
                'created_at': tgt['at'],
                'state': 'BACKFILLED',
                'tp_R': 2.0,
            }
            # Avoid duplicate insert
            try:
                existing = db.new_strategy_signals.find_one({
                    'pair': tgt['pair'], 'strategy': 'combo',
                    'created_at': tgt['at'],
                })
                if existing: continue
                db.new_strategy_signals.insert_one(doc)
                fired += 1
                _BACKFILL_STATE['fired'] = fired
                last_combo_per_pair[tgt['pair']] = tgt['at_ts']
            except Exception as e:
                logger.debug(f'[combo-backfill] insert {tgt["pair"]}: {e}')
    except Exception as e:
        logger.exception(f'[combo-backfill] error: {e}')
        _BACKFILL_STATE['error'] = str(e)

    logger.info(f'[combo-backfill] DONE — fired={fired} '
                 f'skipped_score={skipped_score} skipped_cooldown={skipped_cooldown}')
    _BACKFILL_STATE['running'] = False
    _BACKFILL_STATE['finished_at'] = datetime.now(timezone.utc).isoformat()
    return {
        'fired': fired, 'skipped_score': skipped_score,
        'skipped_cooldown': skipped_cooldown,
        'targets_scanned': len(targets), 'days': days,
    }
