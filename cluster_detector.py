"""Cluster Detector — детектор кластерных сигналов в реальном времени.

Кластер = N+ сигналов одного направления на одной паре за окно T часов.
Параметры настраиваемы через UI (хранятся в MongoDB system._id='cluster_config').

Бэктест на 96ч показал: окно 8ч, min 2 → WR 78.6%, Avg PnL +0.51%.
"""
from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Optional
import logging

from database import _signals, _anomalies, _confluence, _clusters, _cluster_config, utcnow
from pymongo import DESCENDING

logger = logging.getLogger(__name__)


# ── Конфигурация ────────────────────────────────────────────────
DEFAULT_CONFIG = {
    "window_h": 8,           # окно сбора сигналов
    "min_count": 2,          # минимум сигналов для триггера
    "dedup_h": 2,            # не чаще 1 раза в N часов на паре+направлении
    "tp_pct": 1.5,           # фикс TP %
    "sl_pct": 1.5,           # фикс SL %
    "leverage_boost": 2.0,   # paper trading boost при обычном кластере
    "strong_boost": 3.0,     # при cluster+reversal combo
}


def get_config() -> dict:
    """Возвращает конфиг кластеров (из MongoDB, с дефолтами)."""
    try:
        doc = _cluster_config().find_one({"_id": "cluster_config"})
        if not doc:
            return DEFAULT_CONFIG.copy()
        out = DEFAULT_CONFIG.copy()
        out.update({k: doc[k] for k in DEFAULT_CONFIG if k in doc})
        return out
    except Exception as e:
        logger.warning(f"Cluster config load: {e}")
        return DEFAULT_CONFIG.copy()


def save_config(cfg: dict) -> dict:
    """Сохраняет конфиг (только валидные ключи)."""
    safe = {}
    for k, v in cfg.items():
        if k not in DEFAULT_CONFIG:
            continue
        try:
            safe[k] = float(v) if k in ("tp_pct", "sl_pct", "leverage_boost", "strong_boost") else int(v)
        except (TypeError, ValueError):
            continue
    if not safe:
        return get_config()
    _cluster_config().update_one({"_id": "cluster_config"}, {"$set": safe}, upsert=True)
    return get_config()


# ── Сбор сигналов по паре+направлению в окне ───────────────────
def _norm_pair(pair: str) -> str:
    """Нормализованная пара для дедупа: BTC/USDT и BTCUSDT → BTC/USDT."""
    if not pair:
        return ""
    p = pair.upper().strip()
    if "/" in p:
        return p
    if p.endswith("USDT"):
        return p[:-4] + "/USDT"
    return p


def collect_signals_for(pair: str, direction: str, end_at: datetime, window_h: int,
                         include_clusters: bool = False) -> list[dict]:
    """Собирает все сигналы на данной паре+направлении в окне [end_at - window_h, end_at].
    Возвращает список unified-словарей: {source, at, price, meta}

    include_clusters=False по умолчанию — чтобы should_trigger_cluster НЕ учитывал
    предыдущие кластеры как исходные сигналы (иначе дубль подсчёта).
    Для UI-маркеров (pair-signals endpoint) — выставляем True чтобы кластеры рисовались.
    """
    norm = _norm_pair(pair)
    sym_variants = {norm, norm.replace("/", "")}  # ETH/USDT и ETHUSDT
    start = end_at - timedelta(hours=window_h)
    out = []

    # CV
    for s in _signals().find({
        "source": "cryptovizor", "pattern_triggered": True,
        "pattern_triggered_at": {"$gte": start, "$lte": end_at},
        "direction": direction, "pair": {"$in": list(sym_variants)},
    }):
        out.append({
            "source": "cryptovizor",
            "at": s["pattern_triggered_at"],
            "price": s.get("pattern_price") or s.get("entry"),
            "meta": {"pattern_name": s.get("pattern_name", ""),
                     "ai_score": s.get("ai_score")},
        })

    # Tradium
    for s in _signals().find({
        "source": "tradium",
        "received_at": {"$gte": start, "$lte": end_at},
        "direction": direction, "pair": {"$in": list(sym_variants)},
    }):
        out.append({
            "source": "tradium",
            "at": s["received_at"],
            "price": s.get("entry"),
            "meta": {"ai_score": s.get("ai_score"),
                     "setup_number": s.get("setup_number")},
        })

    # Anomaly — symbol может быть ETHUSDT или pair ETH/USDT
    for a in _anomalies().find({
        "detected_at": {"$gte": start, "$lte": end_at},
        "direction": direction,
        "$or": [{"symbol": norm.replace("/", "")}, {"pair": norm}],
    }):
        out.append({
            "source": "anomaly",
            "at": a["detected_at"],
            "price": a.get("price"),
            "meta": {"score": a.get("score"),
                     "types": [x.get("type") for x in a.get("anomalies") or []][:5]},
        })

    # Confluence
    for c in _confluence().find({
        "detected_at": {"$gte": start, "$lte": end_at},
        "direction": direction,
        "$or": [{"symbol": norm.replace("/", "")}, {"pair": norm}],
    }):
        out.append({
            "source": "confluence",
            "at": c["detected_at"],
            "price": c.get("price"),
            "meta": {"score": c.get("score"),
                     "strength": c.get("strength"),
                     "pattern": c.get("pattern")},
        })

    # Clusters — композитные сигналы (только для UI-маркеров, не для cluster-triggering)
    if include_clusters:
        for cl in _clusters().find({
            "trigger_at": {"$gte": start, "$lte": end_at},
            "direction": direction,
            "$or": [{"symbol": norm.replace("/", "")}, {"pair": norm}],
        }):
            out.append({
                "source": "cluster",
                "at": cl["trigger_at"],
                "price": cl.get("trigger_price"),
                "meta": {"strength": cl.get("strength"),
                         "status": cl.get("status"),
                         "signals_count": cl.get("signals_count"),
                         "sources_count": cl.get("sources_count"),
                         "tp": cl.get("tp_price"),
                         "sl": cl.get("sl_price"),
                         "pnl_pct": cl.get("pnl_percent")},
            })

    out.sort(key=lambda x: x["at"])
    return out


# ── Pending state (для UI badge) ───────────────────────────────
def compute_pending_state(pair: str, direction: str) -> dict:
    """Возвращает {count, min, state: 'idle'/'pending'/'ready', time_left_h}."""
    cfg = get_config()
    now = utcnow()
    sigs = collect_signals_for(pair, direction, now, cfg["window_h"])
    count = len(sigs)
    min_n = cfg["min_count"]

    if count == 0:
        return {"count": 0, "min": min_n, "state": "idle", "time_left_h": 0}
    if count >= min_n:
        return {"count": count, "min": min_n, "state": "ready", "time_left_h": 0}

    # Pending: окно истекает когда первый сигнал выходит за рамки
    first_at = sigs[0]["at"]
    expires_at = first_at + timedelta(hours=cfg["window_h"])
    time_left_h = max(0, (expires_at - now).total_seconds() / 3600)
    return {"count": count, "min": min_n, "state": "pending", "time_left_h": round(time_left_h, 1)}


def get_pending_clusters(limit: int = 50) -> list[dict]:
    """Возвращает список пар которые сейчас в pending состоянии (1/N, ждём ещё).
    Сканирует все уникальные пары за последнее окно."""
    cfg = get_config()
    now = utcnow()
    since = now - timedelta(hours=cfg["window_h"])
    # Собираем уникальные (pair, direction) из последнего окна
    pairs_dirs: set = set()

    for s in _signals().find({
        "source": {"$in": ["cryptovizor", "tradium"]},
        "$or": [{"pattern_triggered_at": {"$gte": since}}, {"received_at": {"$gte": since}}],
        "direction": {"$ne": None}, "pair": {"$ne": None},
    }):
        pair = _norm_pair(s.get("pair"))
        d = s.get("direction")
        if pair and d in ("LONG", "SHORT"):
            pairs_dirs.add((pair, d))

    for a in _anomalies().find({"detected_at": {"$gte": since}, "direction": {"$in": ["LONG", "SHORT"]}}):
        pair = _norm_pair(a.get("pair") or a.get("symbol", "").replace("USDT", "/USDT"))
        d = a.get("direction")
        if pair:
            pairs_dirs.add((pair, d))

    for c in _confluence().find({"detected_at": {"$gte": since}, "direction": {"$in": ["LONG", "SHORT"]}}):
        pair = _norm_pair(c.get("pair") or c.get("symbol", "").replace("USDT", "/USDT"))
        d = c.get("direction")
        if pair:
            pairs_dirs.add((pair, d))

    # Для каждой считаем состояние
    out = []
    for pair, direction in pairs_dirs:
        state = compute_pending_state(pair, direction)
        if state["state"] == "pending":
            out.append({
                "pair": pair, "direction": direction,
                "count": state["count"], "min": state["min"],
                "time_left_h": state["time_left_h"],
            })
    out.sort(key=lambda x: -x["count"])  # по убыванию готовности
    return out[:limit]


# ── Проверка: создать кластер? ─────────────────────────────────
def should_trigger_cluster(pair: str, direction: str, at: datetime) -> tuple[bool, list, int]:
    """Проверяет создавать ли кластер на момент `at`.
    Returns: (trigger, signals_in_cluster, count)

    Блокировки (в порядке проверки):
      1. count < min_count         → не хватает голосов
      2. дедуп (тот же кластер был недавно)
      3. Anti-cluster divergence   → источники противоречат друг другу (strong/nuclear)
    """
    cfg = get_config()
    sigs = collect_signals_for(pair, direction, at, cfg["window_h"])
    count = len(sigs)
    if count < cfg["min_count"]:
        return False, sigs, count

    # Дедуп: не создаём если уже был кластер в последние dedup_h часов
    dedup_start = at - timedelta(hours=cfg["dedup_h"])
    existing = _clusters().find_one({
        "pair": _norm_pair(pair), "direction": direction,
        "trigger_at": {"$gte": dedup_start, "$lte": at},
    })
    if existing:
        return False, sigs, count

    # Anti-cluster: блок если источники противоречат
    try:
        from anti_cluster_detector import detect_conflict, log_conflict_block
        conflict = detect_conflict(pair, at, window_h=cfg["window_h"])
        if conflict["has_conflict"] and conflict["severity"] in ("strong", "nuclear"):
            log_conflict_block(pair, direction, conflict, at)
            logger.info(f"[cluster] BLOCKED by anti-cluster: {pair} {direction} "
                        f"severity={conflict['severity']} L={conflict['long_weight']} S={conflict['short_weight']}")
            return False, sigs, count
    except Exception as e:
        logger.warning(f"[cluster] anti-cluster check failed: {e}")

    return True, sigs, count


# ── Cluster strength (комбо с Reversal Meter) ──────────────────
def cluster_strength(direction: str, reversal_score: int, reversal_dir: str) -> str:
    """Комбо: STRONG если |reversal| >= 50 и совпадает направление."""
    sig_bull = direction in ("LONG", "BUY")
    rev_bull = reversal_dir == "BULLISH"
    rev_bear = reversal_dir == "BEARISH"

    aligned = (sig_bull and rev_bull) or (not sig_bull and rev_bear)
    against = (sig_bull and rev_bear) or (not sig_bull and rev_bull)

    if aligned and abs(reversal_score) >= 50:
        return "MEGA"        # 🔥 cluster + strong reversal confirmation
    if aligned:
        return "STRONG"      # ⚡ с reversal
    if against and abs(reversal_score) >= 30:
        return "RISKY"       # ⚠️ против reversal — опасно
    return "NORMAL"          # ⚡ обычный кластер


# ── Создание кластера в БД ─────────────────────────────────────
def create_cluster(pair: str, direction: str, signals_in_cluster: list, at: datetime) -> Optional[dict]:
    """Создаёт запись в БД clusters + возвращает."""
    if not signals_in_cluster:
        return None
    cfg = get_config()
    trigger_price = signals_in_cluster[-1].get("price")
    if not trigger_price:
        # fallback на самый свежий с ценой
        for s in reversed(signals_in_cluster):
            if s.get("price"):
                trigger_price = s["price"]; break
    if not trigger_price:
        return None
    trigger_price = float(trigger_price)
    sources_count = len({s["source"] for s in signals_in_cluster})

    # TP/SL
    is_long = direction in ("LONG", "BUY")
    tp_price = trigger_price * (1 + cfg["tp_pct"]/100) if is_long else trigger_price * (1 - cfg["tp_pct"]/100)
    sl_price = trigger_price * (1 - cfg["sl_pct"]/100) if is_long else trigger_price * (1 + cfg["sl_pct"]/100)

    # Reversal Meter combo
    try:
        from reversal_meter import compute_score as _rs
        meter = _rs(at)
        rev_score = meter["score"]
        rev_dir = meter["direction"]
    except Exception:
        rev_score = 0
        rev_dir = "NEUTRAL"

    strength = cluster_strength(direction, rev_score, rev_dir)

    # ID
    last = _clusters().find_one(sort=[("id", -1)])
    next_id = (last.get("id", 0) + 1) if last else 1

    doc = {
        "id": next_id,
        "symbol": _norm_pair(pair).replace("/", ""),
        "pair": _norm_pair(pair),
        "direction": direction,
        "trigger_at": at,
        "trigger_price": trigger_price,
        "tp_price": tp_price,
        "sl_price": sl_price,
        "signals_in_cluster": [
            {"source": s["source"], "at": s["at"], "price": s.get("price"), "meta": s.get("meta", {})}
            for s in signals_in_cluster
        ],
        "sources_count": sources_count,
        "signals_count": len(signals_in_cluster),
        "strength": strength,
        "reversal_score": rev_score,
        "reversal_direction": rev_dir,
        "status": "OPEN",
        "exit_price": None,
        "closed_at": None,
        "pnl_percent": None,
        "created_at": utcnow(),
    }
    _clusters().insert_one(doc)
    return doc


# ── TP/SL monitoring для открытых кластеров ────────────────────
def check_cluster_outcomes(prices: dict) -> list[dict]:
    """Проверяет открытые кластеры на TP/SL. prices = {pair (with or without /): price}.
    Возвращает список закрытых кластеров для алерта."""
    closed = []
    for cl in _clusters().find({"status": "OPEN"}):
        pair = cl.get("pair", "")
        sym = cl.get("symbol", "")
        # Пробуем найти цену в любом формате
        cur = prices.get(pair) or prices.get(sym) or prices.get(pair.replace("/", ""))
        if cur is None:
            continue
        direction = cl.get("direction")
        entry = cl.get("trigger_price")
        tp = cl.get("tp_price")
        sl = cl.get("sl_price")
        if not entry or not tp or not sl:
            continue
        is_long = direction in ("LONG", "BUY")
        result = None
        exit_price = None
        if is_long:
            if cur >= tp:
                result = "TP"; exit_price = tp
            elif cur <= sl:
                result = "SL"; exit_price = sl
        else:
            if cur <= tp:
                result = "TP"; exit_price = tp
            elif cur >= sl:
                result = "SL"; exit_price = sl
        if result:
            pnl = (exit_price - entry) / entry * 100
            if not is_long:
                pnl = -pnl
            _clusters().update_one({"_id": cl["_id"]}, {"$set": {
                "status": result, "exit_price": exit_price,
                "pnl_percent": pnl, "closed_at": utcnow(),
            }})
            cl["status"] = result; cl["exit_price"] = exit_price; cl["pnl_percent"] = pnl
            closed.append(cl)
    return closed
