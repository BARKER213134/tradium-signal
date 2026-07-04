"""Cluster utils — сбор сигналов по паре (collect_signals_for).

Создание кластеров УДАЛЕНО (2026-07-02): 3 сигнала за 14д, источник мёртв.
Модуль оставлен ради collect_signals_for (pair-signals, anti_cluster).

Кластер = N+ сигналов одного направления на одной паре за окно T часов.
Параметры настраиваемы через UI (хранятся в MongoDB system._id='cluster_config').

Бэктест на 96ч показал: окно 8ч, min 2 → WR 78.6%, Avg PnL +0.51%.
"""
from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Optional
import logging

from database import _signals, _confluence, _clusters, _cluster_config, utcnow
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

    # CV + Tradium удалены вместе с ingestion (2026-07-01)

    # Anomaly удалён (2026-07-02)

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
                     "pattern": c.get("pattern"),
                     "is_top_pick": bool(c.get("is_top_pick"))},
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
                         "pnl_pct": cl.get("pnl_percent"),
                         "is_top_pick": bool(cl.get("is_top_pick"))},
            })

        # New Strategy Signals — whale/shark/combo/volume_surge/triple_confluence/
        # vol_accum/volcano/second_flip. Для chart markers в журнале (без этого
        # на графике не было 🐋/🦈/🧠/🌊 etc эмодзи на pair-signals fallback path).
        # Projection + limit для ускорения (бы стало медленно на больших окнах).
        try:
            from database import _get_db
            nss = _get_db().new_strategy_signals
            for s in nss.find({
                "created_at": {"$gte": start, "$lte": end_at},
                "direction": direction,
                "$or": [{"symbol": norm.replace("/", "")}, {"pair": norm}],
            }, {
                "strategy": 1, "created_at": 1, "entry": 1,
                "whale_tier": 1, "whale_score": 1, "whale_indicators": 1,
                "shark_tier": 1, "shark_score": 1, "shark_indicators": 1,
                "combo_score": 1, "trigger_source": 1, "preceding_sources": 1,
                "vol_ratio": 1, "source_count": 1,
            }).sort("created_at", -1).limit(100):
                strat = s.get("strategy", "?")
                # Meta — стратегия-специфичная info для tooltip
                meta = {}
                if strat == "whale":
                    meta = {"tier": s.get("whale_tier"),
                            "score": s.get("whale_score"),
                            "indicators": s.get("whale_indicators")}
                elif strat == "shark":
                    meta = {"tier": s.get("shark_tier"),
                            "score": s.get("shark_score"),
                            "indicators": s.get("shark_indicators")}
                elif strat == "combo":
                    meta = {"score": s.get("combo_score"),
                            "trigger": s.get("trigger_source"),
                            "preceding": s.get("preceding_sources")}
                else:
                    meta = {"vol_ratio": s.get("vol_ratio"),
                            "source_count": s.get("source_count")}
                out.append({
                    "source": strat,
                    "at": s["created_at"],
                    "price": s.get("entry"),
                    "meta": meta,
                })
        except Exception:
            pass  # mongo unavailable / коллекция отсутствует — не блокируем основной flow

        # BIG BUY блок удалён вместе с Cryptovizor ingestion (2026-07-01)

    out.sort(key=lambda x: x["at"])
    return out


# ── Pending state (для UI badge) ───────────────────────────────




# ── Проверка: создать кластер? ─────────────────────────────────


# ── Cluster strength (комбо с Reversal Meter) ──────────────────
def cluster_strength(direction: str, reversal_score: int, reversal_dir: str) -> str:
    """Комбо: STRONG если |reversal| >= 50 и cluster ПРОТИВ meter (contrarian).

    Бэктест на 202 закрытых кластерах (29.04.2026) показал:
      meter=BULLISH + cluster_LONG  (раньше STRONG/MEGA): WR 40.0% (n=20)
      meter=BULLISH + cluster_SHORT (раньше RISKY):       WR 70.3% (n=37)

    Причина: компоненты reversal_meter (pattern_imbalance, velocity_cluster,
    contrarian_bounce, btc_eth_lead) считают continuation/momentum, не
    reversal. Когда CV выдаёт 80%+ LONG → meter говорит «BULLISH», но это
    пик накачки → цена откатывает. Cluster_LONG в этот момент — разворот
    вниз, поэтому проигрывает.

    Поэтому STRONG/MEGA = cluster ПРОТИВ meter (contrarian),
    RISKY = cluster в направлении meter (momentum-trap).
    """
    sig_bull = direction in ("LONG", "BUY")
    rev_bull = reversal_dir == "BULLISH"
    rev_bear = reversal_dir == "BEARISH"

    # Inverted vs meter direction — потому что meter сейчас считает momentum
    aligned = (sig_bull and rev_bear) or (not sig_bull and rev_bull)  # cluster ПРОТИВ meter
    against = (sig_bull and rev_bull) or (not sig_bull and rev_bear)  # cluster ПО meter (momentum)

    if aligned and abs(reversal_score) >= 50:
        return "MEGA"        # 🔥 contrarian к сильному momentum — лучший сетап (n=29 → 72.4% WR)
    if aligned:
        return "STRONG"      # ⚡ contrarian к умеренному momentum (62.5% WR)
    if against and abs(reversal_score) >= 30:
        return "RISKY"       # ⚠️ momentum-trap — кластер на пике накачки (40% WR)
    return "NORMAL"          # ⚡ meter NEUTRAL — обычный кластер (54.5% WR)


# ── Создание кластера в БД ─────────────────────────────────────


# ── TP/SL monitoring для открытых кластеров ────────────────────
