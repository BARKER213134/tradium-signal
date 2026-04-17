"""Composite Reversal Meter — индикатор раннего разворота рынка.

Объединяет 5 сигналов в один score от -100 до +100:
  1. Pattern Imbalance     (0.10) — перевес CV bullish/bearish за 2ч
  2. Velocity + Cluster    (0.25) — ускорение паттернов + кластер монет
  3. CV/Conf Divergence    (0.20) — CV смотрит bullish vs Conf bearish
  4. Contrarian Bounce     (0.20) — против тренда setup
  5. Funding Squeeze       (0.15) — экстремальные funding
  6. BTC/ETH Lead          (0.10) — паттерн на мажорах как лид

Backtest precision: 88.6% (из 35 срабатываний за 62ч — 31 правильный).
"""
from datetime import datetime, timezone, timedelta
from typing import Literal

from database import _signals, _confluence, _anomalies

Direction = Literal["BULLISH", "BEARISH", "NEUTRAL"]

LOOKBACK_H = 2
ANOMALY_H = 4


# ── Компоненты ──────────────────────────────────────────────────

def _ind_pattern_imbalance(cv_window: list) -> tuple[float, dict]:
    L = sum(1 for c in cv_window if c["direction"] == "LONG")
    S = sum(1 for c in cv_window if c["direction"] == "SHORT")
    N = L + S
    if N < 5:
        return 0.0, {"L": L, "S": S, "N": N, "reason": "N<5"}
    balance = (L - S) / N
    if balance >= 0.7:
        return 1.0, {"balance": balance, "L": L, "S": S}
    if balance <= -0.7:
        return -1.0, {"balance": balance, "L": L, "S": S}
    return 0.0, {"balance": balance, "L": L, "S": S}


def _ind_velocity_cluster(cv_all: list, t: datetime) -> tuple[float, dict]:
    last15 = [c for c in cv_all if t - timedelta(minutes=15) <= c["pattern_triggered_at"] < t]
    prev15 = [c for c in cv_all if t - timedelta(minutes=60) <= c["pattern_triggered_at"] < t - timedelta(minutes=45)]
    v_now = len(last15)
    v_prev = max(len(prev15), 1)
    acc = v_now / v_prev
    last10 = [c for c in cv_all if t - timedelta(minutes=10) <= c["pattern_triggered_at"] < t]
    cluster_L = len({c["pair"] for c in last10 if c["direction"] == "LONG"})
    cluster_S = len({c["pair"] for c in last10 if c["direction"] == "SHORT"})

    if acc >= 2.0 and cluster_L >= 3 and cluster_L > cluster_S:
        return 1.0, {"acc": round(acc, 2), "cluster_L": cluster_L, "cluster_S": cluster_S}
    if acc >= 2.0 and cluster_S >= 3 and cluster_S > cluster_L:
        return -1.0, {"acc": round(acc, 2), "cluster_L": cluster_L, "cluster_S": cluster_S}
    return 0.0, {"acc": round(acc, 2), "cluster_L": cluster_L, "cluster_S": cluster_S}


def _ind_cv_conf_divergence(cv_window: list, cf_window: list) -> tuple[float, dict]:
    cv_L = sum(1 for c in cv_window if c["direction"] == "LONG")
    cv_S = sum(1 for c in cv_window if c["direction"] == "SHORT")
    cv_N = cv_L + cv_S
    cf_L = sum(1 for c in cf_window if c["direction"] == "LONG")
    cf_S = sum(1 for c in cf_window if c["direction"] == "SHORT")
    cf_N = cf_L + cf_S
    if cv_N < 3 or cf_N < 5:
        return 0.0, {"reason": "not enough data"}
    cv_bal = (cv_L - cv_S) / cv_N
    cf_bal = (cf_L - cf_S) / cf_N
    div = cv_bal - cf_bal  # [-2, +2]
    # Нормализуем в [-1, +1]
    norm = max(-1.0, min(1.0, div / 1.5))
    return norm, {"cv_bal": round(cv_bal, 2), "cf_bal": round(cf_bal, 2), "div": round(div, 2)}


def _ind_contrarian_bounce(cv_window: list, cf_window: list) -> tuple[float, dict]:
    cv_L = sum(1 for c in cv_window if c["direction"] == "LONG")
    cv_S = sum(1 for c in cv_window if c["direction"] == "SHORT")
    cv_N = cv_L + cv_S
    cf_L = sum(1 for c in cf_window if c["direction"] == "LONG")
    cf_S = sum(1 for c in cf_window if c["direction"] == "SHORT")
    cf_N = cf_L + cf_S
    if cv_N < 5 or cf_N < 10:
        return 0.0, {"reason": "not enough data"}
    cv_L_pct = cv_L / cv_N
    cv_S_pct = cv_S / cv_N
    cf_S_pct = cf_S / cf_N
    cf_L_pct = cf_L / cf_N
    if cv_L_pct >= 0.8 and cf_S_pct >= 0.6:
        return 1.0, {"cv_L%": round(cv_L_pct, 2), "cf_S%": round(cf_S_pct, 2)}
    if cv_S_pct >= 0.8 and cf_L_pct >= 0.6:
        return -1.0, {"cv_S%": round(cv_S_pct, 2), "cf_L%": round(cf_L_pct, 2)}
    return 0.0, {"cv_L%": round(cv_L_pct, 2), "cf_L%": round(cf_L_pct, 2)}


def _ind_funding_squeeze(an_window: list) -> tuple[float, dict]:
    neg = pos = 0
    samples = []
    for a in an_window:
        for x in a.get("anomalies") or []:
            if x.get("type") != "funding_extreme":
                continue
            v = x.get("value")
            if v is None:
                continue
            samples.append((a.get("symbol"), v))
            if v <= -0.5:
                neg += 1
            elif v >= 0.5:
                pos += 1
    if neg >= 2:
        return 1.0, {"neg": neg, "pos": pos, "samples": samples[:3]}
    if pos >= 2:
        return -1.0, {"neg": neg, "pos": pos, "samples": samples[:3]}
    if neg == 1 and pos == 0:
        return 0.5, {"neg": 1, "pos": 0, "samples": samples[:3]}
    if pos == 1 and neg == 0:
        return -0.5, {"neg": 0, "pos": 1, "samples": samples[:3]}
    return 0.0, {"neg": neg, "pos": pos}


def _ind_btc_eth_lead(cv_all: list, t: datetime) -> tuple[float, dict]:
    window = [c for c in cv_all if t - timedelta(minutes=30) <= c["pattern_triggered_at"] < t]
    for c in window:
        pair = (c.get("pair") or "").upper()
        if pair not in ("BTC/USDT", "ETH/USDT"):
            continue
        if c.get("direction") == "LONG":
            return 1.0, {"pair": pair, "pattern": c.get("pattern_name", "")}
        if c.get("direction") == "SHORT":
            return -1.0, {"pair": pair, "pattern": c.get("pattern_name", "")}
    return 0.0, {}


# ── Публичное API ──────────────────────────────────────────────

def compute_score(at: datetime | None = None,
                   cv_preloaded: list | None = None,
                   cf_preloaded: list | None = None,
                   an_preloaded: list | None = None) -> dict:
    """Возвращает composite score и все компоненты на момент времени `at` (UTC naive).
    Если at не задан — текущий момент.

    Для batch бэктеста можно передать preloaded данные (список за больший период);
    функция сама отфильтрует нужные окна по `at`.

    Returns:
        {
            "score": int,                 # -100..+100
            "direction": "BULLISH"|"BEARISH"|"NEUTRAL",
            "strength": "STRONG"|"MODERATE"|"WEAK"|"NONE",
            "components": {...},
            "cv_count": {"L": ..., "S": ..., "total_2h": ...},
            "conf_count": {"L": ..., "S": ..., "total_2h": ...},
            "at": "ISO timestamp",
        }
    """
    t = at or datetime.now(timezone.utc).replace(tzinfo=None)
    if t.tzinfo is not None:
        t = t.replace(tzinfo=None)

    cv_since = t - timedelta(hours=LOOKBACK_H)
    an_since = t - timedelta(hours=ANOMALY_H)

    if cv_preloaded is not None:
        cv_all = [c for c in cv_preloaded
                  if c.get("pattern_triggered_at") and c["pattern_triggered_at"] <= t]
    else:
        cv_all = list(_signals().find(
            {"source": "cryptovizor", "pattern_triggered": True,
             "pattern_triggered_at": {"$gte": t - timedelta(hours=2), "$lte": t},
             "direction": {"$ne": None}},
            {"pair": 1, "direction": 1, "pattern_name": 1, "pattern_triggered_at": 1, "_id": 0}
        ))
    cv_window = [c for c in cv_all if cv_since <= c.get("pattern_triggered_at") <= t]

    if cf_preloaded is not None:
        cf_window = [c for c in cf_preloaded
                     if c.get("detected_at") and cv_since <= c["detected_at"] <= t]
    else:
        cf_window = list(_confluence().find(
            {"detected_at": {"$gte": cv_since, "$lte": t}, "direction": {"$ne": None}},
            {"symbol": 1, "direction": 1, "detected_at": 1, "_id": 0}
        ))

    if an_preloaded is not None:
        an_window = [a for a in an_preloaded
                     if a.get("detected_at") and an_since <= a["detected_at"] <= t]
    else:
        an_window = list(_anomalies().find(
            {"detected_at": {"$gte": t - timedelta(hours=ANOMALY_H), "$lte": t}},
            {"symbol": 1, "anomalies": 1, "detected_at": 1, "_id": 0}
        ))

    pi_sig, pi_det = _ind_pattern_imbalance(cv_window)
    vc_sig, vc_det = _ind_velocity_cluster(cv_all, t)
    div_sig, div_det = _ind_cv_conf_divergence(cv_window, cf_window)
    co_sig, co_det = _ind_contrarian_bounce(cv_window, cf_window)
    fs_sig, fs_det = _ind_funding_squeeze(an_window)
    be_sig, be_det = _ind_btc_eth_lead(cv_all, t)

    # Weighted score
    weights = {
        "imbalance": 0.10,
        "velocity_cluster": 0.25,
        "divergence": 0.20,
        "contrarian": 0.20,
        "funding": 0.15,
        "btc_eth": 0.10,
    }
    raw = (
        pi_sig * weights["imbalance"] +
        vc_sig * weights["velocity_cluster"] +
        div_sig * weights["divergence"] +
        co_sig * weights["contrarian"] +
        fs_sig * weights["funding"] +
        be_sig * weights["btc_eth"]
    )
    score = int(round(raw * 100))

    direction: Direction
    if score >= 30:
        direction = "BULLISH"
    elif score <= -30:
        direction = "BEARISH"
    else:
        direction = "NEUTRAL"

    abs_s = abs(score)
    if abs_s >= 70:
        strength = "STRONG"
    elif abs_s >= 30:
        strength = "MODERATE"
    elif abs_s >= 10:
        strength = "WEAK"
    else:
        strength = "NONE"

    cv_L = sum(1 for c in cv_window if c["direction"] == "LONG")
    cv_S = sum(1 for c in cv_window if c["direction"] == "SHORT")
    cf_L = sum(1 for c in cf_window if c["direction"] == "LONG")
    cf_S = sum(1 for c in cf_window if c["direction"] == "SHORT")

    return {
        "score": score,
        "direction": direction,
        "strength": strength,
        "components": {
            "imbalance":        {"score": round(pi_sig, 2),  "weight": weights["imbalance"],        "details": pi_det},
            "velocity_cluster": {"score": round(vc_sig, 2),  "weight": weights["velocity_cluster"], "details": vc_det},
            "divergence":       {"score": round(div_sig, 2), "weight": weights["divergence"],       "details": div_det},
            "contrarian":       {"score": round(co_sig, 2),  "weight": weights["contrarian"],       "details": co_det},
            "funding":          {"score": round(fs_sig, 2),  "weight": weights["funding"],          "details": fs_det},
            "btc_eth":          {"score": round(be_sig, 2),  "weight": weights["btc_eth"],          "details": be_det},
        },
        "cv_count": {"L": cv_L, "S": cv_S, "total_2h": len(cv_window)},
        "conf_count": {"L": cf_L, "S": cf_S, "total_2h": len(cf_window)},
        "at": t.isoformat(),
    }


def agreement(signal_direction: str, reversal_direction: str) -> str:
    """Сравнивает направление сигнала с направлением Reversal Meter.

    Returns: "with" | "against" | "neutral"
    """
    if not signal_direction or reversal_direction == "NEUTRAL":
        return "neutral"
    sig = "BULLISH" if signal_direction in ("LONG", "BUY") else "BEARISH" if signal_direction in ("SHORT", "SELL") else ""
    if not sig:
        return "neutral"
    return "with" if sig == reversal_direction else "against"


def format_telegram_block(meter: dict, signal_direction: str | None = None) -> str:
    """Возвращает HTML блок для вставки в Telegram алерт."""
    score = meter["score"]
    direction = meter["direction"]
    strength = meter["strength"]
    cv = meter.get("cv_count", {})

    if direction == "BULLISH":
        emoji = "🟢"
        label = f"{strength} BULLISH" if strength != "NONE" else "BULLISH"
    elif direction == "BEARISH":
        emoji = "🔴"
        label = f"{strength} BEARISH" if strength != "NONE" else "BEARISH"
    else:
        emoji = "⚪"
        label = "NEUTRAL"

    # Signed score with +/-
    sign = "+" if score > 0 else ""
    bar_val = f"<b>{sign}{score}</b>"

    lines = [
        "─── 🔄 REVERSAL METER ───",
        f"{emoji} {bar_val} · {label}",
    ]

    if signal_direction:
        agr = agreement(signal_direction, direction)
        if agr == "with":
            lines.append("✅ С сигналом — подтверждает")
        elif agr == "against":
            lines.append("⚠️ Против сигнала — снизить риск")
        else:
            lines.append("⚪ Нейтрально")

    if cv.get("total_2h", 0) > 0:
        lines.append(f"CV за 2ч: 🟢{cv.get('L',0)} 🔴{cv.get('S',0)}")

    return "\n".join(lines)
