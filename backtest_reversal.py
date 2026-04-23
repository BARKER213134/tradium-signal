"""Бэктест 5 индикаторов раннего разворота на истории БД.

Для каждой точки времени (с шагом 30 мин) считаем 5 индикаторов:
  1. Pattern Imbalance      — перевес bullish/bearish CV патернов
  2. Velocity + Cluster     — ускорение + кластер разных монет
  3. Contrarian Bounce      — CV bullish vs Confluence bearish
  4. Funding Squeeze        — экстремальные funding из anomalies
  5. BTC/ETH Lead           — паттерн на BTC/ETH как lead

+ Composite Score (weighted sum of all)

Оценка:
  Предсказание BULLISH (signal = +1) → считаем успехом если в следующие 2ч
  CV patterns LONG ratio >= 65% (N >= 3)
  Аналогично для BEARISH.
  NEUTRAL → успех если в окне LONG ratio между 35% и 65%.
"""
import sys
sys.stdout.reconfigure(encoding='utf-8') if hasattr(sys.stdout, 'reconfigure') else None

from database import _signals, _anomalies, _confluence
from datetime import datetime, timezone, timedelta
from collections import Counter


# ── Конфиг ──────────────────────────────────────────────────────
LOOKBACK_H = 2           # окно для расчёта индикаторов
FORWARD_H = 2            # окно для проверки исхода
STEP_MIN = 30            # шаг сетки
BULL_THRESHOLD = 0.65    # доля LONG для классификации исхода как bullish
BEAR_THRESHOLD = 0.35
MIN_N = 3                # минимум патернов для надёжного исхода


# BULLISH pattern names (из patterns.py)
BULL_PATTERNS = {
    "Молот", "Перевёрнутый молот", "Бычье поглощение",
    "Пинцет на дне", "Утренняя звезда", "Три белых солдата",
    "Бычий флаг", "Бычий вымпел", "Бычий треугольник",
    "Восходящий треугольник", "Двойное дно", "Тройное дно",
}
BEAR_PATTERNS = {
    "Падающая звезда", "Повешенный", "Медвежье поглощение",
    "Пинцет на вершине", "Вечерняя звезда", "Три чёрных ворона",
    "Медвежий флаг", "Медвежий вымпел", "Медвежий треугольник",
    "Нисходящий треугольник", "Двойная вершина", "Тройная вершина",
}


# ── Загрузка данных (один раз) ─────────────────────────────────
def load_all():
    print("Загрузка данных...")
    cv = list(_signals().find(
        {"source": "cryptovizor", "pattern_triggered": True,
         "pattern_triggered_at": {"$ne": None},
         "direction": {"$ne": None}},
        {"pair": 1, "direction": 1, "pattern_name": 1, "pattern_triggered_at": 1, "_id": 0}
    ))
    cv = [c for c in cv if c.get("pattern_triggered_at")]
    cv.sort(key=lambda x: x["pattern_triggered_at"])

    cf = list(_confluence().find(
        {"detected_at": {"$ne": None}, "direction": {"$ne": None}},
        {"symbol": 1, "direction": 1, "detected_at": 1, "score": 1, "_id": 0}
    ))
    cf.sort(key=lambda x: x["detected_at"])

    an = list(_anomalies().find(
        {"detected_at": {"$ne": None}},
        {"symbol": 1, "direction": 1, "detected_at": 1, "anomalies": 1, "_id": 0}
    ))
    an.sort(key=lambda x: x["detected_at"])

    print(f"CV patterns: {len(cv)}")
    print(f"Confluence: {len(cf)}")
    print(f"Anomalies: {len(an)}")
    return cv, cf, an


# ── Helpers ─────────────────────────────────────────────────────
def filter_range(items, ts_field, start, end):
    """Элементы с start <= ts < end."""
    return [x for x in items if x.get(ts_field) and start <= x[ts_field] < end]


def cv_in_window(cv, start, end):
    return [c for c in cv if start <= c["pattern_triggered_at"] < end]


# ── Индикаторы ─────────────────────────────────────────────────

def ind_pattern_imbalance(cv_window):
    L = sum(1 for c in cv_window if c["direction"] == "LONG")
    S = sum(1 for c in cv_window if c["direction"] == "SHORT")
    N = L + S
    if N < 5:
        return 0, {"L": L, "S": S, "N": N, "reason": "N<5"}
    balance = (L - S) / N
    if balance >= 0.7:
        return +1, {"balance": balance, "L": L, "S": S}
    if balance <= -0.7:
        return -1, {"balance": balance, "L": L, "S": S}
    return 0, {"balance": balance, "L": L, "S": S}


def ind_velocity_cluster(cv, t):
    """Ускорение (15 мин vs 45-60 мин назад) + кластер разных монет в 10 мин."""
    # last 15 min
    last15 = [c for c in cv if t - timedelta(minutes=15) <= c["pattern_triggered_at"] < t]
    # 45-60 min ago
    prev15 = [c for c in cv if t - timedelta(minutes=60) <= c["pattern_triggered_at"] < t - timedelta(minutes=45)]
    v_now = len(last15)
    v_prev = max(len(prev15), 1)
    acc = v_now / v_prev
    # cluster: different pairs in last 10 min
    last10 = [c for c in cv if t - timedelta(minutes=10) <= c["pattern_triggered_at"] < t]
    L10 = {c["pair"] for c in last10 if c["direction"] == "LONG"}
    S10 = {c["pair"] for c in last10 if c["direction"] == "SHORT"}
    cluster_L = len(L10)
    cluster_S = len(S10)

    if acc >= 2.0 and cluster_L >= 3 and cluster_L > cluster_S:
        return +1, {"acc": acc, "cluster_L": cluster_L, "cluster_S": cluster_S}
    if acc >= 2.0 and cluster_S >= 3 and cluster_S > cluster_L:
        return -1, {"acc": acc, "cluster_L": cluster_L, "cluster_S": cluster_S}
    return 0, {"acc": acc, "cluster_L": cluster_L, "cluster_S": cluster_S}


def ind_contrarian(cv_window, cf_window):
    cv_L = sum(1 for c in cv_window if c["direction"] == "LONG")
    cv_S = sum(1 for c in cv_window if c["direction"] == "SHORT")
    cv_N = cv_L + cv_S
    cf_L = sum(1 for c in cf_window if c["direction"] == "LONG")
    cf_S = sum(1 for c in cf_window if c["direction"] == "SHORT")
    cf_N = cf_L + cf_S
    if cv_N < 5 or cf_N < 10:
        return 0, {"reason": "not enough data"}
    cv_L_pct = cv_L / cv_N
    cv_S_pct = cv_S / cv_N
    cf_S_pct = cf_S / cf_N
    cf_L_pct = cf_L / cf_N
    # CV bullish + Confluence bearish → contrarian LONG
    if cv_L_pct >= 0.8 and cf_S_pct >= 0.6:
        return +1, {"cv_L%": cv_L_pct, "cf_S%": cf_S_pct}
    if cv_S_pct >= 0.8 and cf_L_pct >= 0.6:
        return -1, {"cv_S%": cv_S_pct, "cf_L%": cf_L_pct}
    return 0, {"cv_L%": cv_L_pct, "cf_L%": cf_L_pct}


def ind_funding_squeeze(an_window):
    neg = 0; pos = 0
    for a in an_window:
        for x in a.get("anomalies", []) or []:
            if x.get("type") != "funding_extreme":
                continue
            v = x.get("value")
            if v is None:
                continue
            if v <= -1.0:
                neg += 1
            elif v >= 1.0:
                pos += 1
    if neg >= 2:
        return +1, {"neg": neg, "pos": pos}
    if pos >= 2:
        return -1, {"neg": neg, "pos": pos}
    return 0, {"neg": neg, "pos": pos}


def ind_btc_eth_lead(cv, t):
    """Паттерн на BTC/ETH в последние 30 мин."""
    window = [c for c in cv if t - timedelta(minutes=30) <= c["pattern_triggered_at"] < t]
    for c in window:
        pair = (c.get("pair") or "").upper()
        if pair not in ("BTC/USDT", "ETH/USDT"):
            continue
        pn = c.get("pattern_name", "")
        if pn in BULL_PATTERNS or c.get("direction") == "LONG":
            return +1, {"pair": pair, "pattern": pn}
        if pn in BEAR_PATTERNS or c.get("direction") == "SHORT":
            return -1, {"pair": pair, "pattern": pn}
    return 0, {}


def ind_cv_conf_divergence(cv_window, cf_window):
    cv_L = sum(1 for c in cv_window if c["direction"] == "LONG")
    cv_S = sum(1 for c in cv_window if c["direction"] == "SHORT")
    cv_N = cv_L + cv_S
    cf_L = sum(1 for c in cf_window if c["direction"] == "LONG")
    cf_S = sum(1 for c in cf_window if c["direction"] == "SHORT")
    cf_N = cf_L + cf_S
    if cv_N < 3 or cf_N < 5:
        return 0.0
    cv_bal = (cv_L - cv_S) / cv_N
    cf_bal = (cf_L - cf_S) / cf_N
    return cv_bal - cf_bal  # диапазон: [-2, +2]


def ind_composite(pi_sig, vc_sig, co_sig, fs_sig, be_sig, cv_window, cf_window):
    """Взвешенный score от -1 до +1."""
    div = ind_cv_conf_divergence(cv_window, cf_window)
    div_norm = max(-1, min(1, div / 1.5))  # нормализуем
    score = (
        0.25 * vc_sig +
        0.20 * div_norm +
        0.20 * co_sig +
        0.15 * fs_sig +
        0.10 * be_sig +
        0.10 * pi_sig
    )
    if score >= 0.30:
        return +1, score
    if score <= -0.30:
        return -1, score
    return 0, score


# ── Исход: что случилось в следующие N часов ───────────────────
def actual_outcome(cv, t, hours):
    window = [c for c in cv if t <= c["pattern_triggered_at"] < t + timedelta(hours=hours)]
    L = sum(1 for c in window if c["direction"] == "LONG")
    S = sum(1 for c in window if c["direction"] == "SHORT")
    N = L + S
    if N < MIN_N:
        return "INSUFFICIENT", {"L": L, "S": S, "N": N}
    ratio = L / N
    if ratio >= BULL_THRESHOLD:
        return "BULL", {"L": L, "S": S, "N": N, "ratio": ratio}
    if ratio <= BEAR_THRESHOLD:
        return "BEAR", {"L": L, "S": S, "N": N, "ratio": ratio}
    return "MIXED", {"L": L, "S": S, "N": N, "ratio": ratio}


# ── Main ────────────────────────────────────────────────────────
def classify(signal, actual):
    """Оценка: был ли сигнал правильным."""
    if actual == "INSUFFICIENT":
        return "skip"
    if signal == +1 and actual == "BULL": return "TP"
    if signal == -1 and actual == "BEAR": return "TP"
    if signal == +1 and actual == "BEAR": return "FP"
    if signal == -1 and actual == "BULL": return "FP"
    if signal == 0 and actual == "MIXED": return "TN"
    if signal == 0 and actual in ("BULL", "BEAR"): return "FN"
    # signal != 0 and actual == MIXED — weak FP
    if signal != 0 and actual == "MIXED": return "WFP"
    return "?"


def main():
    cv, cf, an = load_all()
    if not cv or not cf:
        print("Мало данных")
        return

    # Временной диапазон
    all_ts = [c["pattern_triggered_at"] for c in cv] + [c["detected_at"] for c in cf]
    t_start = min(all_ts) + timedelta(hours=LOOKBACK_H)
    t_end = max(all_ts) - timedelta(hours=FORWARD_H)
    print(f"Период: {t_start} → {t_end}")
    total_hours = (t_end - t_start).total_seconds() / 3600
    print(f"Всего: {total_hours:.1f} часов")

    # Пробегаем
    indicators = {
        "1. Imbalance":    {"fn": lambda cv_w, cf_w, an_w, t: ind_pattern_imbalance(cv_w)},
        "2. Velocity+Cluster": {"fn": lambda cv_w, cf_w, an_w, t: ind_velocity_cluster(cv, t)},
        "3. Contrarian":   {"fn": lambda cv_w, cf_w, an_w, t: ind_contrarian(cv_w, cf_w)},
        "4. Funding Squeeze": {"fn": lambda cv_w, cf_w, an_w, t: ind_funding_squeeze(an_w)},
        "5. BTC/ETH Lead": {"fn": lambda cv_w, cf_w, an_w, t: ind_btc_eth_lead(cv, t)},
        "6. COMPOSITE":    {"fn": None},  # считается отдельно
    }
    stats = {name: {"TP": 0, "FP": 0, "TN": 0, "FN": 0, "WFP": 0, "skip": 0, "fire": 0} for name in indicators}

    t = t_start
    loops = 0
    while t <= t_end:
        cv_w = cv_in_window(cv, t - timedelta(hours=LOOKBACK_H), t)
        cf_w = filter_range(cf, "detected_at", t - timedelta(hours=LOOKBACK_H), t)
        an_w = filter_range(an, "detected_at", t - timedelta(hours=4), t)

        actual, _ = actual_outcome(cv, t, FORWARD_H)

        # Calc all 5 indicators
        pi_sig, _ = ind_pattern_imbalance(cv_w)
        vc_sig, _ = ind_velocity_cluster(cv, t)
        co_sig, _ = ind_contrarian(cv_w, cf_w)
        fs_sig, _ = ind_funding_squeeze(an_w)
        be_sig, _ = ind_btc_eth_lead(cv, t)
        comp_sig, _ = ind_composite(pi_sig, vc_sig, co_sig, fs_sig, be_sig, cv_w, cf_w)

        sigs = {
            "1. Imbalance": pi_sig,
            "2. Velocity+Cluster": vc_sig,
            "3. Contrarian": co_sig,
            "4. Funding Squeeze": fs_sig,
            "5. BTC/ETH Lead": be_sig,
            "6. COMPOSITE": comp_sig,
        }
        for name, sig in sigs.items():
            if sig != 0:
                stats[name]["fire"] += 1
            cls = classify(sig, actual)
            stats[name][cls] += 1

        t += timedelta(minutes=STEP_MIN)
        loops += 1

    print(f"Проверено точек: {loops}")
    print()

    # Итоги
    print("=" * 96)
    print(f"{'Индикатор':<25} {'Срабат.':>8} {'TP':>5} {'FP':>5} {'WFP':>5} {'FN':>5} {'TN':>5} {'Precision':>10} {'Accuracy':>10}")
    print("-" * 96)
    for name, st in stats.items():
        fire = st["fire"]
        tp = st["TP"]; fp = st["FP"]; wfp = st["WFP"]
        fn = st["FN"]; tn = st["TN"]; skip = st["skip"]
        total_classified = tp + fp + wfp + fn + tn
        precision = tp / max(tp + fp + wfp, 1)
        accuracy = (tp + tn) / max(total_classified, 1)
        print(f"{name:<25} {fire:>8} {tp:>5} {fp:>5} {wfp:>5} {fn:>5} {tn:>5} {precision*100:>9.1f}% {accuracy*100:>9.1f}%")
    print()
    print("TP  = истинное предсказание разворота  (сработал и угадал направление)")
    print("FP  = ложное предсказание              (сработал и промахнулся в направлении)")
    print("WFP = слабое промах                    (сработал, но реально было MIXED)")
    print("FN  = пропущен разворот                (молчал, но был сильный разворот)")
    print("TN  = правильно промолчал              (молчал и был MIXED)")
    print("Precision = TP / (TP + FP + WFP)       — доля правильных сработок")
    print("Accuracy  = (TP + TN) / all            — общая точность")


if __name__ == "__main__":
    main()
