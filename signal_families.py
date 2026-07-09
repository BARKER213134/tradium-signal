"""🧩 Signal Families — семейная группировка сигналов.

Проблема (анализ 56 дней, 2026-07-02): один флип SuperTrend порождает эхо
в 4-6 детекторах (supertrend, second_flip, vol_accum, volume_surge, combo,
triple_confluence, verified) — на графике колонна маркеров, в журнале пачка
строк, а информации как от 1-2 независимых сигналов.

Решение:
  1. FAMILY_OF — каждый источник принадлежит семейству. ST-эхо = 'trend'.
  2. collapse_stacks() — цепочки 2+ сигналов (пара+направление, разрыв
     ≤30 мин) схлопываются в ОДНУ строку source='stack' (🧩) со списком
     участников и числом НЕЗАВИСИМЫХ семейств.
  3. stack_distinct в журнале = distinct (family × direction) — честное
     число независимых подтверждений (было source × direction).

Топ-комбо до дедупа: second_flip+supertrend+vol_accum ×89 — всё одно
семейство 'trend'.
"""
from __future__ import annotations

# Источник → семейство. Всё ST-производное = 'trend' (один голос).
FAMILY_OF = {
    "supertrend": "trend",
    "second_flip": "trend",
    "vol_accum": "trend",
    "volume_surge": "trend",
    "triple_confluence": "trend",
    "volcano": "trend",
    "rsi_cross_12h": "trend",
    "verified": "trend",     # verified проверяет те же ST-сигналы
    "confluence": "levels",  # уровни + свечные паттерны — независимый взгляд
    "impulse": "momentum",   # rocket: higher-TF RSI momentum (research 07-02)
    "ignition": "momentum",  # ранний вход: RSI4h кросс в 58-68 (research 07-06)
    "ten": "momentum",       # 💰 LONG до +10%: IMPULSE-состояние, TP10/SL5/96ч (07-10)
    "rider_short": "trend",  # 🏄 тренд-райдер: слом 55-лоу 4h, трейл-выход (07-09)
    "fade": "momentum",      # short rally in downtrend
    "whale": "whale",        # структура базы + vol spike
    "shark": "shark",        # distribution top
    "paper": "paper",
}

FAMILY_EMOJI = {
    "momentum": "\U0001F680",
    "trend": "🌀", "levels": "🎯", "whale": "🐋", "shark": "🦈",
    "cluster": "💠", "paper": "🤖",
}


def family_of(source: str) -> str:
    """Семейство источника. Неизвестные источники — сами себе семейство."""
    return FAMILY_OF.get(source or "", source or "?")


def collapse_stacks(items: list[dict], gap_s: int = 1800,
                    min_size: int = 2) -> list[dict]:
    """Схлопывает цепочки сигналов (та же пара + направление, разрыв между
    соседними ≤ gap_s) в одну синтетическую строку source='stack'.

    items: журнальные dict'ы с полями pair/direction/source/at_ts/entry.
    Одиночки и источники вне группировки проходят без изменений.
    Paper-сделки не группируются (это записи трейдера, не сигналы).

    Возвращает НОВЫЙ список (сортировка by at_ts desc сохраняется снаружи).
    """
    NO_GROUP = {"paper", "stack", "accum"}  # accum — инфо-событие, не голос
    from collections import defaultdict

    groupable = [it for it in items
                 if it.get("source") not in NO_GROUP
                 and it.get("pair") and it.get("at_ts")
                 and it.get("direction") in ("LONG", "SHORT")]
    g_ids = {id(it) for it in groupable}
    passthrough = [it for it in items if id(it) not in g_ids]

    by_key = defaultdict(list)
    for it in groupable:
        by_key[(it["pair"], it["direction"])].append(it)

    out = list(passthrough)
    for (pair, direction), sigs in by_key.items():
        sigs.sort(key=lambda x: x["at_ts"])
        chain: list[dict] = []
        def flush():
            if not chain:
                return
            if len(chain) < min_size:
                out.extend(chain)
                return
            members = [{
                "source": m.get("source"),
                "at_ts": m.get("at_ts"),
                "at": m.get("at"),
                "entry": m.get("entry"),
                "score": m.get("score"),
                "pattern": (m.get("pattern") or "")[:60],
            } for m in chain]
            fams = sorted({family_of(m.get("source")) for m in chain})
            first = chain[0]
            src_counts: dict = {}
            for m in chain:
                src_counts[m["source"]] = src_counts.get(m["source"], 0) + 1
            comp = " + ".join(
                (f"{s}×{n}" if n > 1 else s)
                for s, n in sorted(src_counts.items(), key=lambda kv: -kv[1]))
            fam_emojis = "".join(FAMILY_EMOJI.get(f, "•") for f in fams)
            stack_row = {
                "source": "stack",
                "symbol": first.get("symbol") or pair.replace("/", "").upper(),
                "pair": pair,
                "direction": direction,
                "entry": first.get("entry"),
                "tp1": None, "sl": None,
                "pattern": f"🧩 STACK ×{len(chain)} · {fam_emojis} {len(fams)} fam · {comp}"[:160],
                "score": len(fams) * 10 + len(chain),
                "st_passed": None, "pump_score": 0,
                "is_top_pick": len(fams) >= 3,
                "top_pick_confirmations_count": len(fams),
                "stack_members": members[:12],
                "stack_size": len(chain),
                "stack_families": len(fams),
                "families": fams,
                # Наследуем максимум полезных полей от участников
                "whale_tier": next((m.get("whale_tier") for m in chain if m.get("whale_tier")), None),
                "shark_tier": next((m.get("shark_tier") for m in chain if m.get("shark_tier")), None),
                "setup_verdict": next((m.get("setup_verdict") for m in chain if m.get("setup_verdict")), None),
                # Наследуем окно-метрики (±3h) от участников — максимум
                "stack_distinct": max((m.get("stack_distinct") or 0) for m in chain) or len(fams),
                "stack_count": max((m.get("stack_count") or 0) for m in chain) or len(chain),
                "stack_long": max((m.get("stack_long") or 0) for m in chain),
                "stack_short": max((m.get("stack_short") or 0) for m in chain),
                "q_score": max((m.get("q_score") or 0) for m in chain),
                "at": first.get("at"),
                "at_ts": first.get("at_ts"),
            }
            out.append(stack_row)

        for s in sigs:
            if not chain or s["at_ts"] - chain[-1]["at_ts"] <= gap_s:
                chain.append(s)
            else:
                flush()
                chain = [s]
        flush()

    out.sort(key=lambda x: x.get("at_ts", 0), reverse=True)
    return out
