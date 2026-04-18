"""Key Levels — парсер + обогащение сигналов emoji-маркерами.

Источник: 3 топика в Tradium группе
  3086 = SUPPORT    ("Price Entered SUPPORT Zone!" / "New SUPPORT Level Detected!")
  3088 = RANGES     ("ASCENDING/DESCENDING/SIDEWAYS RANGE Detected!")
  3091 = RESISTANCE ("Price Entered RESISTANCE Zone!" / "New RESISTANCE Level Detected!")

По бэктесту 14 дней (2399 сигналов) обнаружена инвертированная логика:
- LONG + Price Entered RESISTANCE → Breakout UP (WR 57.6%, лучше baseline)
- SHORT + Price Entered SUPPORT → Breakout DOWN (WR 57.6%)
- LONG + Price Entered SUPPORT → Falling Knife (WR 12.5%, плохо)
- SHORT + Price Entered RESISTANCE → Against Trend (WR 12.5%)
- New Level in same direction → Confirming (WR ~46%, нейтрально+)

Эмоджи (вариант A):
  🎢 = Breakout UP (LONG + entered resistance)
  🧨 = Breakout DOWN (SHORT + entered support)
  🔪 = Falling Knife / Against Trend (опасная зона)
  ⚓ = Confirming (New Level в сторону тренда)
  〰️ = In Range (боковик)

НЕ создаёт новых сигналов — только обогащает существующие.
"""
from __future__ import annotations
import re
import logging
from datetime import datetime, timedelta
from typing import Optional

from database import _key_levels, utcnow

logger = logging.getLogger(__name__)

# Соответствие топиков → тип события
TOPIC_SUPPORT = 3086
TOPIC_RANGES = 3088
TOPIC_RESISTANCE = 3091
TOPIC_NAMES = {
    TOPIC_SUPPORT: "SUPPORT",
    TOPIC_RANGES: "RANGES",
    TOPIC_RESISTANCE: "RESISTANCE",
}

# Окно актуальности KL для обогащения сигнала (±часов от момента сигнала)
ENRICH_WINDOW_H = 2


def parse_key_level(text: str, topic_id: int = None) -> Optional[dict]:
    """Парсит сообщение от Tradium Key Levels бота.

    Args:
        text: raw text сообщения
        topic_id: 3086/3088/3091 (помогает определить тип)

    Returns:
        dict с полями или None если не распознано:
        {
            "pair": "BNB/USDT",
            "pair_norm": "BNBUSDT",
            "tf": "12h",
            "event": "entered_support" | "entered_resistance" |
                     "new_support" | "new_resistance" |
                     "range_ascending" | "range_descending" | "range_sideways",
            "zone_low": float,
            "zone_high": float,
            "current_price": float | None,
            "age_days": int | None,
            "created_at": datetime | None,
        }
    """
    if not text:
        return None

    # Symbol (обязательное)
    sym_m = re.search(r"Symbol:\s*#?(\w+)", text)
    if not sym_m:
        return None
    pair_norm = sym_m.group(1).upper()
    # Привести к BNBUSDT формату
    if not pair_norm.endswith("USDT"):
        pair_norm = pair_norm + "USDT"
    pair = pair_norm[:-4] + "/USDT"

    # Timeframe
    tf_m = re.search(r"Timeframe:\s*(\w+)", text)
    tf = tf_m.group(1).lower() if tf_m else "?"

    # Current Price
    cp_m = re.search(r"Current Price:\s*([\d.]+)", text)
    current_price = float(cp_m.group(1)) if cp_m else None

    # Определяем event type по тексту + topic_id
    event = None
    zone_low = None
    zone_high = None

    if "Price Entered SUPPORT" in text.upper() or "PRICE ENTERED SUPPORT" in text.upper():
        event = "entered_support"
    elif "Price Entered RESISTANCE" in text.upper() or "PRICE ENTERED RESISTANCE" in text.upper():
        event = "entered_resistance"
    elif "NEW SUPPORT LEVEL" in text.upper() or "NEW SUPPORT" in text.upper():
        event = "new_support"
    elif "NEW RESISTANCE LEVEL" in text.upper() or "NEW RESISTANCE" in text.upper():
        event = "new_resistance"
    elif "ASCENDING RANGE" in text.upper():
        event = "range_ascending"
    elif "DESCENDING RANGE" in text.upper():
        event = "range_descending"
    elif "SIDEWAYS RANGE" in text.upper() or "SIDEWAY RANGE" in text.upper():
        event = "range_sideways"
    elif "RANGE DETECTED" in text.upper():
        event = "range_sideways"  # fallback

    if not event:
        return None

    # Зона — для SUPPORT/RESISTANCE (Zone: X - Y)
    if event in ("entered_support", "entered_resistance", "new_support", "new_resistance"):
        z_m = re.search(r"Zone:\s*([\d.]+)\s*-\s*([\d.]+)", text)
        if z_m:
            zone_low = float(z_m.group(1))
            zone_high = float(z_m.group(2))
    # Зона для RANGES (Support Zone + Resistance Zone)
    elif event.startswith("range_"):
        sz_m = re.search(r"Support Zone:\s*([\d.]+)", text)
        rz_m = re.search(r"Resistance Zone:\s*([\d.]+)", text)
        if sz_m and rz_m:
            zone_low = float(sz_m.group(1))
            zone_high = float(rz_m.group(1))

    # Age
    age_m = re.search(r"Age:\s*(\d+)\s*d", text, re.IGNORECASE)
    age_days = int(age_m.group(1)) if age_m else None

    # Created
    created_at = None
    cr_m = re.search(r"Created:\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})", text)
    if cr_m:
        try:
            created_at = datetime.strptime(cr_m.group(1), "%Y-%m-%d %H:%M:%S")
        except Exception:
            pass

    return {
        "pair": pair,
        "pair_norm": pair_norm,
        "tf": tf,
        "event": event,
        "zone_low": zone_low,
        "zone_high": zone_high,
        "current_price": current_price,
        "age_days": age_days,
        "created_at": created_at,
    }


def save_key_level(parsed: dict, message_id: int = None) -> bool:
    """Сохраняет распарсенный KL в БД. Дедуп по (pair_norm, tf, event, zone_low) в окне 1ч.

    Returns:
        True если инсертнули (новая запись), False если дубликат.
    """
    if not parsed or not parsed.get("pair_norm"):
        return False

    col = _key_levels()
    now = utcnow()

    # Дедуп — такой же event+pair+tf+zone в последний час?
    recent = col.find_one({
        "pair_norm": parsed["pair_norm"],
        "tf": parsed["tf"],
        "event": parsed["event"],
        "zone_low": parsed.get("zone_low"),
        "zone_high": parsed.get("zone_high"),
        "detected_at": {"$gte": now - timedelta(hours=1)},
    })
    if recent:
        return False

    doc = {
        **parsed,
        "detected_at": now,
        "message_id": message_id,
    }
    try:
        col.insert_one(doc)
        logger.info(f"[KL] saved {parsed['pair_norm']} {parsed['event']} tf={parsed['tf']} zone={parsed.get('zone_low')}-{parsed.get('zone_high')}")
        return True
    except Exception as e:
        logger.warning(f"[KL] save fail: {e}")
        return False


# ───────────────────────────────────────────────
# Обогащение сигналов — get emoji по правильной логике
# ───────────────────────────────────────────────

# TF power — более крупные TF имеют больший вес
TF_POWER = {"1h": 1, "2h": 1.2, "4h": 1.5, "6h": 1.7, "8h": 1.8, "12h": 2.0, "1d": 2.5}


def _tf_power(tf: str) -> float:
    return TF_POWER.get((tf or "").lower(), 1.0)


def get_signal_emoji(pair: str, direction: str, at: datetime,
                      window_h: int = ENRICH_WINDOW_H) -> Optional[dict]:
    """Обогащает сигнал данными о Key Level событиях.

    Args:
        pair: "BNB/USDT" или "BNBUSDT"
        direction: "LONG" / "SHORT" / "BULLISH" / "BEARISH"
        at: datetime сигнала (naive UTC)
        window_h: окно поиска KL (±часы)

    Returns:
        None если ничего не найдено, иначе:
        {
            "emoji": "🎢",
            "label": "Breakout UP через RESISTANCE 12h",
            "strength": "strong" | "confirming" | "warning" | "neutral",
            "event": "entered_resistance",
            "tf": "12h",
            "age_days": 10,
            "zone_low": 0.45,
            "zone_high": 0.48,
            "kl_time": datetime,
        }
    """
    if not pair or not direction or not at:
        return None
    pair_norm = pair.replace("/", "").upper()
    if not pair_norm.endswith("USDT"):
        pair_norm = pair_norm + "USDT"
    is_long = direction.upper() in ("LONG", "BUY", "BULLISH")

    start = at - timedelta(hours=window_h)
    end = at + timedelta(hours=window_h)

    # Тянем все KL события по паре в окне
    candidates = list(_key_levels().find({
        "pair_norm": pair_norm,
        "detected_at": {"$gte": start, "$lte": end},
    }).sort("detected_at", -1).limit(10))

    if not candidates:
        return None

    # Приоритет: breakout > falling knife > confirming > range > neutral
    # По каждой категории берём самый свежий + самый высокий TF
    strong = []      # breakout (🎢 / 🧨)
    warning = []     # falling knife (🔪)
    confirming = []  # new level same side (⚓)
    range_ev = []    # 〰️

    for kl in candidates:
        ev = kl.get("event", "")
        if is_long:
            if ev == "entered_resistance":
                strong.append((kl, "🎢", "Breakout UP", "strong"))
            elif ev == "entered_support":
                warning.append((kl, "🔪", "Falling knife через SUPPORT", "warning"))
            elif ev == "new_support":
                confirming.append((kl, "⚓", "Новая SUPPORT снизу", "confirming"))
            elif ev == "new_resistance":
                warning.append((kl, "🔪", "Новая RESISTANCE сверху (риск)", "warning"))
            elif ev.startswith("range_"):
                range_ev.append((kl, "〰️", f"In range ({ev.replace('range_', '')})", "neutral"))
        else:  # SHORT
            if ev == "entered_support":
                strong.append((kl, "🧨", "Breakdown DOWN", "strong"))
            elif ev == "entered_resistance":
                warning.append((kl, "🔪", "Против тренда через RESISTANCE", "warning"))
            elif ev == "new_resistance":
                confirming.append((kl, "⚓", "Новая RESISTANCE сверху", "confirming"))
            elif ev == "new_support":
                warning.append((kl, "🔪", "Новая SUPPORT снизу (риск)", "warning"))
            elif ev.startswith("range_"):
                range_ev.append((kl, "〰️", f"In range ({ev.replace('range_', '')})", "neutral"))

    # Финальный выбор — по приоритету + TF power
    def best_of(lst):
        if not lst:
            return None
        # Сортируем по (strength rank, tf power, detected_at desc)
        rank = {"strong": 4, "warning": 3, "confirming": 2, "neutral": 1}
        lst.sort(key=lambda x: (rank.get(x[3], 0), _tf_power(x[0].get("tf", "")), x[0].get("detected_at")), reverse=True)
        return lst[0]

    chosen = best_of(strong) or best_of(warning) or best_of(confirming) or best_of(range_ev)
    if not chosen:
        return None

    kl, emoji, label_prefix, strength = chosen
    tf = kl.get("tf", "?")
    age = kl.get("age_days")
    age_str = f", age {age}d" if age else ""
    label = f"{label_prefix} {tf}{age_str}"

    return {
        "emoji": emoji,
        "label": label,
        "strength": strength,
        "event": kl.get("event"),
        "tf": tf,
        "age_days": age,
        "zone_low": kl.get("zone_low"),
        "zone_high": kl.get("zone_high"),
        "kl_time": kl.get("detected_at"),
        "current_price_at_kl": kl.get("current_price"),
    }


def get_recent_levels(pair: str, hours: int = 48) -> list[dict]:
    """Для UI — возвращает все активные KL уровни по паре за окно.
    Используется для отрисовки зон на графике."""
    if not pair:
        return []
    pair_norm = pair.replace("/", "").upper()
    if not pair_norm.endswith("USDT"):
        pair_norm = pair_norm + "USDT"
    since = utcnow() - timedelta(hours=hours)
    out = []
    for kl in _key_levels().find({
        "pair_norm": pair_norm,
        "detected_at": {"$gte": since},
        "zone_low": {"$ne": None},
        "zone_high": {"$ne": None},
    }).sort("detected_at", -1).limit(20):
        kl.pop("_id", None)
        for k in ("detected_at", "created_at"):
            v = kl.get(k)
            if hasattr(v, "isoformat"):
                kl[k] = v.isoformat()
        out.append(kl)
    return out


def format_tg_block(enrich: dict) -> str:
    """Формирует строку для вставки в Telegram алерт (HTML).
    Пример: '🎢 <b>KEY LEVEL:</b> Breakout UP 12h, age 10d'"""
    if not enrich:
        return ""
    emoji = enrich.get("emoji", "")
    label = enrich.get("label", "")
    zone = ""
    if enrich.get("zone_low") and enrich.get("zone_high"):
        zone = f" · zone {enrich['zone_low']}-{enrich['zone_high']}"
    return f"\n{emoji} <b>KEY LEVEL:</b> {label}{zone}\n"
