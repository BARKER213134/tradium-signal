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


# ════════════════════════════════════════════════════════════════════
# Tier 1 + Tier 2: расширенный блок уровней в Telegram алертах
# ════════════════════════════════════════════════════════════════════

def _zone_mid(kl: dict) -> Optional[float]:
    """Средняя точка зоны уровня."""
    lo, hi = kl.get("zone_low"), kl.get("zone_high")
    if lo is None or hi is None:
        return None
    try:
        return (float(lo) + float(hi)) / 2
    except (TypeError, ValueError):
        return None


def _is_resistance(kl: dict) -> bool:
    ev = kl.get("event", "")
    return "resistance" in ev


def _is_support(kl: dict) -> bool:
    ev = kl.get("event", "")
    return "support" in ev


def _format_price(x: float) -> str:
    if x is None:
        return "—"
    try:
        x = float(x)
    except (TypeError, ValueError):
        return str(x)
    if x >= 1000:  return f"{x:.2f}"
    if x >= 10:    return f"{x:.3f}"
    if x >= 1:     return f"{x:.4f}"
    if x >= 0.01:  return f"{x:.5f}"
    return f"{x:.7f}"


def build_levels_compact(pair: str, direction: str,
                          entry: Optional[float] = None,
                          tp: Optional[float] = None,
                          sl: Optional[float] = None,
                          at: Optional[datetime] = None,
                          hours: int = 48) -> str:
    """Компактная 1-2 строчная версия — для Telegram photo caption (лимит 1024).

    Формат (<=250 chars):
      '\n📐 R 1h 0.03450 (+2.4%, 3TF) · S 4h 0.03320 (-1.6%) · TP⚠️ SL✅\n'
    """
    if not pair:
        return ""
    try:
        pair_norm = pair.replace("/", "").upper()
        if not pair_norm.endswith("USDT"):
            pair_norm = pair_norm + "USDT"
        since = utcnow() - timedelta(hours=hours)
        all_kls = list(_key_levels().find({
            "pair_norm": pair_norm,
            "detected_at": {"$gte": since},
        }).sort("detected_at", -1).limit(100))
        if not all_kls:
            return ""

        is_long = (direction or "").upper() in ("LONG", "BUY", "BULLISH")
        try:
            cur_price = float(entry) if entry is not None else None
        except (TypeError, ValueError):
            cur_price = None
        if cur_price is None:
            for kl in all_kls:
                cp = kl.get("current_price")
                if cp is not None:
                    try: cur_price = float(cp); break
                    except (TypeError, ValueError): pass
        if cur_price is None:
            return ""

        # Группируем уровни R/S по близости (±0.5%)
        def _group(kind: str) -> list[dict]:
            filt = [k for k in all_kls if (
                (kind == 'R' and _is_resistance(k)) or
                (kind == 'S' and _is_support(k))
            ) and _zone_mid(k) is not None]
            groups: list[dict] = []
            for kl in filt:
                mid = _zone_mid(kl)
                placed = False
                for g in groups:
                    if abs(mid - g["mid"]) / max(g["mid"], 1e-9) <= 0.005:
                        g["tfs"].add(kl.get("tf", "?"))
                        placed = True
                        break
                if not placed:
                    groups.append({"mid": mid, "tfs": {kl.get("tf", "?")}})
            groups.sort(key=lambda g: abs(g["mid"] - cur_price))
            return groups

        r_above = next((g for g in _group('R') if g["mid"] > cur_price), None)
        s_below = next((g for g in _group('S') if g["mid"] < cur_price), None)

        if not r_above and not s_below:
            return ""

        parts = []
        if r_above:
            pct = (r_above["mid"] - cur_price) / cur_price * 100
            tfs = "/".join(sorted(r_above["tfs"], key=_tf_power))
            multi = f", {len(r_above['tfs'])}TF" if len(r_above["tfs"]) >= 2 else ""
            tp_tag = ""
            if tp is not None:
                try:
                    if float(tp) >= r_above["mid"]:
                        tp_tag = " TP⚠️"
                    elif float(tp) > cur_price:
                        tp_tag = " TP✅"
                except (TypeError, ValueError): pass
            parts.append(f"🔴{tfs} {_format_price(r_above['mid'])} (+{pct:.1f}%{multi}){tp_tag}")
        if s_below:
            pct = (s_below["mid"] - cur_price) / cur_price * 100
            tfs = "/".join(sorted(s_below["tfs"], key=_tf_power))
            multi = f", {len(s_below['tfs'])}TF" if len(s_below["tfs"]) >= 2 else ""
            sl_tag = ""
            if sl is not None and is_long:
                try:
                    if float(sl) <= s_below["mid"]:
                        sl_tag = " SL✅"
                    else:
                        sl_tag = " SL⚠️"
                except (TypeError, ValueError): pass
            parts.append(f"🟢{tfs} {_format_price(s_below['mid'])} ({pct:.1f}%{multi}){sl_tag}")

        if not parts:
            return ""

        line = "📐 " + " · ".join(parts)

        # Breakout/Retest — только если произошёл в последние 3ч по направлению
        try:
            since_3h = utcnow() - timedelta(hours=3)
            for kl in all_kls:
                ev = kl.get("event", "")
                det = kl.get("detected_at")
                if not det or det < since_3h:
                    continue
                if (is_long and ev == "entered_resistance") or (not is_long and ev == "entered_support"):
                    ago = (utcnow() - det).total_seconds() / 3600
                    emoji = "🎢" if is_long else "🧨"
                    tf = kl.get("tf", "?")
                    line += f"\n{emoji} Breakout {tf} {ago:.0f}ч назад"
                    break
        except Exception:
            pass

        # Ограничиваем на всякий случай до 250 chars
        if len(line) > 250:
            line = line[:247] + "..."
        return f"\n{line}\n"
    except Exception:
        return ""


def build_levels_alert_block(pair: str, direction: str,
                              entry: Optional[float] = None,
                              tp: Optional[float] = None,
                              sl: Optional[float] = None,
                              at: Optional[datetime] = None,
                              hours: int = 48) -> str:
    """Строит подробный блок Key Levels для Telegram алерта.

    Tier 1 (видимая часть):
      1. Расстояние до ближайшего R/S (вверх/вниз от entry)
      2. TP/SL валидация (TP за R? SL под S?)
      3. Multi-TF confluence (сколько TF подтверждают ближайший уровень)

    Tier 2 (фильтры качества):
      4. Breakout/Retest — был ли недавно пробой этого уровня
      5. Age/strength (возраст + кол-во retest'ов)
      6. Obstacle warning — сильный уровень против сделки в пределах ±5%

    Возвращает HTML-строку (с ведущим \\n) или '' если данных нет.
    """
    if not pair:
        return ""
    pair_norm = pair.replace("/", "").upper()
    if not pair_norm.endswith("USDT"):
        pair_norm = pair_norm + "USDT"

    # Тянем все KL за окно
    since = utcnow() - timedelta(hours=hours)
    all_kls = list(_key_levels().find({
        "pair_norm": pair_norm,
        "detected_at": {"$gte": since},
    }).sort("detected_at", -1).limit(200))

    if not all_kls:
        return ""

    is_long = (direction or "").upper() in ("LONG", "BUY", "BULLISH")
    try:
        cur_price = float(entry) if entry is not None else None
    except (TypeError, ValueError):
        cur_price = None
    # Если entry не передан — пробуем текущую цену из самого свежего KL
    if cur_price is None:
        for kl in all_kls:
            cp = kl.get("current_price")
            if cp is not None:
                try: cur_price = float(cp); break
                except (TypeError, ValueError): pass
    if cur_price is None:
        return ""

    # Собираем уникальные уровни R и S (группировка по близости zone_mid в пределах 0.5%)
    # В каждой группе считаем: multi-TF (кол-во разных TF), возраст (самого старого),
    # retest count (кол-во 'entered' событий рядом), самую свежую детекцию.
    def _group_levels(kls: list[dict], kind: str) -> list[dict]:
        """kind: 'R' or 'S'. Возвращает список групп, отсортированных по расстоянию до цены."""
        filtered = [k for k in kls if (
            (kind == 'R' and _is_resistance(k)) or (kind == 'S' and _is_support(k))
        ) and _zone_mid(k) is not None]
        groups: list[dict] = []
        for kl in filtered:
            mid = _zone_mid(kl)
            tf = kl.get("tf", "?")
            det = kl.get("detected_at")
            ev = kl.get("event", "")
            # Ищем существующую группу (zone_mid в пределах ±0.5%)
            placed = False
            for g in groups:
                if abs(mid - g["mid"]) / g["mid"] <= 0.005:
                    g["tfs"].add(tf)
                    if ev.startswith("entered_"):
                        g["retests"] += 1
                    if g["latest"] is None or (det and det > g["latest"]):
                        g["latest"] = det
                    # Возраст = самая ранняя детекция (minimum)
                    if det and (g["oldest"] is None or det < g["oldest"]):
                        g["oldest"] = det
                    g["zone_low"] = min(g["zone_low"], kl.get("zone_low") or g["zone_low"])
                    g["zone_high"] = max(g["zone_high"], kl.get("zone_high") or g["zone_high"])
                    g["count"] += 1
                    placed = True
                    break
            if not placed:
                groups.append({
                    "mid": mid,
                    "tfs": {tf},
                    "retests": 1 if ev.startswith("entered_") else 0,
                    "latest": det,
                    "oldest": det,
                    "zone_low": kl.get("zone_low"),
                    "zone_high": kl.get("zone_high"),
                    "count": 1,
                    "kind": kind,
                })
        # Сортируем: сверху те что ближе к цене
        groups.sort(key=lambda g: abs(g["mid"] - cur_price))
        return groups

    r_groups = _group_levels(all_kls, 'R')
    s_groups = _group_levels(all_kls, 'S')
    nearest_r_above = next((g for g in r_groups if g["mid"] > cur_price), None)
    nearest_s_below = next((g for g in s_groups if g["mid"] < cur_price), None)

    if not nearest_r_above and not nearest_s_below:
        return ""

    # ── Форматирование строки уровня ─────────────────────────────
    def _fmt_group(g: dict) -> str:
        mid = g["mid"]
        pct = (mid - cur_price) / cur_price * 100
        arrow = "↑" if pct > 0 else "↓"
        sign = "+" if pct > 0 else ""
        tf_str = "/".join(sorted(g["tfs"], key=_tf_power))
        multi = f"{len(g['tfs'])}TF" if len(g["tfs"]) > 1 else tf_str
        parts = [f"{_format_price(mid)} ({sign}{pct:.1f}% {arrow})"]
        # Multi-TF confluence
        if len(g["tfs"]) >= 2:
            parts.append(f"<b>{multi}</b>")
        else:
            parts.append(tf_str)
        # Возраст
        if g["oldest"]:
            age_days = max(0, (utcnow() - g["oldest"]).total_seconds() / 86400)
            if age_days >= 1:
                parts.append(f"{age_days:.0f}д")
            else:
                parts.append(f"{age_days*24:.0f}ч")
        # Retest count (считаем 'entered_*' события как retest)
        if g["retests"] >= 1:
            stars = "🔥" if g["retests"] >= 3 else ""
            parts.append(f"{g['retests']}× retest {stars}".strip())
        return " · ".join(parts)

    lines = ["\n─── 📐 <b>Key Levels</b> ───"]

    # Уровни выше и ниже цены
    if nearest_r_above:
        tp_tag = ""
        if tp is not None:
            try:
                tp_f = float(tp)
                if tp_f >= nearest_r_above["mid"]:
                    tp_tag = "   ⚠️ <b>TP за уровнем</b>"
                elif tp_f > cur_price:
                    # TP между ценой и R — ок
                    room_pct = (nearest_r_above["mid"] - tp_f) / cur_price * 100
                    tp_tag = f"   ✅ TP до R (−{room_pct:.1f}% запас)"
            except (TypeError, ValueError):
                pass
        lines.append(f"🔴 R · {_fmt_group(nearest_r_above)}{tp_tag}")

    if nearest_s_below:
        sl_tag = ""
        if sl is not None:
            try:
                sl_f = float(sl)
                if is_long:
                    if sl_f <= nearest_s_below["mid"]:
                        sl_tag = "   ✅ <b>SL защищён</b> (ниже S)"
                    else:
                        # SL выше S — S не защищает
                        sl_tag = "   ⚠️ SL выше S"
            except (TypeError, ValueError):
                pass
        lines.append(f"🟢 S · {_fmt_group(nearest_s_below)}{sl_tag}")

    # ── Tier 2.4: Breakout/Retest детектор ─────────────────────
    # Был ли недавний breakout уровня который совпадает с направлением?
    recent_breakout = None
    since_6h = utcnow() - timedelta(hours=6)
    for kl in all_kls:
        ev = kl.get("event", "")
        det = kl.get("detected_at")
        if not det or det < since_6h:
            continue
        if is_long and ev == "entered_resistance":
            recent_breakout = kl
            break
        if not is_long and ev == "entered_support":
            recent_breakout = kl
            break
    if recent_breakout:
        ago_h = (utcnow() - recent_breakout["detected_at"]).total_seconds() / 3600
        rb_tf = recent_breakout.get("tf", "?")
        rb_mid = _zone_mid(recent_breakout)
        emoji = "🎢" if is_long else "🧨"
        side = "R" if is_long else "S"
        kind_label = "Breakout" if is_long else "Breakdown"
        px = _format_price(rb_mid) if rb_mid else "?"
        lines.append(f"{emoji} {kind_label} {side} {rb_tf} ({px}) · {ago_h:.0f}ч назад → retest?")

    # ── Tier 2.6: Obstacle warning — сильный уровень против сделки в ±5% ──
    # Для LONG — сильная RESISTANCE в пределах +5% выше
    # Для SHORT — сильная SUPPORT в пределах −5% ниже
    obstacle_groups = r_groups if is_long else s_groups
    for g in obstacle_groups:
        pct = (g["mid"] - cur_price) / cur_price * 100
        # Подходит только по направлению
        if is_long and pct <= 0:
            continue
        if not is_long and pct >= 0:
            continue
        # В пределах 5%
        if abs(pct) > 5:
            continue
        # Уже показан как "ближайший" — не дублируем
        if (is_long and g is nearest_r_above) or ((not is_long) and g is nearest_s_below):
            continue
        # "Сильный" = ≥2 TF или TF ≥ 4h
        has_strong_tf = any(_tf_power(tf) >= _tf_power("4h") for tf in g["tfs"])
        if len(g["tfs"]) < 2 and not has_strong_tf:
            continue
        mid = g["mid"]
        sign = "+" if pct > 0 else ""
        tfs = "/".join(sorted(g["tfs"], key=_tf_power))
        lines.append(f"🚨 Сильный {'R' if is_long else 'S'} {tfs} ({_format_price(mid)}, {sign}{pct:.1f}%) на пути")
        break  # Один warning достаточно

    lines.append("")  # Пустая строка в конце
    return "\n".join(lines)
