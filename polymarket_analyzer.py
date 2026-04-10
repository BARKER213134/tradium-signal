"""AI-анализ рынков Polymarket через Claude.

Для каждого рынка:
1. Отправляет вопрос + описание + текущие odds в Claude
2. Claude оценивает реальную вероятность
3. Если edge (разница AI vs рынок) > threshold → сигнал
"""
import asyncio
import logging

import anthropic

from config import ANTHROPIC_API_KEY, ANTHROPIC_MODEL

logger = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

PROMPT = """Ты — аналитик рынков предсказаний (Polymarket).
Тебе дан рынок с вопросом и текущими ценами YES/NO.

Задача: оцени РЕАЛЬНУЮ вероятность исхода YES на основе:
- Здравого смысла и общих знаний
- Текущей политической/экономической ситуации
- Исторических прецедентов
- Deadline события

Верни ТОЛЬКО JSON без markdown:
{
  "probability": 0.65,
  "confidence": "high",
  "side": "YES",
  "reasoning": "Краткое объяснение на русском, 1-2 предложения",
  "edge": 0.10
}

Поля:
- probability: твоя оценка реальной вероятности YES (0.0 - 1.0)
- confidence: "low" / "medium" / "high" — насколько ты уверен
- side: "YES" или "NO" — куда ставить (где есть edge)
- reasoning: почему ты так считаешь
- edge: разница между твоей оценкой и текущей ценой (абсолютная)

Если ты не знаешь достаточно о событии — ставь confidence: "low".
Если edge < 0.05 — side: "SKIP" (не ставить, нет преимущества)."""


async def analyze_market(market: dict) -> dict:
    """Анализирует один рынок. Возвращает dict с probability/side/edge/reasoning."""
    question = market.get("question", "")
    description = market.get("description", "")
    yes_price = market.get("yes_price")
    no_price = market.get("no_price")
    end_date = market.get("end_date", "")
    volume = market.get("volume_24h", 0)
    category = market.get("category", "")

    user_text = (
        f"{PROMPT}\n\n"
        f"Рынок: {question}\n"
        f"Описание: {description[:300]}\n"
        f"Категория: {category}\n"
        f"YES цена: ${yes_price} ({yes_price*100:.0f}%)\n"
        f"NO цена: ${no_price} ({no_price*100:.0f}%)\n"
        f"Объём 24h: ${volume:,.0f}\n"
        f"Deadline: {end_date or 'не указан'}"
    )

    try:
        message = await asyncio.to_thread(
            client.messages.create,
            model=ANTHROPIC_MODEL,
            max_tokens=512,
            messages=[{"role": "user", "content": user_text}],
        )
        raw = message.content[0].text
        import json, re
        try:
            result = json.loads(raw.strip())
        except Exception:
            match = re.search(r"\{.*\}", raw, re.DOTALL)
            if match:
                result = json.loads(match.group())
            else:
                return {"_error": "parse_failed", "_raw": raw}

        # Нормализация
        prob = float(result.get("probability", 0.5))
        prob = max(0.0, min(1.0, prob))
        result["probability"] = prob

        # Вычисляем edge
        if yes_price is not None:
            if result.get("side") == "YES":
                result["edge"] = round(prob - yes_price, 3)
            elif result.get("side") == "NO":
                result["edge"] = round((1 - prob) - no_price, 3)
            else:
                result["edge"] = 0

        result["_raw"] = raw
        return result
    except Exception as e:
        logger.error(f"Polymarket AI error: {e}")
        return {"_error": str(e)}


def kelly_fraction(probability: float, odds_price: float, fraction: float = 0.25) -> float:
    """Kelly criterion — оптимальный размер ставки.

    probability: AI-оценка реальной вероятности
    odds_price: текущая цена на рынке (0-1)
    fraction: Kelly fraction (0.25 = четверть Kelly, консервативно)

    Returns: доля бюджета на ставку (0.0 - 1.0)
    """
    if odds_price <= 0 or odds_price >= 1 or probability <= 0:
        return 0.0
    # Payout = 1/price, но мы тратим price → net profit = (1 - price)
    b = (1.0 / odds_price) - 1.0  # net odds
    q = 1.0 - probability
    kelly = (probability * b - q) / b
    if kelly <= 0:
        return 0.0
    return round(kelly * fraction, 4)  # fraction of full Kelly
