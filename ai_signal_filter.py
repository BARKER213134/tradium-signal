"""AI Signal Filter — Claude анализирует бектест и решает
какие новые сигналы отправлять в @aitradiumbot.

Логика:
1. Берёт backtest summary (статистика по всем прошлым сигналам)
2. Для каждого нового сигнала в статусе ПАТТЕРН отправляет в Claude:
   - Данные сигнала (pair, direction, pattern, trend, entry, price)
   - Backtest summary (что исторически работало)
3. Claude возвращает: send=True/False + score 1-10 + reasoning
4. Если send=True → статус AI_SIGNAL → алерт в @aitradiumbot
"""
import asyncio
import json
import logging
import re

import anthropic

from config import ANTHROPIC_API_KEY, ANTHROPIC_MODEL

logger = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

FILTER_PROMPT = """Ты — AI-трейдер для дейтрейдинга криптовалют.

Тебе дан НОВЫЙ сигнал и БЕКТЕСТ (статистика прошлых сигналов).
На основе бектеста ты НАУЧИЛСЯ определять какие сигналы отрабатывают.

Задача: решить — отправлять этот сигнал трейдеру или нет.

ПРАВИЛА:
- Смотри на бектест: какие паттерны дают лучший win_rate и avg_pnl
- Смотри направление: LONG vs SHORT — что лучше работает
- Смотри время: какие часы дают лучшие результаты
- Смотри тренд (5 кружков): сильный тренд = лучше
- Если паттерн в worst_patterns — НЕ отправлять
- Если паттерн в top_patterns + направление совпадает + время хорошее → отправлять
- Ты сам определяешь параметры и пороги на основе данных

Верни ТОЛЬКО JSON:
{
  "send": true,
  "score": 8,
  "reasoning": "Паттерн Hammer показал 72% win rate в бектесте, LONG в текущее время суток даёт +2.3% avg, сильный тренд RGGGG."
}

Поля:
- send: true (отправлять) / false (пропустить)
- score: 1-10 качество сигнала (10 = лучший)
- reasoning: 1-2 предложения на русском почему"""


async def should_send_signal(signal_data: dict, backtest_summary: str) -> dict:
    """Решает отправлять ли сигнал на основе бектеста.

    signal_data: {pair, direction, pattern, trend, entry, current_price, ai_score, hour}
    backtest_summary: текстовый summary от backtest_summary_for_ai()

    Returns: {send: bool, score: int, reasoning: str}
    """
    user_text = (
        f"{FILTER_PROMPT}\n\n"
        f"=== БЕКТЕСТ ===\n{backtest_summary}\n\n"
        f"=== НОВЫЙ СИГНАЛ ===\n"
        f"Пара: {signal_data.get('pair')}\n"
        f"Направление: {signal_data.get('direction')}\n"
        f"Паттерн: {signal_data.get('pattern')}\n"
        f"Тренд: {signal_data.get('trend')}\n"
        f"Entry: {signal_data.get('entry')}\n"
        f"Текущая цена: {signal_data.get('current_price')}\n"
        f"AI score (визуал): {signal_data.get('ai_score')}\n"
        f"Час UTC: {signal_data.get('hour')}\n"
    )

    try:
        message = await asyncio.to_thread(
            client.messages.create,
            model=ANTHROPIC_MODEL,
            max_tokens=256,
            messages=[{"role": "user", "content": user_text}],
        )
        raw = message.content[0].text

        try:
            result = json.loads(raw.strip())
        except Exception:
            match = re.search(r"\{.*\}", raw, re.DOTALL)
            if match:
                result = json.loads(match.group())
            else:
                return {"send": False, "score": 0, "reasoning": "parse error", "_raw": raw}

        result["send"] = bool(result.get("send", False))
        result["score"] = max(1, min(10, int(result.get("score", 5))))
        result["_raw"] = raw
        return result
    except Exception as e:
        logger.error(f"AI filter error: {e}")
        return {"send": False, "score": 0, "reasoning": str(e)}
