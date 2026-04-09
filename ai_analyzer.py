import anthropic
import asyncio
import base64
import json
import logging
import random
import re
from pathlib import Path
from config import ANTHROPIC_API_KEY, ANTHROPIC_MODEL

logger = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# ─── Retry policy ───────────────────────────────────────────────────────
MAX_ATTEMPTS = 4
BASE_DELAY = 2.0   # секунды
MAX_DELAY = 30.0
# Ошибки, которые имеет смысл повторять
_RETRIABLE = (
    anthropic.APIConnectionError,
    anthropic.APITimeoutError,
    anthropic.RateLimitError,
    anthropic.InternalServerError,
    anthropic.APIStatusError,
)

CHART_PROMPT = """Ты — профессиональный трейдер и аналитик торговых графиков.

Тебе прислали скриншот торгового графика (TradingView или MT4/MT5).
Внимательно изучи его и извлеки ВСЕ данные которые видишь.

Верни ТОЛЬКО JSON без markdown блоков, в таком формате:
{
  "pair": "KAITOUSDT",
  "direction": "LONG",
  "entry": 0.4262,
  "sl": 0.4063,
  "tp1": 0.4661,
  "tp2": null,
  "tp3": null,
  "dca1": 0.4262,
  "dca2": 0.4213,
  "dca3": 0.4162,
  "dca4": 0.4115,
  "timeframe": "1h",
  "pattern": "Support bounce",
  "notes": "Любые дополнительные данные"
}

Правила:
- pair: торговый инструмент (BTCUSDT, KAITOUSDT и т.д.)
- direction: LONG или SHORT (или BUY/SELL)
- entry: цена входа (число)
- sl: стоп-лосс (число)
- tp1/tp2/tp3: тейк-профиты (числа, null если нет)
- dca1/dca2/dca3/dca4: уровни DCA ордеров (BUY DCA #1..#5 на графике). dca4 — цена где открывается ордер #4, это КЛЮЧЕВОЕ поле для нас.
- timeframe: таймфрейм (1m,5m,15m,1h,4h,1D и т.д.)
- pattern: паттерн или сигнал если виден на графике
- notes: всё остальное что видишь на графике

Если какое-то поле не видно на графике — ставь null.
Возвращай ТОЛЬКО валидный JSON, без пояснений."""


async def analyze_chart(image_path: str) -> dict:
    """Отправляет график в Claude Vision с retry + exponential backoff.

    Возвращает словарь с полями или {"_error": ...} после всех попыток.
    """
    path = Path(image_path)
    if not path.exists():
        logger.error(f"Файл графика не найден: {image_path}")
        return {"_error": "file_not_found"}

    suffix = path.suffix.lower()
    media_type = {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png", ".webp": "image/webp",
    }.get(suffix, "image/jpeg")

    with open(image_path, "rb") as f:
        image_data = base64.standard_b64encode(f.read()).decode("utf-8")

    last_err: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            logger.info(f"Claude Vision [{attempt}/{MAX_ATTEMPTS}]: {path.name}")
            message = await asyncio.to_thread(
                client.messages.create,
                model=ANTHROPIC_MODEL,
                max_tokens=1024,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "image", "source": {
                            "type": "base64", "media_type": media_type, "data": image_data,
                        }},
                        {"type": "text", "text": CHART_PROMPT},
                    ],
                }],
            )
            raw_response = message.content[0].text
            logger.info(f"Ответ Claude Vision: {raw_response[:200]}")
            result = parse_ai_response(raw_response)
            if not result:
                # JSON не распарсился — это тоже неуспех, повторяем
                raise ValueError("empty_parsed_response")
            result["_raw"] = raw_response
            result["_attempts"] = attempt
            return result

        except _RETRIABLE as e:
            last_err = e
            if attempt >= MAX_ATTEMPTS:
                break
            delay = min(MAX_DELAY, BASE_DELAY * (2 ** (attempt - 1)))
            delay += random.uniform(0, delay * 0.25)  # jitter
            logger.warning(
                f"Claude Vision retry {attempt}/{MAX_ATTEMPTS} через {delay:.1f}s: {e}"
            )
            await asyncio.sleep(delay)
        except ValueError as e:
            last_err = e
            if attempt >= MAX_ATTEMPTS:
                break
            delay = min(MAX_DELAY, BASE_DELAY * (2 ** (attempt - 1)))
            logger.warning(f"Parse retry {attempt}/{MAX_ATTEMPTS} через {delay:.1f}s")
            await asyncio.sleep(delay)
        except Exception as e:
            # Не-ретраибельная ошибка — сразу выход
            logger.error(f"Неретраибельная ошибка Claude Vision: {e}")
            return {"_error": str(e), "_attempts": attempt}

    logger.error(f"Claude Vision провалился после {MAX_ATTEMPTS} попыток: {last_err}")
    return {"_error": str(last_err), "_attempts": MAX_ATTEMPTS}


QUALITY_PROMPT = """Ты — опытный крипто-трейдер. Оцени качество торгового сетапа.

Тебе дан график + параметры сделки. Задача: вернуть вероятность того что сделка
отработает в TP прежде чем упадёт в SL. Оценка 0-100.

Учитывай:
- Текущая структура рынка (тренд, контртренд, коррекция, флет)
- Разумность R:R и близость SL/TP к структурным уровням
- Качество точки входа (рядом ли уровень поддержки/сопротивления)
- Объём и импульс последних свечей
- Наличие паттерна/divergence/setup
- Контр-сигналы: расходящиеся индикаторы, ключевой уровень против, слабый тренд

Верни ТОЛЬКО JSON, без markdown:
{
  "score": 72,
  "confidence": "high",
  "verdict": "BUY",
  "reasoning": "Цена на ключевой поддержке H4, RSI выходит из перепроданности, объём на отскоке растёт. R:R 2:1 адекватен. Зона DCA даёт запас безопасности.",
  "risks": [
    "Общий тренд BTC нисходящий",
    "Недалеко сильное сопротивление на 0.475"
  ]
}

Поля:
- score: 0-100, где 50=нейтрально, 70+=хороший сетап, 85+=отличный, <40=слабый
- confidence: "low" / "medium" / "high" — насколько ты уверен в оценке
- verdict: "BUY" / "SKIP" / "WAIT" — что бы ты сделал
- reasoning: 1-2 предложения на русском
- risks: список 2-4 коротких рисков"""


async def analyze_signal_quality(image_path: str, signal_ctx: dict) -> dict:
    """Отправляет график + контекст сигнала в Claude для оценки вероятности отработки.

    signal_ctx может содержать: pair, direction, timeframe, entry, sl, tp1,
    dca4, risk_reward, trend, pattern.
    """
    path = Path(image_path)
    if not path.exists():
        return {"_error": "file_not_found"}

    suffix = path.suffix.lower()
    media_type = {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png", ".webp": "image/webp",
    }.get(suffix, "image/jpeg")

    with open(image_path, "rb") as f:
        image_data = base64.standard_b64encode(f.read()).decode("utf-8")

    # Контекст сигнала как компактный JSON-блок
    ctx_json = json.dumps(
        {k: v for k, v in signal_ctx.items() if v is not None},
        ensure_ascii=False,
    )
    user_text = f"{QUALITY_PROMPT}\n\nПараметры сделки:\n{ctx_json}"

    last_err: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            logger.info(f"Claude quality [{attempt}/{MAX_ATTEMPTS}]: {path.name}")
            message = await asyncio.to_thread(
                client.messages.create,
                model=ANTHROPIC_MODEL,
                max_tokens=1024,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "image", "source": {
                            "type": "base64", "media_type": media_type, "data": image_data,
                        }},
                        {"type": "text", "text": user_text},
                    ],
                }],
            )
            raw = message.content[0].text
            logger.info(f"Quality raw: {raw[:200]}")
            parsed = parse_ai_response(raw)
            if not parsed or "score" not in parsed:
                raise ValueError("missing score")
            # нормализация
            try:
                parsed["score"] = int(float(parsed["score"]))
            except Exception:
                parsed["score"] = 0
            parsed["score"] = max(0, min(100, parsed["score"]))
            if "confidence" in parsed:
                parsed["confidence"] = str(parsed["confidence"]).lower()
            parsed["_raw"] = raw
            parsed["_attempts"] = attempt
            return parsed

        except _RETRIABLE as e:
            last_err = e
            if attempt >= MAX_ATTEMPTS:
                break
            delay = min(MAX_DELAY, BASE_DELAY * (2 ** (attempt - 1)))
            delay += random.uniform(0, delay * 0.25)
            logger.warning(f"Quality retry {attempt}/{MAX_ATTEMPTS} через {delay:.1f}s: {e}")
            await asyncio.sleep(delay)
        except ValueError as e:
            last_err = e
            if attempt >= MAX_ATTEMPTS:
                break
            await asyncio.sleep(min(MAX_DELAY, BASE_DELAY * (2 ** (attempt - 1))))
        except Exception as e:
            logger.error(f"Quality неретраибельная ошибка: {e}")
            return {"_error": str(e)}

    logger.error(f"Quality провален после {MAX_ATTEMPTS} попыток: {last_err}")
    return {"_error": str(last_err)}


def parse_ai_response(text: str) -> dict:
    """Извлекает JSON из ответа AI."""
    try:
        # Пробуем напрямую
        return json.loads(text.strip())
    except Exception:
        pass

    # Ищем JSON блок в тексте
    match = re.search(r'\{.*\}', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except Exception:
            pass

    logger.warning("Не удалось распарсить JSON из ответа AI")
    return {}


def merge_signal_data(text_data: dict, chart_data: dict) -> dict:
    """
    Объединяет данные из текста и графика.
    Приоритет: если оба источника дали значение — берём среднее для цифровых,
    текст имеет приоритет для пары и направления.
    """
    merged = {}

    # Пара — приоритет текста, потом график
    merged["pair"] = text_data.get("pair") or chart_data.get("pair")

    # Направление — приоритет текста
    merged["direction"] = text_data.get("direction") or chart_data.get("direction")

    # Числовые значения — приоритет текста, потом график
    for field in ["entry", "sl", "tp1", "tp2", "tp3"]:
        text_val = text_data.get(field)
        chart_val = chart_data.get(field)
        merged[field] = text_val or chart_val

    return merged
