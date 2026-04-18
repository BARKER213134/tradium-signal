FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Системные зависимости для matplotlib/mplfinance + Chromium для Resonance
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        libfreetype6-dev \
        libpng-dev \
        pkg-config \
        curl \
        chromium \
        chromium-driver \
    && rm -rf /var/lib/apt/lists/*

ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

# Сначала requirements — используем кэш слоёв
COPY requirements.txt .
RUN pip install -r requirements.txt

# Потом код
COPY . .

# Папка для графиков и session (mount через volume)
RUN mkdir -p /app/charts /app/data

# Healthcheck — lightweight /healthz (без Mongo I/O чтобы не рестарт
# контейнера при лагах Atlas). /health для ручной диагностики.
# interval 60s + timeout 10s + retries 5 = 5 минут грейса перед unhealthy.
HEALTHCHECK --interval=60s --timeout=10s --start-period=60s --retries=5 \
  CMD curl -fsS http://localhost:${PORT:-8080}/healthz || exit 1

EXPOSE 8080

CMD ["python", "main.py"]
