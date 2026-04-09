FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Системные зависимости для matplotlib/mplfinance
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        libfreetype6-dev \
        libpng-dev \
        pkg-config \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Сначала requirements — используем кэш слоёв
COPY requirements.txt .
RUN pip install -r requirements.txt

# Потом код
COPY . .

# Папка для графиков и session (mount через volume)
RUN mkdir -p /app/charts /app/data

# Healthcheck
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD curl -fsS http://localhost:8000/health || exit 1

EXPOSE 8000

CMD ["python", "main.py"]
