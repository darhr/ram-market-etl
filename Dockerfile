FROM python:3.12-slim AS builder

LABEL maintainer="ram-market-scraper"
LABEL description="ETL pipeline for RAM price monitoring in peruvian stores"

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install dependencies first for layer caching
COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev --no-install-project

FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# Copy .venv from builder, install project on top
COPY --from=builder /app/.venv /app/.venv

COPY . .

RUN groupadd --gid 1000 appuser && \
    useradd --uid 1000 --gid appuser --shell /bin/bash --create-home appuser && \
    chown -R appuser:appuser /app

USER appuser

ENTRYPOINT ["python", "etl.py"]