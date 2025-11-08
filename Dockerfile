# ---------- BRidge API Dockerfile (production) ----------
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    UVICORN_WORKERS=4

# System deps (psycopg, reportlab fonts, libpq)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc curl libpq5 libpq-dev \
    libfreetype6 libfreetype6-dev libjpeg62-turbo libjpeg62-turbo-dev zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# If you have requirements.txt, use it. Otherwise we’ll install pinned libs directly.
COPY requirements.txt /app/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    if [ -f requirements.txt ]; then pip install -r requirements.txt; else \
      pip install \
        fastapi==0.116.1 uvicorn==0.35.0 gunicorn==23.0.0 \
        databases==0.9.0 SQLAlchemy==2.0.36 asyncpg==0.29.0 psycopg[binary]==3.2.3 \
        python-dotenv==1.0.1 passlib[bcrypt]==1.7.4 itsdangerous==2.2.0 \
        httpx==0.27.2 prometheus-fastapi-instrumentator==7.0.0 prometheus-client==0.20.0 \
        slowapi==0.1.9 structlog==24.1.0 sentry-sdk==2.16.0 \
        reportlab==4.2.5 Babel==2.16.0 pytz==2024.2 \
        pandas==2.2.3 openpyxl==3.1.5 boto3==1.35.41 \
        python-multipart==0.0.9; fi

# Copy app
COPY . /app

# Expose web port
EXPOSE 8000

# Healthcheck hits /healthz
HEALTHCHECK --interval=30s --timeout=5s --retries=5 CMD curl -fsS http://127.0.0.1:8000/healthz || exit 1

# Default command (can be overridden by docker-compose)
CMD ["gunicorn","bridge_buyer_backend:app","-k","uvicorn.workers.UvicornWorker","-w","4","--timeout","90","--bind","0.0.0.0:8000"]
