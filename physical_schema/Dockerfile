FROM python:3.11-slim

# Prevents Python from writing .pyc files and buffers
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# App lives here
WORKDIR /app
ENV PYTHONPATH=/app:/app/tools

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

# Non-root user for security
RUN groupadd --gid 1000 nlsql \
 && useradd --uid 1000 --gid nlsql --shell /bin/bash --create-home nlsql

# Install deps first for better layer caching
COPY requirements.api.txt /app/requirements.api.txt
RUN pip install --no-cache-dir -r /app/requirements.api.txt

# Copy the project
COPY . /app
RUN chown -R nlsql:nlsql /app

USER nlsql

# Default envs (override at runtime)
ENV NL_SQL_LOG_LEVEL=INFO
ENV NL_SQL_API_KEY_HEADER=X-API-Key
ENV UVICORN_WORKERS=1

EXPOSE 8000

CMD ["sh", "-c", "uvicorn api.app:app --host 0.0.0.0 --port 8000 --workers ${UVICORN_WORKERS}"]

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD curl -fsS http://localhost:8000/healthz || exit 1
