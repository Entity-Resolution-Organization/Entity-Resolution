# ============================================================
#  Inference Pipeline — Docker Image (Cloud Run)
#  Entity Resolution — Group 13, Northeastern MLOps Spring 2026
# ============================================================
#
#  Single container: FastAPI backend + React frontend (static)
#
#  Build:
#    docker build -t entity-resolution-inference .
#
#  Run:
#    docker run -p 8000:8000 --env-file .env entity-resolution-inference
# ============================================================

# --- Stage 1: Build React frontend ---
FROM node:20-slim AS frontend-build

WORKDIR /frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ ./
RUN npm run build

# --- Stage 2: Python backend ---
FROM python:3.10-slim

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python dependencies (layer cached)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Application code
COPY config/     ./config/
COPY scripts/    ./scripts/
COPY dags/       ./dags/
COPY static/     ./static/

# Copy built frontend from stage 1
COPY --from=frontend-build /frontend/dist ./frontend/dist

# Non-root user
RUN useradd --create-home --shell /bin/bash appuser \
    && chown -R appuser:appuser /app
USER appuser

# Environment defaults
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    PORT=8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# Run FastAPI (serves both API + frontend static files)
CMD ["uvicorn", "scripts.app:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1"]
