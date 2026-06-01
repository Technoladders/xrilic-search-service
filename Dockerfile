# ─────────────────────────────────────────────────────────────────────────────
# xrilic-search-service / Dockerfile
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.12-slim

WORKDIR /app

# Install deps first (cache layer)
COPY sync_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY sync_service/ .

# Non-root user
RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD python -c "import httpx, sys; r=httpx.get('http://localhost:8000/health', timeout=5); sys.exit(0 if r.status_code==200 else 1)"

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]