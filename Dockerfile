FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libpq-dev curl git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (better layer caching)
COPY pyproject.toml .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -e .

# Download spaCy English model
RUN python -m spacy download en_core_web_sm

COPY . .

# Non-root user for security
RUN useradd -m -u 1000 trader && chown -R trader:trader /app
USER trader

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

# No ENTRYPOINT — each service sets its own CMD in docker-compose
CMD ["python", "main.py", "--mode", "paper"]
