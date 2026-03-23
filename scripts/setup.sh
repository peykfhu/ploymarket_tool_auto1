#!/usr/bin/env bash
set -e
echo "=== Polymarket Trader Setup ==="
[ ! -f .env ] && cp .env.example .env && echo "Created .env from .env.example — fill in your secrets."
pip install --upgrade pip
pip install -e ".[dev]"
python -m spacy download en_core_web_sm
mkdir -p logs data/historical/{markets,news,sports,whales}
echo "=== Setup complete. Edit .env then: docker compose up ==="
