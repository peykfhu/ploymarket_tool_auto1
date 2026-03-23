#!/usr/bin/env bash
# One-click stop
set -e
echo "Stopping Polymarket Trader..."
docker compose down
echo "✓ All services stopped."
