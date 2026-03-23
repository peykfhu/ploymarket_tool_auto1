#!/usr/bin/env bash
# One-click stop all services
set -e
if docker compose version &>/dev/null 2>&1; then DC="docker compose"
elif command -v docker-compose &>/dev/null; then DC="docker-compose"
else echo "docker compose not found"; exit 1; fi

echo "Stopping Polymarket Trader..."
$DC down
echo "✓ All services stopped."
