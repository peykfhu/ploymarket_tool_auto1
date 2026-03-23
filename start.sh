#!/usr/bin/env bash
# ============================================================
#  Polymarket Trader — ONE-CLICK STARTUP
#  Usage: ./start.sh [paper|live|backtest]
# ============================================================
set -e
MODE=${1:-paper}
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'; BOLD='\033[1m'

echo -e "${BOLD}"
echo "  ██████╗  ██████╗ ██╗  ██╗   ██╗████████╗██████╗  █████╗ ██████╗ ███████╗██████╗ "
echo "  ██╔══██╗██╔═══██╗██║  ╚██╗ ██╔╝╚══██╔══╝██╔══██╗██╔══██╗██╔══██╗██╔════╝██╔══██╗"
echo "  ██████╔╝██║   ██║██║   ╚████╔╝    ██║   ██████╔╝███████║██║  ██║█████╗  ██████╔╝"
echo "  ██╔═══╝ ██║   ██║██║    ╚██╔╝     ██║   ██╔══██╗██╔══██║██║  ██║██╔══╝  ██╔══██╗"
echo "  ██║     ╚██████╔╝███████╗██║      ██║   ██║  ██║██║  ██║██████╔╝███████╗██║  ██║"
echo "  ╚═╝      ╚═════╝ ╚══════╝╚═╝      ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝╚═════╝ ╚══════╝╚═╝  ╚═╝"
echo -e "${NC}"
echo -e "  ${YELLOW}Mode: ${BOLD}${MODE^^}${NC}  |  Dashboard: ${GREEN}http://localhost:3056${NC}  |  API: ${GREEN}http://localhost:8088${NC}"
echo ""

# ── Pre-flight checks ────────────────────────────────────────────────────────
check() { command -v "$1" &>/dev/null || { echo -e "${RED}✗ $1 not found. Install it first.${NC}"; exit 1; }; echo -e "${GREEN}✓${NC} $1"; }
echo -e "${BOLD}[1/5] Checking dependencies...${NC}"
check docker
check docker-compose 2>/dev/null || check "docker compose"
echo ""

# ── .env setup ───────────────────────────────────────────────────────────────
echo -e "${BOLD}[2/5] Checking environment...${NC}"
if [ ! -f .env ]; then
  cp .env.example .env
  echo -e "${YELLOW}  ⚠  Created .env from .env.example${NC}"
  echo -e "${YELLOW}  ⚠  IMPORTANT: Edit .env and add your secrets, then re-run this script.${NC}"
  echo ""
  echo -e "  Required secrets:"
  echo -e "  ${YELLOW}  DATABASE__POSTGRES_PASSWORD${NC}  — any strong password"
  echo -e "  ${YELLOW}  DATABASE__REDIS_PASSWORD${NC}     — any strong password"
  echo -e "  ${YELLOW}  NOTIFICATION__TELEGRAM_BOT_TOKEN${NC} — from @BotFather"
  echo -e "  ${YELLOW}  NOTIFICATION__TELEGRAM_CHAT_ID${NC}   — your chat ID"
  echo ""
  read -p "  Open .env now? [y/N] " yn
  [ "$yn" = "y" ] && ${EDITOR:-nano} .env
  exit 0
fi

# Validate minimum required vars
source .env 2>/dev/null || true
if [ -z "$DATABASE__POSTGRES_PASSWORD" ] || [ "$DATABASE__POSTGRES_PASSWORD" = "your_strong_password_here" ]; then
  echo -e "${RED}  ✗ DATABASE__POSTGRES_PASSWORD not set in .env${NC}"; exit 1
fi
if [ -z "$DATABASE__REDIS_PASSWORD" ] || [ "$DATABASE__REDIS_PASSWORD" = "your_redis_password_here" ]; then
  echo -e "${RED}  ✗ DATABASE__REDIS_PASSWORD not set in .env${NC}"; exit 1
fi
echo -e "${GREEN}  ✓ .env looks good${NC}"
echo ""

# ── Set mode in .env ─────────────────────────────────────────────────────────
if [ "$MODE" = "live" ]; then
  echo -e "${RED}  ⚠  LIVE MODE — real money will be used!${NC}"
  read -p "  Type 'yes' to confirm: " confirm
  [ "$confirm" != "yes" ] && { echo "Aborted."; exit 1; }
fi
sed -i.bak "s/^EXECUTION__MODE=.*/EXECUTION__MODE=${MODE}/" .env 2>/dev/null || \
  echo "EXECUTION__MODE=${MODE}" >> .env
echo -e "${GREEN}  ✓ Mode set to ${MODE}${NC}"
echo ""

# ── Create required directories ───────────────────────────────────────────────
echo -e "${BOLD}[3/5] Creating directories...${NC}"
mkdir -p logs data/historical/{markets,news,sports,whales}
echo -e "${GREEN}  ✓ logs/ data/ created${NC}"
echo ""

# ── Pull images ───────────────────────────────────────────────────────────────
echo -e "${BOLD}[4/5] Pulling Docker images...${NC}"
docker compose pull postgres redis 2>&1 | grep -E "Pull|already" | sed 's/^/  /'
echo ""

# ── Start services ────────────────────────────────────────────────────────────
echo -e "${BOLD}[5/5] Starting services...${NC}"
docker compose up -d --build

# Wait for health checks
echo -ne "  Waiting for Postgres"
for i in $(seq 1 30); do
  docker compose exec -T postgres pg_isready -U trader -d polymarket &>/dev/null && break
  echo -n "." && sleep 2
done
echo -e " ${GREEN}✓${NC}"

echo -ne "  Waiting for Redis"
for i in $(seq 1 20); do
  docker compose exec -T redis redis-cli -a "${DATABASE__REDIS_PASSWORD:-changeme}" ping &>/dev/null && break
  echo -n "." && sleep 1
done
echo -e " ${GREEN}✓${NC}"

echo -ne "  Waiting for API (port 8088)"
for i in $(seq 1 30); do
  curl -sf http://localhost:8088/health &>/dev/null && break
  echo -n "." && sleep 2
done
echo -e " ${GREEN}✓${NC}"

echo -ne "  Waiting for Dashboard (port 3056)"
for i in $(seq 1 20); do
  curl -sf http://localhost:3056/ &>/dev/null && break
  echo -n "." && sleep 1
done
echo -e " ${GREEN}✓${NC}"
echo ""

# ── Done ──────────────────────────────────────────────────────────────────────
echo -e "${GREEN}${BOLD}═══════════════════════════════════════════════════${NC}"
echo -e "${GREEN}${BOLD}  ✓  Polymarket Trader is running!${NC}"
echo -e "${GREEN}${BOLD}═══════════════════════════════════════════════════${NC}"
echo ""
echo -e "  🖥  Dashboard   →  ${GREEN}http://localhost:3056${NC}"
echo -e "  🔌  API Docs    →  ${GREEN}http://localhost:8088/docs${NC}"
echo -e "  📊  Grafana     →  ${YELLOW}docker compose --profile monitoring up -d${NC}"
echo ""
echo -e "  Useful commands:"
echo -e "  ${YELLOW}docker compose logs -f app${NC}     — follow trading logs"
echo -e "  ${YELLOW}docker compose logs -f api${NC}     — follow API logs"
echo -e "  ${YELLOW}./stop.sh${NC}                      — stop all services"
echo ""
