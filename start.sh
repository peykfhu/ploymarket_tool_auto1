#!/usr/bin/env bash
# =============================================================
#  Polymarket Trader — ONE-CLICK STARTUP
#  Usage:  ./start.sh [paper|live|backtest]
#  Tested: Linux / macOS with Docker installed
# =============================================================
set -e

MODE="${1:-paper}"
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'

# ── Banner ───────────────────────────────────────────────────
echo -e "${BOLD}${BLUE}"
cat << 'BANNER'
  ██████╗  ██████╗ ██╗  ██╗   ██╗████████╗██████╗  █████╗ ██████╗ ███████╗██████╗
  ██╔══██╗██╔═══██╗██║  ╚██╗ ██╔╝╚══██╔══╝██╔══██╗██╔══██╗██╔══██╗██╔════╝██╔══██╗
  ██████╔╝██║   ██║██║   ╚████╔╝    ██║   ██████╔╝███████║██║  ██║█████╗  ██████╔╝
  ██╔═══╝ ██║   ██║██║    ╚██╔╝     ██║   ██╔══██╗██╔══██║██║  ██║██╔══╝  ██╔══██╗
  ██║     ╚██████╔╝███████╗██║      ██║   ██║  ██║██║  ██║██████╔╝███████╗██║  ██║
  ╚═╝      ╚═════╝ ╚══════╝╚═╝      ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝╚═════╝ ╚══════╝╚═╝  ╚═╝
BANNER
echo -e "${NC}"
echo -e "  Mode: ${BOLD}${MODE^^}${NC}  |  Dashboard → ${GREEN}http://localhost:3056${NC}  |  API → ${GREEN}http://localhost:8088${NC}"
echo ""

# ── 1. Check Docker ──────────────────────────────────────────
echo -e "${BOLD}[1/6] Checking Docker...${NC}"
if ! command -v docker &>/dev/null; then
  echo -e "${RED}  ✗ Docker not found. Install from https://docs.docker.com/get-docker/${NC}"
  exit 1
fi
echo -e "  ${GREEN}✓${NC} Docker $(docker --version | grep -oP '\d+\.\d+\.\d+')"

# Detect docker compose command (plugin vs standalone)
if docker compose version &>/dev/null 2>&1; then
  DC="docker compose"
elif command -v docker-compose &>/dev/null; then
  DC="docker-compose"
else
  echo -e "${RED}  ✗ docker compose not found.${NC}"
  echo -e "  Install: sudo apt-get install docker-compose-plugin"
  exit 1
fi
echo -e "  ${GREEN}✓${NC} $($DC version --short 2>/dev/null || echo 'compose')"
echo ""

# ── 2. Set up .env ───────────────────────────────────────────
echo -e "${BOLD}[2/6] Environment setup...${NC}"
if [ ! -f .env ]; then
  cp .env.example .env
  echo -e "  ${YELLOW}⚠  Created .env — you MUST configure it before first use.${NC}"
  echo ""
  echo -e "${BOLD}  Required values in .env:${NC}"
  echo -e "  ${YELLOW}  POSTGRES_PASSWORD${NC}   = any strong password (e.g. MyStr0ngPw!)"
  echo -e "  ${YELLOW}  REDIS_PASSWORD${NC}      = any strong password"
  echo -e "  Telegram (optional but recommended):"
  echo -e "  ${YELLOW}  TELEGRAM_BOT_TOKEN${NC}  = from @BotFather on Telegram"
  echo -e "  ${YELLOW}  TELEGRAM_CHAT_ID${NC}    = your numeric chat ID"
  echo ""
  read -rp "  Open .env in editor now? [Y/n] " yn
  yn="${yn:-Y}"
  if [[ "$yn" =~ ^[Yy] ]]; then
    "${EDITOR:-nano}" .env
  else
    echo -e "  ${YELLOW}Remember to edit .env before running again.${NC}"
    exit 0
  fi
fi

# Source .env safely
set -a
# shellcheck disable=SC1091
source .env 2>/dev/null || true
set +a

# Validate required secrets
MISSING=0
if [ -z "${POSTGRES_PASSWORD}" ] || [ "${POSTGRES_PASSWORD}" = "your_strong_password_here" ]; then
  echo -e "  ${RED}✗ POSTGRES_PASSWORD not set in .env${NC}"; MISSING=1
fi
if [ -z "${REDIS_PASSWORD}" ] || [ "${REDIS_PASSWORD}" = "your_redis_password_here" ]; then
  echo -e "  ${RED}✗ REDIS_PASSWORD not set in .env${NC}"; MISSING=1
fi
[ "$MISSING" = "1" ] && { echo -e "\n  ${RED}Fix the above in .env then re-run.${NC}"; exit 1; }
echo -e "  ${GREEN}✓${NC} .env validated"

# Set mode
if grep -q "^EXECUTION__MODE=" .env 2>/dev/null; then
  sed -i.bak "s/^EXECUTION__MODE=.*/EXECUTION__MODE=${MODE}/" .env
else
  echo "EXECUTION__MODE=${MODE}" >> .env
fi
rm -f .env.bak

# Live mode warning
if [ "$MODE" = "live" ]; then
  echo ""
  echo -e "  ${RED}${BOLD}  ██╗     ██╗██╗   ██╗███████╗${NC}"
  echo -e "  ${RED}${BOLD}  ██║     ██║██║   ██║██╔════╝${NC}"
  echo -e "  ${RED}${BOLD}  ██║     ██║██║   ██║█████╗${NC}"
  echo -e "  ${RED}${BOLD}  ██║     ██║╚██╗ ██╔╝██╔══╝${NC}"
  echo -e "  ${RED}${BOLD}  ███████╗██║ ╚████╔╝ ███████╗${NC}"
  echo -e "  ${RED}${BOLD}  ╚══════╝╚═╝  ╚═══╝  ╚══════╝  MODE${NC}"
  echo ""
  echo -e "  ${RED}Real money will be used. Losses are possible.${NC}"
  read -rp "  Type 'I UNDERSTAND' to confirm: " confirm
  [ "$confirm" != "I UNDERSTAND" ] && { echo "Aborted."; exit 0; }
fi
echo -e "  ${GREEN}✓${NC} Mode → ${MODE}"
echo ""

# ── 3. Create directories ────────────────────────────────────
echo -e "${BOLD}[3/6] Creating data directories...${NC}"
mkdir -p logs data/historical/{markets,news,sports,whales}
echo -e "  ${GREEN}✓${NC} logs/  data/historical/"
echo ""

# ── 4. Build image ───────────────────────────────────────────
echo -e "${BOLD}[4/6] Building Docker image...${NC}"
echo -e "  (This takes ~3–5 min on first run, seconds after that)"
$DC build --quiet 2>&1 | tail -3 | sed 's/^/  /'
echo -e "  ${GREEN}✓${NC} Image built"
echo ""

# ── 5. Start services ────────────────────────────────────────
echo -e "${BOLD}[5/6] Starting services...${NC}"

# Start infra first, wait for healthy
echo -e "  Starting postgres + redis..."
$DC up -d postgres redis

echo -ne "  Waiting for postgres"
for i in $(seq 1 40); do
  $DC exec -T postgres pg_isready -U "${POSTGRES_USER:-trader}" -d "${POSTGRES_DB:-polymarket}" \
    &>/dev/null && break
  echo -n "." && sleep 2
done
echo -e " ${GREEN}✓${NC}"

echo -ne "  Waiting for redis"
for i in $(seq 1 30); do
  $DC exec -T redis redis-cli -a "${REDIS_PASSWORD}" ping &>/dev/null && break
  echo -n "." && sleep 1
done
echo -e " ${GREEN}✓${NC}"

# Now start the app services
echo -e "  Starting app + api + dashboard..."
$DC up -d app api dashboard

echo -ne "  Waiting for API (port 8088)"
for i in $(seq 1 40); do
  curl -sf http://localhost:8088/health &>/dev/null && break
  echo -n "." && sleep 3
done
echo -e " ${GREEN}✓${NC}"

echo -ne "  Waiting for dashboard (port 3056)"
for i in $(seq 1 20); do
  curl -sf http://localhost:3056/ &>/dev/null && break
  echo -n "." && sleep 2
done
echo -e " ${GREEN}✓${NC}"
echo ""

# ── 6. Status check ──────────────────────────────────────────
echo -e "${BOLD}[6/6] Service status:${NC}"
$DC ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null | sed 's/^/  /'
echo ""

# ── Done ─────────────────────────────────────────────────────
echo -e "${GREEN}${BOLD}╔══════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}${BOLD}║   ✓  Polymarket Trader is running!                  ║${NC}"
echo -e "${GREEN}${BOLD}╚══════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  🖥  Control Dashboard  →  ${GREEN}http://localhost:3056${NC}"
echo -e "  🔌  API (REST + Docs)  →  ${GREEN}http://localhost:8088/docs${NC}"
echo -e "  🗄  API Health Check   →  ${GREEN}http://localhost:8088/health${NC}"
echo ""
echo -e "  ${BOLD}Useful commands:${NC}"
echo -e "  ${YELLOW}$DC logs -f app${NC}        follow trading logs"
echo -e "  ${YELLOW}$DC logs -f api${NC}        follow API logs"
echo -e "  ${YELLOW}$DC logs -f dashboard${NC}  follow dashboard logs"
echo -e "  ${YELLOW}$DC ps${NC}                 check service status"
echo -e "  ${YELLOW}./stop.sh${NC}              stop everything"
echo ""
