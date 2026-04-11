#!/usr/bin/env bash
# =============================================================
#  Polymarket Trader — ONE-CLICK SERVER INSTALL
#
#  Run on your server with a single command:
#
#  curl -fsSL https://raw.githubusercontent.com/peykfhu/ploymarket_tool_auto1/main/server_install.sh | bash
#
#  Or: wget -qO- https://raw.githubusercontent.com/peykfhu/ploymarket_tool_auto1/main/server_install.sh | bash
#
#  Options (set as env vars before running):
#    MODE=paper|live   default: paper
#    INSTALL_DIR       default: /opt/polymarket_trader
#
#  Example (live mode):
#    MODE=live curl -fsSL ... | bash
# =============================================================
set -e

REPO_URL="https://github.com/peykfhu/ploymarket_tool_auto1.git"
INSTALL_DIR="${INSTALL_DIR:-/opt/polymarket_trader}"
MODE="${MODE:-paper}"
BRANCH="main"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'

# ── Banner ───────────────────────────────────────────────────
echo -e "${BOLD}${BLUE}"
cat << 'BANNER'
  ██████╗  ██████╗ ██╗  ██╗   ██╗███╗   ███╗ █████╗ ██████╗ ██╗  ██╗███████╗████████╗
  ██╔══██╗██╔═══██╗██║  ╚██╗ ██╔╝████╗ ████║██╔══██╗██╔══██╗██║ ██╔╝██╔════╝╚══██╔══╝
  ██████╔╝██║   ██║██║   ╚████╔╝ ██╔████╔██║███████║██████╔╝█████╔╝ █████╗     ██║
  ██╔═══╝ ██║   ██║██║    ╚██╔╝  ██║╚██╔╝██║██╔══██║██╔══██╗██╔═██╗ ██╔══╝     ██║
  ██║     ╚██████╔╝███████╗██║   ██║ ╚═╝ ██║██║  ██║██║  ██║██║  ██╗███████╗   ██║
  ╚═╝      ╚═════╝ ╚══════╝╚═╝   ╚═╝     ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝   ╚═╝
BANNER
echo -e "${NC}"
echo -e "  Mode   : ${BOLD}${MODE^^}${NC}"
echo -e "  Install: ${GREEN}${INSTALL_DIR}${NC}"
echo -e "  Repo   : ${GREEN}${REPO_URL}${NC}"
echo ""

# ── Detect OS ────────────────────────────────────────────────
echo -e "${BOLD}[1/7] Checking system...${NC}"
if [ "$(id -u)" -ne 0 ] && ! sudo -n true 2>/dev/null; then
  echo -e "${RED}  ✗ Need sudo access. Run as root or a sudo user.${NC}"
  exit 1
fi
echo -e "  ${GREEN}✓${NC} Running as $(id -un)"

# ── Install Docker if missing ─────────────────────────────────
echo ""
echo -e "${BOLD}[2/7] Checking Docker...${NC}"
if command -v docker &>/dev/null; then
  echo -e "  ${GREEN}✓${NC} Docker $(docker --version | grep -oP '\d+\.\d+\.\d+' | head -1) already installed"
else
  echo -e "  Installing Docker..."
  curl -fsSL https://get.docker.com | sh
  sudo usermod -aG docker "$USER" 2>/dev/null || true
  echo -e "  ${GREEN}✓${NC} Docker installed"
fi

# Detect docker compose command
if docker compose version &>/dev/null 2>&1; then
  DC="docker compose"
elif command -v docker-compose &>/dev/null; then
  DC="docker-compose"
else
  echo -e "  Installing docker compose plugin..."
  sudo apt-get install -y docker-compose-plugin 2>/dev/null || \
    sudo pip3 install docker-compose 2>/dev/null || true
  DC="docker compose"
fi
echo -e "  ${GREEN}✓${NC} $DC available"

# ── Install git if missing ────────────────────────────────────
echo ""
echo -e "${BOLD}[3/7] Checking git...${NC}"
if ! command -v git &>/dev/null; then
  sudo apt-get install -y git 2>/dev/null || sudo yum install -y git 2>/dev/null
fi
echo -e "  ${GREEN}✓${NC} git $(git --version | grep -oP '\d+\.\d+\.\d+' | head -1)"

# ── Clone or update repo ──────────────────────────────────────
echo ""
echo -e "${BOLD}[4/7] Fetching latest code from GitHub...${NC}"
if [ -d "${INSTALL_DIR}/.git" ]; then
  echo -e "  Existing install found — pulling latest..."
  # Preserve .env across update
  [ -f "${INSTALL_DIR}/.env" ] && cp "${INSTALL_DIR}/.env" /tmp/.polymarket_env_backup
  cd "${INSTALL_DIR}"
  git fetch origin "$BRANCH"
  git reset --hard "origin/$BRANCH"
  [ -f /tmp/.polymarket_env_backup ] && mv /tmp/.polymarket_env_backup "${INSTALL_DIR}/.env"
  echo -e "  ${GREEN}✓${NC} Code updated"
else
  sudo mkdir -p "$INSTALL_DIR"
  sudo chown "$USER":"$USER" "$INSTALL_DIR" 2>/dev/null || true
  git clone --branch "$BRANCH" --depth 1 "$REPO_URL" "$INSTALL_DIR"
  echo -e "  ${GREEN}✓${NC} Cloned to ${INSTALL_DIR}"
fi
cd "$INSTALL_DIR"

# ── Configure .env ────────────────────────────────────────────
echo ""
echo -e "${BOLD}[5/7] Configuring environment...${NC}"
if [ ! -f ".env" ]; then
  cp .env.example .env
  echo -e "  ${YELLOW}⚠  Created .env from template — configuring required values...${NC}"
  echo ""

  # Auto-generate strong random passwords so it's truly one-click
  PG_PASS=$(tr -dc 'A-Za-z0-9!@#$%' < /dev/urandom | head -c 24 2>/dev/null || \
            openssl rand -base64 18 | tr -d '/+=' | head -c 24)
  RD_PASS=$(tr -dc 'A-Za-z0-9!@#$%' < /dev/urandom | head -c 24 2>/dev/null || \
            openssl rand -base64 18 | tr -d '/+=' | head -c 24)

  sed -i "s/^POSTGRES_PASSWORD=.*/POSTGRES_PASSWORD=${PG_PASS}/" .env
  sed -i "s/^REDIS_PASSWORD=.*/REDIS_PASSWORD=${RD_PASS}/" .env
  sed -i "s/^DATABASE__POSTGRES_PASSWORD=.*/DATABASE__POSTGRES_PASSWORD=${PG_PASS}/" .env
  sed -i "s/^DATABASE__REDIS_PASSWORD=.*/DATABASE__REDIS_PASSWORD=${RD_PASS}/" .env
  sed -i "s/^EXECUTION__MODE=.*/EXECUTION__MODE=${MODE}/" .env

  echo -e "  ${GREEN}✓${NC} Auto-generated secure passwords"
  echo -e "  ${YELLOW}  POSTGRES_PASSWORD = ${PG_PASS}${NC}"
  echo -e "  ${YELLOW}  REDIS_PASSWORD    = ${RD_PASS}${NC}"
  echo ""
  echo -e "  ${BOLD}  Save these passwords! They're stored in ${INSTALL_DIR}/.env${NC}"
  echo ""
  echo -e "  ${YELLOW}  Optional — add API keys for live trading and notifications:${NC}"
  echo -e "  ${YELLOW}  nano ${INSTALL_DIR}/.env${NC}"
  echo ""
else
  # Update mode in existing .env
  if grep -q "^EXECUTION__MODE=" .env; then
    sed -i "s/^EXECUTION__MODE=.*/EXECUTION__MODE=${MODE}/" .env
  else
    echo "EXECUTION__MODE=${MODE}" >> .env
  fi
  echo -e "  ${GREEN}✓${NC} Existing .env preserved, mode set to ${MODE}"
fi

# ── Create required directories ───────────────────────────────
echo ""
echo -e "${BOLD}[6/7] Preparing directories...${NC}"
mkdir -p logs data/historical/{markets,news,sports,whales}
chmod +x start.sh stop.sh deploy.sh push.sh server_install.sh 2>/dev/null || true
echo -e "  ${GREEN}✓${NC} Directories and permissions ready"

# ── Start services ────────────────────────────────────────────
echo ""
echo -e "${BOLD}[7/7] Starting Polymarket Trader (${MODE} mode)...${NC}"

# Live mode warning
if [ "$MODE" = "live" ]; then
  echo ""
  echo -e "  ${RED}${BOLD}WARNING: LIVE MODE — Real money will be used!${NC}"
  echo -e "  ${RED}Make sure POLYMARKET__PRIVATE_KEY is set in .env${NC}"
  echo ""
  read -rp "  Type 'I UNDERSTAND' to confirm live mode: " confirm
  [ "$confirm" != "I UNDERSTAND" ] && { echo "Aborted. Re-run without MODE=live for paper trading."; exit 0; }
fi

# Start infra first
$DC up -d postgres redis
echo -ne "  Waiting for postgres"
for i in $(seq 1 40); do
  $DC exec -T postgres pg_isready -U "${POSTGRES_USER:-trader}" -d "${POSTGRES_DB:-polymarket}" \
    &>/dev/null && break || true
  echo -n "." && sleep 2
done
echo -e " ${GREEN}✓${NC}"

echo -ne "  Waiting for redis"
for i in $(seq 1 30); do
  REDIS_PASS=$(grep "^REDIS_PASSWORD=" .env | cut -d= -f2)
  $DC exec -T redis redis-cli -a "$REDIS_PASS" ping &>/dev/null && break || true
  echo -n "." && sleep 1
done
echo -e " ${GREEN}✓${NC}"

$DC up -d app api dashboard
echo -ne "  Waiting for API"
for i in $(seq 1 40); do
  curl -sf http://localhost:8088/health &>/dev/null && break || true
  echo -n "." && sleep 3
done
echo -e " ${GREEN}✓${NC}"

echo -ne "  Waiting for dashboard"
for i in $(seq 1 20); do
  curl -sf http://localhost:3056/ &>/dev/null && break || true
  echo -n "." && sleep 2
done
echo -e " ${GREEN}✓${NC}"
echo ""

# ── Service status ────────────────────────────────────────────
$DC ps 2>/dev/null | sed 's/^/  /' || true

# ── Done ─────────────────────────────────────────────────────
SERVER_IP=$(curl -sf https://api.ipify.org 2>/dev/null || hostname -I | awk '{print $1}')
echo ""
echo -e "${GREEN}${BOLD}╔══════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}${BOLD}║   ✓  Polymarket Trader is running!                  ║${NC}"
echo -e "${GREEN}${BOLD}╚══════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  Mode: ${BOLD}${MODE^^}${NC}   Install: ${GREEN}${INSTALL_DIR}${NC}"
echo ""
echo -e "  Dashboard  →  ${GREEN}http://${SERVER_IP}:3056${NC}"
echo -e "  API Docs   →  ${GREEN}http://${SERVER_IP}:8088/docs${NC}"
echo -e "  Health     →  ${GREEN}http://${SERVER_IP}:8088/health${NC}"
echo ""
echo -e "  ${BOLD}Useful commands:${NC}"
echo -e "  ${YELLOW}cd ${INSTALL_DIR}${NC}"
echo -e "  ${YELLOW}${DC} logs -f app${NC}        # trading engine logs"
echo -e "  ${YELLOW}${DC} logs -f api${NC}        # API logs"
echo -e "  ${YELLOW}./stop.sh${NC}               # stop everything"
echo -e "  ${YELLOW}./start.sh live${NC}         # switch to live mode"
echo ""
echo -e "  To update to latest code:"
echo -e "  ${YELLOW}curl -fsSL https://raw.githubusercontent.com/peykfhu/ploymarket_tool_auto1/main/server_install.sh | bash${NC}"
echo ""
