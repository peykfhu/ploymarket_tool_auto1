#!/usr/bin/env bash
# ============================================================
#  Polymarket Trader — ONE-CLICK DEPLOYMENT
#  Deploys to a remote Linux server over SSH.
#
#  Usage:
#    ./deploy.sh user@your-server.com
#    ./deploy.sh user@your-server.com --update   (re-deploy existing install)
#    ./deploy.sh user@your-server.com --rollback (revert to previous build)
# ============================================================
set -e

TARGET=$1
UPDATE=${2:-""}
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; BOLD='\033[1m'; NC='\033[0m'

if [ -z "$TARGET" ]; then
  echo -e "${RED}Usage: ./deploy.sh user@host [--update|--rollback]${NC}"
  echo ""
  echo "  Examples:"
  echo "    ./deploy.sh ubuntu@1.2.3.4"
  echo "    ./deploy.sh ubuntu@1.2.3.4 --update"
  echo "    ./deploy.sh ubuntu@1.2.3.4 --rollback"
  exit 1
fi

REMOTE_DIR="/opt/polymarket_trader"
BACKUP_DIR="/opt/polymarket_trader_backup"
ARCHIVE="polymarket_trader_deploy_$(date +%Y%m%d_%H%M%S).tar.gz"

echo -e "${BOLD}"
echo "  ╔══════════════════════════════════════════╗"
echo "  ║    POLYMARKET TRADER — DEPLOY SCRIPT     ║"
echo "  ╚══════════════════════════════════════════╝"
echo -e "${NC}"
echo -e "  Target: ${GREEN}${TARGET}${NC}"
echo -e "  Remote: ${GREEN}${REMOTE_DIR}${NC}"
echo ""

# ── Step 1: Pre-flight ────────────────────────────────────────────────────────
echo -e "${BOLD}[1/7] Pre-flight checks...${NC}"
command -v ssh &>/dev/null  && echo -e "  ${GREEN}✓${NC} ssh"
command -v scp &>/dev/null  && echo -e "  ${GREEN}✓${NC} scp"
command -v tar &>/dev/null  && echo -e "  ${GREEN}✓${NC} tar"

echo -ne "  Testing SSH connection..."
ssh -o ConnectTimeout=10 -o BatchMode=yes "$TARGET" "echo ok" &>/dev/null \
  && echo -e " ${GREEN}✓${NC}" \
  || { echo -e " ${RED}✗ Cannot connect. Check SSH key/host.${NC}"; exit 1; }

# ── Step 2: Rollback path ─────────────────────────────────────────────────────
if [ "$UPDATE" = "--rollback" ]; then
  echo -e "${BOLD}[ROLLBACK] Restoring previous version...${NC}"
  ssh "$TARGET" bash << 'RSSH'
set -e
if [ ! -d /opt/polymarket_trader_backup ]; then
  echo "No backup found to rollback to."; exit 1
fi
cd /opt/polymarket_trader
docker compose down 2>/dev/null || true
cd /opt
rm -rf polymarket_trader
mv polymarket_trader_backup polymarket_trader
cd polymarket_trader
docker compose up -d --build
echo "✓ Rollback complete."
RSSH
  echo -e "${GREEN}✓ Rollback done.${NC}"
  exit 0
fi

# ── Step 3: Build archive ─────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}[2/7] Building deployment archive...${NC}"
TMPDIR=$(mktemp -d)
tar -czf "$TMPDIR/$ARCHIVE" \
  --exclude='.git' \
  --exclude='__pycache__' \
  --exclude='*.pyc' \
  --exclude='*.pyo' \
  --exclude='.env' \
  --exclude='logs/*' \
  --exclude='data/historical/*' \
  --exclude='node_modules' \
  . 2>/dev/null
SIZE=$(du -sh "$TMPDIR/$ARCHIVE" | cut -f1)
echo -e "  ${GREEN}✓${NC} Archive: ${ARCHIVE} (${SIZE})"

# ── Step 4: Check remote for existing install ─────────────────────────────────
echo ""
echo -e "${BOLD}[3/7] Checking remote server...${NC}"
REMOTE_HAS_INSTALL=$(ssh "$TARGET" "[ -d ${REMOTE_DIR} ] && echo yes || echo no")

if [ "$REMOTE_HAS_INSTALL" = "yes" ] && [ "$UPDATE" != "--update" ]; then
  echo -e "${YELLOW}  ⚠  ${REMOTE_DIR} already exists on the server.${NC}"
  echo -e "${YELLOW}  Use ${BOLD}--update${YELLOW} to redeploy, or ${BOLD}--rollback${YELLOW} to revert.${NC}"
  read -p "  Continue anyway? [y/N] " yn
  [ "$yn" != "y" ] && { rm -rf "$TMPDIR"; exit 0; }
fi

# ── Step 5: Install Docker on remote if needed ────────────────────────────────
echo ""
echo -e "${BOLD}[4/7] Ensuring Docker is installed on remote...${NC}"
ssh "$TARGET" bash << 'RSSH'
if command -v docker &>/dev/null; then
  echo "  ✓ docker $(docker --version | cut -d' ' -f3 | tr -d ',')"
else
  echo "  Installing Docker..."
  curl -fsSL https://get.docker.com | sh
  sudo usermod -aG docker $USER
  echo "  ✓ Docker installed."
fi
if docker compose version &>/dev/null 2>&1; then
  echo "  ✓ docker compose plugin"
elif command -v docker-compose &>/dev/null; then
  echo "  ✓ docker-compose"
else
  echo "  Installing docker compose plugin..."
  sudo apt-get install -y docker-compose-plugin 2>/dev/null || \
    sudo pip3 install docker-compose
fi
RSSH

# ── Step 6: Upload & extract ──────────────────────────────────────────────────
echo ""
echo -e "${BOLD}[5/7] Uploading files...${NC}"
scp "$TMPDIR/$ARCHIVE" "$TARGET:/tmp/$ARCHIVE"
echo -e "  ${GREEN}✓${NC} Uploaded"

ssh "$TARGET" bash << RSSH
set -e

# Backup existing install
if [ -d ${REMOTE_DIR} ]; then
  echo "  Backing up existing install..."
  rm -rf ${BACKUP_DIR}
  cp -r ${REMOTE_DIR} ${BACKUP_DIR}
  # Stop existing services gracefully
  cd ${REMOTE_DIR}
  docker compose down 2>/dev/null || true
fi

# Extract new version
mkdir -p ${REMOTE_DIR}
cd ${REMOTE_DIR}
tar -xzf /tmp/${ARCHIVE} --strip-components=0
rm /tmp/${ARCHIVE}
echo "  ✓ Extracted to ${REMOTE_DIR}"

# Preserve .env if it exists in backup
if [ -f ${BACKUP_DIR}/.env ] && [ ! -f ${REMOTE_DIR}/.env ]; then
  cp ${BACKUP_DIR}/.env ${REMOTE_DIR}/.env
  echo "  ✓ Preserved existing .env"
elif [ ! -f ${REMOTE_DIR}/.env ]; then
  cp ${REMOTE_DIR}/.env.example ${REMOTE_DIR}/.env
  echo "  ⚠  Created .env from example — fill in secrets!"
fi

# Preserve data directory
if [ -d ${BACKUP_DIR}/data ] && [ ! -d ${REMOTE_DIR}/data ]; then
  cp -r ${BACKUP_DIR}/data ${REMOTE_DIR}/data
  echo "  ✓ Preserved data directory"
fi

chmod +x ${REMOTE_DIR}/start.sh ${REMOTE_DIR}/stop.sh ${REMOTE_DIR}/deploy.sh 2>/dev/null || true
mkdir -p ${REMOTE_DIR}/logs ${REMOTE_DIR}/data/historical/{markets,news,sports,whales}
echo "  ✓ Directories ready"
RSSH

rm -rf "$TMPDIR"
echo -e "  ${GREEN}✓${NC} Files in place"

# ── Step 7: Check .env and start ─────────────────────────────────────────────
echo ""
echo -e "${BOLD}[6/7] Checking remote .env...${NC}"
ENV_NEEDS_CONFIG=$(ssh "$TARGET" "
  source ${REMOTE_DIR}/.env 2>/dev/null || true
  if [ -z \"\$DATABASE__POSTGRES_PASSWORD\" ] || [ \"\$DATABASE__POSTGRES_PASSWORD\" = 'your_strong_password_here' ]; then
    echo needs_config
  else
    echo ok
  fi
")

if [ "$ENV_NEEDS_CONFIG" = "needs_config" ]; then
  echo -e "${YELLOW}  ⚠  .env on server needs to be configured.${NC}"
  echo ""
  echo -e "  SSH to the server and edit the file:"
  echo -e "  ${YELLOW}ssh ${TARGET}${NC}"
  echo -e "  ${YELLOW}nano ${REMOTE_DIR}/.env${NC}"
  echo ""
  echo -e "  Then start with:"
  echo -e "  ${YELLOW}ssh ${TARGET} 'cd ${REMOTE_DIR} && ./start.sh'${NC}"
  echo ""
else
  echo -e "  ${GREEN}✓${NC} .env configured"
  echo ""
  echo -e "${BOLD}[7/7] Starting services on remote...${NC}"
  ssh "$TARGET" "cd ${REMOTE_DIR} && ./start.sh paper"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
HOST=$(echo "$TARGET" | cut -d@ -f2)
echo ""
echo -e "${GREEN}${BOLD}═══════════════════════════════════════════════════${NC}"
echo -e "${GREEN}${BOLD}  ✓  Deployment complete!${NC}"
echo -e "${GREEN}${BOLD}═══════════════════════════════════════════════════${NC}"
echo ""
echo -e "  🖥  Dashboard  →  ${GREEN}http://${HOST}:3056${NC}"
echo -e "  🔌  API Docs   →  ${GREEN}http://${HOST}:8088/docs${NC}"
echo ""
echo -e "  Remote management:"
echo -e "  ${YELLOW}ssh ${TARGET} 'cd ${REMOTE_DIR} && docker compose logs -f'${NC}"
echo -e "  ${YELLOW}ssh ${TARGET} 'cd ${REMOTE_DIR} && ./stop.sh'${NC}"
echo -e "  ${YELLOW}./deploy.sh ${TARGET} --update${NC}    (redeploy)"
echo -e "  ${YELLOW}./deploy.sh ${TARGET} --rollback${NC}  (revert)"
echo ""
