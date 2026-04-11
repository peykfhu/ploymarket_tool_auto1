#!/usr/bin/env bash
# =============================================================
#  Polymarket Trader — ONE-CLICK GITHUB PUSH
#  Run this locally to push code to GitHub.
#  Usage: ./push.sh [commit message]
#  Example: ./push.sh "update strategy config"
# =============================================================
set -e

REPO_URL="https://github.com/peykfhu/ploymarket_tool_auto1.git"
BRANCH="main"
MSG="${1:-update $(date '+%Y-%m-%d %H:%M')}"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; BOLD='\033[1m'; NC='\033[0m'

echo -e "${BOLD}"
echo "  ╔══════════════════════════════════════════╗"
echo "  ║    POLYMARKET TRADER — GITHUB PUSH       ║"
echo "  ╚══════════════════════════════════════════╝"
echo -e "${NC}"
echo -e "  Repo  : ${GREEN}${REPO_URL}${NC}"
echo -e "  Branch: ${GREEN}${BRANCH}${NC}"
echo -e "  Commit: ${YELLOW}${MSG}${NC}"
echo ""

# ── Check git is installed ────────────────────────────────────
if ! command -v git &>/dev/null; then
  echo -e "${RED}✗ git not found. Install git first.${NC}"
  exit 1
fi

# ── Init repo if needed ───────────────────────────────────────
if [ ! -d ".git" ]; then
  echo -e "${BOLD}[1/5] Initializing git repo...${NC}"
  git init -b "$BRANCH"
  echo -e "  ${GREEN}✓${NC} git init"
else
  echo -e "${BOLD}[1/5] Git repo already initialized.${NC}"
  echo -e "  ${GREEN}✓${NC} .git exists"
fi

# ── Set remote ────────────────────────────────────────────────
echo ""
echo -e "${BOLD}[2/5] Configuring remote...${NC}"
if git remote get-url origin &>/dev/null; then
  CURRENT=$(git remote get-url origin)
  if [ "$CURRENT" != "$REPO_URL" ]; then
    echo -e "  ${YELLOW}Remote was ${CURRENT} — updating to ${REPO_URL}${NC}"
    git remote set-url origin "$REPO_URL"
  fi
  echo -e "  ${GREEN}✓${NC} remote origin → ${REPO_URL}"
else
  git remote add origin "$REPO_URL"
  echo -e "  ${GREEN}✓${NC} remote origin added"
fi

# ── Stage all files ───────────────────────────────────────────
echo ""
echo -e "${BOLD}[3/5] Staging files...${NC}"
git add -A
STAGED=$(git diff --cached --name-only | wc -l | tr -d ' ')
echo -e "  ${GREEN}✓${NC} ${STAGED} files staged"

# ── Commit ────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}[4/5] Committing...${NC}"
if git diff --cached --quiet; then
  echo -e "  ${YELLOW}⚠  Nothing to commit — working tree clean.${NC}"
else
  git commit -m "$MSG"
  echo -e "  ${GREEN}✓${NC} Committed: \"${MSG}\""
fi

# ── Push ─────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}[5/5] Pushing to GitHub...${NC}"
echo -e "  ${YELLOW}(You may be prompted for GitHub credentials)${NC}"
git push -u origin "$BRANCH" 2>&1

echo ""
echo -e "${GREEN}${BOLD}╔══════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}${BOLD}║   ✓  Push complete!                                 ║${NC}"
echo -e "${GREEN}${BOLD}╚══════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  Repo: ${GREEN}https://github.com/peykfhu/ploymarket_tool_auto1${NC}"
echo ""
echo -e "  ${BOLD}Now deploy to server with ONE command:${NC}"
echo -e "  ${YELLOW}curl -fsSL https://raw.githubusercontent.com/peykfhu/ploymarket_tool_auto1/main/server_install.sh | bash${NC}"
echo ""
