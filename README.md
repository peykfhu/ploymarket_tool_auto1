# Polymarket Automated Trading System

A fully automated trading system for Polymarket prediction markets.

## Quick Start (3 steps)

### Step 1 — Prerequisites
Make sure Docker is installed:
```bash
# Linux (Ubuntu/Debian)
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
# Log out and back in, then verify:
docker --version
```

### Step 2 — Configure
```bash
cp .env.example .env
nano .env   # Set POSTGRES_PASSWORD and REDIS_PASSWORD at minimum
```

### Step 3 — Start
```bash
chmod +x start.sh stop.sh deploy.sh
./start.sh           # paper trading (safe, no real money)
./start.sh live      # live trading (real money — use with caution!)
```

## Access
| URL | Description |
|-----|-------------|
| http://localhost:3056 | Control dashboard |
| http://localhost:8088/docs | API documentation |
| http://localhost:8088/health | Health check |

## Service Architecture
```
┌─────────────────────────────────────────────────────────────┐
│  Dashboard  :3056   →   API Backend  :8088                  │
│       ↕                      ↕                              │
│  Trading App  ←→  Redis :6379  ←→  PostgreSQL :5432        │
└─────────────────────────────────────────────────────────────┘
```

## Common Commands
```bash
# View logs
docker compose logs -f app        # trading engine logs
docker compose logs -f api        # API server logs
docker compose logs -f dashboard  # dashboard logs

# Check status
docker compose ps

# Stop everything
./stop.sh

# Restart a single service
docker compose restart app

# Run backtest
docker compose exec app python main.py --mode backtest --start 2024-01-01 --end 2024-06-01

# Collect historical data
docker compose exec app python scripts/collect_history.py --start 2024-01-01 --end 2024-12-31

# Run tests
docker compose exec app pytest tests/ -v
```

## Remote Deployment
```bash
# First deploy to server
./deploy.sh ubuntu@your-server-ip

# Update existing deploy
./deploy.sh ubuntu@your-server-ip --update

# Rollback to previous version
./deploy.sh ubuntu@your-server-ip --rollback
```

## Troubleshooting

**Services keep restarting:**
```bash
docker compose logs app    # check error message
```

**Port already in use:**
```bash
# Change ports in docker-compose.yml:
# "3056:3056" → "3057:3056" etc.
```

**Database connection refused:**
```bash
# Ensure POSTGRES_PASSWORD in .env matches DATABASE__POSTGRES_PASSWORD
docker compose restart app
```

**Permission denied on start.sh:**
```bash
chmod +x start.sh stop.sh deploy.sh
```
