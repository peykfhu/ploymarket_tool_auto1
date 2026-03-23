# Polymarket Automated Trading System

A fully automated trading system for Polymarket prediction markets with real-time news signals, sports data, whale tracking, and multi-strategy execution.

## Quick Start

```bash
# 1. Setup
./scripts/setup.sh

# 2. Configure secrets
nano .env

# 3. Start (paper trading by default)
docker compose up

# 4. Open control dashboard
open http://localhost:3056

# 5. API backend
open http://localhost:8088
```

## Ports
| Service | Port | Description |
|---------|------|-------------|
| Dashboard | 3056 | React control room UI |
| API | 8088 | FastAPI JSON backend |
| Grafana | 3060 | Metrics (--profile monitoring) |
| Prometheus | 9090 | Metrics (--profile monitoring) |
| PostgreSQL | 5432 | Primary database |
| Redis | 6379 | Streams + cache |

## Modes
```bash
python main.py --mode paper      # paper trading (safe default)
python main.py --mode live       # live trading (real money!)
python main.py --mode backtest --start 2024-01-01 --end 2024-06-01
python main.py --mode collect-data
python main.py --mode telegram-only
```

## Run Tests
```bash
pytest tests/ -v
```

## Collect Historical Data (for backtesting)
```bash
python scripts/collect_history.py --start 2024-01-01 --end 2024-12-31
python scripts/run_backtest.py --start 2024-01-01 --end 2024-06-01
```
