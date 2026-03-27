"""
web/api.py  —  FastAPI admin backend on port 8088.

ARCHITECTURE: This runs as a STANDALONE process with its own DB connections.
It reads data directly from PostgreSQL and Redis — no dependency on the
trading app process. This is the correct microservice pattern.

Startup: reads DB credentials from environment, connects on first request.
"""
from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Any

import structlog
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logger = structlog.get_logger(__name__)

# ── Lazy DB singletons ────────────────────────────────────────────────────────
_db_session_factory: Any = None
_redis_client: Any = None
_init_lock: asyncio.Lock | None = None


async def _get_lock() -> asyncio.Lock:
    global _init_lock
    if _init_lock is None:
        _init_lock = asyncio.Lock()
    return _init_lock


async def _ensure_db() -> Any:
    """Lazy-init DB connection. Safe to call on every request."""
    global _db_session_factory
    if _db_session_factory is not None:
        return _db_session_factory

    lock = await _get_lock()
    async with lock:
        if _db_session_factory is not None:
            return _db_session_factory
        try:
            # Build postgres URL from env vars
            host     = os.environ.get("DATABASE__POSTGRES_HOST", "postgres")
            port     = os.environ.get("DATABASE__POSTGRES_PORT", "5432")
            db       = os.environ.get("POSTGRES_DB", "polymarket")
            user     = os.environ.get("POSTGRES_USER", "trader")
            password = os.environ.get("POSTGRES_PASSWORD") or \
                       os.environ.get("DATABASE__POSTGRES_PASSWORD", "")
            url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{db}"

            from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
            from database.models import Base

            engine = create_async_engine(url, pool_size=5, max_overflow=5,
                                          pool_pre_ping=True, echo=False)
            # Create tables if missing (idempotent)
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

            _db_session_factory = async_sessionmaker(
                bind=engine, class_=AsyncSession,
                expire_on_commit=False, autocommit=False, autoflush=False,
            )
            logger.info("api_db_connected", host=host, db=db)
        except Exception as exc:
            logger.error("api_db_connect_failed", error=str(exc))
            _db_session_factory = None
    return _db_session_factory


async def _ensure_redis() -> Any:
    """Lazy-init Redis connection."""
    global _redis_client
    if _redis_client is not None:
        return _redis_client

    lock = await _get_lock()
    async with lock:
        if _redis_client is not None:
            return _redis_client
        try:
            import redis.asyncio as aioredis
            host     = os.environ.get("DATABASE__REDIS_HOST", "redis")
            port     = int(os.environ.get("DATABASE__REDIS_PORT", "6379"))
            password = os.environ.get("REDIS_PASSWORD") or \
                       os.environ.get("DATABASE__REDIS_PASSWORD", "")
            _redis_client = aioredis.Redis(
                host=host, port=port, password=password,
                decode_responses=True, socket_connect_timeout=5,
            )
            await _redis_client.ping()
            logger.info("api_redis_connected", host=host)
        except Exception as exc:
            logger.warning("api_redis_connect_failed", error=str(exc))
            _redis_client = None
    return _redis_client


# ── FastAPI app ───────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Pre-warm connections at startup
    try:
        await _ensure_db()
        await _ensure_redis()
        logger.info("api_server_ready", port=8088)
    except Exception as exc:
        logger.warning("api_startup_warning", error=str(exc))
    yield
    # Cleanup on shutdown
    if _redis_client:
        try:
            await _redis_client.aclose()
        except Exception:
            pass


app = FastAPI(title="Polymarket Trader API", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Request models ────────────────────────────────────────────────────────────

class ClosePositionRequest(BaseModel):
    reason: str = "manual"

class SetModeRequest(BaseModel):
    mode: str

class SetRiskRequest(BaseModel):
    level: str


# ── Helper: read risk state from Redis ───────────────────────────────────────

async def _get_risk_state() -> dict:
    r = await _ensure_redis()
    if not r:
        return {}
    try:
        raw = await r.get("risk:state")
        return json.loads(raw) if raw else {}
    except Exception:
        return {}


async def _get_portfolio_state() -> dict:
    r = await _ensure_redis()
    if not r:
        return {}
    try:
        raw = await r.get("state:portfolio")
        return json.loads(raw) if raw else {}
    except Exception:
        return {}


async def _get_collector_health() -> dict:
    """Read latest heartbeats from Redis stream."""
    r = await _ensure_redis()
    if not r:
        return {}
    try:
        # Read last N heartbeat messages
        msgs = await r.xrevrange("stream:heartbeats", count=20)
        latest: dict[str, dict] = {}
        for msg_id, fields in msgs:
            name = fields.get("collector", "")
            if name and name not in latest:
                latest[name] = {
                    "name": name,
                    "status": fields.get("status", "unknown"),
                    "messages_total": int(fields.get("messages_total", 0)),
                    "errors_total": int(fields.get("errors_total", 0)),
                    "last_error": fields.get("last_error", ""),
                    "latency_ms_avg": float(fields.get("latency_ms_avg", 0)),
                    "is_healthy": fields.get("is_healthy", "False") == "True",
                }
        return latest
    except Exception:
        return {}


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    db_ok = _db_session_factory is not None
    redis_ok = _redis_client is not None
    # Try connecting if not yet connected
    if not db_ok:
        await _ensure_db()
        db_ok = _db_session_factory is not None
    if not redis_ok:
        await _ensure_redis()
        redis_ok = _redis_client is not None
    return {
        "status": "ok",
        "db": "connected" if db_ok else "disconnected",
        "redis": "connected" if redis_ok else "disconnected",
        "time": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/status")
async def get_status():
    risk = await _get_risk_state()
    portfolio = await _get_portfolio_state()
    collectors = await _get_collector_health()

    # Read mode from Redis or env
    mode = portfolio.get("mode", os.environ.get("EXECUTION__MODE", "paper"))

    cb_active = bool(risk.get("circuit_breaker_active", False))
    cb_level  = risk.get("circuit_breaker_level", "none")
    cb_reason = risk.get("circuit_breaker_reason", "")
    resume_at = risk.get("circuit_breaker_resume_at")

    return {
        "mode": mode,
        "paused": False,
        "circuit_breaker": {
            "active": cb_active,
            "level": cb_level,
            "reason": cb_reason,
            "resume_at": resume_at,
        },
        "collectors": collectors,
    }


@app.get("/api/portfolio")
async def get_portfolio():
    """Read positions from DB open trades."""
    factory = await _ensure_db()

    # Default portfolio
    portfolio = {
        "total_balance": 0.0,
        "available_balance": 0.0,
        "total_position_value": 0.0,
        "exposure_pct": 0.0,
        "daily_pnl": 0.0,
        "daily_pnl_pct": 0.0,
        "total_trades_today": 0,
        "wins_today": 0,
        "losses_today": 0,
        "positions": [],
    }

    # Read balance from Redis state
    state = await _get_portfolio_state()
    risk = await _get_risk_state()
    portfolio["total_balance"]     = float(state.get("total_balance", 0))
    portfolio["available_balance"] = float(state.get("available_balance", 0))
    portfolio["total_position_value"] = float(state.get("total_position_value", 0))
    portfolio["daily_pnl"]         = float(risk.get("daily_pnl", 0))
    portfolio["daily_pnl_pct"]     = float(risk.get("daily_pnl_pct", 0))
    portfolio["total_trades_today"]= int(risk.get("daily_trades_count", 0))
    portfolio["wins_today"]        = int(risk.get("daily_wins", 0))
    portfolio["losses_today"]      = int(risk.get("daily_losses", 0))
    total = portfolio["total_balance"]
    pos_val = portfolio["total_position_value"]
    portfolio["exposure_pct"] = pos_val / total if total > 0 else 0.0

    if not factory:
        return portfolio

    try:
        from sqlalchemy import text
        mode = os.environ.get("EXECUTION__MODE", "paper")
        async with factory() as session:
            result = await session.execute(text("""
                SELECT id, market_id, market_question, outcome, side,
                       entry_price, entry_price AS current_price,
                       size_usd, 0.0 AS unrealized_pnl, 0.0 AS unrealized_pnl_pct,
                       0.0 AS take_profit, 0.0 AS stop_loss,
                       strategy_name, signal_id, opened_at
                FROM trades
                WHERE closed_at IS NULL AND mode = :mode
                ORDER BY opened_at DESC
                LIMIT 50
            """), {"mode": mode})
            rows = result.fetchall()

        portfolio["positions"] = [
            {
                "position_id": str(r[0]),
                "market_id": r[1],
                "market_question": (r[2] or "")[:100],
                "outcome": r[3],
                "side": r[4],
                "entry_price": float(r[5] or 0),
                "current_price": float(r[6] or 0),
                "size_usd": float(r[7] or 0),
                "unrealized_pnl": float(r[8] or 0),
                "unrealized_pnl_pct": float(r[9] or 0),
                "take_profit": float(r[10] or 0),
                "stop_loss": float(r[11] or 0),
                "strategy_name": r[12] or "",
                "signal_id": r[13] or "",
                "opened_at": r[14].isoformat() if r[14] else "",
            }
            for r in rows
        ]
        # Recalculate position value from DB
        pos_val = sum(p["size_usd"] for p in portfolio["positions"])
        portfolio["total_position_value"] = pos_val
    except Exception as exc:
        logger.warning("portfolio_db_error", error=str(exc))

    return portfolio


@app.get("/api/trades")
async def get_trades():
    factory = await _ensure_db()
    if not factory:
        return []
    try:
        from sqlalchemy import text
        mode = os.environ.get("EXECUTION__MODE", "paper")
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        async with factory() as session:
            result = await session.execute(text("""
                SELECT id, market_question, strategy_name, outcome,
                       entry_price, exit_price, size_usd,
                       realized_pnl, realized_pnl_pct,
                       close_reason, opened_at, closed_at
                FROM trades
                WHERE mode = :mode AND opened_at >= :today
                ORDER BY opened_at DESC
                LIMIT 100
            """), {"mode": mode, "today": today})
            rows = result.fetchall()
        return [
            {
                "trade_id": str(r[0]),
                "market_question": (r[1] or "")[:80],
                "strategy_name": r[2] or "",
                "outcome": r[3] or "",
                "entry_price": float(r[4] or 0),
                "exit_price": float(r[5]) if r[5] else None,
                "size_usd": float(r[6] or 0),
                "realized_pnl": float(r[7]) if r[7] is not None else None,
                "realized_pnl_pct": float(r[8]) if r[8] is not None else None,
                "close_reason": r[9],
                "opened_at": r[10].isoformat() if r[10] else None,
                "closed_at": r[11].isoformat() if r[11] else None,
            }
            for r in rows
        ]
    except Exception as exc:
        logger.warning("trades_db_error", error=str(exc))
        return []


@app.get("/api/performance")
async def get_performance():
    factory = await _ensure_db()
    if not factory:
        return {"total_pnl": 0, "win_rate": 0, "total_trades": 0}
    try:
        from sqlalchemy import text
        mode = os.environ.get("EXECUTION__MODE", "paper")
        start = datetime.now(timezone.utc) - timedelta(days=30)
        async with factory() as session:
            result = await session.execute(text("""
                SELECT
                    COUNT(*) AS total,
                    COALESCE(SUM(realized_pnl), 0) AS total_pnl,
                    COUNT(*) FILTER (WHERE realized_pnl >= 0) AS wins
                FROM trades
                WHERE mode = :mode
                  AND closed_at IS NOT NULL
                  AND closed_at >= :start
            """), {"mode": mode, "start": start})
            row = result.fetchone()
        total = int(row[0]) if row else 0
        total_pnl = float(row[1]) if row else 0.0
        wins = int(row[2]) if row else 0
        return {
            "total_trades": total,
            "total_pnl": round(total_pnl, 2),
            "total_pnl_pct": 0.0,
            "wins": wins,
            "losses": total - wins,
            "win_rate": wins / total if total > 0 else 0.0,
        }
    except Exception as exc:
        logger.warning("performance_db_error", error=str(exc))
        return {"total_pnl": 0, "win_rate": 0, "total_trades": 0}


@app.get("/api/whales")
async def get_whales(limit: int = 20):
    factory = await _ensure_db()
    if not factory:
        return []
    try:
        from sqlalchemy import text
        async with factory() as session:
            result = await session.execute(text("""
                SELECT wt.whale_address, wp.name, wp.tier, wp.win_rate,
                       wt.action, wt.market_question, wt.amount_usd,
                       wt.price, wt.timestamp
                FROM whale_trades wt
                LEFT JOIN whale_profiles wp ON wp.address = wt.whale_address
                ORDER BY wt.timestamp DESC
                LIMIT :limit
            """), {"limit": limit})
            rows = result.fetchall()
        return [
            {
                "whale_address": r[0] or "",
                "whale_name": r[1] or f"whale_{(r[0] or '')[:6]}",
                "whale_tier": r[2] or "B",
                "whale_win_rate": float(r[3] or 0),
                "action": r[4] or "buy",
                "market_question": (r[5] or "")[:80],
                "amount_usd": float(r[6] or 0),
                "price": float(r[7] or 0),
                "timestamp": r[8].isoformat() if r[8] else datetime.now(timezone.utc).isoformat(),
            }
            for r in rows
        ]
    except Exception as exc:
        logger.warning("whales_db_error", error=str(exc))
        return []


# ── Control endpoints (write to Redis flags) ──────────────────────────────────

async def _redis_set(key: str, value: str) -> bool:
    r = await _ensure_redis()
    if not r:
        raise HTTPException(503, "Redis not available")
    await r.set(key, value)
    return True


@app.post("/api/pause")
async def pause():
    await _redis_set("control:paused", "1")
    return {"status": "paused"}


@app.post("/api/resume")
async def resume():
    await _redis_set("control:paused", "0")
    # Also clear circuit breaker if set
    r = await _ensure_redis()
    if r:
        raw = await r.get("risk:state")
        if raw:
            state = json.loads(raw)
            state["circuit_breaker_active"] = False
            state["circuit_breaker_level"] = "none"
            await r.set("risk:state", json.dumps(state))
    return {"status": "resumed"}


@app.post("/api/positions/{position_id}/close")
async def close_position(position_id: str, req: ClosePositionRequest):
    # Write a close command to Redis — the trading app reads and executes it
    r = await _ensure_redis()
    if not r:
        raise HTTPException(503, "Redis not available")
    cmd = json.dumps({"action": "close_position", "position_id": position_id, "reason": req.reason})
    await r.lpush("control:commands", cmd)
    return {"status": "queued", "position_id": position_id}


@app.post("/api/positions/close-all")
async def close_all():
    r = await _ensure_redis()
    if not r:
        raise HTTPException(503, "Redis not available")
    cmd = json.dumps({"action": "close_all", "reason": "web_dashboard"})
    await r.lpush("control:commands", cmd)
    return {"status": "queued"}


@app.post("/api/mode")
async def set_mode(req: SetModeRequest):
    if req.mode not in ("live", "paper"):
        raise HTTPException(400, "mode must be live or paper")
    await _redis_set("control:mode", req.mode)
    return {"mode": req.mode}


@app.post("/api/risk")
async def set_risk(req: SetRiskRequest):
    levels = {
        "conservative": {"max_single_position_pct": 0.04, "max_daily_loss_pct": 0.03},
        "moderate":     {"max_single_position_pct": 0.08, "max_daily_loss_pct": 0.05},
        "aggressive":   {"max_single_position_pct": 0.12, "max_daily_loss_pct": 0.08},
    }
    if req.level not in levels:
        raise HTTPException(400, f"level must be one of {list(levels)}")
    await _redis_set("control:risk_level", req.level)
    return {"level": req.level}


# ── WebSocket live portfolio push ─────────────────────────────────────────────

class _WSManager:
    def __init__(self):
        self.active: list[WebSocket] = []
    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)
    def disconnect(self, ws: WebSocket):
        self.active = [w for w in self.active if w is not ws]
    async def broadcast(self, data: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

_ws_manager = _WSManager()


@app.websocket("/ws/portfolio")
async def ws_portfolio(websocket: WebSocket):
    await _ws_manager.connect(websocket)
    try:
        while True:
            try:
                state = await _get_portfolio_state()
                risk = await _get_risk_state()
                balance = float(state.get("total_balance", 0))
                await websocket.send_json({
                    "type": "portfolio",
                    "balance": balance,
                    "daily_pnl": float(risk.get("daily_pnl", 0)),
                    "daily_pnl_pct": float(risk.get("daily_pnl_pct", 0)),
                    "positions": 0,
                    "exposure_pct": 0.0,
                })
            except Exception:
                pass
            await asyncio.sleep(5)
    except WebSocketDisconnect:
        _ws_manager.disconnect(websocket)


async def start_api(host: str = "0.0.0.0", port: int = 8088) -> None:
    import uvicorn
    config = uvicorn.Config(app, host=host, port=port, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()
