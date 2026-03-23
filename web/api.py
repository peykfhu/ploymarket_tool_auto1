"""
web/api.py  —  FastAPI admin backend on port 8088.
Serves JSON endpoints consumed by the React dashboard on port 3056.
"""
from __future__ import annotations
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Any
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import structlog

logger = structlog.get_logger(__name__)
app = FastAPI(title="Polymarket Trader API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Injected at startup
_system: Any = None


def set_system(system: Any) -> None:
    global _system
    _system = system


def _sys():
    if _system is None:
        raise HTTPException(503, "System not ready")
    return _system


# ── Models ────────────────────────────────────────────────────────────────────

class ClosePositionRequest(BaseModel):
    reason: str = "manual"

class SetModeRequest(BaseModel):
    mode: str

class SetRiskRequest(BaseModel):
    level: str  # conservative / moderate / aggressive


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}

@app.get("/api/status")
async def get_status():
    s = _sys()
    cb = await s.get_circuit_breaker_status()
    health = s.get_collector_health()
    return {
        "mode": s.mode,
        "paused": s._paused,
        "circuit_breaker": {
            "active": cb.active, "level": cb.level,
            "reason": cb.reason,
            "resume_at": cb.resume_at.isoformat() if cb.resume_at else None,
        },
        "collectors": health,
    }

@app.get("/api/portfolio")
async def get_portfolio():
    s = _sys()
    p = await s.get_portfolio()
    return {
        "total_balance": p.total_balance,
        "available_balance": p.available_balance,
        "total_position_value": p.total_position_value,
        "exposure_pct": p.total_exposure_pct,
        "daily_pnl": p.daily_pnl,
        "daily_pnl_pct": p.daily_pnl_pct,
        "total_trades_today": p.total_trades_today,
        "wins_today": p.wins_today,
        "losses_today": p.losses_today,
        "positions": [
            {
                "position_id": pos.position_id,
                "market_id": pos.market_id,
                "market_question": pos.market_question[:100],
                "outcome": pos.outcome,
                "side": pos.side,
                "entry_price": pos.entry_price,
                "current_price": pos.current_price,
                "size_usd": pos.size_usd,
                "unrealized_pnl": pos.unrealized_pnl,
                "unrealized_pnl_pct": pos.unrealized_pnl_pct,
                "take_profit": pos.take_profit,
                "stop_loss": pos.stop_loss,
                "strategy_name": pos.strategy_name,
                "opened_at": pos.opened_at.isoformat(),
            }
            for pos in p.positions
        ],
    }

@app.get("/api/trades")
async def get_trades(days: int = 1):
    s = _sys()
    trades = await s.get_today_trades()
    return [
        {
            "trade_id": getattr(t, "trade_id", str(t.id) if hasattr(t, "id") else ""),
            "market_question": t.market_question[:80],
            "strategy_name": t.strategy_name,
            "outcome": t.outcome,
            "entry_price": t.entry_price,
            "exit_price": t.exit_price,
            "size_usd": t.size_usd,
            "realized_pnl": t.realized_pnl,
            "realized_pnl_pct": t.realized_pnl_pct,
            "close_reason": t.close_reason,
            "opened_at": t.opened_at.isoformat() if t.opened_at else None,
            "closed_at": t.closed_at.isoformat() if t.closed_at else None,
        }
        for t in trades
    ]

@app.get("/api/performance")
async def get_performance():
    s = _sys()
    return await s.get_performance_stats()

@app.get("/api/whales")
async def get_whales(limit: int = 10):
    s = _sys()
    actions = await s.get_recent_whale_actions(limit)
    return [
        {
            "whale_address": a.whale_address,
            "whale_name": a.whale_name,
            "whale_tier": a.whale_tier,
            "whale_win_rate": a.whale_win_rate,
            "action": a.action,
            "market_question": a.market_question[:80],
            "amount_usd": a.amount_usd,
            "price": a.price,
            "timestamp": a.timestamp.isoformat(),
        }
        for a in actions
    ]

@app.post("/api/pause")
async def pause():
    await _sys().pause_trading()
    return {"status": "paused"}

@app.post("/api/resume")
async def resume():
    await _sys().resume_trading()
    return {"status": "resumed"}

@app.post("/api/positions/{position_id}/close")
async def close_position(position_id: str, req: ClosePositionRequest):
    result = await _sys().close_position(position_id)
    if result:
        return {"status": result.status, "fill_price": result.fill_price}
    raise HTTPException(404, "Position not found")

@app.post("/api/positions/close-all")
async def close_all():
    results = await _sys().close_all_positions("web_dashboard")
    return {"closed": len(results)}

@app.post("/api/mode")
async def set_mode(req: SetModeRequest):
    if req.mode not in ("live", "paper"):
        raise HTTPException(400, "mode must be live or paper")
    await _sys().set_mode(req.mode)
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
    s = _sys()
    for k, v in levels[req.level].items():
        setattr(s.settings.risk, k, v)
    return {"level": req.level}

# ── WebSocket for live portfolio push ─────────────────────────────────────────

class _WSManager:
    def __init__(self):
        self.active: list[WebSocket] = []
    async def connect(self, ws: WebSocket):
        await ws.accept(); self.active.append(ws)
    def disconnect(self, ws: WebSocket):
        self.active = [w for w in self.active if w is not ws]
    async def broadcast(self, data: dict):
        dead = []
        for ws in self.active:
            try: await ws.send_json(data)
            except: dead.append(ws)
        for ws in dead: self.disconnect(ws)

_ws_manager = _WSManager()

@app.websocket("/ws/portfolio")
async def ws_portfolio(websocket: WebSocket):
    await _ws_manager.connect(websocket)
    try:
        while True:
            if _system:
                try:
                    p = await _system.get_portfolio()
                    await websocket.send_json({
                        "type": "portfolio",
                        "balance": p.total_balance,
                        "daily_pnl": p.daily_pnl,
                        "daily_pnl_pct": p.daily_pnl_pct,
                        "positions": len(p.positions),
                        "exposure_pct": p.total_exposure_pct,
                    })
                except Exception:
                    pass
            await asyncio.sleep(5)
    except WebSocketDisconnect:
        _ws_manager.disconnect(websocket)

async def start_api(host: str = "0.0.0.0", port: int = 8088) -> None:
    import uvicorn
    config = uvicorn.Config(app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()
