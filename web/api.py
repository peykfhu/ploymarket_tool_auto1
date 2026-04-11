"""
web/api.py — Enhanced FastAPI backend v2.0
Standalone process reading directly from PostgreSQL + Redis.
New: market scanner, test orders, whale auto-follow, activity logs.
"""
from __future__ import annotations
import asyncio, json, os, uuid, time
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Any, Optional
import structlog
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logger = structlog.get_logger(__name__)

# ── In-memory state (shared within this process) ──────────────────────────────
_activity_log: list[dict] = []          # rolling 500-entry activity log
_test_orders: list[dict] = []           # test order history
_whale_follows: dict[str, dict] = {}    # market_id → follow config
_scanner_results: dict[str, list] = {}  # category → list of markets
_automation_config: dict = {
    "enabled": False,
    "agents": [],                        # ["politics","sports","esports","btc"]
    "min_confidence": 70,
    "max_position_pct": 0.05,
    "paper_only": True,
}
_db_session_factory: Any = None
_redis_client: Any = None
_init_lock: asyncio.Lock | None = None


def _log_activity(level: str, module: str, message: str, extra: dict | None = None):
    entry = {
        "id": uuid.uuid4().hex[:8],
        "time": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "module": module,
        "message": message,
        "extra": extra or {},
    }
    _activity_log.insert(0, entry)
    if len(_activity_log) > 500:
        _activity_log.pop()


async def _get_lock():
    global _init_lock
    if _init_lock is None:
        _init_lock = asyncio.Lock()
    return _init_lock


async def _ensure_db():
    global _db_session_factory
    if _db_session_factory is not None:
        return _db_session_factory
    lock = await _get_lock()
    async with lock:
        if _db_session_factory is not None:
            return _db_session_factory
        try:
            host = os.environ.get("DATABASE__POSTGRES_HOST", "postgres")
            port = os.environ.get("DATABASE__POSTGRES_PORT", "5432")
            db   = os.environ.get("POSTGRES_DB", "polymarket")
            user = os.environ.get("POSTGRES_USER", "trader")
            pw   = os.environ.get("POSTGRES_PASSWORD") or os.environ.get("DATABASE__POSTGRES_PASSWORD", "")
            url  = f"postgresql+asyncpg://{user}:{pw}@{host}:{port}/{db}"
            from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
            from database.models import Base
            engine = create_async_engine(url, pool_size=5, max_overflow=5, pool_pre_ping=True, echo=False)
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            _db_session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False, autocommit=False, autoflush=False)
            _log_activity("INFO", "api", "数据库连接成功", {"host": host})
        except Exception as exc:
            _log_activity("ERROR", "api", f"数据库连接失败: {exc}")
            _db_session_factory = None
    return _db_session_factory


async def _ensure_redis():
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    lock = await _get_lock()
    async with lock:
        if _redis_client is not None:
            return _redis_client
        try:
            import redis.asyncio as aioredis
            host = os.environ.get("DATABASE__REDIS_HOST", "redis")
            port = int(os.environ.get("DATABASE__REDIS_PORT", "6379"))
            pw   = os.environ.get("REDIS_PASSWORD") or os.environ.get("DATABASE__REDIS_PASSWORD", "")
            _redis_client = aioredis.Redis(host=host, port=port, password=pw, decode_responses=True, socket_connect_timeout=5)
            await _redis_client.ping()
            _log_activity("INFO", "api", "Redis连接成功", {"host": host})
        except Exception as exc:
            _log_activity("ERROR", "api", f"Redis连接失败: {exc}")
            _redis_client = None
    return _redis_client


async def _get_risk_state() -> dict:
    r = await _ensure_redis()
    if not r: return {}
    try:
        raw = await r.get("risk:state")
        return json.loads(raw) if raw else {}
    except: return {}


async def _get_portfolio_state() -> dict:
    r = await _ensure_redis()
    if not r: return {}
    try:
        raw = await r.get("state:portfolio")
        return json.loads(raw) if raw else {}
    except: return {}


async def _get_collector_health() -> dict:
    r = await _ensure_redis()
    if not r: return {}
    try:
        msgs = await r.xrevrange("stream:heartbeats", count=20)
        latest: dict[str, dict] = {}
        for msg_id, fields in msgs:
            name = fields.get("collector", "")
            if name and name not in latest:
                latest[name] = {
                    "name": name, "status": fields.get("status", "unknown"),
                    "messages_total": int(fields.get("messages_total", 0)),
                    "errors_total": int(fields.get("errors_total", 0)),
                    "last_error": fields.get("last_error", ""),
                    "latency_ms_avg": float(fields.get("latency_ms_avg", 0)),
                    "is_healthy": fields.get("is_healthy", "False") == "True",
                }
        return latest
    except: return {}


# ── Market Scanner ────────────────────────────────────────────────────────────
GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE  = "https://clob.polymarket.com"

CATEGORY_KEYWORDS = {
    "politics":  ["election", "president", "congress", "senate", "vote", "minister", "party", "referendum", "biden", "trump", "harris"],
    "sports":    ["win", "champion", "nba", "nfl", "mlb", "premier league", "world cup", "final", "playoffs", "series", "match", "beat", "defeat"],
    "esports":   ["league of legends", "lol", "dota", "cs2", "valorant", "esport", "worlds", "tournament", "lck", "lpl", "t1", "faker"],
    "btc":       ["bitcoin", "btc", "crypto", "ethereum", "eth", "price", "$", "above", "below", "reach", "hit"],
}

async def _scan_markets_from_api(category: str, keywords: list[str]) -> list[dict]:
    """Fetch live markets from Polymarket Gamma API matching keywords."""
    import aiohttp, asyncio
    results = []
    now = datetime.now(timezone.utc)

    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15),
            headers={"User-Agent": "Mozilla/5.0 PolyTrader/2.0"}
        ) as session:
            # Fetch active markets with pagination
            cursor = ""
            pages = 0
            while pages < 3:  # max 3 pages = 300 markets
                params: dict = {"active": "true", "closed": "false", "limit": 100}
                if cursor:
                    params["cursor"] = cursor
                try:
                    async with session.get(f"{GAMMA_BASE}/markets", params=params) as resp:
                        if resp.status != 200:
                            break
                        data = await resp.json()
                    markets = data if isinstance(data, list) else data.get("data", [])
                    cursor = data.get("next_cursor", "") if isinstance(data, dict) else ""
                    pages += 1

                    for m in markets:
                        q = (m.get("question") or "").lower()
                        end_date_str = m.get("endDate") or m.get("endDateIso") or ""

                        # Filter by keyword
                        if not any(kw in q for kw in keywords):
                            continue

                        # Parse end date
                        end_dt = None
                        try:
                            if end_date_str:
                                end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                        except (ValueError, TypeError) as exc:
                            logger.debug("end_date_parse_failed", value=end_date_str, error=str(exc))

                        # Only include: currently active OR ending in next 7 days
                        if end_dt:
                            hours_left = (end_dt - now).total_seconds() / 3600
                            if hours_left < 0 or hours_left > 168:  # 0 to 7 days
                                continue
                        else:
                            hours_left = 999

                        liquidity = float(m.get("liquidityNum") or m.get("liquidity") or 0)
                        yes_price = 0.5
                        try:
                            outcomes = m.get("outcomePrices") or []
                            if outcomes and len(outcomes) >= 1:
                                yes_price = float(outcomes[0])
                        except (ValueError, TypeError, IndexError) as exc:
                            logger.debug("yes_price_parse_failed", error=str(exc))

                        results.append({
                            "id": m.get("id") or m.get("conditionId", ""),
                            "question": m.get("question", ""),
                            "category": category,
                            "yes_price": round(yes_price, 4),
                            "no_price": round(1 - yes_price, 4),
                            "liquidity": round(liquidity, 0),
                            "volume": float(m.get("volume") or 0),
                            "end_date": end_dt.isoformat() if end_dt else "",
                            "hours_left": round(hours_left, 1) if hours_left != 999 else None,
                            "url": f"https://polymarket.com/event/{m.get('slug', '')}",
                            "edge_score": _calc_edge_score(yes_price, liquidity, hours_left),
                        })

                    if not cursor or not markets:
                        break
                    await asyncio.sleep(0.5)  # rate limit protection

                except Exception as exc:
                    _log_activity("WARN", "scanner", f"分页获取失败: {exc}")
                    break

    except Exception as exc:
        _log_activity("ERROR", "scanner", f"扫描失败 [{category}]: {exc}")

    # Sort by edge score
    results.sort(key=lambda x: x["edge_score"], reverse=True)
    _log_activity("INFO", "scanner", f"扫描完成 [{category}]: 找到 {len(results)} 个市场")
    return results[:50]


def _calc_edge_score(yes_price: float, liquidity: float, hours_left: float) -> float:
    """Score 0-100: how tradeable/opportunistic is this market."""
    score = 0.0
    # Price near 0.5 = uncertain = opportunity
    uncertainty = 1.0 - abs(yes_price - 0.5) * 2
    score += uncertainty * 30
    # Liquidity (log scale, saturates at $50k)
    import math
    score += min(30, math.log1p(liquidity) / math.log1p(50000) * 30)
    # Time urgency (few hours left = higher urgency for live trades)
    if hours_left is not None and hours_left < 168:
        if hours_left < 2:
            score += 40  # very urgent
        elif hours_left < 24:
            score += 25
        elif hours_left < 72:
            score += 15
        else:
            score += 5
    return round(score, 1)


# ── FastAPI App ───────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await _ensure_db()
    await _ensure_redis()
    _log_activity("INFO", "api", "API服务器启动完成", {"port": 8088})
    yield
    if _redis_client:
        try:
            await _redis_client.aclose()
        except Exception as exc:
            logger.warning("redis_close_failed", error=str(exc))


app = FastAPI(title="Polymarket Trader API v2", version="2.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ── Request Models ─────────────────────────────────────────────────────────────
class ClosePositionRequest(BaseModel):
    reason: str = "manual"

class SetModeRequest(BaseModel):
    mode: str

class SetRiskRequest(BaseModel):
    level: str

class TestOrderRequest(BaseModel):
    market_id: str
    market_question: str
    outcome: str = "Yes"
    size_usd: float = 10.0
    price: float = 0.5
    strategy: str = "manual_test"
    note: str = ""

class WhaleFollowConfig(BaseModel):
    market_id: str
    market_question: str
    enabled: bool = True
    min_whale_tier: str = "A"
    auto_follow: bool = True
    follow_size_pct: float = 0.02

class AutomationConfig(BaseModel):
    enabled: bool
    agents: list[str]
    min_confidence: int = 70
    max_position_pct: float = 0.05
    paper_only: bool = True

class PaperBalanceRequest(BaseModel):
    balance: float


# ── Health ─────────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    db_ok = _db_session_factory is not None
    redis_ok = _redis_client is not None
    if not db_ok: await _ensure_db(); db_ok = _db_session_factory is not None
    if not redis_ok: await _ensure_redis(); redis_ok = _redis_client is not None
    return {"status": "ok", "db": "connected" if db_ok else "disconnected",
            "redis": "connected" if redis_ok else "disconnected",
            "time": datetime.now(timezone.utc).isoformat()}


# ── Status ────────────────────────────────────────────────────────────────────
@app.get("/api/status")
async def get_status():
    risk = await _get_risk_state()
    portfolio = await _get_portfolio_state()
    collectors = await _get_collector_health()
    mode = portfolio.get("mode", os.environ.get("EXECUTION__MODE", "paper"))
    r = await _ensure_redis()
    paused = False
    if r:
        try:
            paused = await r.get("control:paused") == "1"
        except Exception as exc:
            logger.warning("redis_get_paused_failed", error=str(exc))
    return {
        "mode": mode, "paused": paused,
        "circuit_breaker": {
            "active": bool(risk.get("circuit_breaker_active", False)),
            "level": risk.get("circuit_breaker_level", "none"),
            "reason": risk.get("circuit_breaker_reason", ""),
            "resume_at": risk.get("circuit_breaker_resume_at"),
        },
        "collectors": collectors,
        "automation": _automation_config,
    }


# ── Portfolio ─────────────────────────────────────────────────────────────────
@app.get("/api/portfolio")
async def get_portfolio():
    factory = await _ensure_db()
    state = await _get_portfolio_state()
    risk = await _get_risk_state()

    portfolio = {
        "total_balance": float(state.get("total_balance", 0)),
        "available_balance": float(state.get("available_balance", 0)),
        "total_position_value": float(state.get("total_position_value", 0)),
        "exposure_pct": 0.0,
        "daily_pnl": float(risk.get("daily_pnl", 0)),
        "daily_pnl_pct": float(risk.get("daily_pnl_pct", 0)),
        "total_trades_today": int(risk.get("daily_trades_count", 0)),
        "wins_today": int(risk.get("daily_wins", 0)),
        "losses_today": int(risk.get("daily_losses", 0)),
        "positions": [],
    }
    total = portfolio["total_balance"]
    if total > 0:
        portfolio["exposure_pct"] = portfolio["total_position_value"] / total

    if factory:
        try:
            from sqlalchemy import text
            mode = os.environ.get("EXECUTION__MODE", "paper")
            async with factory() as session:
                result = await session.execute(text("""
                    SELECT id, market_id, market_question, outcome, side,
                           entry_price, entry_price, size_usd, 0.0, 0.0,
                           0.0, 0.0, strategy_name, signal_id, opened_at
                    FROM trades WHERE closed_at IS NULL AND mode = :mode
                    ORDER BY opened_at DESC LIMIT 50
                """), {"mode": mode})
                rows = result.fetchall()
            portfolio["positions"] = [{
                "position_id": str(r[0]), "market_id": r[1],
                "market_question": (r[2] or "")[:100], "outcome": r[3],
                "side": r[4], "entry_price": float(r[5] or 0),
                "current_price": float(r[6] or 0), "size_usd": float(r[7] or 0),
                "unrealized_pnl": float(r[8] or 0), "unrealized_pnl_pct": float(r[9] or 0),
                "take_profit": float(r[10] or 0), "stop_loss": float(r[11] or 0),
                "strategy_name": r[12] or "", "signal_id": r[13] or "",
                "opened_at": r[14].isoformat() if r[14] else "",
            } for r in rows]
        except Exception as exc:
            _log_activity("WARN", "api", f"持仓查询失败: {exc}")
    return portfolio


# ── Trades ────────────────────────────────────────────────────────────────────
@app.get("/api/trades")
async def get_trades(days: int = 1):
    factory = await _ensure_db()
    if not factory: return []
    try:
        from sqlalchemy import text
        mode = os.environ.get("EXECUTION__MODE", "paper")
        since = datetime.now(timezone.utc) - timedelta(days=days)
        async with factory() as session:
            result = await session.execute(text("""
                SELECT id, market_question, strategy_name, outcome,
                       entry_price, exit_price, size_usd,
                       realized_pnl, realized_pnl_pct,
                       close_reason, opened_at, closed_at
                FROM trades WHERE mode=:mode AND opened_at>=:since
                ORDER BY opened_at DESC LIMIT 200
            """), {"mode": mode, "since": since})
            rows = result.fetchall()
        return [{"trade_id": str(r[0]), "market_question": (r[1] or "")[:80],
                 "strategy_name": r[2] or "", "outcome": r[3] or "",
                 "entry_price": float(r[4] or 0), "exit_price": float(r[5]) if r[5] else None,
                 "size_usd": float(r[6] or 0),
                 "realized_pnl": float(r[7]) if r[7] is not None else None,
                 "realized_pnl_pct": float(r[8]) if r[8] is not None else None,
                 "close_reason": r[9], "opened_at": r[10].isoformat() if r[10] else None,
                 "closed_at": r[11].isoformat() if r[11] else None} for r in rows]
    except Exception as exc:
        _log_activity("WARN", "api", f"交易记录查询失败: {exc}")
        return []


# ── Performance ───────────────────────────────────────────────────────────────
@app.get("/api/performance")
async def get_performance(days: int = 30):
    factory = await _ensure_db()
    if not factory: return {"total_pnl": 0, "win_rate": 0, "total_trades": 0}
    try:
        from sqlalchemy import text
        mode = os.environ.get("EXECUTION__MODE", "paper")
        start = datetime.now(timezone.utc) - timedelta(days=days)
        async with factory() as session:
            result = await session.execute(text("""
                SELECT COUNT(*) AS total,
                       COALESCE(SUM(realized_pnl), 0) AS total_pnl,
                       COUNT(*) FILTER (WHERE realized_pnl >= 0) AS wins,
                       COALESCE(AVG(realized_pnl), 0) AS avg_pnl
                FROM trades WHERE mode=:mode AND closed_at IS NOT NULL AND closed_at>=:start
            """), {"mode": mode, "start": start})
            row = result.fetchone()
            # Daily equity for chart
            eq_result = await session.execute(text("""
                SELECT DATE(closed_at AT TIME ZONE 'UTC') as day,
                       COALESCE(SUM(realized_pnl), 0) as day_pnl
                FROM trades WHERE mode=:mode AND closed_at IS NOT NULL AND closed_at>=:start
                GROUP BY day ORDER BY day
            """), {"mode": mode, "start": start})
            eq_rows = eq_result.fetchall()
        total = int(row[0]) if row else 0
        total_pnl = float(row[1]) if row else 0.0
        wins = int(row[2]) if row else 0
        equity_curve = []
        running = float((await _get_portfolio_state()).get("total_balance", 10000)) - total_pnl
        for r in eq_rows:
            running += float(r[1])
            equity_curve.append({"date": str(r[0]), "value": round(running, 2)})
        return {
            "total_trades": total, "total_pnl": round(total_pnl, 2),
            "total_pnl_pct": 0.0, "wins": wins, "losses": total - wins,
            "win_rate": round(wins / total, 4) if total > 0 else 0.0,
            "avg_pnl": float(row[3]) if row else 0.0,
            "equity_curve": equity_curve,
        }
    except Exception as exc:
        _log_activity("WARN", "api", f"绩效查询失败: {exc}")
        return {"total_pnl": 0, "win_rate": 0, "total_trades": 0, "equity_curve": []}


# ── Market Scanner ─────────────────────────────────────────────────────────────
@app.get("/api/markets/scan")
async def scan_markets(category: str = "all", refresh: bool = False):
    """Scan live Polymarket markets. Results cached for 5 min."""
    global _scanner_results
    cats = ["politics", "sports", "esports", "btc"] if category == "all" else [category]
    results = {}
    for cat in cats:
        cache_key = f"{cat}_ts"
        cached_ts = _scanner_results.get(cache_key, 0)
        if not refresh and time.time() - cached_ts < 300:  # 5 min cache
            results[cat] = _scanner_results.get(cat, [])
        else:
            keywords = CATEGORY_KEYWORDS.get(cat, [])
            markets = await _scan_markets_from_api(cat, keywords)
            _scanner_results[cat] = markets
            _scanner_results[cache_key] = time.time()
            results[cat] = markets
    return results


@app.get("/api/markets/{market_id}/price")
async def get_market_price(market_id: str):
    """Get current price for a specific market."""
    import aiohttp
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
            async with session.get(f"{CLOB_BASE}/book", params={"token_id": market_id}) as resp:
                if resp.status != 200:
                    return {"yes_price": 0.5, "no_price": 0.5, "spread": 0.02}
                data = await resp.json()
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        best_bid = float(bids[0]["price"]) if bids else 0.48
        best_ask = float(asks[0]["price"]) if asks else 0.52
        mid = (best_bid + best_ask) / 2
        return {"yes_price": round(mid, 4), "no_price": round(1-mid, 4),
                "bid": best_bid, "ask": best_ask, "spread": round(best_ask - best_bid, 4),
                "bids": bids[:5], "asks": asks[:5]}
    except Exception as exc:
        return {"yes_price": 0.5, "no_price": 0.5, "spread": 0.02, "error": str(exc)}


# ── Test Orders ────────────────────────────────────────────────────────────────
@app.get("/api/test-orders")
async def get_test_orders():
    return _test_orders


@app.post("/api/test-orders")
async def place_test_order(req: TestOrderRequest):
    """Simulate a test order against real market price."""
    # Fetch real current price
    price_data = await get_market_price(req.market_id)
    actual_price = price_data["yes_price"] if req.outcome == "Yes" else price_data["no_price"]
    slippage = abs(actual_price - req.price) / req.price if req.price > 0 else 0

    order = {
        "id": uuid.uuid4().hex[:12],
        "market_id": req.market_id,
        "market_question": req.market_question[:100],
        "outcome": req.outcome,
        "requested_price": req.price,
        "fill_price": round(actual_price, 4),
        "size_usd": req.size_usd,
        "tokens": round(req.size_usd / actual_price, 2) if actual_price > 0 else 0,
        "slippage_pct": round(slippage * 100, 3),
        "fee": round(req.size_usd * 0.02, 2),
        "strategy": req.strategy,
        "note": req.note,
        "status": "filled",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "pnl": None,
        "closed_at": None,
    }
    _test_orders.insert(0, order)
    if len(_test_orders) > 200:
        _test_orders.pop()

    # Write to DB if available
    factory = await _ensure_db()
    if factory:
        try:
            from sqlalchemy import text
            async with factory() as session:
                async with session.begin():
                    await session.execute(text("""
                        INSERT INTO trades (id, order_open_id, market_id, market_question,
                            strategy_name, signal_id, side, outcome, entry_price, size_usd,
                            fee_total, opened_at, mode)
                        VALUES (:id, :id, :mid, :mq, :strat, 'test', 'buy', :outcome,
                            :price, :size, :fee, NOW(), 'paper')
                        ON CONFLICT (id) DO NOTHING
                    """), {"id": order["id"], "mid": req.market_id, "mq": req.market_question[:200],
                           "strat": req.strategy, "outcome": req.outcome,
                           "price": actual_price, "size": req.size_usd, "fee": order["fee"]})
        except Exception as exc:
            _log_activity("WARN", "test_order", f"DB写入失败: {exc}")

    _log_activity("INFO", "test_order",
                  f"测试下单: {req.market_question[:50]} | {req.outcome} @ {actual_price:.4f}",
                  {"size": req.size_usd, "slippage": f"{slippage*100:.2f}%"})
    return order


@app.delete("/api/test-orders/{order_id}")
async def cancel_test_order(order_id: str):
    global _test_orders
    _test_orders = [o for o in _test_orders if o["id"] != order_id]
    return {"status": "cancelled"}


# ── Whale Tracking ─────────────────────────────────────────────────────────────
@app.get("/api/whales")
async def get_whales(limit: int = 20):
    factory = await _ensure_db()
    db_whales = []
    if factory:
        try:
            from sqlalchemy import text
            async with factory() as session:
                result = await session.execute(text("""
                    SELECT wt.whale_address, wp.name, wp.tier, wp.win_rate,
                           wt.action, wt.market_question, wt.amount_usd,
                           wt.price, wt.timestamp
                    FROM whale_trades wt
                    LEFT JOIN whale_profiles wp ON wp.address = wt.whale_address
                    ORDER BY wt.timestamp DESC LIMIT :limit
                """), {"limit": limit})
                rows = result.fetchall()
            db_whales = [{
                "whale_address": r[0] or "", "whale_name": r[1] or f"whale_{(r[0] or '')[:6]}",
                "whale_tier": r[2] or "B", "whale_win_rate": float(r[3] or 0),
                "action": r[4] or "buy", "market_question": (r[5] or "")[:80],
                "amount_usd": float(r[6] or 0), "price": float(r[7] or 0),
                "timestamp": r[8].isoformat() if r[8] else datetime.now(timezone.utc).isoformat(),
                "follow_enabled": _whale_follows.get(r[0] or "", {}).get("auto_follow", False),
            } for r in rows]
        except Exception as exc:
            _log_activity("ERROR", "api", f"whale query failed: {exc}")

    # If no DB data, return mock structure showing system is ready
    if not db_whales:
        db_whales = [{
            "whale_address": "0x0000...demo", "whale_name": "等待链上数据",
            "whale_tier": "—", "whale_win_rate": 0,
            "action": "—", "market_question": "大户追踪需要配置Polygon RPC和私钥",
            "amount_usd": 0, "price": 0,
            "timestamp": datetime.now(timezone.utc).isoformat(), "follow_enabled": False,
        }]
    return db_whales


@app.get("/api/whales/follows")
async def get_whale_follows():
    return list(_whale_follows.values())


@app.post("/api/whales/follows")
async def set_whale_follow(config: WhaleFollowConfig):
    _whale_follows[config.market_id] = config.dict()
    _log_activity("INFO", "whale", f"设置大户跟随: {config.market_question[:50]}", {"auto": config.auto_follow})
    return {"status": "ok", "config": config.dict()}


@app.delete("/api/whales/follows/{market_id}")
async def remove_whale_follow(market_id: str):
    _whale_follows.pop(market_id, None)
    return {"status": "removed"}


# ── Automation ────────────────────────────────────────────────────────────────
@app.get("/api/automation")
async def get_automation():
    return _automation_config


@app.post("/api/automation")
async def set_automation(config: AutomationConfig):
    global _automation_config
    _automation_config.update(config.dict())
    r = await _ensure_redis()
    if r:
        try:
            await r.set("control:automation", json.dumps(_automation_config))
        except Exception as exc:
            logger.warning("redis_set_automation_failed", error=str(exc))
    _log_activity("INFO", "automation",
                  f"自动化配置更新: {'启用' if config.enabled else '停用'}",
                  {"agents": config.agents})
    return _automation_config


@app.post("/api/automation/trigger")
async def trigger_automation(background_tasks: BackgroundTasks):
    """Manually trigger a scan + signal generation cycle."""
    background_tasks.add_task(_run_automation_cycle)
    return {"status": "triggered"}


# ── Agents (strategy registry surface for the dashboard) ──────────────────────
#
# Hard-coded registry keyed to what strategy/orchestrator.py actually
# wires up. When a new strategy is added there, mirror it here so the
# dashboard can render its tile.
AGENT_REGISTRY: list[dict] = [
    {
        "id": "tail_end",
        "name": "TailEnd 吃尾盘",
        "category": "scalper",
        "description": (
            "Closing-tape scalper. Scans markets whose end_date is within "
            "the configured window and scalps the favored side when the "
            "book is thin and momentum is confirmed."
        ),
        "risk": "medium",
        "signal_driven": False,
        "scan_driven": True,
    },
    {
        "id": "info_asymmetry",
        "name": "InfoArb 信息差",
        "category": "latency_edge",
        "description": (
            "Exploits latency between primary-source news (politics / "
            "sports) and Polymarket repricing. Fires on NewsSignal and "
            "SportsSignal events that show a high speed_advantage or edge."
        ),
        "risk": "medium",
        "signal_driven": True,
        "scan_driven": False,
    },
    {
        "id": "swing_mean_reversion",
        "name": "Mean Reversion",
        "category": "swing",
        "description": "Buys/sells price deviations from 7-day mean without news catalyst.",
        "risk": "low",
        "signal_driven": True,
        "scan_driven": False,
    },
    {
        "id": "swing_breakout",
        "name": "Breakout",
        "category": "swing",
        "description": "Volume-confirmed breakout after consolidation.",
        "risk": "medium",
        "signal_driven": True,
        "scan_driven": False,
    },
    {
        "id": "swing_event_driven",
        "name": "Event Driven",
        "category": "swing",
        "description": "News-triggered directional trades with entity matching.",
        "risk": "medium",
        "signal_driven": True,
        "scan_driven": False,
    },
    {
        "id": "sports_safe_lock",
        "name": "Safe Lock",
        "category": "sports",
        "description": "Leading team with little time remaining, still underpriced.",
        "risk": "low",
        "signal_driven": True,
        "scan_driven": False,
    },
    {
        "id": "sports_reversal",
        "name": "Reversal Catch",
        "category": "sports",
        "description": "Trailing team with momentum shift; market overly pessimistic.",
        "risk": "high",
        "signal_driven": True,
        "scan_driven": False,
    },
    {
        "id": "sports_live_ou",
        "name": "Live Over/Under",
        "category": "sports",
        "description": "Live-game pace-of-play O/U mispricing.",
        "risk": "medium",
        "signal_driven": True,
        "scan_driven": False,
    },
    {
        "id": "whale_single_follow",
        "name": "Whale Follow",
        "category": "whale",
        "description": "Follow a single S-tier whale's large position after a delay.",
        "risk": "medium",
        "signal_driven": True,
        "scan_driven": False,
    },
    {
        "id": "whale_consensus",
        "name": "Whale Consensus",
        "category": "whale",
        "description": "Follow when N+ whales move the same direction inside a window.",
        "risk": "low",
        "signal_driven": True,
        "scan_driven": False,
    },
]


@app.get("/api/agents")
async def list_agents():
    """
    Return the strategy/agent registry with enabled-flag inferred from
    settings.yaml (via env fallbacks) + the current execution mode.
    """
    enabled_env = os.environ.get("STRATEGY__ENABLED_STRATEGIES", "")
    enabled_set: set[str] = set()
    if enabled_env:
        enabled_set = {x.strip() for x in enabled_env.split(",") if x.strip()}
    else:
        # Fall back to the hard-coded yaml default list
        enabled_set = {
            "swing_mean_reversion", "swing_breakout", "swing_event_driven",
            "sports_safe_lock", "sports_reversal", "sports_live_ou",
            "whale_single_follow", "whale_consensus",
            "tail_end", "info_asymmetry",
        }

    mode = os.environ.get("EXECUTION__MODE", "paper")
    agents = [
        {**a, "enabled": a["id"] in enabled_set}
        for a in AGENT_REGISTRY
    ]
    return {
        "mode": mode,
        "total": len(agents),
        "enabled": sum(1 for a in agents if a["enabled"]),
        "agents": agents,
    }


@app.post("/api/agents/{agent_id}/toggle")
async def toggle_agent(agent_id: str):
    """
    Toggle a single agent on/off via Redis control key. The trading
    engine picks this up on its next loop cycle.
    """
    if not any(a["id"] == agent_id for a in AGENT_REGISTRY):
        raise HTTPException(404, f"unknown agent: {agent_id}")
    r = await _ensure_redis()
    new_state = "on"
    if r:
        try:
            key = f"control:agent:{agent_id}"
            current = await r.get(key)
            new_state = "off" if current == "on" else "on"
            await r.set(key, new_state)
        except Exception as exc:
            logger.warning("toggle_agent_redis_failed", error=str(exc))
    _log_activity("INFO", "agents", f"Agent [{agent_id}] 切换至 {new_state}")
    return {"agent_id": agent_id, "state": new_state}


async def _run_automation_cycle():
    _log_activity("INFO", "automation", "手动触发自动化扫描周期")
    for agent in _automation_config.get("agents", []):
        keywords = CATEGORY_KEYWORDS.get(agent, [])
        if keywords:
            markets = await _scan_markets_from_api(agent, keywords)
            _scanner_results[agent] = markets
            _scanner_results[f"{agent}_ts"] = time.time()
            _log_activity("INFO", "automation", f"代理 [{agent}] 扫描完成: {len(markets)} 个市场")


# ── Activity Log ──────────────────────────────────────────────────────────────
@app.get("/api/logs")
async def get_logs(level: str = "all", limit: int = 100):
    logs = _activity_log
    if level != "all":
        logs = [l for l in logs if l["level"] == level.upper()]
    return logs[:limit]


@app.delete("/api/logs")
async def clear_logs():
    _activity_log.clear()
    return {"status": "cleared"}


# ── Paper balance ─────────────────────────────────────────────────────────────
@app.post("/api/paper/balance")
async def set_paper_balance(req: PaperBalanceRequest):
    r = await _ensure_redis()
    if r:
        state = await _get_portfolio_state()
        state["total_balance"] = req.balance
        state["available_balance"] = req.balance
        state["total_position_value"] = 0.0
        await r.set("state:portfolio", json.dumps(state))
    _log_activity("INFO", "paper", f"模拟账户余额已设置: ${req.balance:,.2f}")
    return {"balance": req.balance}


# ── Control ───────────────────────────────────────────────────────────────────
@app.post("/api/pause")
async def pause():
    await _ensure_redis()
    if _redis_client: await _redis_client.set("control:paused", "1")
    _log_activity("WARN", "control", "交易已暂停")
    return {"status": "paused"}


@app.post("/api/resume")
async def resume():
    await _ensure_redis()
    if _redis_client:
        await _redis_client.set("control:paused", "0")
        raw = await _redis_client.get("risk:state")
        if raw:
            state = json.loads(raw)
            state["circuit_breaker_active"] = False
            await _redis_client.set("risk:state", json.dumps(state))
    _log_activity("INFO", "control", "交易已恢复")
    return {"status": "resumed"}


@app.post("/api/positions/{position_id}/close")
async def close_position(position_id: str, req: ClosePositionRequest):
    r = await _ensure_redis()
    if not r: raise HTTPException(503, "Redis unavailable")
    await r.lpush("control:commands", json.dumps({"action": "close_position", "position_id": position_id, "reason": req.reason}))
    return {"status": "queued"}


@app.post("/api/positions/close-all")
async def close_all():
    r = await _ensure_redis()
    if not r: raise HTTPException(503, "Redis unavailable")
    await r.lpush("control:commands", json.dumps({"action": "close_all", "reason": "web_dashboard"}))
    _log_activity("WARN", "control", "全部平仓指令已发送")
    return {"status": "queued"}


@app.post("/api/mode")
async def set_mode(req: SetModeRequest):
    if req.mode not in ("live", "paper"): raise HTTPException(400, "invalid mode")
    r = await _ensure_redis()
    if r: await r.set("control:mode", req.mode)
    _log_activity("INFO", "control", f"交易模式切换: {req.mode}")
    return {"mode": req.mode}


@app.post("/api/risk")
async def set_risk(req: SetRiskRequest):
    levels = {"conservative": 0.04, "moderate": 0.08, "aggressive": 0.12}
    if req.level not in levels: raise HTTPException(400, "invalid level")
    r = await _ensure_redis()
    if r: await r.set("control:risk_level", req.level)
    _log_activity("INFO", "control", f"风险等级设置: {req.level}")
    return {"level": req.level}


# ── Pending signals (cached by SignalPipeline) ────────────────────────────────
class ExecuteSignalRequest(BaseModel):
    size_multiplier: float = 1.0


@app.get("/api/signals/pending")
async def get_pending_signals(limit: int = 50):
    r = await _ensure_redis()
    if not r:
        return {"signals": []}
    signals: list[dict] = []
    try:
        cursor = 0
        scanned = 0
        while scanned < 500:
            cursor, keys = await r.scan(cursor=cursor, match="cache:signal:*", count=100)
            for key in keys:
                raw = await r.get(key)
                if not raw:
                    continue
                try:
                    data = json.loads(raw)
                except (ValueError, TypeError):
                    continue
                signals.append(data)
                if len(signals) >= limit:
                    break
            scanned += len(keys)
            if cursor == 0 or len(signals) >= limit:
                break
    except Exception as exc:
        _log_activity("ERROR", "signals", f"pending scan failed: {exc}")
    signals.sort(key=lambda s: s.get("confidence_score", 0), reverse=True)
    return {"signals": signals}


@app.post("/api/signals/{signal_id}/execute")
async def execute_pending_signal(signal_id: str, req: ExecuteSignalRequest):
    r = await _ensure_redis()
    if not r:
        raise HTTPException(503, "Redis unavailable")
    cmd = {
        "action": "execute_signal",
        "signal_id": signal_id,
        "size_multiplier": float(req.size_multiplier or 1.0),
    }
    await r.lpush("control:commands", json.dumps(cmd))
    _log_activity("INFO", "signals", f"execute queued: {signal_id}", {"mult": req.size_multiplier})
    return {"status": "queued", "signal_id": signal_id}


@app.post("/api/signals/{signal_id}/skip")
async def skip_pending_signal(signal_id: str):
    r = await _ensure_redis()
    if not r:
        raise HTTPException(503, "Redis unavailable")
    await r.delete(f"cache:signal:{signal_id}")
    await r.lpush(
        "control:commands",
        json.dumps({"action": "skip_signal", "signal_id": signal_id}),
    )
    _log_activity("INFO", "signals", f"skipped: {signal_id}")
    return {"status": "skipped", "signal_id": signal_id}


# ── WebSocket ─────────────────────────────────────────────────────────────────
class _WSManager:
    def __init__(self): self.active: list[WebSocket] = []
    async def connect(self, ws: WebSocket): await ws.accept(); self.active.append(ws)
    def disconnect(self, ws: WebSocket): self.active = [w for w in self.active if w is not ws]
    async def broadcast(self, data: dict):
        dead = []
        for ws in self.active:
            try: await ws.send_json(data)
            except: dead.append(ws)
        for ws in dead: self.disconnect(ws)

_ws_manager = _WSManager()


@app.websocket("/ws/live")
async def ws_live(websocket: WebSocket):
    """Unified live WebSocket — pushes portfolio, logs, scanner updates."""
    await _ws_manager.connect(websocket)
    last_log_count = 0
    try:
        while True:
            state = await _get_portfolio_state()
            risk = await _get_risk_state()
            # Send portfolio update
            await websocket.send_json({
                "type": "portfolio",
                "balance": float(state.get("total_balance", 0)),
                "daily_pnl": float(risk.get("daily_pnl", 0)),
                "daily_pnl_pct": float(risk.get("daily_pnl_pct", 0)),
                "ts": datetime.now(timezone.utc).isoformat(),
            })
            # Push new log entries
            if len(_activity_log) > last_log_count:
                new_logs = _activity_log[:len(_activity_log) - last_log_count]
                await websocket.send_json({"type": "logs", "entries": new_logs[:10]})
                last_log_count = len(_activity_log)
            await asyncio.sleep(3)
    except WebSocketDisconnect:
        _ws_manager.disconnect(websocket)
    except Exception:
        _ws_manager.disconnect(websocket)


# Legacy WS endpoint for backward compat
@app.websocket("/ws/portfolio")
async def ws_portfolio(websocket: WebSocket):
    await _ws_manager.connect(websocket)
    try:
        while True:
            state = await _get_portfolio_state()
            risk = await _get_risk_state()
            await websocket.send_json({
                "type": "portfolio",
                "balance": float(state.get("total_balance", 0)),
                "daily_pnl": float(risk.get("daily_pnl", 0)),
                "daily_pnl_pct": float(risk.get("daily_pnl_pct", 0)),
            })
            await asyncio.sleep(5)
    except: _ws_manager.disconnect(websocket)
