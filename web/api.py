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
    # Politics: broad sweep
    "politics": [
        "election", "elect", "president", "presidential", "congress", "senate", "vote", "voting",
        "minister", "prime minister", "party", "referendum", "republican", "democrat",
        "biden", "trump", "harris", "obama", "governor", "mayor", "poll",
        "legislation", "bill", "law", "court", "supreme", "ruling", "impeach",
        "macron", "sunak", "modi", "xi", "putin", "nato", "un ", "g7", "g20",
        "war", "ceasefire", "sanction", "treaty", "cabinet", "parliament",
    ],
    # Sports: all major sports and competitions
    "sports": [
        "win", "champion", "nba", "nfl", "mlb", "nhl", "mls",
        "premier league", "la liga", "bundesliga", "serie a", "ligue 1",
        "world cup", "euros", "copa", "champions league", "europa league",
        "final", "playoffs", "semifinal", "quarter", "series",
        "match", "game", "beat", "defeat", "score",
        "super bowl", "stanley cup", "masters", "open", "grand slam",
        "wimbledon", "us open", "french open", "australian open",
        "formula 1", "f1", "nascar", "ufc", "boxing", "tennis",
        "basketball", "football", "soccer", "baseball", "hockey",
        "golf", "swimming", "olympics", "medal",
    ],
    # Esports
    "esports": [
        "league of legends", "lol", "dota", "dota 2", "cs2", "csgo",
        "valorant", "esport", "esports", "worlds", "world championship",
        "tournament", "lck", "lpl", "lec", "lcs", "t1", "faker",
        "g2", "fnatic", "navi", "cloud9", "team liquid", "faze",
        "overwatch", "hearthstone", "starcraft", "rocket league",
        "fortnite", "pubg", "apex", "gaming", "gamer",
    ],
    # Crypto / BTC
    "btc": [
        "bitcoin", "btc", "crypto", "cryptocurrency", "ethereum", "eth",
        "price", "above", "below", "reach", "hit", "exceed",
        "solana", "sol", "xrp", "ripple", "bnb", "usdc",
        "defi", "nft", "blockchain", "halving", "etf",
        "coinbase", "binance", "kraken", "sec", "cftc",
        "stablecoin", "altcoin", "market cap", "bull", "bear",
    ],
}

# Gamma API category mapping
GAMMA_CATEGORY_MAP = {
    "politics": "politics",
    "sports": "sports",
    "esports": "pop culture",   # Polymarket groups esports under pop culture/sports
    "btc": "crypto",
}

def _calc_edge_score(yes_price: float, liquidity: float, hours_left) -> float:
    """Score 0-100: how tradeable/opportunistic is this market right now."""
    import math
    score = 0.0
    # Uncertainty: price near 0.5 = maximum opportunity
    uncertainty = 1.0 - abs(yes_price - 0.5) * 2
    score += uncertainty * 30
    # Liquidity score (log scale, caps at $100k)
    score += min(30, math.log1p(max(0, liquidity)) / math.log1p(100_000) * 30)
    # Time urgency bonus
    if hours_left is not None:
        if hours_left < 1:
            score += 40    # ending in < 1 hour — highest urgency
        elif hours_left < 6:
            score += 34
        elif hours_left < 24:
            score += 26
        elif hours_left < 72:
            score += 18
        elif hours_left < 168:
            score += 10
        elif hours_left < 720:
            score += 4
    return round(min(score, 100), 1)


async def _scan_markets_from_api(category: str, keywords: list[str]) -> list[dict]:
    """
    Fetch active markets from Polymarket Gamma API.
    Strategy:
      1. Fetch up to 500 active markets with pagination
      2. Filter by keyword across question+slug+description+tags
      3. Filter by end date (0 to 30 days from now, or no end date)
      4. Score and sort by opportunity score
    """
    import aiohttp  # noqa: F811
    results: list[dict] = []
    now = datetime.now(timezone.utc)
    seen_ids: set = set()

    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=25),
            headers={"User-Agent": "Mozilla/5.0 PolyTrader/2.0"},
        ) as session:
            cursor = ""
            pages = 0

            while pages < 5:   # up to 500 markets (100 per page)
                params: dict = {"active": "true", "closed": "false", "limit": 100}
                if cursor:
                    params["cursor"] = cursor

                try:
                    async with session.get(
                        f"{GAMMA_BASE}/markets", params=params
                    ) as resp:
                        if resp.status != 200:
                            _log_activity("WARN", "scanner",
                                f"Gamma API returned {resp.status} for [{category}]")
                            break
                        raw = await resp.json(content_type=None)

                    # Gamma API returns list directly or {data:[], next_cursor:""}
                    if isinstance(raw, list):
                        markets = raw
                        cursor = ""
                    else:
                        markets = raw.get("data") or raw.get("markets") or []
                        cursor = raw.get("next_cursor") or raw.get("nextCursor") or ""

                    pages += 1

                    for m in markets:
                        market_id = m.get("id") or m.get("conditionId") or ""
                        if not market_id or market_id in seen_ids:
                            continue
                        seen_ids.add(market_id)

                        # Build searchable text from all fields
                        q       = (m.get("question") or "").lower()
                        slug    = (m.get("slug") or "").lower()
                        desc    = (m.get("description") or "").lower()
                        tags_raw = m.get("tags") or m.get("tag") or []
                        if isinstance(tags_raw, str):
                            tags = tags_raw.lower()
                        else:
                            tags = " ".join(
                                (t.get("label") or t.get("name") or t.get("slug") or str(t)).lower()
                                for t in (tags_raw if isinstance(tags_raw, list) else [])
                            )
                        searchable = f"{q} {slug} {desc} {tags}"

                        # Keyword filter (empty keywords = accept all)
                        if keywords and not any(kw.lower() in searchable for kw in keywords):
                            continue

                        # Parse end date from multiple possible fields
                        end_dt = None
                        for field in ("endDate", "endDateIso", "end_date", "expiryDate"):
                            val = m.get(field)
                            if val:
                                try:
                                    if isinstance(val, (int, float)):
                                        end_dt = datetime.fromtimestamp(val, tz=timezone.utc)
                                    else:
                                        end_dt = datetime.fromisoformat(
                                            str(val).replace("Z", "+00:00")
                                        )
                                    break
                                except Exception:
                                    pass

                        # Time filter
                        hours_left: float | None = None
                        if end_dt:
                            hours_left = (end_dt - now).total_seconds() / 3600
                            if hours_left < -1:        # already expired
                                continue
                            if hours_left > 720:       # > 30 days — skip
                                continue
                        # No end_dt = ongoing market (e.g. rolling BTC price) — keep

                        # Parse price from outcomePrices or tokens
                        yes_price = 0.5
                        try:
                            op = m.get("outcomePrices") or []
                            if op and len(op) >= 1:
                                yes_price = float(op[0])
                            elif m.get("bestBid") or m.get("bestAsk"):
                                bid = float(m.get("bestBid") or 0)
                                ask = float(m.get("bestAsk") or 1)
                                yes_price = (bid + ask) / 2
                        except Exception:
                            pass
                        yes_price = max(0.001, min(0.999, yes_price))

                        liquidity = float(
                            m.get("liquidityNum") or m.get("liquidity") or
                            m.get("volume") or 0
                        )
                        volume = float(m.get("volume") or m.get("volumeNum") or 0)

                        results.append({
                            "id": market_id,
                            "question": m.get("question", ""),
                            "category": category,
                            "yes_price": round(yes_price, 4),
                            "no_price": round(1.0 - yes_price, 4),
                            "liquidity": round(liquidity, 0),
                            "volume": round(volume, 0),
                            "end_date": end_dt.isoformat() if end_dt else "",
                            "hours_left": round(hours_left, 1) if hours_left is not None else None,
                            "slug": m.get("slug", ""),
                            "url": f"https://polymarket.com/event/{m.get('slug', market_id)}",
                            "edge_score": _calc_edge_score(yes_price, liquidity, hours_left),
                        })

                    if not cursor or not markets:
                        break
                    await asyncio.sleep(0.3)   # gentle rate limiting

                except Exception as exc:
                    _log_activity("WARN", "scanner", f"页面 {pages} 获取失败: {exc}")
                    break

    except Exception as exc:
        _log_activity("ERROR", "scanner", f"扫描失败 [{category}]: {exc}")

    results.sort(key=lambda x: x["edge_score"], reverse=True)
    _log_activity("INFO", "scanner",
        f"[{category}] 扫描完成: {len(results)} 个市场 (已检查 {len(seen_ids)} 条)")
    return results[:100]


@app.get("/api/markets/scan")
async def scan_markets(category: str = "all", refresh: bool = False):
    """Scan live Polymarket markets. Results cached for 5 min."""
    global _scanner_results
    cats = ["politics", "sports", "esports", "btc"] if category == "all" else [category]
    results = {}
    for cat in cats:
        cache_key = f"{cat}_ts"
        cached_ts = _scanner_results.get(cache_key, 0)
        if not refresh and time.time() - cached_ts < 300:
            results[cat] = _scanner_results.get(cat, [])
        else:
            keywords = CATEGORY_KEYWORDS.get(cat, [])
            # For "all" pass all keywords merged; for specific cat pass just that cat
            markets = await _scan_markets_from_api(cat, keywords)
            _scanner_results[cat] = markets
            _scanner_results[cache_key] = time.time()
            results[cat] = markets
            _log_activity("INFO","scanner",f"[{cat}] 获取到 {len(markets)} 个市场")
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
        except: pass

    # If no DB data, return mock structure showing system is ready
    # Always return status info even if no DB
    status_entry = {
        "whale_address": "", "whale_name": "——",
        "whale_tier": "—", "whale_win_rate": 0.0,
        "action": "none", "market_question":
            "⚠ 需配置Polygon RPC才能显示链上大户数据（当前模式：模拟交易）",
        "amount_usd": 0.0, "price": 0.0,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "follow_enabled": False,
        "is_placeholder": True,
    }
    if not db_whales:
        db_whales = [status_entry]
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
        try: await r.set("control:automation", json.dumps(_automation_config))
        except: pass
    _log_activity("INFO", "automation",
                  f"自动化配置更新: {'启用' if config.enabled else '停用'}",
                  {"agents": config.agents})
    return _automation_config


@app.post("/api/automation/trigger")
async def trigger_automation(background_tasks: BackgroundTasks):
    """Manually trigger a scan + signal generation cycle."""
    background_tasks.add_task(_run_automation_cycle)
    return {"status": "triggered"}


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
