"""
database/repository.py

Repository pattern — one class per aggregate root.
All DB access goes through here; no raw SQL elsewhere.
Uses SQLAlchemy 2.0 async style throughout.
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any

import structlog
from sqlalchemy import and_, desc, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import (
    BacktestRun,
    Market,
    MarketPrice,
    Order,
    PositionSnapshot,
    Signal,
    SystemLog,
    Trade,
    WhaleTrade,
    WhaleProfile,
)

logger = structlog.get_logger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ─── Signal repository ────────────────────────────────────────────────────────

class SignalRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def save(self, signal_data: dict) -> str:
        obj = Signal(**signal_data)
        self._s.add(obj)
        await self._s.flush()
        return obj.id

    async def get_by_id(self, signal_id: str) -> Signal | None:
        result = await self._s.execute(select(Signal).where(Signal.id == signal_id))
        return result.scalar_one_or_none()

    async def get_recent(self, limit: int = 50, mode: str | None = None) -> list[Signal]:
        q = select(Signal).order_by(desc(Signal.created_at)).limit(limit)
        result = await self._s.execute(q)
        return list(result.scalars().all())

    async def get_by_market(self, market_id: str, limit: int = 20) -> list[Signal]:
        q = (
            select(Signal)
            .where(Signal.market_id == market_id)
            .order_by(desc(Signal.created_at))
            .limit(limit)
        )
        result = await self._s.execute(q)
        return list(result.scalars().all())

    async def mark_processed(self, signal_id: str) -> None:
        await self._s.execute(
            update(Signal)
            .where(Signal.id == signal_id)
            .values(status="processed", processed_at=_now())
        )

    async def count_today(self) -> int:
        today_start = _now().replace(hour=0, minute=0, second=0, microsecond=0)
        result = await self._s.execute(
            select(func.count(Signal.id)).where(Signal.created_at >= today_start)
        )
        return result.scalar() or 0


# ─── Order repository ─────────────────────────────────────────────────────────

class OrderRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def save(self, order_data: dict, result_data: dict) -> str:
        """Upsert an order with its fill result."""
        merged = {**order_data, **result_data}
        stmt = pg_insert(Order).values(**merged)
        stmt = stmt.on_conflict_do_update(
            index_elements=["id"],
            set_={
                "fill_price": stmt.excluded.fill_price,
                "fill_size": stmt.excluded.fill_size,
                "fee": stmt.excluded.fee,
                "slippage": stmt.excluded.slippage,
                "status": stmt.excluded.status,
                "filled_at": stmt.excluded.filled_at,
                "error_message": stmt.excluded.error_message,
                "tx_hash": stmt.excluded.tx_hash,
            },
        )
        await self._s.execute(stmt)
        return merged.get("id", "")

    async def get_by_id(self, order_id: str) -> Order | None:
        result = await self._s.execute(select(Order).where(Order.id == order_id))
        return result.scalar_one_or_none()

    async def get_open_orders(self, mode: str) -> list[Order]:
        result = await self._s.execute(
            select(Order)
            .where(and_(Order.status == "pending", Order.mode == mode))
            .order_by(Order.created_at)
        )
        return list(result.scalars().all())


# ─── Trade repository ─────────────────────────────────────────────────────────

class TradeRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def save(self, trade_record: Any) -> str:
        """Upsert a TradeRecord (from execution layer)."""
        data = {
            "id": trade_record.trade_id,
            "order_open_id": trade_record.order_id_open,
            "order_close_id": trade_record.order_id_close,
            "market_id": trade_record.market_id,
            "market_question": trade_record.market_question,
            "strategy_name": trade_record.strategy_name,
            "signal_id": trade_record.signal_id,
            "side": trade_record.side,
            "outcome": trade_record.outcome,
            "entry_price": trade_record.entry_price,
            "exit_price": trade_record.exit_price,
            "size_usd": trade_record.size_usd,
            "realized_pnl": trade_record.realized_pnl,
            "realized_pnl_pct": trade_record.realized_pnl_pct,
            "fee_total": trade_record.fee_total,
            "opened_at": trade_record.opened_at,
            "closed_at": trade_record.closed_at,
            "close_reason": trade_record.close_reason,
            "mode": trade_record.mode,
        }
        stmt = pg_insert(Trade).values(**data)
        stmt = stmt.on_conflict_do_update(
            index_elements=["id"],
            set_={
                "order_close_id": stmt.excluded.order_close_id,
                "exit_price": stmt.excluded.exit_price,
                "realized_pnl": stmt.excluded.realized_pnl,
                "realized_pnl_pct": stmt.excluded.realized_pnl_pct,
                "fee_total": stmt.excluded.fee_total,
                "closed_at": stmt.excluded.closed_at,
                "close_reason": stmt.excluded.close_reason,
            },
        )
        await self._s.execute(stmt)
        return data["id"]

    async def get_open_positions(self, mode: str) -> list[Trade]:
        result = await self._s.execute(
            select(Trade)
            .where(and_(Trade.closed_at.is_(None), Trade.mode == mode))
            .order_by(Trade.opened_at)
        )
        return list(result.scalars().all())

    async def get_daily_trades(self, date: datetime, mode: str) -> list[Trade]:
        day_start = date.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        result = await self._s.execute(
            select(Trade)
            .where(
                and_(
                    Trade.mode == mode,
                    Trade.opened_at >= day_start,
                    Trade.opened_at < day_end,
                )
            )
            .order_by(Trade.opened_at)
        )
        return list(result.scalars().all())

    async def get_performance_stats(
        self, start: datetime, end: datetime, mode: str
    ) -> dict:
        result = await self._s.execute(
            select(
                func.count(Trade.id).label("total"),
                func.sum(Trade.realized_pnl).label("total_pnl"),
                func.avg(Trade.realized_pnl_pct).label("avg_pnl_pct"),
                func.count(Trade.id).filter(Trade.realized_pnl >= 0).label("wins"),
            ).where(
                and_(
                    Trade.mode == mode,
                    Trade.closed_at >= start,
                    Trade.closed_at <= end,
                    Trade.closed_at.is_not(None),
                )
            )
        )
        row = result.one()
        total = row.total or 0
        wins = row.wins or 0
        return {
            "total_trades": total,
            "total_pnl": float(row.total_pnl or 0),
            "avg_pnl_pct": float(row.avg_pnl_pct or 0),
            "wins": wins,
            "losses": total - wins,
            "win_rate": wins / total if total > 0 else 0.0,
        }

    async def get_strategy_stats(
        self, strategy: str, start: datetime, end: datetime
    ) -> dict:
        result = await self._s.execute(
            select(
                func.count(Trade.id).label("total"),
                func.sum(Trade.realized_pnl).label("total_pnl"),
                func.count(Trade.id).filter(Trade.realized_pnl >= 0).label("wins"),
                func.avg(Trade.realized_pnl).label("avg_pnl"),
            ).where(
                and_(
                    Trade.strategy_name == strategy,
                    Trade.closed_at >= start,
                    Trade.closed_at <= end,
                    Trade.closed_at.is_not(None),
                )
            )
        )
        row = result.one()
        total = row.total or 0
        wins = row.wins or 0
        return {
            "strategy": strategy,
            "total_trades": total,
            "total_pnl": float(row.total_pnl or 0),
            "avg_pnl": float(row.avg_pnl or 0),
            "wins": wins,
            "losses": total - wins,
            "win_rate": wins / total if total > 0 else 0.0,
        }

    async def get_all_closed(
        self, start: datetime, end: datetime, mode: str
    ) -> list[Trade]:
        result = await self._s.execute(
            select(Trade)
            .where(
                and_(
                    Trade.mode == mode,
                    Trade.closed_at >= start,
                    Trade.closed_at <= end,
                    Trade.closed_at.is_not(None),
                )
            )
            .order_by(Trade.closed_at)
        )
        return list(result.scalars().all())


# ─── Market repository ────────────────────────────────────────────────────────

class MarketRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def upsert(self, market_data: dict) -> None:
        stmt = pg_insert(Market).values(**market_data)
        stmt = stmt.on_conflict_do_update(
            index_elements=["id"],
            set_={
                "question": stmt.excluded.question,
                "category": stmt.excluded.category,
                "end_date": stmt.excluded.end_date,
                "status": stmt.excluded.status,
                "liquidity": stmt.excluded.liquidity,
                "updated_at": stmt.excluded.updated_at,
            },
        )
        await self._s.execute(stmt)

    async def save_snapshot(self, market_id: str, snapshot: Any) -> None:
        price = MarketPrice(
            market_id=market_id,
            yes_price=snapshot.prices.get("Yes", 0.5),
            no_price=snapshot.prices.get("No", 0.5),
            volume_24h=sum(snapshot.volumes_24h.values()),
            liquidity=snapshot.liquidity,
            spread=snapshot.spread,
            timestamp=snapshot.timestamp,
        )
        self._s.add(price)
        await self._s.flush()

    async def get_price_history(
        self,
        market_id: str,
        start: datetime,
        end: datetime,
    ) -> list[dict]:
        result = await self._s.execute(
            select(MarketPrice)
            .where(
                and_(
                    MarketPrice.market_id == market_id,
                    MarketPrice.timestamp >= start,
                    MarketPrice.timestamp <= end,
                )
            )
            .order_by(MarketPrice.timestamp)
        )
        rows = result.scalars().all()
        return [
            {
                "timestamp": r.timestamp,
                "yes_price": r.yes_price,
                "no_price": r.no_price,
                "liquidity": r.liquidity,
                "spread": r.spread,
            }
            for r in rows
        ]

    async def get_active_markets(self) -> list[Market]:
        result = await self._s.execute(
            select(Market)
            .where(Market.status == "active")
            .order_by(desc(Market.liquidity))
        )
        return list(result.scalars().all())


# ─── Whale repository ─────────────────────────────────────────────────────────

class WhaleRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def upsert_profile(self, profile: Any) -> None:
        data = {
            "address": profile.address,
            "name": profile.name,
            "tier": profile.tier,
            "win_rate": profile.win_rate,
            "total_pnl": profile.total_pnl_usd,
            "total_trades": profile.total_trades,
            "avg_position_size": profile.avg_position_size_usd,
            "updated_at": _now(),
        }
        stmt = pg_insert(WhaleProfile).values(**data)
        stmt = stmt.on_conflict_do_update(
            index_elements=["address"],
            set_={k: stmt.excluded[k] for k in ("name", "tier", "win_rate",
                  "total_pnl", "total_trades", "avg_position_size", "updated_at")},
        )
        await self._s.execute(stmt)

    async def save_trade(self, action: Any) -> None:
        data = {
            "whale_address": action.whale_address,
            "market_id": action.market_id,
            "market_question": action.market_question,
            "action": action.action,
            "outcome": action.outcome,
            "amount_usd": action.amount_usd,
            "price": action.price,
            "tx_hash": action.tx_hash,
            "block_number": action.block_number,
            "timestamp": action.timestamp,
        }
        stmt = pg_insert(WhaleTrade).values(**data)
        stmt = stmt.on_conflict_do_nothing(constraint="uq_whale_trades_tx_hash")
        await self._s.execute(stmt)

    async def get_top_whales(self, tier: str | None = None, limit: int = 20) -> list[WhaleProfile]:
        q = select(WhaleProfile).order_by(desc(WhaleProfile.win_rate)).limit(limit)
        if tier:
            q = q.where(WhaleProfile.tier == tier)
        result = await self._s.execute(q)
        return list(result.scalars().all())

    async def get_recent_actions(self, limit: int = 10) -> list[WhaleTrade]:
        result = await self._s.execute(
            select(WhaleTrade).order_by(desc(WhaleTrade.timestamp)).limit(limit)
        )
        return list(result.scalars().all())


# ─── Backtest repository ──────────────────────────────────────────────────────

class BacktestRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def save_run(self, run_data: dict) -> str:
        obj = BacktestRun(**run_data)
        self._s.add(obj)
        await self._s.flush()
        return obj.id

    async def get_recent_runs(self, limit: int = 10) -> list[BacktestRun]:
        result = await self._s.execute(
            select(BacktestRun).order_by(desc(BacktestRun.created_at)).limit(limit)
        )
        return list(result.scalars().all())


# ─── Log repository ───────────────────────────────────────────────────────────

class LogRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def log(self, level: str, module: str, message: str, extra: dict | None = None) -> None:
        obj = SystemLog(level=level, module=module, message=message, extra=extra or {})
        self._s.add(obj)
        # Don't flush — let natural session commit batch these

    async def get_recent(
        self, level: str | None = None, module: str | None = None, limit: int = 100
    ) -> list[SystemLog]:
        q = select(SystemLog).order_by(desc(SystemLog.timestamp)).limit(limit)
        if level:
            q = q.where(SystemLog.level == level)
        if module:
            q = q.where(SystemLog.module == module)
        result = await self._s.execute(q)
        return list(result.scalars().all())


# ─── Unified Database facade ──────────────────────────────────────────────────

class Database:
    """
    Single injectable DB object used throughout the system.
    Manages session lifecycle and provides access to all repositories.

    Usage:
        db = Database(session_factory)
        await db.save_trade(trade_record)
        stats = await db.get_performance_stats(start, end, "paper")
    """

    def __init__(self, session_factory: Any) -> None:
        self._factory = session_factory
        self._log = logger.bind(component="database")

    async def save_trade(self, trade_record: Any) -> str:
        async with self._factory() as session:
            async with session.begin():
                repo = TradeRepository(session)
                return await repo.save(trade_record)

    async def save_order(self, order: Any, result: Any) -> str:
        async with self._factory() as session:
            async with session.begin():
                repo = OrderRepository(session)
                order_data = {
                    "id": order.order_id,
                    "signal_id": order.signal_id,
                    "strategy_name": order.strategy_name,
                    "market_id": order.market_id,
                    "market_question": order.market_question,
                    "side": order.side,
                    "outcome": order.outcome,
                    "order_type": order.order_type,
                    "requested_price": order.price,
                    "requested_size": order.size_usd,
                    "mode": "paper",
                }
                result_data = {
                    "fill_price": result.fill_price if result.is_filled else None,
                    "fill_size": result.fill_size if result.is_filled else None,
                    "fee": result.fee if result.is_filled else None,
                    "slippage": result.slippage if result.is_filled else None,
                    "status": result.status,
                    "error_message": result.error_message,
                    "tx_hash": result.tx_hash,
                    "filled_at": result.timestamp if result.is_filled else None,
                }
                return await repo.save(order_data, result_data)

    async def save_signal(self, scored_signal: Any) -> str:
        async with self._factory() as session:
            async with session.begin():
                repo = SignalRepository(session)
                sig = scored_signal.signal
                return await repo.save({
                    "id": sig.signal_id,
                    "signal_type": sig.signal_type,
                    "source_type": sig.signal_type.split("_")[0],
                    "market_id": sig.market_id,
                    "market_question": sig.market_question,
                    "direction": sig.direction,
                    "confidence_score": scored_signal.confidence_score,
                    "score_breakdown": scored_signal.score_breakdown,
                    "recommended_action": scored_signal.recommended_action,
                    "status": "pending",
                    "raw_data": sig.to_dict() if hasattr(sig, "to_dict") else {},
                })

    async def get_performance_stats(
        self, start: datetime, end: datetime, mode: str
    ) -> dict:
        async with self._factory() as session:
            repo = TradeRepository(session)
            return await repo.get_performance_stats(start, end, mode)

    async def get_today_trades(self, mode: str) -> list:
        async with self._factory() as session:
            repo = TradeRepository(session)
            return await repo.get_daily_trades(_now(), mode)

    async def get_price_history(
        self, market_id: str, start: datetime, end: datetime
    ) -> list[dict]:
        async with self._factory() as session:
            repo = MarketRepository(session)
            return await repo.get_price_history(market_id, start, end)

    async def get_recent_whale_actions(self, limit: int = 10) -> list:
        async with self._factory() as session:
            repo = WhaleRepository(session)
            return await repo.get_recent_actions(limit)

    async def save_backtest_run(self, result: Any) -> str:
        async with self._factory() as session:
            async with session.begin():
                repo = BacktestRepository(session)
                return await repo.save_run({
                    "start_date": result.start_date,
                    "end_date": result.end_date,
                    "initial_balance": result.initial_balance,
                    "final_balance": result.final_balance,
                    "total_pnl": result.report.total_pnl,
                    "total_pnl_pct": result.report.total_pnl_pct,
                    "sharpe_ratio": result.report.sharpe_ratio,
                    "max_drawdown": result.report.max_drawdown,
                    "win_rate": result.report.win_rate,
                    "total_trades": result.report.total_trades,
                    "parameters": {},
                })

    async def log(self, level: str, module: str, message: str, extra: dict | None = None) -> None:
        try:
            async with self._factory() as session:
                async with session.begin():
                    repo = LogRepository(session)
                    await repo.log(level, module, message, extra)
        except Exception as exc:
            logger.error("db_log_failed", error=str(exc))
