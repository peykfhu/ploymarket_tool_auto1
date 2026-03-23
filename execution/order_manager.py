"""
execution/order_manager.py

OrderManager — unified entry point for all order submission.
Routes to the correct executor, persists results to DB,
updates portfolio state, and triggers Telegram notifications.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Optional

import structlog

from execution.base import BaseExecutor
from execution.models import OrderResult, TradeRecord
from risk.models import RiskDecision
from strategy.models import OrderRequest, Portfolio, Position

logger = structlog.get_logger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class OrderManager:
    """
    Single class that orchestrates the full order lifecycle:
      1. Receive risk-approved OrderRequest
      2. Route to executor (live / paper / backtest)
      3. Persist result to PostgreSQL
      4. Register/deregister positions on the executor
      5. Update RiskState after each trade
      6. Send Telegram notifications
    """

    def __init__(
        self,
        mode: str,
        executor: BaseExecutor,
        db: Any,
        notifier: Any,
        risk_state: Any,
        redis: Any,
        fee_rate: float = 0.02,
    ) -> None:
        self._mode = mode
        self._executor = executor
        self._db = db
        self._notifier = notifier
        self._risk_state = risk_state
        self._redis = redis
        self._fee_rate = fee_rate
        self._log = logger.bind(component="order_manager", mode=mode)

        # market_id:outcome → open TradeRecord (for closing later)
        self._open_trades: dict[str, TradeRecord] = {}

    # ── Primary interface ─────────────────────────────────────────────────────

    async def submit_order(
        self,
        order: OrderRequest,
        risk_decision: RiskDecision,
    ) -> OrderResult:
        """
        Submit an order through the executor. Handles the full post-fill lifecycle.
        Returns the OrderResult regardless of fill status.
        """
        self._log.info(
            "submitting_order",
            order_id=order.order_id,
            strategy=order.strategy_name,
            side=order.side,
            outcome=order.outcome,
            size_usd=order.size_usd,
            market=order.market_question[:60],
        )

        result = await self._executor.place_order(order)

        if result.is_filled:
            await self._on_fill(order, result, risk_decision)
        else:
            self._log.warning(
                "order_not_filled",
                order_id=order.order_id,
                status=result.status,
                error=result.error_message,
            )

        # Always persist order attempt
        if self._db:
            try:
                await self._db.save_order(order, result)
            except Exception as exc:
                self._log.error("db_save_order_failed", error=str(exc))

        return result

    async def close_position(
        self,
        position_id: str,
        reason: str = "manual",
    ) -> OrderResult:
        """Close a specific position by ID."""
        positions = await self._executor.get_positions()
        pos = next((p for p in positions if p.position_id == position_id), None)
        if not pos:
            self._log.warning("close_position_not_found", position_id=position_id)
            return OrderResult.error(position_id, "Position not found")

        close_order = OrderRequest.create(
            signal_id=pos.signal_id,
            strategy_name=pos.strategy_name,
            market_id=pos.market_id,
            market_question=pos.market_question,
            side="sell",
            outcome=pos.outcome,
            order_type="limit",
            price=round(pos.current_price * 0.985, 4),
            size_usd=pos.size_usd,
            position_pct=0.0,
            confidence_score=0,
            take_profit=0.0,
            stop_loss=0.0,
            max_slippage_pct=0.03,
            reasoning=f"Manual close: {reason}",
        )

        from risk.models import RiskDecision as RD
        dummy_decision = RD.approve(close_order)
        return await self.submit_order(close_order, dummy_decision)

    async def close_all_positions(self, reason: str = "circuit_breaker") -> list[OrderResult]:
        """Close all open positions. Called by circuit breaker."""
        self._log.warning("closing_all_positions", reason=reason)
        results = await self._executor.close_all_positions(reason=reason)

        if self._notifier:
            try:
                await self._notifier.send_risk_alert(
                    "critical",
                    f"🛑 All positions closed: {reason}. {len(results)} orders submitted.",
                )
            except Exception:
                pass

        return results

    async def get_portfolio(self) -> Portfolio:
        return await self._executor.get_portfolio()

    # ── Post-fill lifecycle ───────────────────────────────────────────────────

    async def _on_fill(
        self,
        order: OrderRequest,
        result: OrderResult,
        risk_decision: RiskDecision,
    ) -> None:
        """Handle everything that needs to happen after a successful fill."""
        if order.side == "buy":
            await self._on_open(order, result)
        else:
            await self._on_close(order, result)

    async def _on_open(self, order: OrderRequest, result: OrderResult) -> None:
        """Register position, persist trade, notify."""
        # Register with executor for tracking
        if hasattr(self._executor, "register_position"):
            pos = Position.create(
                market_id=order.market_id,
                market_question=order.market_question,
                outcome=order.outcome,
                side="buy",
                entry_price=result.fill_price,
                current_price=result.fill_price,
                size_usd=order.size_usd,
                take_profit=order.take_profit,
                stop_loss=order.stop_loss,
                highest_price_seen=result.fill_price,
                strategy_name=order.strategy_name,
                signal_id=order.signal_id,
                mode=self._mode,
            )
            self._executor.register_position(pos)

        trade = TradeRecord.open(
            order_id=order.order_id,
            market_id=order.market_id,
            market_question=order.market_question,
            strategy_name=order.strategy_name,
            signal_id=order.signal_id,
            side="buy",
            outcome=order.outcome,
            entry_price=result.fill_price,
            size_usd=order.size_usd,
            fee=result.fee,
            mode=self._mode,
        )
        self._open_trades[f"{order.market_id}:{order.outcome}"] = trade

        if self._db:
            try:
                await self._db.save_trade(trade)
            except Exception as exc:
                self._log.error("db_save_trade_failed", error=str(exc))

        if self._notifier:
            try:
                await self._notifier.send_trade_result(result)
            except Exception as exc:
                self._log.warning("notify_trade_failed", error=str(exc))

        self._log.info(
            "position_opened",
            market=order.market_question[:60],
            outcome=order.outcome,
            fill_price=result.fill_price,
            size_usd=order.size_usd,
        )

    async def _on_close(self, order: OrderRequest, result: OrderResult) -> None:
        """Update risk state, persist closed trade, notify."""
        trade_key = f"{order.market_id}:{order.outcome}"
        trade = self._open_trades.pop(trade_key, None)

        if trade:
            trade.close(
                order_id=order.order_id,
                exit_price=result.fill_price,
                fee=result.fee,
                reason=order.reasoning or "unknown",
            )

            pnl_usd = trade.realized_pnl or 0.0
            pnl_pct = trade.realized_pnl_pct or 0.0

            # Update risk state
            try:
                await self._risk_state.update_after_trade(pnl_usd, pnl_pct, self._redis)
            except Exception as exc:
                self._log.error("risk_state_update_failed", error=str(exc))

            if self._db:
                try:
                    await self._db.save_trade(trade)
                except Exception as exc:
                    self._log.error("db_close_trade_failed", error=str(exc))

            self._log.info(
                "position_closed",
                market=order.market_question[:60],
                pnl=round(pnl_usd, 2),
                pnl_pct=round(pnl_pct, 3),
                reason=trade.close_reason,
            )

        # Deregister from executor
        if hasattr(self._executor, "remove_position"):
            positions = await self._executor.get_positions()
            matching = next(
                (p for p in positions
                 if p.market_id == order.market_id and p.outcome == order.outcome),
                None,
            )
            if matching:
                self._executor.remove_position(matching.position_id)

        if self._notifier:
            try:
                await self._notifier.send_trade_result(result)
            except Exception as exc:
                self._log.warning("notify_close_failed", error=str(exc))

    # ── Market data feed (for paper executor) ────────────────────────────────

    def on_market_update(
        self,
        prices: dict[str, dict[str, float]],
        liquidity: dict[str, float],
    ) -> None:
        """Forward live market data to paper executor for mark-to-market."""
        if hasattr(self._executor, "update_market_prices"):
            self._executor.update_market_prices(prices, liquidity)
