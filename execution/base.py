"""
execution/base.py
Abstract base class shared by live, paper, and backtest executors.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from execution.models import OrderResult
from strategy.models import OrderRequest, Portfolio, Position


class BaseExecutor(ABC):
    mode: str = "base"

    @abstractmethod
    async def place_order(self, order: OrderRequest) -> OrderResult:
        """Submit an order and return the fill result."""

    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel a pending order. Returns True if cancelled."""

    @abstractmethod
    async def get_positions(self) -> list[Position]:
        """Return all currently open positions."""

    @abstractmethod
    async def get_balance(self) -> float:
        """Return available USDC balance."""

    @abstractmethod
    async def get_portfolio(self) -> Portfolio:
        """Return full portfolio snapshot."""

    async def close_all_positions(self, reason: str = "manual") -> list[OrderResult]:
        """Close all open positions at market. Used by circuit breaker."""
        results: list[OrderResult] = []
        positions = await self.get_positions()
        for pos in positions:
            close_order = OrderRequest.create(
                signal_id=pos.signal_id,
                strategy_name=pos.strategy_name,
                market_id=pos.market_id,
                market_question=pos.market_question,
                side="sell",
                outcome=pos.outcome,
                order_type="market",
                price=pos.current_price * 0.97,
                size_usd=pos.size_usd,
                position_pct=0.0,
                confidence_score=0,
                take_profit=0.0,
                stop_loss=0.0,
                max_slippage_pct=0.05,
                reasoning=f"close_all: {reason}",
            )
            result = await self.place_order(close_order)
            results.append(result)
        return results
