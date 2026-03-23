"""
execution/paper_executor.py

Paper trading executor. Simulates fills using real-time orderbook data
with realistic slippage and fee modelling. No real orders are placed.
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import structlog

from execution.base import BaseExecutor
from execution.models import OrderResult, TradeRecord
from strategy.models import OrderRequest, Portfolio, Position

logger = structlog.get_logger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _new_id() -> str:
    return uuid.uuid4().hex[:16]


class PaperExecutor(BaseExecutor):
    """
    Simulates live trading without placing real orders.

    Slippage model:
      - If order size <= 2% of available liquidity → minimal slippage (0.1%)
      - If 2-10% of liquidity → linear interpolation up to 1.5%
      - If > 10% of liquidity → 1.5% + extra for market impact

    P&L tracks in-memory and is updated each time market data arrives.
    All positions and trades are persisted to the database for analysis.
    """

    mode = "paper"

    def __init__(
        self,
        initial_balance: float = 10_000.0,
        fee_rate: float = 0.02,
        base_slippage: float = 0.001,
        max_slippage: float = 0.015,
        simulated_latency_ms: int = 200,
        db: Any = None,
    ) -> None:
        self._balance = initial_balance
        self._initial_balance = initial_balance
        self._fee_rate = fee_rate
        self._base_slippage = base_slippage
        self._max_slippage = max_slippage
        self._latency_ms = simulated_latency_ms
        self._db = db

        self._positions: dict[str, Position] = {}          # position_id → Position
        self._open_trades: dict[str, TradeRecord] = {}     # market_id:outcome → TradeRecord
        self._closed_trades: list[TradeRecord] = []
        self._daily_pnl: float = 0.0
        self._total_trades_today: int = 0
        self._wins_today: int = 0
        self._losses_today: int = 0

        # Latest market snapshots for slippage estimation
        self._market_liquidity: dict[str, float] = {}

        self._log = logger.bind(executor="paper")

    # ── Order placement ───────────────────────────────────────────────────────

    async def place_order(self, order: OrderRequest) -> OrderResult:
        # Simulate network latency
        if self._latency_ms > 0:
            await asyncio.sleep(self._latency_ms / 1000)

        self._log.info(
            "paper_order",
            order_id=order.order_id,
            side=order.side,
            outcome=order.outcome,
            size_usd=order.size_usd,
            price=order.price,
            market=order.market_question[:60],
        )

        if order.side == "buy":
            return await self._simulate_buy(order)
        else:
            return await self._simulate_sell(order)

    async def _simulate_buy(self, order: OrderRequest) -> OrderResult:
        cost = order.size_usd
        if cost > self._balance:
            return OrderResult.error(order.order_id, f"Insufficient paper balance: ${self._balance:.2f}")

        # Slippage
        liquidity = self._market_liquidity.get(order.market_id, 10_000.0)
        slippage = self._estimate_slippage(order.size_usd, liquidity)
        fill_price = min(0.999, order.price * (1 + slippage))
        actual_cost = order.size_usd
        fee = actual_cost * self._fee_rate
        tokens_received = actual_cost / fill_price

        self._balance -= (actual_cost + fee)

        # Create position
        pos_key = f"{order.market_id}:{order.outcome}"
        position = Position.create(
            position_id=_new_id(),
            market_id=order.market_id,
            market_question=order.market_question,
            outcome=order.outcome,
            side="buy",
            entry_price=fill_price,
            current_price=fill_price,
            size_usd=order.size_usd,
            take_profit=order.take_profit,
            stop_loss=order.stop_loss,
            highest_price_seen=fill_price,
            strategy_name=order.strategy_name,
            signal_id=order.signal_id,
            mode=self.mode,
        )
        self._positions[position.position_id] = position

        # Track trade
        trade = TradeRecord.open(
            order_id=order.order_id,
            market_id=order.market_id,
            market_question=order.market_question,
            strategy_name=order.strategy_name,
            signal_id=order.signal_id,
            side="buy",
            outcome=order.outcome,
            entry_price=fill_price,
            size_usd=order.size_usd,
            fee=fee,
            mode=self.mode,
        )
        self._open_trades[pos_key] = trade

        if self._db:
            await self._db.save_trade(trade)

        self._log.info(
            "paper_buy_filled",
            fill_price=round(fill_price, 4),
            slippage=round(slippage, 4),
            fee=round(fee, 2),
            balance_after=round(self._balance, 2),
        )

        return OrderResult(
            order_id=order.order_id,
            status="filled",
            fill_price=round(fill_price, 6),
            fill_size=round(actual_cost, 4),
            fill_size_tokens=round(tokens_received, 4),
            fee=round(fee, 4),
            slippage=round(slippage, 6),
            timestamp=_now(),
        )

    async def _simulate_sell(self, order: OrderRequest) -> OrderResult:
        pos_key = f"{order.market_id}:{order.outcome}"

        # Find matching position
        matching_pos = None
        for pos in self._positions.values():
            if pos.market_id == order.market_id and pos.outcome == order.outcome:
                matching_pos = pos
                break

        if not matching_pos:
            return OrderResult.error(order.order_id, f"No open position for {pos_key}")

        liquidity = self._market_liquidity.get(order.market_id, 10_000.0)
        slippage = self._estimate_slippage(order.size_usd, liquidity)
        fill_price = max(0.001, order.price * (1 - slippage))
        proceeds = order.size_usd * (fill_price / matching_pos.entry_price)
        fee = proceeds * self._fee_rate
        net_proceeds = proceeds - fee

        self._balance += net_proceeds

        # Realise P&L
        pnl = net_proceeds - matching_pos.size_usd
        pnl_pct = pnl / matching_pos.size_usd if matching_pos.size_usd > 0 else 0

        self._daily_pnl += pnl
        self._total_trades_today += 1
        if pnl >= 0:
            self._wins_today += 1
        else:
            self._losses_today += 1

        # Close trade record
        trade = self._open_trades.pop(pos_key, None)
        if trade:
            trade.close(
                order_id=order.order_id,
                exit_price=fill_price,
                fee=fee,
                reason=order.reasoning or "manual",
            )
            self._closed_trades.append(trade)
            if self._db:
                await self._db.save_trade(trade)

        # Remove position
        del self._positions[matching_pos.position_id]

        self._log.info(
            "paper_sell_filled",
            fill_price=round(fill_price, 4),
            pnl=round(pnl, 2),
            pnl_pct=round(pnl_pct, 3),
            balance_after=round(self._balance, 2),
        )

        return OrderResult(
            order_id=order.order_id,
            status="filled",
            fill_price=round(fill_price, 6),
            fill_size=round(proceeds, 4),
            fill_size_tokens=round(proceeds / fill_price, 4),
            fee=round(fee, 4),
            slippage=round(slippage, 6),
            timestamp=_now(),
        )

    # ── Slippage model ────────────────────────────────────────────────────────

    def _estimate_slippage(self, order_size_usd: float, market_liquidity_usd: float) -> float:
        """
        Volume-impact slippage model.
          size_fraction = order_size / market_liquidity
          slippage = base + (max - base) * min(1, size_fraction / 0.10)
        """
        if market_liquidity_usd <= 0:
            return self._max_slippage
        size_fraction = order_size_usd / market_liquidity_usd
        if size_fraction <= 0.02:
            return self._base_slippage
        t = min(1.0, (size_fraction - 0.02) / 0.08)
        return self._base_slippage + t * (self._max_slippage - self._base_slippage)

    # ── Market data updates ───────────────────────────────────────────────────

    def update_market_prices(self, prices: dict[str, dict[str, float]], liquidity: dict[str, float]) -> None:
        """
        Called by order manager when fresh market data arrives.
        Updates position mark-to-market and liquidity for slippage estimates.
        """
        self._market_liquidity.update(liquidity)
        for pos in self._positions.values():
            market_prices = prices.get(pos.market_id, {})
            current = market_prices.get(pos.outcome, pos.current_price)
            pos.update_price(current)

    # ── Queries ───────────────────────────────────────────────────────────────

    async def cancel_order(self, order_id: str) -> bool:
        return True   # paper orders are always cancellable instantly

    async def get_balance(self) -> float:
        return round(self._balance, 4)

    async def get_positions(self) -> list[Position]:
        return list(self._positions.values())

    async def get_portfolio(self) -> Portfolio:
        from strategy.models import Portfolio as P
        positions = list(self._positions.values())
        pos_value = sum(p.size_usd for p in positions)
        total = self._balance + pos_value
        daily_pnl_pct = self._daily_pnl / self._initial_balance if self._initial_balance > 0 else 0
        return P(
            total_balance=total,
            available_balance=self._balance,
            total_position_value=pos_value,
            positions=positions,
            daily_pnl=round(self._daily_pnl, 4),
            daily_pnl_pct=round(daily_pnl_pct, 4),
            total_trades_today=self._total_trades_today,
            wins_today=self._wins_today,
            losses_today=self._losses_today,
            mode=self.mode,
        )

    def get_closed_trades(self) -> list[TradeRecord]:
        return list(self._closed_trades)

    def reset_daily_stats(self) -> None:
        self._daily_pnl = 0.0
        self._total_trades_today = 0
        self._wins_today = 0
        self._losses_today = 0
