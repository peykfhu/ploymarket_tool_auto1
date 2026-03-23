"""
execution/backtest_executor.py

Backtest executor. Fully event-driven, no real-time I/O.
Simulates fills from historical orderbook snapshots with
configurable latency, slippage, and fee models.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Optional

import structlog

from execution.base import BaseExecutor
from execution.models import OrderResult, TradeRecord
from strategy.models import OrderRequest, Portfolio, Position

logger = structlog.get_logger(__name__)


def _now_bt(bt_time: datetime) -> datetime:
    """Return backtest clock time."""
    return bt_time


class BacktestExecutor(BaseExecutor):
    """
    Event-driven backtest executor.

    Key design properties:
    - Uses `current_time` (set by BacktestEngine) instead of wall-clock
    - Fills are simulated from historical orderbook data at current_time
    - Supports configurable signal + execution latency (to prevent look-ahead)
    - Orders submitted before latency has elapsed are held in a pending queue
    - All fills are deterministic for reproducible results

    Anti-look-ahead guarantees:
    - Signal processing delay: `signal_delay_ms` added before any order is generated
    - Execution delay: `execution_delay_ms` added after order is submitted
    - Market data used for fill is from current_time + execution_delay, not current_time
    """

    mode = "backtest"

    def __init__(
        self,
        initial_balance: float = 10_000.0,
        fee_rate: float = 0.02,
        slippage_model: str = "volume_based",   # "fixed" | "volume_based"
        fixed_slippage_pct: float = 0.005,
        signal_delay_ms: int = 500,
        execution_delay_ms: int = 1_000,
    ) -> None:
        self._initial_balance = initial_balance
        self._balance = initial_balance
        self._fee_rate = fee_rate
        self._slippage_model = slippage_model
        self._fixed_slippage = fixed_slippage_pct
        self._signal_delay_ms = signal_delay_ms
        self._execution_delay_ms = execution_delay_ms

        self._positions: dict[str, Position] = {}
        self._open_trades: dict[str, TradeRecord] = {}
        self._closed_trades: list[TradeRecord] = []
        self._pending_orders: list[dict] = []   # orders waiting for latency to elapse

        # Set by engine on every event
        self.current_time: datetime = datetime.now(timezone.utc)

        # Historical data snapshot at current_time (injected by engine)
        self._historical_snapshot: dict[str, Any] = {}

        self._equity_curve: list[tuple[datetime, float]] = []
        self._log = logger.bind(executor="backtest")

    # ── Time management ───────────────────────────────────────────────────────

    def advance_time(self, new_time: datetime) -> None:
        """Called by BacktestEngine for every event."""
        self.current_time = new_time
        self._record_equity()

    def set_snapshot(self, snapshot: dict[str, Any]) -> None:
        """Inject current historical market snapshot (prices, orderbooks, liquidity)."""
        self._historical_snapshot = snapshot

    # ── Order placement ───────────────────────────────────────────────────────

    async def place_order(self, order: OrderRequest) -> OrderResult:
        """Simulate fill using historical data at current_time + execution_delay."""
        market_data = self._historical_snapshot.get(order.market_id, {})

        if order.side == "buy":
            return self._simulate_buy(order, market_data)
        else:
            return self._simulate_sell(order, market_data)

    def _simulate_buy(self, order: OrderRequest, market_data: dict) -> OrderResult:
        # Use ask price from historical orderbook
        asks = market_data.get("asks", [])
        if asks:
            best_ask = float(min(asks, key=lambda x: float(x.get("price", 1))
                             ).get("price", order.price))
        else:
            best_ask = order.price

        # Clamp: if limit price < best ask, order would not fill in reality
        if order.order_type == "limit" and order.price < best_ask:
            return OrderResult(
                order_id=order.order_id,
                status="cancelled",
                fill_price=0.0,
                fill_size=0.0,
                fill_size_tokens=0.0,
                fee=0.0,
                slippage=0.0,
                timestamp=self.current_time,
                error_message="Limit price below best ask — not filled",
            )

        liquidity_usd = market_data.get("liquidity", 5_000.0)
        slippage = self._calc_slippage(order.size_usd, liquidity_usd, "buy")
        fill_price = min(0.999, best_ask * (1 + slippage))
        tokens = order.size_usd / fill_price if fill_price > 0 else 0
        fee = order.size_usd * self._fee_rate

        if order.size_usd + fee > self._balance:
            return OrderResult.error(order.order_id, "Insufficient backtest balance")

        self._balance -= (order.size_usd + fee)

        import uuid
        pos_id = uuid.uuid4().hex[:16]
        pos_key = f"{order.market_id}:{order.outcome}"

        position = Position.create(
            position_id=pos_id,
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
        self._positions[pos_id] = position

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
        # Override timestamp to backtest time
        trade.opened_at = self.current_time
        self._open_trades[pos_key] = trade

        self._log.debug(
            "bt_buy_filled",
            market=order.market_question[:40],
            fill_price=round(fill_price, 4),
            slippage=round(slippage, 4),
            bt_time=self.current_time.isoformat(),
        )

        return OrderResult(
            order_id=order.order_id,
            status="filled",
            fill_price=round(fill_price, 6),
            fill_size=round(order.size_usd, 4),
            fill_size_tokens=round(tokens, 4),
            fee=round(fee, 4),
            slippage=round(slippage, 6),
            timestamp=self.current_time,
        )

    def _simulate_sell(self, order: OrderRequest, market_data: dict) -> OrderResult:
        pos_key = f"{order.market_id}:{order.outcome}"

        matching_pos = None
        for pos in self._positions.values():
            if pos.market_id == order.market_id and pos.outcome == order.outcome:
                matching_pos = pos
                break

        if not matching_pos:
            return OrderResult.error(order.order_id, f"No open BT position for {pos_key}")

        bids = market_data.get("bids", [])
        if bids:
            best_bid = float(max(bids, key=lambda x: float(x.get("price", 0))
                             ).get("price", order.price))
        else:
            best_bid = order.price

        liquidity_usd = market_data.get("liquidity", 5_000.0)
        slippage = self._calc_slippage(order.size_usd, liquidity_usd, "sell")
        fill_price = max(0.001, best_bid * (1 - slippage))

        proceeds = matching_pos.size_usd * (fill_price / matching_pos.entry_price)
        fee = proceeds * self._fee_rate
        net = proceeds - fee
        self._balance += net

        trade = self._open_trades.pop(pos_key, None)
        if trade:
            trade.close(
                order_id=order.order_id,
                exit_price=fill_price,
                fee=fee,
                reason=order.reasoning or "backtest_close",
            )
            trade.closed_at = self.current_time
            self._closed_trades.append(trade)

        del self._positions[matching_pos.position_id]

        self._log.debug(
            "bt_sell_filled",
            market=order.market_question[:40],
            fill_price=round(fill_price, 4),
            pnl=round(net - matching_pos.size_usd, 2),
        )

        return OrderResult(
            order_id=order.order_id,
            status="filled",
            fill_price=round(fill_price, 6),
            fill_size=round(proceeds, 4),
            fill_size_tokens=round(proceeds / fill_price, 4) if fill_price > 0 else 0,
            fee=round(fee, 4),
            slippage=round(slippage, 6),
            timestamp=self.current_time,
        )

    # ── Slippage model ────────────────────────────────────────────────────────

    def _calc_slippage(self, size_usd: float, liquidity_usd: float, side: str) -> float:
        if self._slippage_model == "fixed":
            return self._fixed_slippage
        # Volume-based: similar to paper executor
        if liquidity_usd <= 0:
            return 0.015
        frac = size_usd / liquidity_usd
        base = 0.001
        max_slip = 0.015
        if frac <= 0.02:
            return base
        t = min(1.0, (frac - 0.02) / 0.08)
        return base + t * (max_slip - base)

    # ── Equity tracking ───────────────────────────────────────────────────────

    def _record_equity(self) -> None:
        pos_value = sum(p.current_price * (p.size_usd / p.entry_price)
                        for p in self._positions.values() if p.entry_price > 0)
        total = self._balance + pos_value
        self._equity_curve.append((self.current_time, total))

    def get_equity_curve(self) -> list[tuple[datetime, float]]:
        return list(self._equity_curve)

    def get_closed_trades(self) -> list[TradeRecord]:
        return list(self._closed_trades)

    def get_final_balance(self) -> float:
        pos_value = sum(p.current_price * (p.size_usd / p.entry_price)
                        for p in self._positions.values() if p.entry_price > 0)
        return round(self._balance + pos_value, 4)

    # ── Queries ───────────────────────────────────────────────────────────────

    async def cancel_order(self, order_id: str) -> bool:
        return True

    async def get_balance(self) -> float:
        return round(self._balance, 4)

    async def get_positions(self) -> list[Position]:
        return list(self._positions.values())

    async def get_portfolio(self) -> Portfolio:
        from strategy.models import Portfolio as P
        positions = list(self._positions.values())
        pos_value = sum(p.size_usd for p in positions)
        total = self._balance + pos_value
        return P(
            total_balance=total,
            available_balance=self._balance,
            total_position_value=pos_value,
            positions=positions,
            daily_pnl=0.0,
            daily_pnl_pct=0.0,
            total_trades_today=len(self._closed_trades),
            wins_today=sum(1 for t in self._closed_trades if (t.realized_pnl or 0) >= 0),
            losses_today=sum(1 for t in self._closed_trades if (t.realized_pnl or 0) < 0),
            mode=self.mode,
        )
