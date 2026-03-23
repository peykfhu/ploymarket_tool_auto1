"""
risk/stop_manager.py

Dynamic TP/SL manager. Called every 30 seconds to check all open positions.
Implements fixed stop-loss, trailing stop, and time-based stops.
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any

import structlog

from data_ingestion.polymarket_collector import MarketSnapshot
from strategy.models import OrderRequest, Portfolio, Position

logger = structlog.get_logger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class StopManager:
    """
    Checks all positions every 30 seconds and emits close orders when
    stop conditions are triggered.

    Dynamic trailing stop logic:
      profit > 20% → move stop to breakeven (entry price)
      profit > 40% → move stop to +20%
      profit > 60% → move stop to +40%
      continues trailing at -20% of highest_price_seen thereafter
    """

    def __init__(
        self,
        trailing_activation_pct: float = 0.20,
        trailing_step_pct: float = 0.20,
        default_time_stop_hours: float = 48.0,
        min_position_age_for_time_stop_hours: float = 2.0,
    ) -> None:
        self._trailing_activation = trailing_activation_pct
        self._trailing_step = trailing_step_pct
        self._time_stop_hours = default_time_stop_hours
        self._min_age_h = min_position_age_for_time_stop_hours
        self._log = logger.bind(component="stop_manager")

    async def check_all_positions(
        self,
        portfolio: Portfolio,
        market_data: dict[str, MarketSnapshot],
    ) -> list[OrderRequest]:
        """
        Iterate all open positions and return any close orders to execute.
        """
        close_orders: list[OrderRequest] = []

        for position in portfolio.positions:
            snap = market_data.get(position.market_id)
            if not snap:
                continue

            current_price = snap.prices.get(position.outcome, position.current_price)
            position.update_price(current_price)

            # Update trailing stop first
            self._update_trailing_stop(position, current_price)

            order = self._check_triggers(position, current_price)
            if order:
                close_orders.append(order)
                self._log.info(
                    "stop_triggered",
                    position_id=position.position_id,
                    market=position.market_question[:60],
                    reason=order.reasoning,
                    pnl_pct=round(position.unrealized_pnl_pct, 3),
                )

        return close_orders

    def _update_trailing_stop(self, position: Position, current_price: float) -> None:
        """
        Ratchet the trailing stop upward as profit grows.
        Only activates after `trailing_activation_pct` profit.
        """
        if position.side != "buy":
            return

        pnl_pct = position.unrealized_pnl_pct
        entry = position.entry_price

        # Activation: first time profit exceeds 20%
        if pnl_pct >= self._trailing_activation and position.trailing_stop is None:
            position.trailing_stop = entry    # move to breakeven
            self._log.debug("trailing_stop_breakeven", position_id=position.position_id)

        if position.trailing_stop is None:
            return

        # Progressive ratchet based on current pnl
        if pnl_pct >= 0.60:
            # Trail at entry + 40%
            new_stop = entry * 1.40
        elif pnl_pct >= 0.40:
            # Trail at entry + 20%
            new_stop = entry * 1.20
        else:
            # Trail at breakeven (already set)
            new_stop = entry

        # Also trail at 20% below highest price ever seen
        high_based_stop = position.highest_price_seen * (1 - self._trailing_step)
        new_stop = max(new_stop, high_based_stop)

        # Only move stop UP (never down)
        if new_stop > position.trailing_stop:
            position.trailing_stop = round(new_stop, 4)
            self._log.debug(
                "trailing_stop_updated",
                position_id=position.position_id,
                new_stop=position.trailing_stop,
                pnl_pct=round(pnl_pct, 3),
            )

    def _check_triggers(self, position: Position, current_price: float) -> OrderRequest | None:
        """Check all stop conditions and return close order if any triggered."""

        if position.side == "buy":
            # Fixed take-profit
            if self._check_fixed_take_profit(position, current_price):
                return self._close(position, current_price, "take_profit")

            # Trailing stop
            if self._check_trailing_stop(position, current_price):
                return self._close(position, current_price, "trailing_stop")

            # Fixed stop-loss (only if no trailing active — trailing is already lower)
            if position.trailing_stop is None and self._check_fixed_stop_loss(position, current_price):
                return self._close(position, current_price, "stop_loss")

        elif position.side == "sell":
            # For short positions (buying NO): price rising is bad
            if current_price >= position.stop_loss:
                return self._close(position, current_price, "stop_loss")
            if current_price <= position.take_profit:
                return self._close(position, current_price, "take_profit")

        # Time stop
        if self._check_time_stop(position):
            return self._close(position, current_price, "time_stop")

        return None

    def _check_fixed_stop_loss(self, position: Position, price: float) -> bool:
        return position.stop_loss > 0 and price <= position.stop_loss

    def _check_fixed_take_profit(self, position: Position, price: float) -> bool:
        return position.take_profit > 0 and price >= position.take_profit

    def _check_trailing_stop(self, position: Position, price: float) -> bool:
        return (
            position.trailing_stop is not None
            and price <= position.trailing_stop
        )

    def _check_time_stop(self, position: Position) -> bool:
        age_h = (_now() - position.opened_at).total_seconds() / 3600
        # Only apply time stop after minimum age and beyond max hold
        return (
            age_h >= self._min_age_h
            and age_h >= self._time_stop_hours
        )

    def _close(self, position: Position, price: float, reason: str) -> OrderRequest:
        # For buy positions: sell the outcome token back
        # Limit price slightly below current to ensure fill
        sell_price = round(price * 0.985, 4)

        return OrderRequest.create(
            signal_id=position.signal_id,
            strategy_name=position.strategy_name,
            market_id=position.market_id,
            market_question=position.market_question,
            side="sell",
            outcome=position.outcome,
            order_type="limit",
            price=sell_price,
            size_usd=position.size_usd,
            position_pct=0.0,
            confidence_score=0,
            take_profit=0.0,
            stop_loss=0.0,
            reasoning=f"Stop manager: {reason} at {price:.4f} "
                      f"(pnl: {position.unrealized_pnl_pct:+.1%})",
        )
