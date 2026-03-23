"""
strategy/whale_follow_strategy.py

Three whale-following sub-strategies:
  1. SingleWhaleFollowStrategy  — S-tier whale opens large position → follow after delay
  2. MultiWhaleConsensusStrategy — 3+ S/A whales agree → stronger signal
  3. WhaleFadeStrategy          — fade consistent losers (win_rate < 40%)
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from typing import Any

from data_ingestion.polymarket_collector import MarketSnapshot
from signal_processing.models import ScoredSignal, WhaleSignal
from strategy.base import BaseStrategy
from strategy.models import OrderRequest, Portfolio, Position


class SingleWhaleFollowStrategy(BaseStrategy):
    """
    Follows a single S-tier whale after a confirmation delay.

    Logic:
    1. S-tier whale opens position
    2. Wait `follow_delay_minutes` to confirm price hasn't been pushed
    3. Enter if market hasn't moved more than `max_price_move_pct`
    4. Exit slightly ahead of whale (when their position starts to unwind)
    """

    name = "whale_single_follow"

    def __init__(
        self,
        follow_delay_minutes: int = 10,
        max_price_move_pct: float = 0.05,
        min_whale_win_rate: float = 0.55,
        base_position_pct: float = 0.03,
        tp_pct: float = 0.20,
        sl_pct: float = 0.10,
    ) -> None:
        super().__init__()
        self._delay_s = follow_delay_minutes * 60
        self._max_move = max_price_move_pct
        self._min_wr = min_whale_win_rate
        self._base_pct = base_position_pct
        self._tp_pct = tp_pct
        self._sl_pct = sl_pct

        # market_id → (signal, entry_price_at_signal_time, queued_at_monotonic)
        self._queued: dict[str, tuple[WhaleSignal, float, float]] = {}

    def should_handle(self, signal: ScoredSignal) -> bool:
        return (
            self.enabled
            and isinstance(signal.signal, WhaleSignal)
            and signal.signal.signal_type == "whale_follow"
        )

    async def evaluate(
        self, signal: ScoredSignal, portfolio: Portfolio, risk_state: Any
    ) -> OrderRequest | None:
        if not isinstance(signal.signal, WhaleSignal):
            return None

        sig = signal.signal

        # Only follow S-tier whales in single-follow mode
        if sig.consensus_tier != "S":
            return None
        if sig.avg_whale_win_rate < self._min_wr:
            return None
        if sig.consensus_count > 1:
            return None  # MultiWhale strategy handles this

        # Queue the signal for delayed execution
        market_id = sig.market_id
        # We need the current market price — approximated from the whale action price
        entry_ref = sig.whale_actions[-1].price if sig.whale_actions else 0.5
        self._queued[market_id] = (sig, entry_ref, time.monotonic())
        self._log.info(
            "whale_follow_queued",
            market=sig.market_question[:60],
            delay_min=self._delay_s / 60,
        )
        return None  # actual order placed via check_queued()

    async def check_queued(
        self, market_snapshots: dict[str, MarketSnapshot], portfolio: Portfolio
    ) -> list[OrderRequest]:
        """
        Called every 30s by orchestrator.
        Executes queued follow orders whose delay has elapsed.
        """
        now = time.monotonic()
        orders: list[OrderRequest] = []
        to_remove: list[str] = []

        for market_id, (sig, ref_price, queued_at) in self._queued.items():
            age_s = now - queued_at

            # Expire after 2× delay
            if age_s > self._delay_s * 2:
                to_remove.append(market_id)
                self._log.debug("whale_follow_expired", market=market_id)
                continue

            if age_s < self._delay_s:
                continue  # not yet time

            # Check if price moved too much
            snap = market_snapshots.get(market_id)
            if snap is None:
                continue

            current_price = snap.prices.get("Yes", ref_price)
            price_move = abs(current_price - ref_price) / max(ref_price, 0.01)

            if price_move > self._max_move:
                self._log.info(
                    "whale_follow_skipped_price_moved",
                    market=market_id,
                    move=round(price_move, 3),
                )
                to_remove.append(market_id)
                continue

            # Already in this market?
            outcome = "Yes" if sig.direction == "buy_yes" else "No"
            if portfolio.get_position(market_id, outcome):
                to_remove.append(market_id)
                continue

            # Place order
            direction = sig.direction
            tp, sl = self._tp_sl_from_price(current_price, direction, self._tp_pct, self._sl_pct)
            size_pct = self._base_pct
            size_usd = portfolio.total_balance * size_pct

            self._log.info(
                "whale_follow_executing",
                market=sig.market_question[:60],
                delay_s=round(age_s),
                price_move=round(price_move, 3),
            )

            orders.append(OrderRequest.create(
                signal_id=sig.signal_id,
                strategy_name=self.name,
                market_id=market_id,
                market_question=sig.market_question,
                side="buy",
                outcome=outcome,
                price=round(current_price * 1.01, 4),
                size_usd=round(size_usd, 2),
                position_pct=size_pct,
                confidence_score=75,
                take_profit=tp,
                stop_loss=sl,
                reasoning=(
                    f"Whale follow: S-tier whale {sig.whale_actions[-1].whale_name} "
                    f"placed ${sig.total_whale_usd:,.0f}. "
                    f"Followed after {age_s/60:.0f}min delay. Price moved {price_move:.1%}."
                ),
            ))
            to_remove.append(market_id)

        for k in to_remove:
            del self._queued[k]

        return orders

    async def manage_position(
        self, position: Position, market_data: MarketSnapshot
    ) -> OrderRequest | None:
        current = market_data.prices.get(position.outcome, position.current_price)
        position.update_price(current)

        pnl_pct = position.unrealized_pnl_pct

        # Trailing stop: move to breakeven at 20% profit
        if pnl_pct > 0.20 and position.trailing_stop is None:
            position.trailing_stop = position.entry_price

        # Trailing: move stop to 20% if profit > 40%
        if pnl_pct > 0.40 and position.trailing_stop is not None:
            new_stop = position.entry_price * 1.20
            if new_stop > position.trailing_stop:
                position.trailing_stop = new_stop

        if position.side == "buy":
            stop = position.trailing_stop or position.stop_loss
            if current <= stop:
                return self._close(position, current, "stop_or_trailing")
            if current >= position.take_profit:
                return self._close(position, current, "take_profit")
        return None

    def _close(self, position: Position, price: float, reason: str) -> OrderRequest:
        return OrderRequest.create(
            signal_id=position.signal_id,
            strategy_name=self.name,
            market_id=position.market_id,
            market_question=position.market_question,
            side="sell",
            outcome=position.outcome,
            price=round(price * 0.99, 4),
            size_usd=position.size_usd,
            position_pct=0.0,
            confidence_score=0,
            take_profit=0.0,
            stop_loss=0.0,
            reasoning=f"Whale follow close: {reason}",
        )


class MultiWhaleConsensusStrategy(BaseStrategy):
    """
    Triggers when 3+ S/A-tier whales agree on direction within 48 hours.
    Checks for any opposing S/A whale — if found, skips.
    Larger position size than single-follow.
    """

    name = "whale_consensus"

    def __init__(
        self,
        min_consensus: int = 3,
        min_tier: str = "A",            # "S" or "A"
        min_avg_win_rate: float = 0.55,
        base_position_pct: float = 0.04,
        tp_pct: float = 0.25,
        sl_pct: float = 0.12,
    ) -> None:
        super().__init__()
        self._min_consensus = min_consensus
        self._min_tier = min_tier
        self._min_wr = min_avg_win_rate
        self._base_pct = base_position_pct
        self._tp_pct = tp_pct
        self._sl_pct = sl_pct

        # Track opposing whales to detect conflict
        # market_id → {direction → set of whale addresses}
        self._whale_positions: dict[str, dict[str, set[str]]] = defaultdict(
            lambda: {"buy_yes": set(), "buy_no": set()}
        )

    def should_handle(self, signal: ScoredSignal) -> bool:
        return (
            self.enabled
            and isinstance(signal.signal, WhaleSignal)
            and signal.signal.signal_type == "whale_consensus"
        )

    async def evaluate(
        self, signal: ScoredSignal, portfolio: Portfolio, risk_state: Any
    ) -> OrderRequest | None:
        if not isinstance(signal.signal, WhaleSignal):
            return None

        sig = signal.signal

        # Minimum consensus count
        if sig.consensus_count < self._min_consensus:
            return None

        # Win rate gate
        if sig.avg_whale_win_rate < self._min_wr:
            return None

        # Tier check: all consensus whales should be at least min_tier
        tier_order = {"S": 0, "A": 1, "B": 2}
        if tier_order.get(sig.consensus_tier, 3) > tier_order.get(self._min_tier, 1):
            return None

        # Opposing whale check
        market_id = sig.market_id
        direction = sig.direction
        opposite = "buy_no" if direction == "buy_yes" else "buy_yes"

        # Update tracking
        for action in sig.whale_actions:
            self._whale_positions[market_id][direction].add(action.whale_address)

        opposing_addresses = self._whale_positions[market_id][opposite]
        if opposing_addresses:
            self._log.info(
                "whale_consensus_blocked_opposing_whale",
                market=sig.market_question[:60],
                opposing_count=len(opposing_addresses),
            )
            return None

        # Already in this market?
        outcome = "Yes" if direction == "buy_yes" else "No"
        if portfolio.get_position(market_id, outcome):
            return None

        # Price approximation from latest action
        entry = sig.whale_actions[-1].price if sig.whale_actions else 0.5
        if entry <= 0.01 or entry >= 0.99:
            entry = 0.5  # fallback

        tp, sl = self._tp_sl_from_price(entry, direction, self._tp_pct, self._sl_pct)
        size_pct = self._scale_size(self._base_pct, signal.confidence_score, max_pct=0.07)
        size_usd = portfolio.total_balance * size_pct

        self._log.info(
            "whale_consensus_order",
            count=sig.consensus_count,
            tier=sig.consensus_tier,
            total_usd=sig.total_whale_usd,
            market=sig.market_question[:60],
        )

        return OrderRequest.create(
            signal_id=sig.signal_id,
            strategy_name=self.name,
            market_id=market_id,
            market_question=sig.market_question,
            side="buy",
            outcome=outcome,
            price=round(entry * 1.01, 4),
            size_usd=round(size_usd, 2),
            position_pct=size_pct,
            confidence_score=signal.confidence_score,
            take_profit=tp,
            stop_loss=sl,
            reasoning=(
                f"Whale consensus: {sig.consensus_count} {self._min_tier}+ tier whales, "
                f"${sig.total_whale_usd:,.0f} total. "
                f"Avg win rate: {sig.avg_whale_win_rate:.0%}."
            ),
        )

    async def manage_position(
        self, position: Position, market_data: MarketSnapshot
    ) -> OrderRequest | None:
        current = market_data.prices.get(position.outcome, position.current_price)
        position.update_price(current)

        if position.unrealized_pnl_pct >= 0.25:
            return self._close(position, current, "consensus_target")
        if current <= position.stop_loss:
            return self._close(position, current, "stop_loss")
        return None

    def _close(self, position: Position, price: float, reason: str) -> OrderRequest:
        return OrderRequest.create(
            signal_id=position.signal_id,
            strategy_name=self.name,
            market_id=position.market_id,
            market_question=position.market_question,
            side="sell",
            outcome=position.outcome,
            price=round(price * 0.99, 4),
            size_usd=position.size_usd,
            position_pct=0.0,
            confidence_score=0,
            take_profit=0.0,
            stop_loss=0.0,
            reasoning=f"Whale consensus close: {reason}",
        )


class WhaleFadeStrategy(BaseStrategy):
    """
    Fades consistent losers: whales with historical win rate < 40%.
    Takes the opposite side of their large positions.
    """

    name = "whale_fade"

    def __init__(
        self,
        max_whale_win_rate: float = 0.40,
        min_position_usd: float = 10_000.0,
        base_position_pct: float = 0.02,
        tp_pct: float = 0.15,
        sl_pct: float = 0.08,
    ) -> None:
        super().__init__()
        self._max_wr = max_whale_win_rate
        self._min_usd = min_position_usd
        self._base_pct = base_position_pct
        self._tp_pct = tp_pct
        self._sl_pct = sl_pct

    def should_handle(self, signal: ScoredSignal) -> bool:
        return (
            self.enabled
            and isinstance(signal.signal, WhaleSignal)
        )

    async def evaluate(
        self, signal: ScoredSignal, portfolio: Portfolio, risk_state: Any
    ) -> OrderRequest | None:
        if not isinstance(signal.signal, WhaleSignal):
            return None

        sig = signal.signal

        if sig.avg_whale_win_rate > self._max_wr:
            return None  # not a loser whale
        if sig.total_whale_usd < self._min_usd:
            return None  # too small to bother fading

        # Fade: take opposite direction
        direction = "buy_no" if sig.direction == "buy_yes" else "buy_yes"
        outcome = "Yes" if direction == "buy_yes" else "No"

        entry = sig.whale_actions[-1].price if sig.whale_actions else 0.5
        if entry <= 0.01 or entry >= 0.99:
            entry = 0.5

        tp, sl = self._tp_sl_from_price(entry, direction, self._tp_pct, self._sl_pct)
        size_usd = portfolio.total_balance * self._base_pct

        self._log.info(
            "whale_fade_order",
            whale_wr=sig.avg_whale_win_rate,
            fading_direction=sig.direction,
            market=sig.market_question[:60],
        )

        return OrderRequest.create(
            signal_id=sig.signal_id,
            strategy_name=self.name,
            market_id=sig.market_id,
            market_question=sig.market_question,
            side="buy",
            outcome=outcome,
            price=round(entry * (1.01 if direction == "buy_yes" else 0.99), 4),
            size_usd=round(size_usd, 2),
            position_pct=self._base_pct,
            confidence_score=signal.confidence_score,
            take_profit=tp,
            stop_loss=sl,
            reasoning=(
                f"Whale fade: avg win rate {sig.avg_whale_win_rate:.0%} "
                f"(threshold {self._max_wr:.0%}). "
                f"Fading ${sig.total_whale_usd:,.0f} in {sig.direction}."
            ),
        )

    async def manage_position(
        self, position: Position, market_data: MarketSnapshot
    ) -> OrderRequest | None:
        current = market_data.prices.get(position.outcome, position.current_price)
        position.update_price(current)

        if position.unrealized_pnl_pct >= 0.15 or current <= position.stop_loss:
            return OrderRequest.create(
                signal_id=position.signal_id,
                strategy_name=self.name,
                market_id=position.market_id,
                market_question=position.market_question,
                side="sell",
                outcome=position.outcome,
                price=round(current * 0.99, 4),
                size_usd=position.size_usd,
                position_pct=0.0,
                confidence_score=0,
                take_profit=0.0,
                stop_loss=0.0,
                reasoning="Whale fade: exit",
            )
        return None
