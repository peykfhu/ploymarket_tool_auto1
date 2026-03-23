"""
strategy/sports_endgame_strategy.py

Three sports endgame sub-strategies:
  1. SafeLockStrategy      — large lead + little time + underpriced market
  2. ReversalCatchStrategy — trailing team, market overly pessimistic
  3. LiveOverUnderStrategy — pace-of-play O/U opportunities
"""

from __future__ import annotations

from typing import Any

from data_ingestion.polymarket_collector import MarketSnapshot
from signal_processing.models import ScoredSignal, SportsSignal
from strategy.base import BaseStrategy
from strategy.models import OrderRequest, Portfolio, Position


class SafeLockStrategy(BaseStrategy):
    """
    Triggered when:
      - signal_type == "sports_safe_lock"
      - Large lead (≥2 goals or ≥10pts) with little time remaining (≤15 min)
      - Model probability ≥ 85%
      - Market price still leaves ≥8% upside

    Enters all at once (no scale-in — endgame leaves no time).
    Holds until market settlement.
    """

    name = "sports_safe_lock"

    def __init__(
        self,
        min_model_prob: float = 0.85,
        max_remaining_minutes: int = 15,
        base_position_pct: float = 0.04,
        tp_pct: float = 0.30,
        sl_pct: float = 0.12,
    ) -> None:
        super().__init__()
        self._min_prob = min_model_prob
        self._max_remaining = max_remaining_minutes
        self._base_pct = base_position_pct
        self._tp_pct = tp_pct
        self._sl_pct = sl_pct

    def should_handle(self, signal: ScoredSignal) -> bool:
        return (
            self.enabled
            and isinstance(signal.signal, SportsSignal)
            and "safe_lock" in signal.signal.signal_type
        )

    async def evaluate(
        self, signal: ScoredSignal, portfolio: Portfolio, risk_state: Any
    ) -> OrderRequest | None:
        if not isinstance(signal.signal, SportsSignal):
            return None

        sig = signal.signal

        # Hard guards
        if sig.calculated_probability < self._min_prob:
            self._log.debug("safe_lock_prob_too_low", prob=sig.calculated_probability)
            return None
        if sig.sport_event.remaining_minutes > self._max_remaining:
            self._log.debug("safe_lock_too_much_time", remaining=sig.sport_event.remaining_minutes)
            return None
        if sig.edge < 0.08:
            self._log.debug("safe_lock_edge_too_small", edge=sig.edge)
            return None

        # Check if we already have a position in this market
        existing = portfolio.get_position(sig.market_id, "Yes" if sig.direction == "buy_yes" else "No")
        if existing:
            return None  # already in this market

        size_pct = self._scale_size(self._base_pct, signal.confidence_score, max_pct=0.07)
        size_usd = portfolio.total_balance * size_pct

        direction = sig.direction
        outcome = "Yes" if direction == "buy_yes" else "No"
        entry = sig.market_price
        tp, sl = self._tp_sl_from_price(entry, direction, self._tp_pct, self._sl_pct)

        evt = sig.sport_event
        self._log.info(
            "safe_lock_order",
            match=f"{evt.home_team} {evt.home_score}-{evt.away_score} {evt.away_team}",
            remaining=evt.remaining_minutes,
            prob=sig.calculated_probability,
            edge=sig.edge,
        )

        return OrderRequest.create(
            signal_id=sig.signal_id,
            strategy_name=self.name,
            market_id=sig.market_id,
            market_question=sig.market_question,
            side="buy",
            outcome=outcome,
            order_type="limit",
            price=round(entry * 1.01, 4),   # small buffer — need fast fill
            size_usd=round(size_usd, 2),
            position_pct=size_pct,
            confidence_score=signal.confidence_score,
            take_profit=tp,
            stop_loss=sl,
            max_slippage_pct=0.015,
            reasoning=(
                f"Safe lock: {evt.home_team} {evt.home_score}-{evt.away_score} "
                f"{evt.away_team}, {evt.remaining_minutes}' remaining. "
                f"Model: {sig.calculated_probability:.0%} vs market: {entry:.0%}. "
                f"Edge: {sig.edge:+.0%}."
            ),
        )

    async def manage_position(
        self, position: Position, market_data: MarketSnapshot
    ) -> OrderRequest | None:
        """
        Safe locks are held to settlement.
        Only exit on stop loss (unexpected reversal) or market settlement.
        """
        current = market_data.prices.get(position.outcome, position.current_price)
        position.update_price(current)

        # Market is settling (price near 0 or 1)
        if current >= 0.97:
            return OrderRequest.create(
                signal_id=position.signal_id,
                strategy_name=self.name,
                market_id=position.market_id,
                market_question=position.market_question,
                side="sell",
                outcome=position.outcome,
                price=0.97,
                size_usd=position.size_usd,
                position_pct=0.0,
                confidence_score=0,
                take_profit=0.0,
                stop_loss=0.0,
                reasoning="Safe lock: market settling at YES",
            )

        if current <= position.stop_loss:
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
                reasoning="Safe lock stop loss — unexpected reversal",
            )

        return None


class ReversalCatchStrategy(BaseStrategy):
    """
    Triggered when:
      - signal_type == "sports_reversal"
      - Market price < historical comeback probability threshold
      - Match momentum is shifting toward the trailing team

    Small initial position, adds if momentum confirms.
    High risk / high reward — position is sized conservatively.
    """

    name = "sports_reversal"

    def __init__(
        self,
        max_market_price: float = 0.35,
        min_remaining_minutes: int = 15,
        base_position_pct: float = 0.02,
        tp_pct: float = 0.40,
        sl_pct: float = 0.15,
        momentum_add_threshold: float = 0.30,
    ) -> None:
        super().__init__()
        self._max_price = max_market_price
        self._min_remaining = min_remaining_minutes
        self._base_pct = base_position_pct
        self._tp_pct = tp_pct
        self._sl_pct = sl_pct
        self._momentum_threshold = momentum_add_threshold

    def should_handle(self, signal: ScoredSignal) -> bool:
        return (
            self.enabled
            and isinstance(signal.signal, SportsSignal)
            and "reversal" in signal.signal.signal_type
        )

    async def evaluate(
        self, signal: ScoredSignal, portfolio: Portfolio, risk_state: Any
    ) -> OrderRequest | None:
        if not isinstance(signal.signal, SportsSignal):
            return None

        sig = signal.signal
        evt = sig.sport_event

        if evt.remaining_minutes < self._min_remaining:
            return None  # not enough time for a reversal

        if sig.market_price > self._max_market_price_for_direction(sig):
            return None  # market already pricing reversal

        if sig.edge < 0.08:
            return None

        size_pct = self._base_pct  # conservative, no scaling up on reversals
        size_usd = portfolio.total_balance * size_pct

        direction = sig.direction
        outcome = "Yes" if direction == "buy_yes" else "No"
        entry = sig.market_price
        tp, sl = self._tp_sl_from_price(entry, direction, self._tp_pct, self._sl_pct)

        self._log.info(
            "reversal_order",
            match=f"{evt.home_team} {evt.home_score}-{evt.away_score} {evt.away_team}",
            remaining=evt.remaining_minutes,
            edge=sig.edge,
            momentum=sig.momentum_score,
        )

        return OrderRequest.create(
            signal_id=sig.signal_id,
            strategy_name=self.name,
            market_id=sig.market_id,
            market_question=sig.market_question,
            side="buy",
            outcome=outcome,
            price=round(entry * 1.02, 4),
            size_usd=round(size_usd, 2),
            position_pct=size_pct,
            confidence_score=signal.confidence_score,
            take_profit=tp,
            stop_loss=sl,
            max_slippage_pct=0.03,
            reasoning=(
                f"Reversal: {evt.home_team} {evt.home_score}-{evt.away_score} "
                f"{evt.away_team}, {evt.remaining_minutes}' left. "
                f"Market: {entry:.0%}, model: {sig.calculated_probability:.0%}. "
                f"Momentum: {sig.momentum_score:+.2f}."
            ),
        )

    def _max_market_price_for_direction(self, sig: SportsSignal) -> float:
        return self._max_price if sig.direction == "buy_yes" else (1.0 - self._max_price)

    async def manage_position(
        self, position: Position, market_data: MarketSnapshot
    ) -> OrderRequest | None:
        current = market_data.prices.get(position.outcome, position.current_price)
        position.update_price(current)

        if position.unrealized_pnl_pct >= 0.30:
            return self._close(position, current, "reversal_target_hit")
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
            reasoning=f"Reversal close: {reason}",
        )


class LiveOverUnderStrategy(BaseStrategy):
    """
    Monitors over/under contracts.

    Buy Over when:
      - Match is low-scoring but attacking stats (SOT, corners) are high
      - Implied O/U probability < model probability

    Sell (buy No) when:
      - Match is on pace for high goals but market overestimates further scoring
    """

    name = "sports_live_ou"

    def __init__(
        self,
        base_position_pct: float = 0.025,
        tp_pct: float = 0.25,
        sl_pct: float = 0.12,
    ) -> None:
        super().__init__()
        self._base_pct = base_position_pct
        self._tp_pct = tp_pct
        self._sl_pct = sl_pct

    def should_handle(self, signal: ScoredSignal) -> bool:
        return (
            self.enabled
            and isinstance(signal.signal, SportsSignal)
            and "ou" in signal.signal.signal_type
        )

    async def evaluate(
        self, signal: ScoredSignal, portfolio: Portfolio, risk_state: Any
    ) -> OrderRequest | None:
        if not isinstance(signal.signal, SportsSignal):
            return None

        sig = signal.signal
        evt = sig.sport_event

        if sig.edge < 0.07:
            return None

        # O/U markets: YES = "Over the line", NO = "Under"
        direction = sig.direction
        outcome = "Yes" if direction == "buy_yes" else "No"
        entry = sig.market_price
        tp, sl = self._tp_sl_from_price(entry, direction, self._tp_pct, self._sl_pct)

        size_pct = self._scale_size(self._base_pct, signal.confidence_score, max_pct=0.05)
        size_usd = portfolio.total_balance * size_pct

        return OrderRequest.create(
            signal_id=sig.signal_id,
            strategy_name=self.name,
            market_id=sig.market_id,
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
                f"Live O/U: {evt.home_team} vs {evt.away_team} "
                f"({evt.home_score}-{evt.away_score}, {evt.elapsed_minutes}'). "
                f"Edge: {sig.edge:+.0%}."
            ),
        )

    async def manage_position(
        self, position: Position, market_data: MarketSnapshot
    ) -> OrderRequest | None:
        current = market_data.prices.get(position.outcome, position.current_price)
        position.update_price(current)

        # O/U reprices fast after a goal — exit quickly
        if position.unrealized_pnl_pct >= 0.20:
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
                reasoning="O/U: target hit",
            )
        if current <= position.stop_loss:
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
                reasoning="O/U: stop loss",
            )
        return None
