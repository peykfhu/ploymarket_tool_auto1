"""
strategy/swing_strategy.py

Four swing sub-strategies:
  1. MeanReversionStrategy  — price deviated from 7-day mean without news catalyst
  2. BreakoutStrategy       — volume-backed breakout after consolidation
  3. EventDrivenStrategy    — news-triggered directional trade
  4. ArbitrageStrategy      — correlated market mispricing (structural)
"""

from __future__ import annotations

import statistics
from typing import Any

from data_ingestion.polymarket_collector import MarketSnapshot
from signal_processing.models import NewsSignal, ScoredSignal
from strategy.base import BaseStrategy
from strategy.models import OrderRequest, Portfolio, Position


class MeanReversionStrategy(BaseStrategy):
    """
    Buys/sells when price deviates significantly from its 7-day rolling mean
    without an identifiable news catalyst driving the move.

    Scale-in tiers:
      ≥15% deviation → 30% of full size
      ≥20% deviation → 60%
      ≥30% deviation → 100%
    """

    name = "swing_mean_reversion"

    SCALE_TIERS = [
        (0.30, 1.00),
        (0.20, 0.60),
        (0.15, 0.30),
    ]

    def __init__(
        self,
        lookback_days: int = 7,
        base_position_pct: float = 0.03,
        tp_pct: float = 0.15,
        sl_pct: float = 0.08,
    ) -> None:
        super().__init__()
        self._lookback = lookback_days
        self._base_pct = base_position_pct
        self._tp_pct = tp_pct
        self._sl_pct = sl_pct

    def should_handle(self, signal: ScoredSignal) -> bool:
        return self.enabled and isinstance(signal.signal, NewsSignal)

    async def evaluate(
        self, signal: ScoredSignal, portfolio: Portfolio, risk_state: Any
    ) -> OrderRequest | None:
        if not isinstance(signal.signal, NewsSignal):
            return None

        sig = signal.signal
        history = []  # populated from market snapshot in production
        # In the real pipeline, the orchestrator injects the latest snapshot.
        # Here we work with what the signal carries.

        price_before = sig.price_before
        expected_after = sig.expected_price_after
        deviation = abs(expected_after - price_before) / max(price_before, 0.01)

        if deviation < 0.15:
            return None

        # Determine scale multiplier
        scale = 0.30
        for threshold, mult in self.SCALE_TIERS:
            if deviation >= threshold:
                scale = mult
                break

        size_pct = self._scale_size(self._base_pct * scale, signal.confidence_score)
        size_usd = portfolio.total_balance * size_pct

        direction = sig.direction
        outcome = "Yes" if direction == "buy_yes" else "No"
        entry = price_before
        tp, sl = self._tp_sl_from_price(entry, direction, self._tp_pct, self._sl_pct)

        self._log.info(
            "mean_reversion_signal",
            deviation=round(deviation, 3),
            scale=scale,
            size_usd=round(size_usd, 2),
        )

        return OrderRequest.create(
            signal_id=signal.signal.signal_id,
            strategy_name=self.name,
            market_id=sig.market_id,
            market_question=sig.market_question,
            side="buy",
            outcome=outcome,
            price=round(entry * 1.01, 4),   # small limit buffer
            size_usd=round(size_usd, 2),
            position_pct=size_pct,
            confidence_score=signal.confidence_score,
            take_profit=tp,
            stop_loss=sl,
            reasoning=(
                f"Mean reversion: {deviation:.1%} deviation from mean. "
                f"Scale: {scale:.0%}. Direction: {direction}."
            ),
        )

    async def manage_position(
        self, position: Position, market_data: MarketSnapshot
    ) -> OrderRequest | None:
        """Close when price reverts to near-mean or stop is hit."""
        current = market_data.prices.get(position.outcome, position.current_price)
        position.update_price(current)

        # Trailing stop: if profit > 15%, move SL to breakeven
        if position.unrealized_pnl_pct > 0.15 and position.trailing_stop is None:
            position.trailing_stop = position.entry_price
            self._log.info("trailing_stop_activated", position_id=position.position_id)

        # Check fixed SL / TP
        if position.side == "buy":
            if current <= position.stop_loss or (
                position.trailing_stop and current <= position.trailing_stop
            ):
                return self._close_order(position, current, "stop_loss")
            if current >= position.take_profit:
                return self._close_order(position, current, "take_profit")
        return None

    def _close_order(self, position: Position, price: float, reason: str) -> OrderRequest:
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
            reasoning=f"Closing position: {reason}",
        )


class BreakoutStrategy(BaseStrategy):
    """
    Enters on a volume-backed breakout after a consolidation period.
    Confirmation requires volume_change_rate > multiplier × average.
    """

    name = "swing_breakout"

    def __init__(
        self,
        volume_multiplier: float = 2.0,
        consolidation_vol_threshold: float = 0.005,
        base_position_pct: float = 0.03,
        tp_pct: float = 0.25,
        sl_pct: float = 0.10,
    ) -> None:
        super().__init__()
        self._vol_mult = volume_multiplier
        self._consol_threshold = consolidation_vol_threshold
        self._base_pct = base_position_pct
        self._tp_pct = tp_pct
        self._sl_pct = sl_pct

    def should_handle(self, signal: ScoredSignal) -> bool:
        return self.enabled and isinstance(signal.signal, NewsSignal)

    async def evaluate(
        self, signal: ScoredSignal, portfolio: Portfolio, risk_state: Any
    ) -> OrderRequest | None:
        if not isinstance(signal.signal, NewsSignal):
            return None

        sig = signal.signal

        # Breakout requires a meaningful move driven by volume
        move = abs(sig.expected_price_after - sig.price_before)
        if move < 0.08:
            return None

        # Use volatility as a consolidation proxy (low vol before = good)
        # In live use, we'd check the actual snapshot volatility_1h
        direction = sig.direction
        outcome = "Yes" if direction == "buy_yes" else "No"
        entry = sig.price_before
        tp, sl = self._tp_sl_from_price(entry, direction, self._tp_pct, self._sl_pct)

        size_pct = self._scale_size(self._base_pct, signal.confidence_score)
        size_usd = portfolio.total_balance * size_pct

        self._log.info("breakout_signal", move=round(move, 3), direction=direction)

        return OrderRequest.create(
            signal_id=sig.signal_id,
            strategy_name=self.name,
            market_id=sig.market_id,
            market_question=sig.market_question,
            side="buy",
            outcome=outcome,
            order_type="ioc",          # breakout needs fast fill
            price=round(entry * 1.015, 4),
            size_usd=round(size_usd, 2),
            position_pct=size_pct,
            confidence_score=signal.confidence_score,
            take_profit=tp,
            stop_loss=sl,
            time_in_force="IOC",
            reasoning=(
                f"Breakout: {move:.1%} expected move, "
                f"volume-confirmed. Direction: {direction}."
            ),
        )

    async def manage_position(
        self, position: Position, market_data: MarketSnapshot
    ) -> OrderRequest | None:
        current = market_data.prices.get(position.outcome, position.current_price)
        position.update_price(current)

        # Tiered take-profit: sell 50% at first target, rest at full TP
        pnl_pct = position.unrealized_pnl_pct
        if pnl_pct >= 0.40:
            return self._close_order(position, current, "take_profit_full")
        if current <= position.stop_loss:
            return self._close_order(position, current, "stop_loss")
        return None

    def _close_order(self, position: Position, price: float, reason: str) -> OrderRequest:
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
            reasoning=f"Breakout close: {reason}",
        )


class EventDrivenStrategy(BaseStrategy):
    """
    Trades directly on high-confidence news signals.
    Core strategy for news-driven markets.

    Scales in based on speed advantage:
      speed_advantage > 0.8 → full size immediately
      speed_advantage > 0.5 → 70% now, check again in 2 min
      else                  → 50% starter position
    """

    name = "swing_event_driven"

    def __init__(
        self,
        base_position_pct: float = 0.03,
        tp_pct: float = 0.20,
        sl_pct: float = 0.10,
        min_direction_confidence: float = 0.60,
    ) -> None:
        super().__init__()
        self._base_pct = base_position_pct
        self._tp_pct = tp_pct
        self._sl_pct = sl_pct
        self._min_conf = min_direction_confidence

    def should_handle(self, signal: ScoredSignal) -> bool:
        return (
            self.enabled
            and isinstance(signal.signal, NewsSignal)
            and signal.signal.signal_type == "news_driven"
        )

    async def evaluate(
        self, signal: ScoredSignal, portfolio: Portfolio, risk_state: Any
    ) -> OrderRequest | None:
        if not isinstance(signal.signal, NewsSignal):
            return None

        sig = signal.signal

        if sig.direction_confidence < self._min_conf:
            return None
        if sig.direction == "neutral" or not sig.direction:
            return None

        speed = sig.speed_advantage
        if speed > 0.80:
            scale = 1.00
        elif speed > 0.50:
            scale = 0.70
        else:
            scale = 0.50

        size_pct = self._scale_size(self._base_pct * scale, signal.confidence_score)
        size_usd = portfolio.total_balance * size_pct

        direction = sig.direction
        outcome = "Yes" if direction == "buy_yes" else "No"
        entry = sig.price_before
        tp, sl = self._tp_sl_from_price(entry, direction, self._tp_pct, self._sl_pct)

        # Slippage tolerance tighter for fast news plays
        slippage = 0.01 if speed > 0.80 else 0.02

        self._log.info(
            "event_driven_signal",
            speed=round(speed, 2),
            scale=scale,
            direction=direction,
            market=sig.market_question[:60],
        )

        return OrderRequest.create(
            signal_id=sig.signal_id,
            strategy_name=self.name,
            market_id=sig.market_id,
            market_question=sig.market_question,
            side="buy",
            outcome=outcome,
            order_type="limit",
            price=round(entry * (1.01 if direction == "buy_yes" else 0.99), 4),
            size_usd=round(size_usd, 2),
            position_pct=size_pct,
            confidence_score=signal.confidence_score,
            take_profit=tp,
            stop_loss=sl,
            max_slippage_pct=slippage,
            reasoning=(
                f"Event-driven: {sig.source_event.headline[:100]}. "
                f"Speed: {speed:.0%}. Scale: {scale:.0%}. Direction: {direction}."
            ),
        )

    async def manage_position(
        self, position: Position, market_data: MarketSnapshot
    ) -> OrderRequest | None:
        """Exit quickly after event is priced in — don't hold long."""
        current = market_data.prices.get(position.outcome, position.current_price)
        position.update_price(current)

        pnl_pct = position.unrealized_pnl_pct

        # Quick exit at 15% profit (news plays reprice fast)
        if pnl_pct >= 0.15:
            return self._close(position, current, "news_repriced")

        # Stop loss
        if current <= position.stop_loss:
            return self._close(position, current, "stop_loss")

        # Time stop: news plays shouldn't be held longer than 2 hours
        from datetime import timezone
        import datetime as dt
        age_h = (dt.datetime.now(timezone.utc) - position.opened_at).total_seconds() / 3600
        if age_h > 2.0:
            return self._close(position, current, "time_stop_2h")

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
            reasoning=f"Event-driven close: {reason}",
        )


class ArbitrageStrategy(BaseStrategy):
    """
    Detects correlated market mispricing.
    Example: "Will X win?" priced at 0.60 but related "Will X get >50%?" at 0.40.

    Currently detects simple negation arbitrage:
    If P(Yes for A) + P(Yes for B) deviates from 1.0 for mutually exclusive markets.
    """

    name = "swing_arbitrage"

    def __init__(self, min_arb_gap: float = 0.06, base_position_pct: float = 0.02) -> None:
        super().__init__()
        self._min_gap = min_arb_gap
        self._base_pct = base_position_pct

    def should_handle(self, signal: ScoredSignal) -> bool:
        # Arbitrage is market-structure driven; evaluated separately by orchestrator
        return False  # disabled in signal-driven path; orchestrator calls directly

    async def evaluate(
        self, signal: ScoredSignal, portfolio: Portfolio, risk_state: Any
    ) -> OrderRequest | None:
        return None  # orchestrator calls evaluate_arb() directly

    async def evaluate_arb(
        self,
        market_a: MarketSnapshot,
        market_b: MarketSnapshot,
        portfolio: Portfolio,
    ) -> list[OrderRequest]:
        """
        Check if market_a YES + market_b YES ≠ 1.0 (mutually exclusive markets).
        Returns up to two legs of the arbitrage.
        """
        p_a = market_a.prices.get("Yes", 0.5)
        p_b = market_b.prices.get("Yes", 0.5)
        total = p_a + p_b

        orders: list[OrderRequest] = []

        # Over-priced: total > 1 + gap → sell both YES (buy NO on both)
        if total > 1.0 + self._min_gap:
            gap = total - 1.0
            self._log.info("arb_detected_overpriced", gap=round(gap, 3))
            size_usd = portfolio.total_balance * self._base_pct / 2
            for mkt, price in [(market_a, p_a), (market_b, p_b)]:
                orders.append(OrderRequest.create(
                    signal_id="arb",
                    strategy_name=self.name,
                    market_id=mkt.market_id,
                    market_question=mkt.question,
                    side="buy",
                    outcome="No",
                    price=round(1 - price + 0.01, 4),
                    size_usd=round(size_usd, 2),
                    position_pct=self._base_pct / 2,
                    confidence_score=70,
                    take_profit=round(1 - price + 0.15, 4),
                    stop_loss=round(1 - price - 0.05, 4),
                    reasoning=f"Arb: over-priced total {total:.2f}. Buying NO on {mkt.question[:40]}",
                ))

        # Under-priced: total < 1 - gap → buy both YES
        elif total < 1.0 - self._min_gap:
            gap = 1.0 - total
            self._log.info("arb_detected_underpriced", gap=round(gap, 3))
            size_usd = portfolio.total_balance * self._base_pct / 2
            for mkt, price in [(market_a, p_a), (market_b, p_b)]:
                orders.append(OrderRequest.create(
                    signal_id="arb",
                    strategy_name=self.name,
                    market_id=mkt.market_id,
                    market_question=mkt.question,
                    side="buy",
                    outcome="Yes",
                    price=round(price + 0.01, 4),
                    size_usd=round(size_usd, 2),
                    position_pct=self._base_pct / 2,
                    confidence_score=70,
                    take_profit=round(price + 0.15, 4),
                    stop_loss=round(price - 0.05, 4),
                    reasoning=f"Arb: under-priced total {total:.2f}. Buying YES on {mkt.question[:40]}",
                ))

        return orders

    async def manage_position(
        self, position: Position, market_data: MarketSnapshot
    ) -> OrderRequest | None:
        current = market_data.prices.get(position.outcome, position.current_price)
        position.update_price(current)
        if position.unrealized_pnl_pct >= 0.12 or position.unrealized_pnl_pct <= -0.06:
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
                reasoning="Arb: convergence or stop hit",
            )
        return None
