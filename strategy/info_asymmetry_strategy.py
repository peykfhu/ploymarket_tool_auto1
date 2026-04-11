"""
strategy/info_asymmetry_strategy.py

InfoAsymmetryStrategy (体育政治信息差) — "Sports & Politics information edge".

This strategy exists because prediction markets do not absorb news
uniformly fast:

  • Political news from primary sources (Reuters, AP, official feeds)
    often hits Polymarket 30–120 seconds BEFORE the market's average
    price updates. Market makers widen quotes, takers hesitate, and
    there is a window where we can lift (or hit) stale liquidity.

  • Sports news is even starker. Goals, red cards, injuries and lineups
    propagate through different paths for different sources:
      - Live feeds → 1–3s
      - News wires → 15–45s
      - Polymarket prices → 20–90s
    The edge is not the outcome itself — it's the latency gradient.

The strategy consumes NewsSignal and SportsSignal objects that the
upstream pipeline has already scored. Its job is to validate that the
*information advantage* is real, size the trade appropriately for a
short-hold scalp, and exit fast when the price catches up.

Hard requirements:

  1. signal category in {politics, sports} (for NewsSignal) or any
     SportsSignal
  2. speed_advantage ≥ configured threshold (means: we believe the
     market has not yet repriced)
  3. market_relevance ≥ threshold (for NewsSignal), or edge ≥ threshold
     (for SportsSignal)
  4. confidence_score ≥ buy_threshold
  5. No existing position in the same market
  6. Not during the hard circuit-breaker quiet window

Exit is time-boxed: if the market catches up within 10 minutes, take
profit. If it doesn't, close anyway (information-arb decays).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from data_ingestion.polymarket_collector import MarketSnapshot
from signal_processing.models import NewsSignal, ScoredSignal, SportsSignal
from strategy.base import BaseStrategy
from strategy.models import OrderRequest, Portfolio, Position

POLITICAL_CATEGORIES = {"politics", "election", "government", "geopolitics"}
SPORTS_CATEGORIES = {"sports", "football", "basketball", "nfl", "mlb", "tennis"}


class InfoAsymmetryStrategy(BaseStrategy):
    """
    A single strategy that handles both Sports and Political
    information-edge trades through a common rubric.

    It is signal-driven (not scan-driven), so it fits cleanly into the
    existing orchestrator dispatch mechanism.
    """

    name = "info_asymmetry"

    def __init__(
        self,
        min_speed_advantage: float = 0.55,
        min_market_relevance: float = 0.60,
        min_sports_edge: float = 0.06,
        min_confidence_score: int = 65,
        base_position_pct: float = 0.025,
        max_position_pct: float = 0.05,
        tp_pct: float = 0.18,
        sl_pct: float = 0.08,
        hold_timeout_minutes: int = 10,
        max_news_latency_ms: int = 4_000,
    ) -> None:
        super().__init__()
        self._min_speed = min_speed_advantage
        self._min_relevance = min_market_relevance
        self._min_sports_edge = min_sports_edge
        self._min_confidence = min_confidence_score
        self._base_pct = base_position_pct
        self._max_pct = max_position_pct
        self._tp_pct = tp_pct
        self._sl_pct = sl_pct
        self._hold_timeout = hold_timeout_minutes
        self._max_news_latency = max_news_latency_ms

    # ── Dispatch ─────────────────────────────────────────────────────────────

    def should_handle(self, signal: ScoredSignal) -> bool:
        if not self.enabled:
            return False
        sig = signal.signal
        if isinstance(sig, NewsSignal):
            cat = (sig.source_event.category or "").lower()
            return cat in POLITICAL_CATEGORIES or cat in SPORTS_CATEGORIES
        if isinstance(sig, SportsSignal):
            return True
        return False

    # ── Evaluate ─────────────────────────────────────────────────────────────

    async def evaluate(
        self, signal: ScoredSignal, portfolio: Portfolio, risk_state: Any
    ) -> OrderRequest | None:
        if signal.confidence_score < self._min_confidence:
            return None

        if isinstance(signal.signal, NewsSignal):
            return self._evaluate_news(signal, portfolio)
        if isinstance(signal.signal, SportsSignal):
            return self._evaluate_sports(signal, portfolio)
        return None

    # ── News branch ─────────────────────────────────────────────────────────

    def _evaluate_news(
        self, signal: ScoredSignal, portfolio: Portfolio
    ) -> OrderRequest | None:
        sig: NewsSignal = signal.signal  # type: ignore[assignment]
        evt = sig.source_event

        # Latency check — if the news is already stale, the edge is gone
        if evt.latency_ms > self._max_news_latency:
            self._log.debug(
                "info_news_too_stale",
                latency_ms=evt.latency_ms,
                headline=evt.headline[:80],
            )
            return None

        # Speed advantage (our belief the market hasn't repriced)
        if sig.speed_advantage < self._min_speed:
            self._log.debug(
                "info_news_speed_too_low", speed=sig.speed_advantage
            )
            return None

        if sig.market_relevance < self._min_relevance:
            return None

        # Direction & entry
        direction = sig.direction
        if direction not in ("buy_yes", "buy_no"):
            return None
        outcome = "Yes" if direction == "buy_yes" else "No"

        if portfolio.get_position(sig.market_id, outcome) is not None:
            return None

        entry = sig.price_before
        if not (0.01 <= entry <= 0.99):
            return None

        size_pct = self._size_for(signal.confidence_score)
        size_usd = portfolio.total_balance * size_pct

        tp, sl = self._tp_sl_from_price(entry, direction, self._tp_pct, self._sl_pct)

        # For news: pay a small premium — we need to hit before repricing
        entry_price = round(
            min(0.99, max(0.01, entry * (1.015 if direction == "buy_yes" else 1.015))),
            4,
        )

        reasoning = (
            f"InfoArb [news/{evt.category}]: {evt.headline[:80]} — "
            f"latency={evt.latency_ms}ms, speed={sig.speed_advantage:.2f}, "
            f"rel={sig.market_relevance:.2f}, Δp={(sig.expected_price_after - entry):+.2f}"
        )

        self._log.info(
            "info_news_order",
            market=sig.market_id,
            direction=direction,
            confidence=signal.confidence_score,
        )

        return OrderRequest.create(
            signal_id=sig.signal_id,
            strategy_name=self.name,
            market_id=sig.market_id,
            market_question=sig.market_question,
            side="buy",
            outcome=outcome,
            order_type="limit",
            price=entry_price,
            size_usd=round(size_usd, 2),
            position_pct=round(size_pct, 4),
            confidence_score=signal.confidence_score,
            take_profit=tp,
            stop_loss=sl,
            time_in_force="IOC",
            max_slippage_pct=0.02,
            reasoning=reasoning,
        )

    # ── Sports branch ───────────────────────────────────────────────────────

    def _evaluate_sports(
        self, signal: ScoredSignal, portfolio: Portfolio
    ) -> OrderRequest | None:
        sig: SportsSignal = signal.signal  # type: ignore[assignment]
        evt = sig.sport_event

        # For sports we use `edge` as the info-asymmetry proxy
        if sig.edge < self._min_sports_edge:
            self._log.debug("info_sports_edge_low", edge=sig.edge)
            return None

        # A live match with seconds-level data flow is the sweet spot
        if evt.match_status not in (
            "first_half", "second_half", "halftime", "overtime", "in_play"
        ):
            return None

        direction = sig.direction
        if direction not in ("buy_yes", "buy_no"):
            return None
        outcome = "Yes" if direction == "buy_yes" else "No"

        if portfolio.get_position(sig.market_id, outcome) is not None:
            return None

        entry = sig.market_price
        if not (0.01 <= entry <= 0.99):
            return None

        size_pct = self._size_for(signal.confidence_score)
        size_usd = portfolio.total_balance * size_pct

        tp, sl = self._tp_sl_from_price(entry, direction, self._tp_pct, self._sl_pct)

        entry_price = round(min(0.99, max(0.01, entry * 1.015)), 4)

        reasoning = (
            f"InfoArb [sports/{evt.sport}]: {evt.home_team} {evt.home_score}-"
            f"{evt.away_score} {evt.away_team} ({evt.elapsed_minutes}') — "
            f"edge={sig.edge:+.2%}, market={entry:.2f}, "
            f"model={sig.calculated_probability:.2f}"
        )

        self._log.info(
            "info_sports_order",
            market=sig.market_id,
            direction=direction,
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
            price=entry_price,
            size_usd=round(size_usd, 2),
            position_pct=round(size_pct, 4),
            confidence_score=signal.confidence_score,
            take_profit=tp,
            stop_loss=sl,
            time_in_force="IOC",
            max_slippage_pct=0.02,
            reasoning=reasoning,
        )

    # ── Sizing helper ───────────────────────────────────────────────────────

    def _size_for(self, confidence: int) -> float:
        """Linear from base_pct at 65 to max_pct at 95."""
        t = max(0.0, min(1.0, (confidence - self._min_confidence) / 30.0))
        return round(self._base_pct + t * (self._max_pct - self._base_pct), 4)

    # ── Position management ────────────────────────────────────────────────

    async def manage_position(
        self, position: Position, market_data: MarketSnapshot
    ) -> OrderRequest | None:
        current = market_data.prices.get(position.outcome, position.current_price)
        position.update_price(current)

        # Take profit
        if position.unrealized_pnl_pct >= self._tp_pct:
            return self._close(position, current, "info_arb_target")

        # Stop loss
        if current <= position.stop_loss:
            return self._close(position, current, "info_arb_stop")

        # Time-boxed exit — info arbitrage decays
        opened = position.opened_at
        if opened.tzinfo is None:
            opened = opened.replace(tzinfo=timezone.utc)
        age_minutes = (datetime.now(timezone.utc) - opened).total_seconds() / 60.0
        if age_minutes >= self._hold_timeout:
            return self._close(position, current, "info_arb_timeout")

        return None

    def _close(self, position: Position, price: float, reason: str) -> OrderRequest:
        return OrderRequest.create(
            signal_id=position.signal_id,
            strategy_name=self.name,
            market_id=position.market_id,
            market_question=position.market_question,
            side="sell",
            outcome=position.outcome,
            order_type="limit",
            price=round(max(0.01, price * 0.99), 4),
            size_usd=position.size_usd,
            position_pct=0.0,
            confidence_score=0,
            take_profit=0.0,
            stop_loss=0.0,
            time_in_force="IOC",
            max_slippage_pct=0.025,
            reasoning=f"InfoArb exit: {reason}",
        )
