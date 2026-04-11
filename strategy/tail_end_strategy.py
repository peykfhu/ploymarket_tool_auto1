"""
strategy/tail_end_strategy.py

TailEndStrategy (吃尾盘) — "eat the closing tape".

The idea: late in a market's life, several inefficiencies appear that a
passive signal-driven strategy can't see:

  • Liquidity providers pull quotes. Spreads widen. Aggressive hitters
    push price away from fair value.
  • Retail rushes in on whichever side is already winning, overpaying
    for certainty (0.96 → 0.995), or capitulates on the losing side,
    underpricing long-shot tails (0.04 → 0.005).
  • Resolver risk premium. Markets with an ambiguous outcome see the
    favored side discounted because settlement disputes compress gains.

This strategy is NOT signal-driven like the others. It is scan-driven:
the orchestrator calls ``scan()`` every N seconds with the current
``MarketSnapshot`` cache, and we decide whether any market qualifies.

Trades are small, fast (IOC), and always capped to ≤5% of the book so
we don't move the price against ourselves.

Two trade families:

  1. Favorite Lean ("follow the locked side")
     Price ≥ 0.90 AND strong last-hour momentum AND spread ≤ 4%
     → buy the favored side at ask, take profit 0.99, stop 0.90.

  2. Tail Harvest ("sell the ticket that still costs something")
     Market within configured end-window AND favored side price ≥ 0.95
     but the long-shot is still ≥ 0.03 → buy the favored side because
     the market has not yet converged.

Exit is aggressive: these are scalps. Either the market resolves, the
price converges to 0.99+, or we take a small loss at stop.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from data_ingestion.polymarket_collector import MarketSnapshot
from signal_processing.models import ScoredSignal
from strategy.base import BaseStrategy
from strategy.models import OrderRequest, Portfolio, Position


def _hours_to_end(snap: MarketSnapshot, now: datetime | None = None) -> float | None:
    if not snap.end_date:
        return None
    now = now or datetime.now(timezone.utc)
    delta = (snap.end_date - now).total_seconds() / 3600.0
    return delta


class TailEndStrategy(BaseStrategy):
    """
    Scan-driven strategy that watches markets approaching resolution.

    It ignores ScoredSignals entirely (returns None from evaluate) and
    instead exposes ``scan(market_cache, portfolio)`` which the
    orchestrator calls on a periodic loop.
    """

    name = "tail_end"

    def __init__(
        self,
        end_window_hours: float = 6.0,          # only markets closing within this window
        min_favorite_price: float = 0.90,
        tail_harvest_min_favorite: float = 0.95,
        max_spread_pct: float = 0.04,
        min_liquidity_usd: float = 2_000.0,
        base_position_pct: float = 0.02,
        max_position_pct: float = 0.04,
        tp_price: float = 0.99,
        sl_distance: float = 0.06,
        max_pct_of_book: float = 0.05,          # never take >5% of visible liquidity
    ) -> None:
        super().__init__()
        self._end_window = end_window_hours
        self._min_fav = min_favorite_price
        self._tail_min_fav = tail_harvest_min_favorite
        self._max_spread = max_spread_pct
        self._min_liq = min_liquidity_usd
        self._base_pct = base_position_pct
        self._max_pct = max_position_pct
        self._tp = tp_price
        self._sl_distance = sl_distance
        self._max_book_share = max_pct_of_book

    # ── Signal path (unused) ──────────────────────────────────────────────────

    def should_handle(self, signal: ScoredSignal) -> bool:  # noqa: D401
        # TailEnd is scan-driven. It never consumes a ScoredSignal.
        return False

    async def evaluate(
        self, signal: ScoredSignal, portfolio: Portfolio, risk_state: Any
    ) -> OrderRequest | None:
        return None

    # ── Scan entry point ─────────────────────────────────────────────────────

    async def scan(
        self,
        market_cache: dict[str, MarketSnapshot],
        portfolio: Portfolio,
        now: datetime | None = None,
    ) -> list[OrderRequest]:
        """
        Iterate the market cache, find eligible tail-end markets, and
        return a list of OrderRequests (one per market at most).
        The orchestrator runs each through the risk engine before submission.
        """
        orders: list[OrderRequest] = []
        now = now or datetime.now(timezone.utc)

        for market_id, snap in market_cache.items():
            try:
                order = self._maybe_build(snap, portfolio, now)
                if order:
                    orders.append(order)
            except Exception as exc:  # pragma: no cover — defensive
                self._log.warning("tail_end_scan_error", market=market_id, error=str(exc))

        if orders:
            self._log.info("tail_end_scan_produced", count=len(orders))
        return orders

    def _maybe_build(
        self, snap: MarketSnapshot, portfolio: Portfolio, now: datetime
    ) -> OrderRequest | None:
        # Basic gates
        hours_left = _hours_to_end(snap, now)
        if hours_left is None:
            return None
        if hours_left <= 0 or hours_left > self._end_window:
            return None
        if snap.liquidity < self._min_liq:
            return None
        if snap.spread > self._max_spread:
            return None

        yes = float(snap.prices.get("Yes", 0.0))
        no_ = float(snap.prices.get("No", 0.0))
        if yes <= 0 or no_ <= 0:
            return None

        # Pick the favorite
        fav_outcome, fav_price = ("Yes", yes) if yes >= no_ else ("No", no_)

        # Two entry modes, ordered: harvest first, then favorite lean
        mode: str
        if fav_price >= self._tail_min_fav and fav_price < 0.99:
            mode = "tail_harvest"
        elif fav_price >= self._min_fav and fav_price < self._tail_min_fav:
            # Require momentum: recent prices trending toward favorite
            if not self._has_favorite_momentum(snap, fav_outcome):
                return None
            mode = "favorite_lean"
        else:
            return None

        # Don't stack on an existing position in this market
        if portfolio.get_position(snap.market_id, fav_outcome) is not None:
            return None

        # Size: base, clamped by visible liquidity
        size_pct = min(self._max_pct, self._base_pct * (1.5 if mode == "tail_harvest" else 1.0))
        size_usd = portfolio.total_balance * size_pct
        book_cap = snap.liquidity * self._max_book_share
        if book_cap > 0 and size_usd > book_cap:
            size_usd = book_cap
            size_pct = round(size_usd / max(portfolio.total_balance, 1.0), 4)

        if size_usd < 5.0:
            return None  # too small to bother (fees + slippage eat it)

        # Confidence score: translate fav_price and hours_left into 60-90 range
        # Higher fav_price and less time = higher confidence
        time_factor = max(0.0, 1.0 - (hours_left / self._end_window))
        price_factor = (fav_price - self._min_fav) / (0.99 - self._min_fav)
        confidence = int(60 + 30 * max(0.0, min(1.0, 0.5 * time_factor + 0.5 * price_factor)))

        entry = round(min(0.995, fav_price + 0.005), 4)   # pay 0.5 cent over
        stop = round(max(0.01, fav_price - self._sl_distance), 4)

        reasoning = (
            f"TailEnd [{mode}]: {snap.question[:60]} — "
            f"{hours_left:.1f}h left, fav={fav_outcome}@{fav_price:.3f}, "
            f"spread={snap.spread:.2%}, liq=${snap.liquidity:,.0f}"
        )

        return OrderRequest.create(
            signal_id=f"tail:{snap.market_id}:{int(now.timestamp())}",
            strategy_name=self.name,
            market_id=snap.market_id,
            market_question=snap.question,
            side="buy",
            outcome=fav_outcome,
            order_type="limit",
            price=entry,
            size_usd=round(size_usd, 2),
            position_pct=round(size_pct, 4),
            confidence_score=confidence,
            take_profit=self._tp,
            stop_loss=stop,
            time_in_force="IOC",                  # scalping — don't rest
            max_slippage_pct=0.01,
            reasoning=reasoning,
        )

    @staticmethod
    def _has_favorite_momentum(snap: MarketSnapshot, fav_outcome: str) -> bool:
        """Last 3 snapshots trending favorably (>= last-1 >= last-2)."""
        history = snap.price_history or []
        if len(history) < 3:
            # Not enough history — be permissive for illiquid/new listings
            return True
        last3 = history[-3:]
        if fav_outcome == "Yes":
            return last3[2] >= last3[1] >= last3[0]
        # favorite is "No" — equivalent to Yes trending down
        return last3[2] <= last3[1] <= last3[0]

    # ── Position management ─────────────────────────────────────────────────

    async def manage_position(
        self, position: Position, market_data: MarketSnapshot
    ) -> OrderRequest | None:
        current = market_data.prices.get(position.outcome, position.current_price)
        position.update_price(current)

        # Market has essentially resolved
        if current >= self._tp:
            return self._close(position, current, "tail_end_target")

        # Stop loss
        if current <= position.stop_loss:
            return self._close(position, current, "tail_end_stop")

        # Time-based exit — if market expiry is within 5 minutes, close
        if market_data.end_date:
            seconds_left = (market_data.end_date - datetime.now(timezone.utc)).total_seconds()
            if seconds_left < 300 and current > 0:
                return self._close(position, current, "tail_end_timebox")

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
            price=round(max(0.01, price * 0.995), 4),
            size_usd=position.size_usd,
            position_pct=0.0,
            confidence_score=0,
            take_profit=0.0,
            stop_loss=0.0,
            time_in_force="IOC",
            max_slippage_pct=0.02,
            reasoning=f"TailEnd exit: {reason}",
        )
