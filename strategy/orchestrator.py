"""
strategy/orchestrator.py

Strategy orchestrator — the central scheduler for the strategy engine.

Responsibilities:
  1. Receive ScoredSignals and route to matching strategies
  2. Collect all order proposals from strategies
  3. Resolve conflicts between orders for the same market
  4. Submit approved orders to the risk layer
  5. Run periodic position management (every 30s)
  6. Handle delayed whale-follow executions
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any

import structlog

from data_ingestion.polymarket_collector import MarketSnapshot
from signal_processing.models import ScoredSignal, WhaleSignal
from strategy.base import BaseStrategy
from strategy.models import OrderRequest, Portfolio, Position
from strategy.swing_strategy import (
    ArbitrageStrategy,
    BreakoutStrategy,
    EventDrivenStrategy,
    MeanReversionStrategy,
)
from strategy.sports_endgame_strategy import (
    LiveOverUnderStrategy,
    ReversalCatchStrategy,
    SafeLockStrategy,
)
from strategy.whale_follow_strategy import (
    MultiWhaleConsensusStrategy,
    SingleWhaleFollowStrategy,
    WhaleFadeStrategy,
)

logger = structlog.get_logger(__name__)

POSITION_CHECK_INTERVAL_S = 30


class StrategyOrchestrator:
    """
    Wires all strategies together and manages the order lifecycle.
    """

    def __init__(
        self,
        enabled_strategy_names: list[str],
        risk_engine: Any,       # RiskRuleEngine (injected to avoid circular import)
        order_manager: Any,     # OrderManager
        settings: Any,
    ) -> None:
        self._risk = risk_engine
        self._order_manager = order_manager
        self._settings = settings
        self._log = logger.bind(component="orchestrator")

        # Build strategy registry
        self._all_strategies: dict[str, BaseStrategy] = self._build_strategies()
        self._active: list[BaseStrategy] = [
            s for name, s in self._all_strategies.items()
            if name in enabled_strategy_names
        ]
        self._log.info("strategies_loaded", count=len(self._active),
                       names=[s.name for s in self._active])

        # Latest market snapshots for position management
        self._market_cache: dict[str, MarketSnapshot] = {}

        self._position_task: asyncio.Task | None = None
        self._whale_follow_task: asyncio.Task | None = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._position_task = asyncio.create_task(
            self._position_management_loop(), name="orchestrator:positions"
        )
        self._whale_follow_task = asyncio.create_task(
            self._whale_follow_loop(), name="orchestrator:whale_follow"
        )
        self._log.info("orchestrator_started")

    async def stop(self) -> None:
        for task in (self._position_task, self._whale_follow_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._log.info("orchestrator_stopped")

    def update_market_cache(self, snapshots: dict[str, MarketSnapshot]) -> None:
        self._market_cache.update(snapshots)

    # ── Signal processing ─────────────────────────────────────────────────────

    async def process_signal(self, signal: ScoredSignal) -> list[OrderRequest]:
        """
        Route a signal to all matching strategies, collect proposals,
        resolve conflicts, then submit to risk layer.
        Returns the list of orders actually submitted.
        """
        if signal.recommended_action not in ("buy", "strong_buy"):
            return []

        portfolio = await self._order_manager.get_portfolio()
        risk_state = await self._risk.get_state()

        if risk_state.circuit_breaker_active:
            self._log.warning("circuit_breaker_active_skipping_signal")
            return []

        # Gather proposals from all matching strategies
        proposals: list[OrderRequest] = []
        for strategy in self._active:
            if not strategy.should_handle(signal):
                continue
            try:
                order = await strategy.evaluate(signal, portfolio, risk_state)
                if order:
                    proposals.append(order)
            except Exception as exc:
                self._log.error(
                    "strategy_evaluate_error",
                    strategy=strategy.name,
                    error=str(exc),
                    exc_info=True,
                )

        if not proposals:
            return []

        # Resolve conflicts (same market, opposing directions)
        resolved = self._resolve_conflicts(proposals)

        # Submit to risk + execution
        submitted: list[OrderRequest] = []
        for order in resolved:
            try:
                result = await self._submit_order(order, portfolio, risk_state)
                if result:
                    submitted.append(order)
            except Exception as exc:
                self._log.error("order_submit_error", error=str(exc), exc_info=True)

        return submitted

    async def _submit_order(
        self, order: OrderRequest, portfolio: Portfolio, risk_state: Any
    ) -> bool:
        """Run through risk engine then submit to order manager."""
        risk_decision = await self._risk.evaluate(order, portfolio, risk_state)

        if not risk_decision.approved and risk_decision.adjusted_order is None:
            self._log.info(
                "order_rejected_by_risk",
                order_id=order.order_id,
                violations=risk_decision.violations,
            )
            return False

        final_order = risk_decision.adjusted_order or order
        await self._order_manager.submit_order(final_order, risk_decision)
        return True

    # ── Conflict resolution ───────────────────────────────────────────────────

    def _resolve_conflicts(self, orders: list[OrderRequest]) -> list[OrderRequest]:
        """
        Conflict rules:
          1. Multiple orders for the same market+outcome → take the highest-confidence one
          2. Opposing orders (same market, different outcome) → take higher confidence;
             if within 10 points, drop both
        """
        # Group by market_id
        by_market: dict[str, list[OrderRequest]] = defaultdict(list)
        for order in orders:
            by_market[order.market_id].append(order)

        resolved: list[OrderRequest] = []
        for market_id, market_orders in by_market.items():
            if len(market_orders) == 1:
                resolved.append(market_orders[0])
                continue

            yes_orders = [o for o in market_orders if o.outcome == "Yes"]
            no_orders = [o for o in market_orders if o.outcome == "No"]

            best_yes = max(yes_orders, key=lambda o: o.confidence_score) if yes_orders else None
            best_no = max(no_orders, key=lambda o: o.confidence_score) if no_orders else None

            if best_yes and not best_no:
                resolved.append(best_yes)
            elif best_no and not best_yes:
                resolved.append(best_no)
            elif best_yes and best_no:
                # Conflict
                gap = abs(best_yes.confidence_score - best_no.confidence_score)
                if gap >= 10:
                    winner = best_yes if best_yes.confidence_score > best_no.confidence_score else best_no
                    self._log.info(
                        "conflict_resolved",
                        market=market_id,
                        winner=winner.outcome,
                        gap=gap,
                    )
                    resolved.append(winner)
                else:
                    self._log.warning(
                        "conflict_unresolvable_dropping_both",
                        market=market_id,
                        yes_score=best_yes.confidence_score,
                        no_score=best_no.confidence_score,
                    )

        return resolved

    # ── Periodic position management ──────────────────────────────────────────

    async def _position_management_loop(self) -> None:
        """Every 30 seconds, check all open positions for TP/SL/trailing."""
        while True:
            try:
                await asyncio.sleep(POSITION_CHECK_INTERVAL_S)
                await self.periodic_position_management()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._log.error("position_mgmt_error", error=str(exc), exc_info=True)

    async def periodic_position_management(self) -> None:
        portfolio = await self._order_manager.get_portfolio()
        if not portfolio.positions:
            return

        for position in portfolio.positions:
            snap = self._market_cache.get(position.market_id)
            if not snap:
                continue

            # Find the strategy that owns this position
            strategy = self._all_strategies.get(position.strategy_name)
            if not strategy:
                continue

            try:
                close_order = await strategy.manage_position(position, snap)
                if close_order:
                    risk_state = await self._risk.get_state()
                    await self._submit_order(close_order, portfolio, risk_state)
            except Exception as exc:
                self._log.error(
                    "position_manage_error",
                    position_id=position.position_id,
                    error=str(exc),
                    exc_info=True,
                )

    # ── Whale follow delayed execution ────────────────────────────────────────

    async def _whale_follow_loop(self) -> None:
        """Every 60 seconds, check if any delayed whale follow orders are ready."""
        whale_follow: SingleWhaleFollowStrategy | None = self._all_strategies.get(  # type: ignore
            "whale_single_follow"
        )
        while True:
            try:
                await asyncio.sleep(60)
                if whale_follow and self._market_cache:
                    portfolio = await self._order_manager.get_portfolio()
                    orders = await whale_follow.check_queued(self._market_cache, portfolio)
                    for order in orders:
                        risk_state = await self._risk.get_state()
                        await self._submit_order(order, portfolio, risk_state)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._log.error("whale_follow_loop_error", error=str(exc))

    # ── Arbitrage scan ────────────────────────────────────────────────────────

    async def scan_arbitrage(self, market_pairs: list[tuple[str, str]]) -> list[OrderRequest]:
        """
        Called periodically by scheduler.
        Checks related market pairs for arbitrage opportunities.
        """
        arb: ArbitrageStrategy | None = self._all_strategies.get("swing_arbitrage")  # type: ignore
        if not arb:
            return []

        portfolio = await self._order_manager.get_portfolio()
        orders: list[OrderRequest] = []

        for id_a, id_b in market_pairs:
            snap_a = self._market_cache.get(id_a)
            snap_b = self._market_cache.get(id_b)
            if snap_a and snap_b:
                try:
                    arb_orders = await arb.evaluate_arb(snap_a, snap_b, portfolio)
                    orders.extend(arb_orders)
                except Exception as exc:
                    self._log.error("arb_scan_error", error=str(exc))

        return orders

    # ── Strategy factory ──────────────────────────────────────────────────────

    def _build_strategies(self) -> dict[str, BaseStrategy]:
        s = self._settings.strategy
        r = self._settings.risk

        strategies: dict[str, BaseStrategy] = {
            "swing_mean_reversion": MeanReversionStrategy(
                lookback_days=s.mean_reversion_lookback_days,
                base_position_pct=r.score_to_position.get("buy", 0.03),
            ),
            "swing_breakout": BreakoutStrategy(
                volume_multiplier=s.breakout_confirmation_volume_multiplier,
                base_position_pct=r.score_to_position.get("buy", 0.03),
            ),
            "swing_event_driven": EventDrivenStrategy(
                base_position_pct=r.score_to_position.get("buy", 0.03),
            ),
            "swing_arbitrage": ArbitrageStrategy(),
            "sports_safe_lock": SafeLockStrategy(
                max_remaining_minutes=s.safe_lock_max_remaining_minutes,
                base_position_pct=r.score_to_position.get("strong_buy", 0.06),
            ),
            "sports_reversal": ReversalCatchStrategy(
                max_market_price=s.reversal_max_market_price,
                base_position_pct=r.score_to_position.get("buy", 0.03),
            ),
            "sports_live_ou": LiveOverUnderStrategy(
                base_position_pct=r.score_to_position.get("buy", 0.03),
            ),
            "whale_single_follow": SingleWhaleFollowStrategy(
                follow_delay_minutes=self._settings.whale.follow_delay_minutes,
                min_whale_win_rate=self._settings.whale.min_whale_win_rate,
                base_position_pct=r.score_to_position.get("buy", 0.03),
            ),
            "whale_consensus": MultiWhaleConsensusStrategy(
                min_consensus=self._settings.whale.consensus_min_count,
                min_avg_win_rate=self._settings.whale.min_whale_win_rate,
                base_position_pct=r.score_to_position.get("strong_buy", 0.06),
            ),
            "whale_fade": WhaleFadeStrategy(
                base_position_pct=r.score_to_position.get("buy", 0.03),
            ),
        }
        return strategies

    def get_strategy(self, name: str) -> BaseStrategy | None:
        return self._all_strategies.get(name)
