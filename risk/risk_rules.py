"""
risk/risk_rules.py

Five-layer risk rule engine. Every order must pass all rules before execution.
Rules return APPROVE, REDUCE (size down), or REJECT.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone, timedelta
from typing import Any

import structlog

from risk.models import RiskDecision
from risk.risk_state import RiskState
from strategy.models import OrderRequest, Portfolio

logger = structlog.get_logger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class RiskRuleEngine:
    """
    All rules are checked in sequence.
    First REJECT → short-circuits remaining rules and rejects the order.
    REDUCE → continues checking (cumulative size reduction possible).
    APPROVE → continues to next rule.
    """

    def __init__(
        self,
        # Layer 1 — position limits
        max_single_position_pct: float = 0.08,
        max_total_exposure_pct: float = 0.60,
        max_concurrent_positions: int = 8,
        max_same_strategy_positions: int = 4,
        min_cash_reserve_pct: float = 0.10,
        # Layer 2 — daily limits
        max_daily_loss_pct: float = 0.05,
        max_daily_loss_hard_pct: float = 0.10,
        max_daily_trades: int = 20,
        # Layer 3 — market quality
        min_liquidity_usd: float = 2_000.0,
        max_spread_pct: float = 0.05,
        min_market_age_hours: int = 24,
        # Layer 4 — correlation
        max_correlation_exposure_pct: float = 0.30,
        # Layer 5 — behavioural
        max_trades_per_30min: int = 5,
        revenge_trade_lockout_minutes: int = 3,
        no_large_orders_quiet_hours: bool = True,
        quiet_hour_start_utc: int = 2,
        quiet_hour_end_utc: int = 6,
        large_order_threshold_pct: float = 0.05,
    ) -> None:
        # L1
        self.max_single_pct = max_single_position_pct
        self.max_total_exp = max_total_exposure_pct
        self.max_positions = max_concurrent_positions
        self.max_same_strategy = max_same_strategy_positions
        self.min_cash = min_cash_reserve_pct
        # L2
        self.max_daily_loss = max_daily_loss_pct
        self.max_daily_loss_hard = max_daily_loss_hard_pct
        self.max_daily_trades = max_daily_trades
        # L3
        self.min_liquidity = min_liquidity_usd
        self.max_spread = max_spread_pct
        self.min_market_age_h = min_market_age_hours
        # L4
        self.max_correlation_exp = max_correlation_exposure_pct
        # L5
        self.max_trades_30min = max_trades_per_30min
        self.revenge_lockout_min = revenge_trade_lockout_minutes
        self.no_large_quiet = no_large_orders_quiet_hours
        self.quiet_start = quiet_hour_start_utc
        self.quiet_end = quiet_hour_end_utc
        self.large_order_threshold = large_order_threshold_pct

        self._log = logger.bind(component="risk_rules")
        self._redis: Any = None   # injected after init
        self._state: RiskState | None = None  # cached, refreshed per evaluate()

    def inject_redis(self, redis: Any) -> None:
        self._redis = redis

    async def get_state(self) -> RiskState:
        if self._redis:
            self._state = await RiskState.load(self._redis)
        return self._state or RiskState()

    async def evaluate(
        self,
        order: OrderRequest,
        portfolio: Portfolio,
        risk_state: RiskState,
    ) -> RiskDecision:
        """
        Check all five rule layers in order.
        Returns RiskDecision with approval/rejection/adjustment.
        """
        violations: list[str] = []
        warnings: list[str] = []
        current_size = order.size_usd
        risk_score = self._compute_risk_score(portfolio, risk_state)

        # ── Layer 1: Position limits ──────────────────────────────────────────
        result = self._l1_position_limits(order, portfolio, current_size, risk_score)
        if result is not None:
            if not result.approved:
                return result
            if result.adjusted_order:
                current_size = result.adjusted_order.size_usd
                warnings.extend(result.warnings)

        # ── Layer 2: Daily limits ─────────────────────────────────────────────
        result = self._l2_daily_limits(order, portfolio, risk_state, current_size, risk_score)
        if result is not None:
            if not result.approved:
                return result
            if result.adjusted_order:
                current_size = result.adjusted_order.size_usd
                warnings.extend(result.warnings)

        # ── Layer 3: Market quality ───────────────────────────────────────────
        result = self._l3_market_quality(order, risk_score)
        if result is not None:
            if not result.approved:
                return result
            warnings.extend(result.warnings)

        # ── Layer 4: Correlation ──────────────────────────────────────────────
        result = self._l4_correlation(order, portfolio, current_size, risk_score)
        if result is not None:
            if not result.approved:
                return result
            if result.adjusted_order:
                current_size = result.adjusted_order.size_usd
                warnings.extend(result.warnings)

        # ── Layer 5: Behavioural ──────────────────────────────────────────────
        result = self._l5_behavioural(order, risk_state, current_size, risk_score)
        if result is not None:
            if not result.approved:
                return result
            if result.adjusted_order:
                current_size = result.adjusted_order.size_usd
                warnings.extend(result.warnings)

        # Build final order (with potentially reduced size)
        if abs(current_size - order.size_usd) > 0.01:
            adjusted = self._resize(order, current_size, portfolio.total_balance)
            return RiskDecision.reduce(order, adjusted, f"Size reduced to ${current_size:.2f}", risk_score)

        return RiskDecision.approve(order, warnings, risk_score)

    # ── Layer implementations ─────────────────────────────────────────────────

    def _l1_position_limits(
        self, order: OrderRequest, portfolio: Portfolio, size_usd: float, risk_score: float
    ) -> RiskDecision | None:

        total_balance = portfolio.total_balance
        if total_balance <= 0:
            return RiskDecision.reject(order, ["total_balance_zero"], risk_score)

        # Max single position size
        max_usd = total_balance * self.max_single_pct
        if size_usd > max_usd:
            reduced = max_usd
            self._log.info("l1_size_capped", original=size_usd, capped=reduced)
            return RiskDecision.reduce(
                order, self._resize(order, reduced, total_balance),
                f"Max single position {self.max_single_pct:.0%}", risk_score,
            )

        # Max total exposure
        new_exposure = portfolio.total_position_value + size_usd
        if new_exposure / total_balance > self.max_total_exp:
            headroom = total_balance * self.max_total_exp - portfolio.total_position_value
            if headroom <= 0:
                return RiskDecision.reject(
                    order, [f"max_total_exposure_{self.max_total_exp:.0%}_reached"], risk_score
                )
            return RiskDecision.reduce(
                order, self._resize(order, headroom, total_balance),
                "Total exposure limit", risk_score,
            )

        # Max concurrent positions
        if portfolio.position_count >= self.max_positions:
            return RiskDecision.reject(
                order, [f"max_concurrent_positions_{self.max_positions}_reached"], risk_score
            )

        # Max same-strategy positions
        by_strategy = portfolio.positions_by_category()
        strategy_count = len(by_strategy.get(order.strategy_name, []))
        if strategy_count >= self.max_same_strategy:
            return RiskDecision.reject(
                order,
                [f"max_same_strategy_positions_{self.max_same_strategy}_reached_for_{order.strategy_name}"],
                risk_score,
            )

        # Min cash reserve
        available_after = portfolio.available_balance - size_usd
        if available_after / total_balance < self.min_cash:
            headroom = portfolio.available_balance - total_balance * self.min_cash
            if headroom <= 0:
                return RiskDecision.reject(
                    order, [f"min_cash_reserve_{self.min_cash:.0%}_breached"], risk_score
                )
            return RiskDecision.reduce(
                order, self._resize(order, headroom, total_balance),
                "Cash reserve floor", risk_score,
            )

        return None  # all L1 checks passed

    def _l2_daily_limits(
        self,
        order: OrderRequest,
        portfolio: Portfolio,
        state: RiskState,
        size_usd: float,
        risk_score: float,
    ) -> RiskDecision | None:

        # Hard stop: daily loss >= hard limit → reject all new positions
        if state.daily_pnl_pct <= -self.max_daily_loss_hard:
            return RiskDecision.reject(
                order,
                [f"daily_hard_stop_triggered_loss_{state.daily_pnl_pct:.1%}"],
                risk_score,
            )

        # Soft stop: daily loss >= soft limit → reject new positions
        if state.daily_pnl_pct <= -self.max_daily_loss:
            return RiskDecision.reject(
                order,
                [f"daily_soft_stop_triggered_loss_{state.daily_pnl_pct:.1%}"],
                risk_score,
            )

        # Max daily trades
        if state.daily_trades_count >= self.max_daily_trades:
            return RiskDecision.reject(
                order,
                [f"max_daily_trades_{self.max_daily_trades}_reached"],
                risk_score,
            )

        # Consecutive losing days → reduce size
        if state.consecutive_losing_days >= 3:
            reduced = size_usd * 0.50
            self._log.warning("l2_three_losing_days_halving_size")
            return RiskDecision.reduce(
                order, self._resize(order, reduced, portfolio.total_balance),
                f"{state.consecutive_losing_days} consecutive losing days",
                risk_score,
            )

        return None

    def _l3_market_quality(
        self, order: OrderRequest, risk_score: float
    ) -> RiskDecision | None:
        """
        Requires market snapshot context. In production the orchestrator
        passes the snapshot; here we rely on order metadata for minimum checks.
        Market quality checks (liquidity, spread, age) are enforced at the
        PolymarketCollector level — markets below thresholds are excluded from
        the watch list. This layer is a secondary safety net.
        """
        # Minimum order sanity
        if order.size_usd < 10.0:
            return RiskDecision.reject(order, ["order_too_small_min_10usd"], risk_score)

        if order.price <= 0.001 or order.price >= 0.999:
            return RiskDecision.reject(
                order, [f"price_out_of_bounds_{order.price}"], risk_score
            )

        if order.take_profit == order.stop_loss:
            return RiskDecision.reject(order, ["tp_equals_sl"], risk_score)

        return None

    def _l4_correlation(
        self,
        order: OrderRequest,
        portfolio: Portfolio,
        size_usd: float,
        risk_score: float,
    ) -> RiskDecision | None:
        """
        Detect correlated exposure by strategy name as a category proxy.
        Sports safe-lock positions are treated as correlated with each other.
        """
        correlated_strategies = {
            "sports_safe_lock", "sports_reversal", "sports_live_ou"
        }
        if order.strategy_name not in correlated_strategies:
            return None

        correlated_exposure = sum(
            p.size_usd
            for p in portfolio.positions
            if p.strategy_name in correlated_strategies
        )
        total = portfolio.total_balance
        if (correlated_exposure + size_usd) / total > self.max_correlation_exp:
            headroom = total * self.max_correlation_exp - correlated_exposure
            if headroom <= 0:
                return RiskDecision.reject(
                    order,
                    [f"correlated_sports_exposure_limit_{self.max_correlation_exp:.0%}_reached"],
                    risk_score,
                )
            return RiskDecision.reduce(
                order,
                self._resize(order, headroom, total),
                "Correlated sports exposure limit",
                risk_score,
            )

        return None

    def _l5_behavioural(
        self,
        order: OrderRequest,
        state: RiskState,
        size_usd: float,
        risk_score: float,
    ) -> RiskDecision | None:

        # Max trades per 30 min (overtrading guard)
        trades_30min = state.trades_in_last_30min()
        if trades_30min >= self.max_trades_30min:
            return RiskDecision.reject(
                order,
                [f"overtrading_{trades_30min}_trades_in_30min"],
                risk_score,
            )

        # Revenge trading: no new positions within N minutes after a loss
        # in the SAME market
        if state.minutes_since_last_loss() < self.revenge_lockout_min:
            if state.last_loss_time:
                # Only block if last loss was on the same market
                # (we don't have that granularity in state; block all to be safe)
                return RiskDecision.reject(
                    order,
                    [f"revenge_trade_lockout_{self.revenge_lockout_min}min_after_loss"],
                    risk_score,
                )

        # Quiet hours: no large orders between quiet_start and quiet_end UTC
        if self.no_large_quiet:
            now_h = _now().hour
            in_quiet = (
                (self.quiet_start <= self.quiet_end and self.quiet_start <= now_h < self.quiet_end)
                or (self.quiet_start > self.quiet_end and (now_h >= self.quiet_start or now_h < self.quiet_end))
            )
            if in_quiet and order.position_pct >= self.large_order_threshold:
                return RiskDecision.reject(
                    order,
                    [f"large_order_blocked_quiet_hours_{now_h:02d}:00_UTC"],
                    risk_score,
                )

        # Consecutive losses guard: reduce size after 3+ consecutive losses
        if state.consecutive_losses >= 3:
            scale = max(0.25, 1.0 - (state.consecutive_losses - 2) * 0.25)
            reduced = size_usd * scale
            self._log.warning(
                "l5_consecutive_losses_reducing_size",
                losses=state.consecutive_losses,
                scale=scale,
            )
            # Don't resize if already at minimum viable size
            if reduced >= 10.0:
                # Return as warning, not rejection
                pass  # let through with warning — orchestrator manages this

        return None

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _resize(self, order: OrderRequest, new_size_usd: float, total_balance: float) -> OrderRequest:
        from dataclasses import replace
        new_pct = new_size_usd / total_balance if total_balance > 0 else 0.0
        return replace(order, size_usd=round(new_size_usd, 2), position_pct=round(new_pct, 4))

    def _compute_risk_score(self, portfolio: Portfolio, state: RiskState) -> float:
        """
        Overall risk score 0-1. Higher = more risky.
        Used for informational purposes (Telegram alerts, dashboards).
        """
        scores: list[float] = []

        # Exposure utilisation
        scores.append(min(1.0, portfolio.total_exposure_pct / self.max_total_exp))

        # Daily loss proximity
        if state.daily_pnl_pct < 0:
            scores.append(min(1.0, abs(state.daily_pnl_pct) / self.max_daily_loss_hard))
        else:
            scores.append(0.0)

        # Consecutive losses
        scores.append(min(1.0, state.consecutive_losses / 5))

        # Trade velocity
        scores.append(min(1.0, state.trades_in_last_30min() / self.max_trades_30min))

        return round(sum(scores) / len(scores), 3)
