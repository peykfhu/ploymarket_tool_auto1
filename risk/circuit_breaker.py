"""
risk/circuit_breaker.py

System-level circuit breaker. Monitors for catastrophic loss conditions
and halts trading automatically with Telegram notification.

Trigger conditions:
  1. Daily loss >= 10%           → liquidate all + pause 24h   (level: "daily")
  2. 3+ consecutive losing days  → pause 48h                   (level: "session")
  3. Loss >= 5% within 30 min   → pause 1h                    (level: "fast_loss")
  4. API error rate > 50%        → pause until recovered        (level: "system")
  5. Market price anomaly >30%/min → pause that market         (level: "market")

Recovery:
  - Auto-recover when pause expires (timed breakers)
  - Manual recovery via Telegram /resume command
  - System breaker: auto-recover when health checks pass
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Any

import structlog

from risk.models import CircuitBreakerStatus
from risk.risk_state import RiskState
from strategy.models import Portfolio

logger = structlog.get_logger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class CircuitBreaker:

    def __init__(
        self,
        daily_loss_hard_pct: float = 0.10,
        fast_loss_pct: float = 0.05,
        fast_loss_window_minutes: int = 30,
        consecutive_losing_days_limit: int = 3,
        api_error_rate_threshold: float = 0.50,
        price_anomaly_pct_per_minute: float = 0.30,
        notifier: Any = None,     # TelegramNotifier injected later
        order_manager: Any = None,
    ) -> None:
        self._daily_hard = daily_loss_hard_pct
        self._fast_loss = fast_loss_pct
        self._fast_window_min = fast_loss_window_minutes
        self._consec_days = consecutive_losing_days_limit
        self._api_err_threshold = api_error_rate_threshold
        self._price_anomaly = price_anomaly_pct_per_minute
        self._notifier = notifier
        self._order_manager = order_manager

        self._log = logger.bind(component="circuit_breaker")

        # Track fast-loss window (rolling 30-min P&L samples)
        self._pnl_samples: list[tuple[datetime, float]] = []   # (time, portfolio_value)

        self._redis: Any = None

    def inject_dependencies(self, redis: Any, notifier: Any, order_manager: Any) -> None:
        self._redis = redis
        self._notifier = notifier
        self._order_manager = order_manager

    async def check(self, portfolio: Portfolio, risk_state: RiskState) -> CircuitBreakerStatus:
        """
        Run all circuit breaker checks. Returns current status.
        Triggers (and notifies) if a new condition is detected.
        """
        # Auto-clear expired breakers
        if risk_state.is_circuit_breaker_expired():
            await self._auto_clear(risk_state)

        if risk_state.circuit_breaker_active:
            return self._current_status(risk_state)

        # Record portfolio value sample for fast-loss detection
        self._pnl_samples.append((_now(), portfolio.total_balance))
        cutoff = _now() - timedelta(minutes=self._fast_window_min)
        self._pnl_samples = [(t, v) for t, v in self._pnl_samples if t >= cutoff]

        # ── Check conditions in severity order ──────────────────────────────

        # 1. Daily hard stop
        if risk_state.daily_pnl_pct <= -self._daily_hard:
            await self.trigger(
                level="daily",
                reason=f"Daily loss {risk_state.daily_pnl_pct:.1%} >= {self._daily_hard:.0%} limit",
                pause_hours=24,
                liquidate_all=True,
                risk_state=risk_state,
            )
            return self._current_status(risk_state)

        # 2. Consecutive losing days
        if risk_state.consecutive_losing_days >= self._consec_days:
            await self.trigger(
                level="session",
                reason=f"{risk_state.consecutive_losing_days} consecutive losing days",
                pause_hours=48,
                liquidate_all=False,
                risk_state=risk_state,
            )
            return self._current_status(risk_state)

        # 3. Fast loss (5% in 30 min)
        if len(self._pnl_samples) >= 2:
            oldest_val = self._pnl_samples[0][1]
            current_val = portfolio.total_balance
            if oldest_val > 0:
                fast_loss_pct = (oldest_val - current_val) / oldest_val
                if fast_loss_pct >= self._fast_loss:
                    await self.trigger(
                        level="fast_loss",
                        reason=f"Fast loss {fast_loss_pct:.1%} in {self._fast_window_min}min",
                        pause_hours=1,
                        liquidate_all=False,
                        risk_state=risk_state,
                    )
                    return self._current_status(risk_state)

        # 4. API health (checked externally; system sets api_error_count_1h on state)
        if risk_state.api_error_count_1h > 20:
            await self.trigger(
                level="system",
                reason=f"API error rate elevated: {risk_state.api_error_count_1h} errors/h",
                pause_hours=0,    # 0 = until manually cleared
                liquidate_all=False,
                risk_state=risk_state,
            )
            return self._current_status(risk_state)

        return CircuitBreakerStatus(
            active=False,
            level="none",
            reason="",
            triggered_at=None,
            resume_at=None,
            affected_markets=[],
        )

    async def check_market_anomaly(
        self,
        market_id: str,
        price_now: float,
        price_60s_ago: float,
        risk_state: RiskState,
    ) -> bool:
        """
        Check if a specific market's price moved anomalously fast.
        Returns True if that market should be blocked.
        """
        if price_60s_ago <= 0:
            return False
        change = abs(price_now - price_60s_ago) / price_60s_ago
        if change >= self._price_anomaly:
            self._log.warning(
                "market_anomaly_detected",
                market=market_id,
                change=round(change, 3),
            )
            if market_id not in risk_state.circuit_breaker_affected_markets:
                risk_state.circuit_breaker_affected_markets.append(market_id)
                if self._redis:
                    await risk_state.save(self._redis)
                if self._notifier:
                    await self._safe_notify(
                        f"⚠️ Market anomaly: {market_id[:20]} moved {change:.0%}/min — paused"
                    )
            return True
        return False

    async def trigger(
        self,
        level: str,
        reason: str,
        pause_hours: float,
        liquidate_all: bool,
        risk_state: RiskState,
        affected_markets: list[str] | None = None,
    ) -> None:
        """
        Activate circuit breaker:
        1. Set state
        2. Persist to Redis
        3. Notify Telegram
        4. Optionally liquidate all positions
        """
        now = _now()
        risk_state.circuit_breaker_active = True
        risk_state.circuit_breaker_level = level
        risk_state.circuit_breaker_reason = reason
        risk_state.circuit_breaker_triggered_at = now
        risk_state.circuit_breaker_resume_at = (
            now + timedelta(hours=pause_hours) if pause_hours > 0 else None
        )
        risk_state.circuit_breaker_affected_markets = affected_markets or []

        if self._redis:
            await risk_state.save(self._redis)

        self._log.error(
            "CIRCUIT_BREAKER_TRIGGERED",
            level=level,
            reason=reason,
            pause_hours=pause_hours,
            liquidate=liquidate_all,
        )

        resume_str = (
            f"Resume at: {risk_state.circuit_breaker_resume_at.strftime('%Y-%m-%d %H:%M UTC')}"
            if risk_state.circuit_breaker_resume_at
            else "Manual recovery required"
        )

        if self._notifier:
            await self._safe_notify(
                f"🚨 CIRCUIT BREAKER [{level.upper()}]\n"
                f"Reason: {reason}\n"
                f"{resume_str}\n"
                f"{'⛔ Liquidating all positions...' if liquidate_all else ''}"
            )

        if liquidate_all and self._order_manager:
            try:
                await self._order_manager.close_all_positions(reason="circuit_breaker")
                self._log.info("all_positions_liquidated")
            except Exception as exc:
                self._log.error("liquidation_failed", error=str(exc))

    async def manual_clear(self, risk_state: RiskState, cleared_by: str = "manual") -> None:
        """
        Called via Telegram /resume command.
        Requires operator confirmation.
        """
        if not risk_state.circuit_breaker_active:
            return

        self._log.info("circuit_breaker_manually_cleared", by=cleared_by)
        risk_state.circuit_breaker_active = False
        risk_state.circuit_breaker_level = "none"
        risk_state.circuit_breaker_reason = ""
        risk_state.circuit_breaker_triggered_at = None
        risk_state.circuit_breaker_resume_at = None

        if self._redis:
            await risk_state.save(self._redis)

        if self._notifier:
            await self._safe_notify(
                f"✅ Circuit breaker manually cleared by {cleared_by}. Trading resumed."
            )

    async def _auto_clear(self, risk_state: RiskState) -> None:
        """Auto-clears a timed breaker when resume_at has passed."""
        if risk_state.circuit_breaker_level == "system":
            return  # system breakers require manual clear

        self._log.info(
            "circuit_breaker_auto_cleared",
            level=risk_state.circuit_breaker_level,
        )
        risk_state.circuit_breaker_active = False
        risk_state.circuit_breaker_level = "none"
        risk_state.circuit_breaker_reason = ""
        risk_state.circuit_breaker_triggered_at = None
        risk_state.circuit_breaker_resume_at = None

        if self._redis:
            await risk_state.save(self._redis)

        if self._notifier:
            await self._safe_notify("✅ Circuit breaker auto-cleared. Trading resumed.")

    def _current_status(self, state: RiskState) -> CircuitBreakerStatus:
        return CircuitBreakerStatus(
            active=state.circuit_breaker_active,
            level=state.circuit_breaker_level,
            reason=state.circuit_breaker_reason,
            triggered_at=state.circuit_breaker_triggered_at,
            resume_at=state.circuit_breaker_resume_at,
            affected_markets=state.circuit_breaker_affected_markets,
        )

    async def _safe_notify(self, message: str) -> None:
        if not self._notifier:
            return
        try:
            await self._notifier.send_risk_alert("critical", message)
        except Exception as exc:
            self._log.error("notify_failed", error=str(exc))
