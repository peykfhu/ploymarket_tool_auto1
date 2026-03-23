"""
risk/position_sizer.py

Position sizing calculator. Three methods:
  1. fixed_pct         — map confidence score → fixed % of capital
  2. kelly             — half-Kelly based on strategy historical win rate
  3. volatility_adjusted — scale down in high-volatility markets
"""
from __future__ import annotations

import math
from typing import Any

import structlog

from signal_processing.models import ScoredSignal
from strategy.models import Portfolio

logger = structlog.get_logger(__name__)


class PositionSizer:

    def __init__(
        self,
        method: str = "fixed_pct",
        kelly_fraction: float = 0.5,
        max_position_pct: float = 0.08,
        min_position_pct: float = 0.005,
        score_to_pct: dict[str, float] | None = None,
    ) -> None:
        self._method = method
        self._kelly_fraction = kelly_fraction
        self._max_pct = max_position_pct
        self._min_pct = min_position_pct
        self._score_to_pct = score_to_pct or {"strong_buy": 0.06, "buy": 0.03}
        self._log = logger.bind(component="position_sizer")

        # Strategy win-rate cache: strategy_name → (win_rate, avg_win_loss_ratio)
        # Updated from trade history periodically
        self._strategy_stats: dict[str, tuple[float, float]] = {}

    def calculate(
        self,
        signal: ScoredSignal,
        portfolio: Portfolio,
        market_volatility: float = 0.0,
        method: str | None = None,
    ) -> float:
        """
        Returns recommended order size in USD.
        Clamps to [min_position_pct, max_position_pct] of total_balance.
        """
        m = method or self._method
        try:
            if m == "kelly":
                pct = self._kelly(signal)
            elif m == "volatility_adjusted":
                pct = self._volatility_adjusted(signal, market_volatility)
            else:
                pct = self._fixed_pct(signal)
        except Exception as exc:
            self._log.warning("sizer_error_falling_back", method=m, error=str(exc))
            pct = self._fixed_pct(signal)

        pct = max(self._min_pct, min(self._max_pct, pct))
        usd = round(portfolio.total_balance * pct, 2)
        self._log.debug("position_sized", method=m, pct=round(pct, 4), usd=usd)
        return usd

    # ── Methods ───────────────────────────────────────────────────────────────

    def _fixed_pct(self, signal: ScoredSignal) -> float:
        action = signal.recommended_action
        return self._score_to_pct.get(action, self._score_to_pct.get("buy", 0.03))

    def _kelly(self, signal: ScoredSignal) -> float:
        """
        Half-Kelly formula:
          f* = (p * b - q) / b
          actual = f* * kelly_fraction

        p = win probability (from strategy historical win rate or signal confidence)
        b = average win / average loss ratio
        q = 1 - p
        """
        strategy_name = signal.signal.signal_type
        p, b = self._strategy_stats.get(strategy_name, (None, None))

        if p is None:
            # Fall back to signal-derived estimate
            p = max(0.40, min(0.80, signal.confidence_score / 100))
            b = 1.5   # conservative default win/loss ratio

        q = 1.0 - p
        if b <= 0:
            return self._fixed_pct(signal)

        f_star = (p * b - q) / b
        f_star = max(0.0, f_star)   # can't bet negative
        actual = f_star * self._kelly_fraction
        return min(actual, self._max_pct)

    def _volatility_adjusted(self, signal: ScoredSignal, volatility: float) -> float:
        """
        base_position / (1 + volatility * k)
        k = 3.0: 33% vol halves position size
        """
        base = self._fixed_pct(signal)
        k = 3.0
        adjusted = base / (1.0 + volatility * k)
        return max(self._min_pct, adjusted)

    def update_strategy_stats(
        self, strategy_name: str, win_rate: float, avg_win_loss_ratio: float
    ) -> None:
        self._strategy_stats[strategy_name] = (win_rate, avg_win_loss_ratio)
        self._log.debug(
            "strategy_stats_updated",
            strategy=strategy_name,
            win_rate=round(win_rate, 3),
            ratio=round(avg_win_loss_ratio, 3),
        )
