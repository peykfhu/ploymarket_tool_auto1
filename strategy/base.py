"""
strategy/base.py

Abstract base class for all strategies.
Each strategy receives a ScoredSignal and the current portfolio state,
and returns an optional OrderRequest.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import structlog

from data_ingestion.polymarket_collector import MarketSnapshot
from signal_processing.models import ScoredSignal
from strategy.models import OrderRequest, Portfolio, Position


class RiskState:
    """Thin placeholder; real RiskState lives in risk/risk_state.py.
    Imported here as a type hint only to avoid circular imports."""
    circuit_breaker_active: bool = False
    daily_pnl_pct: float = 0.0
    daily_trades_count: int = 0


class BaseStrategy(ABC):
    """
    All strategies inherit from this.

    Subclasses must implement:
      - evaluate()         : signal → optional order
      - manage_position()  : open position → optional close/adjust order
      - should_handle()    : fast pre-filter before evaluate() is called
    """

    name: str = "base"
    enabled: bool = True

    def __init__(self) -> None:
        self._log = structlog.get_logger(__name__).bind(strategy=self.name)

    @abstractmethod
    async def evaluate(
        self,
        signal: ScoredSignal,
        portfolio: Portfolio,
        risk_state: Any,
    ) -> OrderRequest | None:
        """
        Evaluate a scored signal and optionally return an order request.
        Return None to pass (no trade).
        """

    @abstractmethod
    async def manage_position(
        self,
        position: Position,
        market_data: MarketSnapshot,
    ) -> OrderRequest | None:
        """
        Called periodically for each open position.
        Return a sell OrderRequest to close/adjust, or None to hold.
        """

    def should_handle(self, signal: ScoredSignal) -> bool:
        """
        Fast pre-filter. Override to restrict signal types.
        Default: accept all signal types if strategy is enabled.
        """
        return self.enabled

    # ── Shared helpers ────────────────────────────────────────────────────────

    def _tp_sl_from_price(
        self,
        entry: float,
        direction: str,
        tp_pct: float = 0.20,
        sl_pct: float = 0.10,
    ) -> tuple[float, float]:
        """
        Compute take-profit and stop-loss prices.
        direction: "buy_yes" → we own YES tokens; price rising is good.
        """
        if direction == "buy_yes":
            tp = min(0.99, entry * (1 + tp_pct))
            sl = max(0.01, entry * (1 - sl_pct))
        else:
            tp = max(0.01, entry * (1 - tp_pct))
            sl = min(0.99, entry * (1 + sl_pct))
        return round(tp, 4), round(sl, 4)

    def _scale_size(self, base_pct: float, score: int, max_pct: float = 0.08) -> float:
        """Scale position size linearly with confidence score (60-100 → base→max)."""
        t = max(0.0, min(1.0, (score - 60) / 40))
        return round(base_pct + t * (max_pct - base_pct), 4)
