"""strategy/base.py — Abstract base class for all strategies."""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any
import structlog
from data_ingestion.polymarket_collector import MarketSnapshot
from signal_processing.models import ScoredSignal
from strategy.models import OrderRequest, Portfolio, Position

class RiskState:
    circuit_breaker_active: bool = False
    daily_pnl_pct: float = 0.0
    daily_trades_count: int = 0

class BaseStrategy(ABC):
    name: str = "base"
    enabled: bool = True
    def __init__(self): self._log = structlog.get_logger(__name__).bind(strategy=self.name)

    @abstractmethod
    async def evaluate(self, signal: ScoredSignal, portfolio: Portfolio, risk_state: Any) -> OrderRequest | None: ...
    @abstractmethod
    async def manage_position(self, position: Position, market_data: MarketSnapshot) -> OrderRequest | None: ...
    def should_handle(self, signal: ScoredSignal) -> bool: return self.enabled

    def _tp_sl_from_price(self, entry, direction, tp_pct=0.20, sl_pct=0.10):
        if direction == "buy_yes":
            return min(0.99, entry * (1 + tp_pct)), max(0.01, entry * (1 - sl_pct))
        return max(0.01, entry * (1 - tp_pct)), min(0.99, entry * (1 + sl_pct))

    def _scale_size(self, base_pct, score, max_pct=0.08):
        t = max(0.0, min(1.0, (score - 60) / 40))
        return round(base_pct + t * (max_pct - base_pct), 4)
