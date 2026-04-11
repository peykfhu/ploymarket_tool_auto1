"""risk/models.py — Shared dataclasses for the risk management layer."""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from strategy.models import OrderRequest

@dataclass
class RiskDecision:
    approved: bool
    original_order: OrderRequest
    adjusted_order: Optional[OrderRequest]
    violations: list[str]
    warnings: list[str]
    risk_score: float

    @classmethod
    def approve(cls, order, warnings=None, risk_score=0.0):
        return cls(True, order, None, [], warnings or [], risk_score)
    @classmethod
    def reduce(cls, order, adjusted, reason, risk_score=0.0):
        return cls(True, order, adjusted, [], [f"REDUCED: {reason}"], risk_score)
    @classmethod
    def reject(cls, order, violations, risk_score=0.0):
        return cls(False, order, None, violations, [], risk_score)

@dataclass
class TradeResult:
    trade_id: str; order_id: str; market_id: str; strategy_name: str; signal_id: str
    side: str; outcome: str; entry_price: float; exit_price: Optional[float]; size_usd: float
    realized_pnl: Optional[float]; realized_pnl_pct: Optional[float]; fee_total: float
    opened_at: datetime; closed_at: Optional[datetime]; close_reason: Optional[str]; mode: str = "paper"

@dataclass
class CircuitBreakerStatus:
    active: bool; level: str; reason: str; triggered_at: Optional[datetime]
    resume_at: Optional[datetime]; affected_markets: list[str]
