"""
risk/models.py
Shared dataclasses for the risk management layer.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from strategy.models import OrderRequest


@dataclass
class RiskDecision:
    approved: bool
    original_order: OrderRequest
    adjusted_order: Optional[OrderRequest]   # set when REDUCE triggered
    violations: list[str]
    warnings: list[str]
    risk_score: float                        # 0-1, current overall system risk level

    @classmethod
    def approve(cls, order: OrderRequest, warnings: list[str] = None, risk_score: float = 0.0) -> "RiskDecision":
        return cls(approved=True, original_order=order, adjusted_order=None,
                   violations=[], warnings=warnings or [], risk_score=risk_score)

    @classmethod
    def reduce(cls, order: OrderRequest, adjusted: OrderRequest, reason: str, risk_score: float = 0.0) -> "RiskDecision":
        return cls(approved=True, original_order=order, adjusted_order=adjusted,
                   violations=[], warnings=[f"REDUCED: {reason}"], risk_score=risk_score)

    @classmethod
    def reject(cls, order: OrderRequest, violations: list[str], risk_score: float = 0.0) -> "RiskDecision":
        return cls(approved=False, original_order=order, adjusted_order=None,
                   violations=violations, warnings=[], risk_score=risk_score)


@dataclass
class TradeResult:
    trade_id: str
    order_id: str
    market_id: str
    strategy_name: str
    signal_id: str
    side: str
    outcome: str
    entry_price: float
    exit_price: Optional[float]
    size_usd: float
    realized_pnl: Optional[float]
    realized_pnl_pct: Optional[float]
    fee_total: float
    opened_at: datetime
    closed_at: Optional[datetime]
    close_reason: Optional[str]   # "take_profit"|"stop_loss"|"manual"|"time_stop"|"whale_exit"
    mode: str = "paper"


@dataclass
class CircuitBreakerStatus:
    active: bool
    level: str          # "none"|"market"|"session"|"daily"|"hard"
    reason: str
    triggered_at: Optional[datetime]
    resume_at: Optional[datetime]
    affected_markets: list[str]   # empty = all markets
