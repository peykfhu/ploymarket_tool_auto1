"""
execution/models.py
Result dataclasses for the execution layer.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _new_id() -> str:
    return uuid.uuid4().hex[:16]


@dataclass
class OrderResult:
    order_id: str
    status: str              # "filled"|"partially_filled"|"cancelled"|"rejected"|"error"
    fill_price: float
    fill_size: float         # USD value filled
    fill_size_tokens: float  # token quantity filled
    fee: float
    slippage: float          # (fill_price - requested_price) / requested_price
    timestamp: datetime
    error_message: Optional[str] = None
    tx_hash: Optional[str] = None      # live trading only
    raw_response: Optional[dict] = None

    @property
    def is_filled(self) -> bool:
        return self.status in ("filled", "partially_filled")

    @classmethod
    def error(cls, order_id: str, message: str) -> "OrderResult":
        return cls(
            order_id=order_id,
            status="error",
            fill_price=0.0,
            fill_size=0.0,
            fill_size_tokens=0.0,
            fee=0.0,
            slippage=0.0,
            timestamp=_now(),
            error_message=message,
        )


@dataclass
class TradeRecord:
    """Persisted record of a complete open→close trade lifecycle."""
    trade_id: str
    order_id_open: str
    order_id_close: Optional[str]
    market_id: str
    market_question: str
    strategy_name: str
    signal_id: str
    side: str
    outcome: str                     # "Yes" | "No"
    entry_price: float
    exit_price: Optional[float]
    size_usd: float
    realized_pnl: Optional[float]
    realized_pnl_pct: Optional[float]
    fee_total: float
    opened_at: datetime
    closed_at: Optional[datetime]
    close_reason: Optional[str]
    mode: str = "paper"

    @classmethod
    def open(
        cls,
        order_id: str,
        market_id: str,
        market_question: str,
        strategy_name: str,
        signal_id: str,
        side: str,
        outcome: str,
        entry_price: float,
        size_usd: float,
        fee: float,
        mode: str = "paper",
    ) -> "TradeRecord":
        return cls(
            trade_id=_new_id(),
            order_id_open=order_id,
            order_id_close=None,
            market_id=market_id,
            market_question=market_question,
            strategy_name=strategy_name,
            signal_id=signal_id,
            side=side,
            outcome=outcome,
            entry_price=entry_price,
            exit_price=None,
            size_usd=size_usd,
            realized_pnl=None,
            realized_pnl_pct=None,
            fee_total=fee,
            opened_at=_now(),
            closed_at=None,
            close_reason=None,
            mode=mode,
        )

    def close(
        self,
        order_id: str,
        exit_price: float,
        fee: float,
        reason: str,
    ) -> None:
        self.order_id_close = order_id
        self.exit_price = exit_price
        self.closed_at = _now()
        self.close_reason = reason
        self.fee_total += fee
        if self.side == "buy":
            raw_pnl = (exit_price - self.entry_price) * (self.size_usd / self.entry_price)
        else:
            raw_pnl = (self.entry_price - exit_price) * (self.size_usd / self.entry_price)
        self.realized_pnl = round(raw_pnl - self.fee_total, 4)
        self.realized_pnl_pct = round(
            self.realized_pnl / self.size_usd, 4
        ) if self.size_usd > 0 else 0.0
