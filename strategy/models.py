"""
strategy/models.py

Core data models for the strategy engine layer.
OrderRequest, Position, Portfolio, and supporting types.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any


def _new_id() -> str:
    return uuid.uuid4().hex[:16]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class OrderRequest:
    order_id: str
    signal_id: str
    strategy_name: str
    market_id: str
    market_question: str
    side: str                       # "buy" | "sell"
    outcome: str                    # "Yes" | "No"
    order_type: str                 # "limit" | "market" | "ioc"
    price: float                    # limit price (0-1 on Polymarket)
    size_usd: float                 # notional in USD
    position_pct: float             # fraction of total capital
    confidence_score: int
    take_profit: float              # price target
    stop_loss: float                # stop price
    time_in_force: str              # "GTC" | "IOC" | "GTD"
    max_slippage_pct: float
    reasoning: str
    timestamp: datetime = field(default_factory=_now_utc)

    @classmethod
    def create(cls, **kwargs: Any) -> "OrderRequest":
        kwargs.setdefault("order_id", _new_id())
        kwargs.setdefault("timestamp", _now_utc())
        kwargs.setdefault("order_type", "limit")
        kwargs.setdefault("time_in_force", "GTC")
        kwargs.setdefault("max_slippage_pct", 0.02)
        return cls(**kwargs)

    def to_dict(self) -> dict:
        d = self.__dict__.copy()
        d["timestamp"] = self.timestamp.isoformat()
        return d


@dataclass
class Position:
    position_id: str
    market_id: str
    market_question: str
    outcome: str                    # "Yes" | "No"
    side: str                       # "buy" | "sell"
    entry_price: float
    current_price: float
    size_usd: float
    unrealized_pnl: float
    unrealized_pnl_pct: float
    take_profit: float
    stop_loss: float
    trailing_stop: float | None     # None = not activated
    highest_price_seen: float       # for trailing stop tracking
    strategy_name: str
    signal_id: str
    opened_at: datetime
    mode: str = "paper"             # "live" | "paper" | "backtest"

    @classmethod
    def create(cls, **kwargs: Any) -> "Position":
        kwargs.setdefault("position_id", _new_id())
        kwargs.setdefault("opened_at", _now_utc())
        kwargs.setdefault("trailing_stop", None)
        kwargs.setdefault("unrealized_pnl", 0.0)
        kwargs.setdefault("unrealized_pnl_pct", 0.0)
        return cls(**kwargs)

    def update_price(self, current_price: float) -> None:
        self.current_price = current_price
        pnl = (current_price - self.entry_price) * (self.size_usd / self.entry_price)
        self.unrealized_pnl = round(pnl, 4)
        self.unrealized_pnl_pct = round(
            (current_price - self.entry_price) / self.entry_price, 4
        )
        if current_price > self.highest_price_seen:
            self.highest_price_seen = current_price


@dataclass
class Portfolio:
    total_balance: float
    available_balance: float
    total_position_value: float
    positions: list[Position]
    daily_pnl: float
    daily_pnl_pct: float
    total_trades_today: int
    wins_today: int
    losses_today: int
    mode: str = "paper"

    @property
    def total_exposure_pct(self) -> float:
        if self.total_balance == 0:
            return 0.0
        return self.total_position_value / self.total_balance

    @property
    def position_count(self) -> int:
        return len(self.positions)

    def get_position(self, market_id: str, outcome: str) -> Position | None:
        for p in self.positions:
            if p.market_id == market_id and p.outcome == outcome:
                return p
        return None

    def positions_by_category(self) -> dict[str, list[Position]]:
        """Group positions by strategy name as a proxy for category."""
        result: dict[str, list[Position]] = {}
        for p in self.positions:
            result.setdefault(p.strategy_name, []).append(p)
        return result
