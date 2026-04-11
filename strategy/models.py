"""strategy/models.py — Core data models for the strategy engine layer."""
from __future__ import annotations
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

def _new_id(): return uuid.uuid4().hex[:16]
def _now_utc(): return datetime.now(timezone.utc)

@dataclass
class OrderRequest:
    order_id: str; signal_id: str; strategy_name: str; market_id: str; market_question: str
    side: str; outcome: str; order_type: str; price: float; size_usd: float; position_pct: float
    confidence_score: int; take_profit: float; stop_loss: float; time_in_force: str
    max_slippage_pct: float; reasoning: str; timestamp: datetime = field(default_factory=_now_utc)

    @classmethod
    def create(cls, **kw):
        kw.setdefault("order_id", _new_id()); kw.setdefault("timestamp", _now_utc())
        kw.setdefault("order_type", "limit"); kw.setdefault("time_in_force", "GTC"); kw.setdefault("max_slippage_pct", 0.02)
        return cls(**kw)
    def to_dict(self):
        d = self.__dict__.copy(); d["timestamp"] = self.timestamp.isoformat(); return d

@dataclass
class Position:
    position_id: str; market_id: str; market_question: str; outcome: str; side: str
    entry_price: float; current_price: float; size_usd: float; unrealized_pnl: float
    unrealized_pnl_pct: float; take_profit: float; stop_loss: float
    trailing_stop: float | None; highest_price_seen: float; strategy_name: str
    signal_id: str; opened_at: datetime; mode: str = "paper"

    @classmethod
    def create(cls, **kw):
        kw.setdefault("position_id", _new_id()); kw.setdefault("opened_at", _now_utc())
        kw.setdefault("trailing_stop", None); kw.setdefault("unrealized_pnl", 0.0); kw.setdefault("unrealized_pnl_pct", 0.0)
        return cls(**kw)
    def update_price(self, current_price):
        self.current_price = current_price
        pnl = (current_price - self.entry_price) * (self.size_usd / self.entry_price)
        self.unrealized_pnl = round(pnl, 4)
        self.unrealized_pnl_pct = round((current_price - self.entry_price) / self.entry_price, 4)
        if current_price > self.highest_price_seen: self.highest_price_seen = current_price

@dataclass
class Portfolio:
    total_balance: float; available_balance: float; total_position_value: float
    positions: list[Position]; daily_pnl: float; daily_pnl_pct: float
    total_trades_today: int; wins_today: int; losses_today: int; mode: str = "paper"

    @property
    def total_exposure_pct(self): return self.total_position_value / self.total_balance if self.total_balance else 0.0
    @property
    def position_count(self): return len(self.positions)
    def get_position(self, market_id, outcome):
        return next((p for p in self.positions if p.market_id == market_id and p.outcome == outcome), None)
    def positions_by_category(self):
        result = {}
        for p in self.positions: result.setdefault(p.strategy_name, []).append(p)
        return result
