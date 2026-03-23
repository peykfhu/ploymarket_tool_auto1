"""
signal_processing/models.py

Shared dataclasses for all signal types produced by the signal processing layer.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Union

from data_ingestion.news_collector import NewsEvent
from data_ingestion.sports_collector import SportEvent
from data_ingestion.whale_collector import WhaleAction


def _new_id() -> str:
    return uuid.uuid4().hex[:16]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ─── News Signal ──────────────────────────────────────────────────────────────

@dataclass
class NewsSignal:
    signal_id: str
    signal_type: str                     # "news_driven"
    source_event: NewsEvent
    market_id: str
    market_question: str
    direction: str                       # "buy_yes" | "buy_no"
    direction_confidence: float          # 0-1
    market_relevance: float              # 0-1
    speed_advantage: float               # 0-1
    price_before: float
    expected_price_after: float
    news_magnitude: float                # 0-1
    raw_confidence_score: float
    timestamp: datetime
    reasoning: str

    @classmethod
    def create(cls, **kwargs: Any) -> "NewsSignal":
        kwargs.setdefault("signal_id", _new_id())
        kwargs.setdefault("signal_type", "news_driven")
        kwargs.setdefault("timestamp", _now_utc())
        return cls(**kwargs)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        d["source_event"] = {
            "id": self.source_event.id,
            "source": self.source_event.source,
            "headline": self.source_event.headline,
            "timestamp": self.source_event.timestamp.isoformat(),
        }
        return d


# ─── Sports Signal ────────────────────────────────────────────────────────────

@dataclass
class SportsSignal:
    signal_id: str
    signal_type: str               # "sports_safe_lock" | "sports_reversal" | "sports_live_ou"
    sport_event: SportEvent
    market_id: str
    market_question: str
    direction: str                 # "buy_yes" | "buy_no"
    calculated_probability: float  # model probability
    market_price: float
    edge: float                    # calculated_prob - market_price
    time_pressure: float           # 0-1 urgency
    momentum_score: float          # -1 to 1
    reversal_probability: float
    raw_confidence_score: float
    timestamp: datetime
    reasoning: str

    @classmethod
    def create(cls, **kwargs: Any) -> "SportsSignal":
        kwargs.setdefault("signal_id", _new_id())
        kwargs.setdefault("timestamp", _now_utc())
        return cls(**kwargs)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        d["sport_event"] = {
            "id": self.sport_event.id,
            "sport": self.sport_event.sport,
            "league": self.sport_event.league,
            "home_team": self.sport_event.home_team,
            "away_team": self.sport_event.away_team,
            "home_score": self.sport_event.home_score,
            "away_score": self.sport_event.away_score,
            "elapsed_minutes": self.sport_event.elapsed_minutes,
            "remaining_minutes": self.sport_event.remaining_minutes,
            "match_status": self.sport_event.match_status,
        }
        return d


# ─── Whale Signal ─────────────────────────────────────────────────────────────

@dataclass
class WhaleSignal:
    signal_id: str
    signal_type: str               # "whale_follow" | "whale_consensus" | "whale_fade"
    whale_actions: list[WhaleAction]
    market_id: str
    market_question: str
    direction: str                 # "buy_yes" | "buy_no"
    consensus_count: int           # number of whales agreeing
    consensus_tier: str            # highest tier among agreeing whales
    whale_win_rates: list[float]
    avg_whale_win_rate: float
    total_whale_usd: float
    raw_confidence_score: float
    timestamp: datetime
    reasoning: str

    @classmethod
    def create(cls, **kwargs: Any) -> "WhaleSignal":
        kwargs.setdefault("signal_id", _new_id())
        kwargs.setdefault("timestamp", _now_utc())
        return cls(**kwargs)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        d["whale_actions"] = [
            {"address": a.whale_address, "action": a.action, "amount_usd": a.amount_usd}
            for a in self.whale_actions
        ]
        return d


AnySignal = Union[NewsSignal, SportsSignal, WhaleSignal]


# ─── Scored Signal ────────────────────────────────────────────────────────────

@dataclass
class ScoredSignal:
    signal: AnySignal
    confidence_score: int               # 0-100
    score_breakdown: dict[str, float]   # dimension → points
    recommended_action: str             # "strong_buy" | "buy" | "watch" | "skip"
    recommended_position_pct: float     # fraction of total capital
    timestamp: datetime

    @classmethod
    def create(cls, signal: AnySignal, score: int, breakdown: dict, action: str, pct: float) -> "ScoredSignal":
        return cls(
            signal=signal,
            confidence_score=score,
            score_breakdown=breakdown,
            recommended_action=action,
            recommended_position_pct=pct,
            timestamp=_now_utc(),
        )

    def to_stream_dict(self) -> dict[str, str]:
        sig_dict = self.signal.to_dict()
        return {
            "signal_id": self.signal.signal_id,
            "signal_type": self.signal.signal_type,
            "market_id": self.signal.market_id,
            "market_question": self.signal.market_question,
            "direction": self.signal.direction,
            "confidence_score": str(self.confidence_score),
            "score_breakdown": json.dumps(self.score_breakdown),
            "recommended_action": self.recommended_action,
            "recommended_position_pct": str(self.recommended_position_pct),
            "timestamp": self.timestamp.isoformat(),
            "signal_data": json.dumps(sig_dict),
        }
