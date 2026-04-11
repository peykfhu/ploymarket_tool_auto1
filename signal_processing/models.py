"""signal_processing/models.py — Shared dataclasses for all signal types."""
from __future__ import annotations
import json, uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Union
from data_ingestion.news_collector import NewsEvent
from data_ingestion.sports_collector import SportEvent
from data_ingestion.whale_collector import WhaleAction

def _new_id(): return uuid.uuid4().hex[:16]
def _now_utc(): return datetime.now(timezone.utc)

@dataclass
class NewsSignal:
    signal_id: str; signal_type: str; source_event: NewsEvent; market_id: str; market_question: str
    direction: str; direction_confidence: float; market_relevance: float; speed_advantage: float
    price_before: float; expected_price_after: float; news_magnitude: float
    raw_confidence_score: float; timestamp: datetime; reasoning: str
    @classmethod
    def create(cls, **kw):
        kw.setdefault("signal_id", _new_id()); kw.setdefault("signal_type", "news_driven"); kw.setdefault("timestamp", _now_utc())
        return cls(**kw)
    def to_dict(self):
        d = asdict(self); d["timestamp"] = self.timestamp.isoformat()
        d["source_event"] = {"id": self.source_event.id, "source": self.source_event.source, "headline": self.source_event.headline, "timestamp": self.source_event.timestamp.isoformat()}
        return d

@dataclass
class SportsSignal:
    signal_id: str; signal_type: str; sport_event: SportEvent; market_id: str; market_question: str
    direction: str; calculated_probability: float; market_price: float; edge: float
    time_pressure: float; momentum_score: float; reversal_probability: float
    raw_confidence_score: float; timestamp: datetime; reasoning: str
    @classmethod
    def create(cls, **kw):
        kw.setdefault("signal_id", _new_id()); kw.setdefault("timestamp", _now_utc()); return cls(**kw)
    def to_dict(self):
        d = asdict(self); d["timestamp"] = self.timestamp.isoformat()
        e = self.sport_event
        d["sport_event"] = {"id": e.id, "sport": e.sport, "league": e.league, "home_team": e.home_team, "away_team": e.away_team, "home_score": e.home_score, "away_score": e.away_score, "elapsed_minutes": e.elapsed_minutes, "remaining_minutes": e.remaining_minutes, "match_status": e.match_status}
        return d

@dataclass
class WhaleSignal:
    signal_id: str; signal_type: str; whale_actions: list[WhaleAction]; market_id: str
    market_question: str; direction: str; consensus_count: int; consensus_tier: str
    whale_win_rates: list[float]; avg_whale_win_rate: float; total_whale_usd: float
    raw_confidence_score: float; timestamp: datetime; reasoning: str
    @classmethod
    def create(cls, **kw):
        kw.setdefault("signal_id", _new_id()); kw.setdefault("timestamp", _now_utc()); return cls(**kw)
    def to_dict(self):
        d = asdict(self); d["timestamp"] = self.timestamp.isoformat()
        d["whale_actions"] = [{"address": a.whale_address, "action": a.action, "amount_usd": a.amount_usd} for a in self.whale_actions]
        return d

AnySignal = Union[NewsSignal, SportsSignal, WhaleSignal]

@dataclass
class ScoredSignal:
    signal: AnySignal; confidence_score: int; score_breakdown: dict[str, float]
    recommended_action: str; recommended_position_pct: float; timestamp: datetime
    @classmethod
    def create(cls, signal, score, breakdown, action, pct):
        return cls(signal=signal, confidence_score=score, score_breakdown=breakdown,
                   recommended_action=action, recommended_position_pct=pct, timestamp=_now_utc())
    def to_stream_dict(self):
        return {"signal_id": self.signal.signal_id, "signal_type": self.signal.signal_type,
                "market_id": self.signal.market_id, "market_question": self.signal.market_question,
                "direction": self.signal.direction, "confidence_score": str(self.confidence_score),
                "score_breakdown": json.dumps(self.score_breakdown), "recommended_action": self.recommended_action,
                "recommended_position_pct": str(self.recommended_position_pct),
                "timestamp": self.timestamp.isoformat(), "signal_data": json.dumps(self.signal.to_dict())}
