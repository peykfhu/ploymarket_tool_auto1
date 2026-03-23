"""
signal_processing/confidence_scorer.py

The core scoring engine. Every signal must pass through here before any order is placed.

Scoring is multi-dimensional, signal-type-specific, and calibrated via
a sigmoid to produce a final 0-100 integer score.

Weights are stored in Redis and can be updated via online learning
(updated from backtest/live trade outcomes).
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import structlog

from signal_processing.models import (
    AnySignal,
    NewsSignal,
    ScoredSignal,
    SportsSignal,
    WhaleSignal,
)

logger = structlog.get_logger(__name__)


# ─── Default weights ──────────────────────────────────────────────────────────

NEWS_WEIGHTS = {
    "speed_advantage": 0.25,
    "source_reliability": 0.20,
    "direction_certainty": 0.20,
    "market_relevance": 0.15,
    "market_liquidity": 0.10,
    "expected_profit": 0.10,
}

SPORTS_WEIGHTS = {
    "edge_quality": 0.30,
    "time_safety": 0.25,
    "historical_scenario": 0.20,
    "momentum_alignment": 0.15,
    "market_liquidity": 0.10,
}

WHALE_WEIGHTS = {
    "whale_win_rate": 0.30,
    "multi_whale_consensus": 0.25,
    "whale_position_size": 0.15,
    "whale_market_type_history": 0.15,
    "market_liquidity": 0.15,
}

# Source tier → max reliability points (out of 1.0)
SOURCE_RELIABILITY_MAP = [
    (["reuters", "apnews", "ap:"], 1.00),
    (["bloomberg", "wsj", "financial times", "ft:"], 0.80),
    (["bbc", "nytimes", "washington post", "guardian"], 0.70),
    (["newsapi", "rss:"], 0.50),
    (["twitter:", "gnews"], 0.35),
]


def _source_score(source: str) -> float:
    src = source.lower()
    for patterns, score in SOURCE_RELIABILITY_MAP:
        if any(p in src for p in patterns):
            return score
    return 0.25


def _sigmoid(x: float, k: float = 6.0) -> float:
    """Sigmoid centred at 0.5, maps [0,1] → (0,1) with steepness k."""
    return 1.0 / (1.0 + math.exp(-k * (x - 0.5)))


def _to_score(raw: float) -> int:
    """Map raw [0,1] → calibrated 0-100 integer via sigmoid."""
    calibrated = _sigmoid(raw)
    return int(round(calibrated * 100))


def _liquidity_score(liquidity_usd: float) -> float:
    """Normalise liquidity into 0-1. Saturates at $50k."""
    return min(1.0, math.log1p(liquidity_usd) / math.log1p(50_000))


# ─── Scorer ───────────────────────────────────────────────────────────────────

class ConfidenceScorer:
    """
    Converts raw signals into a calibrated confidence score (0-100).

    Thresholds:
      >= 80 → strong_buy  (position 4-8% of capital)
      60-79 → buy         (position 2-4%)
      40-59 → watch
      <  40 → skip

    Weights are loaded from Redis on init and can be updated online.
    """

    STRONG_BUY = 80
    BUY = 60
    WATCH = 40

    def __init__(
        self,
        redis_manager: Any | None = None,
        strong_buy_pct: float = 0.06,
        buy_pct: float = 0.03,
    ) -> None:
        self._redis = redis_manager
        self._strong_buy_pct = strong_buy_pct
        self._buy_pct = buy_pct
        self._news_weights = dict(NEWS_WEIGHTS)
        self._sports_weights = dict(SPORTS_WEIGHTS)
        self._whale_weights = dict(WHALE_WEIGHTS)
        self._log = logger.bind(component="confidence_scorer")

    async def load_weights(self) -> None:
        """Load persisted weights from Redis (if available)."""
        if not self._redis:
            return
        try:
            for name, default in [
                ("news", NEWS_WEIGHTS),
                ("sports", SPORTS_WEIGHTS),
                ("whale", WHALE_WEIGHTS),
            ]:
                raw = await self._redis.get(f"scorer:weights:{name}")
                if raw:
                    loaded = json.loads(raw)
                    # Only update keys that exist in defaults (ignore stale keys)
                    target = getattr(self, f"_{name}_weights")
                    for k in default:
                        if k in loaded:
                            target[k] = loaded[k]
        except Exception as exc:
            self._log.warning("weight_load_failed", error=str(exc))

    async def score(self, signal: AnySignal) -> ScoredSignal:
        """Main entry: score any signal type."""
        if isinstance(signal, NewsSignal):
            raw, breakdown = self._score_news(signal)
        elif isinstance(signal, SportsSignal):
            raw, breakdown = self._score_sports(signal)
        elif isinstance(signal, WhaleSignal):
            raw, breakdown = self._score_whale(signal)
        else:
            raise ValueError(f"Unknown signal type: {type(signal)}")

        final_score = _to_score(raw)
        action, pct = self._recommend(final_score)

        self._log.info(
            "signal_scored",
            signal_id=signal.signal_id,
            signal_type=signal.signal_type,
            score=final_score,
            action=action,
            market=signal.market_question[:60],
        )

        return ScoredSignal.create(
            signal=signal,
            score=final_score,
            breakdown=breakdown,
            action=action,
            pct=pct,
        )

    # ── News scoring ──────────────────────────────────────────────────────────

    def _score_news(self, signal: NewsSignal) -> tuple[float, dict]:
        w = self._news_weights

        # 1. Speed advantage (0-1)
        speed = signal.speed_advantage
        speed_pts = speed  # already 0-1

        # 2. Source reliability
        source_pts = _source_score(signal.source_event.source)

        # 3. Direction certainty
        conf = signal.direction_confidence
        if conf >= 0.80:
            direction_pts = 1.0
        elif conf >= 0.60:
            direction_pts = 0.70
        elif conf >= 0.40:
            direction_pts = 0.40
        else:
            direction_pts = 0.15

        # 4. Market relevance
        relevance_pts = signal.market_relevance

        # 5. Market liquidity — requires snapshot; use magnitude as proxy if unavailable
        liquidity_pts = 0.6  # neutral default

        # 6. Expected profit magnitude
        expected_move = abs(signal.expected_price_after - signal.price_before)
        if expected_move >= 0.20:
            profit_pts = 1.0
        elif expected_move >= 0.10:
            profit_pts = 0.70
        elif expected_move >= 0.05:
            profit_pts = 0.40
        else:
            profit_pts = 0.10

        raw = (
            speed_pts * w["speed_advantage"]
            + source_pts * w["source_reliability"]
            + direction_pts * w["direction_certainty"]
            + relevance_pts * w["market_relevance"]
            + liquidity_pts * w["market_liquidity"]
            + profit_pts * w["expected_profit"]
        )

        breakdown = {
            "speed_advantage": round(speed_pts * w["speed_advantage"] * 100, 1),
            "source_reliability": round(source_pts * w["source_reliability"] * 100, 1),
            "direction_certainty": round(direction_pts * w["direction_certainty"] * 100, 1),
            "market_relevance": round(relevance_pts * w["market_relevance"] * 100, 1),
            "market_liquidity": round(liquidity_pts * w["market_liquidity"] * 100, 1),
            "expected_profit": round(profit_pts * w["expected_profit"] * 100, 1),
        }

        return raw, breakdown

    # ── Sports scoring ────────────────────────────────────────────────────────

    def _score_sports(self, signal: SportsSignal) -> tuple[float, dict]:
        w = self._sports_weights

        # 1. Edge quality: how big is the mispricing?
        edge_abs = min(1.0, abs(signal.edge) / 0.30)

        # 2. Time safety: more time remaining = more risk; less = safer lock
        # For safe lock: high time_pressure = good. For reversal: inverse.
        if "safe_lock" in signal.signal_type:
            time_pts = signal.time_pressure          # want late game
        else:
            time_pts = 1.0 - signal.time_pressure    # want time to recover

        # 3. Historical scenario win rate (use reversal_probability as proxy)
        # Low reversal prob = safe; high reversal prob = risky
        historical_pts = 1.0 - signal.reversal_probability

        # 4. Momentum alignment
        # For buy_yes: want positive momentum (home dominant)
        if signal.direction == "buy_yes":
            momentum_pts = (signal.momentum_score + 1.0) / 2.0
        else:
            momentum_pts = (-signal.momentum_score + 1.0) / 2.0

        # 5. Market liquidity proxy
        liquidity_pts = 0.6

        raw = (
            edge_abs * w["edge_quality"]
            + time_pts * w["time_safety"]
            + historical_pts * w["historical_scenario"]
            + momentum_pts * w["momentum_alignment"]
            + liquidity_pts * w["market_liquidity"]
        )

        breakdown = {
            "edge_quality": round(edge_abs * w["edge_quality"] * 100, 1),
            "time_safety": round(time_pts * w["time_safety"] * 100, 1),
            "historical_scenario": round(historical_pts * w["historical_scenario"] * 100, 1),
            "momentum_alignment": round(momentum_pts * w["momentum_alignment"] * 100, 1),
            "market_liquidity": round(liquidity_pts * w["market_liquidity"] * 100, 1),
        }

        return raw, breakdown

    # ── Whale scoring ─────────────────────────────────────────────────────────

    def _score_whale(self, signal: WhaleSignal) -> tuple[float, dict]:
        w = self._whale_weights

        # 1. Win rate
        wr = signal.avg_whale_win_rate
        if wr >= 0.70:
            wr_pts = 1.0
        elif wr >= 0.60:
            wr_pts = 0.75
        elif wr >= 0.55:
            wr_pts = 0.50
        else:
            wr_pts = 0.20

        # 2. Multi-whale consensus
        count = signal.consensus_count
        if count >= 5:
            consensus_pts = 1.0
        elif count >= 3:
            consensus_pts = 0.75
        elif count == 2:
            consensus_pts = 0.50
        else:
            consensus_pts = 0.25

        # 3. Position size (tier-based)
        tier_pts = {"S": 1.0, "A": 0.70, "B": 0.40}.get(signal.consensus_tier, 0.30)

        # 4. Whale market type history (use win rate as proxy here)
        market_type_pts = min(1.0, wr * 1.2)

        # 5. Liquidity proxy
        liquidity_pts = 0.6

        raw = (
            wr_pts * w["whale_win_rate"]
            + consensus_pts * w["multi_whale_consensus"]
            + tier_pts * w["whale_position_size"]
            + market_type_pts * w["whale_market_type_history"]
            + liquidity_pts * w["market_liquidity"]
        )

        breakdown = {
            "whale_win_rate": round(wr_pts * w["whale_win_rate"] * 100, 1),
            "multi_whale_consensus": round(consensus_pts * w["multi_whale_consensus"] * 100, 1),
            "whale_position_size": round(tier_pts * w["whale_position_size"] * 100, 1),
            "whale_market_type_history": round(market_type_pts * w["whale_market_type_history"] * 100, 1),
            "market_liquidity": round(liquidity_pts * w["market_liquidity"] * 100, 1),
        }

        return raw, breakdown

    # ── Recommendation ────────────────────────────────────────────────────────

    def _recommend(self, score: int) -> tuple[str, float]:
        if score >= self.STRONG_BUY:
            return "strong_buy", self._strong_buy_pct
        if score >= self.BUY:
            return "buy", self._buy_pct
        if score >= self.WATCH:
            return "watch", 0.0
        return "skip", 0.0

    # ── Online weight updates ─────────────────────────────────────────────────

    async def update_weights(self, signal: ScoredSignal, actual_pnl_pct: float) -> None:
        """
        Nudge weights toward dimensions that predicted well.
        Uses a simple gradient: reward dimensions that contributed
        to correct direction, penalise those that didn't.

        Called after a trade closes with its realised P&L.
        """
        if not self._redis:
            return

        outcome_sign = 1.0 if actual_pnl_pct > 0 else -1.0
        lr = 0.01  # learning rate

        sig = signal.signal
        if isinstance(sig, NewsSignal):
            weights = self._news_weights
            weight_key = "news"
        elif isinstance(sig, SportsSignal):
            weights = self._sports_weights
            weight_key = "sports"
        elif isinstance(sig, WhaleSignal):
            weights = self._whale_weights
            weight_key = "whale"
        else:
            return

        # Each dimension's contribution to the signal score
        breakdown = signal.score_breakdown
        total_pts = sum(breakdown.values()) or 1.0

        for dim, pts in breakdown.items():
            contribution = pts / total_pts
            # If outcome was good, increase weight for high-contributing dims
            gradient = outcome_sign * contribution * lr
            if dim in weights:
                weights[dim] = max(0.01, min(0.50, weights[dim] + gradient))

        # Renormalise weights to sum to 1.0
        total_w = sum(weights.values())
        for k in weights:
            weights[k] = round(weights[k] / total_w, 4)

        # Persist
        try:
            await self._redis.set(
                f"scorer:weights:{weight_key}",
                json.dumps(weights),
            )
        except Exception as exc:
            self._log.warning("weight_save_failed", error=str(exc))
