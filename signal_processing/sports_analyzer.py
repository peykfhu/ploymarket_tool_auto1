"""
signal_processing/sports_analyzer.py

Sports live-state analysis engine.
Uses statistical models (Poisson/Dixon-Coles for football, point-differential
tables for basketball/NFL) to compute win probabilities, then compares against
Polymarket prices to find mispriced markets.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import structlog

from data_ingestion.polymarket_collector import MarketSnapshot
from data_ingestion.sports_collector import SportEvent
from signal_processing.models import SportsSignal

logger = structlog.get_logger(__name__)


# ─── Football: Poisson / Dixon-Coles model ───────────────────────────────────

def _poisson_pmf(k: int, lam: float) -> float:
    """P(X=k) for Poisson(lambda)."""
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return math.exp(-lam) * (lam ** k) / math.factorial(k)


def _football_win_probability(
    home_score: int,
    away_score: int,
    elapsed: int,
    total: int = 90,
    home_xg_rate: float = 1.5,   # expected goals/90min for home team
    away_xg_rate: float = 1.2,   # expected goals/90min for away team
) -> dict[str, float]:
    """
    Poisson-based in-play win probability for football.
    
    Models remaining goals as independent Poisson processes.
    Expected goals in remaining time = rate * (remaining/90).
    Evaluates all score combinations up to +6 goals each.
    
    Returns {"home": p_home_win, "draw": p_draw, "away": p_away_win}
    """
    remaining = max(0, total - elapsed)
    if remaining == 0:
        # Final result
        if home_score > away_score:
            return {"home": 1.0, "draw": 0.0, "away": 0.0}
        if away_score > home_score:
            return {"home": 0.0, "draw": 0.0, "away": 1.0}
        return {"home": 0.0, "draw": 1.0, "away": 0.0}

    # Expected additional goals in remaining time
    time_fraction = remaining / total
    lam_home = home_xg_rate * time_fraction
    lam_away = away_xg_rate * time_fraction

    p_home_win = 0.0
    p_draw = 0.0
    p_away_win = 0.0

    max_goals = 7  # P(>7 goals remaining) is negligible
    for h in range(max_goals):
        for a in range(max_goals):
            p = _poisson_pmf(h, lam_home) * _poisson_pmf(a, lam_away)
            final_home = home_score + h
            final_away = away_score + a
            if final_home > final_away:
                p_home_win += p
            elif final_away > final_home:
                p_away_win += p
            else:
                p_draw += p

    total_p = p_home_win + p_draw + p_away_win
    if total_p == 0:
        return {"home": 1/3, "draw": 1/3, "away": 1/3}

    return {
        "home": round(p_home_win / total_p, 4),
        "draw": round(p_draw / total_p, 4),
        "away": round(p_away_win / total_p, 4),
    }


# ─── Basketball: point-differential win probability ──────────────────────────

def _basketball_win_probability(
    score_diff: int,          # home - away (positive = home leading)
    seconds_remaining: int,
    possessions_per_second: float = 0.033,   # ~2 possessions/min per team
    points_per_possession: float = 1.05,
) -> dict[str, float]:
    """
    Normal-approximation model for basketball.
    
    With N possessions remaining per team, each scoring ~1.05 pts,
    net point change ≈ Normal(0, σ) where σ = sqrt(N) * PPP.
    
    P(home wins) = P(net_change > -score_diff)
    """
    if seconds_remaining <= 0:
        return {"home": 1.0 if score_diff > 0 else (0.5 if score_diff == 0 else 0.0),
                "away": 0.0 if score_diff > 0 else (0.5 if score_diff == 0 else 1.0)}

    n_possessions = seconds_remaining * possessions_per_second
    sigma = math.sqrt(n_possessions) * points_per_possession

    if sigma < 0.001:
        p_home = 1.0 if score_diff > 0 else (0.5 if score_diff == 0 else 0.0)
        return {"home": p_home, "away": 1.0 - p_home}

    # P(home wins) = P(home net - away net > -score_diff)
    # net = Normal(0, sigma*sqrt(2))
    z = score_diff / (sigma * math.sqrt(2))
    p_home = 0.5 * (1 + math.erf(z / math.sqrt(2)))

    return {"home": round(p_home, 4), "away": round(1.0 - p_home, 4)}


# ─── NFL: score + time + possession model ────────────────────────────────────

def _nfl_win_probability(
    score_diff: int,          # positive = home leading
    seconds_remaining: int,
    home_has_possession: bool = False,
) -> dict[str, float]:
    """
    Simplified NFL WP model based on score differential + time.
    Uses logistic regression coefficients estimated from historical NFL data.
    
    Logit(WP) ≈ 0.065 * score_diff + 0.003 * (seconds_remaining ** 0.5)
                - 0.010 * (score_diff * seconds_remaining / 3600)
    """
    if seconds_remaining <= 0:
        if score_diff > 0:
            return {"home": 1.0, "away": 0.0}
        elif score_diff < 0:
            return {"home": 0.0, "away": 1.0}
        else:
            return {"home": 0.5, "away": 0.5}

    logit = (
        0.065 * score_diff
        + 0.003 * math.sqrt(seconds_remaining)
        - 0.010 * score_diff * seconds_remaining / 3600
        + (0.05 if home_has_possession else 0.0)
    )
    p_home = 1 / (1 + math.exp(-logit))

    return {"home": round(p_home, 4), "away": round(1.0 - p_home, 4)}


# ─── Historical comeback rates ────────────────────────────────────────────────

# P(comeback | goal_diff, minutes_remaining) based on historical EPL data
_FOOTBALL_COMEBACK_TABLE: dict[tuple[int, int], float] = {
    (1, 10): 0.12, (1, 20): 0.18, (1, 30): 0.25, (1, 45): 0.32, (1, 60): 0.28,
    (2, 10): 0.02, (2, 20): 0.05, (2, 30): 0.10, (2, 45): 0.15, (2, 60): 0.12,
    (3, 10): 0.00, (3, 20): 0.01, (3, 30): 0.03, (3, 45): 0.05, (3, 60): 0.04,
}


def _football_reversal_prob(goal_diff: int, remaining: int) -> float:
    """Look up historical reversal probability (losing team comes back)."""
    # Snap remaining to nearest bucket
    buckets = [10, 20, 30, 45, 60]
    bucket = min(buckets, key=lambda b: abs(b - remaining))
    diff = min(3, abs(goal_diff))
    return _FOOTBALL_COMEBACK_TABLE.get((diff, bucket), 0.01)


# ─── Main analyzer ────────────────────────────────────────────────────────────

class SportsAnalyzer:
    """
    Analyzes live SportEvent objects against MarketSnapshot data.
    Generates SportsSignal when a statistically significant edge is detected.
    """

    def __init__(
        self,
        min_edge: float = 0.08,
        min_time_remaining: int = 3,
        max_time_remaining_safe_lock: int = 15,
    ) -> None:
        self._min_edge = min_edge
        self._min_time_remaining = min_time_remaining
        self._max_time_safe_lock = max_time_remaining_safe_lock
        self._log = logger.bind(component="sports_analyzer")

    async def analyze(
        self, sport_event: SportEvent, market_snapshot: MarketSnapshot
    ) -> list[SportsSignal]:
        """Main entry point. Returns 0 or 1 SportsSignal."""
        # Don't analyze finished or not-started matches
        if sport_event.match_status in ("finished", "not_started", "postponed", "cancelled"):
            return []
        if sport_event.remaining_minutes < self._min_time_remaining:
            return []

        probs = self._calculate_win_probability(sport_event)
        if not probs:
            return []

        momentum = sport_event.momentum
        opportunity_type = self._classify_opportunity(sport_event, probs, market_snapshot)
        if not opportunity_type:
            return []

        # Which outcome does the market price as YES?
        # Polymarket usually structures sports markets as "Will [home_team] win?"
        home_prob = probs.get("home", 0.5)
        market_yes_price = market_snapshot.prices.get("Yes", 0.5)

        # Detect direction
        edge = home_prob - market_yes_price

        if abs(edge) < self._min_edge:
            return []

        direction = "buy_yes" if edge > 0 else "buy_no"
        calculated_prob = home_prob if edge > 0 else (1.0 - home_prob)

        # Reversal probability (for losing team)
        score_diff = sport_event.home_score - sport_event.away_score
        reversal_prob = 0.0
        if score_diff != 0:
            loser_diff = abs(score_diff)
            reversal_prob = _football_reversal_prob(loser_diff, sport_event.remaining_minutes)

        # Time pressure: how urgent is this signal?
        time_pressure = 1.0 - (sport_event.remaining_minutes / sport_event.total_minutes)
        time_pressure = max(0.0, min(1.0, time_pressure))

        raw_score = (
            min(1.0, abs(edge) / 0.30) * 0.35    # edge quality
            + (1.0 - reversal_prob) * 0.25         # safety
            + (abs(momentum) * 0.5 + 0.5) * 0.20  # momentum alignment
            + time_pressure * 0.20                  # urgency
        )

        signal = SportsSignal.create(
            signal_type=opportunity_type,
            sport_event=sport_event,
            market_id=market_snapshot.market_id,
            market_question=market_snapshot.question,
            direction=direction,
            calculated_probability=round(calculated_prob, 4),
            market_price=market_yes_price,
            edge=round(edge, 4),
            time_pressure=round(time_pressure, 3),
            momentum_score=round(momentum, 3),
            reversal_probability=round(reversal_prob, 3),
            raw_confidence_score=round(raw_score, 3),
            reasoning=(
                f"{sport_event.home_team} {sport_event.home_score}-{sport_event.away_score} "
                f"{sport_event.away_team} ({sport_event.elapsed_minutes}' played). "
                f"Model: {calculated_prob:.1%} vs market: {market_yes_price:.1%}. "
                f"Edge: {edge:+.1%}. Type: {opportunity_type}."
            ),
        )

        self._log.info(
            "sports_signal",
            type=opportunity_type,
            edge=edge,
            direction=direction,
            market=market_snapshot.question[:60],
        )
        return [signal]

    def _calculate_win_probability(self, event: SportEvent) -> dict[str, float] | None:
        try:
            sport = event.sport.lower()
            elapsed = event.elapsed_minutes
            remaining = event.remaining_minutes
            home_s = event.home_score
            away_s = event.away_score

            if sport == "football":
                home_xg = float(event.stats.get("home_shots_on_target", 3)) * 0.35
                away_xg = float(event.stats.get("away_shots_on_target", 2.5)) * 0.35
                home_xg = max(0.5, home_xg)
                away_xg = max(0.4, away_xg)
                return _football_win_probability(home_s, away_s, elapsed, 90, home_xg, away_xg)

            elif sport in ("basketball", "nba"):
                seconds_remaining = remaining * 60
                return _basketball_win_probability(home_s - away_s, seconds_remaining)

            elif sport in ("nfl", "american_football"):
                seconds_remaining = remaining * 60
                return _nfl_win_probability(home_s - away_s, seconds_remaining)

            else:
                # Generic: use basketball model as approximation
                seconds_remaining = remaining * 60
                return _basketball_win_probability(home_s - away_s, seconds_remaining)

        except Exception as exc:
            self._log.warning("win_prob_calc_error", sport=event.sport, error=str(exc))
            return None

    def _classify_opportunity(
        self, event: SportEvent, probs: dict[str, float], snapshot: MarketSnapshot
    ) -> str | None:
        """
        Classify the type of opportunity:
          - sports_safe_lock: large lead, little time, market still underpriced
          - sports_reversal: trailing team has higher model probability
          - sports_live_ou: over/under opportunity based on pace
        """
        remaining = event.remaining_minutes
        home_p = probs.get("home", 0.5)
        score_diff = event.home_score - event.away_score
        market_yes = snapshot.prices.get("Yes", 0.5)

        # Safe lock: home leading big with little time
        if (
            score_diff >= 2
            and remaining <= self._max_time_safe_lock
            and home_p >= 0.85
            and market_yes < home_p - self._min_edge
        ):
            return "sports_safe_lock"

        # Safe lock for away team
        if (
            score_diff <= -2
            and remaining <= self._max_time_safe_lock
            and probs.get("away", 0.5) >= 0.85
            and market_yes > (1 - probs.get("away", 0.5)) + self._min_edge
        ):
            return "sports_safe_lock"

        # Reversal: trailing team model > market implies
        if score_diff < 0 and home_p > market_yes + self._min_edge and remaining > 15:
            return "sports_reversal"
        if score_diff > 0 and (1 - home_p) > (1 - market_yes) + self._min_edge and remaining > 15:
            return "sports_reversal"

        # General edge
        if abs(home_p - market_yes) >= self._min_edge:
            return "sports_edge"

        return None
