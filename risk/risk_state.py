"""
risk/risk_state.py

Real-time risk state manager. All state is persisted in Redis so it
survives restarts and is shared across processes if needed.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

import structlog

logger = structlog.get_logger(__name__)
REDIS_KEY = "risk:state"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt else None


def _from_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


class RiskState:
    """
    Mutable risk state object. Persisted to Redis after every update.
    Load via RiskState.load(redis). Save via state.save(redis).
    """

    def __init__(self) -> None:
        # Daily metrics (reset at UTC 00:00)
        self.daily_pnl: float = 0.0
        self.daily_pnl_pct: float = 0.0
        self.daily_trades_count: int = 0
        self.daily_wins: int = 0
        self.daily_losses: int = 0

        # Streak tracking
        self.consecutive_losses: int = 0
        self.consecutive_wins: int = 0
        self.consecutive_losing_days: int = 0

        # Timing
        self.last_trade_time: Optional[datetime] = None
        self.last_loss_time: Optional[datetime] = None
        self.last_daily_reset: Optional[datetime] = None

        # Sliding window: trades in last 30 min
        self.recent_trade_times: list[str] = []   # ISO strings

        # Circuit breaker
        self.circuit_breaker_active: bool = False
        self.circuit_breaker_level: str = "none"
        self.circuit_breaker_reason: str = ""
        self.circuit_breaker_triggered_at: Optional[datetime] = None
        self.circuit_breaker_resume_at: Optional[datetime] = None
        self.circuit_breaker_affected_markets: list[str] = []

        # System health
        self.api_error_count_1h: int = 0
        self.last_api_error_time: Optional[datetime] = None

    # ── Persistence ───────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        return {
            "daily_pnl": self.daily_pnl,
            "daily_pnl_pct": self.daily_pnl_pct,
            "daily_trades_count": self.daily_trades_count,
            "daily_wins": self.daily_wins,
            "daily_losses": self.daily_losses,
            "consecutive_losses": self.consecutive_losses,
            "consecutive_wins": self.consecutive_wins,
            "consecutive_losing_days": self.consecutive_losing_days,
            "last_trade_time": _iso(self.last_trade_time),
            "last_loss_time": _iso(self.last_loss_time),
            "last_daily_reset": _iso(self.last_daily_reset),
            "recent_trade_times": self.recent_trade_times,
            "circuit_breaker_active": self.circuit_breaker_active,
            "circuit_breaker_level": self.circuit_breaker_level,
            "circuit_breaker_reason": self.circuit_breaker_reason,
            "circuit_breaker_triggered_at": _iso(self.circuit_breaker_triggered_at),
            "circuit_breaker_resume_at": _iso(self.circuit_breaker_resume_at),
            "circuit_breaker_affected_markets": self.circuit_breaker_affected_markets,
            "api_error_count_1h": self.api_error_count_1h,
            "last_api_error_time": _iso(self.last_api_error_time),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "RiskState":
        s = cls()
        s.daily_pnl = float(d.get("daily_pnl", 0))
        s.daily_pnl_pct = float(d.get("daily_pnl_pct", 0))
        s.daily_trades_count = int(d.get("daily_trades_count", 0))
        s.daily_wins = int(d.get("daily_wins", 0))
        s.daily_losses = int(d.get("daily_losses", 0))
        s.consecutive_losses = int(d.get("consecutive_losses", 0))
        s.consecutive_wins = int(d.get("consecutive_wins", 0))
        s.consecutive_losing_days = int(d.get("consecutive_losing_days", 0))
        s.last_trade_time = _from_iso(d.get("last_trade_time"))
        s.last_loss_time = _from_iso(d.get("last_loss_time"))
        s.last_daily_reset = _from_iso(d.get("last_daily_reset"))
        s.recent_trade_times = d.get("recent_trade_times", [])
        s.circuit_breaker_active = bool(d.get("circuit_breaker_active", False))
        s.circuit_breaker_level = d.get("circuit_breaker_level", "none")
        s.circuit_breaker_reason = d.get("circuit_breaker_reason", "")
        s.circuit_breaker_triggered_at = _from_iso(d.get("circuit_breaker_triggered_at"))
        s.circuit_breaker_resume_at = _from_iso(d.get("circuit_breaker_resume_at"))
        s.circuit_breaker_affected_markets = d.get("circuit_breaker_affected_markets", [])
        s.api_error_count_1h = int(d.get("api_error_count_1h", 0))
        s.last_api_error_time = _from_iso(d.get("last_api_error_time"))
        return s

    async def save(self, redis: Any) -> None:
        try:
            await redis.set(REDIS_KEY, json.dumps(self.to_dict()))
        except Exception as exc:
            logger.error("risk_state_save_failed", error=str(exc))

    @classmethod
    async def load(cls, redis: Any) -> "RiskState":
        try:
            raw = await redis.get(REDIS_KEY)
            if raw:
                return cls.from_dict(json.loads(raw))
        except Exception as exc:
            logger.warning("risk_state_load_failed", error=str(exc))
        return cls()

    # ── Mutation helpers ──────────────────────────────────────────────────────

    async def update_after_trade(self, pnl_usd: float, pnl_pct: float, redis: Any) -> None:
        now = _now()
        self.daily_pnl += pnl_usd
        self.daily_trades_count += 1
        self.last_trade_time = now

        # Track in 30-min sliding window
        self.recent_trade_times.append(now.isoformat())
        cutoff = (now - timedelta(minutes=30)).isoformat()
        self.recent_trade_times = [t for t in self.recent_trade_times if t >= cutoff]

        if pnl_pct >= 0:
            self.daily_wins += 1
            self.consecutive_losses = 0
            self.consecutive_wins += 1
        else:
            self.daily_losses += 1
            self.consecutive_wins = 0
            self.consecutive_losses += 1
            self.last_loss_time = now

        # Recompute daily_pnl_pct — caller should pass total_balance
        await self.save(redis)

    async def reset_daily(self, redis: Any) -> None:
        was_losing_day = self.daily_pnl < 0
        self.daily_pnl = 0.0
        self.daily_pnl_pct = 0.0
        self.daily_trades_count = 0
        self.daily_wins = 0
        self.daily_losses = 0
        self.recent_trade_times = []
        self.last_daily_reset = _now()

        if was_losing_day:
            self.consecutive_losing_days += 1
        else:
            self.consecutive_losing_days = 0

        # Auto-clear timed-out circuit breakers
        if (
            self.circuit_breaker_active
            and self.circuit_breaker_resume_at
            and _now() >= self.circuit_breaker_resume_at
        ):
            self.circuit_breaker_active = False
            self.circuit_breaker_level = "none"
            self.circuit_breaker_reason = ""
            logger.info("circuit_breaker_auto_cleared_on_daily_reset")

        await self.save(redis)
        logger.info("risk_state_daily_reset")

    def trades_in_last_30min(self) -> int:
        now = _now().isoformat()
        cutoff = (_now() - timedelta(minutes=30)).isoformat()
        return sum(1 for t in self.recent_trade_times if t >= cutoff)

    def minutes_since_last_loss(self) -> float:
        if not self.last_loss_time:
            return float("inf")
        return (_now() - self.last_loss_time).total_seconds() / 60

    def is_circuit_breaker_expired(self) -> bool:
        if not self.circuit_breaker_active:
            return False
        if not self.circuit_breaker_resume_at:
            return False
        return _now() >= self.circuit_breaker_resume_at
