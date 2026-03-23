"""
data_ingestion/sports_collector.py

Live sports score collector with second-level latency.
Sources: API-Football (via RapidAPI), The Odds API, ESPN (unofficial).
All events normalized to SportEvent and published to stream:sports.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

import aiohttp
import structlog

from data_ingestion.base import BaseCollector

logger = structlog.get_logger(__name__)


@dataclass
class SportEvent:
    id: str                          # "{sport}:{league}:{home_team}_vs_{away_team}:{date}"
    sport: str                       # football / basketball / nfl / mlb / tennis
    league: str
    home_team: str
    away_team: str
    home_score: int
    away_score: int
    match_status: str                # not_started/first_half/halftime/second_half/finished/etc.
    elapsed_minutes: int
    total_minutes: int               # 90 for football, 48 for basketball, etc.
    remaining_minutes: int
    events: list[dict]               # goals, cards, substitutions, etc.
    stats: dict                      # shots, possession, corners, etc.
    odds: dict                       # live odds per outcome
    momentum: float                  # custom indicator -1.0 (away dominant) to +1.0 (home dominant)
    timestamp: datetime
    external_id: str = ""            # source-specific ID for dedup

    def to_stream_dict(self) -> dict[str, str]:
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        d["events"] = json.dumps(self.events)
        d["stats"] = json.dumps(self.stats)
        d["odds"] = json.dumps(self.odds)
        return {k: str(v) for k, v in d.items()}

    @staticmethod
    def make_id(sport: str, league: str, home: str, away: str, date: str) -> str:
        return f"{sport}:{league}:{home}_vs_{away}:{date}"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _calculate_momentum(events: list[dict], stats: dict, home_team: str) -> float:
    """
    Compute a momentum score from -1.0 to +1.0.
    Positive = home team dominant, Negative = away team dominant.

    Factors:
    - Recent events (goals, cards) in last 15 minutes (weight 0.4)
    - Shots on target ratio (weight 0.3)
    - Possession (weight 0.2)
    - Attack pressure (corners + free kicks) (weight 0.1)
    """
    score = 0.0

    # Recent events (last 15 min)
    now_min = 90  # approximate; real elapsed comes from the API
    recent_events = [e for e in events if e.get("elapsed", 0) >= now_min - 15]
    for evt in recent_events:
        team = evt.get("team", {}).get("name", "")
        evt_type = evt.get("type", "").lower()
        is_home = team == home_team
        sign = 1.0 if is_home else -1.0
        if "goal" in evt_type:
            score += sign * 0.3
        elif "card" in evt_type and "red" in evt.get("detail", "").lower():
            score -= sign * 0.2  # red card hurts the team that received it

    # Shots on target
    home_sot = stats.get("home_shots_on_target", 0) or 0
    away_sot = stats.get("away_shots_on_target", 0) or 0
    total_sot = home_sot + away_sot
    if total_sot > 0:
        sot_ratio = (home_sot - away_sot) / total_sot
        score += sot_ratio * 0.3

    # Possession
    home_poss = stats.get("home_possession", 50) or 50
    poss_diff = (home_poss - 50) / 50  # normalize to -1..+1
    score += poss_diff * 0.2

    # Corners
    home_corners = stats.get("home_corners", 0) or 0
    away_corners = stats.get("away_corners", 0) or 0
    total_corners = home_corners + away_corners
    if total_corners > 0:
        corner_ratio = (home_corners - away_corners) / total_corners
        score += corner_ratio * 0.1

    return max(-1.0, min(1.0, score))


# ─── API-Football Collector ───────────────────────────────────────────────────

class APIFootballCollector(BaseCollector):
    """
    Polls API-Football (via RapidAPI) for live football fixtures.
    Endpoint: GET /fixtures?live=all
    Rate limit: varies by plan; default 10s polling is safe on most tiers.

    Docs: https://www.api-football.com/documentation-v3
    """

    STREAM_KEY = "stream:sports"
    COLLECTOR_NAME = "api_football"
    BASE_URL = "https://api-football-v1.p.rapidapi.com/v3"

    def __init__(
        self,
        redis_manager: Any,
        api_key: str,
        poll_interval_s: int = 10,
        tracked_leagues: list[str] | None = None,
    ) -> None:
        super().__init__(redis_manager)
        self._api_key = api_key
        self._poll_interval_s = poll_interval_s
        self._tracked_leagues = set(tracked_leagues or [])
        self._headers = {
            "X-RapidAPI-Key": api_key,
            "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com",
        }
        # Cache fixture details to compute momentum deltas
        self._fixture_cache: dict[str, dict] = {}

    async def _run_loop(self) -> None:
        async with aiohttp.ClientSession() as session:
            while not self._stop_event.is_set():
                await self._poll_live_fixtures(session)
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self._poll_interval_s)
                except asyncio.TimeoutError:
                    pass

    async def _poll_live_fixtures(self, session: aiohttp.ClientSession) -> None:
        try:
            async with session.get(
                f"{self.BASE_URL}/fixtures",
                headers=self._headers,
                params={"live": "all"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status == 429:
                    self._log.warning("api_football_rate_limited")
                    await asyncio.sleep(30)
                    return
                if resp.status != 200:
                    self._log.warning("api_football_error", status=resp.status)
                    return

                data = await resp.json()
                fixtures = data.get("response", [])
                self._log.debug("api_football_live_fixtures", count=len(fixtures))

                for fixture_data in fixtures:
                    event = self._parse_fixture(fixture_data)
                    if event:
                        await self._publish(event.to_stream_dict())

        except asyncio.TimeoutError:
            self._log.warning("api_football_timeout")
        except Exception as exc:
            self._log.error("api_football_poll_error", error=str(exc))
            raise

    def _parse_fixture(self, data: dict) -> SportEvent | None:
        try:
            fixture = data.get("fixture", {})
            league_info = data.get("league", {})
            teams = data.get("teams", {})
            goals = data.get("goals", {})
            status = data.get("fixture", {}).get("status", {})
            statistics = data.get("statistics", [])

            league_name = league_info.get("name", "")
            if self._tracked_leagues and league_name not in self._tracked_leagues:
                return None

            home_team = teams.get("home", {}).get("name", "")
            away_team = teams.get("away", {}).get("name", "")
            home_score = goals.get("home") or 0
            away_score = goals.get("away") or 0

            status_short = status.get("short", "NS")
            status_map = {
                "NS": "not_started", "1H": "first_half", "HT": "halftime",
                "2H": "second_half", "ET": "extra_time", "BT": "break",
                "P": "penalty", "FT": "finished", "AET": "finished",
                "PEN": "finished", "SUSP": "suspended", "INT": "interrupted",
                "PST": "postponed", "CANC": "cancelled",
            }
            match_status = status_map.get(status_short, status_short.lower())
            elapsed = status.get("elapsed") or 0

            # Parse stats
            stats: dict = {}
            for team_stats in statistics:
                team_name = team_stats.get("team", {}).get("name", "")
                prefix = "home" if team_name == home_team else "away"
                for stat in team_stats.get("statistics", []):
                    stat_type = stat.get("type", "").lower().replace(" ", "_")
                    value = stat.get("value")
                    if value is not None:
                        try:
                            if isinstance(value, str) and value.endswith("%"):
                                value = float(value[:-1])
                            else:
                                value = float(value)
                        except (ValueError, TypeError):
                            value = 0.0
                        stats[f"{prefix}_{stat_type}"] = value

            # Parse fixture events (goals, cards)
            events_raw = data.get("events", [])

            fixture_date = fixture.get("date", "")[:10]
            event_id = SportEvent.make_id("football", league_name, home_team, away_team, fixture_date)

            momentum = _calculate_momentum(events_raw, stats, home_team)

            total_minutes = 90
            remaining = max(0, total_minutes - elapsed)

            # Fetch live odds from cache or leave empty (OddsCollector fills this)
            odds = self._fixture_cache.get(str(fixture.get("id", "")), {}).get("odds", {})

            return SportEvent(
                id=event_id,
                sport="football",
                league=league_name,
                home_team=home_team,
                away_team=away_team,
                home_score=int(home_score),
                away_score=int(away_score),
                match_status=match_status,
                elapsed_minutes=elapsed,
                total_minutes=total_minutes,
                remaining_minutes=remaining,
                events=events_raw,
                stats=stats,
                odds=odds,
                momentum=round(momentum, 3),
                timestamp=_now_utc(),
                external_id=str(fixture.get("id", "")),
            )
        except Exception as exc:
            self._log.warning("fixture_parse_error", error=str(exc), exc_info=True)
            return None


# ─── ESPN Unofficial API (NBA / NFL) ─────────────────────────────────────────

class ESPNCollector(BaseCollector):
    """
    Polls ESPN's unofficial JSON API for NBA and NFL live scores.
    No auth required. Endpoints observed from espn.com network traffic.

    NBA:  https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard
    NFL:  https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard
    """

    STREAM_KEY = "stream:sports"
    COLLECTOR_NAME = "espn"
    ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"

    SPORT_CONFIGS = {
        "basketball/nba": ("basketball", "NBA", 48, 12),    # (sport, league, total_min, period_min)
        "football/nfl": ("nfl", "NFL", 60, 15),
        "baseball/mlb": ("mlb", "MLB", 27, 9),             # 27 outs, 9 per inning
    }

    def __init__(
        self,
        redis_manager: Any,
        poll_interval_s: int = 15,
        tracked_sports: list[str] | None = None,
    ) -> None:
        super().__init__(redis_manager)
        self._poll_interval_s = poll_interval_s
        self._tracked = tracked_sports or list(self.SPORT_CONFIGS.keys())

    async def _run_loop(self) -> None:
        async with aiohttp.ClientSession() as session:
            while not self._stop_event.is_set():
                for sport_path in self._tracked:
                    if self._stop_event.is_set():
                        break
                    await self._poll_sport(session, sport_path)
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self._poll_interval_s)
                except asyncio.TimeoutError:
                    pass

    async def _poll_sport(self, session: aiohttp.ClientSession, sport_path: str) -> None:
        url = f"{self.ESPN_BASE}/{sport_path}/scoreboard"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()

            sport_name, league_name, total_min, period_min = self.SPORT_CONFIGS[sport_path]
            events_raw = data.get("events", [])

            for event_data in events_raw:
                event = self._parse_espn_event(event_data, sport_name, league_name, total_min)
                if event:
                    await self._publish(event.to_stream_dict())

        except asyncio.TimeoutError:
            self._log.warning("espn_timeout", sport=sport_path)
        except Exception as exc:
            self._log.error("espn_error", sport=sport_path, error=str(exc))

    def _parse_espn_event(
        self, data: dict, sport: str, league: str, total_min: int
    ) -> SportEvent | None:
        try:
            competitions = data.get("competitions", [{}])
            comp = competitions[0] if competitions else {}
            competitors = comp.get("competitors", [])

            if len(competitors) < 2:
                return None

            # ESPN puts home team last or marks it with homeAway
            home = next((c for c in competitors if c.get("homeAway") == "home"), competitors[0])
            away = next((c for c in competitors if c.get("homeAway") == "away"), competitors[1])

            home_name = home.get("team", {}).get("displayName", "")
            away_name = away.get("team", {}).get("displayName", "")
            home_score = int(home.get("score", 0) or 0)
            away_score = int(away.get("score", 0) or 0)

            status_type = data.get("status", {}).get("type", {})
            status_name = status_type.get("name", "STATUS_SCHEDULED")
            status_map = {
                "STATUS_SCHEDULED": "not_started",
                "STATUS_IN_PROGRESS": "in_progress",
                "STATUS_HALFTIME": "halftime",
                "STATUS_FINAL": "finished",
                "STATUS_POSTPONED": "postponed",
            }
            match_status = status_map.get(status_name, "unknown")

            # Clock/period → elapsed minutes
            clock = data.get("status", {}).get("displayClock", "0:00")
            period = data.get("status", {}).get("period", 1)
            try:
                parts = clock.split(":")
                clock_seconds = int(parts[0]) * 60 + int(parts[1])
                # For basketball: (period-1)*12 min + (12 - remaining)
                # Approximate elapsed
                period_seconds = (period - 1) * (total_min // 4) * 60
                elapsed_s = period_seconds + ((total_min // 4) * 60 - clock_seconds)
                elapsed = max(0, elapsed_s // 60)
            except Exception:
                elapsed = 0

            event_date = data.get("date", "")[:10]
            event_id = SportEvent.make_id(sport, league, home_name, away_name, event_date)

            return SportEvent(
                id=event_id,
                sport=sport,
                league=league,
                home_team=home_name,
                away_team=away_name,
                home_score=home_score,
                away_score=away_score,
                match_status=match_status,
                elapsed_minutes=elapsed,
                total_minutes=total_min,
                remaining_minutes=max(0, total_min - elapsed),
                events=[],
                stats={},
                odds={},
                momentum=0.0,
                timestamp=_now_utc(),
                external_id=str(data.get("id", "")),
            )
        except Exception as exc:
            self._log.warning("espn_parse_error", error=str(exc))
            return None


# ─── Composite SportsCollector ────────────────────────────────────────────────

class SportsCollector(BaseCollector):
    """
    Façade managing all sports sub-collectors.
    """

    STREAM_KEY = "stream:sports"
    COLLECTOR_NAME = "sports"

    def __init__(
        self,
        redis_manager: Any,
        api_football_key: str = "",
        espn_poll_interval_s: int = 15,
        football_poll_interval_s: int = 10,
        tracked_leagues: list[str] | None = None,
    ) -> None:
        super().__init__(redis_manager)
        self._children: list[BaseCollector] = []

        if api_football_key:
            self._children.append(
                APIFootballCollector(
                    redis_manager, api_football_key,
                    poll_interval_s=football_poll_interval_s,
                    tracked_leagues=tracked_leagues,
                )
            )

        self._children.append(
            ESPNCollector(redis_manager, poll_interval_s=espn_poll_interval_s)
        )

    async def start(self) -> None:
        for child in self._children:
            await child.start()

    async def stop(self) -> None:
        for child in self._children:
            await child.stop()

    async def _run_loop(self) -> None:
        pass  # children manage their own loops
