"""
data_ingestion/manager.py

Collector manager: unified startup/shutdown, health checks, auto-restart.
All collectors write to Redis Streams; this manager orchestrates them.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import structlog

from config.settings import Settings
from data_ingestion.base import BaseCollector, CollectorStatus
from data_ingestion.news_collector import NewsCollector
from data_ingestion.polymarket_collector import PolymarketCollector
from data_ingestion.sports_collector import SportsCollector
from data_ingestion.whale_collector import WhaleCollector

logger = structlog.get_logger(__name__)

HEALTH_CHECK_INTERVAL_S = 30
AUTO_RESTART_AFTER_ERRORS = 5


class CollectorManager:
    """
    Manages the lifecycle of all data collectors.

    Responsibilities:
    - Build and wire up all collectors from settings
    - Start / stop them as a unit
    - Periodic health checks with auto-restart on failure
    - Expose health dashboard for Telegram bot and admin API
    """

    def __init__(self, settings: Settings, redis_manager: Any) -> None:
        self._settings = settings
        self._redis = redis_manager
        self._collectors: dict[str, BaseCollector] = {}
        self._health_task: asyncio.Task | None = None
        self._log = logger.bind(component="collector_manager")

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def build(self) -> None:
        """
        Instantiate all collectors from settings.
        Call once before start().
        """
        s = self._settings

        self._collectors["news"] = NewsCollector(
            redis_manager=self._redis,
            twitter_bearer_token=s.news.twitter_bearer_token.get_secret_value(),
            newsapi_key=s.news.newsapi_key.get_secret_value(),
            rss_feeds=s.news.rss_feeds,
            twitter_watchlist=s.news.twitter_watchlist,
            newsapi_poll_interval_s=s.news.newsapi_poll_interval_s,
            rss_poll_interval_s=s.news.rss_poll_interval_s,
        )

        self._collectors["sports"] = SportsCollector(
            redis_manager=self._redis,
            api_football_key=s.sports.api_football_key.get_secret_value(),
            football_poll_interval_s=s.sports.football_poll_interval_s,
            espn_poll_interval_s=s.sports.basketball_poll_interval_s,
            tracked_leagues=s.sports.tracked_leagues,
        )

        self._collectors["polymarket"] = PolymarketCollector(
            redis_manager=self._redis,
            market_refresh_interval_s=s.polymarket.market_refresh_interval_s,
            price_refresh_interval_s=s.polymarket.price_refresh_interval_s,
            max_markets_to_watch=s.polymarket.max_markets_to_watch,
            min_liquidity_usd=s.risk.min_market_liquidity_usd,
        )

        self._collectors["whale"] = WhaleCollector(
            redis_manager=self._redis,
            rpc_url_ws=s.polymarket.rpc_url_ws,
            rpc_url_http=s.polymarket.rpc_url,
            seed_addresses=s.whale.seed_addresses,
            tier_s_min_usd=s.whale.tier_s_min_usd,
            tier_a_min_usd=s.whale.tier_a_min_usd,
            tier_b_min_usd=s.whale.tier_b_min_usd,
        )

        self._log.info("collectors_built", names=list(self._collectors.keys()))

    async def start_all(self) -> None:
        """Start all collectors concurrently."""
        if not self._collectors:
            self.build()

        start_tasks = [c.start() for c in self._collectors.values()]
        results = await asyncio.gather(*start_tasks, return_exceptions=True)

        for name, result in zip(self._collectors.keys(), results):
            if isinstance(result, Exception):
                self._log.error("collector_start_failed", name=name, error=str(result))
            else:
                self._log.info("collector_started", name=name)

        self._health_task = asyncio.create_task(
            self._health_check_loop(), name="collector_health_check"
        )

    async def stop_all(self) -> None:
        """Gracefully stop all collectors."""
        self._log.info("stopping_all_collectors")

        if self._health_task and not self._health_task.done():
            self._health_task.cancel()
            try:
                await self._health_task
            except asyncio.CancelledError:
                pass

        stop_tasks = [c.stop() for c in self._collectors.values()]
        await asyncio.gather(*stop_tasks, return_exceptions=True)
        self._log.info("all_collectors_stopped")

    async def restart_collector(self, name: str) -> bool:
        """Restart a specific collector by name."""
        collector = self._collectors.get(name)
        if not collector:
            self._log.warning("restart_unknown_collector", name=name)
            return False

        self._log.info("restarting_collector", name=name)
        await collector.stop()
        await asyncio.sleep(2)
        await collector.start()
        self._log.info("collector_restarted", name=name)
        return True

    # ── Health monitoring ─────────────────────────────────────────────────────

    async def _health_check_loop(self) -> None:
        """
        Periodically check all collectors.
        Auto-restart any that are unhealthy or have too many errors.
        """
        while True:
            try:
                await asyncio.sleep(HEALTH_CHECK_INTERVAL_S)
                await self._run_health_checks()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._log.error("health_check_loop_error", error=str(exc))

    async def _run_health_checks(self) -> None:
        for name, collector in self._collectors.items():
            health = collector.health
            if not health.is_healthy:
                self._log.warning(
                    "collector_unhealthy",
                    name=name,
                    status=health.status.value,
                    errors=health.errors_total,
                    last_message_age_s=self._last_message_age_s(health),
                    last_error=health.last_error,
                )

                # Auto-restart if stuck in ERROR state
                if health.status == CollectorStatus.ERROR:
                    self._log.warning("auto_restarting_collector", name=name)
                    await self.restart_collector(name)

    @staticmethod
    def _last_message_age_s(health: Any) -> float:
        if health.last_message_at is None:
            return float("inf")
        return (datetime.now(timezone.utc) - health.last_message_at).total_seconds()

    # ── Status API ────────────────────────────────────────────────────────────

    def get_health_report(self) -> dict[str, Any]:
        """Returns health status of all collectors. Used by admin API + Telegram."""
        return {
            name: collector.health.to_dict()
            for name, collector in self._collectors.items()
        }

    def get_collector(self, name: str) -> BaseCollector | None:
        return self._collectors.get(name)

    @property
    def polymarket_collector(self) -> PolymarketCollector | None:
        c = self._collectors.get("polymarket")
        return c if isinstance(c, PolymarketCollector) else None

    @property
    def whale_collector(self) -> WhaleCollector | None:
        c = self._collectors.get("whale")
        return c if isinstance(c, WhaleCollector) else None
