"""
core/scheduler.py
APScheduler-based task scheduler for all periodic system jobs.
"""
from __future__ import annotations
import asyncio
from datetime import datetime, timezone
from typing import Any
import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

logger = structlog.get_logger(__name__)


class Scheduler:
    def __init__(self, system: Any) -> None:
        self._system = system
        self._scheduler = AsyncIOScheduler(timezone="UTC")
        self._log = logger.bind(component="scheduler")

    def start(self) -> None:
        s = self._system

        # Every 30s — position TP/SL checks
        self._scheduler.add_job(
            s.check_positions, IntervalTrigger(seconds=30), id="check_positions",
            max_instances=1, coalesce=True,
        )
        # Every 1 min — update market data cache
        self._scheduler.add_job(
            s.refresh_market_cache, IntervalTrigger(minutes=1), id="refresh_markets",
            max_instances=1, coalesce=True,
        )
        # Every 1h — update whale performance stats
        self._scheduler.add_job(
            s.update_whale_stats, IntervalTrigger(hours=1), id="whale_stats",
            max_instances=1, coalesce=True,
        )
        # Every 5 min — circuit breaker check
        self._scheduler.add_job(
            s.run_circuit_breaker_check, IntervalTrigger(minutes=5), id="circuit_breaker",
            max_instances=1, coalesce=True,
        )
        # Daily at 00:00 UTC — reset daily state + send report
        self._scheduler.add_job(
            s.daily_reset, CronTrigger(hour=0, minute=0), id="daily_reset",
        )
        # Every Sunday 08:00 UTC — weekly report
        self._scheduler.add_job(
            s.send_weekly_report, CronTrigger(day_of_week="sun", hour=8), id="weekly_report",
        )
        # Every 5 min — arbitrage scan
        self._scheduler.add_job(
            s.scan_arbitrage, IntervalTrigger(minutes=5), id="arb_scan",
            max_instances=1, coalesce=True,
        )
        # Every 30 min — snapshot open positions to DB
        self._scheduler.add_job(
            s.snapshot_positions, IntervalTrigger(minutes=30), id="position_snapshot",
            max_instances=1, coalesce=True,
        )

        self._scheduler.start()
        self._log.info("scheduler_started", jobs=len(self._scheduler.get_jobs()))

    def stop(self) -> None:
        self._scheduler.shutdown(wait=False)
        self._log.info("scheduler_stopped")
