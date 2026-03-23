"""
data_ingestion/base.py

Abstract base class for all data collectors.
Enforces a unified interface: start/stop/reconnect/heartbeat/error handling.
All collected data is pushed to Redis Streams in a standardized envelope.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class CollectorStatus(str, Enum):
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    RECONNECTING = "reconnecting"
    ERROR = "error"


@dataclass
class CollectorHealth:
    name: str
    status: CollectorStatus
    started_at: datetime | None = None
    last_message_at: datetime | None = None
    messages_total: int = 0
    errors_total: int = 0
    reconnects_total: int = 0
    last_error: str | None = None
    latency_ms_avg: float = 0.0

    @property
    def is_healthy(self) -> bool:
        if self.status != CollectorStatus.RUNNING:
            return False
        if self.last_message_at is None:
            return False
        age_s = (datetime.now(timezone.utc) - self.last_message_at).total_seconds()
        return age_s < 120  # stale if no message in 2 minutes

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "last_message_at": self.last_message_at.isoformat() if self.last_message_at else None,
            "messages_total": self.messages_total,
            "errors_total": self.errors_total,
            "reconnects_total": self.reconnects_total,
            "last_error": self.last_error,
            "latency_ms_avg": round(self.latency_ms_avg, 2),
            "is_healthy": self.is_healthy,
        }


class BaseCollector(ABC):
    """
    Abstract base for all data collectors.

    Subclasses implement _run_loop() which is the inner collection logic.
    This base class handles:
      - start/stop lifecycle
      - automatic reconnection with exponential backoff
      - heartbeat monitoring
      - pushing data to Redis Streams
      - health reporting
    """

    # Override in subclass
    STREAM_KEY: str = "stream:unknown"
    COLLECTOR_NAME: str = "unknown"
    HEARTBEAT_INTERVAL_S: float = 30.0
    MAX_RECONNECT_DELAY_S: float = 60.0
    RECONNECT_BASE_DELAY_S: float = 1.0

    def __init__(self, redis_manager: Any) -> None:
        self._redis = redis_manager
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._health = CollectorHealth(
            name=self.COLLECTOR_NAME,
            status=CollectorStatus.STOPPED,
        )
        self._log = logger.bind(collector=self.COLLECTOR_NAME)
        self._reconnect_delay = self.RECONNECT_BASE_DELAY_S

    # ── Public interface ───────────────────────────────────────────────────────

    async def start(self) -> None:
        """Start the collector. Idempotent."""
        if self._health.status == CollectorStatus.RUNNING:
            self._log.warning("already_running")
            return

        self._stop_event.clear()
        self._health.status = CollectorStatus.STARTING
        self._health.started_at = datetime.now(timezone.utc)

        self._task = asyncio.create_task(self._run_with_reconnect(), name=f"collector_{self.COLLECTOR_NAME}")
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name=f"heartbeat_{self.COLLECTOR_NAME}")
        self._log.info("collector_started")

    async def stop(self) -> None:
        """Gracefully stop the collector."""
        self._log.info("collector_stopping")
        self._stop_event.set()
        self._health.status = CollectorStatus.STOPPED

        for task in (self._task, self._heartbeat_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        await self._on_stop()
        self._log.info("collector_stopped")

    @property
    def health(self) -> CollectorHealth:
        return self._health

    @property
    def is_running(self) -> bool:
        return self._health.status == CollectorStatus.RUNNING

    # ── Abstract interface for subclasses ──────────────────────────────────────

    @abstractmethod
    async def _run_loop(self) -> None:
        """
        Core collection logic. Called repeatedly (with reconnection on failure).
        Should run until self._stop_event is set or raise on unrecoverable error.
        Must call self._publish(data) to emit collected events.
        """

    async def _on_stop(self) -> None:
        """Optional cleanup hook called when stopping."""

    async def _on_reconnect(self) -> None:
        """Optional hook called before each reconnection attempt."""

    # ── Internal machinery ─────────────────────────────────────────────────────

    async def _run_with_reconnect(self) -> None:
        """Wraps _run_loop with automatic reconnection + exponential backoff."""
        while not self._stop_event.is_set():
            try:
                self._health.status = CollectorStatus.RUNNING
                self._reconnect_delay = self.RECONNECT_BASE_DELAY_S
                await self._run_loop()

                # _run_loop returned cleanly → normal stop
                if self._stop_event.is_set():
                    break

            except asyncio.CancelledError:
                break

            except Exception as exc:
                self._health.status = CollectorStatus.RECONNECTING
                self._health.errors_total += 1
                self._health.last_error = str(exc)
                self._log.warning(
                    "collector_error_reconnecting",
                    error=str(exc),
                    delay_s=self._reconnect_delay,
                    exc_info=True,
                )

                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._reconnect_delay,
                    )
                    break  # stop was requested during wait
                except asyncio.TimeoutError:
                    pass

                # Exponential backoff with jitter
                self._reconnect_delay = min(
                    self._reconnect_delay * 2,
                    self.MAX_RECONNECT_DELAY_S,
                )
                self._health.reconnects_total += 1
                await self._on_reconnect()

        self._health.status = CollectorStatus.STOPPED

    async def _heartbeat_loop(self) -> None:
        """Periodically logs health and pushes heartbeat to Redis."""
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(self.HEARTBEAT_INTERVAL_S)
                if self._stop_event.is_set():
                    break

                self._log.debug(
                    "heartbeat",
                    messages=self._health.messages_total,
                    errors=self._health.errors_total,
                    latency_ms=self._health.latency_ms_avg,
                    healthy=self._health.is_healthy,
                )

                # Push heartbeat so the manager can detect stale collectors
                await self._redis.publish_to_stream(
                    "stream:heartbeats",
                    {"collector": self.COLLECTOR_NAME, **self._health.to_dict()},
                )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._log.error("heartbeat_error", error=str(exc))

    async def _publish(self, data: dict[str, Any], stream_key: str | None = None) -> None:
        """
        Push a normalized event dict to Redis Streams.
        Tracks message count and rolling average latency.
        """
        key = stream_key or self.STREAM_KEY
        now = datetime.now(timezone.utc)

        # Track latency if event_time is provided
        if "timestamp" in data:
            try:
                event_time = datetime.fromisoformat(data["timestamp"])
                if event_time.tzinfo is None:
                    event_time = event_time.replace(tzinfo=timezone.utc)
                latency_ms = (now - event_time).total_seconds() * 1000
                # Rolling average (exponential moving average, α=0.1)
                self._health.latency_ms_avg = (
                    0.9 * self._health.latency_ms_avg + 0.1 * latency_ms
                )
            except Exception:
                pass

        self._health.last_message_at = now
        self._health.messages_total += 1

        try:
            await self._redis.publish_to_stream(key, data)
        except Exception as exc:
            self._log.error("publish_failed", stream=key, error=str(exc))
            self._health.errors_total += 1
