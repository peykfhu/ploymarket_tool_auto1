"""
notifications/rate_limiter.py

Push rate control: prevents message spam, batches low-priority
notifications, and enforces quiet hours.
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from datetime import datetime, timezone
from enum import IntEnum
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class Priority(IntEnum):
    CRITICAL = 0    # circuit breaker, hard stop — always sent immediately
    HIGH = 1        # trade fills, stop triggers
    MEDIUM = 2      # signal alerts
    LOW = 3         # whale alerts, system status


@dataclass_like = None   # avoid importing dataclasses at module level


class PendingMessage:
    __slots__ = ("priority", "text", "parse_mode", "queued_at", "photo")

    def __init__(self, priority: Priority, text: str, parse_mode: str = "MarkdownV2", photo: Any = None) -> None:
        self.priority = priority
        self.text = text
        self.parse_mode = parse_mode
        self.queued_at = time.monotonic()
        self.photo = photo


class RateLimiter:
    """
    Token-bucket rate limiter + priority queue for Telegram messages.

    Rules:
    - CRITICAL messages bypass all limits
    - max_per_minute applies to non-critical messages
    - Quiet hours (02:00-06:00 UTC): only CRITICAL and HIGH allowed
    - LOW messages are batched and sent together every batch_interval_s
    """

    def __init__(
        self,
        max_per_minute: int = 10,
        quiet_hour_start: int = 2,
        quiet_hour_end: int = 6,
        batch_interval_s: int = 60,
    ) -> None:
        self._max_per_minute = max_per_minute
        self._quiet_start = quiet_hour_start
        self._quiet_end = quiet_hour_end
        self._batch_interval = batch_interval_s

        # Token bucket: timestamps of recent sends (deque of monotonic times)
        self._sent_times: deque[float] = deque()

        # Priority queues (one per priority level)
        self._queues: dict[Priority, list[PendingMessage]] = {p: [] for p in Priority}

        self._last_batch_flush = time.monotonic()
        self._log = logger.bind(component="rate_limiter")

    def is_quiet_hours(self) -> bool:
        h = datetime.now(timezone.utc).hour
        if self._quiet_start <= self._quiet_end:
            return self._quiet_start <= h < self._quiet_end
        return h >= self._quiet_start or h < self._quiet_end

    def can_send(self) -> bool:
        """Check if we're within rate limit."""
        now = time.monotonic()
        # Evict tokens older than 60 seconds
        while self._sent_times and now - self._sent_times[0] > 60:
            self._sent_times.popleft()
        return len(self._sent_times) < self._max_per_minute

    def record_send(self) -> None:
        self._sent_times.append(time.monotonic())

    def enqueue(self, message: PendingMessage) -> None:
        self._queues[message.priority].append(message)

    def should_batch_flush(self) -> bool:
        return time.monotonic() - self._last_batch_flush >= self._batch_interval

    def flush_batch(self) -> list[PendingMessage]:
        """Return batched LOW priority messages as a single combined message."""
        self._last_batch_flush = time.monotonic()
        low = self._queues[Priority.LOW]
        if not low:
            return []
        batch = list(low)
        self._queues[Priority.LOW].clear()
        return batch

    def pop_next(self) -> PendingMessage | None:
        """Return highest priority pending message, respecting quiet hours."""
        quiet = self.is_quiet_hours()
        for priority in Priority:
            if quiet and priority > Priority.HIGH:
                continue
            queue = self._queues[priority]
            if queue:
                return queue.pop(0)
        return None

    def stats(self) -> dict:
        return {
            "queue_sizes": {p.name: len(q) for p, q in self._queues.items()},
            "sent_last_60s": len(self._sent_times),
            "quiet_hours": self.is_quiet_hours(),
            "can_send": self.can_send(),
        }
