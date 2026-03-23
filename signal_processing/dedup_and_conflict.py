"""
signal_processing/dedup_and_conflict.py

Signal deduplication and conflict detection.

Dedup: prevents multiple signals for the same market+direction within a time window.
Conflict: detects opposing signals for the same market and applies resolution strategy.
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import structlog

from signal_processing.models import AnySignal, ScoredSignal

logger = structlog.get_logger(__name__)


@dataclass
class ConflictRecord:
    market_id: str
    buy_yes_signal: ScoredSignal | None
    buy_no_signal: ScoredSignal | None
    detected_at: datetime

    @property
    def net_score(self) -> int:
        yes_s = self.buy_yes_signal.confidence_score if self.buy_yes_signal else 0
        no_s = self.buy_no_signal.confidence_score if self.buy_no_signal else 0
        return yes_s - no_s


class DedupAndConflictFilter:
    """
    Stateful filter that:
    1. Deduplicates: same market + direction within dedup_window_s → drop duplicate
    2. Detects conflicts: opposing direction signals for same market within conflict_window_s
    3. Resolves conflicts by keeping the higher-confidence signal, or flagging for review
    """

    def __init__(
        self,
        dedup_window_s: int = 300,
        conflict_window_s: int = 600,
        min_score_gap_to_resolve: int = 15,
    ) -> None:
        self._dedup_window_s = dedup_window_s
        self._conflict_window_s = conflict_window_s
        self._min_gap = min_score_gap_to_resolve

        # market_id:direction → (signal_id, timestamp)
        self._seen: dict[str, tuple[str, float]] = {}

        # market_id → list of active ScoredSignals (pending conflict check)
        self._pending: dict[str, list[ScoredSignal]] = defaultdict(list)

        self._log = logger.bind(component="dedup_conflict")

    def process(self, scored: ScoredSignal) -> ScoredSignal | None:
        """
        Returns the signal if it should proceed, None if deduped or conflicted out.
        May return a *different* signal if conflict resolution swaps signals.
        """
        now = time.monotonic()
        market_id = scored.signal.market_id
        direction = scored.signal.direction
        dedup_key = f"{market_id}:{direction}"

        # ── 1. Deduplication ──────────────────────────────────────────────────
        if dedup_key in self._seen:
            prev_id, prev_ts = self._seen[dedup_key]
            age = now - prev_ts
            if age < self._dedup_window_s:
                self._log.debug(
                    "signal_deduped",
                    signal_id=scored.signal.signal_id,
                    prev_id=prev_id,
                    age_s=round(age, 1),
                    market=market_id,
                )
                return None

        self._seen[dedup_key] = (scored.signal.signal_id, now)
        self._cleanup_seen(now)

        # ── 2. Conflict detection ─────────────────────────────────────────────
        opposite_dir = "buy_no" if direction == "buy_yes" else "buy_yes"
        opposite_key = f"{market_id}:{opposite_dir}"

        if opposite_key in self._seen:
            _, opp_ts = self._seen[opposite_key]
            if now - opp_ts < self._conflict_window_s:
                return self._resolve_conflict(scored, now)

        # No conflict — pass through
        self._pending[market_id].append(scored)
        return scored

    def _resolve_conflict(self, incoming: ScoredSignal, now: float) -> ScoredSignal | None:
        """
        Conflict resolution strategy:
        - Gap >= min_score_gap: trust higher-confidence signal, discard lower
        - Gap < min_score_gap: flag both as conflict, discard both (manual review)
        """
        market_id = incoming.signal.market_id
        direction = incoming.signal.direction
        opposite_dir = "buy_no" if direction == "buy_yes" else "buy_yes"

        # Find the opposing signal in pending
        opposing: ScoredSignal | None = None
        for sig in self._pending.get(market_id, []):
            if sig.signal.direction == opposite_dir:
                opposing = sig
                break

        if opposing is None:
            # Opposing signal not in pending (may have already been processed)
            # Let incoming through with a warning
            self._log.warning(
                "conflict_detected_opposing_not_found",
                market=market_id,
                direction=direction,
            )
            return incoming

        incoming_score = incoming.confidence_score
        opposing_score = opposing.confidence_score
        gap = abs(incoming_score - opposing_score)

        self._log.warning(
            "conflict_detected",
            market=market_id,
            direction_a=direction,
            score_a=incoming_score,
            direction_b=opposing_dir,
            score_b=opposing_score,
            gap=gap,
        )

        if gap >= self._min_gap:
            # Clear winner
            if incoming_score > opposing_score:
                self._log.info("conflict_resolved_keep_incoming", score=incoming_score)
                # Remove opposing from pending
                self._pending[market_id] = [
                    s for s in self._pending[market_id] if s.signal.direction != opposite_dir
                ]
                return incoming
            else:
                self._log.info("conflict_resolved_keep_opposing", score=opposing_score)
                return None  # drop incoming, keep opposing (already in pipeline)
        else:
            # Too close to call — drop both
            self._log.warning(
                "conflict_unresolvable_dropping_both",
                market=market_id,
                gap=gap,
            )
            self._pending[market_id] = [
                s for s in self._pending[market_id]
                if s.signal.direction not in (direction, opposite_dir)
            ]
            # Clear from seen so future signals can come through
            self._seen.pop(f"{market_id}:{direction}", None)
            self._seen.pop(f"{market_id}:{opposite_dir}", None)
            return None

    def _cleanup_seen(self, now: float) -> None:
        """Remove stale entries from _seen to prevent unbounded growth."""
        stale_keys = [
            k for k, (_, ts) in self._seen.items()
            if now - ts > max(self._dedup_window_s, self._conflict_window_s) * 2
        ]
        for k in stale_keys:
            del self._seen[k]

        # Also trim pending
        for market_id in list(self._pending.keys()):
            self._pending[market_id] = [
                s for s in self._pending[market_id]
                if (now - s.timestamp.timestamp()) < self._conflict_window_s * 2
            ]
            if not self._pending[market_id]:
                del self._pending[market_id]

    def stats(self) -> dict:
        return {
            "tracked_market_directions": len(self._seen),
            "pending_signals": sum(len(v) for v in self._pending.values()),
        }
