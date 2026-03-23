"""
signal_processing/pipeline.py

Signal processing pipeline.

Consumes raw events from Redis Streams:
  stream:news    → NewsAnalyzer
  stream:sports  → SportsAnalyzer (needs matching market snapshot)
  stream:whales  → WhaleSignal builder

Each raw event is:
  1. Routed to the appropriate analyzer
  2. Scored by ConfidenceScorer
  3. Filtered by DedupAndConflictFilter
  4. Published to stream:scored_signals
  5. Sent to Telegram notifier (if score >= buy threshold)
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

import structlog

from data_ingestion.news_collector import NewsEvent
from data_ingestion.polymarket_collector import MarketSnapshot
from data_ingestion.sports_collector import SportEvent
from data_ingestion.whale_collector import WhaleAction
from signal_processing.confidence_scorer import ConfidenceScorer
from signal_processing.dedup_and_conflict import DedupAndConflictFilter
from signal_processing.models import (
    AnySignal,
    NewsSignal,
    ScoredSignal,
    SportsSignal,
    WhaleSignal,
)
from signal_processing.news_analyzer import NewsAnalyzer
from signal_processing.sports_analyzer import SportsAnalyzer

logger = structlog.get_logger(__name__)


class WhaleSignalBuilder:
    """
    Aggregates WhaleActions into WhaleSignals.
    Buffers actions per market, emits signals when consensus is reached
    or a single S-tier whale makes a significant move.
    """

    CONSENSUS_WINDOW_S = 48 * 3600  # 48 hours
    SINGLE_WHALE_MIN_USD = 10_000.0

    def __init__(self) -> None:
        # market_id → list of (WhaleAction, timestamp)
        self._buffer: dict[str, list[tuple[WhaleAction, float]]] = {}
        self._log = logger.bind(component="whale_signal_builder")

    def ingest(self, action: WhaleAction) -> WhaleSignal | None:
        import time
        now = time.monotonic()
        mid = action.market_id

        self._buffer.setdefault(mid, [])
        self._buffer[mid].append((action, now))

        # Prune old actions
        cutoff = now - self.CONSENSUS_WINDOW_S
        self._buffer[mid] = [(a, t) for a, t in self._buffer[mid] if t > cutoff]

        actions = [a for a, _ in self._buffer[mid]]

        # Split by direction
        buy_yes = [a for a in actions if a.action == "buy"]
        buy_no = [a for a in actions if a.action == "sell"]

        dominant = buy_yes if len(buy_yes) >= len(buy_no) else buy_no
        direction = "buy_yes" if dominant is buy_yes else "buy_no"

        if not dominant:
            return None

        # Single S-tier whale with large position
        s_tier = [a for a in dominant if a.whale_tier == "S"]
        if s_tier and s_tier[-1].amount_usd >= self.SINGLE_WHALE_MIN_USD:
            return self._build_signal(dominant, direction, "whale_follow")

        # Multi-whale consensus (3+)
        if len(dominant) >= 3:
            return self._build_signal(dominant, direction, "whale_consensus")

        return None

    def _build_signal(
        self, actions: list[WhaleAction], direction: str, signal_type: str
    ) -> WhaleSignal:
        win_rates = [a.whale_win_rate for a in actions if a.whale_win_rate > 0]
        avg_wr = sum(win_rates) / len(win_rates) if win_rates else 0.5

        tiers = [a.whale_tier for a in actions]
        best_tier = min(tiers, key=lambda t: {"S": 0, "A": 1, "B": 2}.get(t, 3))

        total_usd = sum(a.amount_usd for a in actions)
        raw_score = (
            avg_wr * 0.40
            + min(1.0, len(actions) / 5) * 0.30
            + {"S": 1.0, "A": 0.65, "B": 0.35}.get(best_tier, 0.2) * 0.30
        )

        last_action = actions[-1]
        return WhaleSignal.create(
            signal_type=signal_type,
            whale_actions=actions,
            market_id=last_action.market_id,
            market_question=last_action.market_question,
            direction=direction,
            consensus_count=len(actions),
            consensus_tier=best_tier,
            whale_win_rates=win_rates,
            avg_whale_win_rate=round(avg_wr, 3),
            total_whale_usd=round(total_usd, 2),
            raw_confidence_score=round(raw_score, 3),
            reasoning=(
                f"{len(actions)} whale(s) (best tier: {best_tier}) placed "
                f"${total_usd:,.0f} total in direction {direction}. "
                f"Avg win rate: {avg_wr:.1%}."
            ),
        )


class SignalPipeline:
    """
    Orchestrates the full signal processing pipeline.

    Usage:
        pipeline = SignalPipeline(...)
        await pipeline.start()    # begins consuming Redis Streams
        await pipeline.stop()
    """

    CONSUMER_GROUP = "signal_pipeline"
    CONSUMER_NAME = "worker_1"

    def __init__(
        self,
        redis_manager: Any,
        news_analyzer: NewsAnalyzer,
        sports_analyzer: SportsAnalyzer,
        scorer: ConfidenceScorer,
        dedup_filter: DedupAndConflictFilter,
        notifier: Any | None = None,      # TelegramNotifier (injected later)
        buy_threshold: int = 60,
    ) -> None:
        self._redis = redis_manager
        self._news_analyzer = news_analyzer
        self._sports_analyzer = sports_analyzer
        self._scorer = scorer
        self._dedup = dedup_filter
        self._notifier = notifier
        self._buy_threshold = buy_threshold
        self._whale_builder = WhaleSignalBuilder()

        # Cache of latest market snapshots keyed by market_id
        self._market_cache: dict[str, MarketSnapshot] = {}

        self._running = False
        self._tasks: list[asyncio.Task] = []
        self._log = logger.bind(component="signal_pipeline")

    def set_notifier(self, notifier: Any) -> None:
        self._notifier = notifier

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        await self._scorer.load_weights()
        self._running = True

        # Ensure consumer groups exist
        for stream in ("stream:news", "stream:sports", "stream:whales", "stream:markets"):
            await self._redis.ensure_consumer_group(stream, self.CONSUMER_GROUP)

        self._tasks = [
            asyncio.create_task(self._consume_news(), name="pipeline:news"),
            asyncio.create_task(self._consume_sports(), name="pipeline:sports"),
            asyncio.create_task(self._consume_whales(), name="pipeline:whales"),
            asyncio.create_task(self._consume_markets(), name="pipeline:markets"),
        ]
        self._log.info("signal_pipeline_started")

    async def stop(self) -> None:
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._log.info("signal_pipeline_stopped")

    # ── Stream consumers ──────────────────────────────────────────────────────

    async def _consume_news(self) -> None:
        async for raw in self._redis.stream_iter("stream:news", self.CONSUMER_GROUP, self.CONSUMER_NAME):
            if not self._running:
                break
            try:
                event = self._deserialise_news(raw)
                signals = await self._news_analyzer.analyze(event)
                for signal in signals:
                    await self._process_signal(signal)
            except Exception as exc:
                self._log.error("news_consume_error", error=str(exc), exc_info=True)

    async def _consume_sports(self) -> None:
        async for raw in self._redis.stream_iter("stream:sports", self.CONSUMER_GROUP, self.CONSUMER_NAME):
            if not self._running:
                break
            try:
                sport_event = self._deserialise_sport(raw)
                # Match to open markets
                matching_markets = self._find_sport_markets(sport_event)
                for snapshot in matching_markets:
                    signals = await self._sports_analyzer.analyze(sport_event, snapshot)
                    for signal in signals:
                        await self._process_signal(signal)
            except Exception as exc:
                self._log.error("sports_consume_error", error=str(exc), exc_info=True)

    async def _consume_whales(self) -> None:
        async for raw in self._redis.stream_iter("stream:whales", self.CONSUMER_GROUP, self.CONSUMER_NAME):
            if not self._running:
                break
            try:
                action = self._deserialise_whale(raw)
                signal = self._whale_builder.ingest(action)
                if signal:
                    await self._process_signal(signal)
            except Exception as exc:
                self._log.error("whale_consume_error", error=str(exc), exc_info=True)

    async def _consume_markets(self) -> None:
        """Keep market snapshot cache fresh. Also feeds into news analyzer."""
        async for raw in self._redis.stream_iter("stream:markets", self.CONSUMER_GROUP, self.CONSUMER_NAME):
            if not self._running:
                break
            try:
                snapshot = self._deserialise_market(raw)
                self._market_cache[snapshot.market_id] = snapshot

                # Feed fresh snapshots to news analyzer for relevance scoring
                self._news_analyzer.update_markets(list(self._market_cache.values()))
            except Exception as exc:
                self._log.error("market_consume_error", error=str(exc), exc_info=True)

    # ── Core signal processing ────────────────────────────────────────────────

    async def _process_signal(self, signal: AnySignal) -> None:
        """Score → dedup/conflict filter → publish → notify."""
        try:
            scored = await self._scorer.score(signal)

            filtered = self._dedup.process(scored)
            if filtered is None:
                return

            # Publish to Redis
            await self._redis.publish_to_stream("stream:scored_signals", filtered.to_stream_dict())

            # Notify Telegram for actionable signals
            if filtered.confidence_score >= self._buy_threshold and self._notifier:
                try:
                    await self._notifier.send_signal_alert(filtered)
                except Exception as exc:
                    self._log.warning("notify_error", error=str(exc))

            self._log.info(
                "signal_processed",
                signal_id=scored.signal.signal_id,
                score=scored.confidence_score,
                action=scored.recommended_action,
            )

        except Exception as exc:
            self._log.error("signal_process_error", error=str(exc), exc_info=True)

    # ── Market matching helper ────────────────────────────────────────────────

    def _find_sport_markets(self, event: SportEvent) -> list[MarketSnapshot]:
        """Find market snapshots that likely correspond to this sport event."""
        results: list[MarketSnapshot] = []
        search_terms = {
            event.home_team.lower(),
            event.away_team.lower(),
            event.league.lower(),
        }
        for snap in self._market_cache.values():
            question_lower = snap.question.lower()
            if any(term in question_lower for term in search_terms if len(term) > 3):
                results.append(snap)
        return results[:5]  # cap to avoid excessive analysis

    # ── Deserialisers ─────────────────────────────────────────────────────────

    def _deserialise_news(self, raw: dict) -> NewsEvent:
        from data_ingestion.news_collector import NewsEvent as NE
        import dateutil.parser
        ts = dateutil.parser.parse(raw["timestamp"])
        received_at = dateutil.parser.parse(raw["received_at"])
        return NE(
            id=raw["id"],
            source=raw["source"],
            timestamp=ts,
            received_at=received_at,
            latency_ms=int(raw.get("latency_ms", 0)),
            headline=raw["headline"],
            body=raw.get("body", ""),
            url=raw.get("url", ""),
            category=raw.get("category", "other"),
            entities=json.loads(raw.get("entities", "[]")),
            raw_data=json.loads(raw.get("raw_data", "{}")),
        )

    def _deserialise_sport(self, raw: dict) -> SportEvent:
        from data_ingestion.sports_collector import SportEvent as SE
        import dateutil.parser
        return SE(
            id=raw["id"],
            sport=raw["sport"],
            league=raw["league"],
            home_team=raw["home_team"],
            away_team=raw["away_team"],
            home_score=int(raw.get("home_score", 0)),
            away_score=int(raw.get("away_score", 0)),
            match_status=raw.get("match_status", "unknown"),
            elapsed_minutes=int(raw.get("elapsed_minutes", 0)),
            total_minutes=int(raw.get("total_minutes", 90)),
            remaining_minutes=int(raw.get("remaining_minutes", 0)),
            events=json.loads(raw.get("events", "[]")),
            stats=json.loads(raw.get("stats", "{}")),
            odds=json.loads(raw.get("odds", "{}")),
            momentum=float(raw.get("momentum", 0.0)),
            timestamp=dateutil.parser.parse(raw["timestamp"]),
            external_id=raw.get("external_id", ""),
        )

    def _deserialise_whale(self, raw: dict) -> WhaleAction:
        from data_ingestion.whale_collector import WhaleAction as WA
        import dateutil.parser
        return WA(
            whale_address=raw["whale_address"],
            whale_tier=raw["whale_tier"],
            whale_name=raw["whale_name"],
            whale_win_rate=float(raw.get("whale_win_rate", 0.0)),
            whale_total_pnl=float(raw.get("whale_total_pnl", 0.0)),
            action=raw["action"],
            market_id=raw["market_id"],
            market_question=raw.get("market_question", ""),
            outcome=raw.get("outcome", "Yes"),
            amount_usd=float(raw.get("amount_usd", 0.0)),
            price=float(raw.get("price", 0.0)),
            timestamp=dateutil.parser.parse(raw["timestamp"]),
            tx_hash=raw.get("tx_hash", ""),
            block_number=int(raw.get("block_number", 0)),
        )

    def _deserialise_market(self, raw: dict) -> MarketSnapshot:
        from data_ingestion.polymarket_collector import MarketSnapshot as MS
        import dateutil.parser
        end_date_str = raw.get("end_date", "")
        end_date = dateutil.parser.parse(end_date_str) if end_date_str else None
        return MS(
            market_id=raw["market_id"],
            condition_id=raw.get("condition_id", raw["market_id"]),
            question=raw.get("question", ""),
            category=raw.get("category", "other"),
            end_date=end_date,
            outcomes=json.loads(raw.get("outcomes", '["Yes","No"]')),
            prices=json.loads(raw.get("prices", '{"Yes":0.5,"No":0.5}')),
            volumes_24h=json.loads(raw.get("volumes_24h", "{}")),
            liquidity=float(raw.get("liquidity", 0.0)),
            spread=float(raw.get("spread", 0.05)),
            orderbook=json.loads(raw.get("orderbook", "{}")),
            price_history=json.loads(raw.get("price_history", "[]")),
            volatility_1h=float(raw.get("volatility_1h", 0.0)),
            volatility_24h=float(raw.get("volatility_24h", 0.0)),
            volume_change_rate=float(raw.get("volume_change_rate", 0.0)),
            timestamp=dateutil.parser.parse(raw["timestamp"]),
        )
