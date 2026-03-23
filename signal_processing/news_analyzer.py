"""
signal_processing/news_analyzer.py

News NLP analysis engine. Two-track approach:
  Track A: Rule engine  — regex + keyword matching, <50ms, always runs
  Track B: LLM (OpenAI) — semantic understanding, ~500-2000ms, runs when score warrants it

Pipeline per news event:
  1. Extract events (entities, keywords, category)
  2. Match to active Polymarket markets
  3. Determine direction (buy_yes / buy_no) + confidence
  4. Assess speed advantage (how stale is the info?)
  5. Emit NewsSignal objects
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import structlog

from data_ingestion.news_collector import NewsEvent
from data_ingestion.polymarket_collector import MarketSnapshot
from signal_processing.models import NewsSignal

logger = structlog.get_logger(__name__)


# ─── Internal dataclasses ────────────────────────────────────────────────────

@dataclass
class ExtractedEvent:
    entities: list[str]
    keywords: list[str]
    category: str
    sentiment_polarity: float   # -1.0 negative → +1.0 positive
    magnitude: float            # 0-1 importance
    summary: str


@dataclass
class MarketMatch:
    market_id: str
    market_question: str
    relevance_score: float      # 0-1
    matched_tokens: list[str]


@dataclass
class DirectionResult:
    direction: str              # "buy_yes" | "buy_no" | "neutral"
    confidence: float           # 0-1
    expected_price_move: float  # absolute change expected
    reasoning: str


# ─── Rule-based helpers ──────────────────────────────────────────────────────

# Bearish-for-YES patterns (news makes YES less likely)
_BEARISH_PATTERNS = [
    r"\b(drops?\s+out|withdrawn?|concedes?|loses?|lost|defeated?|banned?|arrested?|disqualified?)\b",
    r"\b(ruled?\s+out|cancelled?|postponed?|abandoned?|halted?)\b",
    r"\b(fails?\s+to|unable\s+to|won't|will\s+not)\b",
    r"\b(dead|died|death|passed?\s+away)\b",
]

# Bullish-for-YES patterns
_BULLISH_PATTERNS = [
    r"\b(wins?|won|victory|victorious|champion|first\s+place)\b",
    r"\b(elected?|appointed?|confirmed?|inaugurated?|sworn\s+in)\b",
    r"\b(passes?|approved?|signed?|enacted?|ratified?)\b",
    r"\b(announces?|confirmed?|officially)\b",
    r"\b(leads?|ahead|front.runner|favourite|favorite)\b",
]

# Source reliability tiers (substring match on source field)
_SOURCE_TIERS: list[tuple[list[str], float]] = [
    (["reuters", "ap:", "apnews", "associated press"], 1.0),
    (["bloomberg", "wsj", "wall street journal", "ft:", "financial times", "bbc"], 0.85),
    (["nytimes", "washington post", "guardian", "economist"], 0.75),
    (["newsapi", "rss:"], 0.55),
    (["twitter:", "gnews"], 0.40),
]


def _source_reliability(source: str) -> float:
    src_lower = source.lower()
    for patterns, score in _SOURCE_TIERS:
        if any(p in src_lower for p in patterns):
            return score
    return 0.30


def _tokenize(text: str) -> set[str]:
    """Lowercase alphanumeric tokens, 3+ chars."""
    return {t.lower() for t in re.findall(r"\b[a-zA-Z]{3,}\b", text)}


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _rule_direction(text: str) -> tuple[str, float]:
    """
    Returns (direction, confidence) using pattern matching.
    direction: "buy_yes" | "buy_no" | "neutral"
    """
    text_lower = text.lower()
    bearish_hits = sum(
        1 for p in _BEARISH_PATTERNS if re.search(p, text_lower)
    )
    bullish_hits = sum(
        1 for p in _BULLISH_PATTERNS if re.search(p, text_lower)
    )

    if bullish_hits == 0 and bearish_hits == 0:
        return "neutral", 0.0

    total = bullish_hits + bearish_hits
    if bullish_hits > bearish_hits:
        conf = min(0.9, 0.4 + (bullish_hits / total) * 0.5)
        return "buy_yes", conf
    elif bearish_hits > bullish_hits:
        conf = min(0.9, 0.4 + (bearish_hits / total) * 0.5)
        return "buy_no", conf
    else:
        return "neutral", 0.2


# ─── LLM client ──────────────────────────────────────────────────────────────

class _LLMDirectionClient:
    """Thin wrapper around OpenAI chat completions for direction inference."""

    SYSTEM_PROMPT = (
        "You are a prediction market analyst. Given a news headline and a market question, "
        "determine if the news makes the YES outcome MORE or LESS likely, and by how much. "
        "Respond ONLY with valid JSON: "
        '{"direction": "buy_yes"|"buy_no"|"neutral", "confidence": 0.0-1.0, '
        '"expected_price_move": 0.0-0.5, "reasoning": "one sentence"}'
    )

    def __init__(self, api_key: str, model: str = "gpt-4o-mini", timeout_s: float = 3.0) -> None:
        self._api_key = api_key
        self._model = model
        self._timeout = timeout_s

    async def infer(self, headline: str, market_question: str) -> DirectionResult | None:
        if not self._api_key:
            return None

        import aiohttp
        payload = {
            "model": self._model,
            "max_tokens": 150,
            "temperature": 0.1,
            "messages": [
                {"role": "system", "content": self.SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": f"Headline: {headline}\n\nMarket question: {market_question}",
                },
            ],
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={"Authorization": f"Bearer {self._api_key}"},
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=self._timeout),
                ) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()

            content = data["choices"][0]["message"]["content"].strip()
            # Strip markdown fences if present
            content = re.sub(r"```json|```", "", content).strip()
            parsed = json.loads(content)

            return DirectionResult(
                direction=parsed.get("direction", "neutral"),
                confidence=float(parsed.get("confidence", 0.5)),
                expected_price_move=float(parsed.get("expected_price_move", 0.05)),
                reasoning=parsed.get("reasoning", ""),
            )
        except asyncio.TimeoutError:
            logger.debug("llm_direction_timeout")
            return None
        except Exception as exc:
            logger.warning("llm_direction_error", error=str(exc))
            return None


# ─── Main analyzer ────────────────────────────────────────────────────────────

class NewsAnalyzer:
    """
    Consumes NewsEvent objects, matches them to active markets,
    and produces NewsSignal objects ready for confidence scoring.
    """

    def __init__(
        self,
        openai_api_key: str = "",
        openai_model: str = "gpt-4o-mini",
        llm_timeout_s: float = 3.0,
        use_llm: bool = True,
        market_match_threshold: float = 0.60,
        speed_advantage_max_move: float = 0.05,
    ) -> None:
        self._llm = _LLMDirectionClient(openai_api_key, openai_model, llm_timeout_s) if use_llm else None
        self._threshold = market_match_threshold
        self._max_speed_move = speed_advantage_max_move

        # Populated externally by pipeline when market snapshots arrive
        self._market_index: dict[str, MarketSnapshot] = {}
        self._market_token_index: dict[str, set[str]] = {}

        self._log = logger.bind(component="news_analyzer")

    def update_markets(self, snapshots: list[MarketSnapshot]) -> None:
        """Called by pipeline whenever fresh market snapshots arrive."""
        for snap in snapshots:
            self._market_index[snap.market_id] = snap
            tokens = _tokenize(snap.question)
            self._market_token_index[snap.market_id] = tokens

    async def analyze(self, news: NewsEvent) -> list[NewsSignal]:
        """Entry point: NewsEvent → list[NewsSignal]"""
        t0 = time.monotonic()

        event = self._extract_event(news)
        if event.magnitude < 0.10:
            return []  # not newsworthy enough

        matches = self._match_markets(event, news)
        if not matches:
            return []

        signals: list[NewsSignal] = []
        for match in matches:
            direction_result = await self._determine_direction(news, event, match)
            if direction_result.direction == "neutral":
                continue

            snapshot = self._market_index.get(match.market_id)
            price_before = snapshot.prices.get("Yes", 0.5) if snapshot else 0.5
            speed_adv = self._assess_speed_advantage(news, snapshot)

            # Rough expected price after
            move = direction_result.expected_price_move
            if direction_result.direction == "buy_yes":
                price_after = min(0.99, price_before + move)
            else:
                price_after = max(0.01, price_before - move)

            raw_score = (
                direction_result.confidence * 0.35
                + match.relevance_score * 0.25
                + speed_adv * 0.20
                + event.magnitude * 0.20
            )

            signal = NewsSignal.create(
                source_event=news,
                market_id=match.market_id,
                market_question=match.market_question,
                direction=direction_result.direction,
                direction_confidence=direction_result.confidence,
                market_relevance=match.relevance_score,
                speed_advantage=speed_adv,
                price_before=price_before,
                expected_price_after=price_after,
                news_magnitude=event.magnitude,
                raw_confidence_score=round(raw_score, 3),
                reasoning=direction_result.reasoning,
            )
            signals.append(signal)

        elapsed_ms = int((time.monotonic() - t0) * 1000)
        if signals:
            self._log.info(
                "news_signals_generated",
                count=len(signals),
                elapsed_ms=elapsed_ms,
                headline=news.headline[:80],
            )

        return signals

    # ── Private helpers ───────────────────────────────────────────────────────

    def _extract_event(self, news: NewsEvent) -> ExtractedEvent:
        """Fast rule-based event extraction."""
        text = f"{news.headline} {news.body}"
        tokens = _tokenize(text)

        # Magnitude heuristics
        magnitude = 0.3  # base
        high_impact_words = {
            "breaking", "urgent", "alert", "exclusive", "first", "official",
            "announces", "confirmed", "signed", "elected", "passed", "dead",
        }
        hits = len(tokens & high_impact_words)
        magnitude = min(1.0, magnitude + hits * 0.12)

        # Boost for short, punchy headlines (more likely to be breaking)
        if len(news.headline) < 80:
            magnitude = min(1.0, magnitude + 0.1)

        # Source boosts
        rel = _source_reliability(news.source)
        magnitude = min(1.0, magnitude * (0.7 + rel * 0.3))

        direction, sentiment = _rule_direction(text)
        polarity = 0.5 if direction == "buy_yes" else (-0.5 if direction == "buy_no" else 0.0)

        return ExtractedEvent(
            entities=news.entities,
            keywords=list(tokens & high_impact_words),
            category=news.category,
            sentiment_polarity=polarity,
            magnitude=round(magnitude, 3),
            summary=news.headline[:200],
        )

    def _match_markets(self, event: ExtractedEvent, news: NewsEvent) -> list[MarketMatch]:
        """Match extracted event to active markets using token overlap."""
        news_tokens = _tokenize(f"{news.headline} {news.body}") | set(
            e.lower() for e in news.entities
        )

        matches: list[MarketMatch] = []
        for market_id, market_tokens in self._market_token_index.items():
            score = _jaccard(news_tokens, market_tokens)

            # Boost if category matches
            snap = self._market_index.get(market_id)
            if snap and snap.category.lower() == news.category.lower():
                score = min(1.0, score * 1.3)

            # Boost for entity hits (proper nouns are more meaningful)
            entity_tokens = {e.lower() for e in news.entities}
            entity_overlap = len(entity_tokens & market_tokens)
            score = min(1.0, score + entity_overlap * 0.05)

            if score >= self._threshold:
                question = snap.question if snap else market_id
                matches.append(MarketMatch(
                    market_id=market_id,
                    market_question=question,
                    relevance_score=round(score, 3),
                    matched_tokens=list(news_tokens & market_tokens)[:10],
                ))

        # Sort by relevance, take top 3
        matches.sort(key=lambda m: m.relevance_score, reverse=True)
        return matches[:3]

    async def _determine_direction(
        self, news: NewsEvent, event: ExtractedEvent, match: MarketMatch
    ) -> DirectionResult:
        """
        Two-track direction detection.
        Rule engine is always tried first. LLM is called when:
          - Rule engine is uncertain (confidence < 0.7)
          - OR market relevance is high (>0.8) and LLM is enabled
        """
        rule_dir, rule_conf = _rule_direction(f"{news.headline} {news.body}")

        if self._llm and (rule_conf < 0.70 or match.relevance_score > 0.80):
            llm_result = await self._llm.infer(news.headline, match.market_question)
            if llm_result and llm_result.direction != "neutral":
                # Blend: LLM wins if it disagrees with high confidence
                if llm_result.confidence > rule_conf + 0.15:
                    return llm_result
                # Otherwise blend confidences
                if llm_result.direction == rule_dir:
                    blended_conf = min(0.95, (rule_conf + llm_result.confidence) / 2 + 0.1)
                    return DirectionResult(
                        direction=rule_dir,
                        confidence=blended_conf,
                        expected_price_move=llm_result.expected_price_move,
                        reasoning=llm_result.reasoning,
                    )
                return llm_result  # LLM disagrees with rule engine → trust LLM

        # Fallback to rule-only
        if rule_dir == "neutral":
            return DirectionResult("neutral", 0.0, 0.0, "No clear directional signal from rules")

        move = 0.05 + rule_conf * 0.15  # rough expected move
        return DirectionResult(
            direction=rule_dir,
            confidence=rule_conf,
            expected_price_move=round(move, 3),
            reasoning=f"Rule engine: {rule_dir} with confidence {rule_conf:.2f}",
        )

    def _assess_speed_advantage(self, news: NewsEvent, snapshot: MarketSnapshot | None) -> float:
        """
        Returns 0-1 speed advantage.
        High when: we got news fast AND market hasn't moved yet.
        """
        # Latency penalty
        latency_s = news.latency_ms / 1000
        if latency_s > 300:  # >5 min stale
            return 0.0
        latency_score = max(0.0, 1.0 - latency_s / 300)

        # Market movement penalty
        if snapshot and len(snapshot.price_history) >= 2:
            recent = snapshot.price_history[-1]
            prev = snapshot.price_history[-2]
            move = abs(recent - prev)
            if move >= self._max_speed_move:
                return 0.0  # market already repriced
            market_score = 1.0 - (move / self._max_speed_move)
        else:
            market_score = 0.8  # no history → assume no move

        return round(latency_score * 0.6 + market_score * 0.4, 3)
