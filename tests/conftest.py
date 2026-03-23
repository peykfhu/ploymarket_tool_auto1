"""tests/conftest.py — shared fixtures."""
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock
from config.settings import Settings


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def settings():
    return Settings()


@pytest.fixture
def mock_redis():
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.set = AsyncMock(return_value=True)
    redis.publish_to_stream = AsyncMock(return_value="1-0")
    return redis


@pytest.fixture
def sample_portfolio():
    from strategy.models import Portfolio
    return Portfolio(
        total_balance=10_000.0,
        available_balance=8_000.0,
        total_position_value=2_000.0,
        positions=[],
        daily_pnl=150.0,
        daily_pnl_pct=0.015,
        total_trades_today=3,
        wins_today=2,
        losses_today=1,
        mode="paper",
    )


@pytest.fixture
def sample_news_signal():
    from unittest.mock import MagicMock
    from signal_processing.models import NewsSignal
    from data_ingestion.news_collector import NewsEvent
    from datetime import datetime, timezone

    news = NewsEvent(
        id="abc123", source="reuters:Reuters",
        timestamp=datetime.now(timezone.utc),
        received_at=datetime.now(timezone.utc),
        latency_ms=500, headline="Candidate X officially withdraws from race",
        body="Breaking: Candidate X has withdrawn.", url="https://reuters.com/1",
        category="politics", entities=["Candidate", "X"], raw_data={},
    )
    return NewsSignal.create(
        source_event=news, market_id="mkt_001",
        market_question="Will Candidate X win the election?",
        direction="buy_no", direction_confidence=0.85,
        market_relevance=0.90, speed_advantage=0.75,
        price_before=0.60, expected_price_after=0.25,
        news_magnitude=0.80, raw_confidence_score=0.82,
        reasoning="Candidate withdrew — bearish for YES",
    )
