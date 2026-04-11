"""Tests for strategy/info_asymmetry_strategy.py (体育/政治信息差)."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from data_ingestion.news_collector import NewsEvent
from data_ingestion.sports_collector import SportEvent
from signal_processing.models import NewsSignal, ScoredSignal, SportsSignal
from strategy.info_asymmetry_strategy import InfoAsymmetryStrategy
from strategy.models import Portfolio


NOW = datetime(2026, 4, 11, 12, 0, 0, tzinfo=timezone.utc)


def make_portfolio(balance: float = 10_000.0) -> Portfolio:
    return Portfolio(
        total_balance=balance,
        available_balance=balance * 0.9,
        total_position_value=balance * 0.1,
        positions=[],
        daily_pnl=0.0,
        daily_pnl_pct=0.0,
        total_trades_today=0,
        wins_today=0,
        losses_today=0,
        mode="paper",
    )


def make_news_event(
    *,
    category: str = "politics",
    latency_ms: int = 1_500,
    headline: str = "Candidate withdraws from race",
) -> NewsEvent:
    return NewsEvent(
        id="n1",
        source="reuters:Reuters",
        timestamp=NOW,
        received_at=NOW,
        latency_ms=latency_ms,
        headline=headline,
        body=headline,
        url="https://reuters.com/x",
        category=category,
        entities=["Candidate", "X"],
        raw_data={},
    )


def make_news_signal(
    *,
    category: str = "politics",
    latency_ms: int = 1_500,
    speed_advantage: float = 0.8,
    market_relevance: float = 0.85,
    direction: str = "buy_no",
    price_before: float = 0.55,
    expected_price_after: float = 0.25,
) -> NewsSignal:
    return NewsSignal.create(
        source_event=make_news_event(category=category, latency_ms=latency_ms),
        market_id="mkt_news_1",
        market_question="Will Candidate X win?",
        direction=direction,
        direction_confidence=0.88,
        market_relevance=market_relevance,
        speed_advantage=speed_advantage,
        price_before=price_before,
        expected_price_after=expected_price_after,
        news_magnitude=0.9,
        raw_confidence_score=0.85,
        reasoning="unit test news",
    )


def make_scored(signal, score: int = 75) -> ScoredSignal:
    return ScoredSignal.create(
        signal=signal,
        score=score,
        breakdown={"base": 60, "speed": 15},
        action="buy",
        pct=0.03,
    )


def make_sport_event(
    *,
    sport: str = "football",
    status: str = "second_half",
    elapsed: int = 70,
) -> SportEvent:
    return SportEvent(
        id="sp1",
        sport=sport,
        league="Premier League",
        home_team="Arsenal",
        away_team="Liverpool",
        home_score=2,
        away_score=1,
        match_status=status,
        elapsed_minutes=elapsed,
        total_minutes=90,
        remaining_minutes=max(0, 90 - elapsed),
        events=[],
        stats={},
        odds={},
        momentum=0.3,
        timestamp=NOW,
    )


def make_sports_signal(
    *,
    edge: float = 0.08,
    direction: str = "buy_yes",
    market_price: float = 0.60,
    status: str = "second_half",
    calculated_probability: float = 0.72,
) -> SportsSignal:
    return SportsSignal.create(
        signal_type="sports_info_arb",
        sport_event=make_sport_event(status=status),
        market_id="mkt_sports_1",
        market_question="Arsenal beats Liverpool?",
        direction=direction,
        calculated_probability=calculated_probability,
        market_price=market_price,
        edge=edge,
        time_pressure=0.5,
        momentum_score=0.3,
        reversal_probability=0.2,
        raw_confidence_score=0.8,
        reasoning="unit test sports",
    )


# ── should_handle ────────────────────────────────────────────────────────────

def test_should_handle_politics_news():
    s = InfoAsymmetryStrategy()
    scored = make_scored(make_news_signal(category="politics"))
    assert s.should_handle(scored)


def test_should_handle_sports_news():
    s = InfoAsymmetryStrategy()
    scored = make_scored(make_news_signal(category="sports"))
    assert s.should_handle(scored)


def test_should_handle_ignores_crypto_news():
    s = InfoAsymmetryStrategy()
    scored = make_scored(make_news_signal(category="crypto"))
    assert not s.should_handle(scored)


def test_should_handle_sports_signal():
    s = InfoAsymmetryStrategy()
    scored = make_scored(make_sports_signal())
    assert s.should_handle(scored)


# ── News branch ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_news_happy_path():
    s = InfoAsymmetryStrategy(min_confidence_score=65)
    scored = make_scored(make_news_signal(), score=80)
    order = await s.evaluate(scored, make_portfolio(), None)
    assert order is not None
    assert order.strategy_name == "info_asymmetry"
    assert order.market_id == "mkt_news_1"
    assert order.outcome == "No"  # direction="buy_no"
    assert 0.01 < order.price < 0.99
    assert order.time_in_force == "IOC"


@pytest.mark.asyncio
async def test_news_rejects_stale_latency():
    s = InfoAsymmetryStrategy(max_news_latency_ms=1_000)
    scored = make_scored(make_news_signal(latency_ms=5_000))
    order = await s.evaluate(scored, make_portfolio(), None)
    assert order is None


@pytest.mark.asyncio
async def test_news_rejects_low_speed_advantage():
    s = InfoAsymmetryStrategy(min_speed_advantage=0.70)
    scored = make_scored(make_news_signal(speed_advantage=0.30))
    order = await s.evaluate(scored, make_portfolio(), None)
    assert order is None


@pytest.mark.asyncio
async def test_news_rejects_low_confidence():
    s = InfoAsymmetryStrategy(min_confidence_score=70)
    scored = make_scored(make_news_signal(), score=50)
    order = await s.evaluate(scored, make_portfolio(), None)
    assert order is None


@pytest.mark.asyncio
async def test_news_rejects_low_relevance():
    s = InfoAsymmetryStrategy(min_market_relevance=0.80)
    scored = make_scored(make_news_signal(market_relevance=0.50))
    order = await s.evaluate(scored, make_portfolio(), None)
    assert order is None


# ── Sports branch ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_sports_happy_path():
    s = InfoAsymmetryStrategy(min_sports_edge=0.05)
    scored = make_scored(make_sports_signal(edge=0.10), score=78)
    order = await s.evaluate(scored, make_portfolio(), None)
    assert order is not None
    assert order.outcome == "Yes"
    assert order.strategy_name == "info_asymmetry"


@pytest.mark.asyncio
async def test_sports_rejects_low_edge():
    s = InfoAsymmetryStrategy(min_sports_edge=0.10)
    scored = make_scored(make_sports_signal(edge=0.02))
    order = await s.evaluate(scored, make_portfolio(), None)
    assert order is None


@pytest.mark.asyncio
async def test_sports_rejects_pre_match():
    s = InfoAsymmetryStrategy()
    scored = make_scored(make_sports_signal(status="not_started"))
    order = await s.evaluate(scored, make_portfolio(), None)
    assert order is None


@pytest.mark.asyncio
async def test_sizing_scales_with_confidence():
    s = InfoAsymmetryStrategy(
        min_confidence_score=65,
        base_position_pct=0.02,
        max_position_pct=0.05,
    )
    low = make_scored(make_sports_signal(), score=65)
    high = make_scored(make_sports_signal(), score=95)
    o_low = await s.evaluate(low, make_portfolio(), None)
    o_high = await s.evaluate(high, make_portfolio(), None)
    assert o_low is not None and o_high is not None
    assert o_high.size_usd > o_low.size_usd


# ── Position management ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_manage_position_time_boxed_exit():
    from datetime import timedelta
    from strategy.models import Position
    from data_ingestion.polymarket_collector import MarketSnapshot

    s = InfoAsymmetryStrategy(hold_timeout_minutes=5)
    # Use the real wall-clock (UTC) so the age comparison in the
    # strategy is deterministic across machines and timezones.
    opened = datetime.now(timezone.utc) - timedelta(minutes=30)
    pos = Position(
        position_id="p1", market_id="mkt_sports_1", market_question="q",
        outcome="Yes", side="buy", entry_price=0.60, current_price=0.60,
        size_usd=100.0, unrealized_pnl=0.0, unrealized_pnl_pct=0.0,
        take_profit=0.72, stop_loss=0.55,
        trailing_stop=None, highest_price_seen=0.60,
        strategy_name="info_asymmetry", signal_id="sig",
        opened_at=opened, mode="paper",
    )
    snap = MarketSnapshot(
        market_id="mkt_sports_1", condition_id="c", question="q",
        category="sports", end_date=None, outcomes=["Yes", "No"],
        prices={"Yes": 0.61, "No": 0.39},
        volumes_24h={"Yes": 0, "No": 0}, liquidity=5_000.0, spread=0.02,
        orderbook={}, price_history=[], volatility_1h=0.0, volatility_24h=0.0,
        volume_change_rate=0.0, timestamp=NOW,
    )
    close = await s.manage_position(pos, snap)
    assert close is not None
    assert "timeout" in close.reasoning
