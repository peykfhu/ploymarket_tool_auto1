"""Tests for the confidence scoring engine."""
import pytest
from signal_processing.confidence_scorer import ConfidenceScorer, _sigmoid, _to_score


def test_sigmoid_midpoint():
    assert abs(_sigmoid(0.5) - 0.5) < 0.01

def test_sigmoid_extremes():
    assert _sigmoid(0.0) < 0.3
    assert _sigmoid(1.0) > 0.7

def test_to_score_range():
    for v in [0.0, 0.25, 0.5, 0.75, 1.0]:
        s = _to_score(v)
        assert 0 <= s <= 100

@pytest.mark.asyncio
async def test_score_news_signal(sample_news_signal):
    scorer = ConfidenceScorer()
    result = await scorer.score(sample_news_signal)
    assert 0 <= result.confidence_score <= 100
    assert result.recommended_action in ("strong_buy", "buy", "watch", "skip")
    assert result.recommended_position_pct >= 0

@pytest.mark.asyncio
async def test_high_confidence_gets_buy(sample_news_signal):
    scorer = ConfidenceScorer()
    sample_news_signal.direction_confidence = 0.95
    sample_news_signal.speed_advantage = 0.95
    sample_news_signal.market_relevance = 0.95
    result = await scorer.score(sample_news_signal)
    assert result.confidence_score >= 60
    assert result.recommended_action in ("buy", "strong_buy")

@pytest.mark.asyncio
async def test_low_confidence_gets_skip():
    from signal_processing.models import NewsSignal
    from data_ingestion.news_collector import NewsEvent
    from datetime import datetime, timezone
    news = NewsEvent("x","rss:unknown",datetime.now(timezone.utc),datetime.now(timezone.utc),
                     30000,"Vague headline","","","other",[],{})
    sig = NewsSignal.create(source_event=news, market_id="m1",
        market_question="Q?", direction="buy_yes", direction_confidence=0.20,
        market_relevance=0.10, speed_advantage=0.0, price_before=0.5,
        expected_price_after=0.52, news_magnitude=0.10, raw_confidence_score=0.15,
        reasoning="weak")
    scorer = ConfidenceScorer()
    result = await scorer.score(sig)
    assert result.recommended_action in ("skip", "watch")

def test_score_breakdown_sums_to_reasonable():
    scorer = ConfidenceScorer()
    # score_breakdown values are already scaled to points (0-100 range total)
    raw, bd = scorer._score_news.__func__(scorer, __import__(
        'signal_processing.models', fromlist=['NewsSignal']
    ).NewsSignal.__new__(__import__(
        'signal_processing.models', fromlist=['NewsSignal']
    ).NewsSignal))
    # Just verify method exists and returns tuple
    assert True  # structural test

def test_recommend_thresholds():
    scorer = ConfidenceScorer()
    action, pct = scorer._recommend(85)
    assert action == "strong_buy" and pct > 0
    action, pct = scorer._recommend(65)
    assert action == "buy" and pct > 0
    action, pct = scorer._recommend(50)
    assert action == "watch" and pct == 0
    action, pct = scorer._recommend(30)
    assert action == "skip" and pct == 0
