"""Tests for position sizing methods."""
import pytest
from risk.position_sizer import PositionSizer
from unittest.mock import MagicMock


def make_scored(action="buy", score=70):
    sig = MagicMock()
    sig.signal_type = "news_driven"
    scored = MagicMock()
    scored.recommended_action = action
    scored.confidence_score = score
    scored.signal = sig
    return scored


def make_portfolio(balance=10_000.0):
    p = MagicMock()
    p.total_balance = balance
    return p


def test_fixed_pct_buy():
    sizer = PositionSizer(method="fixed_pct", score_to_pct={"buy": 0.03, "strong_buy": 0.06})
    usd = sizer.calculate(make_scored("buy"), make_portfolio())
    assert abs(usd - 300.0) < 1.0

def test_fixed_pct_strong_buy():
    sizer = PositionSizer(method="fixed_pct", score_to_pct={"buy": 0.03, "strong_buy": 0.06})
    usd = sizer.calculate(make_scored("strong_buy"), make_portfolio())
    assert abs(usd - 600.0) < 1.0

def test_kelly_is_bounded():
    sizer = PositionSizer(method="kelly", max_position_pct=0.08)
    sizer.update_strategy_stats("news_driven", win_rate=0.60, avg_win_loss_ratio=1.5)
    usd = sizer.calculate(make_scored("buy"), make_portfolio(10_000))
    assert usd <= 10_000 * 0.08 + 1

def test_kelly_negative_edge_floors_at_min():
    sizer = PositionSizer(method="kelly", min_position_pct=0.005)
    sizer.update_strategy_stats("news_driven", win_rate=0.30, avg_win_loss_ratio=0.5)
    usd = sizer.calculate(make_scored("buy"), make_portfolio(10_000))
    assert usd >= 10_000 * 0.005 - 1

def test_volatility_adjusted_reduces_size():
    sizer = PositionSizer(method="volatility_adjusted", score_to_pct={"buy": 0.04})
    low_vol = sizer.calculate(make_scored("buy"), make_portfolio(), market_volatility=0.01)
    high_vol = sizer.calculate(make_scored("buy"), make_portfolio(), market_volatility=0.50)
    assert low_vol > high_vol

def test_respects_min_max_bounds():
    sizer = PositionSizer(min_position_pct=0.01, max_position_pct=0.05)
    usd_min = sizer.calculate(make_scored("skip"), make_portfolio(10_000))
    assert usd_min >= 100  # 1% of 10k
    assert usd_min <= 500  # 5% of 10k
