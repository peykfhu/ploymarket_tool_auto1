"""Tests for the backtest engine and performance analyzer."""
import asyncio
import math
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from execution.backtest_executor import BacktestExecutor
from backtest.performance import PerformanceAnalyzer
from execution.models import TradeRecord


def make_closed_trade(pnl: float, strategy: str = "test", hours_held: int = 2):
    from datetime import datetime, timezone, timedelta
    t = MagicMock(spec=TradeRecord)
    t.trade_id = "t1"
    t.strategy_name = strategy
    t.realized_pnl = pnl
    t.realized_pnl_pct = pnl / 100
    t.size_usd = 100.0
    t.opened_at = datetime.now(timezone.utc) - timedelta(hours=hours_held)
    t.closed_at = datetime.now(timezone.utc)
    t.close_reason = "take_profit" if pnl > 0 else "stop_loss"
    return t


def make_equity_curve(values: list[float]) -> list[tuple[datetime, float]]:
    base = datetime.now(timezone.utc) - timedelta(days=len(values))
    return [(base + timedelta(days=i), v) for i, v in enumerate(values)]


def test_performance_win_rate():
    trades = [make_closed_trade(10) for _ in range(7)] + [make_closed_trade(-5) for _ in range(3)]
    curve = make_equity_curve([1000 + i*10 for i in range(30)])
    analyzer = PerformanceAnalyzer()
    report = analyzer.analyze(trades, curve, 1000.0)
    assert abs(report.win_rate - 0.70) < 0.01

def test_performance_profit_factor():
    trades = [make_closed_trade(20)] * 5 + [make_closed_trade(-10)] * 5
    curve = make_equity_curve([1000, 1050, 1100, 1080, 1120, 1150])
    analyzer = PerformanceAnalyzer()
    report = analyzer.analyze(trades, curve, 1000.0)
    assert abs(report.profit_factor - 2.0) < 0.01

def test_performance_max_drawdown():
    curve = make_equity_curve([1000, 1100, 1050, 900, 950, 1000])
    trades = [make_closed_trade(-50)]
    analyzer = PerformanceAnalyzer()
    report = analyzer.analyze(trades, curve, 1000.0)
    assert report.max_drawdown > 0

def test_performance_sharpe_positive_for_rising_equity():
    curve = make_equity_curve([1000 + i * 5 for i in range(100)])
    trades = [make_closed_trade(5) for _ in range(10)]
    analyzer = PerformanceAnalyzer()
    report = analyzer.analyze(trades, curve, 1000.0)
    # Consistent gains should produce positive Sharpe
    assert report.sharpe_ratio > 0

def test_backtest_executor_buy_fill():
    executor = BacktestExecutor(initial_balance=10_000.0)
    executor.advance_time(datetime.now(timezone.utc))
    executor.set_snapshot({"mkt1": {
        "yes_price": 0.60, "no_price": 0.40,
        "asks": [{"price": "0.61", "size": "1000"}],
        "bids": [{"price": "0.59", "size": "1000"}],
        "liquidity": 5000,
    }})
    from strategy.models import OrderRequest
    order = OrderRequest.create(
        signal_id="s1", strategy_name="test", market_id="mkt1",
        market_question="Test?", side="buy", outcome="Yes",
        price=0.65, size_usd=500.0, position_pct=0.05,
        confidence_score=75, take_profit=0.80, stop_loss=0.50,
        reasoning="test",
    )
    result = executor._simulate_buy(order, executor._historical_snapshot.get("mkt1", {}))
    assert result.status == "filled"
    assert result.fill_price > 0
    assert executor._balance < 10_000.0

def test_backtest_executor_limit_not_filled_when_price_too_low():
    executor = BacktestExecutor(initial_balance=10_000.0)
    executor.advance_time(datetime.now(timezone.utc))
    executor.set_snapshot({"mkt1": {
        "asks": [{"price": "0.70", "size": "100"}],
        "bids": [], "liquidity": 1000,
    }})
    from strategy.models import OrderRequest
    order = OrderRequest.create(
        signal_id="s1", strategy_name="test", market_id="mkt1",
        market_question="Test?", side="buy", outcome="Yes",
        order_type="limit", price=0.50,  # below ask of 0.70
        size_usd=100.0, position_pct=0.01,
        confidence_score=65, take_profit=0.80, stop_loss=0.40,
        reasoning="test",
    )
    result = executor._simulate_buy(order, executor._historical_snapshot.get("mkt1", {}))
    assert result.status == "cancelled"

def test_consecutive_streaks():
    trades = (
        [make_closed_trade(10)] * 4 +
        [make_closed_trade(-5)] * 2 +
        [make_closed_trade(10)] * 3
    )
    curve = make_equity_curve([1000 + i*3 for i in range(20)])
    analyzer = PerformanceAnalyzer()
    report = analyzer.analyze(trades, curve, 1000.0)
    assert report.max_consecutive_wins == 4
    assert report.max_consecutive_losses == 2
