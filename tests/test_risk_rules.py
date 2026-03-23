"""Tests for the five-layer risk rule engine."""
import pytest
from risk.risk_rules import RiskRuleEngine
from risk.risk_state import RiskState
from strategy.models import OrderRequest, Portfolio, Position


def make_order(**kwargs):
    defaults = dict(
        signal_id="sig1", strategy_name="swing_event_driven",
        market_id="mkt1", market_question="Will X happen?",
        side="buy", outcome="Yes", order_type="limit",
        price=0.60, size_usd=300.0, position_pct=0.03,
        confidence_score=75, take_profit=0.75, stop_loss=0.50,
        reasoning="test order",
    )
    defaults.update(kwargs)
    return OrderRequest.create(**defaults)


def make_portfolio(balance=10_000.0, positions=None, available=None):
    return Portfolio(
        total_balance=balance,
        available_balance=available if available is not None else balance * 0.9,
        total_position_value=balance * 0.1,
        positions=positions or [],
        daily_pnl=0.0, daily_pnl_pct=0.0,
        total_trades_today=0, wins_today=0, losses_today=0,
    )


@pytest.mark.asyncio
async def test_approve_normal_order():
    engine = RiskRuleEngine()
    order = make_order()
    portfolio = make_portfolio()
    state = RiskState()
    decision = await engine.evaluate(order, portfolio, state)
    assert decision.approved

@pytest.mark.asyncio
async def test_reject_daily_hard_stop():
    engine = RiskRuleEngine()
    order = make_order()
    portfolio = make_portfolio()
    state = RiskState()
    state.daily_pnl_pct = -0.12  # exceeds 10% hard stop
    decision = await engine.evaluate(order, portfolio, state)
    assert not decision.approved
    assert any("hard_stop" in v for v in decision.violations)

@pytest.mark.asyncio
async def test_reject_max_positions():
    engine = RiskRuleEngine(max_concurrent_positions=2)
    order = make_order()
    from unittest.mock import MagicMock
    pos1 = MagicMock(); pos2 = MagicMock()
    pos1.strategy_name = "other"; pos2.strategy_name = "other"
    portfolio = make_portfolio(positions=[pos1, pos2])
    state = RiskState()
    decision = await engine.evaluate(order, portfolio, state)
    assert not decision.approved

@pytest.mark.asyncio
async def test_reduce_oversized_order():
    engine = RiskRuleEngine(max_single_position_pct=0.05)
    order = make_order(size_usd=900.0, position_pct=0.09)  # 9% > 5% max
    portfolio = make_portfolio(balance=10_000.0)
    state = RiskState()
    decision = await engine.evaluate(order, portfolio, state)
    assert decision.approved
    assert decision.adjusted_order is not None
    assert decision.adjusted_order.size_usd <= 10_000 * 0.05 + 1

@pytest.mark.asyncio
async def test_reject_overtrading():
    engine = RiskRuleEngine(max_trades_per_30min=3)
    order = make_order()
    portfolio = make_portfolio()
    state = RiskState()
    from datetime import datetime, timezone
    now_iso = datetime.now(timezone.utc).isoformat()
    state.recent_trade_times = [now_iso] * 4  # 4 trades in 30 min
    decision = await engine.evaluate(order, portfolio, state)
    assert not decision.approved
    assert any("overtrading" in v for v in decision.violations)

@pytest.mark.asyncio
async def test_reject_price_out_of_bounds():
    engine = RiskRuleEngine()
    order = make_order(price=0.0005)
    portfolio = make_portfolio()
    state = RiskState()
    decision = await engine.evaluate(order, portfolio, state)
    assert not decision.approved

def test_risk_score_range():
    engine = RiskRuleEngine()
    portfolio = make_portfolio()
    state = RiskState()
    score = engine._compute_risk_score(portfolio, state)
    assert 0.0 <= score <= 1.0
