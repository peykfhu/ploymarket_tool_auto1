"""End-to-end paper-mode integration test.

Covers the signal → risk → order → paper fill → db persistence path using
an in-memory fake database, without requiring Postgres/Redis.
"""
from __future__ import annotations

import pytest

from execution.order_manager import OrderManager
from execution.paper_executor import PaperExecutor
from risk.risk_rules import RiskRuleEngine
from risk.risk_state import RiskState
from risk.models import RiskDecision
from strategy.models import OrderRequest


class FakeDB:
    def __init__(self):
        self.saved_trades = []
        self.saved_orders = []

    async def save_trade(self, trade):
        self.saved_trades.append(trade)

    async def save_order(self, order, result):
        self.saved_orders.append((order, result))


class FakeNotifier:
    def __init__(self):
        self.events = []

    async def send_trade_result(self, result):
        self.events.append(("trade_result", result))

    async def send_risk_alert(self, level, msg):
        self.events.append(("risk_alert", level, msg))


class FakeRedis:
    def __init__(self):
        self.store = {}

    async def cache_set(self, key, value, ttl=None):
        self.store[key] = value

    async def cache_get(self, key):
        return self.store.get(key)

    async def cache_delete(self, key):
        self.store.pop(key, None)

    async def get_cached_market_data(self, market_id):
        return self.store.get(f"cache:market:{market_id}")

    async def set(self, key, value, ttl=None):
        self.store[key] = value

    async def get(self, key):
        return self.store.get(key)


def _make_order(**overrides) -> OrderRequest:
    defaults = dict(
        signal_id="sig_test_1",
        strategy_name="swing_event_driven",
        market_id="market_xyz",
        market_question="Will integration test pass?",
        side="buy",
        outcome="Yes",
        order_type="limit",
        price=0.55,
        size_usd=200.0,
        position_pct=0.02,
        confidence_score=72,
        take_profit=0.70,
        stop_loss=0.45,
        max_slippage_pct=0.02,
        reasoning="integration test",
    )
    defaults.update(overrides)
    return OrderRequest.create(**defaults)


@pytest.fixture
def paper_stack():
    db = FakeDB()
    notifier = FakeNotifier()
    redis = FakeRedis()
    risk_state = RiskState()
    executor = PaperExecutor(
        initial_balance=10_000.0,
        fee_rate=0.02,
        simulated_latency_ms=0,
        db=db,
    )
    order_manager = OrderManager(
        mode="paper",
        executor=executor,
        db=db,
        notifier=notifier,
        risk_state=risk_state,
        redis=redis,
        fee_rate=0.02,
    )
    return {
        "db": db,
        "notifier": notifier,
        "redis": redis,
        "risk_state": risk_state,
        "executor": executor,
        "order_manager": order_manager,
    }


@pytest.mark.asyncio
async def test_paper_buy_flow_opens_position_and_persists(paper_stack):
    om = paper_stack["order_manager"]
    db = paper_stack["db"]
    executor = paper_stack["executor"]

    order = _make_order()
    decision = RiskDecision.approve(order)

    result = await om.submit_order(order, decision)

    assert result.is_filled
    assert result.fill_price > 0

    portfolio = await executor.get_portfolio()
    assert portfolio.position_count == 1
    assert portfolio.positions[0].market_id == "market_xyz"
    assert portfolio.available_balance < 10_000.0

    # DB should have received both the trade record and the order persistence hook
    assert len(db.saved_trades) >= 1
    assert len(db.saved_orders) == 1
    saved_order, saved_result = db.saved_orders[0]
    assert saved_order.order_id == order.order_id
    assert saved_result.status == "filled"


@pytest.mark.asyncio
async def test_paper_buy_then_close_realises_pnl(paper_stack):
    om = paper_stack["order_manager"]
    executor = paper_stack["executor"]

    buy = _make_order(price=0.50, size_usd=500.0)
    await om.submit_order(buy, RiskDecision.approve(buy))

    portfolio = await executor.get_portfolio()
    assert portfolio.position_count == 1
    pos = portfolio.positions[0]

    result = await om.close_position(pos.position_id, reason="integration test")
    assert result.is_filled

    portfolio = await executor.get_portfolio()
    assert portfolio.position_count == 0
    # Closed trade should appear in closed_trades list
    closed = executor.get_closed_trades()
    assert len(closed) == 1
    assert closed[0].realized_pnl is not None


@pytest.mark.asyncio
async def test_risk_engine_blocks_oversized_order(paper_stack):
    executor = paper_stack["executor"]
    engine = RiskRuleEngine(max_single_position_pct=0.05)

    order = _make_order(size_usd=9_000.0, position_pct=0.90)
    portfolio = await executor.get_portfolio()
    state = paper_stack["risk_state"]

    decision = await engine.evaluate(order, portfolio, state)
    # Either blocked outright or adjusted down below the cap
    if decision.approved:
        assert decision.adjusted_order is not None
        assert decision.adjusted_order.size_usd <= portfolio.total_balance * 0.05 + 1
    else:
        assert decision.violations
