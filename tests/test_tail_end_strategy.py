"""Tests for strategy/tail_end_strategy.py (吃尾盘)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from data_ingestion.polymarket_collector import MarketSnapshot
from strategy.models import Portfolio
from strategy.tail_end_strategy import TailEndStrategy


NOW = datetime(2026, 4, 11, 12, 0, 0, tzinfo=timezone.utc)


def make_snap(
    *,
    market_id: str = "mkt_tail_1",
    end_in_hours: float = 3.0,
    yes: float = 0.92,
    no: float = 0.08,
    liquidity: float = 5_000.0,
    spread: float = 0.02,
    history: list[float] | None = None,
) -> MarketSnapshot:
    return MarketSnapshot(
        market_id=market_id,
        condition_id=f"cond_{market_id}",
        question=f"Market question for {market_id}?",
        category="politics",
        end_date=NOW + timedelta(hours=end_in_hours),
        outcomes=["Yes", "No"],
        prices={"Yes": yes, "No": no},
        volumes_24h={"Yes": 10_000.0, "No": 5_000.0},
        liquidity=liquidity,
        spread=spread,
        orderbook={"bids": [], "asks": []},
        price_history=history if history is not None else [0.85, 0.88, 0.91],
        volatility_1h=0.02,
        volatility_24h=0.05,
        volume_change_rate=0.1,
        timestamp=NOW,
    )


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


# ── should_handle / evaluate ──────────────────────────────────────────────────

def test_should_handle_is_always_false():
    s = TailEndStrategy()
    # TailEnd is scan-driven — it MUST NOT claim any ScoredSignal.
    class Dummy:  # noqa
        pass
    assert s.should_handle(Dummy()) is False  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_evaluate_returns_none():
    s = TailEndStrategy()
    result = await s.evaluate(None, make_portfolio(), None)  # type: ignore[arg-type]
    assert result is None


# ── scan() gating ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_scan_produces_order_for_eligible_market():
    s = TailEndStrategy(end_window_hours=6.0, min_favorite_price=0.90)
    snap = make_snap(end_in_hours=3.0, yes=0.92, no=0.08)
    orders = await s.scan({snap.market_id: snap}, make_portfolio(), now=NOW)
    assert len(orders) == 1
    o = orders[0]
    assert o.market_id == snap.market_id
    assert o.outcome == "Yes"
    assert o.side == "buy"
    assert 0.90 <= o.price <= 1.0
    assert o.strategy_name == "tail_end"
    assert o.time_in_force == "IOC"
    assert o.confidence_score >= 60


@pytest.mark.asyncio
async def test_scan_ignores_markets_outside_window():
    s = TailEndStrategy(end_window_hours=6.0)
    # Market closes 10 hours from now — outside window
    snap = make_snap(end_in_hours=10.0)
    orders = await s.scan({snap.market_id: snap}, make_portfolio(), now=NOW)
    assert orders == []


@pytest.mark.asyncio
async def test_scan_ignores_expired_markets():
    s = TailEndStrategy()
    snap = make_snap(end_in_hours=-1.0)
    orders = await s.scan({snap.market_id: snap}, make_portfolio(), now=NOW)
    assert orders == []


@pytest.mark.asyncio
async def test_scan_ignores_wide_spread():
    s = TailEndStrategy(max_spread_pct=0.04)
    snap = make_snap(spread=0.08)
    orders = await s.scan({snap.market_id: snap}, make_portfolio(), now=NOW)
    assert orders == []


@pytest.mark.asyncio
async def test_scan_ignores_thin_liquidity():
    s = TailEndStrategy(min_liquidity_usd=2_000.0)
    snap = make_snap(liquidity=500.0)
    orders = await s.scan({snap.market_id: snap}, make_portfolio(), now=NOW)
    assert orders == []


@pytest.mark.asyncio
async def test_scan_ignores_no_favorite():
    s = TailEndStrategy(min_favorite_price=0.90)
    snap = make_snap(yes=0.52, no=0.48)
    orders = await s.scan({snap.market_id: snap}, make_portfolio(), now=NOW)
    assert orders == []


@pytest.mark.asyncio
async def test_scan_requires_momentum_for_favorite_lean():
    s = TailEndStrategy()
    # Fav price is in the favorite_lean band (0.90 ≤ p < 0.95)
    # and momentum is REVERSING (declining history) → should skip
    snap = make_snap(yes=0.91, no=0.09, history=[0.96, 0.94, 0.92])
    orders = await s.scan({snap.market_id: snap}, make_portfolio(), now=NOW)
    assert orders == []


@pytest.mark.asyncio
async def test_scan_tail_harvest_mode_skips_momentum_check():
    s = TailEndStrategy()
    # Fav price ≥ 0.95 → tail_harvest mode. Bad history still OK.
    snap = make_snap(yes=0.97, no=0.03, history=[0.99, 0.98, 0.97])
    orders = await s.scan({snap.market_id: snap}, make_portfolio(), now=NOW)
    assert len(orders) == 1


@pytest.mark.asyncio
async def test_scan_caps_size_to_book_share():
    s = TailEndStrategy(base_position_pct=0.10, max_position_pct=0.10)
    # Balance = 10_000 → intended 1_000 but liquidity = 2_000 → cap 100 (5% of 2k)
    snap = make_snap(yes=0.97, liquidity=2_000.0)
    orders = await s.scan({snap.market_id: snap}, make_portfolio(), now=NOW)
    assert len(orders) == 1
    assert orders[0].size_usd <= 2_000.0 * 0.05 + 0.01


@pytest.mark.asyncio
async def test_scan_skips_if_already_positioned():
    from strategy.models import Position
    s = TailEndStrategy()
    snap = make_snap(yes=0.97)
    pf = make_portfolio()
    pf.positions.append(
        Position.create(
            market_id=snap.market_id,
            market_question=snap.question,
            outcome="Yes",
            side="buy",
            entry_price=0.95,
            current_price=0.97,
            size_usd=100.0,
            take_profit=0.99,
            stop_loss=0.90,
            highest_price_seen=0.97,
            strategy_name="tail_end",
            signal_id="prev",
        )
    )
    orders = await s.scan({snap.market_id: snap}, pf, now=NOW)
    assert orders == []


# ── Position management ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_manage_position_closes_on_target():
    from strategy.models import Position
    s = TailEndStrategy()
    pos = Position.create(
        market_id="m1", market_question="q",
        outcome="Yes", side="buy",
        entry_price=0.93, current_price=0.93, size_usd=100.0,
        take_profit=0.99, stop_loss=0.87,
        highest_price_seen=0.93,
        strategy_name="tail_end", signal_id="sig1",
    )
    snap = make_snap(yes=0.995, no=0.005)
    close = await s.manage_position(pos, snap)
    assert close is not None
    assert close.side == "sell"
    assert "tail_end_target" in close.reasoning


@pytest.mark.asyncio
async def test_manage_position_closes_on_stop():
    from strategy.models import Position
    s = TailEndStrategy()
    pos = Position.create(
        market_id="m1", market_question="q",
        outcome="Yes", side="buy",
        entry_price=0.93, current_price=0.93, size_usd=100.0,
        take_profit=0.99, stop_loss=0.87,
        highest_price_seen=0.93,
        strategy_name="tail_end", signal_id="sig1",
    )
    snap = make_snap(yes=0.85, no=0.15)
    close = await s.manage_position(pos, snap)
    assert close is not None
    assert "tail_end_stop" in close.reasoning
