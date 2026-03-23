"""
backtest/engine.py

Event-driven backtest engine.

Architecture:
  1. Load all historical data (market prices, news, sports, whales)
  2. Merge into a single time-sorted event stream
  3. Replay events one-by-one through the full pipeline:
       MarketPriceUpdate → update cache, check stops
       NewsEvent / SportEvent / WhaleAction → signal pipeline → strategy → risk → execution
  4. Generate PerformanceReport

Anti-look-ahead guarantees:
  - signal_delay_ms injected before signal reaches strategy
  - execution_delay_ms injected before order reaches executor
  - Fill uses historical data at time T + execution_delay, not T
  - No future price data is ever passed to analyzers
"""
from __future__ import annotations

import asyncio
import heapq
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

import structlog

from backtest.data_loader import BacktestDataLoader
from backtest.performance import PerformanceAnalyzer, PerformanceReport
from execution.backtest_executor import BacktestExecutor
from signal_processing.confidence_scorer import ConfidenceScorer
from signal_processing.dedup_and_conflict import DedupAndConflictFilter
from signal_processing.news_analyzer import NewsAnalyzer
from signal_processing.sports_analyzer import SportsAnalyzer
from signal_processing.pipeline import WhaleSignalBuilder

logger = structlog.get_logger(__name__)


# ─── Event wrappers ───────────────────────────────────────────────────────────

@dataclass(order=True)
class _Event:
    """Heap-sortable event wrapper."""
    timestamp: datetime
    seq: int                        # tiebreaker for same-timestamp events
    kind: str = field(compare=False)
    payload: Any = field(compare=False)


_SEQ = 0


def _evt(ts: datetime, kind: str, payload: Any) -> _Event:
    global _SEQ
    _SEQ += 1
    return _Event(timestamp=ts, seq=_SEQ, kind=kind, payload=payload)


# ─── BacktestResult ───────────────────────────────────────────────────────────

@dataclass
class BacktestResult:
    start_date: datetime
    end_date: datetime
    initial_balance: float
    final_balance: float
    report: PerformanceReport

    def to_markdown(self) -> str:
        header = (
            f"## Run: {self.start_date.date()} → {self.end_date.date()}\n"
            f"Initial: ${self.initial_balance:,.2f} | Final: ${self.final_balance:,.2f}\n\n"
        )
        return header + self.report.to_markdown()


# ─── Optimisation result ──────────────────────────────────────────────────────

@dataclass
class OptimizationResult:
    best_params: dict
    best_score: float
    optimization_target: str
    all_results: list[dict]


# ─── Engine ───────────────────────────────────────────────────────────────────

class BacktestEngine:
    """
    Full event-driven backtesting engine.
    Instantiate once, call run() with different date ranges.
    """

    def __init__(
        self,
        settings: Any,
        data_loader: BacktestDataLoader | None = None,
        db: Any = None,
    ) -> None:
        self._settings = settings
        self._bt = settings.backtest
        self._data_loader = data_loader or BacktestDataLoader(
            data_dir=self._bt.data_dir, db=db
        )
        self._log = logger.bind(component="backtest_engine")

        # Built fresh each run()
        self._executor: BacktestExecutor | None = None
        self._news_analyzer: NewsAnalyzer | None = None
        self._sports_analyzer: SportsAnalyzer | None = None
        self._scorer: ConfidenceScorer | None = None
        self._dedup: DedupAndConflictFilter | None = None
        self._whale_builder: WhaleSignalBuilder | None = None

        # Market snapshot cache keyed by market_id
        self._market_cache: dict[str, dict] = {}

    # ── Public API ────────────────────────────────────────────────────────────

    async def run(
        self,
        start_date: datetime,
        end_date: datetime,
        market_ids: list[str] | None = None,
        sports: list[str] | None = None,
        progress_callback: Callable[[float], None] | None = None,
    ) -> BacktestResult:
        """Run a full backtest. Returns BacktestResult with performance report."""
        self._log.info(
            "backtest_starting",
            start=start_date.date(),
            end=end_date.date(),
        )
        global _SEQ
        _SEQ = 0

        # ── 1. Build executor + analyzers ─────────────────────────────────────
        self._executor = BacktestExecutor(
            initial_balance=self._bt.initial_balance,
            fee_rate=self._bt.fee_rate,
            slippage_model=self._bt.slippage_model,
            fixed_slippage_pct=self._bt.fixed_slippage_pct,
            signal_delay_ms=self._bt.signal_delay_ms,
            execution_delay_ms=self._bt.execution_delay_ms,
        )
        self._news_analyzer = NewsAnalyzer(
            use_llm=False,   # LLM disabled in backtest for reproducibility + speed
            market_match_threshold=self._settings.signal.market_match_threshold,
        )
        self._sports_analyzer = SportsAnalyzer()
        self._scorer = ConfidenceScorer(
            strong_buy_pct=self._settings.risk.score_to_position.get("strong_buy", 0.06),
            buy_pct=self._settings.risk.score_to_position.get("buy", 0.03),
        )
        self._dedup = DedupAndConflictFilter(
            dedup_window_s=self._settings.signal.dedup_window_s
        )
        self._whale_builder = WhaleSignalBuilder()

        # ── 2. Load data ──────────────────────────────────────────────────────
        self._log.info("loading_historical_data")
        market_data, news_events, sport_events, whale_actions = await asyncio.gather(
            self._data_loader.load_all_markets(start_date, end_date),
            self._data_loader.load_news_history(start_date, end_date),
            self._data_loader.load_sports_history(start_date, end_date, sports),
            self._data_loader.load_whale_history(start_date, end_date),
        )

        # Filter markets if requested
        if market_ids:
            market_data = {k: v for k, v in market_data.items() if k in market_ids}

        self._log.info(
            "data_loaded",
            markets=len(market_data),
            news=len(news_events),
            sports=len(sport_events),
            whales=len(whale_actions),
        )

        # ── 3. Build event stream ─────────────────────────────────────────────
        event_heap: list[_Event] = []

        # Market price updates (one per row per market)
        for mid, df in market_data.items():
            for ts, row in df.iterrows():
                ts_dt = ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts
                heapq.heappush(event_heap, _evt(ts_dt, "market", {"market_id": mid, "row": row}))

        for ne in news_events:
            heapq.heappush(event_heap, _evt(ne.timestamp, "news", ne))
        for se in sport_events:
            heapq.heappush(event_heap, _evt(se.timestamp, "sport", se))
        for wa in whale_actions:
            heapq.heappush(event_heap, _evt(wa.timestamp, "whale", wa))

        total_events = len(event_heap)
        processed = 0
        self._log.info("event_stream_built", total_events=total_events)

        # ── 4. Event loop ─────────────────────────────────────────────────────
        last_stop_check = datetime.min.replace(tzinfo=timezone.utc)
        stop_check_interval = timedelta(seconds=30)

        while event_heap:
            evt = heapq.heappop(event_heap)
            self._executor.advance_time(evt.timestamp)

            if evt.kind == "market":
                await self._handle_market_event(evt.payload)

                # Periodic stop-loss checks
                if evt.timestamp - last_stop_check >= stop_check_interval:
                    await self._check_stops()
                    last_stop_check = evt.timestamp

            elif evt.kind == "news":
                await self._handle_news_event(evt.payload)

            elif evt.kind == "sport":
                await self._handle_sport_event(evt.payload)

            elif evt.kind == "whale":
                await self._handle_whale_event(evt.payload)

            processed += 1
            if progress_callback and processed % 10_000 == 0:
                progress_callback(processed / total_events)

        # ── 5. Close any remaining open positions at last known price ─────────
        portfolio = await self._executor.get_portfolio()
        for pos in portfolio.positions:
            snap = self._market_cache.get(pos.market_id, {})
            last_price = snap.get("yes_price", pos.current_price)
            close_order = self._market_close_order(pos, last_price, "backtest_end")
            await self._executor.place_order(close_order)

        # ── 6. Generate report ────────────────────────────────────────────────
        trades = self._executor.get_closed_trades()
        equity_curve = self._executor.get_equity_curve()
        final_balance = self._executor.get_final_balance()

        analyzer = PerformanceAnalyzer()
        report = analyzer.analyze(trades, equity_curve, self._bt.initial_balance)

        self._log.info(
            "backtest_complete",
            trades=len(trades),
            final_balance=round(final_balance, 2),
            total_pnl_pct=round(report.total_pnl_pct * 100, 2),
            sharpe=round(report.sharpe_ratio, 2),
            max_dd=round(report.max_drawdown * 100, 2),
        )

        return BacktestResult(
            start_date=start_date,
            end_date=end_date,
            initial_balance=self._bt.initial_balance,
            final_balance=final_balance,
            report=report,
        )

    # ── Event handlers ────────────────────────────────────────────────────────

    async def _handle_market_event(self, payload: dict) -> None:
        mid = payload["market_id"]
        row = payload["row"]

        yes_price = float(row.get("yes_price", 0.5))
        liquidity = float(row.get("liquidity", 0.0))
        spread = float(row.get("spread", 0.05))
        bids = row.get("bids", [])
        asks = row.get("asks", [])

        self._market_cache[mid] = {
            "yes_price": yes_price,
            "no_price": round(1.0 - yes_price, 4),
            "liquidity": liquidity,
            "spread": spread,
            "bids": bids if isinstance(bids, list) else [],
            "asks": asks if isinstance(asks, list) else [],
        }

        # Inject into executor's snapshot
        self._executor.set_snapshot(self._market_cache)

        # Update news analyzer with latest prices
        from data_ingestion.polymarket_collector import MarketSnapshot
        import json
        from datetime import timezone
        snap = MarketSnapshot(
            market_id=mid,
            condition_id=mid,
            question=str(row.get("question", "")),
            category=str(row.get("category", "other")),
            end_date=None,
            outcomes=["Yes", "No"],
            prices={"Yes": yes_price, "No": 1 - yes_price},
            volumes_24h={},
            liquidity=liquidity,
            spread=spread,
            orderbook={"bids": [], "asks": []},
            price_history=[yes_price],
            volatility_1h=0.0,
            volatility_24h=0.0,
            volume_change_rate=0.0,
            timestamp=self._executor.current_time,
        )
        self._news_analyzer.update_markets([snap])

    async def _handle_news_event(self, news: Any) -> None:
        await asyncio.sleep(self._bt.signal_delay_ms / 1000)
        signals = await self._news_analyzer.analyze(news)
        for signal in signals:
            await self._process_signal(signal)

    async def _handle_sport_event(self, sport_evt: Any) -> None:
        for mid, snap_data in self._market_cache.items():
            question = snap_data.get("question", "")
            if not (sport_evt.home_team.lower() in question.lower()
                    or sport_evt.away_team.lower() in question.lower()):
                continue
            from data_ingestion.polymarket_collector import MarketSnapshot
            snap = MarketSnapshot(
                market_id=mid,
                condition_id=mid,
                question=question,
                category="sports",
                end_date=None,
                outcomes=["Yes", "No"],
                prices={"Yes": snap_data.get("yes_price", 0.5),
                        "No": snap_data.get("no_price", 0.5)},
                volumes_24h={},
                liquidity=snap_data.get("liquidity", 0),
                spread=snap_data.get("spread", 0.05),
                orderbook={},
                price_history=[snap_data.get("yes_price", 0.5)],
                volatility_1h=0.0,
                volatility_24h=0.0,
                volume_change_rate=0.0,
                timestamp=self._executor.current_time,
            )
            await asyncio.sleep(self._bt.signal_delay_ms / 1000)
            signals = await self._sports_analyzer.analyze(sport_evt, snap)
            for signal in signals:
                await self._process_signal(signal)

    async def _handle_whale_event(self, action: Any) -> None:
        signal = self._whale_builder.ingest(action)
        if signal:
            await asyncio.sleep(self._bt.signal_delay_ms / 1000)
            await self._process_signal(signal)

    async def _process_signal(self, signal: Any) -> None:
        """Score → dedup → simplified strategy evaluation → executor."""
        try:
            scored = await self._scorer.score(signal)
            filtered = self._dedup.process(scored)
            if filtered is None:
                return
            if filtered.recommended_action not in ("buy", "strong_buy"):
                return

            # Simplified strategy selection in backtest
            order = self._signal_to_order(filtered)
            if order:
                await asyncio.sleep(self._bt.execution_delay_ms / 1000)
                await self._executor.place_order(order)
        except Exception as exc:
            self._log.debug("bt_signal_error", error=str(exc))

    def _signal_to_order(self, scored: Any) -> Any:
        """Convert a ScoredSignal directly to an OrderRequest for backtest simplicity."""
        from strategy.models import OrderRequest
        sig = scored.signal
        direction = sig.direction
        if not direction or direction == "neutral":
            return None

        outcome = "Yes" if direction == "buy_yes" else "No"
        snap = self._market_cache.get(sig.market_id, {})
        price = snap.get("yes_price" if outcome == "Yes" else "no_price", 0.5)
        if price <= 0.01 or price >= 0.99:
            return None

        entry = price
        tp = min(0.99, entry * 1.20)
        sl = max(0.01, entry * 0.90)

        size_pct = scored.recommended_position_pct
        bt_balance = self._executor._balance + sum(
            p.size_usd for p in self._executor._positions.values()
        )
        size_usd = bt_balance * size_pct
        if size_usd < 10:
            return None

        return OrderRequest.create(
            signal_id=sig.signal_id,
            strategy_name=sig.signal_type,
            market_id=sig.market_id,
            market_question=sig.market_question,
            side="buy",
            outcome=outcome,
            price=round(entry * 1.005, 4),
            size_usd=round(size_usd, 2),
            position_pct=size_pct,
            confidence_score=scored.confidence_score,
            take_profit=tp,
            stop_loss=sl,
            reasoning=f"Backtest: {sig.signal_type}",
        )

    async def _check_stops(self) -> None:
        """Check all positions for stop-loss / take-profit."""
        from risk.stop_manager import StopManager
        from data_ingestion.polymarket_collector import MarketSnapshot

        portfolio = await self._executor.get_portfolio()
        if not portfolio.positions:
            return

        # Build minimal snapshots from cache
        snaps: dict[str, MarketSnapshot] = {}
        for pos in portfolio.positions:
            snap_data = self._market_cache.get(pos.market_id, {})
            snaps[pos.market_id] = MarketSnapshot(
                market_id=pos.market_id,
                condition_id=pos.market_id,
                question=pos.market_question,
                category="other",
                end_date=None,
                outcomes=["Yes", "No"],
                prices={
                    "Yes": snap_data.get("yes_price", pos.current_price),
                    "No": snap_data.get("no_price", 1 - pos.current_price),
                },
                volumes_24h={},
                liquidity=snap_data.get("liquidity", 0),
                spread=0.02,
                orderbook={},
                price_history=[],
                volatility_1h=0.0,
                volatility_24h=0.0,
                volume_change_rate=0.0,
                timestamp=self._executor.current_time,
            )

        stop_mgr = StopManager()
        close_orders = await stop_mgr.check_all_positions(portfolio, snaps)
        for order in close_orders:
            await self._executor.place_order(order)

    def _market_close_order(self, position: Any, price: float, reason: str) -> Any:
        from strategy.models import OrderRequest
        return OrderRequest.create(
            signal_id=position.signal_id,
            strategy_name=position.strategy_name,
            market_id=position.market_id,
            market_question=position.market_question,
            side="sell",
            outcome=position.outcome,
            order_type="market",
            price=round(price * 0.98, 4),
            size_usd=position.size_usd,
            position_pct=0.0,
            confidence_score=0,
            take_profit=0.0,
            stop_loss=0.0,
            reasoning=reason,
        )

    # ── Parameter optimisation ────────────────────────────────────────────────

    async def optimize(
        self,
        start_date: datetime,
        end_date: datetime,
        param_grid: dict[str, list],
        optimization_target: str = "sharpe_ratio",
        method: str = "grid",
    ) -> OptimizationResult:
        """
        Grid search or Bayesian optimisation over param_grid.
        param_grid example:
          {"backtest.fee_rate": [0.01, 0.02], "risk.max_daily_loss_pct": [0.05, 0.08]}
        """
        if method == "grid":
            return await self._grid_search(start_date, end_date, param_grid, optimization_target)
        else:
            return await self._bayesian_optimize(start_date, end_date, param_grid, optimization_target)

    async def _grid_search(
        self,
        start: datetime,
        end: datetime,
        param_grid: dict[str, list],
        target: str,
    ) -> OptimizationResult:
        import itertools

        keys = list(param_grid.keys())
        values = list(param_grid.values())
        combinations = list(itertools.product(*values))

        best_score = float("-inf")
        best_params: dict = {}
        all_results: list[dict] = []

        self._log.info("grid_search_starting", combinations=len(combinations))

        for combo in combinations:
            params = dict(zip(keys, combo))
            self._apply_params(params)
            try:
                result = await self.run(start, end)
                score = getattr(result.report, target, 0.0)
                all_results.append({"params": params, "score": score})
                if score > best_score:
                    best_score = score
                    best_params = dict(params)
                self._log.debug("grid_combo", params=params, score=round(score, 4))
            except Exception as exc:
                self._log.warning("grid_combo_failed", params=params, error=str(exc))

        self._apply_params(best_params)
        return OptimizationResult(
            best_params=best_params,
            best_score=best_score,
            optimization_target=target,
            all_results=all_results,
        )

    async def _bayesian_optimize(
        self,
        start: datetime,
        end: datetime,
        param_grid: dict[str, list],
        target: str,
    ) -> OptimizationResult:
        """
        Bayesian optimisation using scikit-learn GaussianProcessRegressor.
        Falls back to grid search if scikit-learn is unavailable.
        """
        try:
            from sklearn.gaussian_process import GaussianProcessRegressor
            from sklearn.gaussian_process.kernels import Matern
        except ImportError:
            self._log.warning("sklearn_not_available_falling_back_to_grid")
            return await self._grid_search(start, end, param_grid, target)

        import numpy as np

        keys = list(param_grid.keys())
        bounds = [(0, len(v) - 1) for v in param_grid.values()]
        X_sampled: list[list[float]] = []
        y_sampled: list[float] = []
        all_results: list[dict] = []

        # Initial random samples
        n_init = min(5, max(3, len(keys) * 2))
        rng = np.random.default_rng(42)
        for _ in range(n_init):
            indices = [rng.integers(0, len(param_grid[k])) for k in keys]
            params = {k: param_grid[k][i] for k, i in zip(keys, indices)}
            self._apply_params(params)
            result = await self.run(start, end)
            score = float(getattr(result.report, target, 0.0))
            X_sampled.append([float(i) for i in indices])
            y_sampled.append(score)
            all_results.append({"params": params, "score": score})

        # GP-guided exploration
        gp = GaussianProcessRegressor(kernel=Matern(nu=2.5), normalize_y=True)
        for iteration in range(10):
            X_arr = np.array(X_sampled)
            y_arr = np.array(y_sampled)
            gp.fit(X_arr, y_arr)

            # Acquisition: upper confidence bound
            import itertools
            candidates = list(itertools.product(*[range(len(v)) for v in param_grid.values()]))
            X_cand = np.array([[float(i) for i in c] for c in candidates])
            mu, sigma = gp.predict(X_cand, return_std=True)
            ucb = mu + 1.96 * sigma
            best_idx = int(np.argmax(ucb))
            best_cand = candidates[best_idx]

            params = {k: param_grid[k][i] for k, i in zip(keys, best_cand)}
            self._apply_params(params)
            result = await self.run(start, end)
            score = float(getattr(result.report, target, 0.0))
            X_sampled.append([float(i) for i in best_cand])
            y_sampled.append(score)
            all_results.append({"params": params, "score": score})
            self._log.debug("bayesian_iteration", iter=iteration, score=round(score, 4))

        best_result = max(all_results, key=lambda r: r["score"])
        self._apply_params(best_result["params"])
        return OptimizationResult(
            best_params=best_result["params"],
            best_score=best_result["score"],
            optimization_target=target,
            all_results=all_results,
        )

    def _apply_params(self, params: dict) -> None:
        """Apply dot-separated param path to settings object."""
        for path, value in params.items():
            parts = path.split(".")
            obj = self._settings
            for part in parts[:-1]:
                obj = getattr(obj, part, obj)
            try:
                setattr(obj, parts[-1], value)
            except Exception:
                pass
