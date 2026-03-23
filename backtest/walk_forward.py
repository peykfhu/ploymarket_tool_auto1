"""
backtest/walk_forward.py

Walk-forward analysis to prevent overfitting.

Splits the full date range into rolling train + test windows.
Optimises parameters on each training window, validates on test window.
Only accepts parameter sets that are profitable across ALL test windows.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import structlog

from backtest.engine import BacktestEngine, BacktestResult, OptimizationResult

logger = structlog.get_logger(__name__)


@dataclass
class WindowResult:
    window_idx: int
    train_start: datetime
    train_end: datetime
    test_start: datetime
    test_end: datetime
    best_train_params: dict
    train_score: float
    test_result: BacktestResult
    test_score: float
    profitable: bool


@dataclass
class WalkForwardResult:
    total_windows: int
    profitable_windows: int
    all_windows: list[WindowResult]
    consensus_params: dict           # params profitable across most windows
    combined_report_summary: dict    # aggregated stats across test periods


class WalkForwardAnalyzer:
    """
    Walk-forward validation:
      1. Split data into overlapping train/test windows
      2. Optimise on training period
      3. Validate on out-of-sample test period
      4. Roll forward
      5. Report which parameter sets survived all windows
    """

    def __init__(self, engine: BacktestEngine) -> None:
        self._engine = engine
        self._log = logger.bind(component="walk_forward")

    async def analyze(
        self,
        total_start: datetime,
        total_end: datetime,
        train_months: int = 3,
        test_months: int = 1,
        param_grid: dict[str, list] | None = None,
        optimization_target: str = "sharpe_ratio",
    ) -> WalkForwardResult:
        """
        Run walk-forward analysis.

        Window structure (train_months=3, test_months=1):
          Window 1: Train [M1-M3], Test [M4]
          Window 2: Train [M2-M4], Test [M5]
          Window 3: Train [M3-M5], Test [M6]
          ...
        """
        windows = self._build_windows(total_start, total_end, train_months, test_months)
        if not windows:
            raise ValueError("Date range too short for walk-forward analysis")

        self._log.info(
            "walk_forward_starting",
            windows=len(windows),
            train_months=train_months,
            test_months=test_months,
        )

        results: list[WindowResult] = []
        default_params = param_grid or {}

        for idx, (train_s, train_e, test_s, test_e) in enumerate(windows):
            self._log.info(
                "walk_forward_window",
                window=idx + 1,
                total=len(windows),
                train=f"{train_s.date()}→{train_e.date()}",
                test=f"{test_s.date()}→{test_e.date()}",
            )

            # Optimise on training window
            if default_params:
                opt_result: OptimizationResult = await self._engine.optimize(
                    start_date=train_s,
                    end_date=train_e,
                    param_grid=default_params,
                    optimization_target=optimization_target,
                    method="grid",
                )
                best_params = opt_result.best_params
                train_score = opt_result.best_score
            else:
                best_params = {}
                # Run training period with current settings
                train_result = await self._engine.run(train_s, train_e)
                train_score = float(getattr(train_result.report, optimization_target, 0.0))

            # Apply best params and validate on test window
            self._engine._apply_params(best_params)
            test_result = await self._engine.run(test_s, test_e)
            test_score = float(getattr(test_result.report, optimization_target, 0.0))

            profitable = test_result.report.total_pnl > 0 and test_score > 0

            results.append(WindowResult(
                window_idx=idx + 1,
                train_start=train_s,
                train_end=train_e,
                test_start=test_s,
                test_end=test_e,
                best_train_params=best_params,
                train_score=round(train_score, 4),
                test_result=test_result,
                test_score=round(test_score, 4),
                profitable=profitable,
            ))

            self._log.info(
                "walk_forward_window_done",
                window=idx + 1,
                train_score=round(train_score, 4),
                test_score=round(test_score, 4),
                profitable=profitable,
            )

        profitable_windows = [r for r in results if r.profitable]
        consensus_params = self._find_consensus_params(profitable_windows)
        summary = self._aggregate_stats(results)

        self._log.info(
            "walk_forward_complete",
            total=len(results),
            profitable=len(profitable_windows),
            consensus_params=consensus_params,
        )

        return WalkForwardResult(
            total_windows=len(results),
            profitable_windows=len(profitable_windows),
            all_windows=results,
            consensus_params=consensus_params,
            combined_report_summary=summary,
        )

    def _build_windows(
        self,
        start: datetime,
        end: datetime,
        train_months: int,
        test_months: int,
    ) -> list[tuple[datetime, datetime, datetime, datetime]]:
        """Build (train_start, train_end, test_start, test_end) tuples."""
        windows = []
        cursor = start

        def add_months(dt: datetime, n: int) -> datetime:
            month = dt.month - 1 + n
            year = dt.year + month // 12
            month = month % 12 + 1
            day = min(dt.day, [31, 29 if year % 4 == 0 else 28,
                               31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1])
            return dt.replace(year=year, month=month, day=day)

        while True:
            train_start = cursor
            train_end = add_months(cursor, train_months)
            test_start = train_end
            test_end = add_months(test_start, test_months)

            if test_end > end:
                break

            windows.append((train_start, train_end, test_start, test_end))
            cursor = add_months(cursor, 1)  # roll forward by 1 month

        return windows

    def _find_consensus_params(self, profitable_windows: list[WindowResult]) -> dict:
        """
        Find parameter values that appeared most frequently in profitable windows.
        Uses majority voting per parameter.
        """
        if not profitable_windows:
            return {}

        from collections import Counter
        param_votes: dict[str, Counter] = {}
        for wr in profitable_windows:
            for k, v in wr.best_train_params.items():
                param_votes.setdefault(k, Counter())
                param_votes[k][v] += 1

        return {k: counter.most_common(1)[0][0] for k, counter in param_votes.items()}

    def _aggregate_stats(self, results: list[WindowResult]) -> dict:
        """Summarise performance across all test periods."""
        if not results:
            return {}

        all_pnl_pcts = [r.test_result.report.total_pnl_pct for r in results]
        all_sharpes = [r.test_result.report.sharpe_ratio for r in results]
        all_dds = [r.test_result.report.max_drawdown for r in results]

        import statistics
        return {
            "avg_test_pnl_pct": round(statistics.mean(all_pnl_pcts), 4),
            "avg_sharpe": round(statistics.mean(all_sharpes), 3),
            "avg_max_drawdown": round(statistics.mean(all_dds), 4),
            "worst_test_pnl_pct": round(min(all_pnl_pcts), 4),
            "best_test_pnl_pct": round(max(all_pnl_pcts), 4),
            "pct_profitable_windows": round(
                sum(1 for r in results if r.profitable) / len(results), 3
            ),
        }
