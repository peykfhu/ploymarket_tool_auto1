"""
backtest/performance.py

Performance analyzer. Computes all standard quant metrics from a list of
TradeRecords and an equity curve, and generates matplotlib charts.
"""
from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
import structlog

logger = structlog.get_logger(__name__)


@dataclass
class PerformanceReport:
    # ── Overall ──────────────────────────────────────────────────────────────
    total_pnl: float
    total_pnl_pct: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    profit_factor: float

    # ── Returns ──────────────────────────────────────────────────────────────
    annual_return: float
    monthly_avg_return: float
    best_month: float
    worst_month: float

    # ── Risk ─────────────────────────────────────────────────────────────────
    max_drawdown: float
    max_drawdown_duration: timedelta
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float
    volatility: float

    # ── Trade stats ───────────────────────────────────────────────────────────
    avg_win: float
    avg_loss: float
    avg_win_loss_ratio: float
    avg_holding_time: timedelta
    max_consecutive_wins: int
    max_consecutive_losses: int

    # ── Breakdown ─────────────────────────────────────────────────────────────
    strategy_breakdown: dict[str, dict]
    monthly_returns: pd.DataFrame
    plots: dict[str, Any] = field(default_factory=dict)

    def to_markdown(self) -> str:
        lines = [
            "# Backtest Performance Report",
            "",
            "## Summary",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| Total P&L | ${self.total_pnl:,.2f} ({self.total_pnl_pct:+.1%}) |",
            f"| Total Trades | {self.total_trades} |",
            f"| Win Rate | {self.win_rate:.1%} |",
            f"| Profit Factor | {self.profit_factor:.2f} |",
            f"| Annual Return | {self.annual_return:.1%} |",
            f"| Max Drawdown | {self.max_drawdown:.1%} |",
            f"| Sharpe Ratio | {self.sharpe_ratio:.2f} |",
            f"| Sortino Ratio | {self.sortino_ratio:.2f} |",
            f"| Calmar Ratio | {self.calmar_ratio:.2f} |",
            f"| Volatility | {self.volatility:.1%} |",
            f"| Avg Win | ${self.avg_win:.2f} |",
            f"| Avg Loss | ${self.avg_loss:.2f} |",
            f"| Win/Loss Ratio | {self.avg_win_loss_ratio:.2f} |",
            f"| Avg Holding Time | {str(self.avg_holding_time).split('.')[0]} |",
            f"| Max Consec. Wins | {self.max_consecutive_wins} |",
            f"| Max Consec. Losses | {self.max_consecutive_losses} |",
            "",
            "## Strategy Breakdown",
            "| Strategy | Trades | Win Rate | Avg P&L |",
            "|----------|--------|----------|---------|",
        ]
        for strat, stats in self.strategy_breakdown.items():
            lines.append(
                f"| {strat} | {stats['trades']} | "
                f"{stats['win_rate']:.1%} | ${stats['avg_pnl']:.2f} |"
            )
        return "\n".join(lines)

    def to_html(self) -> str:
        md = self.to_markdown()
        try:
            import markdown
            return markdown.markdown(md, extensions=["tables"])
        except ImportError:
            return f"<pre>{md}</pre>"


class PerformanceAnalyzer:
    """Generates a PerformanceReport from trade history and equity curve."""

    def __init__(self, risk_free_rate: float = 0.05) -> None:
        self._rfr = risk_free_rate
        self._log = logger.bind(component="performance_analyzer")

    def analyze(
        self,
        trades: list[Any],
        equity_curve: list[tuple[datetime, float]],
        initial_balance: float,
    ) -> PerformanceReport:
        closed = [t for t in trades if t.realized_pnl is not None]
        eq_series = self._equity_series(equity_curve)

        total_pnl = sum(t.realized_pnl for t in closed)
        total_pnl_pct = total_pnl / initial_balance if initial_balance > 0 else 0.0
        wins = [t for t in closed if (t.realized_pnl or 0) >= 0]
        losses = [t for t in closed if (t.realized_pnl or 0) < 0]
        win_rate = len(wins) / len(closed) if closed else 0.0

        gross_wins = sum(t.realized_pnl for t in wins) if wins else 0.0
        gross_losses = abs(sum(t.realized_pnl for t in losses)) if losses else 0.0
        profit_factor = gross_wins / gross_losses if gross_losses > 0 else float("inf")

        returns = self._calculate_returns(eq_series)
        dd_stats = self._calculate_drawdown(eq_series)
        sharpe = self._calculate_sharpe(returns)
        sortino = self._calculate_sortino(returns)

        annual_return = (1 + total_pnl_pct) ** (365 / max(1, self._days_span(equity_curve))) - 1
        calmar = self._calculate_calmar(annual_return, dd_stats["max_drawdown"])

        monthly_df = self._monthly_returns(eq_series)

        avg_win = statistics.mean([t.realized_pnl for t in wins]) if wins else 0.0
        avg_loss = statistics.mean([abs(t.realized_pnl) for t in losses]) if losses else 0.0
        ratio = avg_win / avg_loss if avg_loss > 0 else float("inf")

        avg_hold = self._avg_holding_time(closed)
        max_cw, max_cl = self._consecutive_streaks(closed)

        strat_breakdown = self._strategy_breakdown(closed)

        plots = self._generate_plots(eq_series, closed, monthly_df)

        return PerformanceReport(
            total_pnl=round(total_pnl, 2),
            total_pnl_pct=round(total_pnl_pct, 4),
            total_trades=len(closed),
            winning_trades=len(wins),
            losing_trades=len(losses),
            win_rate=round(win_rate, 4),
            profit_factor=round(profit_factor, 4),
            annual_return=round(annual_return, 4),
            monthly_avg_return=round(monthly_df["return"].mean() if not monthly_df.empty else 0, 4),
            best_month=round(monthly_df["return"].max() if not monthly_df.empty else 0, 4),
            worst_month=round(monthly_df["return"].min() if not monthly_df.empty else 0, 4),
            max_drawdown=round(dd_stats["max_drawdown"], 4),
            max_drawdown_duration=dd_stats["max_drawdown_duration"],
            sharpe_ratio=round(sharpe, 3),
            sortino_ratio=round(sortino, 3),
            calmar_ratio=round(calmar, 3),
            volatility=round(returns.std() * math.sqrt(252) if len(returns) > 1 else 0, 4),
            avg_win=round(avg_win, 4),
            avg_loss=round(avg_loss, 4),
            avg_win_loss_ratio=round(ratio, 4),
            avg_holding_time=avg_hold,
            max_consecutive_wins=max_cw,
            max_consecutive_losses=max_cl,
            strategy_breakdown=strat_breakdown,
            monthly_returns=monthly_df,
            plots=plots,
        )

    # ── Metric calculations ───────────────────────────────────────────────────

    def _equity_series(self, curve: list[tuple[datetime, float]]) -> pd.Series:
        if not curve:
            return pd.Series(dtype=float)
        times, values = zip(*curve)
        return pd.Series(values, index=pd.DatetimeIndex(times, tz="UTC"), name="equity")

    def _calculate_returns(self, equity: pd.Series) -> pd.Series:
        if len(equity) < 2:
            return pd.Series(dtype=float)
        return equity.pct_change().dropna()

    def _calculate_drawdown(self, equity: pd.Series) -> dict:
        if equity.empty:
            return {"max_drawdown": 0.0, "max_drawdown_duration": timedelta(0)}
        roll_max = equity.cummax()
        drawdown = (equity - roll_max) / roll_max.replace(0, float("nan"))
        max_dd = float(drawdown.min()) if not drawdown.empty else 0.0

        # Compute max drawdown duration
        in_dd = drawdown < 0
        max_dur = timedelta(0)
        start = None
        for ts, flag in in_dd.items():
            if flag and start is None:
                start = ts
            elif not flag and start is not None:
                dur = ts - start
                if dur > max_dur:
                    max_dur = dur
                start = None
        if start is not None:
            dur = in_dd.index[-1] - start
            if dur > max_dur:
                max_dur = dur

        return {"max_drawdown": abs(max_dd), "max_drawdown_duration": max_dur}

    def _calculate_sharpe(self, returns: pd.Series) -> float:
        if len(returns) < 2:
            return 0.0
        excess = returns - (self._rfr / 252)
        std = excess.std()
        if std == 0:
            return 0.0
        return float(excess.mean() / std * math.sqrt(252))

    def _calculate_sortino(self, returns: pd.Series) -> float:
        if len(returns) < 2:
            return 0.0
        target = self._rfr / 252
        downside = returns[returns < target] - target
        downside_std = math.sqrt((downside ** 2).mean()) if len(downside) > 0 else 0.0
        if downside_std == 0:
            return 0.0
        return float((returns.mean() - target) / downside_std * math.sqrt(252))

    def _calculate_calmar(self, annual_return: float, max_drawdown: float) -> float:
        return annual_return / max_drawdown if max_drawdown > 0 else 0.0

    def _monthly_returns(self, equity: pd.Series) -> pd.DataFrame:
        if equity.empty:
            return pd.DataFrame(columns=["year", "month", "return"])
        monthly = equity.resample("ME").last()
        pct = monthly.pct_change().dropna()
        rows = [
            {"year": ts.year, "month": ts.month, "return": round(float(r), 4)}
            for ts, r in pct.items()
        ]
        return pd.DataFrame(rows)

    def _strategy_breakdown(self, trades: list[Any]) -> dict[str, dict]:
        from collections import defaultdict
        grouped: dict[str, list] = defaultdict(list)
        for t in trades:
            grouped[t.strategy_name].append(t)

        result = {}
        for strat, strat_trades in grouped.items():
            wins = [t for t in strat_trades if (t.realized_pnl or 0) >= 0]
            result[strat] = {
                "trades": len(strat_trades),
                "win_rate": len(wins) / len(strat_trades) if strat_trades else 0,
                "total_pnl": round(sum(t.realized_pnl or 0 for t in strat_trades), 2),
                "avg_pnl": round(
                    statistics.mean([t.realized_pnl or 0 for t in strat_trades]), 2
                ) if strat_trades else 0,
            }
        return result

    def _avg_holding_time(self, trades: list[Any]) -> timedelta:
        durations = [
            t.closed_at - t.opened_at
            for t in trades
            if t.closed_at and t.opened_at
        ]
        if not durations:
            return timedelta(0)
        total_s = sum(d.total_seconds() for d in durations)
        return timedelta(seconds=total_s / len(durations))

    def _consecutive_streaks(self, trades: list[Any]) -> tuple[int, int]:
        max_wins = max_losses = cur_wins = cur_losses = 0
        for t in trades:
            if (t.realized_pnl or 0) >= 0:
                cur_wins += 1
                cur_losses = 0
                max_wins = max(max_wins, cur_wins)
            else:
                cur_losses += 1
                cur_wins = 0
                max_losses = max(max_losses, cur_losses)
        return max_wins, max_losses

    def _days_span(self, curve: list[tuple[datetime, float]]) -> int:
        if len(curve) < 2:
            return 1
        return max(1, (curve[-1][0] - curve[0][0]).days)

    # ── Charts ────────────────────────────────────────────────────────────────

    def _generate_plots(
        self,
        equity: pd.Series,
        trades: list[Any],
        monthly_df: pd.DataFrame,
    ) -> dict[str, Any]:
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            import matplotlib.dates as mdates
            from collections import Counter

            plots: dict[str, Any] = {}

            # 1. Equity curve + drawdown
            if not equity.empty:
                fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 7), sharex=True)
                ax1.plot(equity.index, equity.values, color="#2563eb", linewidth=1.5)
                ax1.set_title("Equity Curve")
                ax1.set_ylabel("Portfolio Value ($)")
                ax1.grid(True, alpha=0.3)

                roll_max = equity.cummax()
                dd = (equity - roll_max) / roll_max.replace(0, float("nan"))
                ax2.fill_between(dd.index, dd.values, 0, color="#ef4444", alpha=0.4)
                ax2.set_ylabel("Drawdown")
                ax2.set_xlabel("Date")
                ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
                plt.tight_layout()
                plots["equity_curve"] = fig

            # 2. Monthly returns heatmap
            if not monthly_df.empty:
                pivot = monthly_df.pivot(index="year", columns="month", values="return")
                fig2, ax = plt.subplots(figsize=(12, max(3, len(pivot) * 0.6)))
                im = ax.imshow(pivot.values, cmap="RdYlGn", aspect="auto",
                               vmin=-0.1, vmax=0.1)
                ax.set_xticks(range(12))
                ax.set_xticklabels(["Jan","Feb","Mar","Apr","May","Jun",
                                    "Jul","Aug","Sep","Oct","Nov","Dec"])
                ax.set_yticks(range(len(pivot)))
                ax.set_yticklabels(pivot.index.tolist())
                for i in range(len(pivot)):
                    for j in range(12):
                        val = pivot.values[i, j]
                        if not math.isnan(val):
                            ax.text(j, i, f"{val:.1%}", ha="center", va="center",
                                    fontsize=7, color="black")
                plt.colorbar(im, ax=ax, label="Return")
                ax.set_title("Monthly Returns Heatmap")
                plt.tight_layout()
                plots["monthly_heatmap"] = fig2

            # 3. P&L distribution histogram
            if trades:
                pnls = [t.realized_pnl for t in trades if t.realized_pnl is not None]
                if pnls:
                    fig3, ax = plt.subplots(figsize=(10, 5))
                    ax.hist(pnls, bins=40, color="#6366f1", edgecolor="white", alpha=0.8)
                    ax.axvline(0, color="#ef4444", linewidth=1.5, linestyle="--")
                    ax.set_title("P&L Distribution")
                    ax.set_xlabel("P&L ($)")
                    ax.set_ylabel("Frequency")
                    plt.tight_layout()
                    plots["pnl_distribution"] = fig3

            # 4. Strategy pie chart
            if trades:
                by_strat: dict[str, float] = {}
                for t in trades:
                    if t.realized_pnl and t.realized_pnl > 0:
                        by_strat[t.strategy_name] = by_strat.get(t.strategy_name, 0) + t.realized_pnl
                if by_strat:
                    fig4, ax = plt.subplots(figsize=(8, 6))
                    ax.pie(by_strat.values(), labels=by_strat.keys(),
                           autopct="%1.1f%%", startangle=90)
                    ax.set_title("Profit by Strategy")
                    plt.tight_layout()
                    plots["strategy_pie"] = fig4

            # 5. Holding time histogram
            if trades:
                hold_hours = [
                    (t.closed_at - t.opened_at).total_seconds() / 3600
                    for t in trades if t.closed_at and t.opened_at
                ]
                if hold_hours:
                    fig5, ax = plt.subplots(figsize=(10, 5))
                    ax.hist(hold_hours, bins=30, color="#10b981", edgecolor="white", alpha=0.8)
                    ax.set_title("Holding Time Distribution")
                    ax.set_xlabel("Hours")
                    ax.set_ylabel("Frequency")
                    plt.tight_layout()
                    plots["holding_time"] = fig5

            return plots

        except Exception as exc:
            logger.warning("plot_generation_failed", error=str(exc))
            return {}
