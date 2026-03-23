"""
notifications/formatters.py

Message templates and formatting utilities for the Telegram bot.
All message construction lives here — the bot just calls these functions.
"""
from __future__ import annotations

import io
import math
from datetime import datetime, timezone
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _pct(v: float) -> str:
    return f"{v:+.1%}"


def _usd(v: float) -> str:
    return f"${v:,.2f}"


def _bar(value: float, max_value: float = 100.0, width: int = 10) -> str:
    """Render a compact Unicode progress bar."""
    filled = int(round(value / max_value * width))
    filled = max(0, min(width, filled))
    return "█" * filled + "░" * (width - filled)


def _score_emoji(score: int) -> str:
    if score >= 80:
        return "🟢"
    if score >= 60:
        return "🟡"
    return "🔴"


def _action_emoji(action: str) -> str:
    return {"strong_buy": "🚀", "buy": "📈", "watch": "👀", "skip": "⏭️"}.get(action, "❓")


def _side_emoji(side: str) -> str:
    return "🟢" if side.lower() == "buy" else "🔴"


# ─── Signal alert ─────────────────────────────────────────────────────────────

def format_signal_alert(scored: Any, order: Any | None = None) -> str:
    sig = scored.signal
    score = scored.confidence_score
    breakdown = scored.score_breakdown
    action = scored.recommended_action

    lines = [
        f"🚨 *New Signal* | `{sig.signal_type}`",
        "━━━━━━━━━━━━━━━━━━",
        f"📌 *Market:* {_escape(sig.market_question[:120])}",
        f"📊 *Confidence:* `{score}/100` {_score_emoji(score)}",
        "",
        "📈 *Score Breakdown:*",
    ]
    for dim, pts in sorted(breakdown.items(), key=lambda x: -x[1]):
        bar = _bar(pts, max_value=30)
        lines.append(f"  `{dim[:22]:<22}` {bar} `{pts:.0f}pt`")

    lines += [
        "",
        f"🎯 *Direction:* `{sig.direction}`",
        f"⚡ *Action:* {_action_emoji(action)} `{action.upper()}`",
    ]

    if order:
        lines += [
            "",
            "💰 *Suggested Order:*",
            f"  Outcome: `{order.outcome}` @ `{order.price:.4f}`",
            f"  Size: {_usd(order.size_usd)} `({order.position_pct:.1%} of capital)`",
            f"  Take Profit: `{order.take_profit:.4f}` \\| Stop Loss: `{order.stop_loss:.4f}`",
        ]

    if hasattr(sig, "reasoning") and sig.reasoning:
        lines += ["", f"💡 _{_escape(sig.reasoning[:200])}_"]

    lines += [
        "━━━━━━━━━━━━━━━━━━",
        f"⏰ `{_now().strftime('%Y\\-%m\\-%d %H:%M UTC')}`",
    ]
    return "\n".join(lines)


# ─── Trade result ─────────────────────────────────────────────────────────────

def format_trade_result(result: Any, order: Any | None = None) -> str:
    status_emoji = "✅" if result.is_filled else "❌"
    lines = [
        f"{status_emoji} *Trade {result.status.upper()}*",
        f"  Fill: `{result.fill_price:.4f}` | Size: {_usd(result.fill_size)}",
        f"  Fee: {_usd(result.fee)} | Slippage: `{result.slippage:+.2%}`",
    ]
    if result.tx_hash:
        lines.append(f"  TX: `{result.tx_hash[:20]}…`")
    if result.error_message:
        lines.append(f"  ⚠️ _{_escape(result.error_message)}_")
    return "\n".join(lines)


# ─── Position / stop triggered ────────────────────────────────────────────────

def format_stop_triggered(position: Any, trigger_type: str, current_price: float) -> str:
    emoji = "🎯" if trigger_type == "take_profit" else "🛑"
    pnl = position.unrealized_pnl
    pnl_pct = position.unrealized_pnl_pct
    lines = [
        f"{emoji} *{trigger_type.replace('_', ' ').title()} Triggered*",
        f"  Market: _{_escape(position.market_question[:80])}_",
        f"  Outcome: `{position.outcome}` | Entry: `{position.entry_price:.4f}` → `{current_price:.4f}`",
        f"  P&L: `{_usd(pnl)}` `({_pct(pnl_pct)})`",
        f"  Strategy: `{position.strategy_name}`",
    ]
    return "\n".join(lines)


# ─── Whale alert ──────────────────────────────────────────────────────────────

def format_whale_alert(action: Any) -> str:
    tier_emoji = {"S": "🐳", "A": "🦈", "B": "🐟"}.get(action.whale_tier, "🐠")
    act_emoji = "🟢" if action.action == "buy" else "🔴"
    lines = [
        f"{tier_emoji} *Whale Alert* | Tier {action.whale_tier}",
        f"  {act_emoji} `{action.action.upper()}` on _{_escape(action.market_question[:80])}_",
        f"  Amount: `{_usd(action.amount_usd)}` @ `{action.price:.4f}`",
        f"  Whale: `{action.whale_name}` | Win Rate: `{action.whale_win_rate:.0%}`",
        f"  TX: `{action.tx_hash[:20]}…`",
    ]
    return "\n".join(lines)


# ─── Risk alert ───────────────────────────────────────────────────────────────

def format_risk_alert(level: str, message: str) -> str:
    emoji = {"critical": "🚨", "warning": "⚠️", "info": "ℹ️"}.get(level, "⚠️")
    return f"{emoji} *Risk Alert [{level.upper()}]*\n_{_escape(message)}_"


# ─── Circuit breaker ──────────────────────────────────────────────────────────

def format_circuit_breaker(reason: str, duration: str, level: str) -> str:
    return (
        f"🔴 *CIRCUIT BREAKER TRIGGERED*\n"
        f"  Level: `{level}`\n"
        f"  Reason: _{_escape(reason)}_\n"
        f"  Duration: `{duration}`\n"
        f"  Trading is *HALTED*\\. Use /resume to manually clear\\."
    )


# ─── Portfolio status ─────────────────────────────────────────────────────────

def format_portfolio(portfolio: Any) -> str:
    lines = [
        "📊 *Portfolio Status*",
        "━━━━━━━━━━━━━━━━━━",
        f"  💵 Total Balance: `{_usd(portfolio.total_balance)}`",
        f"  💰 Available: `{_usd(portfolio.available_balance)}`",
        f"  📈 Positions Value: `{_usd(portfolio.total_position_value)}`",
        f"  📉 Exposure: `{portfolio.total_exposure_pct:.1%}`",
        f"  📅 Daily P&L: `{_usd(portfolio.daily_pnl)}` `({_pct(portfolio.daily_pnl_pct)})`",
        f"  🏆 Today: `{portfolio.wins_today}W / {portfolio.losses_today}L` "
        f"of `{portfolio.total_trades_today}` trades",
    ]
    if portfolio.positions:
        lines += ["", "📋 *Open Positions:*"]
        for pos in portfolio.positions[:8]:
            pnl_str = f"`{_pct(pos.unrealized_pnl_pct)}`"
            lines.append(
                f"  {_side_emoji(pos.side)} `{pos.outcome}` "
                f"_{_escape(pos.market_question[:50])}_\n"
                f"    `{_usd(pos.size_usd)}` @ `{pos.entry_price:.4f}` → `{pos.current_price:.4f}` {pnl_str}"
            )
    else:
        lines.append("\n  _No open positions_")
    return "\n".join(lines)


# ─── Daily report ─────────────────────────────────────────────────────────────

def format_daily_report(stats: dict) -> str:
    pnl = stats.get("daily_pnl", 0)
    pnl_pct = stats.get("daily_pnl_pct", 0)
    trades = stats.get("total_trades", 0)
    wins = stats.get("wins", 0)
    losses = stats.get("losses", 0)
    wr = wins / trades if trades else 0

    lines = [
        f"📅 *Daily Report — {_now().strftime('%Y\\-%m\\-%d')}*",
        "━━━━━━━━━━━━━━━━━━",
        f"  P&L: `{_usd(pnl)}` `({_pct(pnl_pct)})` {'🟢' if pnl >= 0 else '🔴'}",
        f"  Trades: `{trades}` | W/L: `{wins}/{losses}` | WR: `{wr:.0%}`",
        f"  Balance: `{_usd(stats.get('balance', 0))}`",
    ]
    top = stats.get("top_trades", [])
    if top:
        lines += ["", "🏆 *Best Trades:*"]
        for t in top[:3]:
            lines.append(
                f"  ✅ _{_escape(str(t.get('market', ''))[:50])}_ `{_pct(t.get('pnl_pct', 0))}`"
            )
    return "\n".join(lines)


# ─── System status ────────────────────────────────────────────────────────────

def format_system_status(health: dict, mode: str, cb_active: bool) -> str:
    mode_emoji = {"live": "🔴", "paper": "🟡", "backtest": "🔵"}.get(mode, "⚪")
    cb_str = "🔴 *ACTIVE*" if cb_active else "🟢 Clear"
    lines = [
        f"⚙️ *System Status*",
        f"  Mode: {mode_emoji} `{mode.upper()}`",
        f"  Circuit Breaker: {cb_str}",
        "",
        "📡 *Collectors:*",
    ]
    for name, h in health.items():
        ok = h.get("is_healthy", False)
        emoji = "🟢" if ok else "🔴"
        msgs = h.get("messages_total", 0)
        lines.append(f"  {emoji} `{name:<15}` msgs: `{msgs:,}` errs: `{h.get('errors_total', 0)}`")
    return "\n".join(lines)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _escape(text: str) -> str:
    """Escape MarkdownV2 special characters."""
    special = r"\_*[]()~`>#+-=|{}.!"
    return "".join(f"\\{c}" if c in special else c for c in str(text))


def make_progress_bar(value: float, total: float, width: int = 12) -> str:
    pct = value / total if total > 0 else 0
    return _bar(pct, 1.0, width) + f" `{pct:.0%}`"


async def send_chart_to_telegram(
    bot: Any,
    chat_id: str,
    fig: Any,
    caption: str = "",
) -> None:
    """Render a matplotlib figure and send as photo."""
    try:
        import matplotlib
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=120, bbox_inches="tight",
                    facecolor="white", edgecolor="none")
        buf.seek(0)
        await bot.send_photo(chat_id=chat_id, photo=buf, caption=caption[:1024])
        buf.close()
        import matplotlib.pyplot as plt
        plt.close(fig)
    except Exception as exc:
        logger.warning("chart_send_failed", error=str(exc))
