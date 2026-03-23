"""
notifications/telegram_bot.py

Full Telegram Bot implementation using python-telegram-bot v20+.

Features:
  1. Push notifications  — signals, trades, whale alerts, daily/weekly reports
  2. Interactive commands — /status /portfolio /balance /trades /pause /resume etc.
  3. Inline keyboard confirmations — high-risk actions require explicit approval
  4. Chart sending — weekly reports include matplotlib figures
  5. Rate limiting — via RateLimiter (priority queue + token bucket)
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import structlog
from telegram import (
    Bot,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

from notifications.formatters import (
    _escape,
    format_circuit_breaker,
    format_daily_report,
    format_portfolio,
    format_risk_alert,
    format_signal_alert,
    format_stop_triggered,
    format_system_status,
    format_trade_result,
    format_whale_alert,
    send_chart_to_telegram,
)
from notifications.rate_limiter import Priority, PendingMessage, RateLimiter

logger = structlog.get_logger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class TelegramNotifier:
    """
    Manages all outbound Telegram communication.

    Inject into any system component via:
        notifier = TelegramNotifier(token=..., chat_id=..., admin_ids=[...])
        await notifier.start()

    Then call push methods from anywhere:
        await notifier.send_signal_alert(scored_signal, order)
        await notifier.send_trade_result(result)
    """

    CONFIRMATION_TIMEOUT_S = 60

    def __init__(
        self,
        bot_token: str,
        chat_id: str,
        admin_ids: list[int],
        max_messages_per_minute: int = 10,
        quiet_start: int = 2,
        quiet_end: int = 6,
        system_ref: Any = None,   # TradingSystem — injected after init
    ) -> None:
        self._token = bot_token
        self._chat_id = chat_id
        self._admin_ids = set(admin_ids)
        self._system = system_ref
        self._limiter = RateLimiter(
            max_per_minute=max_messages_per_minute,
            quiet_hour_start=quiet_start,
            quiet_hour_end=quiet_end,
        )
        self._log = logger.bind(component="telegram")

        self._app: Application | None = None
        self._bot: Bot | None = None

        # Pending confirmation callbacks: callback_data → asyncio.Future
        self._confirmations: dict[str, asyncio.Future] = {}

        # Dispatcher task
        self._dispatch_task: asyncio.Task | None = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        if not self._token:
            self._log.warning("telegram_token_missing_bot_disabled")
            return

        self._app = (
            Application.builder()
            .token(self._token)
            .build()
        )
        self._bot = self._app.bot
        self._register_handlers()

        await self._app.initialize()
        await self._app.start()
        await self._app.updater.start_polling(drop_pending_updates=True)

        self._dispatch_task = asyncio.create_task(
            self._dispatch_loop(), name="telegram_dispatch"
        )
        self._log.info("telegram_bot_started", chat_id=self._chat_id)

    async def stop(self) -> None:
        if self._dispatch_task:
            self._dispatch_task.cancel()
            try:
                await self._dispatch_task
            except asyncio.CancelledError:
                pass
        if self._app:
            await self._app.updater.stop()
            await self._app.stop()
            await self._app.shutdown()

    def set_system(self, system: Any) -> None:
        self._system = system

    # ── Push notification methods ─────────────────────────────────────────────

    async def send_signal_alert(self, scored: Any, order: Any = None) -> None:
        text = format_signal_alert(scored, order)
        buttons = None
        if order:
            buttons = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Execute", callback_data=f"exec:{scored.signal.signal_id}"),
                InlineKeyboardButton("❌ Skip", callback_data=f"skip:{scored.signal.signal_id}"),
                InlineKeyboardButton("📝 Half Size", callback_data=f"half:{scored.signal.signal_id}"),
            ]])
        await self._enqueue(text, Priority.MEDIUM, reply_markup=buttons)

    async def send_trade_result(self, result: Any) -> None:
        text = format_trade_result(result)
        await self._enqueue(text, Priority.HIGH)

    async def send_stop_triggered(self, position: Any, trigger_type: str, current_price: float = 0.0) -> None:
        text = format_stop_triggered(position, trigger_type, current_price)
        await self._enqueue(text, Priority.HIGH)

    async def send_whale_alert(self, action: Any) -> None:
        text = format_whale_alert(action)
        await self._enqueue(text, Priority.LOW)

    async def send_risk_alert(self, level: str, message: str) -> None:
        text = format_risk_alert(level, message)
        priority = Priority.CRITICAL if level == "critical" else Priority.HIGH
        await self._enqueue(text, priority)

    async def send_circuit_breaker(self, reason: str, duration: str, level: str = "daily") -> None:
        text = format_circuit_breaker(reason, duration, level)
        await self._enqueue(text, Priority.CRITICAL)

    async def send_daily_report(self, stats: dict) -> None:
        text = format_daily_report(stats)
        await self._enqueue(text, Priority.LOW)

    async def send_weekly_report(self, report: Any) -> None:
        text = f"📊 *Weekly Performance Report*\n\n`{_escape(report.to_markdown()[:3000])}`"
        await self._enqueue(text, Priority.LOW)

        # Send charts if available
        for chart_name, fig in (report.plots or {}).items():
            await send_chart_to_telegram(
                self._bot, self._chat_id, fig,
                caption=f"Weekly — {chart_name.replace('_', ' ').title()}"
            )

    async def send_message(self, text: str, priority: Priority = Priority.MEDIUM) -> None:
        """Generic send — usable from anywhere."""
        await self._enqueue(text, priority)

    # ── Confirmation mechanism ────────────────────────────────────────────────

    async def request_confirmation(
        self,
        action: str,
        details: str,
        timeout_s: int = CONFIRMATION_TIMEOUT_S,
    ) -> bool:
        """
        Send a confirmation request with InlineKeyboard.
        Returns True if approved within timeout, False otherwise.
        """
        import uuid
        token = uuid.uuid4().hex[:8]
        approve_cb = f"confirm_yes:{token}"
        reject_cb = f"confirm_no:{token}"

        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._confirmations[approve_cb] = future
        self._confirmations[reject_cb] = future

        text = (
            f"⚠️ *Confirmation Required*\n\n"
            f"Action: `{_escape(action)}`\n"
            f"Details: _{_escape(details)}_\n\n"
            f"_Expires in {timeout_s}s_"
        )
        buttons = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Approve", callback_data=approve_cb),
            InlineKeyboardButton("❌ Cancel", callback_data=reject_cb),
        ]])

        await self._send_now(text, reply_markup=buttons)

        try:
            result = await asyncio.wait_for(future, timeout=timeout_s)
            return result
        except asyncio.TimeoutError:
            self._confirmations.pop(approve_cb, None)
            self._confirmations.pop(reject_cb, None)
            await self._send_now("⏰ _Confirmation timed out\\. Action cancelled\\._")
            return False

    # ── Command handlers ──────────────────────────────────────────────────────

    def _register_handlers(self) -> None:
        app = self._app
        commands = [
            ("start", self._cmd_start),
            ("help", self._cmd_help),
            ("status", self._cmd_status),
            ("portfolio", self._cmd_portfolio),
            ("balance", self._cmd_balance),
            ("trades", self._cmd_trades),
            ("performance", self._cmd_performance),
            ("signals", self._cmd_signals),
            ("whales", self._cmd_whales),
            ("pause", self._cmd_pause),
            ("resume", self._cmd_resume),
            ("close", self._cmd_close),
            ("close_all", self._cmd_close_all),
            ("set_mode", self._cmd_set_mode),
            ("set_risk", self._cmd_set_risk),
            ("backtest", self._cmd_backtest),
            ("report", self._cmd_report),
        ]
        for cmd, handler in commands:
            app.add_handler(CommandHandler(cmd, handler))
        app.add_handler(CallbackQueryHandler(self._handle_callback))

    def _is_admin(self, user_id: int) -> bool:
        return not self._admin_ids or user_id in self._admin_ids

    async def _cmd_start(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        await update.message.reply_text(
            "👋 *Polymarket Trading Bot*\n\nType /help for available commands\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )

    async def _cmd_help(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        text = (
            "📋 *Available Commands*\n\n"
            "*Queries*\n"
            "/status \\— System status and health\n"
            "/portfolio \\— Current positions and P&L\n"
            "/balance \\— Capital balance\n"
            "/trades \\— Today's trade records\n"
            "/performance \\— Performance stats\n"
            "/signals \\— Pending signal queue\n"
            "/whales \\— Latest whale activity\n\n"
            "*Control* \\(admin only\\)\n"
            "/pause \\— Pause all trading\n"
            "/resume \\— Resume trading\n"
            "/close \\<id\\> \\— Close a specific position\n"
            "/close\\_all \\— Close all positions\n"
            "/set\\_mode \\<live|paper\\> \\— Switch mode\n"
            "/set\\_risk \\<conservative|moderate|aggressive\\>\n"
            "/backtest \\<start\\> \\<end\\> \\— Run a backtest\n"
            "/report \\— Generate report now\n"
        )
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2)

    async def _cmd_status(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._system:
            await update.message.reply_text("_System not connected\\._", parse_mode=ParseMode.MARKDOWN_V2)
            return
        try:
            health = self._system.get_collector_health()
            mode = self._system.mode
            cb = await self._system.get_circuit_breaker_status()
            text = format_system_status(health, mode, cb.active)
            await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2)
        except Exception as exc:
            await update.message.reply_text(f"_Error: {_escape(str(exc))}_", parse_mode=ParseMode.MARKDOWN_V2)

    async def _cmd_portfolio(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._system:
            return
        try:
            portfolio = await self._system.get_portfolio()
            text = format_portfolio(portfolio)
            await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2)
        except Exception as exc:
            await update.message.reply_text(f"_Error: {_escape(str(exc))}_", parse_mode=ParseMode.MARKDOWN_V2)

    async def _cmd_balance(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._system:
            return
        try:
            portfolio = await self._system.get_portfolio()
            text = (
                f"💵 *Balance*\n"
                f"  Available: `${portfolio.available_balance:,.2f}`\n"
                f"  In Positions: `${portfolio.total_position_value:,.2f}`\n"
                f"  Total: `${portfolio.total_balance:,.2f}`"
            )
            await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2)
        except Exception as exc:
            await update.message.reply_text(f"_Error: {_escape(str(exc))}_", parse_mode=ParseMode.MARKDOWN_V2)

    async def _cmd_trades(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._system:
            return
        try:
            trades = await self._system.get_today_trades()
            if not trades:
                await update.message.reply_text("_No trades today\\._", parse_mode=ParseMode.MARKDOWN_V2)
                return
            lines = [f"📋 *Today's Trades* \\({len(trades)}\\)"]
            for t in trades[:10]:
                pnl = t.realized_pnl or 0
                emoji = "✅" if pnl >= 0 else "❌"
                lines.append(
                    f"{emoji} `{t.outcome}` _{_escape(t.market_question[:50])}_\n"
                    f"    Entry `{t.entry_price:.4f}` → Exit `{t.exit_price or 0:.4f}` "
                    f"P&L `${pnl:+.2f}`"
                )
            await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN_V2)
        except Exception as exc:
            await update.message.reply_text(f"_Error: {_escape(str(exc))}_", parse_mode=ParseMode.MARKDOWN_V2)

    async def _cmd_performance(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._system:
            return
        try:
            stats = await self._system.get_performance_stats()
            pnl = stats.get("total_pnl", 0)
            wr = stats.get("win_rate", 0)
            sharpe = stats.get("sharpe_ratio", 0)
            max_dd = stats.get("max_drawdown", 0)
            text = (
                f"📊 *Performance Summary*\n\n"
                f"  Total P&L: `${pnl:+,.2f}` `({stats.get('total_pnl_pct', 0):+.1%})`\n"
                f"  Win Rate: `{wr:.1%}` \\({stats.get('wins', 0)}W / {stats.get('losses', 0)}L\\)\n"
                f"  Sharpe: `{sharpe:.2f}` \\| Max DD: `{max_dd:.1%}`\n"
                f"  Trades: `{stats.get('total_trades', 0)}`"
            )
            await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2)
        except Exception as exc:
            await update.message.reply_text(f"_Error: {_escape(str(exc))}_", parse_mode=ParseMode.MARKDOWN_V2)

    async def _cmd_signals(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        await update.message.reply_text(
            "_Signal queue display coming soon\\._", parse_mode=ParseMode.MARKDOWN_V2
        )

    async def _cmd_whales(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._system:
            return
        try:
            whales = await self._system.get_recent_whale_actions(limit=5)
            if not whales:
                await update.message.reply_text("_No recent whale activity\\._", parse_mode=ParseMode.MARKDOWN_V2)
                return
            lines = [f"🐳 *Recent Whale Activity*"]
            for w in whales:
                tier_e = {"S": "🐳", "A": "🦈", "B": "🐟"}.get(w.whale_tier, "🐠")
                act_e = "🟢" if w.action == "buy" else "🔴"
                lines.append(
                    f"{tier_e}{act_e} `{w.whale_name}` \\(Tier {w.whale_tier}\\)\n"
                    f"  `{w.action.upper()}` `${w.amount_usd:,.0f}` on _{_escape(w.market_question[:50])}_"
                )
            await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN_V2)
        except Exception as exc:
            await update.message.reply_text(f"_Error: {_escape(str(exc))}_", parse_mode=ParseMode.MARKDOWN_V2)

    async def _cmd_pause(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("🚫 _Admin only\\._", parse_mode=ParseMode.MARKDOWN_V2)
            return
        confirmed = await self.request_confirmation(
            action="PAUSE TRADING",
            details="All new position opening will be halted. Existing positions remain open.",
        )
        if confirmed and self._system:
            await self._system.pause_trading()
            await self._send_now("⏸️ *Trading paused\\.*")
        elif not confirmed:
            await self._send_now("_Pause cancelled\\._")

    async def _cmd_resume(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("🚫 _Admin only\\._", parse_mode=ParseMode.MARKDOWN_V2)
            return
        if self._system:
            await self._system.resume_trading()
            await self._send_now("▶️ *Trading resumed\\.*")

    async def _cmd_close(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("🚫 _Admin only\\._", parse_mode=ParseMode.MARKDOWN_V2)
            return
        args = ctx.args or []
        if not args:
            await update.message.reply_text("_Usage: /close <position\\_id>_", parse_mode=ParseMode.MARKDOWN_V2)
            return
        position_id = args[0]
        confirmed = await self.request_confirmation(
            action=f"CLOSE POSITION {position_id}",
            details="This will immediately sell the position at market price.",
        )
        if confirmed and self._system:
            result = await self._system.close_position(position_id)
            await self._send_now(format_trade_result(result))

    async def _cmd_close_all(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("🚫 _Admin only\\._", parse_mode=ParseMode.MARKDOWN_V2)
            return
        # Double confirmation for close_all
        confirmed1 = await self.request_confirmation("CLOSE ALL POSITIONS", "This will sell ALL open positions.")
        if not confirmed1:
            return
        confirmed2 = await self.request_confirmation("CONFIRM CLOSE ALL", "Are you absolutely sure?")
        if confirmed2 and self._system:
            results = await self._system.close_all_positions("manual_telegram")
            await self._send_now(f"🛑 Closed `{len(results)}` positions\\.")

    async def _cmd_set_mode(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("🚫 _Admin only\\._", parse_mode=ParseMode.MARKDOWN_V2)
            return
        args = ctx.args or []
        if not args or args[0] not in ("live", "paper"):
            await update.message.reply_text("_Usage: /set\\_mode <live|paper>_", parse_mode=ParseMode.MARKDOWN_V2)
            return
        mode = args[0]
        confirmed = await self.request_confirmation(
            f"SWITCH TO {mode.upper()} MODE",
            "live mode uses real money!" if mode == "live" else "paper mode uses simulated money.",
        )
        if confirmed and self._system:
            await self._system.set_mode(mode)
            await self._send_now(f"🔄 Mode switched to `{mode}`\\.")

    async def _cmd_set_risk(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_admin(update.effective_user.id):
            await update.message.reply_text("🚫 _Admin only\\._", parse_mode=ParseMode.MARKDOWN_V2)
            return
        args = ctx.args or []
        levels = {
            "conservative": {"max_single_position_pct": 0.04, "max_daily_loss_pct": 0.03},
            "moderate": {"max_single_position_pct": 0.08, "max_daily_loss_pct": 0.05},
            "aggressive": {"max_single_position_pct": 0.12, "max_daily_loss_pct": 0.08},
        }
        if not args or args[0] not in levels:
            await update.message.reply_text(
                "_Usage: /set\\_risk <conservative|moderate|aggressive>_",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return
        level = args[0]
        if self._system:
            for k, v in levels[level].items():
                setattr(self._system.settings.risk, k, v)
        await self._send_now(f"⚙️ Risk level set to `{level}`\\.")

    async def _cmd_backtest(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        args = ctx.args or []
        if len(args) < 2:
            await update.message.reply_text(
                "_Usage: /backtest <YYYY\\-MM\\-DD> <YYYY\\-MM\\-DD>_",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return
        start_str, end_str = args[0], args[1]
        await update.message.reply_text(
            f"⏳ Running backtest `{_escape(start_str)}` → `{_escape(end_str)}`\\.\\.\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        if self._system:
            try:
                from datetime import datetime
                start = datetime.fromisoformat(start_str)
                end = datetime.fromisoformat(end_str)
                result = await self._system.run_backtest(start, end)
                summary = result.to_markdown()[:3000]
                await self._send_now(f"```\n{summary}\n```")
            except Exception as exc:
                await self._send_now(f"❌ Backtest failed: _{_escape(str(exc))}_")

    async def _cmd_report(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if self._system:
            stats = await self._system.get_performance_stats()
            await self.send_daily_report(stats)

    # ── Callback query handler ────────────────────────────────────────────────

    async def _handle_callback(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        await query.answer()
        data = query.data or ""

        # Confirmation callbacks
        if data.startswith("confirm_yes:") or data.startswith("confirm_no:"):
            future = self._confirmations.pop(data, None)
            if future and not future.done():
                future.set_result(data.startswith("confirm_yes:"))
            await query.edit_message_reply_markup(reply_markup=None)
            return

        # Signal execution callbacks
        if data.startswith("exec:"):
            signal_id = data[5:]
            if self._system:
                await self._system.execute_pending_signal(signal_id)
            await query.edit_message_text(
                f"✅ Signal `{signal_id[:8]}` queued for execution\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
        elif data.startswith("skip:"):
            signal_id = data[5:]
            if self._system:
                await self._system.skip_signal(signal_id)
            await query.edit_message_text(
                f"⏭️ Signal `{signal_id[:8]}` skipped\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
        elif data.startswith("half:"):
            signal_id = data[5:]
            if self._system:
                await self._system.execute_pending_signal(signal_id, size_multiplier=0.5)
            await query.edit_message_text(
                f"📝 Signal `{signal_id[:8]}` queued at 50% size\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )

    # ── Internal send machinery ───────────────────────────────────────────────

    async def _enqueue(
        self,
        text: str,
        priority: Priority,
        parse_mode: str = ParseMode.MARKDOWN_V2,
        reply_markup: Any = None,
    ) -> None:
        if not self._bot:
            return
        msg = PendingMessage(priority=priority, text=text, parse_mode=parse_mode)
        msg.reply_markup = reply_markup   # type: ignore[attr-defined]

        if priority == Priority.CRITICAL:
            await self._send_now(text, parse_mode=parse_mode, reply_markup=reply_markup)
        else:
            self._limiter.enqueue(msg)

    async def _dispatch_loop(self) -> None:
        """Background task: drain the message queue respecting rate limits."""
        while True:
            try:
                # Batch flush for LOW priority
                if self._limiter.should_batch_flush():
                    batch = self._limiter.flush_batch()
                    if batch:
                        combined = "\n\n".join(m.text for m in batch)[:4000]
                        await self._send_now(combined)

                # Send next queued message if within rate limit
                if self._limiter.can_send():
                    msg = self._limiter.pop_next()
                    if msg:
                        rm = getattr(msg, "reply_markup", None)
                        await self._send_now(msg.text, msg.parse_mode, rm)
                        self._limiter.record_send()

                await asyncio.sleep(0.5)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._log.error("dispatch_loop_error", error=str(exc))
                await asyncio.sleep(2)

    async def _send_now(
        self,
        text: str,
        parse_mode: str = ParseMode.MARKDOWN_V2,
        reply_markup: Any = None,
    ) -> None:
        if not self._bot:
            return
        try:
            await self._bot.send_message(
                chat_id=self._chat_id,
                text=text[:4096],
                parse_mode=parse_mode,
                reply_markup=reply_markup,
                disable_web_page_preview=True,
            )
        except Exception as exc:
            self._log.error("telegram_send_failed", error=str(exc))
