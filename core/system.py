"""
core/system.py

TradingSystem — the root object that assembles every module and manages lifecycle.
"""
from __future__ import annotations
import asyncio
import signal
from datetime import datetime, timezone, timedelta
from typing import Any
import structlog
from config.settings import Settings

logger = structlog.get_logger(__name__)


class TradingSystem:
    def __init__(self, settings: Settings, telegram_only: bool = False) -> None:
        self.settings = settings
        self.mode = settings.execution.mode
        self._telegram_only = telegram_only
        self._running = False
        self._log = logger.bind(component="system", mode=self.mode)

        # Module references — populated by initialize()
        self.redis: Any = None
        self.db: Any = None
        self.collector_manager: Any = None
        self.signal_pipeline: Any = None
        self.strategy_orchestrator: Any = None
        self.risk_engine: Any = None
        self.circuit_breaker: Any = None
        self.risk_state: Any = None
        self.order_manager: Any = None
        self.executor: Any = None
        self.notifier: Any = None
        self.scheduler: Any = None
        self._paused: bool = False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def initialize(self) -> None:
        self._log.info("system_initializing")

        # 1. Database connections
        from database.connection import init_db, init_redis, get_redis_pool
        from database.redis_manager import RedisManager
        from database.repository import Database
        from sqlalchemy.ext.asyncio import async_sessionmaker
        from database.connection import get_engine

        await init_db(
            self.settings.database.postgres_url,
            pool_size=self.settings.database.postgres_pool_size,
        )
        await init_redis(
            self.settings.database.redis_url,
            pool_size=self.settings.database.redis_pool_size,
        )
        from database.connection import _session_factory
        self.db = Database(_session_factory)
        self.redis = RedisManager(get_redis_pool())
        self._log.info("databases_connected")

        # 2. Risk state
        from risk.risk_state import RiskState
        self.risk_state = await RiskState.load(self.redis)

        # 3. Telegram notifier
        n = self.settings.notification
        from notifications.telegram_bot import TelegramNotifier
        self.notifier = TelegramNotifier(
            bot_token=n.telegram_bot_token.get_secret_value(),
            chat_id=n.telegram_chat_id,
            admin_ids=n.telegram_admin_ids,
            max_messages_per_minute=n.max_messages_per_minute,
            quiet_start=n.quiet_hours_start_utc,
            quiet_end=n.quiet_hours_end_utc,
            system_ref=self,
        )
        await self.notifier.start()

        if self._telegram_only:
            self._log.info("telegram_only_mode")
            return

        # 4. Executor
        self.executor = self._build_executor()

        # 5. Risk engine + circuit breaker
        from risk.risk_rules import RiskRuleEngine
        from risk.circuit_breaker import CircuitBreaker
        r = self.settings.risk
        self.risk_engine = RiskRuleEngine(
            max_single_position_pct=r.max_single_position_pct,
            max_total_exposure_pct=r.max_total_exposure_pct,
            max_concurrent_positions=r.max_concurrent_positions,
            max_daily_loss_pct=r.max_daily_loss_pct,
            max_daily_loss_hard_pct=r.max_daily_loss_hard_pct,
            max_daily_trades=r.max_daily_trades,
            min_liquidity_usd=r.min_market_liquidity_usd,
            max_spread_pct=r.max_spread_pct,
        )
        self.risk_engine.inject_redis(self.redis)
        self.circuit_breaker = CircuitBreaker(
            daily_loss_hard_pct=r.max_daily_loss_hard_pct,
        )
        self.circuit_breaker.inject_dependencies(self.redis, self.notifier, None)

        # 6. Order manager
        from execution.order_manager import OrderManager
        self.order_manager = OrderManager(
            mode=self.mode,
            executor=self.executor,
            db=self.db,
            notifier=self.notifier,
            risk_state=self.risk_state,
            redis=self.redis,
            fee_rate=self.settings.execution.fee_rate,
        )
        self.circuit_breaker._order_manager = self.order_manager

        # 7. Strategy orchestrator
        from strategy.orchestrator import StrategyOrchestrator
        self.strategy_orchestrator = StrategyOrchestrator(
            enabled_strategy_names=self.settings.strategy.enabled_strategies,
            risk_engine=self.risk_engine,
            order_manager=self.order_manager,
            settings=self.settings,
        )

        # 8. Signal pipeline
        from signal_processing.news_analyzer import NewsAnalyzer
        from signal_processing.sports_analyzer import SportsAnalyzer
        from signal_processing.confidence_scorer import ConfidenceScorer
        from signal_processing.dedup_and_conflict import DedupAndConflictFilter
        from signal_processing.pipeline import SignalPipeline
        s = self.settings.signal
        news_analyzer = NewsAnalyzer(
            openai_api_key=s.openai_api_key.get_secret_value(),
            openai_model=s.openai_model,
            use_llm=s.use_llm_for_direction,
            market_match_threshold=s.market_match_threshold,
        )
        self.signal_pipeline = SignalPipeline(
            redis_manager=self.redis,
            news_analyzer=news_analyzer,
            sports_analyzer=SportsAnalyzer(),
            scorer=ConfidenceScorer(redis_manager=self.redis),
            dedup_filter=DedupAndConflictFilter(dedup_window_s=s.dedup_window_s),
            notifier=self.notifier,
            buy_threshold=self.settings.strategy.buy_threshold,
        )

        # 9. Data collectors
        from data_ingestion.manager import CollectorManager
        self.collector_manager = CollectorManager(self.settings, self.redis)
        self.collector_manager.build()

        # 10. Scheduler
        from core.scheduler import Scheduler
        self.scheduler = Scheduler(self)

        self._log.info("system_initialized")

    async def run(self) -> None:
        self._running = True
        self._log.info("system_running")

        if self._telegram_only:
            await asyncio.Event().wait()
            return

        await asyncio.gather(
            self.collector_manager.start_all(),
            self.signal_pipeline.start(),
            self.strategy_orchestrator.start(),
        )
        self.scheduler.start()
        self._control_task = asyncio.create_task(
            self._consume_control_commands(), name="system:control_queue"
        )

        try:
            await asyncio.Event().wait()
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        if not self._running:
            return
        self._running = False
        self._log.info("system_shutting_down")

        ctrl = getattr(self, "_control_task", None)
        if ctrl and not ctrl.done():
            ctrl.cancel()
            try:
                await ctrl
            except (asyncio.CancelledError, Exception):
                pass
        if self.scheduler:
            self.scheduler.stop()
        if self.signal_pipeline:
            await self.signal_pipeline.stop()
        if self.strategy_orchestrator:
            await self.strategy_orchestrator.stop()
        if self.collector_manager:
            await self.collector_manager.stop_all()
        if self.notifier:
            await self.notifier.stop()

        from database.connection import close_db, close_redis
        await close_db()
        await close_redis()
        self._log.info("system_stopped")

    # ── Scheduled task implementations ────────────────────────────────────────

    async def check_positions(self) -> None:
        if self._paused or not self.order_manager:
            return
        from risk.stop_manager import StopManager
        portfolio = await self.order_manager.get_portfolio()
        snaps = {}
        if self.collector_manager and self.collector_manager.polymarket_collector:
            pc = self.collector_manager.polymarket_collector
            for mid in pc._watched_market_ids:
                cached = await self.redis.get_cached_market_data(mid)
                if cached:
                    snaps[mid] = type("S", (), {"prices": {
                        "Yes": cached.get("yes_price", 0.5),
                        "No": cached.get("no_price", 0.5),
                    }})()
        stop_mgr = StopManager()
        close_orders = await stop_mgr.check_all_positions(portfolio, snaps)
        for order in close_orders:
            from risk.models import RiskDecision
            await self.order_manager.submit_order(order, RiskDecision.approve(order))

    async def refresh_market_cache(self) -> None:
        """Forwards market snapshots from the collector into the orchestrator cache."""
        if not (self.collector_manager and self.strategy_orchestrator):
            return
        pc = getattr(self.collector_manager, "polymarket_collector", None)
        if not pc:
            return
        snapshots: dict[str, Any] = {}
        for mid in getattr(pc, "_watched_market_ids", []) or []:
            cached = await self.redis.get_cached_market_data(mid)
            if cached:
                snapshots[mid] = cached
        if snapshots:
            self.strategy_orchestrator.update_market_cache(snapshots)

    async def update_whale_stats(self) -> None:
        """Recompute whale win rates from completed trades and write them to Redis."""
        if not self.db:
            return
        try:
            actions = await self.db.get_recent_whale_actions(500)
        except Exception as exc:
            self._log.warning("update_whale_stats_query_failed", error=str(exc))
            return
        stats: dict[str, dict[str, float]] = {}
        for a in actions or []:
            addr = getattr(a, "whale_address", None) or (a.get("whale_address") if isinstance(a, dict) else None)
            if not addr:
                continue
            pnl = getattr(a, "realized_pnl", None)
            if pnl is None and isinstance(a, dict):
                pnl = a.get("realized_pnl", 0.0)
            pnl = float(pnl or 0.0)
            entry = stats.setdefault(addr, {"total": 0, "wins": 0, "pnl": 0.0})
            entry["total"] += 1
            entry["pnl"] += pnl
            if pnl > 0:
                entry["wins"] += 1
        for addr, s in stats.items():
            total = s["total"] or 1
            win_rate = s["wins"] / total
            await self.redis.cache_set(
                f"whale:stats:{addr}",
                {"win_rate": round(win_rate, 4), "total_trades": s["total"], "total_pnl": round(s["pnl"], 2)},
                ttl=86400,
            )
        self._log.info("whale_stats_updated", whale_count=len(stats))

    async def run_circuit_breaker_check(self) -> None:
        if not self.order_manager:
            return
        portfolio = await self.order_manager.get_portfolio()
        await self.circuit_breaker.check(portfolio, self.risk_state)

    async def daily_reset(self) -> None:
        await self.risk_state.reset_daily(self.redis)
        stats = await self.db.get_performance_stats(
            datetime.now(timezone.utc).replace(hour=0, minute=0, second=0),
            datetime.now(timezone.utc),
            self.mode,
        )
        if self.notifier:
            await self.notifier.send_daily_report(stats)

    async def send_weekly_report(self) -> None:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=7)
        trades = await self.db.get_performance_stats(start, end, self.mode)
        if self.notifier:
            await self.notifier.send_daily_report(trades)

    async def scan_arbitrage(self) -> None:
        if self.strategy_orchestrator:
            await self.strategy_orchestrator.scan_arbitrage([])

    async def snapshot_positions(self) -> None:
        """Write portfolio state to Redis so the API container can read it."""
        if not self.order_manager or not self.redis:
            return
        try:
            import json
            portfolio = await self.order_manager.get_portfolio()
            state = {
                "total_balance": portfolio.total_balance,
                "available_balance": portfolio.available_balance,
                "total_position_value": portfolio.total_position_value,
                "daily_pnl": portfolio.daily_pnl,
                "daily_pnl_pct": portfolio.daily_pnl_pct,
                "mode": portfolio.mode,
            }
            await self.redis.set("state:portfolio", json.dumps(state))
        except Exception as exc:
            self._log.debug("snapshot_positions_error", error=str(exc))

    # ── Public API (used by Telegram bot and web dashboard) ──────────────────

    async def get_portfolio(self) -> Any:
        if self.order_manager:
            return await self.order_manager.get_portfolio()
        from strategy.models import Portfolio
        return Portfolio(0, 0, 0, [], 0, 0, 0, 0, 0, self.mode)

    async def get_performance_stats(self) -> dict:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=30)
        return await self.db.get_performance_stats(start, end, self.mode)

    async def get_today_trades(self) -> list:
        return await self.db.get_today_trades(self.mode)

    async def get_recent_whale_actions(self, limit: int = 10) -> list:
        return await self.db.get_recent_whale_actions(limit)

    async def get_circuit_breaker_status(self) -> Any:
        from risk.models import CircuitBreakerStatus
        if self.risk_state and self.risk_state.circuit_breaker_active:
            return CircuitBreakerStatus(
                active=True, level=self.risk_state.circuit_breaker_level,
                reason=self.risk_state.circuit_breaker_reason,
                triggered_at=self.risk_state.circuit_breaker_triggered_at,
                resume_at=self.risk_state.circuit_breaker_resume_at,
                affected_markets=[],
            )
        return CircuitBreakerStatus(False, "none", "", None, None, [])

    def get_collector_health(self) -> dict:
        if self.collector_manager:
            return self.collector_manager.get_health_report()
        return {}

    async def pause_trading(self) -> None:
        self._paused = True
        self._log.warning("trading_paused")

    async def resume_trading(self) -> None:
        self._paused = False
        if self.risk_state and self.risk_state.circuit_breaker_active:
            await self.circuit_breaker.manual_clear(self.risk_state, "telegram")
        self._log.info("trading_resumed")

    async def close_position(self, position_id: str) -> Any:
        if self.order_manager:
            return await self.order_manager.close_position(position_id)

    async def close_all_positions(self, reason: str) -> list:
        if self.order_manager:
            return await self.order_manager.close_all_positions(reason)
        return []

    async def set_mode(self, mode: str) -> None:
        """
        Hot-swap paper/live/backtest execution mode.

        Rebuilds the executor so orders routed after this call use the
        new venue. If the mode is unchanged, this is a no-op.
        """
        if mode not in ("live", "paper", "backtest"):
            raise ValueError(f"invalid mode: {mode}")
        if mode == self.mode:
            return

        self._log.info("execution_mode_switch", previous=self.mode, target=mode)
        self.mode = mode
        self.settings.execution.mode = mode

        # Rebuild executor in place. OrderManager keeps its existing
        # portfolio state; only the downstream venue changes.
        try:
            new_exec = self._build_executor()
            self.executor = new_exec
            if self.order_manager is not None:
                # OrderManager references the executor; let it know.
                if hasattr(self.order_manager, "set_executor"):
                    await self.order_manager.set_executor(new_exec)  # type: ignore[func-returns-value]
                else:
                    self.order_manager.executor = new_exec  # best-effort fallback
            self._log.info("execution_mode_switched", mode=mode)
        except Exception as exc:
            self._log.error("execution_mode_switch_failed", error=str(exc), exc_info=True)
            raise

    async def run_backtest(self, start: datetime, end: datetime) -> Any:
        from backtest.engine import BacktestEngine
        from backtest.data_loader import BacktestDataLoader
        loader = BacktestDataLoader(data_dir=self.settings.backtest.data_dir, db=self.db)
        engine = BacktestEngine(settings=self.settings, data_loader=loader)
        return await engine.run(start, end)

    async def execute_pending_signal(self, signal_id: str, size_multiplier: float = 1.0) -> Any:
        """Look up a cached ScoredSignal summary and force-execute it through risk + orders."""
        if not (self.redis and self.order_manager and self.risk_engine):
            self._log.warning("execute_pending_signal_not_ready", signal_id=signal_id)
            return None

        cached = await self.redis.cache_get(f"cache:signal:{signal_id}")
        if not cached:
            self._log.warning("signal_not_found_in_cache", signal_id=signal_id)
            if self.notifier:
                await self.notifier.send_alert(f"Signal {signal_id} not found (expired or invalid)")
            return None

        market_id = cached.get("market_id", "")
        outcome = cached.get("outcome", "Yes")

        # Pull current market price from Redis cache; fall back to 0.5
        price = 0.5
        market_data = await self.redis.get_cached_market_data(market_id)
        if market_data:
            price = float(market_data.get("yes_price" if outcome == "Yes" else "no_price", 0.5))

        portfolio = await self.order_manager.get_portfolio()
        position_pct = float(cached.get("recommended_position_pct", 0.02)) * max(0.1, size_multiplier)
        size_usd = round(portfolio.total_balance * position_pct, 2)
        if size_usd <= 0:
            self._log.warning("execute_pending_signal_zero_size", signal_id=signal_id)
            return None

        from strategy.models import OrderRequest
        order = OrderRequest.create(
            signal_id=signal_id,
            strategy_name="manual_force_execute",
            market_id=market_id,
            market_question=cached.get("market_question", ""),
            side="buy",
            outcome=outcome,
            price=price,
            size_usd=size_usd,
            position_pct=position_pct,
            confidence_score=int(cached.get("confidence_score", 0)),
            take_profit=round(price * 1.15, 4),
            stop_loss=round(price * 0.85, 4),
            reasoning=f"Manual force-execute of signal {signal_id}: {cached.get('reasoning', '')}",
        )

        risk_state = await self.risk_engine.get_state()
        risk_decision = await self.risk_engine.evaluate(order, portfolio, risk_state)
        if not risk_decision.approved and risk_decision.adjusted_order is None:
            self._log.warning(
                "force_execute_blocked_by_risk",
                signal_id=signal_id,
                violations=risk_decision.violations,
            )
            if self.notifier:
                await self.notifier.send_alert(
                    f"⛔ Signal {signal_id} blocked by risk: {risk_decision.violations}"
                )
            return None

        final_order = risk_decision.adjusted_order or order
        await self.order_manager.submit_order(final_order, risk_decision)
        await self.redis.cache_delete(f"cache:signal:{signal_id}")
        self._log.info("signal_force_executed", signal_id=signal_id, size_usd=final_order.size_usd)
        return final_order

    async def _consume_control_commands(self) -> None:
        """Poll Redis `control:commands` list for commands issued from the web dashboard."""
        import json
        self._log.info("control_queue_consumer_started")
        while self._running:
            try:
                raw = None
                try:
                    client = self.redis._client() if self.redis else None
                    if client is not None:
                        async with client as c:
                            popped = await c.blpop("control:commands", timeout=5)
                        if popped:
                            _, raw = popped
                            if isinstance(raw, bytes):
                                raw = raw.decode()
                except Exception as exc:
                    self._log.debug("control_queue_poll_error", error=str(exc))
                    await asyncio.sleep(5)
                    continue

                if not raw:
                    continue
                try:
                    cmd = json.loads(raw)
                except Exception:
                    self._log.warning("control_cmd_malformed", raw=str(raw)[:200])
                    continue

                action = cmd.get("action", "")
                self._log.info("control_cmd_received", action=action)
                try:
                    if action == "close_position":
                        await self.close_position(cmd.get("position_id", ""))
                    elif action == "close_all":
                        await self.close_all_positions(cmd.get("reason", "web_dashboard"))
                    elif action == "execute_signal":
                        await self.execute_pending_signal(
                            cmd.get("signal_id", ""),
                            float(cmd.get("size_multiplier", 1.0) or 1.0),
                        )
                    elif action == "skip_signal":
                        await self.skip_signal(cmd.get("signal_id", ""))
                    else:
                        self._log.warning("control_cmd_unknown", action=action)
                except Exception as exc:
                    self._log.error("control_cmd_handler_error", action=action, error=str(exc), exc_info=True)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._log.error("control_queue_loop_error", error=str(exc), exc_info=True)
                await asyncio.sleep(5)
        self._log.info("control_queue_consumer_stopped")

    async def skip_signal(self, signal_id: str) -> bool:
        """Mark a cached pending signal as skipped (delete from cache)."""
        if not self.redis:
            return False
        cached = await self.redis.cache_get(f"cache:signal:{signal_id}")
        if not cached:
            self._log.info("skip_signal_not_found", signal_id=signal_id)
            return False
        await self.redis.cache_delete(f"cache:signal:{signal_id}")
        self._log.info("signal_skipped", signal_id=signal_id)
        return True

    # ── Builder helpers ───────────────────────────────────────────────────────

    def _build_executor(self) -> Any:
        mode = self.mode
        if mode == "live":
            from execution.live_executor import LiveExecutor
            p = self.settings.polymarket
            return LiveExecutor(
                private_key=p.private_key.get_secret_value(),
                api_key=p.api_key.get_secret_value(),
                api_secret=p.api_secret.get_secret_value(),
                api_passphrase=p.api_passphrase.get_secret_value(),
                chain_id=p.chain_id,
                clob_api_url=p.clob_api_url,
                fee_rate=self.settings.execution.fee_rate,
                max_slippage_pct=self.settings.execution.max_slippage_pct,
            )
        elif mode == "backtest":
            from execution.backtest_executor import BacktestExecutor
            return BacktestExecutor(
                initial_balance=self.settings.backtest.initial_balance,
                fee_rate=self.settings.backtest.fee_rate,
            )
        else:
            from execution.paper_executor import PaperExecutor
            return PaperExecutor(
                initial_balance=self.settings.execution.paper_initial_balance,
                fee_rate=self.settings.execution.fee_rate,
                db=self.db,
            )
