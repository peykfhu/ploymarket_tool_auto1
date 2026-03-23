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

        try:
            await asyncio.Event().wait()
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        if not self._running:
            return
        self._running = False
        self._log.info("system_shutting_down")

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
        pass  # Handled by PolymarketCollector internally

    async def update_whale_stats(self) -> None:
        pass  # Future: query DB for whale trade outcomes and update win rates

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
        self.mode = mode
        self.settings.execution.mode = mode

    async def run_backtest(self, start: datetime, end: datetime) -> Any:
        from backtest.engine import BacktestEngine
        from backtest.data_loader import BacktestDataLoader
        loader = BacktestDataLoader(data_dir=self.settings.backtest.data_dir, db=self.db)
        engine = BacktestEngine(settings=self.settings, data_loader=loader)
        return await engine.run(start, end)

    async def execute_pending_signal(self, signal_id: str, size_multiplier: float = 1.0) -> None:
        pass  # Future: look up cached signal and force-execute

    async def skip_signal(self, signal_id: str) -> None:
        pass

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
