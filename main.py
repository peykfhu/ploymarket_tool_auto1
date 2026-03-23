"""
main.py - Polymarket Trading System Entry Point

Usage:
  python main.py --mode live
  python main.py --mode paper
  python main.py --mode backtest --start 2024-01-01 --end 2024-06-01
  python main.py --mode collect-data
  python main.py --mode telegram-only
"""

import argparse
import asyncio
import logging
import signal
import sys
from datetime import datetime
from pathlib import Path

import structlog

# Bootstrap logging before any imports that might log
def _configure_logging(level: str = "INFO", fmt: str = "console") -> None:
    processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]
    if fmt == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


logger = structlog.get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Polymarket Automated Trading System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        choices=["live", "paper", "backtest", "collect-data", "telegram-only"],
        required=True,
        help="Operating mode",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Backtest start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="Backtest end date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config/settings.yaml",
        help="Path to settings YAML (default: config/settings.yaml)",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Override log level from config",
    )
    return parser.parse_args()


async def run(args: argparse.Namespace) -> None:
    # Lazy import to allow logging to init first
    from config.settings import Settings
    from config.settings import get_settings

    settings = get_settings()

    level = args.log_level or settings.logging.level
    _configure_logging(level=level, fmt=settings.logging.format)

    # Override execution mode from CLI
    settings.execution.mode = args.mode if args.mode in ("live", "paper", "backtest") else settings.execution.mode

    logger.info("system_starting", mode=args.mode, version="0.1.0")

    if args.mode == "backtest":
        if not args.start or not args.end:
            logger.error("backtest_requires_dates", hint="Pass --start YYYY-MM-DD --end YYYY-MM-DD")
            sys.exit(1)

        start_dt = datetime.fromisoformat(args.start)
        end_dt = datetime.fromisoformat(args.end)

        # Imported lazily so individual phases can be developed independently
        from backtest.engine import BacktestEngine
        engine = BacktestEngine(settings=settings)
        result = await engine.run(start_date=start_dt, end_date=end_dt)
        print(result.to_markdown())
        return

    if args.mode == "collect-data":
        from backtest.data_collector_historical import HistoricalDataCollector
        collector = HistoricalDataCollector(settings=settings)
        await collector.collect_all()
        return

    # live / paper / telegram-only — full system startup
    from core.system import TradingSystem
    system = TradingSystem(settings=settings, telegram_only=(args.mode == "telegram-only"))

    # Graceful shutdown on SIGINT / SIGTERM
    loop = asyncio.get_running_loop()

    def _handle_signal(sig: signal.Signals) -> None:
        logger.info("shutdown_signal_received", signal=sig.name)
        asyncio.ensure_future(system.shutdown())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal, sig)

    await system.initialize()
    await system.run()


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        # Fallback if structlog isn't up yet
        print(f"[FATAL] {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
