"""Quick backtest runner."""
import asyncio, argparse, sys; sys.path.insert(0,'.')

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    args = parser.parse_args()
    from config.settings import get_settings
    from backtest.engine import BacktestEngine
    from datetime import datetime, timezone
    settings = get_settings()
    engine = BacktestEngine(settings=settings)
    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)

    def progress(pct): print(f"\r  Progress: {pct:.0%}", end="", flush=True)
    result = await engine.run(start, end, progress_callback=progress)
    print()
    print(result.to_markdown())

asyncio.run(main())
