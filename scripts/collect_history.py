"""Collect historical data for backtesting."""
import asyncio, argparse, sys; sys.path.insert(0,'.')

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', default='2024-01-01')
    parser.add_argument('--end', default='2024-12-31')
    args = parser.parse_args()
    from config.settings import get_settings
    from backtest.data_collector_historical import HistoricalDataCollector
    from datetime import datetime, timezone
    settings = get_settings()
    collector = HistoricalDataCollector(settings)
    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    await collector.collect_all(start, end)
    print("Historical data collection complete.")

asyncio.run(main())
