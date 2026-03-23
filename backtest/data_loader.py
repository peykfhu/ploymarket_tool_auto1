"""
backtest/data_loader.py

Historical data loader for backtesting.
Loads market prices, news, sports, and whale data from
Parquet files and/or PostgreSQL, returns unified time-sorted structures.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

import pandas as pd
import structlog

logger = structlog.get_logger(__name__)


class BacktestDataLoader:
    """
    Loads and manages historical data for the backtest engine.

    Priority:
      1. Parquet files in data_dir (fast, preferred for large datasets)
      2. PostgreSQL via db adapter (fallback)

    All DataFrames have a UTC-aware 'timestamp' column as the index.
    """

    def __init__(
        self,
        data_dir: str = "data/historical",
        db: Any = None,
    ) -> None:
        self._data_dir = Path(data_dir)
        self._db = db
        self._log = logger.bind(component="data_loader")

    # ── Market price history ──────────────────────────────────────────────────

    async def load_market_history(
        self,
        market_id: str,
        start_date: datetime,
        end_date: datetime,
        resolution: str = "1m",
    ) -> pd.DataFrame:
        """
        Returns a DataFrame with columns:
          timestamp, yes_price, no_price, liquidity, spread,
          volume_24h, bids (JSON str), asks (JSON str)
        Indexed by timestamp (UTC).
        """
        parquet_path = self._data_dir / "markets" / f"{market_id}.parquet"

        df = await self._load_parquet_or_db(
            parquet_path,
            fallback=lambda: self._load_market_from_db(market_id, start_date, end_date),
        )

        if df is None or df.empty:
            self._log.warning("no_market_data", market_id=market_id)
            return pd.DataFrame()

        df = self._filter_time(df, start_date, end_date)
        df = self._resample_ohlc(df, resolution)
        return df

    async def load_all_markets(
        self,
        start_date: datetime,
        end_date: datetime,
        categories: list[str] | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Load price history for all available markets."""
        market_dir = self._data_dir / "markets"
        if not market_dir.exists():
            self._log.warning("market_dir_missing", path=str(market_dir))
            return {}

        results: dict[str, pd.DataFrame] = {}
        files = list(market_dir.glob("*.parquet"))
        self._log.info("loading_markets", count=len(files))

        for f in files:
            market_id = f.stem
            loop = asyncio.get_event_loop()
            try:
                df = await loop.run_in_executor(None, pd.read_parquet, f)
                df = self._ensure_utc_index(df)
                df = self._filter_time(df, start_date, end_date)
                if not df.empty:
                    results[market_id] = df
            except Exception as exc:
                self._log.warning("market_load_error", file=str(f), error=str(exc))

        self._log.info("markets_loaded", count=len(results))
        return results

    # ── News history ──────────────────────────────────────────────────────────

    async def load_news_history(
        self,
        start_date: datetime,
        end_date: datetime,
        categories: list[str] | None = None,
    ) -> list[Any]:
        """
        Returns a list of NewsEvent objects sorted by timestamp.
        Loaded from Parquet (data/historical/news/YYYY-MM.parquet).
        """
        from data_ingestion.news_collector import NewsEvent
        import json

        news_dir = self._data_dir / "news"
        if not news_dir.exists():
            self._log.warning("news_dir_missing")
            return []

        frames: list[pd.DataFrame] = []
        loop = asyncio.get_event_loop()

        for f in sorted(news_dir.glob("*.parquet")):
            try:
                df = await loop.run_in_executor(None, pd.read_parquet, f)
                frames.append(df)
            except Exception as exc:
                self._log.warning("news_load_error", file=str(f), error=str(exc))

        if not frames:
            return []

        df = pd.concat(frames, ignore_index=True)
        df = self._ensure_utc_index(df)
        df = self._filter_time(df, start_date, end_date)

        if categories:
            df = df[df["category"].isin(categories)]

        events: list[Any] = []
        for _, row in df.iterrows():
            try:
                ts = row.name if hasattr(row.name, "tzinfo") else pd.Timestamp(row.name).to_pydatetime()
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                event = NewsEvent(
                    id=str(row.get("id", "")),
                    source=str(row.get("source", "")),
                    timestamp=ts,
                    received_at=ts,
                    latency_ms=int(row.get("latency_ms", 0)),
                    headline=str(row.get("headline", "")),
                    body=str(row.get("body", "")),
                    url=str(row.get("url", "")),
                    category=str(row.get("category", "other")),
                    entities=json.loads(str(row.get("entities", "[]"))),
                    raw_data={},
                )
                events.append(event)
            except Exception as exc:
                self._log.debug("news_row_error", error=str(exc))

        self._log.info("news_loaded", count=len(events))
        return events

    # ── Sports history ────────────────────────────────────────────────────────

    async def load_sports_history(
        self,
        start_date: datetime,
        end_date: datetime,
        sports: list[str] | None = None,
    ) -> list[Any]:
        """
        Returns list of SportEvent objects (minute-by-minute score changes).
        Loaded from data/historical/sports/SPORT_YYYY-MM.parquet.
        """
        from data_ingestion.sports_collector import SportEvent
        import json

        sports_dir = self._data_dir / "sports"
        if not sports_dir.exists():
            self._log.warning("sports_dir_missing")
            return []

        frames: list[pd.DataFrame] = []
        loop = asyncio.get_event_loop()
        pattern = "*.parquet"

        for f in sorted(sports_dir.glob(pattern)):
            if sports and not any(s.lower() in f.stem.lower() for s in sports):
                continue
            try:
                df = await loop.run_in_executor(None, pd.read_parquet, f)
                frames.append(df)
            except Exception as exc:
                self._log.warning("sports_load_error", file=str(f), error=str(exc))

        if not frames:
            return []

        df = pd.concat(frames, ignore_index=True)
        df = self._ensure_utc_index(df)
        df = self._filter_time(df, start_date, end_date)

        events: list[Any] = []
        for _, row in df.iterrows():
            try:
                ts = row.name
                if hasattr(ts, "to_pydatetime"):
                    ts = ts.to_pydatetime()
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                event = SportEvent(
                    id=str(row.get("id", "")),
                    sport=str(row.get("sport", "")),
                    league=str(row.get("league", "")),
                    home_team=str(row.get("home_team", "")),
                    away_team=str(row.get("away_team", "")),
                    home_score=int(row.get("home_score", 0)),
                    away_score=int(row.get("away_score", 0)),
                    match_status=str(row.get("match_status", "in_progress")),
                    elapsed_minutes=int(row.get("elapsed_minutes", 0)),
                    total_minutes=int(row.get("total_minutes", 90)),
                    remaining_minutes=int(row.get("remaining_minutes", 0)),
                    events=json.loads(str(row.get("events", "[]"))),
                    stats=json.loads(str(row.get("stats", "{}"))),
                    odds=json.loads(str(row.get("odds", "{}"))),
                    momentum=float(row.get("momentum", 0.0)),
                    timestamp=ts,
                    external_id=str(row.get("external_id", "")),
                )
                events.append(event)
            except Exception as exc:
                self._log.debug("sports_row_error", error=str(exc))

        self._log.info("sports_loaded", count=len(events))
        return events

    # ── Whale history ─────────────────────────────────────────────────────────

    async def load_whale_history(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> list[Any]:
        """Returns list of WhaleAction objects."""
        from data_ingestion.whale_collector import WhaleAction

        whale_path = self._data_dir / "whales" / "whale_trades.parquet"
        loop = asyncio.get_event_loop()

        try:
            df = await loop.run_in_executor(None, pd.read_parquet, whale_path)
        except FileNotFoundError:
            self._log.warning("whale_file_missing", path=str(whale_path))
            return []
        except Exception as exc:
            self._log.error("whale_load_error", error=str(exc))
            return []

        df = self._ensure_utc_index(df)
        df = self._filter_time(df, start_date, end_date)

        actions: list[Any] = []
        for _, row in df.iterrows():
            try:
                ts = row.name
                if hasattr(ts, "to_pydatetime"):
                    ts = ts.to_pydatetime()
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                action = WhaleAction(
                    whale_address=str(row.get("whale_address", "")),
                    whale_tier=str(row.get("whale_tier", "B")),
                    whale_name=str(row.get("whale_name", "")),
                    whale_win_rate=float(row.get("whale_win_rate", 0.5)),
                    whale_total_pnl=float(row.get("whale_total_pnl", 0.0)),
                    action=str(row.get("action", "buy")),
                    market_id=str(row.get("market_id", "")),
                    market_question=str(row.get("market_question", "")),
                    outcome=str(row.get("outcome", "Yes")),
                    amount_usd=float(row.get("amount_usd", 0.0)),
                    price=float(row.get("price", 0.5)),
                    timestamp=ts,
                    tx_hash=str(row.get("tx_hash", "")),
                    block_number=int(row.get("block_number", 0)),
                )
                actions.append(action)
            except Exception as exc:
                self._log.debug("whale_row_error", error=str(exc))

        self._log.info("whales_loaded", count=len(actions))
        return actions

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _load_parquet_or_db(
        self, path: Path, fallback: Callable
    ) -> pd.DataFrame | None:
        if path.exists():
            loop = asyncio.get_event_loop()
            try:
                return await loop.run_in_executor(None, pd.read_parquet, path)
            except Exception as exc:
                self._log.warning("parquet_read_error", path=str(path), error=str(exc))
        if self._db:
            try:
                return await fallback()
            except Exception as exc:
                self._log.warning("db_fallback_error", error=str(exc))
        return None

    async def _load_market_from_db(
        self, market_id: str, start: datetime, end: datetime
    ) -> pd.DataFrame:
        if not self._db:
            return pd.DataFrame()
        rows = await self._db.get_price_history(market_id, start, end)
        return pd.DataFrame(rows)

    def _ensure_utc_index(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure DataFrame has a UTC-aware DatetimeIndex."""
        if "timestamp" in df.columns:
            df = df.set_index("timestamp")
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index, utc=True)
        elif df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")
        return df.sort_index()

    def _filter_time(
        self, df: pd.DataFrame, start: datetime, end: datetime
    ) -> pd.DataFrame:
        if df.empty:
            return df
        start_utc = start.astimezone(timezone.utc) if start.tzinfo else start.replace(tzinfo=timezone.utc)
        end_utc = end.astimezone(timezone.utc) if end.tzinfo else end.replace(tzinfo=timezone.utc)
        mask = (df.index >= start_utc) & (df.index <= end_utc)
        return df[mask]

    def _resample_ohlc(self, df: pd.DataFrame, resolution: str) -> pd.DataFrame:
        """Resample price data to requested resolution."""
        if "yes_price" not in df.columns or resolution == "tick":
            return df
        freq_map = {"1m": "1T", "5m": "5T", "15m": "15T", "1h": "1H", "1d": "1D"}
        freq = freq_map.get(resolution, "1T")
        try:
            resampled = df["yes_price"].resample(freq).ohlc()
            resampled["yes_price"] = resampled["close"]
            if "liquidity" in df.columns:
                resampled["liquidity"] = df["liquidity"].resample(freq).mean()
            if "spread" in df.columns:
                resampled["spread"] = df["spread"].resample(freq).mean()
            return resampled.dropna(subset=["yes_price"])
        except Exception:
            return df
