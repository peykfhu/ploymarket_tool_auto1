"""
backtest/data_collector_historical.py

One-time historical data collection utility.
Run this before backtesting to populate data/historical/.

Usage:
  python scripts/collect_history.py --start 2024-01-01 --end 2024-12-31
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import aiohttp
import pandas as pd
import structlog

logger = structlog.get_logger(__name__)


class HistoricalDataCollector:
    """
    Fetches and stores historical data for all four data types:
      1. Polymarket market prices    → data/historical/markets/{market_id}.parquet
      2. News events                 → data/historical/news/YYYY-MM.parquet
      3. Sports events               → data/historical/sports/{sport}_YYYY-MM.parquet
      4. Whale trades                → data/historical/whales/whale_trades.parquet
    """

    GAMMA_BASE = "https://gamma-api.polymarket.com"
    CLOB_BASE = "https://clob.polymarket.com"

    def __init__(
        self,
        settings: Any,
        data_dir: str = "data/historical",
        db: Any = None,
    ) -> None:
        self._settings = settings
        self._data_dir = Path(data_dir)
        self._db = db
        self._log = logger.bind(component="historical_collector")
        self._data_dir.mkdir(parents=True, exist_ok=True)

    async def collect_all(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> None:
        """Collect all data types concurrently."""
        start = start_date or (datetime.now(timezone.utc) - timedelta(days=180))
        end = end_date or datetime.now(timezone.utc)

        self._log.info("collecting_all_historical", start=start.date(), end=end.date())

        await asyncio.gather(
            self.collect_polymarket_history(start, end),
            self.collect_news_history(start, end),
            self.collect_sports_history(start, end),
            return_exceptions=True,
        )
        self._log.info("historical_collection_complete")

    # ── Polymarket ────────────────────────────────────────────────────────────

    async def collect_polymarket_history(
        self,
        start_date: datetime,
        end_date: datetime,
        categories: list[str] | None = None,
    ) -> None:
        """
        Fetch all resolved markets from Gamma API and their price histories
        from the CLOB timeseries endpoint.
        """
        market_dir = self._data_dir / "markets"
        market_dir.mkdir(exist_ok=True)

        async with aiohttp.ClientSession() as session:
            markets = await self._fetch_all_markets(session, start_date, end_date, categories)
            self._log.info("markets_to_collect", count=len(markets))

            sem = asyncio.Semaphore(10)   # max 10 concurrent requests
            tasks = [
                self._collect_market_prices(session, sem, m, start_date, end_date, market_dir)
                for m in markets
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            success = sum(1 for r in results if not isinstance(r, Exception))
            self._log.info("markets_collected", success=success, total=len(markets))

    async def _fetch_all_markets(
        self,
        session: aiohttp.ClientSession,
        start: datetime,
        end: datetime,
        categories: list[str] | None,
    ) -> list[dict]:
        markets: list[dict] = []
        cursor = ""
        while True:
            params: dict = {"limit": 100, "closed": "true"}
            if cursor:
                params["cursor"] = cursor
            if categories:
                params["category"] = ",".join(categories)

            try:
                async with session.get(
                    f"{self.GAMMA_BASE}/markets",
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        break
                    data = await resp.json()

                batch = data if isinstance(data, list) else data.get("data", [])
                markets.extend(batch)
                cursor = data.get("next_cursor", "") if isinstance(data, dict) else ""
                if not cursor or not batch:
                    break

            except Exception as exc:
                self._log.warning("market_list_fetch_error", error=str(exc))
                break

        return markets

    async def _collect_market_prices(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        market: dict,
        start: datetime,
        end: datetime,
        out_dir: Path,
    ) -> None:
        market_id = market.get("id") or market.get("conditionId", "")
        if not market_id:
            return

        out_path = out_dir / f"{market_id}.parquet"
        if out_path.exists():
            return   # already collected

        async with sem:
            try:
                # CLOB timeseries endpoint
                async with session.get(
                    f"{self.CLOB_BASE}/prices-history",
                    params={
                        "market": market_id,
                        "startTs": int(start.timestamp()),
                        "endTs": int(end.timestamp()),
                        "interval": "1m",
                    },
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    if resp.status != 200:
                        return
                    data = await resp.json()

                history = data.get("history", [])
                if not history:
                    return

                rows = []
                for point in history:
                    ts = datetime.fromtimestamp(float(point.get("t", 0)), tz=timezone.utc)
                    price = float(point.get("p", 0.5))
                    rows.append({
                        "timestamp": ts,
                        "yes_price": price,
                        "no_price": round(1.0 - price, 4),
                        "liquidity": float(market.get("liquidityNum", 0)),
                        "spread": 0.02,  # CLOB doesn't provide spread in history
                        "question": market.get("question", ""),
                        "category": market.get("category", "other"),
                    })

                if rows:
                    df = pd.DataFrame(rows).set_index("timestamp")
                    df.to_parquet(out_path)
                    self._log.debug("market_saved", market_id=market_id[:20], rows=len(rows))

            except Exception as exc:
                self._log.debug("market_price_collect_error", market_id=market_id[:20], error=str(exc))

    # ── News ──────────────────────────────────────────────────────────────────

    async def collect_news_history(
        self, start_date: datetime, end_date: datetime
    ) -> None:
        """
        Fetch historical news from NewsAPI for each month in range.
        Stored as monthly Parquet files.
        """
        news_dir = self._data_dir / "news"
        news_dir.mkdir(exist_ok=True)

        api_key = self._settings.news.newsapi_key.get_secret_value()
        if not api_key:
            self._log.warning("newsapi_key_missing_skipping_news")
            return

        current = start_date.replace(day=1)
        async with aiohttp.ClientSession() as session:
            while current <= end_date:
                month_end = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
                out_path = news_dir / f"{current.strftime('%Y-%m')}.parquet"

                if not out_path.exists():
                    await self._collect_news_month(
                        session, api_key, current, min(month_end, end_date), out_path
                    )

                current = month_end

    async def _collect_news_month(
        self,
        session: aiohttp.ClientSession,
        api_key: str,
        start: datetime,
        end: datetime,
        out_path: Path,
    ) -> None:
        rows = []
        for category in ["politics", "business", "technology", "sports", "general"]:
            try:
                async with session.get(
                    "https://newsapi.org/v2/everything",
                    params={
                        "apiKey": api_key,
                        "from": start.strftime("%Y-%m-%d"),
                        "to": end.strftime("%Y-%m-%d"),
                        "language": "en",
                        "sortBy": "publishedAt",
                        "pageSize": 100,
                        "q": category,
                    },
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()

                for article in data.get("articles", []):
                    rows.append({
                        "timestamp": article.get("publishedAt", ""),
                        "id": "",
                        "source": article.get("source", {}).get("name", ""),
                        "headline": article.get("title", ""),
                        "body": article.get("description", ""),
                        "url": article.get("url", ""),
                        "category": category,
                        "entities": "[]",
                        "latency_ms": 0,
                    })
                await asyncio.sleep(0.5)   # Rate limit

            except Exception as exc:
                self._log.warning("news_month_error", category=category, error=str(exc))

        if rows:
            df = pd.DataFrame(rows)
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            df = df.set_index("timestamp").sort_index()
            df.to_parquet(out_path)
            self._log.info("news_month_saved", month=out_path.stem, rows=len(rows))

    # ── Sports ────────────────────────────────────────────────────────────────

    async def collect_sports_history(
        self, start_date: datetime, end_date: datetime
    ) -> None:
        """
        Fetch historical football results from API-Football.
        Stored as monthly Parquet files.
        """
        sports_dir = self._data_dir / "sports"
        sports_dir.mkdir(exist_ok=True)

        api_key = self._settings.sports.api_football_key.get_secret_value()
        if not api_key:
            self._log.warning("api_football_key_missing_skipping_sports")
            return

        headers = {
            "X-RapidAPI-Key": api_key,
            "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com",
        }
        BASE = "https://api-football-v1.p.rapidapi.com/v3"

        # Collect season data for major leagues
        league_ids = {"Premier League": 39, "La Liga": 140, "Champions League": 2}
        season = start_date.year

        async with aiohttp.ClientSession() as session:
            for league_name, league_id in league_ids.items():
                out_path = sports_dir / f"football_{league_name.replace(' ', '_')}_{season}.parquet"
                if out_path.exists():
                    continue

                try:
                    async with session.get(
                        f"{BASE}/fixtures",
                        headers=headers,
                        params={"league": league_id, "season": season, "status": "FT"},
                        timeout=aiohttp.ClientTimeout(total=20),
                    ) as resp:
                        if resp.status != 200:
                            continue
                        data = await resp.json()

                    rows = []
                    for fixture in data.get("response", []):
                        f = fixture.get("fixture", {})
                        teams = fixture.get("teams", {})
                        goals = fixture.get("goals", {})
                        ts = datetime.fromtimestamp(f.get("timestamp", 0), tz=timezone.utc)

                        rows.append({
                            "timestamp": ts,
                            "id": f"{league_name}:{teams.get('home',{}).get('name','')}:{ts.date()}",
                            "sport": "football",
                            "league": league_name,
                            "home_team": teams.get("home", {}).get("name", ""),
                            "away_team": teams.get("away", {}).get("name", ""),
                            "home_score": goals.get("home", 0) or 0,
                            "away_score": goals.get("away", 0) or 0,
                            "match_status": "finished",
                            "elapsed_minutes": 90,
                            "total_minutes": 90,
                            "remaining_minutes": 0,
                            "events": "[]",
                            "stats": "{}",
                            "odds": "{}",
                            "momentum": 0.0,
                            "external_id": str(f.get("id", "")),
                        })

                    if rows:
                        df = pd.DataFrame(rows).set_index("timestamp").sort_index()
                        df.to_parquet(out_path)
                        self._log.info(
                            "sports_saved",
                            league=league_name,
                            rows=len(rows),
                        )

                    await asyncio.sleep(1.0)   # Rate limit RapidAPI

                except Exception as exc:
                    self._log.warning("sports_collect_error", league=league_name, error=str(exc))
