"""
data_ingestion/polymarket_collector.py

Polymarket market data collector.
  - Fetches all active markets every 5 minutes via Gamma API
  - Fetches real-time prices/depth for watched markets every 10 seconds via CLOB API
  - Publishes MarketSnapshot to stream:markets
"""

from __future__ import annotations

import asyncio
import json
import statistics
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

import aiohttp
import structlog

from data_ingestion.base import BaseCollector

logger = structlog.get_logger(__name__)


@dataclass
class MarketSnapshot:
    market_id: str
    condition_id: str
    question: str
    category: str
    end_date: datetime | None
    outcomes: list[str]                  # ["Yes", "No"]
    prices: dict[str, float]             # {"Yes": 0.65, "No": 0.35}
    volumes_24h: dict[str, float]
    liquidity: float
    spread: float
    orderbook: dict                      # {"bids": [...], "asks": [...]}
    price_history: list[float]           # last N yes prices
    volatility_1h: float
    volatility_24h: float
    volume_change_rate: float
    timestamp: datetime

    def to_stream_dict(self) -> dict[str, str]:
        d = asdict(self)
        d["end_date"] = self.end_date.isoformat() if self.end_date else ""
        d["timestamp"] = self.timestamp.isoformat()
        d["outcomes"] = json.dumps(self.outcomes)
        d["prices"] = json.dumps(self.prices)
        d["volumes_24h"] = json.dumps(self.volumes_24h)
        d["orderbook"] = json.dumps(self.orderbook)
        d["price_history"] = json.dumps(self.price_history)
        return {k: str(v) for k, v in d.items()}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt_optional(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, tz=timezone.utc)
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _compute_volatility(prices: list[float]) -> float:
    """Annualised volatility approximation from a price series."""
    if len(prices) < 2:
        return 0.0
    import math
    returns = [
        math.log(prices[i] / prices[i - 1])
        for i in range(1, len(prices))
        if prices[i - 1] > 0 and prices[i] > 0
    ]
    if len(returns) < 2:
        return 0.0
    std = statistics.stdev(returns)
    # Scale: assume each data point is ~1 minute apart → annualise
    return round(std * (525_600 ** 0.5), 4)


class PolymarketCollector(BaseCollector):
    """
    Collects Polymarket market data.

    Two concurrant loops:
      1. Market list refresh loop  (every market_refresh_interval_s, default 5 min)
      2. Price polling loop        (every price_refresh_interval_s, default 10s)
         — only polls markets in self._watched_market_ids

    The watched set is auto-populated from all active markets on startup,
    and can be pruned via remove_from_watch() to focus on high-signal markets.
    """

    STREAM_KEY = "stream:markets"
    COLLECTOR_NAME = "polymarket"

    GAMMA_BASE = "https://gamma-api.polymarket.com"
    CLOB_BASE = "https://clob.polymarket.com"

    def __init__(
        self,
        redis_manager: Any,
        market_refresh_interval_s: int = 300,
        price_refresh_interval_s: int = 10,
        max_markets_to_watch: int = 50,
        min_liquidity_usd: float = 1_000.0,
    ) -> None:
        super().__init__(redis_manager)
        self._market_refresh_interval_s = market_refresh_interval_s
        self._price_refresh_interval_s = price_refresh_interval_s
        self._max_watch = max_markets_to_watch
        self._min_liquidity = min_liquidity_usd

        # market_id → basic market info (question, category, end_date)
        self._market_registry: dict[str, dict] = {}
        # market_id → condition_id (needed for CLOB API)
        self._condition_ids: dict[str, str] = {}
        # markets currently being price-polled
        self._watched_market_ids: set[str] = set()
        # rolling price history for volatility
        self._price_history: dict[str, list[float]] = {}
        self._price_history_prev_volume: dict[str, float] = {}

    def add_to_watch(self, market_id: str) -> None:
        self._watched_market_ids.add(market_id)

    def remove_from_watch(self, market_id: str) -> None:
        self._watched_market_ids.discard(market_id)

    async def _run_loop(self) -> None:
        async with aiohttp.ClientSession() as session:
            # Initial market load
            await self._refresh_markets(session)

            market_refresh_task = asyncio.create_task(
                self._market_refresh_loop(session)
            )
            price_poll_task = asyncio.create_task(
                self._price_poll_loop(session)
            )

            await self._stop_event.wait()

            market_refresh_task.cancel()
            price_poll_task.cancel()
            for t in (market_refresh_task, price_poll_task):
                try:
                    await t
                except asyncio.CancelledError:
                    pass

    async def _market_refresh_loop(self, session: aiohttp.ClientSession) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._market_refresh_interval_s
                )
            except asyncio.TimeoutError:
                pass
            if not self._stop_event.is_set():
                await self._refresh_markets(session)

    async def _price_poll_loop(self, session: aiohttp.ClientSession) -> None:
        while not self._stop_event.is_set():
            if self._watched_market_ids:
                # Poll in chunks to avoid overwhelming the API
                watched = list(self._watched_market_ids)
                chunk_size = 5
                for i in range(0, len(watched), chunk_size):
                    chunk = watched[i:i + chunk_size]
                    await asyncio.gather(
                        *[self._fetch_market_price(session, mid) for mid in chunk],
                        return_exceptions=True,
                    )
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._price_refresh_interval_s
                )
            except asyncio.TimeoutError:
                pass

    async def _refresh_markets(self, session: aiohttp.ClientSession) -> None:
        """Fetch all active markets from Gamma API and update registry."""
        try:
            cursor = ""
            all_markets: list[dict] = []

            while True:
                params: dict[str, Any] = {
                    "active": "true",
                    "closed": "false",
                    "limit": 100,
                }
                if cursor:
                    params["cursor"] = cursor

                async with session.get(
                    f"{self.GAMMA_BASE}/markets",
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        self._log.warning("gamma_api_error", status=resp.status)
                        return
                    data = await resp.json()

                markets = data if isinstance(data, list) else data.get("data", [])
                all_markets.extend(markets)

                # Pagination
                next_cursor = data.get("next_cursor") if isinstance(data, dict) else None
                if not next_cursor or not markets:
                    break
                cursor = next_cursor

            # Update registry
            for m in all_markets:
                mid = m.get("id") or m.get("conditionId") or ""
                if not mid:
                    continue
                liquidity = float(m.get("liquidityNum") or m.get("liquidity") or 0)
                if liquidity < self._min_liquidity:
                    continue

                self._market_registry[mid] = {
                    "question": m.get("question", ""),
                    "category": m.get("category", "other"),
                    "end_date": m.get("endDate") or m.get("endDateIso"),
                    "condition_id": m.get("conditionId", mid),
                    "liquidity": liquidity,
                }
                self._condition_ids[mid] = m.get("conditionId", mid)

            # Auto-populate watch list: top N by liquidity
            sorted_markets = sorted(
                self._market_registry.items(),
                key=lambda x: x[1].get("liquidity", 0),
                reverse=True,
            )
            self._watched_market_ids = {
                mid for mid, _ in sorted_markets[: self._max_watch]
            }

            self._log.info(
                "markets_refreshed",
                total=len(self._market_registry),
                watching=len(self._watched_market_ids),
            )

        except Exception as exc:
            self._log.error("market_refresh_error", error=str(exc), exc_info=True)
            raise

    async def _fetch_market_price(self, session: aiohttp.ClientSession, market_id: str) -> None:
        """Fetch CLOB orderbook + price for a single market."""
        cid = self._condition_ids.get(market_id, market_id)
        try:
            # CLOB book endpoint
            async with session.get(
                f"{self.CLOB_BASE}/book",
                params={"token_id": cid},
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status != 200:
                    return
                book_data = await resp.json()

            snapshot = self._build_snapshot(market_id, cid, book_data)
            if snapshot:
                await self._publish(snapshot.to_stream_dict())

        except asyncio.TimeoutError:
            self._log.debug("clob_timeout", market_id=market_id)
        except Exception as exc:
            self._log.warning("clob_price_error", market_id=market_id, error=str(exc))

    def _build_snapshot(self, market_id: str, cid: str, book_data: dict) -> MarketSnapshot | None:
        try:
            info = self._market_registry.get(market_id, {})
            bids = book_data.get("bids", [])
            asks = book_data.get("asks", [])

            # Best bid/ask for YES token
            best_bid = float(bids[0]["price"]) if bids else 0.0
            best_ask = float(asks[0]["price"]) if asks else 1.0
            mid_price = (best_bid + best_ask) / 2
            spread = best_ask - best_bid

            yes_price = round(mid_price, 4)
            no_price = round(1.0 - yes_price, 4)

            # Volume from bids/asks depth
            bid_volume = sum(float(b.get("size", 0)) for b in bids[:10])
            ask_volume = sum(float(a.get("size", 0)) for a in asks[:10])
            liquidity = bid_volume + ask_volume

            # Rolling price history
            history = self._price_history.setdefault(market_id, [])
            history.append(yes_price)
            if len(history) > 1440:  # keep 24h at 1-min resolution
                history.pop(0)

            # Volatility
            vol_1h = _compute_volatility(history[-60:]) if len(history) >= 60 else 0.0
            vol_24h = _compute_volatility(history) if len(history) >= 2 else 0.0

            # Volume change rate (compare last to previous)
            prev_vol = self._price_history_prev_volume.get(market_id, liquidity)
            vol_change_rate = (liquidity - prev_vol) / prev_vol if prev_vol > 0 else 0.0
            self._price_history_prev_volume[market_id] = liquidity

            return MarketSnapshot(
                market_id=market_id,
                condition_id=cid,
                question=info.get("question", ""),
                category=info.get("category", "other"),
                end_date=_parse_dt_optional(info.get("end_date")),
                outcomes=["Yes", "No"],
                prices={"Yes": yes_price, "No": no_price},
                volumes_24h={"Yes": bid_volume, "No": ask_volume},
                liquidity=round(liquidity, 2),
                spread=round(spread, 4),
                orderbook={"bids": bids[:10], "asks": asks[:10]},
                price_history=history[-60:],
                volatility_1h=vol_1h,
                volatility_24h=vol_24h,
                volume_change_rate=round(vol_change_rate, 4),
                timestamp=_now_utc(),
            )
        except Exception as exc:
            self._log.warning("snapshot_build_error", market_id=market_id, error=str(exc))
            return None
