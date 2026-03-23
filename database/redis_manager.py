"""
database/redis_manager.py

Redis manager for Streams, Cache, State, and PubSub.
Uses redis.asyncio with a shared connection pool.
"""
from __future__ import annotations

import json
from datetime import timedelta
from typing import Any, AsyncGenerator

import structlog

logger = structlog.get_logger(__name__)

# Max stream length before Redis auto-trims (MAXLEN ~)
STREAM_MAXLEN = 10_000
# Default cache TTL
DEFAULT_TTL = 60


class RedisManager:
    """
    Wraps redis.asyncio to provide:
      - Stream publish/consume (Redis Streams)
      - Key/value cache
      - Persistent state storage
      - PubSub helpers
    """

    def __init__(self, pool: Any) -> None:
        self._pool = pool
        self._log = logger.bind(component="redis")

    def _client(self):
        import redis.asyncio as aioredis
        return aioredis.Redis(connection_pool=self._pool)

    # ── Streams ───────────────────────────────────────────────────────────────

    async def publish_to_stream(self, stream_name: str, data: dict) -> str | None:
        """
        Append a message to a Redis Stream.
        All values are JSON-serialised strings (Streams only support strings).
        Returns the message ID.
        """
        try:
            serialised = {k: (json.dumps(v) if not isinstance(v, str) else v)
                          for k, v in data.items()}
            async with self._client() as r:
                msg_id = await r.xadd(
                    stream_name, serialised, maxlen=STREAM_MAXLEN, approximate=True
                )
            return msg_id
        except Exception as exc:
            self._log.error("stream_publish_error", stream=stream_name, error=str(exc))
            return None

    async def ensure_consumer_group(self, stream_name: str, group_name: str) -> None:
        """Create consumer group if it doesn't exist, starting from newest messages."""
        try:
            async with self._client() as r:
                try:
                    await r.xgroup_create(stream_name, group_name, id="$", mkstream=True)
                except Exception as e:
                    if "BUSYGROUP" not in str(e):
                        raise
        except Exception as exc:
            self._log.warning("ensure_group_error", stream=stream_name, error=str(exc))

    async def consume_stream(
        self,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        count: int = 10,
        block_ms: int = 1000,
    ) -> list[dict]:
        """
        Read new messages from a consumer group.
        Acknowledges messages automatically after return.
        Returns list of decoded message dicts.
        """
        try:
            async with self._client() as r:
                messages = await r.xreadgroup(
                    group_name,
                    consumer_name,
                    {stream_name: ">"},
                    count=count,
                    block=block_ms,
                )
            if not messages:
                return []

            results: list[dict] = []
            msg_ids: list[str] = []

            for _, entries in messages:
                for msg_id, fields in entries:
                    decoded = {
                        k.decode() if isinstance(k, bytes) else k:
                        v.decode() if isinstance(v, bytes) else v
                        for k, v in fields.items()
                    }
                    decoded["_msg_id"] = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                    results.append(decoded)
                    msg_ids.append(decoded["_msg_id"])

            # Acknowledge
            if msg_ids:
                async with self._client() as r:
                    await r.xack(stream_name, group_name, *msg_ids)

            return results

        except Exception as exc:
            self._log.error("stream_consume_error", stream=stream_name, error=str(exc))
            return []

    async def stream_iter(
        self,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        count: int = 10,
    ) -> AsyncGenerator[dict, None]:
        """
        Async generator that continuously yields messages from a stream.
        Runs until the caller breaks or StopAsyncIteration.
        """
        while True:
            messages = await self.consume_stream(
                stream_name, group_name, consumer_name, count=count, block_ms=2000
            )
            for msg in messages:
                yield msg

    # ── Cache ─────────────────────────────────────────────────────────────────

    async def cache_market_data(
        self, market_id: str, snapshot: Any, ttl: int = DEFAULT_TTL
    ) -> None:
        try:
            data = {
                "market_id": snapshot.market_id,
                "question": snapshot.question,
                "yes_price": snapshot.prices.get("Yes", 0.5),
                "no_price": snapshot.prices.get("No", 0.5),
                "liquidity": snapshot.liquidity,
                "spread": snapshot.spread,
                "timestamp": snapshot.timestamp.isoformat(),
            }
            async with self._client() as r:
                await r.set(f"cache:market:{market_id}", json.dumps(data), ex=ttl)
        except Exception as exc:
            self._log.warning("cache_set_error", key=market_id, error=str(exc))

    async def get_cached_market_data(self, market_id: str) -> dict | None:
        try:
            async with self._client() as r:
                raw = await r.get(f"cache:market:{market_id}")
            return json.loads(raw) if raw else None
        except Exception as exc:
            self._log.warning("cache_get_error", key=market_id, error=str(exc))
            return None

    async def cache_set(self, key: str, value: Any, ttl: int | None = DEFAULT_TTL) -> None:
        try:
            serialised = json.dumps(value) if not isinstance(value, str) else value
            async with self._client() as r:
                if ttl:
                    await r.set(key, serialised, ex=ttl)
                else:
                    await r.set(key, serialised)
        except Exception as exc:
            self._log.warning("cache_set_error", key=key, error=str(exc))

    async def cache_get(self, key: str) -> Any | None:
        try:
            async with self._client() as r:
                raw = await r.get(key)
            if raw is None:
                return None
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return raw
        except Exception as exc:
            self._log.warning("cache_get_error", key=key, error=str(exc))
            return None

    async def cache_delete(self, key: str) -> None:
        try:
            async with self._client() as r:
                await r.delete(key)
        except Exception:
            pass

    # ── State (persistent, no TTL) ────────────────────────────────────────────

    async def save_risk_state(self, state: Any) -> None:
        await self.set("risk:state", json.dumps(state.to_dict()))

    async def load_risk_state(self) -> Any | None:
        raw = await self.get("risk:state")
        if raw:
            from risk.risk_state import RiskState
            return RiskState.from_dict(json.loads(raw))
        return None

    async def save_portfolio_state(self, portfolio: Any) -> None:
        """Serialise portfolio to Redis for cross-restart recovery."""
        data = {
            "total_balance": portfolio.total_balance,
            "available_balance": portfolio.available_balance,
            "total_position_value": portfolio.total_position_value,
            "daily_pnl": portfolio.daily_pnl,
            "mode": portfolio.mode,
        }
        await self.set("state:portfolio", json.dumps(data))

    async def load_portfolio_state(self) -> dict | None:
        raw = await self.get("state:portfolio")
        return json.loads(raw) if raw else None

    # ── Raw get/set (used by RiskState) ───────────────────────────────────────

    async def set(self, key: str, value: str, ttl: int | None = None) -> None:
        try:
            async with self._client() as r:
                if ttl:
                    await r.set(key, value, ex=ttl)
                else:
                    await r.set(key, value)
        except Exception as exc:
            self._log.error("redis_set_error", key=key, error=str(exc))

    async def get(self, key: str) -> str | None:
        try:
            async with self._client() as r:
                val = await r.get(key)
            return val.decode() if isinstance(val, bytes) else val
        except Exception as exc:
            self._log.error("redis_get_error", key=key, error=str(exc))
            return None

    async def delete(self, key: str) -> None:
        try:
            async with self._client() as r:
                await r.delete(key)
        except Exception:
            pass

    # ── Heartbeat / health ────────────────────────────────────────────────────

    async def ping(self) -> bool:
        try:
            async with self._client() as r:
                return await r.ping()
        except Exception:
            return False

    async def stream_info(self, stream_name: str) -> dict:
        try:
            async with self._client() as r:
                info = await r.xinfo_stream(stream_name)
            return {"length": info.get("length", 0), "groups": info.get("groups", 0)}
        except Exception:
            return {"length": 0, "groups": 0}
