"""
database/connection.py

Async SQLAlchemy engine + session factory, and Redis connection pool.
Everything is created once at startup and shared application-wide.
"""
from __future__ import annotations

from typing import AsyncGenerator

import structlog
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from database.models import Base

logger = structlog.get_logger(__name__)

# Singletons – populated by init_db() / init_redis()
_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker | None = None
_redis_pool = None


async def init_db(postgres_url: str, pool_size: int = 10, max_overflow: int = 20) -> None:
    global _engine, _session_factory
    _engine = create_async_engine(
        postgres_url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True,
        echo=False,
    )
    _session_factory = async_sessionmaker(
        bind=_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )
    # Create all tables (idempotent; Alembic handles migrations in production)
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("database_initialized")


async def init_redis(redis_url: str, pool_size: int = 20) -> None:
    global _redis_pool
    import redis.asyncio as aioredis
    _redis_pool = aioredis.ConnectionPool.from_url(
        redis_url,
        max_connections=pool_size,
        decode_responses=True,
    )
    logger.info("redis_pool_initialized")


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    if _session_factory is None:
        raise RuntimeError("Database not initialised. Call init_db() first.")
    async with _session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


def get_engine() -> AsyncEngine:
    if _engine is None:
        raise RuntimeError("Database not initialised.")
    return _engine


def get_redis_pool():
    if _redis_pool is None:
        raise RuntimeError("Redis not initialised. Call init_redis() first.")
    return _redis_pool


async def close_db() -> None:
    global _engine
    if _engine:
        await _engine.dispose()
        _engine = None
    logger.info("database_closed")


async def close_redis() -> None:
    global _redis_pool
    if _redis_pool:
        await _redis_pool.disconnect()
        _redis_pool = None
    logger.info("redis_closed")
