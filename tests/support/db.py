"""Test-only async SQLite session factory (in-memory, no Docker)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from app.adapters.persistence.tables import Base


@asynccontextmanager
async def sqlite_session_factory() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """Yield a session factory over a fresh in-memory SQLite DB.

    StaticPool + a single shared in-memory connection keeps the schema alive
    for the lifetime of the engine (a brand-new ':memory:' per connection would
    otherwise be empty). Tables are created from Base.metadata via run_sync.
    """
    engine: AsyncEngine = create_async_engine(
        "sqlite+aiosqlite://",  # in-memory
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,  # one connection, shared -> persistent :memory:
    )
    # create_all is sync DDL -> run it through the async connection via run_sync.
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    try:
        yield async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    finally:
        await engine.dispose()  # teardown: no leaked connection
