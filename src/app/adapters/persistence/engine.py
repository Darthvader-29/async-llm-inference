"""Async SQLAlchemy engine + session factory, built from Settings.

The composition root (Phase 6 AppContainer) calls build_engine() once at
startup and dispose() once at shutdown. Everything else receives the
async_sessionmaker (a factory), never a live session.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import Settings


def build_engine(settings: Settings) -> AsyncEngine:
    """Create the async asyncpg Engine from Settings.

    Builds the production/dev ``postgresql+asyncpg://`` engine. Pool params are
    sensible fixed defaults (promote them to Settings if a deployment ever needs
    to tune them). Unit tests do NOT call this — they use the in-memory SQLite
    helper in ``tests/support/db.py`` with an explicit StaticPool.
    """
    return create_async_engine(
        settings.database_url,  # canonical Settings field is a str DSN
        echo=False,  # SQL logging off; flip locally to debug
        pool_pre_ping=True,  # validate a pooled conn before use ->
        #   transparently recovers from a server
        #   restart / dropped TCP connection
        pool_size=10,  # steady-state pooled connections
        max_overflow=20,  # burst connections above pool_size
        pool_recycle=1800,  # recycle conns older than 30 min
    )


def build_session_factory(
    engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    """Create the session factory shared by the repository.

    expire_on_commit=False is REQUIRED for the async session-per-operation
    pattern: it keeps attributes populated after commit() so the mapper can
    read them WITHOUT triggering implicit lazy I/O (which async forbids).
    """
    return async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
        class_=AsyncSession,  # explicit; async_sessionmaker defaults to this
    )


async def dispose(engine: AsyncEngine) -> None:
    """Dispose the engine's connection pool (teardown / exit-criterion 3).

    Called by AppContainer.aclose(); closes every pooled connection so no
    file descriptor / asyncpg connection is left dangling.
    """
    await engine.dispose()
