# migrations/env.py — async Alembic environment.
"""Runs migrations against Settings().database_url using an async engine.

Pattern (verified against the Alembic asyncio cookbook):
  - build an async engine
  - open an async connection
  - connection.run_sync(do_run_migrations) -> Alembic's sync migration API
    runs INSIDE the greenlet-adapted sync function.
"""

from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from app.adapters.persistence.tables import Base
from app.core.config import Settings

# Alembic Config object (values from alembic.ini).
config = context.config

# Inject the real DB URL from Settings (alembic.ini leaves it blank).
# Settings() reads AIE_-prefixed env vars (and .env), so dev/test/prod all work.
config.set_main_option("sqlalchemy.url", str(Settings().database_url))

# Configure Python logging from the ini [loggers] section (if present).
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Autogenerate + online migrations diff against this metadata.
target_metadata = Base.metadata


def do_run_migrations(connection: Connection) -> None:
    """Synchronous migration body, run inside run_sync()."""
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,          # detect column TYPE changes in autogenerate
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Create an async engine, open a connection, run migrations via run_sync."""
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,    # one-shot migration run: no pooling needed
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_offline() -> None:
    """Offline ('--sql') mode: emit SQL without a live DB connection."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Online mode entry point: drive the async runner from sync Alembic."""
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
