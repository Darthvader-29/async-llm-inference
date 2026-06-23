"""Repository round-trip on REAL Postgres. Requires infra up + migrations.

Run with:  uv run poe test-int      (i.e. pytest -m integration)
Skipped by the default `pytest -m "not integration"` quality run.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.adapters.persistence.engine import build_engine, dispose
from app.adapters.persistence.repository import SqlAlchemyJobRepository
from app.core.config import Settings
from app.domain.models import InferenceJob, JobStatus, JobType

pytestmark = pytest.mark.integration  # whole module is integration-tier


@pytest.fixture
async def pg_session_factory() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """Session factory bound to the real Postgres from Settings.

    Assumes `alembic upgrade head` has already created the schema (the CI
    integration job and `poe migrate` do this before invoking pytest).
    """
    settings = Settings()
    # Guard: make the requirement explicit if someone runs this without PG.
    if "asyncpg" not in str(settings.database_url):
        pytest.skip("integration test requires a postgresql+asyncpg DATABASE_URL")

    engine = build_engine(settings)
    factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    try:
        yield factory
    finally:
        await dispose(engine)  # exit-criterion 3: clean teardown


async def test_pg_round_trip(pg_session_factory: async_sessionmaker[AsyncSession]) -> None:
    repo = SqlAlchemyJobRepository(pg_session_factory)
    job = InferenceJob.new(
        job_type=JobType.EMBED_DOCUMENT,
        payload={"document_uri": "s3://aie-artifacts/in.txt", "chunk_size": 512},
    )

    await repo.add(job)
    loaded = await repo.get(job.id)
    assert loaded.status is JobStatus.PENDING
    assert loaded.payload["chunk_size"] == 512  # JSONB round-trip on real PG

    job.mark_running()
    await repo.update(job)
    job.mark_failed(error="upstream 503")  # RUNNING -> FAILED
    await repo.update(job)

    final = await repo.get(job.id)
    assert final.status is JobStatus.FAILED
    assert final.error == "upstream 503"
    assert final.updated_at >= final.created_at
