"""Repository round-trip on in-memory SQLite — NO Docker, NO clock."""

from __future__ import annotations

import uuid

import pytest

from app.adapters.persistence.repository import SqlAlchemyJobRepository
from app.domain.exceptions import JobNotFound
from app.domain.models import InferenceJob, JobStatus, JobType
from tests.support.db import sqlite_session_factory

# asyncio_mode="auto" (Phase 1 pyproject) -> no @pytest.mark.asyncio needed.


def _make_pending_job() -> InferenceJob:
    """A fresh PENDING job (domain factory mirrors Phase 1 constructor)."""
    return InferenceJob.new(
        job_type=JobType.RAG_QUERY,
        payload={"query": "what is hexagonal architecture?", "top_k": 3},
    )


async def test_add_then_get_round_trip() -> None:
    async with sqlite_session_factory() as factory:
        repo = SqlAlchemyJobRepository(factory)
        job = _make_pending_job()

        await repo.add(job)
        loaded = await repo.get(job.id)

        # Identity + every mapped field survives the round-trip unchanged.
        assert loaded.id == job.id
        assert loaded.job_type is JobType.RAG_QUERY  # str -> StrEnum re-validated
        assert loaded.status is JobStatus.PENDING
        assert loaded.payload == {"query": "what is hexagonal architecture?", "top_k": 3}
        assert loaded.attempts == 0
        assert loaded.result_ref is None
        assert loaded.error is None
        assert loaded.created_at is not None  # server_default now()


async def test_update_persists_status_transition() -> None:
    async with sqlite_session_factory() as factory:
        repo = SqlAlchemyJobRepository(factory)
        job = _make_pending_job()
        await repo.add(job)

        # Drive the DOMAIN state machine, then persist.
        job.mark_running()  # PENDING -> RUNNING
        await repo.update(job)
        running = await repo.get(job.id)
        assert running.status is JobStatus.RUNNING
        assert running.attempts == 1  # mark_running bumped attempts

        job.mark_success(result_ref="s3://aie-artifacts/x.json", duration_ms=42)
        await repo.update(job)
        done = await repo.get(job.id)
        assert done.status is JobStatus.SUCCESS
        assert done.result_ref == "s3://aie-artifacts/x.json"
        assert done.duration_ms == 42
        assert done.updated_at >= done.created_at  # transition bumped updated_at


async def test_get_missing_raises_job_not_found() -> None:
    async with sqlite_session_factory() as factory:
        repo = SqlAlchemyJobRepository(factory)
        with pytest.raises(JobNotFound):
            await repo.get(uuid.uuid4())  # never inserted


async def test_update_missing_raises_job_not_found() -> None:
    async with sqlite_session_factory() as factory:
        repo = SqlAlchemyJobRepository(factory)
        ghost = _make_pending_job()  # never added
        with pytest.raises(JobNotFound):
            await repo.update(ghost)
