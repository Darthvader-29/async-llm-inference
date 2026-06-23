"""SQLAlchemy implementation of the JobRepository port.

Hexagonal boundary: this is the ONLY module that knows JobRow exists.
Public methods accept/return domain InferenceJob; private mappers convert.
Session-per-operation: each method opens its own short-lived AsyncSession
from the injected factory.
"""

from __future__ import annotations

import uuid

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.adapters.persistence.tables import JobRow
from app.domain.exceptions import JobNotFound
from app.domain.models import InferenceJob, JobStatus, JobType


class SqlAlchemyJobRepository:
    """Durable job store backed by SQLAlchemy 2.0 async.

    Conforms structurally to the JobRepository Protocol (Phase 2) — it does
    NOT import the protocol; mypy --strict checks conformance where the
    AppContainer injects it.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        # A FACTORY, not a session. Shared safely across concurrent tasks;
        # each call below opens its own session.
        self._session_factory = session_factory

    # ------------------------------------------------------------------ #
    # Port methods (domain in, domain out — never a JobRow)
    # ------------------------------------------------------------------ #
    async def add(self, job: InferenceJob) -> None:
        """Insert a new job row (called by ingestion before publishing)."""
        # session.begin() opens BEGIN ... COMMIT (or ROLLBACK on error); the
        # `session` target binds before begin() is evaluated, so one combined
        # `async with` is correct. expire_on_commit=False -> no implicit refresh.
        async with self._session_factory() as session, session.begin():
            session.add(_to_row(job))

    async def get(self, job_id: uuid.UUID) -> InferenceJob:
        """Load a job by id. Raises JobNotFound if absent.

        Used by the worker's idempotency guard (Phase 7) and GET /v1/jobs/{id}.
        """
        async with self._session_factory() as session:
            row = await session.get(JobRow, job_id)  # PK lookup via identity map/SELECT
            if row is None:
                raise JobNotFound(job_id)
            return _to_domain(row)

    async def update(self, job: InferenceJob) -> None:
        """Persist the current state of an existing job (after a transition).

        Uses session.merge() so the detached domain->row mapping is reconciled
        with the persistent row in one UPDATE. Raises JobNotFound if the id
        does not exist (merge would otherwise INSERT — we forbid that here).
        """
        async with self._session_factory() as session, session.begin():
            exists = await session.get(JobRow, job.id)
            if exists is None:
                raise JobNotFound(job.id)
            await session.merge(_to_row(job))  # reconcile -> UPDATE
            # commit on block exit


# ---------------------------------------------------------------------- #
# Mapping boundary — the ONLY place row<->domain conversion happens.
# Keeping these module-private functions (not methods) makes the boundary
# explicit and trivially unit-testable.
# ---------------------------------------------------------------------- #
def _to_row(job: InferenceJob) -> JobRow:
    """domain InferenceJob -> ORM JobRow."""
    return JobRow(
        id=job.id,
        type=job.job_type.value,  # StrEnum -> str
        status=job.status.value,  # StrEnum -> str
        payload=job.payload,  # plain dict -> JSONB/JSON
        result_ref=job.result_ref,
        error=job.error,
        attempts=job.attempts,
        duration_ms=job.duration_ms,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )


def _to_domain(row: JobRow) -> InferenceJob:
    """ORM JobRow -> domain InferenceJob. No ORM type leaks past this return."""
    return InferenceJob(
        id=row.id,
        job_type=JobType(row.type),  # str -> StrEnum (validates membership)
        status=JobStatus(row.status),  # str -> StrEnum
        payload=row.payload,
        result_ref=row.result_ref,
        error=row.error,
        attempts=row.attempts,
        duration_ms=row.duration_ms,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )
