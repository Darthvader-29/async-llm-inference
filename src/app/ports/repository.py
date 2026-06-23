"""Persistence port: store and retrieve InferenceJob aggregates.

PostgreSQL is the single source of truth for job state. This port exposes the
*minimal* surface services need: create, fetch-by-id, and persist updates after
a state transition. Row↔domain mapping lives entirely in the adapter
(Phase 3); this contract speaks only the domain language.
"""

from __future__ import annotations

from typing import Protocol
from uuid import UUID

from app.domain.models import InferenceJob


class JobRepository(Protocol):
    """Async repository for :class:`app.domain.models.InferenceJob`.

    Implementations MUST be safe to use within a single logical unit of work
    (e.g. one SQLAlchemy ``AsyncSession``); the adapter, not this port, owns
    transaction boundaries.
    """

    async def add(self, job: InferenceJob) -> None:
        """Persist a brand-new job (status ``PENDING``).

        Raises a domain/persistence error if a job with the same id already
        exists. Does not return the entity (the caller already holds it).
        """
        ...

    async def get(self, job_id: UUID) -> InferenceJob:
        """Load a job by id.

        Raises ``JobNotFound`` (domain error) if no such job exists. The
        returned entity is a *detached* domain object — mutating it does not
        write back until ``update`` is called.
        """
        ...

    async def update(self, job: InferenceJob) -> None:
        """Persist the current state of an existing job.

        Used after ``mark_running``/``mark_success``/``mark_failed``/``requeue``.
        Implementations should be idempotent w.r.t. re-applying the same
        terminal state (supports at-least-once delivery, Phase 5).
        """
        ...
