"""Domain entities and the job-status state machine.

``InferenceJob`` is an aggregate root modeled as a frozen-by-discipline mutable
dataclass (``slots=True``). All status changes go through guarded methods that
enforce the legal transition graph:

    PENDING --mark_running-->  RUNNING
    RUNNING --mark_success-->  SUCCESS   (terminal)
    RUNNING --mark_failed-->   FAILED    (terminal)
    RUNNING --requeue------->  PENDING   (transient-retry edge)

Any other (current, target) pair raises ``InvalidTransition``. The methods
mutate in place, bump ``updated_at``, and adjust ``attempts`` where relevant.
No persistence, no I/O — that is a port/adapter concern (Phase 3+).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from app.domain.exceptions import InvalidTransition


class JobStatus(StrEnum):
    """Lifecycle state of an inference job. Members ARE their string value."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class JobType(StrEnum):
    """The kind of pipeline a job runs (selects the pipeline in Phase 7)."""

    RAG_QUERY = "rag_query"
    EMBED_DOCUMENT = "embed_document"


# Terminal states cannot transition further. Used by the idempotency guard
# (Phase 5/7): a re-delivered message whose job is already terminal is skipped.
_TERMINAL: frozenset[JobStatus] = frozenset({JobStatus.SUCCESS, JobStatus.FAILED})

# The legal transition graph, as an adjacency map. This is the ONE place the
# rule is encoded; the transition methods consult it via ``_require``.
_ALLOWED: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.PENDING: frozenset({JobStatus.RUNNING}),
    JobStatus.RUNNING: frozenset({JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.PENDING}),
    JobStatus.SUCCESS: frozenset(),  # terminal
    JobStatus.FAILED: frozenset(),  # terminal
}


def _utcnow() -> datetime:
    """Timezone-aware UTC now. Centralized so tests can monkeypatch one symbol.

    Note: production code calls this; deterministic tests assert on *ordering*
    (updated_at advances) rather than on absolute clock values, and may patch
    this symbol when an exact instant is required — never ``time.sleep``.
    """
    return datetime.now(UTC)


@dataclass(slots=True)
class InferenceJob:
    """Aggregate root for a single asynchronous inference request.

    ``slots=True`` forbids attributes that aren't declared here (catches typos
    like ``job.staus = ...`` at runtime) and trims per-instance memory.
    """

    job_type: JobType
    payload: dict[str, object]
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    status: JobStatus = JobStatus.PENDING
    attempts: int = 0
    result_ref: str | None = None
    error: str | None = None
    duration_ms: int | None = None
    # ``lambda: _utcnow()`` (not the bare ``_utcnow``) so the factory resolves the
    # module-level name at call time — tests that monkeypatch ``models._utcnow``
    # then deterministically control creation timestamps too, not just transitions.
    created_at: datetime = field(default_factory=lambda: _utcnow())
    updated_at: datetime = field(default_factory=lambda: _utcnow())

    # -- factory -------------------------------------------------------------
    @classmethod
    def new(cls, job_type: JobType, payload: dict[str, object]) -> InferenceJob:
        """Create a brand-new PENDING job (fresh UUID, ``attempts=0``).

        The canonical creation path used by the ingestion service (Phase 6).
        Equivalent to the constructor with only the required fields, but named
        so call sites read intention-first: ``InferenceJob.new(job_type, payload)``.
        """
        return cls(job_type=job_type, payload=payload)

    # -- read-only derived properties ---------------------------------------
    @property
    def is_terminal(self) -> bool:
        """True when the job can never transition again (SUCCESS or FAILED)."""
        return self.status in _TERMINAL

    # -- internal guard ------------------------------------------------------
    def _require(self, target: JobStatus) -> None:
        """Raise ``InvalidTransition`` unless ``current -> target`` is legal."""
        if target not in _ALLOWED[self.status]:
            raise InvalidTransition(self.status, target)
        self.status = target
        self.updated_at = _utcnow()

    # -- guarded transitions -------------------------------------------------
    def mark_running(self) -> None:
        """PENDING -> RUNNING. Increments ``attempts`` (this is a new try)."""
        self._require(JobStatus.RUNNING)
        self.attempts += 1
        # A fresh attempt clears any error recorded by a previous failed run.
        self.error = None

    def mark_success(self, result_ref: str, duration_ms: int | None = None) -> None:
        """RUNNING -> SUCCESS. Records the object-store reference for the output.

        ``duration_ms`` is the wall-clock execution time measured by the worker
        (Phase 7) via ``time.monotonic()``, persisted as an execution metric.
        It is optional so the pure-domain tests can call ``mark_success(ref)``.
        """
        self._require(JobStatus.SUCCESS)
        self.result_ref = result_ref
        if duration_ms is not None:
            self.duration_ms = duration_ms

    def mark_failed(self, error: str, duration_ms: int | None = None) -> None:
        """RUNNING -> FAILED (terminal). Records the failure reason (+ duration)."""
        self._require(JobStatus.FAILED)
        self.error = error
        if duration_ms is not None:
            self.duration_ms = duration_ms

    def requeue(self) -> None:
        """RUNNING -> PENDING. The transient-retry edge.

        Used by the broker (Phase 5) when an attempt failed transiently and
        retry budget remains. ``attempts`` is NOT bumped here — it is bumped on
        the next ``mark_running``, so ``attempts`` always equals the number of
        times the job actually started executing.
        """
        self._require(JobStatus.PENDING)
