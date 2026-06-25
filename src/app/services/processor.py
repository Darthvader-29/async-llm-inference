"""JobProcessor — drives one job through its lifecycle.

Invoked by the StreamConsumer (Phase 5) with a job id and the delivery's
attempt number. Responsibilities:
  * load the authoritative row from PostgreSQL (source of truth),
  * idempotency guard: ack-and-skip if the job is already terminal,
  * transition PENDING -> RUNNING and persist,
  * dispatch to PIPELINES[job_type] and time it with time.monotonic(),
  * on success: mark_success(result_ref, duration_ms) and persist,
  * on failure: re-raise so the broker applies its retry/DLQ policy.

Division of responsibility (IMPORTANT): on failure the processor leaves the row
RUNNING and re-raises. The broker (Phase 5 ``StreamConsumer``) owns the terminal
transition — it ``requeue()``s (RUNNING->PENDING) on a transient retry or
``mark_failed()``s (RUNNING->FAILED) on DLQ. Both require the row to still be
RUNNING, so the processor must NOT mark it terminal itself; doing so would make
the broker's transition raise ``InvalidTransition`` and break retry/DLQ routing.
"""

from __future__ import annotations

import time
from uuid import UUID

import structlog

from app.adapters.broker.messages import JobMessage
from app.domain.exceptions import (
    JobNotFound,
    PermanentUpstreamError,
    TransientUpstreamError,
)
from app.ports.repository import JobRepository
from app.services.pipelines import PIPELINES, PipelineContext

log = structlog.get_logger(__name__)


class JobProcessor:
    """Process a single job by id. Holds collaborators, not state."""

    def __init__(self, repository: JobRepository, ctx: PipelineContext) -> None:
        self._repo = repository
        self._ctx = ctx

    async def __call__(self, message: JobMessage) -> None:
        """Dispatch entry point used by the StreamConsumer (Phase 5).

        The consumer's ``JobProcessor`` Protocol is ``__call__(message)``; we
        unpack the pointer and delegate to ``process`` (which the unit tests
        call directly with a job id + attempt).
        """
        await self.process(message.job_id, message.attempt)

    async def process(self, job_id: UUID, attempt: int) -> None:
        """Process the job identified by ``job_id``.

        ``attempt`` is the broker's delivery attempt (1-based), carried in the
        stream pointer. It is recorded for observability; the *decision* to
        retry or DLQ belongs to the broker, not here.

        Returns normally on success or on an idempotent skip (the broker will
        XACK). Raises on failure so the broker can retry or DLQ.
        """
        bound = log.bind(job_id=str(job_id), attempt=attempt)

        # --- Load the authoritative row (PG is the single source of truth) ---
        try:
            job = await self._repo.get(job_id)
        except JobNotFound:
            # The pointer references a row that does not exist. Nothing to do;
            # returning lets the broker XACK and drop the phantom message.
            bound.warning("job.not_found")
            return

        # --- Idempotency guard (at-least-once delivery is expected) ----------
        # Redis Streams + XAUTOCLAIM reclaim can deliver the same job twice. If
        # it already reached a terminal state, ack-and-skip: do NOT re-run the
        # pipeline or re-write artifacts.
        if job.is_terminal:
            bound.info("job.skip_terminal", status=job.status.value)
            return

        # --- Transition to RUNNING and persist before doing any work --------
        job.mark_running()  # raises InvalidTransition if not from PENDING
        await self._repo.update(job)
        bound.info("job.running")

        started = time.monotonic()  # monotonic: correct for measuring elapsed time
        try:
            pipeline = PIPELINES[job.job_type]
            result_ref = await pipeline.run(job, self._ctx)
        except (TransientUpstreamError, PermanentUpstreamError) as exc:
            # Re-raise, leaving the row RUNNING. The broker decides: transient +
            # attempts remaining -> requeue (RUNNING->PENDING); permanent or
            # exhausted -> DLQ + mark_failed (RUNNING->FAILED). See module docstring.
            bound.warning("job.failed", error=str(exc), kind=type(exc).__name__)
            raise
        except Exception as exc:
            # Unexpected pipeline bug -> classify as permanent so the broker DLQs
            # it rather than retrying forever. Still re-raise to hand over control.
            bound.exception("job.failed_unexpected")
            raise PermanentUpstreamError(str(exc)) from exc

        # --- Success: record the artifact ref and elapsed time --------------
        duration_ms = int((time.monotonic() - started) * 1000)
        job.mark_success(result_ref=result_ref, duration_ms=duration_ms)
        await self._repo.update(job)
        bound.info("job.success", result_ref=result_ref, duration_ms=duration_ms)
