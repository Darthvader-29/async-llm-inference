"""IngestionService — the thin write-path behind ``POST /v1/jobs``.

Contract: persist a PENDING InferenceJob, then publish a ``{job_id, job_type,
attempt}`` pointer (with bounded retry). Returns the new job id. Touches ONLY
the repository and the queue — no providers, so no pipeline work can leak in.
"""

from __future__ import annotations

import logging
import uuid

from app.core.config import RetrySettings
from app.core.retry import retrying
from app.domain.models import InferenceJob, JobType
from app.ports.queue import JobQueue
from app.ports.repository import JobRepository

logger = logging.getLogger(__name__)


class IngestionService:
    """Stateless orchestrator; one instance per request (cheap to build)."""

    def __init__(
        self,
        repository: JobRepository,
        queue: JobQueue,
        retry_settings: RetrySettings,
    ) -> None:
        self._repository = repository
        self._queue = queue
        self._retry = retry_settings

    async def submit(self, job_type: JobType, payload: dict[str, object]) -> uuid.UUID:
        """Insert PENDING -> publish pointer (retried) -> return id.

        Args:
            job_type: discriminator already validated by the schema layer.
            payload: the model-dumped payload dict (stored verbatim as JSONB).

        Returns:
            The new job's UUID, surfaced to the client as ``job_id``.
        """
        job = InferenceJob.new(job_type=job_type, payload=payload)
        # 1) Persist FIRST so the row exists before any worker can read it.
        await self._repository.add(job)

        # 2) Publish a pointer, NOT the payload. PG is the source of truth.
        #    Wrap in retry: a transient Redis blip must not 500 the client. The
        #    adapter (StreamProducer) translates raw redis errors into
        #    Transient/PermanentUpstreamError, so this loop retries only genuine
        #    transients (see app.adapters.broker._errors.classify_redis_error).
        #
        #    Orphaned-PENDING decision: if publish ultimately fails (retries
        #    exhausted, or a permanent error), the upstream error propagates and
        #    the row is DELIBERATELY left PENDING. We do NOT mark it FAILED here:
        #      * the domain state machine has no PENDING->FAILED edge (only
        #        RUNNING->FAILED) — forging one would widen a locked invariant for
        #        a path that never executed the job;
        #      * the client gets a 5xx and never received the job_id, so there is
        #        no row to poll — the orphan is inert (no stream pointer exists, so
        #        no worker will ever touch it) and harmless to leave;
        #      * reaping stale PENDING rows is an operational concern, intentionally
        #        out of scope here (no reaper subsystem).
        #    Asserted by tests/unit/api/test_ingestion_retry.py.
        async for attempt in retrying(self._retry):
            with attempt:
                await self._queue.publish(job)

        logger.info("job accepted (job_id=%s, job_type=%s)", job.id, job.job_type.value)
        return job.id
