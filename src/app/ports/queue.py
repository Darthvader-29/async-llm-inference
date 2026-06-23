"""Queue port: publish a job *pointer* onto the broker.

Per the locked design, stream messages carry only ``{job_id, job_type,
attempt}`` — PostgreSQL holds the authoritative payload/status. Keeping the
port to a single ``publish`` method means the consumer side (the worker's
``consume_once`` loop) is *not* part of this port; consumption is an adapter
internal detail (Phase 5), not something services call.
"""

from __future__ import annotations

from typing import Protocol

from app.domain.models import InferenceJob


class JobQueue(Protocol):
    """Async producer for enqueuing work."""

    async def publish(self, job: InferenceJob) -> None:
        """Enqueue a job's *pointer* (``{id, job_type, attempt=1}``) for async work.

        The queue extracts only the pointer fields from ``job`` — the payload and
        status stay in PostgreSQL (the source of truth). Retries are issued by the
        consumer internally (re-XADD with ``attempt+1``), not through this port.
        The ingestion service wraps this call in the retry policy so a transient
        broker blip does not fail the API request on the first attempt.
        """
        ...
