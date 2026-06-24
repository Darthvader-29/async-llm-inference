"""The on-wire job pointer and its (de)serialization.

A stream entry is intentionally tiny: just enough to locate the job in
PostgreSQL (the source of truth) and carry the retry attempt counter. All three
fields are strings so the wire encoding is unambiguous regardless of the
client's ``decode_responses`` setting.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from uuid import UUID

from app.domain.models import InferenceJob, JobType


def _as_str(value: object) -> str:
    """Normalize a field that may arrive as bytes (decode_responses=False) or
    str (decode_responses=True). The broker tolerates either client config."""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


@dataclass(frozen=True, slots=True)
class JobMessage:
    """Pointer carried by a single Redis Stream entry."""

    job_id: UUID
    job_type: JobType
    attempt: int  # 1-based; first delivery is attempt 1

    # --- serialization -----------------------------------------------------
    def to_fields(self) -> dict[str, str]:
        """Flat str->str map for XADD. All-string values keep the wire encoding
        unambiguous regardless of the client's ``decode_responses`` setting."""
        return {
            "job_id": str(self.job_id),
            "job_type": self.job_type.value,
            "attempt": str(self.attempt),
        }

    @classmethod
    def from_fields(cls, fields: Mapping[object, object]) -> JobMessage:
        """Inverse of :meth:`to_fields`. Accepts bytes-or-str keys/values so the
        same path works for fakeredis and a real client in either decode mode."""
        decoded = {_as_str(k): _as_str(v) for k, v in fields.items()}
        return cls(
            job_id=UUID(decoded["job_id"]),
            job_type=JobType(decoded["job_type"]),
            attempt=int(decoded["attempt"]),
        )

    # --- convenience -------------------------------------------------------
    @classmethod
    def first_delivery(cls, job: InferenceJob) -> JobMessage:
        """Build the initial pointer for a freshly-ingested job."""
        return cls(job_id=job.id, job_type=job.job_type, attempt=1)

    def next_attempt(self) -> JobMessage:
        """Pointer for the next retry; ``attempt`` incremented by one."""
        return JobMessage(self.job_id, self.job_type, self.attempt + 1)
