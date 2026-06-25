"""Pydantic v2 wire models for the ingestion API.

These models define the *external contract*. They are intentionally separate
from the domain entity (``app.domain.models.InferenceJob``): the domain is a
slotted dataclass with a state machine; these are validation/serialization
models. Translation between the two happens in the route/service layer.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from app.domain.models import JobStatus, JobType

# --------------------------------------------------------------------------- #
# Request payloads — a discriminated union keyed on ``job_type``.
# Each member carries a Literal[...] tag equal to the corresponding JobType
# value, so pydantic dispatches in O(1) and emits a precise per-member error.
# --------------------------------------------------------------------------- #


class _PayloadBase(BaseModel):
    # forbid unknown keys so typos like "quer" become 422s, not silent drops.
    model_config = ConfigDict(extra="forbid")


class RagQueryPayload(_PayloadBase):
    """Submit a retrieval-augmented-generation query."""

    job_type: Literal[JobType.RAG_QUERY] = JobType.RAG_QUERY
    query: str = Field(min_length=1, max_length=4_000)
    top_k: int = Field(default=5, ge=1, le=50)


class EmbedDocumentPayload(_PayloadBase):
    """Submit a document to be chunked, embedded, and upserted."""

    job_type: Literal[JobType.EMBED_DOCUMENT] = JobType.EMBED_DOCUMENT
    document_id: str = Field(min_length=1, max_length=256)
    text: str = Field(min_length=1, max_length=200_000)


# The discriminated union. ``discriminator="job_type"`` tells pydantic to read
# the ``job_type`` tag first and validate against exactly one member.
JobPayload = Annotated[
    RagQueryPayload | EmbedDocumentPayload,
    Field(discriminator="job_type"),
]


class JobSubmission(BaseModel):
    """Top-level request body for ``POST /v1/jobs``.

    Wrapping the union in a field (rather than using a bare-union body) gives a
    stable JSON shape ``{"payload": {...}}`` and a clean place to add
    request-level metadata later (e.g. an idempotency key) without breaking the
    contract.
    """

    model_config = ConfigDict(extra="forbid")
    payload: JobPayload


# --------------------------------------------------------------------------- #
# Responses
# --------------------------------------------------------------------------- #
class JobAccepted(BaseModel):
    """202 body: the client polls ``status_url`` for terminal state."""

    job_id: uuid.UUID
    status: JobStatus  # always PENDING at acceptance time
    status_url: str = Field(
        description="Relative URL to poll for status, e.g. /v1/jobs/{id}.",
    )


class JobStatusResponse(BaseModel):
    """200 body for ``GET /v1/jobs/{id}``: the full audit view of one job."""

    # ``from_attributes`` lets us validate straight off the domain dataclass;
    # ``job_id`` reads the entity's ``id`` attribute via the validation alias.
    model_config = ConfigDict(from_attributes=True)

    job_id: uuid.UUID = Field(validation_alias="id")
    job_type: JobType
    status: JobStatus
    attempts: int
    result_ref: str | None = Field(
        default=None,
        description="s3://… pointer to the result artifact once SUCCESS.",
    )
    error: str | None = None
    created_at: datetime
    updated_at: datetime
    duration_ms: int | None = None


class HealthStatus(BaseModel):
    """200 body for ``GET /health`` (liveness)."""

    status: Literal["ok"] = "ok"


class ProbeResult(BaseModel):
    """One dependency probe's outcome, embedded in the readiness response."""

    name: str
    ok: bool
    detail: str | None = None


class ReadinessStatus(BaseModel):
    """Body for ``GET /health/ready``; HTTP code conveys overall readiness."""

    status: Literal["ready", "not_ready"]
    checks: list[ProbeResult]


class ErrorResponse(BaseModel):
    """Uniform error envelope used by custom exception handlers."""

    detail: str
