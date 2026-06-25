"""Job ingestion + status routes (``/v1/jobs``)."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, status

from app.api.auth import ApiKeyDep
from app.api.dependencies import IngestionServiceDep, RepositoryDep
from app.api.schemas import JobAccepted, JobStatusResponse, JobSubmission
from app.domain.models import JobStatus

router = APIRouter(prefix="/v1/jobs", tags=["jobs"])


@router.post(
    "",
    response_model=JobAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit an inference job (returns immediately).",
    responses={
        401: {"description": "Missing or invalid API key."},
        422: {"description": "Payload failed validation."},
    },
)
async def submit_job(
    body: JobSubmission,
    service: IngestionServiceDep,
    _: ApiKeyDep,  # presence enforces auth; value unused here.
) -> JobAccepted:
    """Persist a PENDING job and enqueue a pointer; return 202 + tracking id.

    The body is already a validated discriminated union (``body.payload`` is a
    concrete RagQueryPayload | EmbedDocumentPayload). We dump it to a JSON-mode
    dict for storage and hand it to the service. NO pipeline work happens here.
    """
    payload = body.payload
    job_id = await service.submit(
        job_type=payload.job_type,
        payload=payload.model_dump(mode="json"),
    )
    return JobAccepted(
        job_id=job_id,
        status=JobStatus.PENDING,  # acceptance always means PENDING
        status_url=f"/v1/jobs/{job_id}",
    )


@router.get(
    "/{job_id}",
    response_model=JobStatusResponse,
    summary="Fetch the current status of a job.",
    responses={404: {"description": "No job with that id."}},
)
async def get_job(
    job_id: uuid.UUID,
    repository: RepositoryDep,
    _: ApiKeyDep,
) -> JobStatusResponse:
    """Read-through to the repository; 404 if unknown.

    ``repository.get`` raises ``JobNotFound`` for an unknown id; the app-level
    exception handler (app.py) maps that domain error to a 404.
    """
    job = await repository.get(job_id)
    return JobStatusResponse.model_validate(job)
