"""Pure domain core: entities, value objects, and the job state machine.

This package has ZERO third-party imports. If you find yourself importing
pydantic, sqlalchemy, fastapi, redis, or boto3 here, stop — that logic belongs
in an adapter (``app.adapters.*``) behind a port (``app.ports.*``).
"""

from app.domain.exceptions import (
    DomainError,
    InvalidTransition,
    JobNotFound,
    PermanentUpstreamError,
    TransientUpstreamError,
)
from app.domain.models import InferenceJob, JobStatus, JobType

__all__ = [
    "DomainError",
    "InferenceJob",
    "InvalidTransition",
    "JobNotFound",
    "JobStatus",
    "JobType",
    "PermanentUpstreamError",
    "TransientUpstreamError",
]
