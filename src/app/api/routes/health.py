"""Liveness (``/health``) and readiness (``/health/ready``) endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Response, status
from sqlalchemy import text

from app.api.dependencies import ContainerDep
from app.api.schemas import HealthStatus, ProbeResult, ReadinessStatus

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthStatus, summary="Liveness probe.")
async def health() -> HealthStatus:
    """Always 200 if the process is up. NO dependency checks here.

    A liveness probe must not fail just because Postgres is briefly slow — that
    would make the orchestrator kill an otherwise-healthy process.
    """
    return HealthStatus()


@router.get(
    "/health/ready",
    response_model=ReadinessStatus,
    summary="Readiness probe (checks DB, Redis, object store).",
    responses={503: {"model": ReadinessStatus, "description": "A dependency is down."}},
)
async def readiness(container: ContainerDep, response: Response) -> ReadinessStatus:
    """Probe each dependency; 200 only if all are reachable, else 503.

    Each probe is independent and failure-isolated so the response enumerates
    exactly which dependency is unhealthy — invaluable during an incident.
    """
    checks: list[ProbeResult] = []

    # 1) PostgreSQL: a trivial round-trip proves the pool can hand out a live
    #    connection and the DB answers.
    try:
        async with container.session_factory() as session:
            await session.execute(text("SELECT 1"))
        checks.append(ProbeResult(name="postgres", ok=True))
    except Exception as exc:  # probe must capture ANY failure, hence broad
        checks.append(ProbeResult(name="postgres", ok=False, detail=type(exc).__name__))

    # 2) Redis: PING.
    try:
        await container.redis.ping()
        checks.append(ProbeResult(name="redis", ok=True))
    except Exception as exc:
        checks.append(ProbeResult(name="redis", ok=False, detail=type(exc).__name__))

    # 3) Object store: head_bucket via the port (offloaded boto3 under the hood).
    try:
        await container.object_store.bucket_exists()
        checks.append(ProbeResult(name="object_store", ok=True))
    except Exception as exc:
        checks.append(ProbeResult(name="object_store", ok=False, detail=type(exc).__name__))

    all_ok = all(c.ok for c in checks)
    if not all_ok:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return ReadinessStatus(status="ready" if all_ok else "not_ready", checks=checks)
