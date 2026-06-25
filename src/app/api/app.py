"""FastAPI application factory + lifespan-owned composition root.

Boot in production with:  ``uvicorn app.api.app:create_app --factory``
``create_app()`` has no import-time side effects; all resource acquisition
happens inside the lifespan, on startup, within the running event loop.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.api.routes import health, jobs
from app.api.schemas import ErrorResponse
from app.container import AppContainer
from app.core.config import Settings
from app.core.logging import configure_logging
from app.domain.exceptions import (
    InvalidTransition,
    JobNotFound,
    PermanentUpstreamError,
)

logger = logging.getLogger(__name__)


def _make_lifespan(
    settings: Settings | None,
    container: AppContainer | None,
) -> Callable[[FastAPI], AbstractAsyncContextManager[None]]:
    """Build the lifespan context manager.

    - Normal boot: ``settings`` provided, ``container`` None -> we create+own it.
    - Tests: a pre-built (all-fakes) ``container`` is injected -> we DO NOT
      create or close it here; the test's LifespanManager/fixture owns its
      lifecycle.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        owns_container = container is None
        if owns_container:
            assert settings is not None  # guaranteed by create_app()
            configure_logging(settings)  # structlog setup (Phase 1)
            created = await AppContainer.create(settings)
            app.state.container = created
            logger.info("API lifespan startup complete")
        else:
            app.state.container = container  # injected for tests
        try:
            yield
        finally:
            if owns_container:
                await app.state.container.aclose()
                logger.info("API lifespan shutdown complete")

    return lifespan


def create_app(
    settings: Settings | None = None,
    *,
    container: AppContainer | None = None,
) -> FastAPI:
    """Construct the FastAPI app.

    Args:
        settings: app settings. Defaults to ``Settings()`` (env-driven). Ignored
            when ``container`` is supplied.
        container: a pre-built container (tests). When given, the lifespan does
            not create/destroy it — the caller owns it.

    Returns:
        A configured FastAPI instance ready for ASGI serving or ASGITransport.
    """
    if container is None and settings is None:
        settings = Settings()  # read env (AIE_*), apply zero-cloud redirect

    app = FastAPI(
        title="Asynchronous AI Serving Engine",
        version="1.0.0",
        summary="Decoupled, non-blocking ingestion for AI inference workloads.",
        lifespan=_make_lifespan(settings, container),
    )

    # Routers. Health is unauthenticated; jobs requires X-API-Key per-route.
    app.include_router(health.router)
    app.include_router(jobs.router)

    _register_exception_handlers(app)
    return app


def _register_exception_handlers(app: FastAPI) -> None:
    """Map domain/validation errors to clean, uniform JSON responses."""

    @app.exception_handler(RequestValidationError)
    async def _on_validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        # FastAPI's default already returns 422; we keep the structured errors
        # but wrap a top-level ``detail`` string for client uniformity. The
        # literal 422 avoids Starlette's deprecated HTTP_422_UNPROCESSABLE_ENTITY
        # constant (renamed across versions) — the wire code is what matters.
        return JSONResponse(
            status_code=422,
            content={"detail": "Request validation failed.", "errors": exc.errors()},
        )

    @app.exception_handler(JobNotFound)
    async def _on_job_not_found(request: Request, exc: JobNotFound) -> JSONResponse:
        # Unknown job id (e.g. GET /v1/jobs/{id}) -> 404 Not Found.
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content=ErrorResponse(detail=str(exc)).model_dump(),
        )

    @app.exception_handler(InvalidTransition)
    async def _on_invalid_transition(request: Request, exc: InvalidTransition) -> JSONResponse:
        # Illegal state move (should be rare on the write path) -> 409 Conflict.
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content=ErrorResponse(detail=str(exc)).model_dump(),
        )

    @app.exception_handler(PermanentUpstreamError)
    async def _on_permanent_upstream(request: Request, exc: PermanentUpstreamError) -> JSONResponse:
        # A non-retryable upstream failure surfaced on the request path -> 502.
        return JSONResponse(
            status_code=status.HTTP_502_BAD_GATEWAY,
            content=ErrorResponse(detail="Upstream dependency failed.").model_dump(),
        )
