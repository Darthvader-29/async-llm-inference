"""FastAPI dependency providers — the only bridge from Starlette to the root.

Every provider reaches the container via ``request.app.state.container``. There
are NO module-level container/engine/redis globals; that is the spec's
"reject global module singletons" requirement, enforced structurally.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Request

from app.container import AppContainer
from app.core.config import Settings
from app.ports.object_store import ObjectStore
from app.ports.queue import JobQueue
from app.ports.repository import JobRepository
from app.services.ingestion import IngestionService


def get_container(request: Request) -> AppContainer:
    """Return the process-wide container stored by the lifespan on startup.

    Reads ``request.app.state.container``. No globals; the app instance carries
    the state, so test apps get their own container without monkeypatching.
    """
    container: AppContainer = request.app.state.container
    return container


ContainerDep = Annotated[AppContainer, Depends(get_container)]


def get_settings(container: ContainerDep) -> Settings:
    return container.settings


SettingsDep = Annotated[Settings, Depends(get_settings)]


def get_queue(container: ContainerDep) -> JobQueue:
    return container.queue


QueueDep = Annotated[JobQueue, Depends(get_queue)]


def get_object_store(container: ContainerDep) -> ObjectStore:
    return container.object_store


ObjectStoreDep = Annotated[ObjectStore, Depends(get_object_store)]


def get_repository(container: ContainerDep) -> JobRepository:
    """Return the process-wide repository (session-per-operation, Phase 3).

    The repository holds the session *factory* and opens/closes a short-lived
    session inside each add/get/update call, so there is no request-scoped
    session to manage and the pool returns to zero after each operation.
    """
    return container.repository


RepositoryDep = Annotated[JobRepository, Depends(get_repository)]


def get_ingestion_service(
    repository: RepositoryDep,
    queue: QueueDep,
    settings: SettingsDep,
) -> IngestionService:
    """Assemble the write-path service from request-scoped ports."""
    return IngestionService(repository=repository, queue=queue, retry_settings=settings.retry)


IngestionServiceDep = Annotated[IngestionService, Depends(get_ingestion_service)]
