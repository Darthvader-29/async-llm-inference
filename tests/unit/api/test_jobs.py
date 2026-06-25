"""POST/GET /v1/jobs route tests — 202 / 401 / 422 / 404, all clock-free.

The app is built with a fakes container; ``LifespanManager`` drives startup and
shutdown; ``ASGITransport`` calls the app in-process. The write-path leaf
dependencies are overridden with shared in-memory instances so the test asserts
exactly what was persisted/published — no DB, no Redis, no sockets, no sleeps.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest
from asgi_lifespan import LifespanManager

from app.api.app import create_app
from app.api.dependencies import get_ingestion_service, get_repository
from app.services.ingestion import IngestionService
from tests.support.container import build_fake_container, fake_settings
from tests.support.fakes import FakeQueue, InMemoryRepository

API_KEY = "test-key-123"


@pytest.fixture
async def client_and_fakes() -> AsyncIterator[
    tuple[httpx.AsyncClient, InMemoryRepository, FakeQueue]
]:
    """An ASGITransport client over an app whose I/O ports are fakes."""
    settings = fake_settings()
    repo = InMemoryRepository()
    queue = FakeQueue()

    # A container is still needed for app.state (auth/get_settings/health paths).
    container = build_fake_container(settings)
    app = create_app(container=container)

    # Override the write-path deps with the shared in-memory instances so the
    # test can assert on what was persisted/published.
    app.dependency_overrides[get_repository] = lambda: repo
    app.dependency_overrides[get_ingestion_service] = lambda: IngestionService(
        repository=repo, queue=queue, retry_settings=settings.retry
    )

    async with LifespanManager(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            yield client, repo, queue


async def test_submit_rag_query_returns_202(
    client_and_fakes: tuple[httpx.AsyncClient, InMemoryRepository, FakeQueue],
) -> None:
    client, repo, queue = client_and_fakes
    resp = await client.post(
        "/v1/jobs",
        headers={"X-API-Key": API_KEY},
        json={"payload": {"job_type": "rag_query", "query": "what is hexagonal?"}},
    )
    assert resp.status_code == 202
    body = resp.json()
    job_id = body["job_id"]
    assert body["status"] == "pending"
    assert body["status_url"] == f"/v1/jobs/{job_id}"
    # The row was persisted PENDING and exactly one pointer was published.
    assert len(repo.store) == 1
    assert len(queue.published) == 1
    assert str(queue.published[0].id) == job_id
    assert queue.published[0].job_type.value == "rag_query"


async def test_submit_embed_document_returns_202(
    client_and_fakes: tuple[httpx.AsyncClient, InMemoryRepository, FakeQueue],
) -> None:
    client, repo, _ = client_and_fakes
    resp = await client.post(
        "/v1/jobs",
        headers={"X-API-Key": API_KEY},
        json={
            "payload": {
                "job_type": "embed_document",
                "document_id": "doc-1",
                "text": "hello world",
            }
        },
    )
    assert resp.status_code == 202
    assert len(repo.store) == 1


async def test_missing_api_key_is_401(
    client_and_fakes: tuple[httpx.AsyncClient, InMemoryRepository, FakeQueue],
) -> None:
    client, *_ = client_and_fakes
    resp = await client.post(
        "/v1/jobs",
        json={"payload": {"job_type": "rag_query", "query": "x"}},
    )
    assert resp.status_code == 401


async def test_wrong_api_key_is_401(
    client_and_fakes: tuple[httpx.AsyncClient, InMemoryRepository, FakeQueue],
) -> None:
    client, *_ = client_and_fakes
    resp = await client.post(
        "/v1/jobs",
        headers={"X-API-Key": "definitely-wrong"},
        json={"payload": {"job_type": "rag_query", "query": "x"}},
    )
    assert resp.status_code == 401


async def test_unknown_job_type_is_422(
    client_and_fakes: tuple[httpx.AsyncClient, InMemoryRepository, FakeQueue],
) -> None:
    client, *_ = client_and_fakes
    resp = await client.post(
        "/v1/jobs",
        headers={"X-API-Key": API_KEY},
        json={"payload": {"job_type": "not_a_real_type", "query": "x"}},
    )
    assert resp.status_code == 422


async def test_missing_required_field_is_422(
    client_and_fakes: tuple[httpx.AsyncClient, InMemoryRepository, FakeQueue],
) -> None:
    client, *_ = client_and_fakes
    # rag_query requires ``query``; omit it.
    resp = await client.post(
        "/v1/jobs",
        headers={"X-API-Key": API_KEY},
        json={"payload": {"job_type": "rag_query"}},
    )
    assert resp.status_code == 422


async def test_extra_field_is_rejected_422(
    client_and_fakes: tuple[httpx.AsyncClient, InMemoryRepository, FakeQueue],
) -> None:
    client, *_ = client_and_fakes
    resp = await client.post(
        "/v1/jobs",
        headers={"X-API-Key": API_KEY},
        json={"payload": {"job_type": "rag_query", "query": "x", "bogus": 1}},
    )
    assert resp.status_code == 422  # extra="forbid"


async def test_get_unknown_job_is_404(
    client_and_fakes: tuple[httpx.AsyncClient, InMemoryRepository, FakeQueue],
) -> None:
    client, *_ = client_and_fakes
    resp = await client.get(
        "/v1/jobs/00000000-0000-0000-0000-000000000000",
        headers={"X-API-Key": API_KEY},
    )
    assert resp.status_code == 404


async def test_submit_then_get_roundtrip(
    client_and_fakes: tuple[httpx.AsyncClient, InMemoryRepository, FakeQueue],
) -> None:
    client, _repo, _ = client_and_fakes
    post = await client.post(
        "/v1/jobs",
        headers={"X-API-Key": API_KEY},
        json={"payload": {"job_type": "rag_query", "query": "roundtrip"}},
    )
    job_id = post.json()["job_id"]
    got = await client.get(f"/v1/jobs/{job_id}", headers={"X-API-Key": API_KEY})
    assert got.status_code == 200
    assert got.json()["status"] == "pending"
    assert got.json()["job_type"] == "rag_query"
