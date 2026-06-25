"""Liveness is always 200; readiness is 200 with healthy fakes, 503 when not.

No infra: ``StubRedis.ping``, ``FakeObjectStore.bucket_exists`` and the fake
session's ``execute`` all succeed under ``ready=True`` and fail under
``ready=False`` — so both branches are exercised deterministically.
"""

from __future__ import annotations

import httpx
from asgi_lifespan import LifespanManager

from app.api.app import create_app
from tests.support.container import build_fake_container, fake_settings


async def test_liveness_always_ok() -> None:
    app = create_app(container=build_fake_container(fake_settings()))
    async with (
        LifespanManager(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c,
    ):
        resp = await c.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


async def test_readiness_ok_with_fakes() -> None:
    app = create_app(container=build_fake_container(fake_settings()))
    async with (
        LifespanManager(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c,
    ):
        resp = await c.get("/health/ready")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ready"
    assert {ch["name"] for ch in body["checks"]} == {"postgres", "redis", "object_store"}
    assert all(ch["ok"] for ch in body["checks"])


async def test_readiness_503_when_dependencies_down() -> None:
    app = create_app(container=build_fake_container(fake_settings(), ready=False))
    async with (
        LifespanManager(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c,
    ):
        resp = await c.get("/health/ready")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "not_ready"
    failed = {ch["name"] for ch in body["checks"] if not ch["ok"]}
    # The DB SELECT 1 and the redis PING both fail; the fake object store is up.
    assert {"postgres", "redis"} <= failed
