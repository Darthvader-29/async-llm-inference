"""Integration half of Exit Criterion 3: the engine pool returns to zero.

Needs real Postgres + Redis + MinIO (the ``integration`` marker gates it). After
serving a request that checks out a connection (readiness ``SELECT 1``), and
again after the lifespan exits (``aclose`` -> ``engine.dispose``), no connection
is left checked out.
"""

from __future__ import annotations

import httpx
import pytest
from asgi_lifespan import LifespanManager

from app.api.app import create_app
from app.core.config import Settings

pytestmark = pytest.mark.integration  # needs a real Postgres + Redis + MinIO


async def test_pool_checkedout_is_zero_after_lifespan() -> None:
    settings = Settings()  # reads AIE_* from the CI/integration env

    app = create_app(settings)  # real container created by the lifespan
    async with LifespanManager(app):
        engine = app.state.container.engine
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            # Exercise a path that checks out a connection (readiness SELECT 1).
            r = await client.get("/health/ready")
            assert r.status_code == 200
        # Serving finished; the session-per-operation ``async with`` returned the
        # connection. The live pool must be fully checked in.
        assert engine.pool.checkedout() == 0

    # After LifespanManager exit, aclose() ran -> engine disposed; checkedout
    # stays 0 (a disposed engine lazily builds a fresh, empty pool on next use).
    assert engine.pool.checkedout() == 0
