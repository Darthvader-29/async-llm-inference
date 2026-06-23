"""Shared pytest fixtures for the whole test suite.

Phase 1 provides only configuration helpers. The domain tests need NO fixtures
(they construct ``InferenceJob`` directly). Later phases add container/db/redis
fixtures here. Everything stays deterministic and clock-free — no fixture sleeps.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from app.core.config import Environment, Settings, get_settings


@pytest.fixture
def dev_settings() -> Settings:
    """A fully-defaulted ``dev`` Settings instance.

    Because every field has a default (or a default_factory), constructing
    ``Settings()`` with an empty-ish environment is valid and exercises the
    zero-cloud redirect (dev + no endpoint → MinIO).
    """
    # _env_file=None prevents a developer's local .env from leaking into tests,
    # keeping the fixture hermetic and reproducible across machines.
    return Settings(env=Environment.DEV, _env_file=None)


@pytest.fixture(autouse=True)
def _clear_settings_cache() -> Iterator[None]:
    """Ensure ``get_settings()``'s lru_cache never bleeds between tests."""
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
