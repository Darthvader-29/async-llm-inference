"""Shared pytest fixtures for the whole test suite.

Phase 1 provides only configuration helpers. The domain tests need NO fixtures
(they construct ``InferenceJob`` directly). Later phases add container/db/redis
fixtures here. Everything stays deterministic and clock-free — no fixture sleeps.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from app.core.config import Environment, RetrySettings, Settings, get_settings
from tests.support.offloader import RecordingOffloader


@pytest.fixture
def recording_offloader() -> RecordingOffloader:
    """A fresh recording spy per test (no shared state across tests)."""
    return RecordingOffloader()


@pytest.fixture
def retry_settings() -> RetrySettings:
    """Zero-delay retry config so attempt-counting tests run instantly.

    ``base_delay_s=0`` and ``jitter_s=0`` make ``wait_exponential_jitter``
    collapse to ~0s — we never sleep meaningfully, yet exercise the real wait
    object. ``max_attempts=3`` is the count tests assert against.
    """
    return RetrySettings(
        max_attempts=3,
        base_delay_s=0.0,
        max_delay_s=0.0,
        exp_base=2.0,
        jitter_s=0.0,
    )


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
