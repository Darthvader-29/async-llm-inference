"""Container teardown is leak-free and idempotent (Exit Criterion 3, unit half).

``aclose()`` shuts the executor and closes the redis/engine clients; a second
call must not raise. Fake-vs-real selection is all-fake with no keys.
"""

from __future__ import annotations

import pytest

from app.container import AppContainer
from tests.support.container import StubEngine, StubRedis, build_fake_container, fake_settings


async def test_aclose_shuts_executor_and_closes_clients() -> None:
    container = build_fake_container(fake_settings())
    executor = container.executor
    redis = container.redis
    engine = container.engine
    assert isinstance(redis, StubRedis)  # narrow to the stub for flag access
    assert isinstance(engine, StubEngine)

    await container.aclose()

    # Public-API proof the executor is shut down: scheduling new work raises.
    with pytest.raises(RuntimeError):
        executor.submit(lambda: None)
    # Stub clients flipped their flags via aclose()/dispose().
    assert redis.closed is True
    assert engine.disposed is True


async def test_aclose_is_idempotent_and_best_effort() -> None:
    container = build_fake_container(fake_settings())
    await container.aclose()
    # Second close must not raise (defensive guards in aclose()).
    await container.aclose()


async def test_aclose_completes_teardown_on_real_install_path() -> None:
    """Regression: with ``_installed_executor=True`` (the real ``create()`` path)
    ``aclose()`` must still close redis AND dispose the engine.

    The loop-default-executor cleanup must never strand the redis/engine
    teardown steps — it previously raised ``TypeError`` (``set_default_executor``
    rejects ``None`` on Python 3.11+), which escaped the ``finally`` and skipped
    both, leaking the Redis client and the DB pool.
    """
    container = build_fake_container(fake_settings(), installed_executor=True)
    redis = container.redis
    engine = container.engine
    assert isinstance(redis, StubRedis)
    assert isinstance(engine, StubEngine)

    await container.aclose()  # must NOT raise

    assert redis.closed is True
    assert engine.disposed is True
    assert container._installed_executor is False


def test_provider_modes_are_all_fake_without_keys() -> None:
    settings = fake_settings()  # no provider keys, web search disabled
    modes = AppContainer._provider_modes(settings)
    assert modes == {
        "embedding": "fake",
        "llm": "fake",
        "vector": "fake",
        "search": "fake",
    }
