"""Drive run_worker with a pre-set stop Event to prove a clean exit + drain.

No signals, no subprocess, no sleep. We build an all-fakes container (the same
``build_fake_container`` the API leak test uses) but swap in a ``fakeredis``
client so ``consumer.start()``'s ``XGROUP CREATE`` works in-process. With the
stop Event pre-set, ``run_worker`` must run zero loop iterations, drain (nothing
in flight), and return — deterministically.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest
from fakeredis.aioredis import FakeRedis

from app.container import AppContainer
from app.worker.runner import run_worker
from tests.support.container import build_fake_container, fake_settings


@pytest.fixture
async def all_fakes_container() -> AsyncIterator[AppContainer]:
    container = build_fake_container(fake_settings())
    # The default stub redis only implements ping/aclose; the consumer needs a
    # client that supports XGROUP CREATE. fakeredis is the in-process stand-in.
    container.redis = FakeRedis()
    try:
        yield container
    finally:
        await container.aclose()


async def test_run_worker_exits_when_stop_preset(all_fakes_container: AppContainer) -> None:
    container = all_fakes_container
    stop = asyncio.Event()
    stop.set()  # already requested -> loop body should not run; drain + return

    # Should return promptly without hanging and without raising. The wait_for is
    # a hang *watchdog*, NOT a timing assertion — run_worker exits because
    # stop.is_set() is True, deterministically, well under the timeout.
    await asyncio.wait_for(run_worker(container, container.settings, stop), timeout=5.0)
