"""Real-Redis broker tests. Marked ``integration``; skipped in the default run.

Run with: ``uv run poe up`` (compose Redis) then ``uv run poe test-int``.
Self-contained: builds its own settings/repo/processor so it does not depend on
the unit-tier broker conftest fixtures.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest
from redis.asyncio import Redis

from app.adapters.broker.consumer import StreamConsumer
from app.adapters.broker.keys import BrokerKeys
from app.adapters.broker.producer import StreamProducer, ensure_group
from app.core.config import BrokerSettings
from tests.unit.broker.conftest import FakeRepository, RecordingProcessor, make_job

pytestmark = pytest.mark.integration


def _settings(reclaim_idle_ms: int = 0) -> BrokerSettings:
    return BrokerSettings(
        stream="aie:jobs:it",
        group="aie-workers",
        dlq="aie:jobs:it:dlq",
        max_attempts=2,
        block_ms=0,
        reclaim_idle_ms=reclaim_idle_ms,
        worker_concurrency=8,
        max_delivery_count=3,
        maxlen=1000,
    )


@pytest.fixture
async def real_redis() -> AsyncIterator[Redis]:
    client = Redis.from_url("redis://localhost:6379/15")  # db 15 = test scratch
    await client.flushdb()
    try:
        yield client
    finally:
        await client.flushdb()
        await client.aclose()


async def test_publish_consume_ack_round_trip(real_redis: Redis) -> None:
    settings = _settings()
    keys = BrokerKeys.from_settings(settings)
    repository = FakeRepository()
    processor = RecordingProcessor()
    await ensure_group(real_redis, keys)
    producer = StreamProducer(real_redis, keys, settings)
    consumer = StreamConsumer(real_redis, keys, settings, repository, processor, producer)
    await consumer.start()

    job = make_job()
    repository.seed(job)
    await producer.publish(job)

    await consumer.consume_once()
    await asyncio.gather(*tuple(consumer._in_flight))

    assert [m.job_id for m in processor.calls] == [job.id]
    pending: Any = await real_redis.xpending(keys.stream, keys.group)
    assert pending["pending"] == 0


async def test_xautoclaim_reclaims_orphan_on_real_redis(real_redis: Redis) -> None:
    """RECLAIM CONTINGENCY TIER. Real Redis guarantees XAUTOCLAIM idle-time
    semantics. We deliver a message under consumer 'ghost', never ACK it, then a
    SECOND consumer reclaims it after the idle threshold."""
    settings = _settings(reclaim_idle_ms=50)
    keys = BrokerKeys.from_settings(settings)
    repository = FakeRepository()
    processor = RecordingProcessor()
    await ensure_group(real_redis, keys)
    producer = StreamProducer(real_redis, keys, settings)

    job = make_job()
    repository.seed(job)
    await producer.publish(job)

    # Consumer 'ghost' reads it (enters its PEL) but "crashes" without ACK.
    await real_redis.xreadgroup(
        groupname=keys.group,
        consumername="ghost",
        streams={keys.stream: ">"},
        count=10,
    )
    await asyncio.sleep(0.1)  # let it idle past the threshold (integration may use real time)

    # A fresh consumer reclaims and processes it.
    consumer = StreamConsumer(real_redis, keys, settings, repository, processor, producer)
    await consumer.start()
    await consumer.consume_once()
    await asyncio.gather(*tuple(consumer._in_flight))

    assert [m.job_id for m in processor.calls] == [job.id]
