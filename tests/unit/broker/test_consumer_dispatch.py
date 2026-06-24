"""Dispatch + backpressure: consume_once dispatches, ACKs on success, and never
reads past capacity (the in-flight set IS the budget)."""

from __future__ import annotations

import asyncio

from fakeredis.aioredis import FakeRedis

from app.adapters.broker.consumer import StreamConsumer
from app.adapters.broker.keys import BrokerKeys
from app.adapters.broker.messages import JobMessage
from app.adapters.broker.producer import StreamProducer
from app.core.config import BrokerSettings
from tests.unit.broker.conftest import FakeRepository, RecordingProcessor, make_job


async def test_consume_once_dispatches_and_acks_on_success(
    redis: FakeRedis,
    keys: BrokerKeys,
    consumer: StreamConsumer,
    producer: StreamProducer,
    repository: FakeRepository,
    processor: RecordingProcessor,
) -> None:
    job = make_job()
    repository.seed(job)  # PENDING row exists (idempotency guard reads it)
    await producer.publish(job)

    n = await consumer.consume_once()  # one pass; default block (0) = no wait
    assert n == 1  # one NEW message read

    # Dispatch is a tracked task; let it run to completion deterministically.
    await asyncio.gather(*tuple(consumer._in_flight))

    # Processor saw exactly this job...
    assert [m.job_id for m in processor.calls] == [job.id]
    # ...and a successful run ACKed it -> PEL empty -> nothing pending.
    pending = await redis.xpending(keys.stream, keys.group)
    assert pending["pending"] == 0


async def test_consume_once_empty_stream_returns_zero(consumer: StreamConsumer) -> None:
    assert await consumer.consume_once() == 0


async def test_backpressure_limits_reads_to_capacity(
    redis: FakeRedis,
    keys: BrokerKeys,
    broker_settings: BrokerSettings,
    repository: FakeRepository,
    producer: StreamProducer,
) -> None:
    """worker_concurrency=1 -> at most 1 in-flight -> consume_once reads 1 even
    when 3 are queued. Pure capacity arithmetic, no timing."""
    settings = broker_settings.model_copy(update={"worker_concurrency": 1})

    # A processor that blocks until released, so the in-flight slot stays taken.
    release = asyncio.Event()

    async def block_until_released(_msg: JobMessage) -> None:
        await release.wait()

    for _ in range(3):
        j = make_job()
        repository.seed(j)
        await producer.publish(j)

    c = StreamConsumer(redis, keys, settings, repository, block_until_released, producer)
    await c.start()

    first = await c.consume_once()  # reads 1 (capacity=1), dispatches it
    assert first == 1
    assert c.in_flight_count == 1

    second = await c.consume_once()  # capacity now 0 -> reads nothing
    assert second == 0
    assert c.in_flight_count == 1  # still just the one

    release.set()  # let the in-flight task finish
    await asyncio.gather(*tuple(c._in_flight))
    assert c.in_flight_count == 0
