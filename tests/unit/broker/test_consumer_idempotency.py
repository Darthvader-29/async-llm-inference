"""Idempotency guard: a redelivered pointer whose row is already terminal must be
ACKed and skipped — the processor never runs twice (at-least-once made safe)."""

from __future__ import annotations

import asyncio

from fakeredis.aioredis import FakeRedis

from app.adapters.broker.consumer import StreamConsumer
from app.adapters.broker.keys import BrokerKeys
from app.adapters.broker.producer import StreamProducer
from tests.unit.broker.conftest import FakeRepository, RecordingProcessor, make_job


async def test_terminal_row_is_acked_and_skipped(
    redis: FakeRedis,
    keys: BrokerKeys,
    consumer: StreamConsumer,
    producer: StreamProducer,
    repository: FakeRepository,
    processor: RecordingProcessor,
) -> None:
    job = make_job()
    job.mark_running()
    job.mark_success(result_ref="s3://bucket/result.json", duration_ms=5)  # terminal
    repository.seed(job)
    await producer.publish(job)  # simulate a redelivered pointer

    await consumer.consume_once()
    await asyncio.gather(*tuple(consumer._in_flight))

    # Processor never ran (guard short-circuited)...
    assert processor.calls == []
    # ...but the message was still ACKed (removed from PEL) so it won't loop.
    assert (await redis.xpending(keys.stream, keys.group))["pending"] == 0


async def test_unseeded_ghost_row_is_acked_and_skipped(
    redis: FakeRedis,
    keys: BrokerKeys,
    consumer: StreamConsumer,
    producer: StreamProducer,
    processor: RecordingProcessor,
) -> None:
    """A pointer whose row does NOT exist (a 'ghost') is treated as terminal:
    the idempotency guard's JobNotFound -> return True branch ACK-and-skips it so
    a vanished row is never reprocessed."""
    job = make_job()  # deliberately NOT seeded into the repository
    await producer.publish(job)

    await consumer.consume_once()
    await asyncio.gather(*tuple(consumer._in_flight))

    assert processor.calls == []  # processor never ran for a ghost
    assert (await redis.xpending(keys.stream, keys.group))["pending"] == 0  # still ACKed
