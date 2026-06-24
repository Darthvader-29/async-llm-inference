"""Retry & DLQ routing: transient+room -> requeue; exhausted/permanent -> DLQ+FAILED."""

from __future__ import annotations

import asyncio
from typing import Any

from app.adapters.broker.consumer import StreamConsumer
from app.adapters.broker.keys import BrokerKeys
from app.adapters.broker.messages import JobMessage
from app.adapters.broker.producer import StreamProducer
from app.core.config import BrokerSettings
from app.domain.exceptions import PermanentUpstreamError, TransientUpstreamError
from app.domain.models import JobStatus
from tests.unit.broker.conftest import FakeRepository, RecordingProcessor, make_job


async def _run_one(consumer: StreamConsumer) -> None:
    await consumer.consume_once()
    await asyncio.gather(*tuple(consumer._in_flight))


async def test_transient_with_attempts_left_requeues(
    redis: Any,
    keys: BrokerKeys,
    consumer: StreamConsumer,
    producer: StreamProducer,
    repository: FakeRepository,
    processor: RecordingProcessor,
) -> None:
    job = make_job()
    job.mark_running()  # simulate it had started
    repository.seed(job)
    await producer.publish(job)  # attempt=1

    def transient(_m: JobMessage) -> None:
        raise TransientUpstreamError("503 from upstream")

    processor.behavior[job.id] = transient

    await _run_one(consumer)

    # A FRESH entry with attempt=2 is on the stream (the old delivery was ACKed)...
    entries = await redis.xrange(keys.stream)
    attempts = sorted(JobMessage.from_fields(f).attempt for _i, f in entries)
    assert 2 in attempts  # retry entry present
    # ...the OLD attempt=1 delivery was ACKed (NOT left in the PEL for reclaim) —
    # the fresh attempt=2 entry has not been delivered yet, so nothing is pending.
    assert (await redis.xpending(keys.stream, keys.group))["pending"] == 0
    # ...and the row was reset to PENDING for the retry.
    assert (await repository.get(job.id)).status == JobStatus.PENDING


async def test_transient_attempts_exhausted_goes_to_dlq(
    redis: Any,
    keys: BrokerKeys,
    broker_settings: BrokerSettings,
    repository: FakeRepository,
    processor: RecordingProcessor,
    producer: StreamProducer,
) -> None:
    """max_attempts=2 and the message is already attempt=2 -> transient failure
    routes to DLQ + FAILED, NOT another retry."""
    job = make_job()
    job.mark_running()
    repository.seed(job)
    # publish directly at attempt=2 (the last allowed attempt)
    await producer.republish(JobMessage(job.id, job.job_type, attempt=2))

    def transient(_m: JobMessage) -> None:
        raise TransientUpstreamError("still failing")

    processor.behavior[job.id] = transient

    c = StreamConsumer(redis, keys, broker_settings, repository, processor, producer)
    await c.start()
    await c.consume_once()
    await asyncio.gather(*tuple(c._in_flight))

    # Exactly one DLQ entry, carrying the job + a reason field...
    assert await redis.xlen(keys.dlq) == 1
    dlq_entries = await redis.xrange(keys.dlq)
    _id, fields = dlq_entries[0]
    assert JobMessage.from_fields(fields).job_id == job.id
    assert "reason" in {k.decode() if isinstance(k, bytes) else k for k in fields}
    # ...and the row is terminally FAILED.
    assert (await repository.get(job.id)).status == JobStatus.FAILED


async def test_permanent_failure_goes_straight_to_dlq(
    redis: Any,
    keys: BrokerKeys,
    consumer: StreamConsumer,
    producer: StreamProducer,
    repository: FakeRepository,
    processor: RecordingProcessor,
) -> None:
    job = make_job()
    job.mark_running()
    repository.seed(job)
    await producer.publish(job)  # attempt=1, but permanent -> no retry

    def permanent(_m: JobMessage) -> None:
        raise PermanentUpstreamError("400 invalid input")

    processor.behavior[job.id] = permanent

    await _run_one(consumer)

    assert await redis.xlen(keys.dlq) == 1
    assert (await repository.get(job.id)).status == JobStatus.FAILED
    # main stream PEL is clear (original was ACKed during DLQ routing)
    assert (await redis.xpending(keys.stream, keys.group))["pending"] == 0
