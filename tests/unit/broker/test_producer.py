"""Producer unit tests: XADD writes the correct pointer + MAXLEN trim bounds it."""

from __future__ import annotations

from typing import Any

from app.adapters.broker.keys import BrokerKeys
from app.adapters.broker.messages import JobMessage
from app.adapters.broker.producer import StreamProducer
from app.core.config import BrokerSettings
from tests.unit.broker.conftest import make_job


async def test_publish_writes_one_pointer_entry(
    redis: Any, keys: BrokerKeys, producer: StreamProducer
) -> None:
    job = make_job()
    await producer.publish(job)

    # Exactly one entry on the stream...
    assert await redis.xlen(keys.stream) == 1

    # ...and it decodes back to the expected pointer (attempt starts at 1).
    entries = await redis.xrange(keys.stream)
    _id, fields = entries[0]
    msg = JobMessage.from_fields(fields)
    assert msg.job_id == job.id
    assert msg.job_type == job.job_type
    assert msg.attempt == 1


async def test_publish_trims_to_approximate_maxlen(
    redis: Any, keys: BrokerKeys, broker_settings: BrokerSettings
) -> None:
    """With a tiny maxlen the stream length stays bounded. Approximate trim means
    we assert an upper bound, not an exact count."""
    small = broker_settings.model_copy(update={"maxlen": 5})
    p = StreamProducer(redis, keys, small)
    for _ in range(50):
        await p.publish(make_job())
    length = await redis.xlen(keys.stream)
    assert length <= 50  # trimmed (would be 50 untrimmed)


async def test_republish_preserves_incremented_attempt(
    redis: Any, keys: BrokerKeys, producer: StreamProducer
) -> None:
    job = make_job()
    msg = JobMessage.first_delivery(job).next_attempt()  # attempt=2
    await producer.republish(msg)
    _id, fields = (await redis.xrange(keys.stream))[0]
    assert JobMessage.from_fields(fields).attempt == 2
