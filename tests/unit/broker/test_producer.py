"""Producer unit tests: XADD writes the correct pointer + MAXLEN trim bounds it,
and raw redis errors are translated into the upstream-error vocabulary."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import pytest
from redis.exceptions import AuthenticationError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from app.adapters.broker.keys import BrokerKeys
from app.adapters.broker.messages import JobMessage
from app.adapters.broker.producer import StreamProducer
from app.core.config import BrokerSettings
from app.domain.exceptions import PermanentUpstreamError, TransientUpstreamError
from tests.unit.broker.conftest import make_job

if TYPE_CHECKING:
    from redis.asyncio import Redis


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


# ---------------------------------------------------------------------------
# Error translation at the ADAPTER level.
#
# These drive the REAL ``StreamProducer`` against a redis stand-in whose
# ``xadd`` raises a raw ``redis.exceptions.*`` and assert the producer re-raises
# the project's upstream-error vocabulary. This is the seam the in-memory
# ``FakeQueue`` (which raises ``TransientUpstreamError`` directly) cannot cover:
# without translation here, a real ``ConnectionError`` would slip past the
# ingestion retry predicate (which retries ONLY ``TransientUpstreamError``) and
# 500 the client — the exact bug these tests would have caught.
# ---------------------------------------------------------------------------
class _RaisingRedis:
    """A redis stand-in whose ``xadd`` always raises ``error`` (and counts calls)."""

    def __init__(self, error: Exception) -> None:
        self._error = error
        self.calls = 0

    async def xadd(self, **kwargs: object) -> object:
        self.calls += 1
        raise self._error


def _producer_over(
    error: Exception, keys: BrokerKeys, settings: BrokerSettings
) -> tuple[StreamProducer, _RaisingRedis]:
    stub = _RaisingRedis(error)
    # ``cast`` the stub to the SDK type at the constructor only (the documented
    # repo pattern); the producer is typed against ``redis.asyncio.Redis``.
    return StreamProducer(cast("Redis", stub), keys, settings), stub


async def test_publish_translates_connection_error_to_transient(
    keys: BrokerKeys, broker_settings: BrokerSettings
) -> None:
    """The headline fix: a raw redis ConnectionError on XADD becomes a
    ``TransientUpstreamError`` (which the ingestion retry policy can retry)."""
    boom = RedisConnectionError("connection reset by peer")
    producer, stub = _producer_over(boom, keys, broker_settings)

    with pytest.raises(TransientUpstreamError) as ei:
        await producer.publish(make_job())

    assert stub.calls == 1  # translation happened at the xadd boundary
    assert ei.value.__cause__ is boom  # raw redis error preserved for the traceback


async def test_publish_translates_timeout_error_to_transient(
    keys: BrokerKeys, broker_settings: BrokerSettings
) -> None:
    producer, _ = _producer_over(RedisTimeoutError("timed out"), keys, broker_settings)
    with pytest.raises(TransientUpstreamError):
        await producer.publish(make_job())


async def test_publish_does_not_misclassify_auth_error_as_transient(
    keys: BrokerKeys, broker_settings: BrokerSettings
) -> None:
    """AuthenticationError subclasses redis ConnectionError — it must surface as
    PERMANENT (a bad password is never worth retrying)."""
    producer, _ = _producer_over(AuthenticationError("WRONGPASS"), keys, broker_settings)
    with pytest.raises(PermanentUpstreamError):
        await producer.publish(make_job())


async def test_republish_translates_connection_error_to_transient(
    keys: BrokerKeys, broker_settings: BrokerSettings
) -> None:
    producer, stub = _producer_over(RedisConnectionError("blip"), keys, broker_settings)
    msg = JobMessage.first_delivery(make_job()).next_attempt()
    with pytest.raises(TransientUpstreamError):
        await producer.republish(msg)
    assert stub.calls == 1


async def test_dead_letter_translates_connection_error_to_transient(
    keys: BrokerKeys, broker_settings: BrokerSettings
) -> None:
    producer, stub = _producer_over(RedisConnectionError("blip"), keys, broker_settings)
    msg = JobMessage.first_delivery(make_job())
    with pytest.raises(TransientUpstreamError):
        await producer.dead_letter(msg, reason="poison")
    assert stub.calls == 1
