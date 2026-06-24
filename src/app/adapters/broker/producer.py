"""Producer: implements the JobQueue port over a Redis Stream.

Also owns the idempotent consumer-group bootstrap (``ensure_group``), kept next
to the stream it operates on.
"""

from __future__ import annotations

from typing import Any

import structlog
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from app.adapters.broker.keys import BrokerKeys
from app.adapters.broker.messages import JobMessage
from app.core.config import BrokerSettings
from app.domain.models import InferenceJob

log = structlog.get_logger(__name__)


async def ensure_group(redis: Redis, keys: BrokerKeys) -> None:
    """Create the consumer group idempotently.

    ``XGROUP CREATE key group $ MKSTREAM``: ``id="$"`` starts the group at new
    messages; ``mkstream=True`` creates the stream if absent so we never need a
    separate "create stream" step. Redis raises ``BUSYGROUP`` if the group
    already exists — the normal outcome on every boot after the first and every
    worker beyond the first — so we swallow exactly that and re-raise anything
    else.
    """
    try:
        await redis.xgroup_create(
            name=keys.stream,
            groupname=keys.group,
            id="$",
            mkstream=True,
        )
        log.info("broker.group.created", stream=keys.stream, group=keys.group)
    except ResponseError as exc:
        if "BUSYGROUP" in str(exc):
            log.debug("broker.group.exists", stream=keys.stream, group=keys.group)
            return
        raise  # genuine error — propagate


class StreamProducer:
    """Publishes job pointers onto the work stream. Conforms structurally to the
    ``JobQueue`` Protocol (``async def publish(job) -> None``)."""

    def __init__(self, redis: Redis, keys: BrokerKeys, settings: BrokerSettings) -> None:
        self._redis = redis
        self._keys = keys
        self._maxlen = settings.maxlen  # approximate cap, e.g. 10_000

    async def publish(self, job: InferenceJob) -> None:
        """Append the first-delivery pointer for ``job`` to the stream.

        Trimmed with an *approximate* MAXLEN (``~``) so Redis can evict at
        radix-node boundaries — cheap amortized vs an exact O(n) trim on the hot
        ingestion path.
        """
        message = JobMessage.first_delivery(job)
        # Widen to dict[Any, Any]: redis-py's xadd `fields` is an *invariant*
        # union-keyed dict, so a dict[str, str] is rejected despite str being a
        # valid member. The wider local annotation satisfies the signature.
        fields: dict[Any, Any] = message.to_fields()
        message_id = await self._redis.xadd(
            name=self._keys.stream,
            fields=fields,
            maxlen=self._maxlen,
            approximate=True,  # the "~" modifier; cheap trimming
        )
        log.info(
            "broker.published",
            job_id=str(job.id),
            job_type=job.job_type.value,
            attempt=message.attempt,
            message_id=_decode_id(message_id),
            stream=self._keys.stream,
        )

    async def republish(self, message: JobMessage) -> str:
        """Re-add an *already-incremented* pointer (the retry path calls this).

        Returns the new stream message id. Separated from ``publish`` because the
        retry path already owns a :class:`JobMessage` with the right attempt and
        must NOT reset it to 1.
        """
        fields: dict[Any, Any] = message.to_fields()
        message_id = await self._redis.xadd(
            name=self._keys.stream,
            fields=fields,
            maxlen=self._maxlen,
            approximate=True,
        )
        return _decode_id(message_id)

    async def dead_letter(self, message: JobMessage, reason: str) -> str:
        """Append a poison pointer to the DLQ stream, with a failure reason.

        The DLQ is just another stream, trimmed like the main one so it cannot
        grow without bound.
        """
        fields: dict[Any, Any] = {**message.to_fields(), "reason": reason}
        message_id = await self._redis.xadd(
            name=self._keys.dlq,
            fields=fields,
            maxlen=self._maxlen,
            approximate=True,
        )
        decoded = _decode_id(message_id)
        log.warning(
            "broker.dead_lettered",
            job_id=str(message.job_id),
            attempt=message.attempt,
            reason=reason,
            dlq=self._keys.dlq,
            message_id=decoded,
        )
        return decoded


def _decode_id(message_id: object) -> str:
    """XADD returns the new id as bytes (decode_responses=False) or str."""
    return message_id.decode() if isinstance(message_id, bytes) else str(message_id)
