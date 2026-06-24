"""Reclaim routing, decoupled from fakeredis idle-clock fidelity.

We stub the XPENDING call ``_handle_reclaimed`` makes, so the over-delivery ->
DLQ decision (and its complement) is verified deterministically regardless of
fakeredis' XAUTOCLAIM / XPENDING idle accounting. (Real idle-time reclaim lives
in the integration tier.)
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from fakeredis.aioredis import FakeRedis

from app.adapters.broker.consumer import StreamConsumer
from app.adapters.broker.keys import BrokerKeys
from app.adapters.broker.messages import JobMessage
from tests.unit.broker.conftest import FakeRepository, RecordingProcessor, make_job


async def test_overdelivered_reclaim_routes_to_dlq(
    consumer: StreamConsumer,
    keys: BrokerKeys,
    redis: FakeRedis,
    processor: RecordingProcessor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = make_job()
    msg = JobMessage.first_delivery(job)
    fake_id = "1718000000000-0"

    # Force XPENDING to report a delivery count beyond max_delivery_count (3).
    async def fake_pending_range(**_kwargs: object) -> list[dict[str, Any]]:
        return [
            {
                "message_id": fake_id,
                "consumer": "ghost",
                "time_since_delivered": 99999,
                "times_delivered": 9,
            }
        ]

    monkeypatch.setattr(consumer._redis, "xpending_range", fake_pending_range)

    await consumer._handle_reclaimed(fake_id, msg.to_fields())

    # Poison message routed to the DLQ; the processor never ran for it.
    assert await redis.xlen(keys.dlq) == 1
    assert processor.calls == []


async def test_under_delivered_reclaim_dispatches(
    consumer: StreamConsumer,
    keys: BrokerKeys,
    redis: FakeRedis,
    repository: FakeRepository,
    processor: RecordingProcessor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A reclaimed message still within the delivery ceiling is dispatched, not
    DLQ'd — the complementary branch of the over-delivery guard."""
    job = make_job()
    repository.seed(job)  # PENDING row so the dispatched task's guard proceeds
    msg = JobMessage.first_delivery(job)
    fake_id = "1718000000001-0"

    async def fake_pending_range(**_kwargs: object) -> list[dict[str, Any]]:
        return [{"times_delivered": 1}]  # within the ceiling

    monkeypatch.setattr(consumer._redis, "xpending_range", fake_pending_range)

    await consumer._handle_reclaimed(fake_id, msg.to_fields())
    await asyncio.gather(*tuple(consumer._in_flight))

    assert await redis.xlen(keys.dlq) == 0  # not DLQ'd
    assert [m.job_id for m in processor.calls] == [job.id]  # dispatched


async def test_in_flight_message_is_not_self_reclaimed(
    consumer: StreamConsumer,
    keys: BrokerKeys,
    redis: FakeRedis,
    processor: RecordingProcessor,
) -> None:
    """Self-reclaim guard: XAUTOCLAIM can re-claim an entry THIS consumer is still
    processing (a job slower than reclaim_idle_ms). _handle_reclaimed must skip it
    — no double-dispatch, no DLQ — because the original task still owns it."""
    job = make_job()
    mid = "1718000000002-0"
    consumer._in_flight_ids.add(mid)  # as _dispatch would, for the live task

    await consumer._handle_reclaimed(mid, JobMessage.first_delivery(job).to_fields())

    assert consumer.in_flight_count == 0  # NOT re-dispatched
    assert await redis.xlen(keys.dlq) == 0  # NOT dead-lettered
    assert processor.calls == []
