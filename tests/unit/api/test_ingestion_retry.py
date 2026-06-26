"""Publish-retry is clock-free: attempts are COUNTED, never timed.

``fail_times`` drives how many transient publish failures occur; ``max_delay_s=0``
(via ``fake_settings``) caps every backoff to zero so the loop never sleeps.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from app.adapters.broker.keys import BrokerKeys
from app.adapters.broker.producer import StreamProducer
from app.core.config import BrokerSettings
from app.domain.exceptions import TransientUpstreamError
from app.domain.models import JobStatus, JobType
from app.services.ingestion import IngestionService
from tests.support.container import fake_settings
from tests.support.fakes import FakeQueue, InMemoryRepository

if TYPE_CHECKING:
    from redis.asyncio import Redis


async def test_publish_retries_then_succeeds_without_sleeping() -> None:
    settings = fake_settings()  # max_attempts=3, base/max delay 0
    repo = InMemoryRepository()
    queue = FakeQueue(fail_times=2)  # fail twice, succeed on the 3rd attempt
    svc = IngestionService(repo, queue, settings.retry)

    job_id = await svc.submit(JobType.RAG_QUERY, {"job_type": "rag_query", "query": "x"})

    # Row persisted once; publish ultimately succeeded after 2 transient fails.
    assert job_id in repo.store
    assert len(queue.published) == 1


async def test_publish_exhausts_attempts_and_reraises() -> None:
    settings = fake_settings()  # max_attempts=3
    repo = InMemoryRepository()
    queue = FakeQueue(fail_times=99)  # always fails
    svc = IngestionService(repo, queue, settings.retry)

    with pytest.raises(TransientUpstreamError):
        await svc.submit(JobType.RAG_QUERY, {"job_type": "rag_query", "query": "x"})

    # The row was still written PENDING before publish failed (source of truth).
    assert len(repo.store) == 1
    assert len(queue.published) == 0


class _FlakyRedis:
    """A redis stand-in whose ``xadd`` raises ConnectionError the first
    ``fail_times`` calls, then records the write. Wired under a REAL
    ``StreamProducer`` so the adapter's error translation and the ingestion retry
    loop are exercised together — no ``FakeQueue`` shortcut that pre-raises a
    domain error."""

    def __init__(self, fail_times: int) -> None:
        self._fail_times = fail_times
        self.added: list[dict[str, object]] = []

    async def xadd(self, **kwargs: object) -> bytes:
        if self._fail_times > 0:
            self._fail_times -= 1
            raise RedisConnectionError("transient blip")
        self.added.append(kwargs)
        return b"1-0"


async def test_submit_retries_through_real_producer_error_translation() -> None:
    """End-to-end proof of the fix: a raw redis ConnectionError raised by the
    real ``StreamProducer`` is translated to ``TransientUpstreamError`` and thus
    retried by ``submit`` — counted (base_delay_s=0), never timed."""
    settings = fake_settings()  # max_attempts=3, delays 0
    repo = InMemoryRepository()
    flaky = _FlakyRedis(fail_times=2)  # 2 transient blips, succeed on the 3rd try
    keys = BrokerKeys(stream="aie:jobs", group="aie-workers", dlq="aie:jobs:dlq")
    producer = StreamProducer(cast("Redis", flaky), keys, BrokerSettings())
    svc = IngestionService(repo, producer, settings.retry)

    job_id = await svc.submit(JobType.RAG_QUERY, {"job_type": "rag_query", "query": "x"})

    assert job_id in repo.store  # row persisted
    assert len(flaky.added) == 1  # published exactly once, after two retried blips


async def test_orphan_row_left_pending_when_publish_exhausts() -> None:
    """Documented orphaned-PENDING decision (see ``IngestionService.submit``): on
    terminal publish failure the row is DELIBERATELY left PENDING — NOT marked
    FAILED (the domain has no PENDING->FAILED edge, and the client never received
    the job_id to poll). The orphan is inert: no stream pointer exists, so no
    worker will ever touch it. Reaping is out of scope (no reaper subsystem)."""
    settings = fake_settings()
    repo = InMemoryRepository()
    queue = FakeQueue(fail_times=99)  # always fails -> retries exhausted
    svc = IngestionService(repo, queue, settings.retry)

    with pytest.raises(TransientUpstreamError):
        await svc.submit(JobType.RAG_QUERY, {"job_type": "rag_query", "query": "x"})

    # Exactly one row, still PENDING (and no error recorded — never ran).
    (job,) = repo.store.values()
    assert job.status is JobStatus.PENDING
    assert job.error is None
