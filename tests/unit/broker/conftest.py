"""Deterministic broker test fixtures — fakeredis + in-memory fakes only.

Every unit test runs against ``fakeredis.aioredis`` (no Docker) and drives the
``consume_once()`` seam. The fakes (``FakeRepository``, ``RecordingProcessor``)
structurally satisfy the ``JobRepository`` / ``JobProcessor`` Protocols — no
inheritance, checked at the injection site by mypy --strict.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from uuid import UUID

import pytest
from fakeredis.aioredis import FakeRedis

from app.adapters.broker.consumer import StreamConsumer
from app.adapters.broker.keys import BrokerKeys
from app.adapters.broker.messages import JobMessage
from app.adapters.broker.producer import StreamProducer, ensure_group
from app.core.config import BrokerSettings
from app.domain.exceptions import JobNotFound
from app.domain.models import InferenceJob, JobType


@pytest.fixture
def keys() -> BrokerKeys:
    return BrokerKeys(stream="aie:jobs", group="aie-workers", dlq="aie:jobs:dlq")


@pytest.fixture
def broker_settings() -> BrokerSettings:
    # Clock-free knobs: never block, immediately reclaimable, small attempt cap.
    return BrokerSettings(
        stream="aie:jobs",
        group="aie-workers",
        dlq="aie:jobs:dlq",
        max_attempts=2,
        block_ms=0,
        reclaim_idle_ms=0,
        worker_concurrency=8,
        max_delivery_count=3,
        maxlen=1000,
    )


@pytest.fixture
async def redis() -> AsyncIterator[FakeRedis]:
    """A fresh in-memory async Redis per test. decode_responses=False mirrors the
    production default; our (de)serialization tolerates bytes either way."""
    client = FakeRedis()
    try:
        yield client
    finally:
        await client.flushall()
        await client.aclose()


class FakeRepository:
    """In-memory JobRepository conforming to the port — deterministic, no DB."""

    def __init__(self) -> None:
        self._rows: dict[UUID, InferenceJob] = {}

    def seed(self, job: InferenceJob) -> InferenceJob:
        self._rows[job.id] = job
        return job

    async def add(self, job: InferenceJob) -> None:
        self._rows[job.id] = job

    async def get(self, job_id: UUID) -> InferenceJob:
        try:
            return self._rows[job_id]  # raises JobNotFound, mirroring the real repo
        except KeyError:
            raise JobNotFound(job_id) from None

    async def update(self, job: InferenceJob) -> None:
        self._rows[job.id] = job


class RecordingProcessor:
    """Records every dispatched message and lets a test choose the outcome.

    ``behavior`` maps job_id -> a callable(message) that may raise the exception
    the test wants (Transient/Permanent) or return None for success; an async
    callable is awaited. Default (no entry): success.
    """

    def __init__(self) -> None:
        self.calls: list[JobMessage] = []
        self.behavior: dict[UUID, Callable[[JobMessage], object]] = {}
        # An Event a test can await to know a job finished — used by the shutdown
        # test to gate the drain WITHOUT sleeping.
        self.processed = asyncio.Event()

    async def __call__(self, message: JobMessage) -> None:
        self.calls.append(message)
        action = self.behavior.get(message.job_id)
        try:
            if action is not None:
                result = action(message)
                if asyncio.iscoroutine(result):
                    await result
        finally:
            self.processed.set()


@pytest.fixture
def repository() -> FakeRepository:
    return FakeRepository()


@pytest.fixture
def processor() -> RecordingProcessor:
    return RecordingProcessor()


@pytest.fixture
async def producer(
    redis: FakeRedis, keys: BrokerKeys, broker_settings: BrokerSettings
) -> StreamProducer:
    await ensure_group(redis, keys)  # create the group up front
    return StreamProducer(redis, keys, broker_settings)


@pytest.fixture
async def consumer(
    redis: FakeRedis,
    keys: BrokerKeys,
    broker_settings: BrokerSettings,
    repository: FakeRepository,
    processor: RecordingProcessor,
    producer: StreamProducer,
) -> StreamConsumer:
    c = StreamConsumer(
        redis=redis,
        keys=keys,
        settings=broker_settings,
        repository=repository,
        processor=processor,
        producer=producer,
    )
    await c.start()
    return c


def make_job(job_type: JobType = JobType.RAG_QUERY) -> InferenceJob:
    """A PENDING job seeded into the repo by tests (fresh UUID via the factory)."""
    return InferenceJob.new(job_type=job_type, payload={"q": "hi"})
