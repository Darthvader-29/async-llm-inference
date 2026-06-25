"""End-to-end: produce a pointer, run ONE consume iteration, assert terminal.

Marked ``integration`` because it builds a real ``AppContainer`` (real Postgres,
Redis, MinIO). It exercises the SAME container-built processor the production
worker uses. Still clock-free: we drive a single ``consume_once()`` + ``drain()``
rather than spinning the loop or sleeping.

Run with infra up + migrated:
    uv run poe up
    uv run poe migrate
    uv run poe test-int
"""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator
from typing import cast

import pytest

from app.adapters.broker.consumer import StreamConsumer
from app.adapters.broker.keys import BrokerKeys
from app.adapters.broker.producer import StreamProducer
from app.container import AppContainer
from app.core.config import Settings
from app.domain.models import InferenceJob, JobStatus, JobType
from app.services.pipelines import PipelineContext
from app.services.processor import JobProcessor

pytestmark = pytest.mark.integration


@pytest.fixture
async def worker_container() -> AsyncIterator[AppContainer]:
    """A real, container-built engine wired exactly as the production worker.

    ``Settings()`` reads the integration ``AIE_*`` env; with no provider keys
    the bundle is all-fakes, so the pipeline runs zero-cloud while still using
    real Redis/Postgres/MinIO for the queue, the row, and the artifact.
    """
    settings = Settings()
    container = await AppContainer.create(settings)
    try:
        yield container
    finally:
        await container.aclose()


async def test_enqueue_then_consume_reaches_success(worker_container: AppContainer) -> None:
    container = worker_container
    settings = container.settings
    keys = BrokerKeys.from_settings(settings.broker)

    # Clean slate: deleting the stream also drops the group, so we recreate it
    # below via consumer.start() BEFORE publishing — XREADGROUP '>' only delivers
    # entries added after the group exists.
    await container.redis.delete(keys.stream)

    # Build the SAME processor the worker builds, bind it into a consumer.
    ctx = PipelineContext(
        search=container.providers.search,
        embedding=container.providers.embedding,
        vector_store=container.providers.vector_store,
        llm=container.providers.llm,
        object_store=container.object_store,
    )
    consumer = StreamConsumer(
        redis=container.redis,
        keys=keys,
        settings=settings.broker,
        repository=container.repository,
        processor=JobProcessor(repository=container.repository, ctx=ctx),
        producer=cast(StreamProducer, container.queue),
    )
    await consumer.start()  # group now exists

    # 1. Insert a PENDING row through the repository (as the API would).
    job = InferenceJob(
        id=uuid.uuid4(),
        job_type=JobType.RAG_QUERY,
        status=JobStatus.PENDING,
        payload={"query": "integration question", "top_k": 2},
    )
    await container.repository.add(job)

    # 2. Publish the pointer onto the real stream (the JobQueue port == producer).
    await container.queue.publish(job)

    # 3. Process exactly one delivery, then drain the spawned task deterministically.
    await consumer.consume_once()
    await consumer.drain()

    # 4. Assert the authoritative row is terminal and the artifact exists.
    stored = await container.repository.get(job.id)
    assert stored.status is JobStatus.SUCCESS
    expected_key = f"results/rag_query/{job.id}.json"
    assert stored.result_ref == f"s3://{settings.object_store.bucket}/{expected_key}"

    body = await container.object_store.get_bytes(expected_key)
    assert json.loads(body)["query"] == "integration question"
