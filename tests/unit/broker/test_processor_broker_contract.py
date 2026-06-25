"""The real JobProcessor + StreamConsumer retry/DLQ contract (no infra).

This guards the Phase 7 cross-phase decision: the processor leaves a failed row
RUNNING and re-raises, so the *broker* owns the terminal transition —
``requeue()`` (RUNNING->PENDING) on a transient retry and ``mark_failed()``
(RUNNING->FAILED) on DLQ. Both require the row to still be RUNNING. If the
processor marked the row terminal itself (as an earlier draft did), these broker
transitions would raise ``InvalidTransition`` and these tests would fail.

Deterministic: fakeredis + in-memory fakes, ``base_delay``-free, no clocks.
"""

from __future__ import annotations

import asyncio
from typing import Any

from fakeredis.aioredis import FakeRedis

from app.adapters.broker.consumer import StreamConsumer
from app.adapters.broker.keys import BrokerKeys
from app.adapters.broker.messages import JobMessage
from app.adapters.broker.producer import StreamProducer, ensure_group
from app.core.config import BrokerSettings
from app.domain.exceptions import PermanentUpstreamError, TransientUpstreamError
from app.domain.models import InferenceJob, JobStatus, JobType
from app.ports.providers import SearchResult
from app.services.pipelines import PipelineContext
from app.services.processor import JobProcessor
from tests.support.recording_providers import (
    CallLog,
    RecordingEmbedding,
    RecordingObjectStore,
    RecordingSearch,
    RecordingVectorStore,
)
from tests.unit.broker.conftest import FakeRepository


def _settings() -> BrokerSettings:
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


class RecordingLLMOK:
    async def complete(self, prompt: str, *, max_new_tokens: int) -> str:
        return "ok"


async def _wire(
    redis: FakeRedis, repo: FakeRepository, ctx: PipelineContext
) -> tuple[StreamConsumer, StreamProducer, BrokerKeys]:
    settings = _settings()
    keys = BrokerKeys.from_settings(settings)
    await ensure_group(redis, keys)
    producer = StreamProducer(redis, keys, settings)
    consumer = StreamConsumer(redis, keys, settings, repo, JobProcessor(repo, ctx), producer)
    await consumer.start()
    return consumer, producer, keys


async def test_transient_failure_via_real_processor_requeues_row_to_pending() -> None:
    redis: Any = FakeRedis()  # Any: fakeredis is a structural redis stand-in
    log = CallLog()

    class BoomLLM:  # transient at the LLM step (the pipeline's last port)
        async def complete(self, prompt: str, *, max_new_tokens: int) -> str:
            raise TransientUpstreamError("upstream 503")

    ctx = PipelineContext(
        search=RecordingSearch(log),
        embedding=RecordingEmbedding(log),
        vector_store=RecordingVectorStore(log),
        llm=BoomLLM(),
        object_store=RecordingObjectStore(log),
    )
    repo = FakeRepository()
    job = InferenceJob.new(JobType.RAG_QUERY, {"query": "hi", "top_k": 1})
    repo.seed(job)

    consumer, producer, keys = await _wire(redis, repo, ctx)
    try:
        await producer.publish(job)  # attempt=1
        await consumer.consume_once()
        await asyncio.gather(*tuple(consumer._in_flight))

        # The broker requeued the row (RUNNING->PENDING) — only possible because
        # the processor left it RUNNING after re-raising the transient error.
        assert (await repo.get(job.id)).status is JobStatus.PENDING
        # ...and a fresh attempt=2 entry is on the stream.
        entries = await redis.xrange(keys.stream)
        assert 2 in {JobMessage.from_fields(f).attempt for _id, f in entries}
    finally:
        await redis.aclose()


async def test_permanent_failure_via_real_processor_dlqs_and_marks_failed() -> None:
    redis: Any = FakeRedis()  # Any: fakeredis is a structural redis stand-in
    log = CallLog()

    class BoomSearch:  # permanent at the very first port
        async def search(self, query: str, *, max_results: int) -> list[SearchResult]:
            raise PermanentUpstreamError("400 invalid input")

    ctx = PipelineContext(
        search=BoomSearch(),
        embedding=RecordingEmbedding(log),
        vector_store=RecordingVectorStore(log),
        llm=RecordingLLMOK(),
        object_store=RecordingObjectStore(log),
    )
    repo = FakeRepository()
    job = InferenceJob.new(JobType.RAG_QUERY, {"query": "hi", "top_k": 1})
    repo.seed(job)

    consumer, producer, keys = await _wire(redis, repo, ctx)
    try:
        await producer.publish(job)  # attempt=1
        await consumer.consume_once()
        await asyncio.gather(*tuple(consumer._in_flight))

        # The broker DLQ'd it and marked the row FAILED (RUNNING->FAILED) — only
        # possible because the processor left it RUNNING after re-raising.
        assert (await repo.get(job.id)).status is JobStatus.FAILED
        assert await redis.xlen(keys.dlq) == 1
    finally:
        await redis.aclose()
