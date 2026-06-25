"""Unit tests for JobProcessor with fakes. Clock-free, network-free.

Proves: the PENDING->RUNNING->SUCCESS lifecycle, the exact artifact bytes the
pipeline wrote, the idempotency ack-and-skip on a terminal row, the phantom-row
return, and that a TransientUpstreamError re-raises (so the broker can retry)
while the processor leaves the row RUNNING for the broker to transition.
"""

from __future__ import annotations

import json
import uuid

import pytest

from app.domain.exceptions import TransientUpstreamError
from app.domain.models import InferenceJob, JobStatus, JobType
from app.services.pipelines import PipelineContext
from app.services.processor import JobProcessor
from tests.support.fakes import InMemoryRepository
from tests.support.recording_providers import (
    CallLog,
    RecordingEmbedding,
    RecordingLLM,
    RecordingObjectStore,
    RecordingSearch,
    RecordingVectorStore,
)


def _ctx(log: CallLog, store: RecordingObjectStore) -> PipelineContext:
    return PipelineContext(
        search=RecordingSearch(log),
        embedding=RecordingEmbedding(log),
        vector_store=RecordingVectorStore(log),
        llm=RecordingLLM(log),
        object_store=store,
    )


def _pending(job_type: JobType, payload: dict[str, object]) -> InferenceJob:
    return InferenceJob(
        id=uuid.uuid4(),
        job_type=job_type,
        status=JobStatus.PENDING,
        payload=payload,
    )


def _repo_with(job: InferenceJob) -> InMemoryRepository:
    repo = InMemoryRepository()
    repo.store[job.id] = job
    return repo


async def test_rag_query_full_lifecycle_and_artifact_bytes() -> None:
    log = CallLog()
    store = RecordingObjectStore(log)
    job = _pending(JobType.RAG_QUERY, {"query": "what is hexagonal architecture?", "top_k": 2})
    repo = _repo_with(job)
    processor = JobProcessor(repository=repo, ctx=_ctx(log, store))

    await processor.process(job.id, attempt=1)

    # Status reached SUCCESS via RUNNING (transition methods enforce the order).
    stored = await repo.get(job.id)
    assert stored.status is JobStatus.SUCCESS
    assert stored.result_ref == f"s3://aie-artifacts/results/rag_query/{job.id}.json"
    assert stored.duration_ms is not None  # measured, not asserted on a value

    # The exact artifact bytes the pipeline produced are inspectable.
    body = store.objects[f"results/rag_query/{job.id}.json"]
    parsed = json.loads(body)
    assert parsed["job_id"] == str(job.id)
    assert parsed["query"] == "what is hexagonal architecture?"
    assert parsed["answer"].startswith("answer:")


async def test_embed_document_writes_manifest() -> None:
    log = CallLog()
    store = RecordingObjectStore(log)
    job = _pending(
        JobType.EMBED_DOCUMENT,
        {"text": "x" * 1000, "document_id": "doc-7", "chunk_size": 300, "chunk_overlap": 50},
    )
    repo = _repo_with(job)
    processor = JobProcessor(repository=repo, ctx=_ctx(log, store))

    await processor.process(job.id, attempt=1)

    stored = await repo.get(job.id)
    assert stored.status is JobStatus.SUCCESS
    manifest = json.loads(store.objects[f"results/embed_document/{job.id}-manifest.json"])
    assert manifest["document_id"] == "doc-7"
    assert manifest["chunk_count"] == len(manifest["chunks"]) > 0
    assert manifest["namespace"] == "doc:doc-7"


async def test_idempotency_skips_terminal_job() -> None:
    log = CallLog()
    store = RecordingObjectStore(log)
    job = _pending(JobType.RAG_QUERY, {"query": "already done"})
    job.mark_running()
    job.mark_success(result_ref="s3://aie-artifacts/old.json", duration_ms=5)  # already SUCCESS
    repo = _repo_with(job)
    processor = JobProcessor(repository=repo, ctx=_ctx(log, store))

    await processor.process(job.id, attempt=2)  # redelivery

    # No pipeline work happened: the call log is empty and no new artifact wrote.
    assert log.methods() == []
    assert store.objects == {}
    stored = await repo.get(job.id)
    assert stored.result_ref == "s3://aie-artifacts/old.json"  # unchanged


async def test_missing_row_returns_without_error() -> None:
    log = CallLog()
    store = RecordingObjectStore(log)
    processor = JobProcessor(repository=InMemoryRepository(), ctx=_ctx(log, store))
    # No row for this id -> returns (broker will XACK the phantom), no raise.
    await processor.process(uuid.uuid4(), attempt=1)
    assert log.methods() == []


async def test_transient_failure_reraises_leaving_row_running() -> None:
    log = CallLog()
    store = RecordingObjectStore(log)

    class BoomLLM:
        async def complete(self, prompt: str, *, max_new_tokens: int) -> str:
            raise TransientUpstreamError("upstream 503")

    job = _pending(JobType.RAG_QUERY, {"query": "trigger failure", "top_k": 1})
    repo = _repo_with(job)
    ctx = _ctx(log, store)
    ctx.llm = BoomLLM()  # swap in the failing port (structurally an LLMProvider)
    processor = JobProcessor(repository=repo, ctx=ctx)

    with pytest.raises(TransientUpstreamError):  # re-raised to the broker
        await processor.process(job.id, attempt=1)

    # The processor does NOT mark the row terminal: the broker (Phase 5) owns the
    # RUNNING->FAILED / RUNNING->PENDING transition. The row is left RUNNING so the
    # broker's requeue()/mark_failed() (which require RUNNING) succeed.
    stored = await repo.get(job.id)
    assert stored.status is JobStatus.RUNNING
    assert stored.error is None
