"""Unit tests asserting the exact port-call ORDER of each pipeline.

The shared CallLog captures (port, method) tuples in the order they happened.
We assert that ordering directly — the deterministic way to verify
orchestration without timing or network.
"""

from __future__ import annotations

import uuid

from app.domain.models import InferenceJob, JobStatus, JobType
from app.services.pipelines import (
    EmbedDocumentPipeline,
    PipelineContext,
    RagQueryPipeline,
)
from tests.support.recording_providers import (
    CallLog,
    RecordingEmbedding,
    RecordingLLM,
    RecordingObjectStore,
    RecordingSearch,
    RecordingVectorStore,
)


def _job(job_type: JobType, payload: dict[str, object]) -> InferenceJob:
    return InferenceJob(
        id=uuid.uuid4(), job_type=job_type, status=JobStatus.RUNNING, payload=payload
    )


def _ctx(log: CallLog) -> PipelineContext:
    return PipelineContext(
        search=RecordingSearch(log),
        embedding=RecordingEmbedding(log),
        vector_store=RecordingVectorStore(log),
        llm=RecordingLLM(log),
        object_store=RecordingObjectStore(log),
    )


async def test_rag_query_calls_all_five_ports_in_order() -> None:
    log = CallLog()
    job = _job(JobType.RAG_QUERY, {"query": "q", "top_k": 2})
    ref = await RagQueryPipeline().run(job, _ctx(log))

    assert log.methods() == [
        ("search", "search"),
        ("embedding", "embed"),  # embed snippets
        ("vector_store", "upsert"),  # index snippets
        ("embedding", "embed"),  # embed query
        ("vector_store", "query"),  # retrieve
        ("llm", "complete"),  # answer
        ("object_store", "put_bytes"),  # persist result
    ]
    assert ref == f"s3://aie-artifacts/results/rag_query/{job.id}.json"


async def test_embed_document_calls_ports_in_order() -> None:
    log = CallLog()
    job = _job(JobType.EMBED_DOCUMENT, {"text": "y" * 700, "document_id": "d1"})
    ref = await EmbedDocumentPipeline().run(job, _ctx(log))

    # chunk_text is pure (no port) -> not in the log; the I/O order is:
    assert log.methods() == [
        ("embedding", "embed"),
        ("vector_store", "upsert"),
        ("object_store", "put_bytes"),
    ]
    assert ref == f"s3://aie-artifacts/results/embed_document/{job.id}-manifest.json"


async def test_rag_query_handles_empty_search_gracefully() -> None:
    log = CallLog()
    # Corpus of size 0 -> search returns nothing; the pipeline must still finish
    # (embed query, skip upsert/query, call LLM with empty context, persist).
    job = _job(JobType.RAG_QUERY, {"query": "q", "top_k": 1})
    ctx = _ctx(log)
    ctx.search = RecordingSearch(log, corpus=[])  # no snippets to retrieve
    await RagQueryPipeline().run(job, ctx)

    methods = log.methods()
    assert ("search", "search") in methods
    assert ("embedding", "embed") in methods  # query still embedded
    assert ("llm", "complete") in methods
    assert ("object_store", "put_bytes") in methods
    # No upsert/query happened because there were no snippets to index.
    assert ("vector_store", "upsert") not in methods
    assert ("vector_store", "query") not in methods
