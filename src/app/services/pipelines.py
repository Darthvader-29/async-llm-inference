"""Inference pipelines: orchestration over ports, one per JobType.

A pipeline is pure orchestration — it calls ports (search, embed, vector,
LLM, object store) in a deterministic order and returns a storage reference
to the artifact it wrote. It never imports an SDK and never offloads or
retries directly: those concerns live inside the adapters the ports point
to (Phase 2/4). That keeps pipelines synchronous-looking, easy to read, and
trivially testable with fakes.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Protocol

from app.domain.chunking import chunk_text
from app.domain.exceptions import PermanentUpstreamError
from app.domain.models import InferenceJob, JobType
from app.ports.object_store import ObjectStore
from app.ports.providers import (
    EmbeddingProvider,
    LLMProvider,
    SearchProvider,
    VectorStore,
)


@dataclass(slots=True)
class PipelineContext:
    """The ports a pipeline may use, supplied by the AppContainer.

    Holding ports (not adapters) here is the hexagonal boundary in action:
    the container injects fakes by default and real SDK adapters when keys
    are configured, and the pipeline code is identical either way.
    """

    search: SearchProvider
    embedding: EmbeddingProvider
    vector_store: VectorStore
    llm: LLMProvider
    object_store: ObjectStore  # bucket is bound inside the adapter (no bucket field here)


class Pipeline(Protocol):
    """Structural contract every pipeline satisfies.

    ``run`` returns a storage reference (e.g. ``s3://bucket/key``) to the
    artifact produced, which the JobProcessor records as the job's result.
    """

    async def run(self, job: InferenceJob, ctx: PipelineContext) -> str: ...


def _result_key(job: InferenceJob, suffix: str) -> str:
    """Deterministic object key for a job's artifact (no clock, no random).

    Using the job id keys the artifact predictably so tests can fetch and
    assert the exact bytes, and re-processing the same job overwrites rather
    than duplicating.
    """
    return f"results/{job.job_type.value}/{job.id}{suffix}"


def _require_str(payload: dict[str, object], key: str) -> str:
    """Read a required string field from the (JSONB-loaded) payload.

    Payload values arrive typed as ``object`` (loaded from JSONB), so we
    narrow explicitly. A malformed payload that the API schema should have
    rejected is a *permanent* error — raising ``PermanentUpstreamError`` sends
    it straight to the DLQ instead of retrying a request that can never work.
    """
    value = payload.get(key)
    if not isinstance(value, str):
        raise PermanentUpstreamError(
            f"payload field {key!r} must be a string, got {type(value).__name__}"
        )
    return value


def _optional_int(payload: dict[str, object], key: str, default: int) -> int:
    """Read an optional int field; JSON numbers load as ``int``/``float``."""
    value = payload.get(key, default)
    if isinstance(value, bool):  # bool is an int subclass — reject it explicitly
        raise PermanentUpstreamError(f"payload field {key!r} must be an int, got bool")
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    raise PermanentUpstreamError(
        f"payload field {key!r} must be an int, got {type(value).__name__}"
    )


def _rag_prompt(query: str, context: list[str]) -> str:
    """Fold retrieved context into a single grounded prompt for the LLM port.

    The ``LLMProvider.complete`` port takes a single ``prompt`` (no separate
    context arg), so RAG grounding is the pipeline's job: we build the prompt
    here and the adapter just completes it.
    """
    if not context:
        return query
    context_block = "\n\n".join(context)
    return f"Context:\n{context_block}\n\nQuestion: {query}\n\nAnswer:"


class RagQueryPipeline:
    """RAG over the configured providers.

    Order (and this order is asserted in tests):
        1. search(query)                  -> candidate snippets
        2. embedding.embed(snippets)      -> snippet vectors
        3. vector_store.upsert(...)       -> index the snippets
        4. embedding.embed([query])       -> query vector
        5. vector_store.query(qvec, k)    -> nearest snippet ids
        6. llm.complete(prompt)           -> grounded answer
        7. object_store.put_bytes(json)   -> persist the result artifact

    This single method exercises all five provider ports plus the object
    store sequentially — the spec's "decoupled processing workflow" shown
    end to end.
    """

    async def run(self, job: InferenceJob, ctx: PipelineContext) -> str:
        payload = job.payload
        query = _require_str(payload, "query")
        top_k = _optional_int(payload, "top_k", 3)

        # 1. Retrieve candidate documents from the search provider.
        snippets = await ctx.search.search(query, max_results=top_k * 2)
        snippet_texts = [s["snippet"] for s in snippets]  # SearchResult is a TypedDict

        # 2. Embed the retrieved snippets so we can index them.
        snippet_vectors = await ctx.embedding.embed(snippet_texts) if snippet_texts else []

        # 3. Upsert snippet vectors into the vector store (namespaced by job).
        namespace = f"job:{job.id}"
        records: list[tuple[str, list[float], dict[str, object]]] = [
            (f"snip:{i}", vec, {"text": text})
            for i, (vec, text) in enumerate(zip(snippet_vectors, snippet_texts, strict=True))
        ]
        if records:
            await ctx.vector_store.upsert(records, namespace=namespace)

        # 4. Embed the query itself.
        (query_vector,) = await ctx.embedding.embed([query])

        # 5. Retrieve the nearest snippets for the query.
        matches = (
            await ctx.vector_store.query(query_vector, top_k=top_k, namespace=namespace)
            if records
            else []
        )
        # VectorMatch is a TypedDict; the snippet text was stored in metadata at upsert.
        context_texts = [str(m["metadata"]["text"]) for m in matches if "text" in m["metadata"]]

        # 6. Ask the LLM to answer. The port takes a single prompt, so we fold
        #    the retrieved context into the prompt (RAG grounding) here.
        prompt = _rag_prompt(query, context_texts)
        answer = await ctx.llm.complete(prompt, max_new_tokens=512)

        # 7. Persist the structured result as a JSON artifact in object storage.
        result: dict[str, object] = {
            "job_id": str(job.id),
            "query": query,
            "answer": answer,
            "context": context_texts,
            "retrieved_ids": [m["id"] for m in matches],
        }
        body = json.dumps(result, indent=2, sort_keys=True).encode("utf-8")
        return await ctx.object_store.put_bytes(
            _result_key(job, ".json"), body, content_type="application/json"
        )


class EmbedDocumentPipeline:
    """Chunk a document, embed the chunks, index them, and write a manifest.

    Order (asserted in tests):
        1. chunk_text(document)           -> deterministic Chunk list (pure)
        2. embedding.embed(chunk_texts)   -> chunk vectors
        3. vector_store.upsert(...)       -> index the chunks
        4. object_store.put_bytes(json)   -> manifest artifact
    """

    async def run(self, job: InferenceJob, ctx: PipelineContext) -> str:
        payload = job.payload
        document = _require_str(payload, "text")  # EmbedDocumentPayload field (Phase 6 schema)
        doc_id = str(payload.get("document_id", job.id))
        size = _optional_int(payload, "chunk_size", 512)
        overlap = _optional_int(payload, "chunk_overlap", 64)

        # 1. Pure, deterministic chunking (no I/O) — see domain/chunking.py.
        chunks = chunk_text(document, size=size, overlap=overlap)

        # 2. Embed every chunk in one offloaded batch call.
        vectors = await ctx.embedding.embed([c.text for c in chunks]) if chunks else []

        # 3. Index the chunk vectors under a per-document namespace.
        namespace = f"doc:{doc_id}"
        records: list[tuple[str, list[float], dict[str, object]]] = [
            (
                f"{doc_id}:chunk:{c.index}",
                vec,
                {"chunk_index": c.index, "start": c.start, "end": c.end},
            )
            for c, vec in zip(chunks, vectors, strict=True)
        ]
        if records:
            await ctx.vector_store.upsert(records, namespace=namespace)

        # 4. Write a manifest describing what was indexed (the artifact).
        manifest: dict[str, object] = {
            "job_id": str(job.id),
            "document_id": doc_id,
            "chunk_count": len(chunks),
            "namespace": namespace,
            "chunks": [{"index": c.index, "start": c.start, "end": c.end} for c in chunks],
        }
        body = json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8")
        return await ctx.object_store.put_bytes(
            _result_key(job, "-manifest.json"), body, content_type="application/json"
        )


# The registry: dispatch a JobType to its pipeline. Adding a new job type is
# a one-line registration here — the JobProcessor never grows an if/elif.
PIPELINES: dict[JobType, Pipeline] = {
    JobType.RAG_QUERY: RagQueryPipeline(),
    JobType.EMBED_DOCUMENT: EmbedDocumentPipeline(),
}
