"""Stub fakes for the ports — *shapes only*.

These minimal in-memory implementations exist so Phase 2 can type-check
conformance and later phases have a stable import target. The behavior-rich
deterministic fakes (seeded embeddings, cosine vector store, templated LLM,
canned search) are implemented in Phase 4 — see
``Docs/phases/phase-4-object-store-providers.md``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from uuid import UUID

from app.domain.models import InferenceJob
from app.ports import SearchResult, VectorMatch


@dataclass(slots=True)
class FakeJobRepository:
    """In-memory ``JobRepository`` stub (shape only; expanded in later phases)."""

    _store: dict[UUID, InferenceJob] = field(default_factory=dict)

    async def add(self, job: InferenceJob) -> None:
        self._store[job.id] = job

    async def get(self, job_id: UUID) -> InferenceJob:
        return self._store[job_id]  # KeyError → tests expecting JobNotFound refine this in Phase 6

    async def update(self, job: InferenceJob) -> None:
        self._store[job.id] = job


@dataclass(slots=True)
class FakeJobQueue:
    """In-memory ``JobQueue`` stub recording published job ids."""

    published: list[UUID] = field(default_factory=list)

    async def publish(self, job: InferenceJob) -> None:
        self.published.append(job.id)


@dataclass(slots=True)
class FakeObjectStore:
    """In-memory ``ObjectStore`` stub (dict-backed), bound to one bucket."""

    bucket: str = "aie-artifacts"
    _blobs: dict[str, bytes] = field(default_factory=dict)

    async def ensure_bucket(self) -> None:
        return None

    async def bucket_exists(self) -> bool:
        return True

    async def put_bytes(
        self, key: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> str:
        self._blobs[key] = data
        return f"s3://{self.bucket}/{key}"

    async def get_bytes(self, key: str) -> bytes:
        return self._blobs[key]


@dataclass(slots=True)
class StubEmbeddingProvider:
    """Returns fixed-dimensionality zero vectors (shape only)."""

    dim: int = 8

    async def embed(self, texts: list[str]) -> list[list[float]]:
        return [[0.0] * self.dim for _ in texts]


@dataclass(slots=True)
class StubLLMProvider:
    """Echoes a templated answer (shape only)."""

    async def complete(self, prompt: str, *, max_new_tokens: int) -> str:
        return f"stub-answer for: {prompt[:32]}"


@dataclass(slots=True)
class StubVectorStore:
    """No-op upsert; empty query (shape only)."""

    async def upsert(
        self, vectors: list[tuple[str, list[float], dict[str, object]]], *, namespace: str
    ) -> None:
        return None

    async def query(self, vector: list[float], *, top_k: int, namespace: str) -> list[VectorMatch]:
        return []


@dataclass(slots=True)
class StubSearchProvider:
    """Returns an empty result set (shape only)."""

    async def search(self, query: str, *, max_results: int) -> list[SearchResult]:
        return []
