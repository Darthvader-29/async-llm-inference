"""In-process test doubles for the ports — injected throughout the unit suite.

Two flavours live here, both structurally conforming to their port Protocol
(no inheritance; mypy --strict checks conformance at the injection site):

* **Behaviour-rich doubles** that the write-path/broker/container tests depend
  on: ``InMemoryRepository`` / ``FakeJobRepository`` (dict-backed; ``get`` raises
  the real ``JobNotFound`` contract), ``FakeJobQueue`` / ``FakeQueue`` (record
  published jobs; ``FakeQueue`` can simulate N transient publish failures to
  drive retry deterministically), and ``FakeObjectStore`` (dict-backed bucket).
* **Minimal deterministic provider stubs** (``StubEmbeddingProvider``,
  ``StubLLMProvider``, ``StubVectorStore``, ``StubSearchProvider``) — fixed-shape
  stand-ins used where a test only needs a conforming provider, not realistic
  output. The production-grade deterministic provider fakes (seeded embeddings,
  cosine vector store, templated LLM, canned search) live with the real adapters
  in ``src/app/adapters/providers/fake.py`` and are selected by the all-fakes
  bundle — this module does NOT duplicate them.

Everything is clock-free and zero-dependency (no network, no Docker).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from uuid import UUID

from app.domain.exceptions import JobNotFound, TransientUpstreamError
from app.domain.models import InferenceJob
from app.ports import SearchResult, VectorMatch


@dataclass(slots=True)
class FakeJobRepository:
    """In-memory ``JobRepository`` double (dict-backed; ``get`` raises ``KeyError``).

    The lifecycle/wiring tests that use it never hit the missing-row path;
    ``InMemoryRepository`` below is the variant that raises the real ``JobNotFound``.
    """

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


# ---------------------------------------------------------------------------
# Behaviour-rich doubles for the ingestion write-path tests. Public attributes
# (``store`` / ``published``) let tests assert on what was persisted/published;
# ``get`` raises ``JobNotFound`` (the real port contract); ``FakeQueue`` can be
# told to fail transiently N times to exercise publish retry deterministically
# (counted, never timed — no clocks).
# ---------------------------------------------------------------------------
class FakeQueue:
    """Records published jobs; can be told to fail transiently N times."""

    def __init__(self, fail_times: int = 0) -> None:
        self.published: list[InferenceJob] = []
        self._fail_times = fail_times

    async def publish(self, job: InferenceJob) -> None:
        if self._fail_times > 0:
            self._fail_times -= 1
            raise TransientUpstreamError("simulated transient publish failure")
        self.published.append(job)


class InMemoryRepository:
    """Dict-backed ``JobRepository`` (``get`` raises ``JobNotFound``)."""

    def __init__(self) -> None:
        self.store: dict[UUID, InferenceJob] = {}

    async def add(self, job: InferenceJob) -> None:
        self.store[job.id] = job

    async def get(self, job_id: UUID) -> InferenceJob:
        try:
            return self.store[job_id]
        except KeyError:
            raise JobNotFound(job_id) from None

    async def update(self, job: InferenceJob) -> None:
        self.store[job.id] = job
