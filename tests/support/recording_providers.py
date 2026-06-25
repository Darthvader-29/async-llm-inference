"""Recording provider fakes for deterministic, clock-free pipeline tests.

Each fake appends a ``(port, method)`` tuple to a shared ``calls`` log before
returning canned, deterministic data. Tests assert the ordering of that log
to prove a pipeline touched the ports in the right sequence — no timing, no
network, fully reproducible.

The method signatures match the Phase 2 provider ``Protocol``s *exactly*
(parameter names included), so mypy --strict accepts them at the
``PipelineContext`` injection site without casts.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from app.ports.providers import SearchResult, VectorMatch


@dataclass
class CallLog:
    """Shared, ordered record of port interactions across all fakes."""

    calls: list[tuple[str, str]] = field(default_factory=list)

    def record(self, port: str, method: str) -> None:
        self.calls.append((port, method))

    def methods(self) -> list[tuple[str, str]]:
        return list(self.calls)


class RecordingSearch:
    def __init__(self, log: CallLog, corpus: list[str] | None = None) -> None:
        self._log = log
        self._corpus = corpus if corpus is not None else ["alpha doc", "beta doc", "gamma doc"]

    async def search(self, query: str, *, max_results: int) -> list[SearchResult]:
        self._log.record("search", "search")
        # Deterministic: first ``max_results`` corpus entries as SearchResult dicts.
        return [
            SearchResult(title=f"doc {i}", url=f"https://example.test/{i}", snippet=t)
            for i, t in enumerate(self._corpus[:max_results])
        ]


class RecordingEmbedding:
    def __init__(self, log: CallLog, dim: int = 8) -> None:
        self._log = log
        self.dim = dim  # public — the EmbeddingProvider port exposes `dim`

    async def embed(self, texts: list[str]) -> list[list[float]]:
        self._log.record("embedding", "embed")
        # Seeded vector derived from the text content (stable within a run; the
        # tests assert call order / artifact JSON, not vector values).
        return [[float((hash(t) >> i) & 0xFF) for i in range(self.dim)] for t in texts]


class RecordingVectorStore:
    def __init__(self, log: CallLog) -> None:
        self._log = log
        self._store: dict[str, list[tuple[str, list[float], dict[str, object]]]] = {}

    async def upsert(
        self, vectors: list[tuple[str, list[float], dict[str, object]]], *, namespace: str
    ) -> None:
        self._log.record("vector_store", "upsert")
        self._store.setdefault(namespace, []).extend(vectors)

    async def query(self, vector: list[float], *, top_k: int, namespace: str) -> list[VectorMatch]:
        self._log.record("vector_store", "query")
        items = self._store.get(namespace, [])[:top_k]
        return [VectorMatch(id=rid, score=1.0, metadata=meta) for rid, _vec, meta in items]


class RecordingLLM:
    def __init__(self, log: CallLog) -> None:
        self._log = log

    async def complete(self, prompt: str, *, max_new_tokens: int) -> str:
        self._log.record("llm", "complete")
        # Echo a bounded slice of the prompt so tests assert deterministically.
        return f"answer: {prompt[:48]}"


class RecordingObjectStore:
    def __init__(self, log: CallLog, bucket: str = "aie-artifacts") -> None:
        self._log = log
        self.bucket = bucket  # bound at construction, like the real S3ObjectStore
        self.objects: dict[str, bytes] = {}

    async def put_bytes(
        self, key: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> str:
        self._log.record("object_store", "put_bytes")
        self.objects[key] = data
        return f"s3://{self.bucket}/{key}"

    async def get_bytes(self, key: str) -> bytes:
        self._log.record("object_store", "get_bytes")
        return self.objects[key]

    async def ensure_bucket(self) -> None:
        return None

    async def bucket_exists(self) -> bool:
        return True
