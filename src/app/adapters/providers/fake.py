"""Deterministic, zero-key, zero-network provider fakes (the default bundle).

These are not mocks — they are real, working in-process implementations with
sensible semantics (hash-seeded embeddings, cosine-ranked retrieval,
context-echoing completion, a canned corpus). They implement the same ``async``
port methods as the real adapters so they are drop-in interchangeable, and they
do no I/O so they need no offloader. dev, test, and the demo all run against
these.
"""

from __future__ import annotations

import hashlib
import math
import struct
from typing import Final

from app.ports.providers import SearchResult, VectorMatch

_DEFAULT_DIM: Final[int] = 384  # mirrors all-MiniLM-L6-v2; small, fast, realistic


class FakeEmbedding:
    """Deterministic hash-based embedding provider (no model, no network).

    For a given text and dim, the vector is byte-for-byte reproducible across
    runs, processes, and OSes. Vectors are L2-normalized so cosine similarity in
    FakeVectorStore behaves like the real cosine metric.
    """

    def __init__(self, dim: int = _DEFAULT_DIM) -> None:
        self.dim = dim

    async def embed(self, texts: list[str]) -> list[list[float]]:
        return [self._vector(t) for t in texts]

    def _vector(self, text: str) -> list[float]:
        # Expand a BLAKE2b digest into `dim` float32 values, then L2-normalize.
        raw = bytearray()
        counter = 0
        # 4 bytes -> 1 float; keep hashing with a counter until we have enough.
        while len(raw) < self.dim * 4:
            h = hashlib.blake2b(
                text.encode("utf-8"),
                digest_size=64,
                salt=struct.pack("<Q", counter)[:16].ljust(16, b"\x00")[:16],
            )
            raw.extend(h.digest())
            counter += 1
        floats = [struct.unpack_from("<f", bytes(raw), i * 4)[0] for i in range(self.dim)]
        # Replace non-finite values (NaN/inf from arbitrary bit patterns) with 0.0.
        floats = [f if math.isfinite(f) else 0.0 for f in floats]
        norm = math.sqrt(sum(f * f for f in floats)) or 1.0
        return [f / norm for f in floats]


class FakeVectorStore:
    """In-memory vector store with exact cosine-similarity ranking.

    Stores (values, metadata) per id per namespace. query() ranks all stored
    vectors by cosine similarity to the query vector and returns the top_k.
    """

    def __init__(self) -> None:
        # namespace -> id -> (values, metadata)
        self._ns: dict[str, dict[str, tuple[list[float], dict[str, object]]]] = {}

    async def upsert(
        self,
        vectors: list[tuple[str, list[float], dict[str, object]]],
        *,
        namespace: str,
    ) -> None:
        bucket = self._ns.setdefault(namespace, {})
        for vid, values, metadata in vectors:
            bucket[vid] = (list(values), dict(metadata))

    async def query(
        self,
        vector: list[float],
        *,
        top_k: int,
        namespace: str,
    ) -> list[VectorMatch]:
        bucket = self._ns.get(namespace, {})
        scored: list[VectorMatch] = [
            VectorMatch(id=vid, score=_cosine(vector, values), metadata=metadata)
            for vid, (values, metadata) in bucket.items()
        ]
        # Deterministic ordering: score desc, then id asc to break ties stably.
        scored.sort(key=lambda m: (-m["score"], m["id"]))
        return scored[:top_k]


class FakeLLM:
    """Templated completion provider that echoes the prompt.

    The output is deterministic and observable: a test can assert the answer
    reflects the prompt and is bounded by max_new_tokens. This makes RAG pipeline
    tests meaningful without a model.
    """

    async def complete(self, prompt: str, *, max_new_tokens: int) -> str:
        # A stable, inspectable template. Truncate to a word budget so
        # max_new_tokens has an observable effect.
        body = f"Based on the provided context, here is the answer to your query. {prompt.strip()}"
        words = body.split()
        return " ".join(words[: max(1, max_new_tokens)])


class FakeSearch:
    """Canned in-memory search corpus (no network).

    Returns up to max_results hits whose title/snippet contains the query terms,
    with a deterministic fallback so a query always yields *something* for the
    RAG pipeline to embed. Ordering is stable.
    """

    _CORPUS: Final[tuple[SearchResult, ...]] = (
        SearchResult(
            title="Asynchronous I/O in Python",
            url="https://example.test/async-python",
            snippet="asyncio offloads blocking calls to a thread pool via to_thread.",
        ),
        SearchResult(
            title="Hexagonal Architecture",
            url="https://example.test/hexagonal",
            snippet="Ports and adapters isolate the core from frameworks and SDKs.",
        ),
        SearchResult(
            title="Vector Databases and Cosine Similarity",
            url="https://example.test/vector-db",
            snippet="Embeddings are ranked by cosine similarity for retrieval.",
        ),
        SearchResult(
            title="Retry with Exponential Backoff",
            url="https://example.test/retry-backoff",
            snippet="Transient upstream errors are retried with jittered backoff.",
        ),
    )

    async def search(self, query: str, *, max_results: int) -> list[SearchResult]:
        terms = {t.lower() for t in query.split() if t}
        ranked = sorted(self._CORPUS, key=lambda r: (-_term_overlap(r, terms), r["url"]))
        hits = [r for r in ranked if _term_overlap(r, terms) > 0]
        # Guarantee non-empty output for the pipeline (fallback to top corpus docs).
        if not hits:
            hits = list(ranked)
        return hits[:max_results]


# ---- pure helpers ---------------------------------------------------------


def _cosine(a: list[float], b: list[float]) -> float:
    """Cosine similarity; safe on length mismatch and zero vectors."""
    n = min(len(a), len(b))
    if n == 0:
        return 0.0
    dot = sum(a[i] * b[i] for i in range(n))
    na = math.sqrt(sum(a[i] * a[i] for i in range(n)))
    nb = math.sqrt(sum(b[i] * b[i] for i in range(n)))
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / (na * nb)


def _term_overlap(result: SearchResult, terms: set[str]) -> int:
    """Count query terms appearing in a corpus doc's title+snippet."""
    haystack = f"{result['title']} {result['snippet']}".lower()
    return sum(1 for t in terms if t in haystack)
