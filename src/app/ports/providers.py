"""Provider ports: the engine's four AI capabilities.

Each is a small ``Protocol`` so that a deterministic in-process fake and a real
SDK-backed adapter are interchangeable at the injection site. The pipelines
(Phase 7) depend on these ports, never on a concrete SDK.

Vectors are plain ``list[float]`` to avoid coupling core to numpy; fakes and
adapters convert at their boundary if they use ndarrays internally.
"""

from __future__ import annotations

from typing import Protocol, TypedDict


class SearchResult(TypedDict):
    """One web-search hit, normalized across providers."""

    title: str
    url: str
    snippet: str


class VectorMatch(TypedDict):
    """One vector-search hit, normalized across providers.

    The matched chunk's text (when stored) lives in ``metadata`` (e.g.
    ``metadata["text"]``), mirroring how Pinecone returns arbitrary metadata.
    """

    id: str
    score: float
    metadata: dict[str, object]


class EmbeddingProvider(Protocol):
    """Turn text into dense vectors."""

    dim: int  # fixed embedding dimensionality (callers may read it to size an index)

    async def embed(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of ``texts``.

        Returns one vector per input, in input order, all of identical
        dimensionality. A blocking SDK call here is offloaded by the adapter.
        """
        ...


class LLMProvider(Protocol):
    """Generate a single-shot text completion for a prompt."""

    async def complete(self, prompt: str, *, max_new_tokens: int) -> str:
        """Return the model's text completion for ``prompt``.

        RAG grounding is the caller's job: the pipeline folds retrieved context
        into ``prompt`` before calling. ``max_new_tokens`` bounds the output.
        """
        ...


class VectorStore(Protocol):
    """Upsert and query dense vectors (e.g. Pinecone, or an in-memory fake)."""

    async def upsert(
        self,
        vectors: list[tuple[str, list[float], dict[str, object]]],
        *,
        namespace: str,
    ) -> None:
        """Insert/replace ``(id, values, metadata)`` triples in ``namespace``.

        ``namespace`` partitions vectors (e.g. per job) so concurrent jobs do
        not pollute each other's retrieval results.
        """
        ...

    async def query(
        self,
        vector: list[float],
        *,
        top_k: int,
        namespace: str,
    ) -> list[VectorMatch]:
        """Return the ``top_k`` nearest matches to ``vector`` within ``namespace``,
        ordered by descending similarity score."""
        ...


class SearchProvider(Protocol):
    """Fetch external knowledge (e.g. DuckDuckGo via ``ddgs``, or a canned fake)."""

    async def search(self, query: str, *, max_results: int) -> list[SearchResult]:
        """Return up to ``max_results`` hits for ``query``.

        Implementations offload the blocking HTTP/SDK call and translate
        transport errors to upstream errors.
        """
        ...
