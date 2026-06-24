"""Real ``VectorStore`` over the pinecone SDK.

An already-resolved index handle is injected (the composition root builds
``Pinecone(api_key=...).Index(name=...)``); ``upsert``/``query`` route through
``offloader.run`` inside a retry, convert the port's plain tuples to/from
Pinecone's dict shapes, and translate errors via ``classify_pinecone_error``.
The untyped SDK surface is confined to this module and normalized at the
boundary into ``VectorMatch``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from app.adapters.providers._errors import classify_pinecone_error
from app.core.retry import retrying
from app.ports.providers import VectorMatch

if TYPE_CHECKING:
    from app.core.config import RetrySettings
    from app.ports.offloader import SyncOffloader


class PineconeVectorStore:
    """VectorStore over a pinecone Index handle.

    The handle is injected so tests can pass a stub exposing the same
    ``.upsert``/``.query`` surface with zero network.
    """

    def __init__(
        self,
        index: Any,  # pinecone Index — untyped in stubs; kept as Any
        *,
        offloader: SyncOffloader,
        retry: RetrySettings,
    ) -> None:
        self._index = index
        self._offload = offloader
        self._retry = retry

    async def upsert(
        self,
        vectors: list[tuple[str, list[float], dict[str, object]]],
        *,
        namespace: str,
    ) -> None:
        # Pinecone wants a list of {"id","values","metadata"} dicts. Copy
        # values/metadata so the caller's data can't be mutated by the SDK.
        payload = [
            {"id": vid, "values": list(values), "metadata": dict(metadata)}
            for (vid, values, metadata) in vectors
        ]
        await self._call(self._index.upsert, vectors=payload, namespace=namespace)

    async def query(
        self,
        vector: list[float],
        *,
        top_k: int,
        namespace: str,
    ) -> list[VectorMatch]:
        response = await self._call(
            self._index.query,
            vector=list(vector),
            top_k=top_k,
            namespace=namespace,
            include_metadata=True,
            include_values=False,
        )
        return _matches_from_response(response)

    async def _call[**P, R](self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
        async for attempt in retrying(self._retry):
            with attempt:
                try:
                    return await self._offload.run(fn, *args, **kwargs)
                except Exception as exc:
                    raise classify_pinecone_error(exc) from exc
        raise AssertionError("unreachable")  # pragma: no cover


def _matches_from_response(response: Any) -> list[VectorMatch]:
    """Normalize a Pinecone QueryResponse into ``list[VectorMatch]``.

    The response supports dict access (``response["matches"]``, ``m["id"]`` ...);
    we also tolerate attribute access for forward/backward SDK compatibility.
    """
    raw_matches = _get(response, "matches") or []
    return [
        VectorMatch(
            id=str(_get(m, "id")),
            score=float(_get(m, "score")),
            metadata=dict(_get(m, "metadata") or {}),
        )
        for m in raw_matches
    ]


def _get(obj: Any, key: str) -> Any:
    """Read ``key`` from a dict-like or attribute-like SDK object."""
    if isinstance(obj, dict):
        return obj.get(key)
    if hasattr(obj, "__getitem__"):
        try:
            return obj[key]
        except (KeyError, TypeError):
            pass
    return getattr(obj, key, None)
