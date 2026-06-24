"""Real ``SearchProvider`` over ddgs (DuckDuckGo).

A ``DDGS`` factory is injected; ``search`` runs the whole blocking interaction
(construct client + ``.text()`` + drain the iterator) inside one offloaded
closure, normalizes the raw ``{title, href, body}`` dicts into the port's
``{title, url, snippet}`` shape, and translates errors via
``classify_ddgs_error``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from app.adapters.providers._errors import classify_ddgs_error
from app.core.retry import retrying
from app.ports.providers import SearchResult

if TYPE_CHECKING:
    from app.core.config import RetrySettings
    from app.ports.offloader import SyncOffloader


class DdgsSearch:
    """SearchProvider over ``ddgs.DDGS().text(...)``.

    A ``ddgs_factory`` (callable returning a fresh DDGS) is injected rather than
    a long-lived client: DDGS holds an HTTP session, is cheap to create, and a
    per-call instance avoids cross-thread session sharing. Tests inject a factory
    returning a stub with a ``.text(...)`` method.
    """

    def __init__(
        self,
        ddgs_factory: Any,  # Callable[[], DDGS-like]
        *,
        region: str,
        offloader: SyncOffloader,
        retry: RetrySettings,
    ) -> None:
        self._ddgs_factory = ddgs_factory
        self._region = region
        self._offload = offloader
        self._retry = retry

    async def search(self, query: str, *, max_results: int) -> list[SearchResult]:
        raw = await self._call(self._run_text, query, max_results)
        return _normalize_hits(raw)

    def _run_text(self, query: str, max_results: int) -> list[dict[str, Any]]:
        """Blocking ddgs call — ALWAYS executed via offloader.run, never inline.

        DDGS supports use as a context manager; we open/close it per call so its
        HTTP session lifetime is bounded to the offloaded thread. ``list(...)``
        forces full materialization inside the thread in case .text() is lazy.
        """
        ddgs = self._ddgs_factory()
        enter = getattr(ddgs, "__enter__", None)
        if callable(enter):
            with ddgs as client:
                return list(
                    client.text(
                        query,
                        region=self._region,
                        safesearch="moderate",
                        max_results=max_results,
                    )
                )
        return list(
            ddgs.text(
                query,
                region=self._region,
                safesearch="moderate",
                max_results=max_results,
            )
        )

    async def _call[**P, R](self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
        async for attempt in retrying(self._retry):
            with attempt:
                try:
                    return await self._offload.run(fn, *args, **kwargs)
                except Exception as exc:
                    raise classify_ddgs_error(exc) from exc
        raise AssertionError("unreachable")  # pragma: no cover


def _normalize_hits(raw: list[dict[str, Any]]) -> list[SearchResult]:
    """Map ddgs ``{title, href, body}`` dicts to the port's ``SearchResult``."""
    return [
        SearchResult(
            title=str(hit.get("title", "")),
            url=str(hit.get("href", "")),
            snippet=str(hit.get("body", "")),
        )
        for hit in raw
    ]
