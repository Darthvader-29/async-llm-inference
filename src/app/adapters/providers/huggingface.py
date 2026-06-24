"""Real ``EmbeddingProvider`` / ``LLMProvider`` over huggingface_hub.

The synchronous ``InferenceClient`` is injected; the adapters touch it *only*
through ``offloader.run`` inside a retry, normalize the numpy / str returns to
plain Python at the boundary, and translate errors via ``classify_hf_error``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from app.adapters.providers._errors import classify_hf_error
from app.core.retry import retrying

if TYPE_CHECKING:
    from huggingface_hub import InferenceClient

    from app.core.config import RetrySettings
    from app.ports.offloader import SyncOffloader


class HuggingFaceEmbedding:
    """EmbeddingProvider over ``InferenceClient.feature_extraction``.

    feature_extraction(text) returns a float32 numpy array; we convert to plain
    ``list[list[float]]`` at the boundary so the port stays SDK-agnostic.
    """

    def __init__(
        self,
        client: InferenceClient,
        *,
        model: str,
        dim: int,
        offloader: SyncOffloader,
        retry: RetrySettings,
    ) -> None:
        self._client = client
        self._model = model
        self.dim = dim
        self._offload = offloader
        self._retry = retry

    async def embed(self, texts: list[str]) -> list[list[float]]:
        # feature_extraction accepts str | list[str]; pass the whole batch in one
        # offloaded call and normalize the ndarray shape afterwards.
        result = await self._call(self._client.feature_extraction, texts, model=self._model)
        return _ndarray_to_lists(result, batch=len(texts))

    async def _call[**P, R](self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
        async for attempt in retrying(self._retry):
            with attempt:
                try:
                    return await self._offload.run(fn, *args, **kwargs)
                except Exception as exc:
                    raise classify_hf_error(exc) from exc
        raise AssertionError("unreachable")  # pragma: no cover


class HuggingFaceLLM:
    """LLMProvider over ``InferenceClient.text_generation``.

    With ``details=False, stream=False`` the call returns a plain ``str`` — the
    exact ``LLMProvider.complete`` contract.
    """

    def __init__(
        self,
        client: InferenceClient,
        *,
        model: str,
        offloader: SyncOffloader,
        retry: RetrySettings,
    ) -> None:
        self._client = client
        self._model = model
        self._offload = offloader
        self._retry = retry

    async def complete(self, prompt: str, *, max_new_tokens: int) -> str:
        result = await self._call(
            self._client.text_generation,
            prompt,
            model=self._model,
            max_new_tokens=max_new_tokens,
            details=False,
            stream=False,
        )
        # The SDK's declared return is a union; with details/stream False it is a
        # str. Guard defensively so mypy --strict is satisfied without a cast.
        return result if isinstance(result, str) else str(result)

    async def _call[**P, R](self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
        async for attempt in retrying(self._retry):
            with attempt:
                try:
                    return await self._offload.run(fn, *args, **kwargs)
                except Exception as exc:
                    raise classify_hf_error(exc) from exc
        raise AssertionError("unreachable")  # pragma: no cover


# ---- boundary normalization -----------------------------------------------


def _ndarray_to_lists(result: object, *, batch: int) -> list[list[float]]:
    """Normalize feature_extraction output to ``list[list[float]]``.

    feature_extraction returns a numpy.ndarray. For a batch of N texts it is
    typically shaped (N, dim); for a single string some backends collapse to
    (dim,). We use the array's own ``.tolist()`` (no top-level numpy import) and
    re-wrap a flat vector as a one-row batch.
    """
    to_list = getattr(result, "tolist", None)
    data = to_list() if callable(to_list) else result

    if isinstance(data, list) and data and isinstance(data[0], (int, float)):
        # Flat vector -> wrap as a single-row batch.
        rows: list[list[float]] = [[float(x) for x in data]]
    elif isinstance(data, list):
        rows = [[float(x) for x in row] for row in data]
    else:  # pragma: no cover - unexpected shape
        raise TypeError(f"Unexpected feature_extraction output: {type(result)!r}")

    # Some endpoints collapse a 1-item batch; tolerate but keep it explicit.
    if len(rows) != batch and batch == 1 and len(rows) >= 1:
        return rows[:1]
    return rows
