"""Per-SDK error translators — provider analogues of ``classify_botocore_error``.

Three pure functions map raw SDK exceptions to the upstream-error vocabulary so
the retry policy retries only genuine transients. Each returns a *new* exception
(carrying ``cause=exc`` → ``__cause__``) for the adapter to raise. SDK exception
types are imported lazily *inside* each function so this module loads cleanly in
a fakes-only environment where the heavy SDKs may be absent.
"""

from __future__ import annotations

from app.domain.exceptions import PermanentUpstreamError, TransientUpstreamError


def classify_hf_error(exc: Exception) -> Exception:
    """huggingface_hub errors -> upstream-error vocabulary.

    * InferenceTimeoutError                    -> transient (model warming up / 503)
    * HfHubHTTPError with 5xx / 429 / unknown  -> transient
    * HfHubHTTPError with 4xx (401/403/404/422)-> permanent
    * Anything else                            -> permanent
    """
    try:
        from huggingface_hub.errors import HfHubHTTPError, InferenceTimeoutError
    except ImportError:  # pragma: no cover - real path requires the dep
        return PermanentUpstreamError(f"HF error (hub not installed): {exc!r}", cause=exc)

    if isinstance(exc, InferenceTimeoutError):
        return TransientUpstreamError(f"HF inference timeout: {exc}", cause=exc)

    if isinstance(exc, HfHubHTTPError):
        status = _hf_status(exc)
        if status is None or status >= 500 or status == 429:
            return TransientUpstreamError(f"HF transient HTTP {status}: {exc}", cause=exc)
        return PermanentUpstreamError(f"HF permanent HTTP {status}: {exc}", cause=exc)

    return PermanentUpstreamError(f"HF error: {exc!r}", cause=exc)


def _hf_status(exc: Exception) -> int | None:
    """Best-effort extraction of an HTTP status from an HfHubHTTPError."""
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    return int(status) if isinstance(status, int) else None


def classify_pinecone_error(exc: Exception) -> Exception:
    """pinecone SDK errors -> upstream-error vocabulary, classified by status.

    The SDK's API exceptions carry a ``.status``: 5xx / 429 -> transient,
    4xx -> permanent. Connection-level errors may lack a status, so we fall back
    to a name heuristic (timeout/connection/service -> transient).
    """
    status = getattr(exc, "status", None)
    if isinstance(status, int):
        if status >= 500 or status == 429:
            return TransientUpstreamError(f"Pinecone transient {status}: {exc}", cause=exc)
        return PermanentUpstreamError(f"Pinecone permanent {status}: {exc}", cause=exc)

    name = type(exc).__name__.lower()
    if "timeout" in name or "connection" in name or "service" in name:
        return TransientUpstreamError(f"Pinecone connection error: {exc}", cause=exc)
    return PermanentUpstreamError(f"Pinecone error: {exc!r}", cause=exc)


def classify_ddgs_error(exc: Exception) -> Exception:
    """ddgs errors -> upstream-error vocabulary.

    ``ddgs.exceptions`` defines ``DDGSException`` (base), ``RatelimitException``,
    ``TimeoutException``. Rate-limit and timeout are transient; other DDGS errors
    are permanent.
    """
    try:
        from ddgs.exceptions import DDGSException, RatelimitException, TimeoutException
    except ImportError:  # pragma: no cover - real path requires the dep
        return PermanentUpstreamError(f"ddgs error (not installed): {exc!r}", cause=exc)

    if isinstance(exc, RatelimitException | TimeoutException):
        return TransientUpstreamError(f"ddgs transient: {exc}", cause=exc)
    if isinstance(exc, DDGSException):
        return PermanentUpstreamError(f"ddgs error: {exc}", cause=exc)
    return PermanentUpstreamError(f"ddgs unexpected error: {exc!r}", cause=exc)
