"""Retry policy: a Settings-driven tenacity AsyncRetrying factory.

Composition order across the engine is **retry wraps offload**: the retry loop
re-issues a fresh ``offloader.run(...)`` on each attempt. Adapters translate raw
SDK errors into ``TransientUpstreamError`` / ``PermanentUpstreamError`` first;
this policy retries *only* the transient kind.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, NoReturn

from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from app.domain.exceptions import (
    PermanentUpstreamError,
    TransientUpstreamError,
)

if TYPE_CHECKING:
    # Imported only for typing to keep core import-light; RetrySettings is a
    # nested model on Settings (Phase 1).
    from app.core.config import RetrySettings


def retrying(settings: RetrySettings) -> AsyncRetrying:
    """Build an ``AsyncRetrying`` from retry settings.

    Parameters are pulled from ``settings`` so tests can construct a policy with
    ``base_delay_s=0`` (instant attempts) and assert the *attempt count* without
    ever measuring wall-clock time.

    Configuration:
      * ``stop_after_attempt(max_attempts)`` — total tries, not *additional*
        retries. ``max_attempts=3`` ⇒ at most 3 invocations.
      * ``wait_exponential_jitter(initial, max, exp_base, jitter)`` — backoff
        ``min(initial * exp_base**n + uniform(0, jitter), max)``.
      * ``retry_if_exception_type(TransientUpstreamError)`` — retry ONLY
        transient upstream errors; everything else propagates immediately.
      * ``reraise=True`` — on exhaustion, re-raise the *last* underlying
        exception (e.g. ``TransientUpstreamError``) rather than wrapping it in a
        ``tenacity.RetryError``. Callers/services then see a domain error.
    """
    return AsyncRetrying(
        stop=stop_after_attempt(settings.max_attempts),
        wait=wait_exponential_jitter(
            initial=settings.base_delay_s,
            max=settings.max_delay_s,
            exp_base=settings.exp_base,
            jitter=settings.jitter_s,
        ),
        retry=retry_if_exception_type(TransientUpstreamError),
        reraise=True,
    )


def translate_upstream_error(
    error: Exception,
    *,
    is_transient: Callable[[Exception], bool],
    context: str,
) -> NoReturn:
    """Re-raise a raw SDK ``error`` as a transient or permanent upstream error.

    Adapters call this in their ``except`` block; ``is_transient`` encodes the
    SDK-specific rule (e.g. "5xx or connection error → transient"). This keeps
    SDK error taxonomy at the edge and feeds the retry predicate a clean
    domain type.

    Always raises — annotated ``NoReturn`` so mypy knows control does not return.
    """
    if is_transient(error):
        raise TransientUpstreamError(f"{context}: {error}", cause=error) from error
    raise PermanentUpstreamError(f"{context}: {error}", cause=error) from error
