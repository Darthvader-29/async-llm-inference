"""Domain exception hierarchy.

Two independent trees, both rooted at ``DomainError``:

1. **Invariant violations** — e.g. ``InvalidTransition`` raised by the
   ``InferenceJob`` state machine when an illegal status change is attempted.
2. **Upstream classification** — ``TransientUpstreamError`` (worth retrying)
   vs ``PermanentUpstreamError`` (do not retry; dead-letter immediately).
   Adapters (Phase 4) translate raw SDK/botocore exceptions into these so the
   retry policy (Phase 2) and broker DLQ logic (Phase 5) never import an SDK.
"""

from __future__ import annotations


class DomainError(Exception):
    """Base class for every error originating in the domain/application core."""


# ---------------------------------------------------------------------------
# Invariant violations
# ---------------------------------------------------------------------------
class InvalidTransition(DomainError):
    """Raised when an illegal job-status transition is attempted.

    Carries the offending ``current`` and ``target`` statuses so the message is
    self-explanatory in logs and test failures.
    """

    def __init__(self, current: object, target: object) -> None:
        # ``object`` typing avoids a circular import with models.py while still
        # producing a precise message (the enum members stringify to their value).
        self.current = current
        self.target = target
        super().__init__(f"illegal job transition: {current} -> {target}")


class JobNotFound(DomainError):
    """Raised when a job id cannot be found in the repository.

    The persistence adapter (Phase 3) translates a missing row into this domain
    error so services/routes never see a raw SQLAlchemy ``NoResultFound``.
    Phase 6's ``GET /v1/jobs/{id}`` maps it to an HTTP 404.
    """

    def __init__(self, job_id: object) -> None:
        self.job_id = job_id
        super().__init__(f"job not found: {job_id}")


# ---------------------------------------------------------------------------
# Upstream failure classification (used by adapters + retry + DLQ)
# ---------------------------------------------------------------------------
class UpstreamError(DomainError):
    """Base for any failure crossing an external (network) boundary.

    Carries an optional ``cause`` — the original SDK/transport exception — so
    structured logs can record it without the core importing the SDK's error
    types. Adapters raise these via ``translate_upstream_error`` (Phase 2),
    e.g. ``raise TransientUpstreamError(msg, cause=e) from e``.
    """

    def __init__(self, message: str, *, cause: BaseException | None = None) -> None:
        super().__init__(message)
        self.cause = cause
        if cause is not None:
            # Mirror ``raise ... from cause`` so ``__cause__`` is set even when an
            # adapter *returns* (rather than raises) the translated error — which
            # is exactly what ``classify_*`` helpers do (Phase 4).
            self.__cause__ = cause


class TransientUpstreamError(UpstreamError):
    """A retryable upstream failure (timeout, 5xx, connection reset, throttle).

    The retry policy (Phase 2) retries ONLY on this type. Adapters must map
    raw transient SDK errors to this before re-raising.
    """


class PermanentUpstreamError(UpstreamError):
    """A non-retryable upstream failure (auth/403, 400 validation, 404).

    Retrying cannot help; the broker (Phase 5) dead-letters immediately.
    """
