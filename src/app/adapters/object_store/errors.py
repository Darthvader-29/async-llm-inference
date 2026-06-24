"""Botocore error classification — the single source of truth for S3 retries.

A pure function maps any exception thrown by a ``boto3`` S3 call into the
project's upstream-error vocabulary: ``TransientUpstreamError`` (retryable) for
5xx / throttling / connection blips, ``PermanentUpstreamError`` (fail fast) for
4xx / validation / anything unrecognized. Keeping it pure (exception in,
exception out — no I/O, no logging) makes it exhaustively table-testable.
"""

from __future__ import annotations

from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)
from botocore.exceptions import ConnectionError as BotoConnectionError

from app.domain.exceptions import PermanentUpstreamError, TransientUpstreamError

# Service-level error codes that are safe to retry (load shedding / throttling).
_TRANSIENT_CLIENT_CODES: frozenset[str] = frozenset(
    {
        "InternalError",  # generic S3 5xx
        "ServiceUnavailable",  # 503
        "SlowDown",  # 503 throttling
        "RequestTimeout",  # 400-coded but retryable
        "RequestTimeoutException",
        "ThrottlingException",  # 429-style
        "TooManyRequests",  # 429
    }
)

# Connection-layer botocore errors are always transient (network blips). All of
# these are subclasses of BotoCoreError (verified against the installed botocore).
_TRANSIENT_BOTOCORE_TYPES: tuple[type[BotoCoreError], ...] = (
    EndpointConnectionError,
    ConnectionClosedError,
    ConnectTimeoutError,
    ReadTimeoutError,
    BotoConnectionError,
)


def classify_botocore_error(exc: Exception) -> Exception:
    """Translate a raw boto3/botocore exception into an upstream-error type.

    Returns a *new* exception instance to raise; never raises itself. The
    returned error carries ``cause=exc`` so ``__cause__`` is preserved for the
    chained traceback (see ``UpstreamError`` in the domain layer).

    Rules:
      * HTTP 5xx                      -> transient
      * Throttling / SlowDown / 429   -> transient
      * Connection / timeout errors   -> transient
      * 4xx (403/404/400/validation)  -> permanent
      * Anything unrecognized         -> permanent (fail safe, don't retry blind)
    """
    # 1) Connection-layer failures: no HTTP response at all.
    if isinstance(exc, _TRANSIENT_BOTOCORE_TYPES):
        return TransientUpstreamError(f"S3 connection error: {exc}", cause=exc)

    # 2) Service responses carry a structured error payload.
    if isinstance(exc, ClientError):
        error = exc.response.get("Error", {})
        code = str(error.get("Code", ""))
        meta = exc.response.get("ResponseMetadata", {})
        status = int(meta.get("HTTPStatusCode", 0) or 0)

        if status >= 500 or code in _TRANSIENT_CLIENT_CODES or status == 429:
            return TransientUpstreamError(
                f"S3 transient {status} {code}: {error.get('Message', '')}", cause=exc
            )

        # 403 / 404 / 400 / validation -> permanent.
        return PermanentUpstreamError(
            f"S3 permanent {status} {code}: {error.get('Message', '')}", cause=exc
        )

    # 3) Other BotoCoreError (param validation, no-creds, etc.) -> permanent.
    if isinstance(exc, BotoCoreError):
        return PermanentUpstreamError(f"S3 client error: {exc}", cause=exc)

    # 4) Truly unexpected -> permanent (don't retry the unknown).
    return PermanentUpstreamError(f"Unexpected S3 error: {exc!r}", cause=exc)
