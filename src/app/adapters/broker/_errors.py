"""redis-py error classification — the single source of truth for broker retries.

The broker analogue of ``classify_botocore_error`` (object store) and the
provider translators in ``providers/_errors.py``. A pure function maps any
exception raised by a redis-py Stream call (``XADD`` and friends) into the
project's upstream-error vocabulary: ``TransientUpstreamError`` (retryable) for
connection blips / timeouts / load-shedding signals, ``PermanentUpstreamError``
(fail fast) for auth failures, protocol/response errors, and anything
unrecognized. Keeping it pure (exception in, exception out — no I/O, no logging)
makes it exhaustively table-testable.
"""

from __future__ import annotations

from redis.exceptions import (
    AuthenticationError,
    AuthenticationWrongNumberOfArgsError,
    RedisError,
    TryAgainError,
)
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from app.domain.exceptions import PermanentUpstreamError, TransientUpstreamError

# Auth failures are NEVER retryable, but ``AuthenticationError`` is a subclass of
# ``ConnectionError`` (and ``AuthenticationWrongNumberOfArgsError`` a subclass of
# ``ResponseError``) — so they MUST be matched *before* the transient
# connection-layer check below, or a bad password would be retried forever.
_PERMANENT_REDIS_TYPES: tuple[type[RedisError], ...] = (
    AuthenticationError,
    AuthenticationWrongNumberOfArgsError,
)

# Connection-layer / timeout / "try again" failures are transient: a network
# blip, the server loading its dataset into memory (``BusyLoadingError``), pool
# exhaustion under load (``MaxConnectionsError``), a command timeout, or a
# cluster ``TRYAGAIN`` during resharding. ``BusyLoadingError`` and
# ``MaxConnectionsError`` are ``ConnectionError`` subclasses, so the base types
# below already cover them.
_TRANSIENT_REDIS_TYPES: tuple[type[RedisError], ...] = (
    RedisConnectionError,
    RedisTimeoutError,
    TryAgainError,
)


def classify_redis_error(exc: Exception) -> Exception:
    """Translate a raw redis-py exception into an upstream-error type.

    Returns a *new* exception instance to raise; never raises itself. The
    returned error carries ``cause=exc`` so ``__cause__`` is preserved for the
    chained traceback (see ``UpstreamError`` in the domain layer).

    Rules:
      * Auth failures (bad ACL / password)         -> permanent
      * Connection / timeout / cluster TRYAGAIN    -> transient
      * Other ``RedisError`` (``ResponseError``,
        OOM, ``WRONGTYPE``, protocol/data errors)  -> permanent
      * Anything unrecognized                       -> permanent (fail safe, don't retry blind)
    """
    # 1) Auth failures first: ``AuthenticationError`` is a ``ConnectionError``
    #    subclass, so it must be excluded before the transient check in (2).
    if isinstance(exc, _PERMANENT_REDIS_TYPES):
        return PermanentUpstreamError(f"Redis auth error: {exc}", cause=exc)

    # 2) Connection-layer / timeout / retryable server signal -> transient.
    if isinstance(exc, _TRANSIENT_REDIS_TYPES):
        return TransientUpstreamError(f"Redis transient error: {exc}", cause=exc)

    # 3) Other redis protocol/response errors (WRONGTYPE, OOM, NOGROUP, bad
    #    arguments, ...) -> permanent: a bug or server-state problem that
    #    retrying cannot fix.
    if isinstance(exc, RedisError):
        return PermanentUpstreamError(f"Redis permanent error: {exc}", cause=exc)

    # 4) Truly unexpected (non-redis) -> permanent (don't retry the unknown).
    return PermanentUpstreamError(f"Unexpected Redis error: {exc!r}", cause=exc)
