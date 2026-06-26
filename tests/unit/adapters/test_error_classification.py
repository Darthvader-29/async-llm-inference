"""Table-driven tests for the error classifiers (the most-tested logic in Phase 4).

``classify_botocore_error`` and the provider translators are pure functions:
exception in, exception out. We assert the transient/permanent verdict and that
the original cause is preserved on ``__cause__`` for the chained traceback.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from botocore.exceptions import ClientError, ConnectionClosedError, EndpointConnectionError
from huggingface_hub.errors import HfHubHTTPError, InferenceTimeoutError
from redis.exceptions import (
    AuthenticationError,
    AuthenticationWrongNumberOfArgsError,
    BusyLoadingError,
    DataError,
    OutOfMemoryError,
    ResponseError,
    TryAgainError,
)
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from app.adapters.broker._errors import classify_redis_error
from app.adapters.object_store.errors import classify_botocore_error
from app.adapters.providers._errors import (
    classify_ddgs_error,
    classify_hf_error,
    classify_pinecone_error,
)
from app.domain.exceptions import PermanentUpstreamError, TransientUpstreamError


def _ce(code: str, status: int) -> ClientError:
    response: Any = {
        "Error": {"Code": code, "Message": "m"},
        "ResponseMetadata": {"HTTPStatusCode": status},
    }
    return ClientError(error_response=response, operation_name="PutObject")


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (_ce("InternalError", 500), TransientUpstreamError),
        (_ce("ServiceUnavailable", 503), TransientUpstreamError),
        (_ce("SlowDown", 503), TransientUpstreamError),
        (_ce("ThrottlingException", 429), TransientUpstreamError),
        (_ce("AccessDenied", 403), PermanentUpstreamError),
        (_ce("NoSuchKey", 404), PermanentUpstreamError),
        (_ce("InvalidArgument", 400), PermanentUpstreamError),
        (EndpointConnectionError(endpoint_url="x"), TransientUpstreamError),
        (ConnectionClosedError(endpoint_url="x"), TransientUpstreamError),
        (ValueError("weird"), PermanentUpstreamError),
    ],
)
def test_classify_botocore_error(exc: Exception, expected: type[Exception]) -> None:
    result = classify_botocore_error(exc)
    assert isinstance(result, expected)
    assert result.__cause__ is exc  # original cause preserved for tracebacks


# ---- redis (broker) classifier --------------------------------------------


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        # Connection-layer / timeout / cluster-retry signals -> transient.
        (RedisConnectionError("reset by peer"), TransientUpstreamError),
        (RedisTimeoutError("command timed out"), TransientUpstreamError),
        (BusyLoadingError("loading dataset in memory"), TransientUpstreamError),
        (TryAgainError("TRYAGAIN"), TransientUpstreamError),
        # Auth failures are permanent — and AuthenticationError is a *subclass of
        # ConnectionError*, so this row proves the guard order (it must NOT be
        # swept up as a transient connection blip).
        (AuthenticationError("WRONGPASS"), PermanentUpstreamError),
        (AuthenticationWrongNumberOfArgsError("bad AUTH args"), PermanentUpstreamError),
        # Other protocol / server-state ResponseErrors -> permanent.
        (ResponseError("WRONGTYPE Operation against a key"), PermanentUpstreamError),
        (OutOfMemoryError("OOM command not allowed"), PermanentUpstreamError),
        (DataError("Invalid input"), PermanentUpstreamError),
        # Non-redis exception -> permanent (fail safe, don't retry the unknown).
        (ValueError("not a redis error"), PermanentUpstreamError),
    ],
)
def test_classify_redis_error(exc: Exception, expected: type[Exception]) -> None:
    result = classify_redis_error(exc)
    assert isinstance(result, expected)
    assert result.__cause__ is exc  # original cause preserved for tracebacks


# ---- provider translators -------------------------------------------------


class _StatusError(Exception):
    """Stand-in for a pinecone API exception carrying a numeric ``.status``."""

    def __init__(self, status: int) -> None:
        super().__init__(f"status {status}")
        self.status = status


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (500, TransientUpstreamError),
        (503, TransientUpstreamError),
        (429, TransientUpstreamError),
        (400, PermanentUpstreamError),
        (404, PermanentUpstreamError),
    ],
)
def test_classify_pinecone_status(status: int, expected: type[Exception]) -> None:
    exc = _StatusError(status)
    result = classify_pinecone_error(exc)
    assert isinstance(result, expected)
    assert result.__cause__ is exc


def test_classify_pinecone_connection_name_is_transient() -> None:
    class _ConnectionTimeoutError(Exception):
        pass

    result = classify_pinecone_error(_ConnectionTimeoutError("boom"))
    assert isinstance(result, TransientUpstreamError)


def test_classify_ddgs_ratelimit_is_transient() -> None:
    from ddgs.exceptions import RatelimitException

    exc = RatelimitException("rate limited")
    result = classify_ddgs_error(exc)
    assert isinstance(result, TransientUpstreamError)
    assert result.__cause__ is exc


def test_classify_ddgs_generic_is_permanent() -> None:
    from ddgs.exceptions import DDGSException

    result = classify_ddgs_error(DDGSException("nope"))
    assert isinstance(result, PermanentUpstreamError)


def test_classify_hf_generic_is_permanent() -> None:
    exc = RuntimeError("unexpected")
    result = classify_hf_error(exc)
    assert isinstance(result, PermanentUpstreamError)
    assert result.__cause__ is exc


_HF_REQUEST = httpx.Request("GET", "https://hf.test/model")


def _hf_http(status: int) -> HfHubHTTPError:
    return HfHubHTTPError("boom", response=httpx.Response(status, request=_HF_REQUEST))


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (_hf_http(500), TransientUpstreamError),
        (_hf_http(503), TransientUpstreamError),
        (_hf_http(429), TransientUpstreamError),
        (_hf_http(403), PermanentUpstreamError),
        (_hf_http(404), PermanentUpstreamError),
        (_hf_http(422), PermanentUpstreamError),
        (InferenceTimeoutError("warming up"), TransientUpstreamError),
    ],
)
def test_classify_hf_status(exc: Exception, expected: type[Exception]) -> None:
    result = classify_hf_error(exc)
    assert isinstance(result, expected)
    assert result.__cause__ is exc


def test_classify_ddgs_timeout_is_transient() -> None:
    from ddgs.exceptions import TimeoutException

    exc = TimeoutException("slow")
    result = classify_ddgs_error(exc)
    assert isinstance(result, TransientUpstreamError)
    assert result.__cause__ is exc


def test_classify_pinecone_unknown_is_permanent() -> None:
    # No .status and a class name matching none of timeout/connection/service.
    class _OpaqueError(Exception):
        pass

    exc = _OpaqueError("mystery")
    result = classify_pinecone_error(exc)
    assert isinstance(result, PermanentUpstreamError)
    assert result.__cause__ is exc
