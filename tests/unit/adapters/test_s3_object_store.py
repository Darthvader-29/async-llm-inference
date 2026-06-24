"""``S3ObjectStore`` behavior: ref format, ensure_bucket 404→create, error
classification, and clock-free retry proof (attempt counting with base_delay=0).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from app.adapters.object_store.s3 import S3ObjectStore
from app.core.config import RetrySettings
from app.domain.exceptions import PermanentUpstreamError, TransientUpstreamError
from tests.support.offloader import RecordingOffloader
from tests.unit.adapters.conftest import StubS3Client

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


def _store(
    stub: StubS3Client, bucket: str, offloader: RecordingOffloader, retry: RetrySettings
) -> S3ObjectStore:
    return S3ObjectStore(cast("S3Client", stub), bucket, offloader, retry)


def _client_error(code: str, status: int) -> ClientError:
    response: Any = {
        "Error": {"Code": code, "Message": "msg"},
        "ResponseMetadata": {"HTTPStatusCode": status},
    }
    return ClientError(error_response=response, operation_name="HeadBucket")


async def test_put_bytes_returns_s3_uri(
    stub_s3: StubS3Client, offloader: RecordingOffloader, retry_zero: RetrySettings
) -> None:
    store = _store(stub_s3, "my-bucket", offloader, retry_zero)
    ref = await store.put_bytes("docs/a.json", b"{}", "application/json")
    assert ref == "s3://my-bucket/docs/a.json"
    assert stub_s3.store["docs/a.json"] == b"{}"


async def test_get_bytes_round_trips_through_streaming_body(
    stub_s3: StubS3Client, offloader: RecordingOffloader, retry_zero: RetrySettings
) -> None:
    stub_s3.store["k"] = b"payload-bytes"
    store = _store(stub_s3, "b", offloader, retry_zero)
    assert await store.get_bytes("k") == b"payload-bytes"


async def test_ensure_bucket_creates_on_404(
    stub_s3: StubS3Client, offloader: RecordingOffloader, retry_zero: RetrySettings
) -> None:
    stub_s3.head_bucket_error = _client_error("404", 404)
    store = _store(stub_s3, "new-bucket", offloader, retry_zero)
    await store.ensure_bucket()
    assert stub_s3.created_buckets == ["new-bucket"]


async def test_ensure_bucket_noop_when_exists(
    stub_s3: StubS3Client, offloader: RecordingOffloader, retry_zero: RetrySettings
) -> None:
    store = _store(stub_s3, "exists", offloader, retry_zero)
    await store.ensure_bucket()
    assert stub_s3.created_buckets == []
    assert "create_bucket" not in stub_s3.calls


async def test_ensure_bucket_403_is_permanent(
    stub_s3: StubS3Client, offloader: RecordingOffloader, retry_zero: RetrySettings
) -> None:
    stub_s3.head_bucket_error = _client_error("403", 403)
    store = _store(stub_s3, "forbidden", offloader, retry_zero)
    with pytest.raises(PermanentUpstreamError):
        await store.ensure_bucket()
    assert stub_s3.created_buckets == []  # never tried to create on 403


async def test_ensure_bucket_transient_5xx_probe_propagates(
    stub_s3: StubS3Client, offloader: RecordingOffloader, retry_zero: RetrySettings
) -> None:
    # A 503 on the existence HEAD must surface as transient (caller/retry decides)
    # and must NOT be mistaken for "bucket missing" -> never creates.
    stub_s3.head_bucket_error = _client_error("ServiceUnavailable", 503)
    store = _store(stub_s3, "b", offloader, retry_zero)
    with pytest.raises(TransientUpstreamError):
        await store.ensure_bucket()
    assert stub_s3.created_buckets == []


async def test_ensure_bucket_connection_blip_probe_propagates(
    stub_s3: StubS3Client, offloader: RecordingOffloader, retry_zero: RetrySettings
) -> None:
    # A connection-layer blip during the probe hits the `except TransientUpstreamError`
    # guard in bucket_exists and propagates without creating the bucket.
    stub_s3.head_bucket_error = EndpointConnectionError(endpoint_url="http://localhost:9000")
    store = _store(stub_s3, "b", offloader, retry_zero)
    with pytest.raises(TransientUpstreamError):
        await store.ensure_bucket()
    assert stub_s3.created_buckets == []


async def test_transient_5xx_retries_then_succeeds(
    stub_s3: StubS3Client, offloader: RecordingOffloader, retry_zero: RetrySettings
) -> None:
    # First put_object raises a 503, second succeeds: attempt COUNTING proves retry.
    calls = {"n": 0}
    real_put = stub_s3.put_object

    def flaky_put(**kwargs: Any) -> dict[str, Any]:
        calls["n"] += 1
        if calls["n"] == 1:
            raise _client_error("ServiceUnavailable", 503)
        return real_put(**kwargs)

    stub_s3.put_object = flaky_put  # type: ignore[method-assign]
    store = _store(stub_s3, "b", offloader, retry_zero)
    ref = await store.put_bytes("k", b"d", "text/plain")
    assert ref == "s3://b/k"
    assert calls["n"] == 2  # exactly one retry — counted, not timed


async def test_transient_exhausts_attempts_then_raises(
    stub_s3: StubS3Client, offloader: RecordingOffloader, retry_zero: RetrySettings
) -> None:
    def always_503(**kwargs: Any) -> dict[str, Any]:
        raise _client_error("ServiceUnavailable", 503)

    stub_s3.put_object = always_503  # type: ignore[method-assign]
    store = _store(stub_s3, "b", offloader, retry_zero)
    with pytest.raises(TransientUpstreamError):
        await store.put_bytes("k", b"d", "text/plain")


async def test_connection_error_is_transient(
    stub_s3: StubS3Client, offloader: RecordingOffloader, retry_zero: RetrySettings
) -> None:
    def conn_fail(**kwargs: Any) -> dict[str, Any]:
        raise EndpointConnectionError(endpoint_url="http://localhost:9000")

    stub_s3.put_object = conn_fail  # type: ignore[method-assign]
    store = _store(stub_s3, "b", offloader, retry_zero)
    with pytest.raises(TransientUpstreamError):
        await store.put_bytes("k", b"d", "text/plain")
