"""Stub SDKs + recording-offloader fixtures for the adapter unit tests.

The stubs are shaped like the *real SDKs* (not like the ports): the boto3 stub
exposes ``head_bucket``/``put_object``/``get_object``; the HF stub returns a
numpy-stand-in with ``.tolist()``; the pinecone stub returns the verified
``{"matches": [...]}`` dict shape; the ddgs stub is a context-manager with
``.text()`` → ``{title, href, body}``. This lets the *real adapter code paths*
(including boundary normalization) run end-to-end with zero network — the
adapter doesn't know it isn't talking to the real SDK.

Adapters are typed against the concrete SDK classes; tests ``cast`` a stub to
the SDK type at the constructor only. The fixtures keep the stub's own type so
tests can read its recorded internals (``.calls``, ``.store``, ``.upserted``).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from app.core.config import RetrySettings
from tests.support.offloader import RecordingOffloader


@pytest.fixture
def offloader() -> RecordingOffloader:
    """Spy offloader: records (fn.__qualname__, args, kwargs) then runs fn inline."""
    return RecordingOffloader()


@pytest.fixture
def retry_zero() -> RetrySettings:
    """Retry policy with base_delay_s=0 so attempt COUNTING is clock-free."""
    return RetrySettings(max_attempts=3, base_delay_s=0.0, max_delay_s=0.0)


class StubS3Client:
    """Minimal boto3-shaped S3 client. Records calls; raises preprogrammed errors."""

    def __init__(self) -> None:
        self.store: dict[str, bytes] = {}
        self.calls: list[str] = []
        self.head_bucket_error: Exception | None = None
        self.created_buckets: list[str] = []

    def head_bucket(self, *, Bucket: str) -> dict[str, Any]:
        self.calls.append("head_bucket")
        if self.head_bucket_error is not None:
            raise self.head_bucket_error
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def create_bucket(self, *, Bucket: str) -> dict[str, Any]:
        self.calls.append("create_bucket")
        self.created_buckets.append(Bucket)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, ContentType: str) -> dict[str, Any]:
        self.calls.append("put_object")
        self.store[Key] = Body
        return {"ResponseMetadata": {"HTTPStatusCode": 200}, "ETag": '"fake"'}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        self.calls.append("get_object")
        return {"Body": _StubBody(self.store[Key])}


class _StubBody:
    """Mimics botocore StreamingBody: a .read()/.close() pair."""

    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data

    def close(self) -> None:
        return None


class _FakeNdarray:
    """Mimics numpy.ndarray.tolist() for the HF embedding normalization path."""

    def __init__(self, rows: list[list[float]]) -> None:
        self._rows = rows

    def tolist(self) -> list[list[float]]:
        return self._rows


class StubInferenceClient:
    """huggingface_hub.InferenceClient stub: feature_extraction + text_generation."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def feature_extraction(self, text: Any, *, model: str | None = None) -> _FakeNdarray:
        self.calls.append("feature_extraction")
        # Return a numpy-like object exposing .tolist() -> (N, 3) batch.
        n = len(text) if isinstance(text, list) else 1
        return _FakeNdarray([[0.1, 0.2, 0.3] for _ in range(n)])

    def text_generation(self, prompt: str, *, model: str | None = None, **_: Any) -> str:
        self.calls.append("text_generation")
        return f"completion::{prompt}"


class StubPineconeIndex:
    """pinecone Index stub exposing .upsert / .query with dict responses."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.upserted: list[dict[str, Any]] = []

    def upsert(self, *, vectors: list[dict[str, Any]], namespace: str) -> dict[str, Any]:
        self.calls.append("upsert")
        self.upserted.extend(vectors)
        return {"upserted_count": len(vectors)}

    def query(self, **_: Any) -> dict[str, Any]:
        self.calls.append("query")
        return {
            "matches": [
                {"id": "a", "score": 0.99, "metadata": {"text": "hi"}},
                {"id": "b", "score": 0.50, "metadata": {"text": "yo"}},
            ],
            "namespace": "ns",
            "usage": {"read_units": 1},
        }


class StubDdgs:
    """ddgs DDGS stub: context-manager + .text() returning {title,href,body}."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def __enter__(self) -> StubDdgs:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def text(self, query: str, **_: Any) -> list[dict[str, str]]:
        self.calls.append("text")
        return [
            {"title": "T1", "href": "https://e.test/1", "body": "B1"},
            {"title": "T2", "href": "https://e.test/2", "body": "B2"},
        ]


@pytest.fixture
def stub_s3() -> StubS3Client:
    return StubS3Client()


@pytest.fixture
def stub_hf() -> StubInferenceClient:
    return StubInferenceClient()


@pytest.fixture
def stub_index() -> StubPineconeIndex:
    return StubPineconeIndex()


@pytest.fixture
def stub_ddgs_factory() -> Callable[[], StubDdgs]:
    stub = StubDdgs()
    return lambda: stub
