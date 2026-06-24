"""Real adapters with stub SDKs: boundary normalization + error translation.

No network: each adapter runs its real code path against a SDK-shaped stub. We
assert the return-shape mapping (ndarray→lists, dict matches→VectorMatch,
href/body→url/snippet) and that raw SDK errors are translated to upstream errors.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from app.adapters.providers.huggingface import HuggingFaceEmbedding, HuggingFaceLLM
from app.adapters.providers.pinecone_store import PineconeVectorStore
from app.adapters.providers.search import DdgsSearch
from app.core.config import RetrySettings
from app.domain.exceptions import PermanentUpstreamError, TransientUpstreamError
from tests.support.offloader import RecordingOffloader
from tests.unit.adapters.conftest import StubInferenceClient, StubPineconeIndex

if TYPE_CHECKING:
    from collections.abc import Callable

    from huggingface_hub import InferenceClient


async def test_hf_embedding_normalizes_ndarray(
    stub_hf: StubInferenceClient, offloader: RecordingOffloader, retry_zero: RetrySettings
) -> None:
    emb = HuggingFaceEmbedding(
        cast("InferenceClient", stub_hf), model="m", dim=3, offloader=offloader, retry=retry_zero
    )
    out = await emb.embed(["a", "b"])
    assert out == [[0.1, 0.2, 0.3], [0.1, 0.2, 0.3]]  # .tolist() normalized to lists


async def test_hf_llm_returns_str(
    stub_hf: StubInferenceClient, offloader: RecordingOffloader, retry_zero: RetrySettings
) -> None:
    llm = HuggingFaceLLM(
        cast("InferenceClient", stub_hf), model="m", offloader=offloader, retry=retry_zero
    )
    assert await llm.complete("hi", max_new_tokens=8) == "completion::hi"


async def test_pinecone_upsert_payload_shape(
    stub_index: StubPineconeIndex, offloader: RecordingOffloader, retry_zero: RetrySettings
) -> None:
    store = PineconeVectorStore(stub_index, offloader=offloader, retry=retry_zero)
    await store.upsert([("id1", [0.1, 0.2], {"k": "v"})], namespace="ns")
    assert stub_index.upserted == [{"id": "id1", "values": [0.1, 0.2], "metadata": {"k": "v"}}]


async def test_pinecone_query_normalizes_matches(
    stub_index: StubPineconeIndex, offloader: RecordingOffloader, retry_zero: RetrySettings
) -> None:
    store = PineconeVectorStore(stub_index, offloader=offloader, retry=retry_zero)
    matches = await store.query([0.1, 0.2], top_k=2, namespace="ns")
    assert matches == [
        {"id": "a", "score": 0.99, "metadata": {"text": "hi"}},
        {"id": "b", "score": 0.50, "metadata": {"text": "yo"}},
    ]


async def test_ddgs_normalizes_href_and_body(
    stub_ddgs_factory: Callable[[], object],
    offloader: RecordingOffloader,
    retry_zero: RetrySettings,
) -> None:
    search = DdgsSearch(stub_ddgs_factory, region="us-en", offloader=offloader, retry=retry_zero)
    hits = await search.search("q", max_results=2)
    assert hits == [
        {"title": "T1", "url": "https://e.test/1", "snippet": "B1"},
        {"title": "T2", "url": "https://e.test/2", "snippet": "B2"},
    ]


async def test_hf_error_is_translated(
    stub_hf: StubInferenceClient, offloader: RecordingOffloader, retry_zero: RetrySettings
) -> None:
    # A generic (non-HF-typed) error must still be translated to an upstream error.
    def boom(*a: object, **k: object) -> object:
        raise RuntimeError("warming up")

    stub_hf.feature_extraction = boom  # type: ignore[assignment]
    emb = HuggingFaceEmbedding(
        cast("InferenceClient", stub_hf), model="m", dim=3, offloader=offloader, retry=retry_zero
    )
    with pytest.raises((TransientUpstreamError, PermanentUpstreamError)):
        await emb.embed(["x"])
