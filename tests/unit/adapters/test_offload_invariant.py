"""The headline test — offloading invariant for EVERY adapter method.

One parametrized test builds each adapter with the ``RecordingOffloader``, calls
each method with representative args, and asserts the spy recorded an offloaded
call whose recorded function is the expected SDK operation. No timing, no
network. Adding a new adapter method = adding one row to the table.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, cast

import pytest

from app.adapters.object_store.s3 import S3ObjectStore
from app.adapters.providers.huggingface import HuggingFaceEmbedding, HuggingFaceLLM
from app.adapters.providers.pinecone_store import PineconeVectorStore
from app.adapters.providers.search import DdgsSearch
from app.core.config import RetrySettings
from tests.support.offloader import RecordingOffloader
from tests.unit.adapters.conftest import (
    StubInferenceClient,
    StubPineconeIndex,
    StubS3Client,
)

if TYPE_CHECKING:
    from huggingface_hub import InferenceClient
    from mypy_boto3_s3.client import S3Client

# A method's coroutine factory + the SDK-op substring expected in the offload log.
AdapterMatrix = dict[str, tuple[Callable[[], Awaitable[None]], str]]


@pytest.fixture
def adapters(
    stub_s3: StubS3Client,
    stub_hf: StubInferenceClient,
    stub_index: StubPineconeIndex,
    stub_ddgs_factory: Callable[[], object],
    offloader: RecordingOffloader,
    retry_zero: RetrySettings,
) -> AdapterMatrix:
    """All adapter method-calls under test, keyed by a readable id.

    Each coroutine, when awaited, must cause the RecordingOffloader to record at
    least one call whose ``fn.__qualname__`` contains the expected SDK op name.
    """
    s3 = S3ObjectStore(cast("S3Client", stub_s3), "bucket", offloader, retry_zero)
    hf_embed = HuggingFaceEmbedding(
        cast("InferenceClient", stub_hf), model="m", dim=3, offloader=offloader, retry=retry_zero
    )
    hf_llm = HuggingFaceLLM(
        cast("InferenceClient", stub_hf), model="m", offloader=offloader, retry=retry_zero
    )
    pine = PineconeVectorStore(stub_index, offloader=offloader, retry=retry_zero)
    ddg = DdgsSearch(stub_ddgs_factory, region="us-en", offloader=offloader, retry=retry_zero)

    async def s3_put() -> None:
        await s3.put_bytes("k", b"d", "text/plain")

    async def s3_get() -> None:
        stub_s3.store["k"] = b"d"
        await s3.get_bytes("k")

    async def s3_ensure() -> None:
        await s3.ensure_bucket()

    async def hf_embed_call() -> None:
        await hf_embed.embed(["alpha", "beta"])

    async def hf_llm_call() -> None:
        await hf_llm.complete("prompt", max_new_tokens=16)

    async def pine_upsert() -> None:
        await pine.upsert([("id1", [0.1, 0.2, 0.3], {"t": "x"})], namespace="ns")

    async def pine_query() -> None:
        await pine.query([0.1, 0.2, 0.3], top_k=2, namespace="ns")

    async def ddg_search() -> None:
        await ddg.search("hello world", max_results=2)

    return {
        "s3.put_bytes": (s3_put, "put_object"),
        # get_bytes offloads the fetch+read CLOSURE (get_object + StreamingBody
        # .read() together, off-loop) — so the recorded callable is the closure,
        # proving the body read never touches the event loop.
        "s3.get_bytes": (s3_get, "_fetch_and_read"),
        "s3.ensure_bucket": (s3_ensure, "head_bucket"),
        "hf.embed": (hf_embed_call, "feature_extraction"),
        "hf.complete": (hf_llm_call, "text_generation"),
        "pinecone.upsert": (pine_upsert, "upsert"),
        "pinecone.query": (pine_query, "query"),
        "ddgs.search": (ddg_search, "text"),  # offloaded closure _run_text calls .text
    }


@pytest.mark.parametrize(
    "method_id",
    [
        "s3.put_bytes",
        "s3.get_bytes",
        "s3.ensure_bucket",
        "hf.embed",
        "hf.complete",
        "pinecone.upsert",
        "pinecone.query",
        "ddgs.search",
    ],
)
async def test_every_adapter_method_offloads(
    adapters: AdapterMatrix, offloader: RecordingOffloader, method_id: str
) -> None:
    """INVARIANT: every adapter method routes its SDK work through offloader.run.

    Proven by the RecordingOffloader spy — no clock, no sleep, no network.
    """
    coro_factory, expected_op = adapters[method_id]

    assert offloader.calls == [], "offloader should start clean"
    await coro_factory()

    # 1) The spy recorded at least one offloaded call.
    assert offloader.calls, f"{method_id} did not offload anything"

    # 2) At least one recorded call targets the expected SDK operation.
    recorded_names = offloader.qualnames
    assert any(expected_op in name for name in recorded_names), (
        f"{method_id} offloaded {recorded_names}, expected one to contain '{expected_op}'"
    )


def test_adapter_matrix_is_complete(adapters: AdapterMatrix) -> None:
    """Guard: the matrix covers exactly the eight known adapter methods.

    Written out explicitly so a renamed/missing method is a loud failure rather
    than a silently-shrunken parametrize set.
    """
    assert set(adapters) == {
        "s3.put_bytes",
        "s3.get_bytes",
        "s3.ensure_bucket",
        "hf.embed",
        "hf.complete",
        "pinecone.upsert",
        "pinecone.query",
        "ddgs.search",
    }
