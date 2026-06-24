"""Determinism + correctness of the four deterministic fakes.

Fakes do no I/O, so these tests need no offloader and no clock — they assert
byte-for-byte reproducibility, cosine ranking, namespace isolation, prompt echo,
and the never-empty search fallback.
"""

from __future__ import annotations

import math

import pytest

from app.adapters.providers.fake import FakeEmbedding, FakeLLM, FakeSearch, FakeVectorStore


async def test_fake_embedding_is_deterministic_and_normalized() -> None:
    emb = FakeEmbedding(dim=384)
    v1 = (await emb.embed(["hello world"]))[0]
    v2 = (await emb.embed(["hello world"]))[0]
    assert v1 == v2  # byte-for-byte reproducible
    assert len(v1) == 384
    assert math.isclose(math.sqrt(sum(x * x for x in v1)), 1.0, rel_tol=1e-6)


async def test_fake_embedding_matches_cross_platform_golden() -> None:
    # Pinned golden so any change to the BLAKE2b -> little-endian float32 ->
    # L2-normalize pipeline (endianness, digest size, salt packing) is a LOUD
    # failure, not a silent vector shift. The values are platform-independent by
    # construction (explicit "<" byte order, hashlib, IEEE-754 ops only).
    golden = [-0.0, -1.251415e-06, 0.0, 0.0, 0.0, 0.0, -0.999999999999, -0.0]
    vec = (await FakeEmbedding(dim=8).embed(["hello world"]))[0]
    assert vec == pytest.approx(golden, abs=1e-9)


async def test_fake_embedding_differs_per_text() -> None:
    emb = FakeEmbedding(dim=64)
    a = (await emb.embed(["cats"]))[0]
    b = (await emb.embed(["dogs"]))[0]
    assert a != b


async def test_fake_vector_store_ranks_by_cosine() -> None:
    emb = FakeEmbedding(dim=64)
    store = FakeVectorStore()
    docs = ["the quick brown fox", "lorem ipsum dolor", "the quick brown dog"]
    vecs = await emb.embed(docs)
    await store.upsert(
        [(f"d{i}", vecs[i], {"text": docs[i]}) for i in range(len(docs))],
        namespace="t",
    )
    q = (await emb.embed(["the quick brown fox"]))[0]
    matches = await store.query(q, top_k=3, namespace="t")
    assert matches[0]["id"] == "d0"  # exact match ranks first
    assert matches[0]["score"] >= matches[1]["score"] >= matches[2]["score"]


async def test_fake_vector_store_namespaces_are_isolated() -> None:
    store = FakeVectorStore()
    await store.upsert([("x", [1.0, 0.0], {})], namespace="A")
    assert await store.query([1.0, 0.0], top_k=5, namespace="B") == []


async def test_fake_llm_echoes_and_bounds_length() -> None:
    llm = FakeLLM()
    out = await llm.complete("What is hexagonal architecture?", max_new_tokens=5)
    assert len(out.split()) <= 5
    full = await llm.complete("What is hexagonal architecture?", max_new_tokens=999)
    assert "hexagonal architecture" in full.lower()


async def test_fake_search_matches_and_never_empty() -> None:
    search = FakeSearch()
    hits = await search.search("vector cosine similarity", max_results=2)
    assert 1 <= len(hits) <= 2
    assert all({"title", "url", "snippet"} <= set(h) for h in hits)
    # Nonsense query still yields fallback hits (pipeline never starves).
    fallback = await search.search("zzz-no-match-xyz", max_results=3)
    assert len(fallback) >= 1
