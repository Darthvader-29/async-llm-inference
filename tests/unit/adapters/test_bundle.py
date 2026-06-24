"""``build_providers`` selection invariant — the fake-vs-real decision seam.

These prove two IRON RULES the rest of the suite never exercised: (1) the default
(no keys) bundle is ALL fakes — the zero-cloud/zero-key path; (2) selection is
per-capability and independent — setting one secret/flag swaps exactly one port
and leaves the others fake. Construction-only: no network. The Pinecone branch is
monkeypatched because ``pc.Index(name=...)`` would otherwise resolve a host
online.
"""

from __future__ import annotations

import pytest
from pydantic import SecretStr

from app.adapters.providers.bundle import build_providers
from app.adapters.providers.fake import FakeEmbedding, FakeLLM, FakeSearch, FakeVectorStore
from app.adapters.providers.huggingface import HuggingFaceEmbedding, HuggingFaceLLM
from app.adapters.providers.pinecone_store import PineconeVectorStore
from app.adapters.providers.search import DdgsSearch
from app.core.config import Environment, ProviderSettings, Settings
from tests.support.offloader import RecordingOffloader


def _settings(**overrides: object) -> Settings:
    # _env_file=None keeps the fixture hermetic (no local .env bleed-through).
    return Settings(env=Environment.DEV, _env_file=None, **overrides)  # type: ignore[arg-type]


def test_default_bundle_is_all_fakes() -> None:
    bundle = build_providers(_settings(), RecordingOffloader())
    assert isinstance(bundle.embedding, FakeEmbedding)
    assert isinstance(bundle.llm, FakeLLM)
    assert isinstance(bundle.vector_store, FakeVectorStore)
    assert isinstance(bundle.search, FakeSearch)


def test_hf_token_selects_real_embedding_and_llm_only() -> None:
    bundle = build_providers(
        _settings(huggingface_token=SecretStr("hf_test_token")), RecordingOffloader()
    )
    assert isinstance(bundle.embedding, HuggingFaceEmbedding)
    assert isinstance(bundle.llm, HuggingFaceLLM)
    # Independence: the other two stay fake.
    assert isinstance(bundle.vector_store, FakeVectorStore)
    assert isinstance(bundle.search, FakeSearch)


def test_pinecone_key_selects_real_vector_store_only(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeIndex:
        pass

    class _FakePinecone:
        def __init__(self, *, api_key: str) -> None:
            self.api_key = api_key

        def Index(self, *, name: str) -> _FakeIndex:  # noqa: N802 - mirrors the pinecone SDK
            return _FakeIndex()

    monkeypatch.setattr("pinecone.Pinecone", _FakePinecone)
    bundle = build_providers(
        _settings(pinecone_api_key=SecretStr("pc_test_key")), RecordingOffloader()
    )
    assert isinstance(bundle.vector_store, PineconeVectorStore)
    # Independence: embedding/llm/search stay fake.
    assert isinstance(bundle.embedding, FakeEmbedding)
    assert isinstance(bundle.llm, FakeLLM)
    assert isinstance(bundle.search, FakeSearch)


def test_enable_web_search_flag_selects_ddgs_only() -> None:
    bundle = build_providers(
        _settings(providers=ProviderSettings(enable_web_search=True)), RecordingOffloader()
    )
    assert isinstance(bundle.search, DdgsSearch)
    # Independence: the other three stay fake.
    assert isinstance(bundle.embedding, FakeEmbedding)
    assert isinstance(bundle.llm, FakeLLM)
    assert isinstance(bundle.vector_store, FakeVectorStore)
