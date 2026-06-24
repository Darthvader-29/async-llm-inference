"""Provider selection — fakes by default, real adapters when keys are configured.

``build_providers`` is the ONLY place that decides fake vs real. Phase 7's
pipelines receive a ``ProviderBundle`` and never branch on implementation.
Selection is per-capability and independent (real embeddings with a fake vector
store is valid); the default — no keys — is all-fakes, the zero-cloud/zero-key
path the spec requires. SDK imports are local to each branch so the default path
never loads the heavy SDKs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from app.adapters.providers.fake import FakeEmbedding, FakeLLM, FakeSearch, FakeVectorStore

if TYPE_CHECKING:
    from app.core.config import Settings
    from app.ports.offloader import SyncOffloader
    from app.ports.providers import (
        EmbeddingProvider,
        LLMProvider,
        SearchProvider,
        VectorStore,
    )


@dataclass(slots=True, frozen=True)
class ProviderBundle:
    """The four provider ports the pipelines depend on, resolved together."""

    embedding: EmbeddingProvider
    vector_store: VectorStore
    llm: LLMProvider
    search: SearchProvider


def build_providers(settings: Settings, offloader: SyncOffloader) -> ProviderBundle:
    """Select real adapters where keys/flags are configured; fakes otherwise."""
    embedding: EmbeddingProvider
    vector_store: VectorStore
    llm: LLMProvider
    search: SearchProvider

    # Embeddings + LLM share the HF token (a flat top-level secret on Settings).
    if settings.huggingface_token is not None:
        from huggingface_hub import InferenceClient

        from app.adapters.providers.huggingface import HuggingFaceEmbedding, HuggingFaceLLM

        client = InferenceClient(token=settings.huggingface_token.get_secret_value())
        embedding = HuggingFaceEmbedding(
            client,
            model=settings.providers.hf_embedding_model,
            dim=settings.providers.embedding_dim,
            offloader=offloader,
            retry=settings.retry,
        )
        llm = HuggingFaceLLM(
            client,
            model=settings.providers.hf_llm_model,
            offloader=offloader,
            retry=settings.retry,
        )
    else:
        embedding = FakeEmbedding(dim=settings.providers.embedding_dim)
        llm = FakeLLM()

    # Vector store: Pinecone if key present, else in-memory fake.
    if settings.pinecone_api_key is not None:
        from pinecone import Pinecone

        from app.adapters.providers.pinecone_store import PineconeVectorStore

        pc = Pinecone(api_key=settings.pinecone_api_key.get_secret_value())
        index = pc.Index(name=settings.providers.pinecone_index)
        vector_store = PineconeVectorStore(index, offloader=offloader, retry=settings.retry)
    else:
        vector_store = FakeVectorStore()

    # Search: real ddgs needs no key, so it's opt-in via an explicit flag.
    if settings.providers.enable_web_search:
        from ddgs import DDGS

        from app.adapters.providers.search import DdgsSearch

        search = DdgsSearch(
            lambda: DDGS(),
            region=settings.providers.search_region,
            offloader=offloader,
            retry=settings.retry,
        )
    else:
        search = FakeSearch()

    return ProviderBundle(
        embedding=embedding,
        vector_store=vector_store,
        llm=llm,
        search=search,
    )
