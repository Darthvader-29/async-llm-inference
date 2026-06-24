"""Provider adapters (Phase 4): fakes (default) + real SDK adapters + selector.

Re-exports the four deterministic fakes, the four real SDK adapters, and the
``ProviderBundle`` / ``build_providers`` selection seam. Importing this package
does NOT import any heavy SDK — the real adapters reference their SDKs only
under ``TYPE_CHECKING`` or lazily inside ``build_providers`` branches.
"""

from __future__ import annotations

from app.adapters.providers.bundle import ProviderBundle, build_providers
from app.adapters.providers.fake import (
    FakeEmbedding,
    FakeLLM,
    FakeSearch,
    FakeVectorStore,
)
from app.adapters.providers.huggingface import HuggingFaceEmbedding, HuggingFaceLLM
from app.adapters.providers.pinecone_store import PineconeVectorStore
from app.adapters.providers.search import DdgsSearch

__all__ = [
    "DdgsSearch",
    "FakeEmbedding",
    "FakeLLM",
    "FakeSearch",
    "FakeVectorStore",
    "HuggingFaceEmbedding",
    "HuggingFaceLLM",
    "PineconeVectorStore",
    "ProviderBundle",
    "build_providers",
]
