"""Public port surface (all structural ``typing.Protocol`` contracts).

Importing from ``app.ports`` (not the submodules) keeps adapters and services
decoupled from the file layout and makes the hexagon's inward boundary explicit.
"""

from __future__ import annotations

from app.ports.object_store import ObjectStore
from app.ports.offloader import SyncOffloader
from app.ports.providers import (
    EmbeddingProvider,
    LLMProvider,
    SearchProvider,
    SearchResult,
    VectorMatch,
    VectorStore,
)
from app.ports.queue import JobQueue
from app.ports.repository import JobRepository

__all__ = [
    "EmbeddingProvider",
    "JobQueue",
    "JobRepository",
    "LLMProvider",
    "ObjectStore",
    "SearchProvider",
    "SearchResult",
    "SyncOffloader",
    "VectorMatch",
    "VectorStore",
]
