"""AppContainer — the one composition root, shared by API and worker.

NOTHING here imports FastAPI/Starlette. The API holds an instance on
``app.state.container``; the worker (Phase 7) constructs one in ``__main__``.
This is the *only* module that constructs concrete adapters and the *only*
place that decides fake-vs-real per provider.
"""

from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import boto3
import redis.asyncio as aioredis
from botocore.config import Config
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.adapters.broker.keys import BrokerKeys
from app.adapters.broker.producer import StreamProducer
from app.adapters.object_store.s3 import S3ObjectStore
from app.adapters.persistence.engine import build_engine, build_session_factory
from app.adapters.persistence.repository import SqlAlchemyJobRepository
from app.adapters.providers.bundle import ProviderBundle, build_providers
from app.core.concurrency import (
    ThreadOffloader,
    build_executor,
    install_default_executor,
)
from app.core.config import ObjectStoreSettings, Settings
from app.ports.object_store import ObjectStore
from app.ports.offloader import SyncOffloader
from app.ports.queue import JobQueue
from app.ports.repository import JobRepository

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class AppContainer:
    """Owns every long-lived resource for one process (API or worker).

    Construct via ``await AppContainer.create(settings)``; destroy via
    ``await container.aclose()``. Fields are concrete *adapters* typed as ports,
    so consumers depend only on the Protocols.
    """

    settings: Settings
    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]
    repository: JobRepository
    redis: aioredis.Redis
    offloader: SyncOffloader
    executor: ThreadPoolExecutor
    object_store: ObjectStore
    queue: JobQueue
    providers: ProviderBundle
    # Bookkeeping: whether create() installed our executor as the loop default.
    _installed_executor: bool = field(default=False, repr=False)

    # ------------------------------------------------------------------ #
    # Construction
    # ------------------------------------------------------------------ #
    @classmethod
    async def create(cls, settings: Settings) -> AppContainer:
        """Wire all resources. Order matters; aclose() reverses it.

        Wiring order (low-level first):
          1. executor  -> install as loop default (powers asyncio.to_thread)
          2. offloader -> ThreadOffloader dispatches into that executor
          3. engine + session_factory + repository (SQLAlchemy async)
          4. redis client
          5. object_store (S3/MinIO) via offloader+retry; ensure_bucket() in dev
          6. queue (Redis Streams producer)
          7. providers (fake vs real, chosen from Settings)
        """
        # 1) Sized thread pool -> install as the loop's default executor so that
        #    every bare ``asyncio.to_thread(...)`` lands in *this* bounded pool.
        loop = asyncio.get_running_loop()
        executor = build_executor(settings.offload_max_workers)
        install_default_executor(loop, executor)
        installed_executor = True

        # 2) Offloader port. ThreadOffloader is a thin ``to_thread`` passthrough;
        #    the default-executor install above bounds it.
        offloader: SyncOffloader = ThreadOffloader()

        # 3) Async SQLAlchemy engine + session factory + repository.
        #    The repository is session-per-operation, so ONE instance is shared
        #    process-wide (it holds the factory, not a live session).
        engine = build_engine(settings)
        session_factory = build_session_factory(engine)
        repository: JobRepository = SqlAlchemyJobRepository(session_factory)

        # 4) Redis client (decode_responses=False: the broker controls encoding).
        redis = aioredis.Redis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=False,
        )

        # 5) Object store. Build the boto3 client, wrap it in the offloader+retry
        #    adapter; create the bucket in non-prod so demos "just work".
        s3_client = _build_s3_client(settings.object_store)
        object_store: ObjectStore = S3ObjectStore(
            s3_client, settings.object_store.bucket, offloader, settings.retry
        )
        if not settings.is_prod:
            await object_store.ensure_bucket()  # dev/test convenience only

        # 6) Queue producer (Redis Streams). Pointers only; PG is source of truth.
        keys = BrokerKeys.from_settings(settings.broker)
        queue: JobQueue = StreamProducer(redis, keys, settings.broker)

        # 7) Providers: fake-vs-real decided HERE, once (Phase 4's build_providers;
        #    HF/Pinecone SDK clients are constructed lazily inside it).
        providers = build_providers(settings, offloader)

        container = cls(
            settings=settings,
            engine=engine,
            session_factory=session_factory,
            repository=repository,
            redis=redis,
            offloader=offloader,
            executor=executor,
            object_store=object_store,
            queue=queue,
            providers=providers,
            _installed_executor=installed_executor,
        )
        logger.info(
            "AppContainer created (env=%s, providers=%s)",
            settings.env,
            cls._provider_modes(settings),
        )
        return container

    # ------------------------------------------------------------------ #
    # Fake-vs-real provider selection (the actual wiring lives in Phase 4's
    # build_providers; this is only a diagnostic summary for logging/tests).
    # ------------------------------------------------------------------ #
    @staticmethod
    def _provider_modes(settings: Settings) -> dict[str, str]:
        """Diagnostic: which providers are 'real' vs 'fake' for this run."""
        hf = "real" if settings.huggingface_token else "fake"
        pc = "real" if settings.pinecone_api_key else "fake"
        return {
            "embedding": hf,
            "llm": hf,
            "vector": pc,
            # ddgs needs no key; default to fake unless explicitly enabled.
            "search": "real" if settings.providers.enable_web_search else "fake",
        }

    # ------------------------------------------------------------------ #
    # Teardown — strict REVERSE order, best-effort, idempotent.
    # ------------------------------------------------------------------ #
    async def aclose(self) -> None:
        """Release every resource in reverse construction order.

        Providers/queue/object_store hold no OS resources of their own beyond
        the shared redis client + executor, so the teardown set is:
            executor (shutdown, no-wait) -> redis (close) -> engine (dispose).
        Each step is guarded so one failure cannot strand the others.
        """
        # 1) Executor first: stop accepting new offloaded work. cancel_futures
        #    drops queued-but-not-started tasks; in-flight threads finish.
        try:
            self.executor.shutdown(wait=False, cancel_futures=True)
        except Exception:  # pragma: no cover - defensive
            logger.exception("executor shutdown failed")
        finally:
            # We installed this executor as the loop's default in create().
            # asyncio offers NO public way to *clear* a default executor:
            # ``set_default_executor(None)`` raises TypeError on 3.11+ (Phase 2
            # forbids that call). We deliberately do not attempt it — aclose()
            # is terminal in the API/worker (the loop closes next), and a
            # re-created container's install_default_executor() overwrites the
            # default. Clearing the flag here keeps aclose() idempotent and,
            # crucially, lets the redis + engine teardown below ALWAYS run.
            self._installed_executor = False

        # 2) Redis connection pool.
        try:
            await self.redis.aclose()  # redis>=5 async client close
        except Exception:
            logger.exception("redis close failed")

        # 3) Database engine: returns + closes all pooled connections.
        try:
            await self.engine.dispose()
        except Exception:
            logger.exception("engine dispose failed")

        logger.info("AppContainer closed")


def _build_s3_client(s: ObjectStoreSettings) -> S3Client:
    """Construct the boto3 S3 client the object-store adapter wraps.

    Path-style addressing for MinIO; boto3's own retries disabled because
    tenacity owns retries at the adapter boundary (Phase 4). The endpoint is
    already forced to MinIO in non-prod by the Phase-1 redirect validator.
    """
    return boto3.client(
        "s3",
        endpoint_url=s.endpoint_url,
        aws_access_key_id=(s.access_key_id.get_secret_value() if s.access_key_id else None),
        aws_secret_access_key=(
            s.secret_access_key.get_secret_value() if s.secret_access_key else None
        ),
        region_name=s.region,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path" if s.force_path_style else "auto"},
            retries={"max_attempts": 0},  # WE own retries (tenacity); disable boto3's
        ),
    )
