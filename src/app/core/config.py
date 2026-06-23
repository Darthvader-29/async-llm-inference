"""Application settings.

Loaded from environment variables prefixed ``AIE_`` (and optionally a ``.env``
file). Nested groups use the ``__`` delimiter, e.g. ``AIE_BROKER__WORKER_CONCURRENCY``
populates ``Settings.broker.worker_concurrency``.

The single most important rule encoded here is the **zero-cloud redirect**: in
any non-prod environment, if no S3 ``endpoint_url`` is configured, object storage
is forced to the local MinIO container (``http://localhost:9000``) with path-style
addressing — so the app NEVER reaches AWS during local dev, tests, or CI.
"""

from __future__ import annotations

from enum import StrEnum
from functools import lru_cache
from typing import Annotated, Self

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

# Local MinIO defaults (the compose service from Phase 3 listens here).
_MINIO_ENDPOINT = "http://localhost:9000"


class Environment(StrEnum):
    """Deployment environment. Drives the zero-cloud redirect and log format."""

    DEV = "dev"
    TEST = "test"
    PROD = "prod"


# ---------------------------------------------------------------------------
# Nested settings groups — plain BaseModel (NOT BaseSettings).
# pydantic-settings populates these from AIE_<GROUP>__<FIELD> env vars.
# ---------------------------------------------------------------------------
class ObjectStoreSettings(BaseModel):
    """S3/MinIO object-store configuration."""

    bucket: str = "aie-artifacts"
    region: str = "us-east-1"
    # When None in a non-prod env, the validator below forces MinIO.
    endpoint_url: str | None = None
    # Path-style (http://host:9000/bucket/key) is required by MinIO; AWS uses
    # virtual-host style by default. The redirect forces path-style for MinIO.
    force_path_style: bool = False
    access_key_id: SecretStr | None = None
    secret_access_key: SecretStr | None = None


class BrokerSettings(BaseModel):
    """Redis Streams broker configuration (consumed in Phase 5)."""

    stream: str = "aie:jobs"
    group: str = "aie-workers"
    dlq: str = "aie:jobs:dlq"
    max_attempts: int = 3  # retry budget before dead-lettering
    block_ms: int = 5_000  # XREADGROUP block timeout (ms)
    reclaim_idle_ms: int = 60_000  # XAUTOCLAIM idle threshold for orphans (ms)
    worker_concurrency: int = 8  # semaphore size = in-flight job ceiling
    max_delivery_count: int = 5  # reclaim bounces before DLQ (Phase 5)
    maxlen: int = 10_000  # approximate XADD MAXLEN trim cap (Phase 5)


class RetrySettings(BaseModel):
    """tenacity backoff parameters (consumed in Phase 2).

    ``base_delay_s = 0`` in tests makes retries instantaneous so the suite can
    COUNT attempts deterministically instead of measuring wall-clock time.
    """

    max_attempts: int = Field(default=3, ge=1)
    base_delay_s: float = Field(default=0.2, ge=0.0)  # initial=; ge=0 lets tests pin to 0
    max_delay_s: float = Field(default=10.0, ge=0.0)  # wait_exponential_jitter(max=)
    exp_base: float = Field(default=2.0, gt=1.0)  # wait_exponential_jitter(exp_base=)
    jitter_s: float = Field(default=1.0, ge=0.0)  # wait_exponential_jitter(jitter=) max jitter


class ProviderSettings(BaseModel):
    """Non-secret AI-provider configuration (consumed in Phase 4 by build_providers).

    Secrets live as flat top-level fields on Settings (huggingface_token,
    pinecone_api_key); this group holds only the non-secret knobs (model ids,
    embedding dimension, index name, search opt-in/region).
    """

    hf_embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"  # 384-dim
    hf_llm_model: str = "HuggingFaceH4/zephyr-7b-beta"
    embedding_dim: int = 384  # must match the real embedding model's dim
    pinecone_index: str = "aie-index"
    enable_web_search: bool = False  # ddgs needs no key → gated by an explicit flag
    search_region: str = "wt-wt"


class Settings(BaseSettings):
    """Top-level application settings.

    Example env vars (all optional; sane dev defaults apply):
        AIE_ENV=dev
        AIE_DATABASE_URL=postgresql+asyncpg://aie:aie@localhost:5432/aie
        AIE_REDIS_URL=redis://localhost:6379/0
        AIE_OBJECT_STORE__BUCKET=aie-artifacts
        AIE_BROKER__WORKER_CONCURRENCY=16
        AIE_RETRY__BASE_DELAY_S=0
        AIE_API_KEYS=key-one,key-two
        AIE_HUGGINGFACE_TOKEN=hf_xxx          # optional; activates real adapter
    """

    model_config = SettingsConfigDict(
        env_prefix="AIE_",
        env_nested_delimiter="__",  # AIE_BROKER__BLOCK_MS -> broker.block_ms
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",  # tolerate unrelated env vars in the process
    )

    # --- environment ---
    env: Environment = Environment.DEV

    # --- connection URLs (driver-qualified; consumed in Phase 3/5) ---
    database_url: str = "postgresql+asyncpg://aie:aie@localhost:5432/aie"
    redis_url: str = "redis://localhost:6379/0"

    # --- nested groups ---
    object_store: ObjectStoreSettings = Field(default_factory=ObjectStoreSettings)
    broker: BrokerSettings = Field(default_factory=BrokerSettings)
    retry: RetrySettings = Field(default_factory=RetrySettings)
    providers: ProviderSettings = Field(default_factory=ProviderSettings)

    # --- concurrency ---
    # Sized ThreadPoolExecutor installed as the loop default executor (Phase 2/6).
    offload_max_workers: int = 32

    # --- auth ---
    # Comma-separated env (AIE_API_KEYS=a,b,c) parsed by the validator below.
    # NoDecode disables pydantic-settings' default JSON decoding so the raw
    # string reaches the validator; frozenset makes the set immutable after load.
    api_keys: Annotated[frozenset[str], NoDecode] = frozenset()

    # --- optional provider secrets (activate real adapters when set) ---
    huggingface_token: SecretStr | None = None
    pinecone_api_key: SecretStr | None = None

    # ------------------------------------------------------------------
    # ZERO-CLOUD REDIRECT — the spec's "Zero-Cloud Isolation" exit criterion.
    # ------------------------------------------------------------------
    @model_validator(mode="after")
    def _force_minio_outside_prod(self) -> Self:
        """In any non-prod env with no explicit S3 endpoint, force local MinIO.

        Runs AFTER all fields are populated/validated. Mutating ``self`` here is
        the documented pattern for ``mode="after"`` validators (they receive and
        return the model instance).
        """
        if self.env is not Environment.PROD and self.object_store.endpoint_url is None:
            self.object_store.endpoint_url = _MINIO_ENDPOINT
            self.object_store.force_path_style = True
        return self

    @field_validator("api_keys", mode="before")
    @classmethod
    def _split_api_keys(cls, v: object) -> object:
        """Parse a comma-separated ``AIE_API_KEYS`` string into a list of keys.

        ``NoDecode`` on the field disables pydantic-settings' default JSON
        decoding of collection-typed fields, so the raw env string reaches this
        validator and we split on commas (``k1,k2,k3``). A value that is already
        a collection (e.g. passed directly in a test) passes through untouched.
        """
        if isinstance(v, str):
            return [item.strip() for item in v.split(",") if item.strip()]
        return v

    @property
    def is_prod(self) -> bool:
        """Convenience flag used by logging config and adapter selection."""
        return self.env is Environment.PROD


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a process-wide cached ``Settings`` instance.

    The composition root (Phase 6) calls this once at startup and injects the
    result everywhere — there is no module-global ``settings`` object. The cache
    simply avoids re-parsing the environment on repeated calls within a process.
    Tests that need a custom environment construct ``Settings(...)`` directly
    (bypassing the cache) or call ``get_settings.cache_clear()``.
    """
    return Settings()
