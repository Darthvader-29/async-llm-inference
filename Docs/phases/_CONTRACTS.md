<!--
CANONICAL CONTRACTS — the single source of truth for cross-phase symbol names
and signatures. The 9 phase docs were authored in parallel, so each had to
*assume* the contracts owned by other phases. This file resolves those
assumptions. When a phase doc disagrees with this file, the phase doc is wrong
and gets fixed (unless this file is updated deliberately and propagated).

Status legend:
  [LOCKED]   validated against the owning phase doc; authoritative.
  [PROPOSED] best canonical call; confirm when validating the owning phase.
  [PENDING]  not yet decided; resolve when validating the owning phase.
-->

# Canonical Contracts

Cross-phase API surface for the [Asynchronous AI Serving Engine](../implementation-plan.md). Every phase doc must conform to the signatures below. Maintained during the senior-dev validation pass.

## Domain — `app.domain` (owner: Phase 1) — [LOCKED]

### `InferenceJob` — `@dataclass(slots=True)`
Fields (in order): `job_type: JobType`, `payload: dict[str, object]`, `id: UUID = uuid4()`, `status: JobStatus = PENDING`, `attempts: int = 0`, `result_ref: str | None = None`, `error: str | None = None`, `duration_ms: int | None = None`, `created_at: datetime`, `updated_at: datetime`.

> The entity has **no** `started_at` / `finished_at`. Persistence (Phase 3) must map only these fields (+ derive nothing the entity can't supply). Execution timing is the single `duration_ms` int.

Creation (both valid; `.new()` is the canonical call site form):
- `InferenceJob.new(job_type: JobType, payload: dict[str, object]) -> InferenceJob`
- `InferenceJob(job_type=..., payload=...)`

Methods (all mutate in place, bump `updated_at`, raise `InvalidTransition` on illegal move):
- `mark_running() -> None` — PENDING→RUNNING; `attempts += 1`; clears `error`.
- `mark_success(result_ref: str, duration_ms: int | None = None) -> None` — RUNNING→SUCCESS.
- `mark_failed(error: str, duration_ms: int | None = None) -> None` — RUNNING→FAILED.
- `requeue() -> None` — RUNNING→PENDING; does **not** bump `attempts`.
- `is_terminal: bool` (property) — True in SUCCESS/FAILED.

### Enums (`StrEnum`; members UPPER, values lower)
- `JobStatus`: `PENDING="pending"`, `RUNNING="running"`, `SUCCESS="success"`, `FAILED="failed"`.
- `JobType`: `RAG_QUERY="rag_query"`, `EMBED_DOCUMENT="embed_document"`.

### Exceptions — `app.domain.exceptions`
```
DomainError
├── InvalidTransition(current, target)
├── JobNotFound(job_id)
└── UpstreamError(message, *, cause: BaseException | None = None)
    ├── TransientUpstreamError   # retry predicate fires ONLY on this
    └── PermanentUpstreamError   # broker DLQs immediately
```
`UpstreamError(message, *, cause=None)` stores `.cause` **and** sets `__cause__` when `cause` is given — so `classify_*` helpers can `return TransientUpstreamError(msg, cause=e)` (valid expression) and the adapter just `raise`s it. Secrets `huggingface_token`/`pinecone_api_key` stay **flat** on `Settings` (not in `ProviderSettings`). `RetrySettings` uses `Field(ge=…/gt=…)` constraints (so tests can pin delays to `0`).

## Settings — `app.core.config.Settings` (owner: Phase 1) — [LOCKED]

`env_prefix="AIE_"`, `env_nested_delimiter="__"`, `extra="ignore"`. Construct directly; `get_settings()` is `@lru_cache`'d and injected by the container (never imported as a global).

| Path | Type | Default |
|------|------|---------|
| `env` | `Environment` (dev/test/prod) | `dev` |
| `database_url` | `str` | `postgresql+asyncpg://aie:aie@localhost:5432/aie` |
| `redis_url` | `str` | `redis://localhost:6379/0` |
| `object_store` | `ObjectStoreSettings` | factory |
| `object_store.bucket` | `str` | `aie-artifacts` |
| `object_store.region` | `str` | `us-east-1` |
| `object_store.endpoint_url` | `str \| None` | `None` → MinIO outside prod |
| `object_store.force_path_style` | `bool` | `False` → `True` outside prod |
| `object_store.access_key_id` | `SecretStr \| None` | `None` |
| `object_store.secret_access_key` | `SecretStr \| None` | `None` |
| `broker.stream` | `str` | `aie:jobs` |
| `broker.group` | `str` | `aie-workers` |
| `broker.dlq` | `str` | `aie:jobs:dlq` |
| `broker.max_attempts` | `int` | `3` |
| `broker.block_ms` | `int` | `5000` |
| `broker.reclaim_idle_ms` | `int` | `60000` |
| `broker.worker_concurrency` | `int` | `8` |
| `broker.max_delivery_count` | `int` | `5` |
| `broker.maxlen` | `int` | `10000` |
| `retry.max_attempts` | `int` | `3` |
| `retry.base_delay_s` | `float` | `0.2` (tests set `0`) |
| `retry.max_delay_s` | `float` | `10.0` |
| `retry.exp_base` | `float` | `2.0` |
| `retry.jitter_s` | `float` | `1.0` |
| `providers` | `ProviderSettings` | factory |
| `providers.hf_embedding_model` | `str` | `sentence-transformers/all-MiniLM-L6-v2` |
| `providers.hf_llm_model` | `str` | `HuggingFaceH4/zephyr-7b-beta` |
| `providers.embedding_dim` | `int` | `384` |
| `providers.pinecone_index` | `str` | `aie-index` |
| `providers.enable_web_search` | `bool` | `false` |
| `providers.search_region` | `str` | `wt-wt` |
| `offload_max_workers` | `int` | `32` |
| `api_keys` | `Annotated[frozenset[str], NoDecode]` | `frozenset()` (comma-split via before-validator) |
| `huggingface_token` | `SecretStr \| None` | `None` |
| `pinecone_api_key` | `SecretStr \| None` | `None` |

## Ports — `app.ports` (owner: Phase 2) — [LOCKED]

All `typing.Protocol`; all I/O methods `async`. `@runtime_checkable` where a runtime isinstance smoke helps.
- `SyncOffloader.run[**P, R](fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R`
- `JobRepository`: `add(job) -> None` · `get(job_id: UUID) -> InferenceJob` (raises `JobNotFound`) · `update(job) -> None`
- `JobQueue`: `publish(job: InferenceJob) -> None` (queue extracts the pointer; retries are consumer-internal re-XADD, not via this port)
- `ObjectStore` (**bucket bound at construction — NO `bucket` param on any method**): `ensure_bucket() -> None` · `bucket_exists() -> bool` (readiness probe) · `put_bytes(key: str, data: bytes, content_type: str = "application/octet-stream") -> str` (returns `s3://{bucket}/{key}`) · `get_bytes(key: str) -> bytes`
- `EmbeddingProvider`: `dim: int` (attribute) · `embed(texts: list[str]) -> list[list[float]]`
- `LLMProvider`: `complete(prompt: str, *, max_new_tokens: int) -> str` — **no `context` param**; the pipeline folds retrieved context into `prompt`
- `VectorStore`: `upsert(vectors: list[tuple[str, list[float], dict[str, object]]], *, namespace: str) -> None` · `query(vector: list[float], *, top_k: int, namespace: str) -> list[VectorMatch]`
- `SearchProvider`: `search(query: str, *, max_results: int) -> list[SearchResult]`

Provider port methods take **no default args** (callers pass explicitly; fakes + real adapters omit defaults too, so they conform structurally).

Value objects = **`TypedDict`** in `app.ports.providers` (plain dicts at runtime; dict access `m["id"]`):
- `SearchResult(title: str, url: str, snippet: str)`
- `VectorMatch(id: str, score: float, metadata: dict[str, object])` — matched chunk text lives in `metadata["text"]` (NOT a `ScoredChunk` dataclass)

### Test spy — `tests/support/offloader.py` (owner: Phase 2) — [LOCKED]
`RecordingOffloader` implements `SyncOffloader`, runs `fn` **inline**, records each call in `.calls: list[OffloadCall]` where `OffloadCall(qualname: str, args: tuple, kwargs: dict)`. Also exposes `.qualnames -> list[str]`, `.assert_offloaded(qualname) -> OffloadCall` (suffix-matches `.method`), and `.last_run_thread_id`. Phase 4's offload-invariant test asserts on `.qualnames` / `assert_offloaded`.

## Persistence — `app.adapters.persistence` (owner: Phase 3) — [LOCKED]
- `SqlAlchemyJobRepository(session_factory: async_sessionmaker[AsyncSession])` implements `JobRepository` (session-per-operation; `add`/`get`/`update`; `get`/`update` raise `JobNotFound`).
- `build_engine(settings) -> AsyncEngine` (asyncpg; `settings.database_url` is a `str`; pool params are FIXED defaults in `engine.py` — there are NO `settings.db_*` fields) · `build_session_factory(engine)` (`expire_on_commit=False`) · `dispose(engine)`.
- `JobRow` columns map **1:1** to the entity: `id, type, status, payload, result_ref, error, attempts, duration_ms, created_at, updated_at` (+ composite `(status, created_at)` index). **No `started_at`/`finished_at`.**
- CHECK values are **lowercase** (`status in ('pending','running','success','failed')`, `type in ('rag_query','embed_document')`) to match `StrEnum` values.
- Object-store env vars use the nested form **`AIE_OBJECT_STORE__{ENDPOINT_URL,ACCESS_KEY_ID,SECRET_ACCESS_KEY,BUCKET}`** — one set feeds both `Settings.object_store` and the MinIO container. Postgres container uses compose-only `AIE_POSTGRES_{USER,PASSWORD,DB}` (the app uses the full `AIE_DATABASE_URL`).

## Composition root — `app.container.AppContainer` (owner: Phase 6) — [LOCKED]
`@dataclass(slots=True)`; `@classmethod async def create(settings) -> AppContainer`; `async def aclose()` (reverse teardown: executor → redis → engine).
Fields: `settings`, `engine`, `session_factory`, `repository` (= `SqlAlchemyJobRepository(session_factory)`), `redis`, `offloader` (`ThreadOffloader`), `executor` (`build_executor(offload_max_workers)` + `install_default_executor`), `object_store` (`S3ObjectStore(_build_s3_client(...), bucket, offloader, retry)`), `queue` (`StreamProducer(redis, BrokerKeys.from_settings(broker), broker)`), `providers` (`build_providers(settings, offloader)` → `ProviderBundle{embedding, vector_store, llm, search}` from Phase 4).
- `create()` uses Phase 3 `build_engine`/`build_session_factory`; `ensure_bucket()` only when `not settings.is_prod`.
- `_build_s3_client(ObjectStoreSettings)` lives in `container.py` (boto3 client; path-style; boto3 retries disabled).
- API: `get_repository` returns `container.repository` (session-per-operation — no per-request session); routes rely on a `JobNotFound -> 404` exception handler in `app.py`.

## Broker — `app.adapters.broker` (owner: Phase 5) — [LOCKED]
- Class names: **`StreamProducer`** and **`StreamConsumer`** (NOT `RedisStreamProducer`).
- `BrokerKeys.from_settings(settings.broker) -> BrokerKeys(stream, group, dlq)`; `make_consumer_name() -> "{host}-{pid}-{rand}"`; `ensure_group(redis, keys)` (idempotent `XGROUP CREATE … MKSTREAM`, swallow BUSYGROUP).
- `StreamProducer(redis, keys: BrokerKeys, settings: BrokerSettings)` — `publish(job)` (XADD attempt=1), `republish(message: JobMessage) -> str` (retry; attempt pre-incremented), `dead_letter(message, reason) -> str`. Takes **no** offloader/retry.
- `StreamConsumer(redis, keys, settings, repository, processor, producer)` — `start()` (ensure group), `consume_once(*, block_ms=None) -> int` (test seam), `run(stop: asyncio.Event)` (loop + drain), `drain()` (public; awaits in-flight). The worker calls `consumer.run(stop)`.
- Dispatch target (`JobProcessor` Protocol): `async __call__(message: JobMessage) -> None`. Phase 7's `JobProcessor` implements `__call__(message)` delegating to `process(job_id: UUID, attempt: int)`.
- `JobMessage(job_id: UUID, job_type: JobType, attempt: int)` with `to_fields()`/`from_fields()`, `next_attempt()`.

## Open reconciliation items (fix during each phase's validation)

**Resolved**
- ✔ `mark_success/mark_failed(..., duration_ms=None)` — added in Phase 1 (P7 assumption now valid).
- ✔ `InferenceJob.new(...)` — added in Phase 1 (P3/P5/P7 assumption now valid).
- ✔ Upstream exceptions: base `DomainError` + `UpstreamError(cause=…)` — Phase 1 owns; Phase 2 fixed (was redefining base as `AppError`).
- ✔ `ObjectStore` bucket bound at construction — Phase 2 port + diagram + fake + retry example fixed (was bucket-per-call).

**Resolved — Phase 3 (persistence)**
- ✔ `JobRow` maps 1:1 to the entity (dropped `started_at`/`finished_at`, added `updated_at`); status CHECK fixed to lowercase; `engine.py` uses fixed pool defaults (no `settings.db_*`); object-store env unified to `AIE_OBJECT_STORE__*`.

**Resolved — Phase 4 (ports reconciled: Phase 2 updated to match Phase 4's shapes)**
- ✔ Value objects `TypedDict` `SearchResult`/`VectorMatch`; `VectorStore` has `namespace`; `EmbeddingProvider` has `dim`; `LLMProvider.complete(prompt, *, max_new_tokens)`. Phase 2 ports + classDiagram + `__init__` + stubs updated (dropped `ScoredChunk`).
- ✔ `errors.py` rewritten to valid Python (`cause=exc`; no `return … from`).
- ✔ `bundle.py` reads flat secrets `settings.huggingface_token`/`pinecone_api_key`; non-secret config from `settings.providers.*` (`ProviderSettings` added to Phase 1).
- ✔ §8 object-store env vars match `ObjectStoreSettings` (`access_key_id`/`secret_access_key`/`force_path_style`); `put_bytes` content_type default.

**Pending — Phase 7 (pipelines must call the canonical ports)**
- `object_store.put_bytes(key, data, content_type=...)` — **drop the `bucket` arg** (bucket bound at construction).
- `vector_store.upsert(list[tuple[id, values, metadata]], *, namespace=...)` — tuples (not dicts), `namespace` required.
- `vector_store.query(vec, *, top_k=..., namespace=...)` → `list[VectorMatch]`; read via `m["id"]`/`m["score"]`/`m["metadata"]["text"]`.
- `search.search(query, *, max_results=...)` — rename `limit` → `max_results`.
- `llm.complete(prompt, *, max_new_tokens=...)` — **no `context` param**; fold retrieved context into `prompt` first.
- Job creation `InferenceJob.new(...)`; `mark_success(result_ref, duration_ms=...)` / `mark_failed(error, duration_ms=...)`.

**Resolved — Phase 5 (broker)**
- ✔ Names LOCKED `StreamProducer`/`StreamConsumer`. Fixed `job.job_type` (not `job.type`), `job.is_terminal` (not `job.status.is_terminal`), `repo.get` raising `JobNotFound` (consumer try/except + FakeRepository), `InferenceJob.new(job_type, payload)`, §8 defaults (`maxlen` 10000, `reclaim_idle_ms` 60000, flat `AIE_REDIS_URL`). `JobQueue.publish(job)`.

**Resolved — Phase 6 (composition root + API)**
- ✔ `container.py` rewritten to canonical: `StreamProducer` (not `RedisStreamProducer`), `build_engine`/`build_session_factory`, `build_providers`/`ProviderBundle` (field `vector_store`), `_build_s3_client`, added `repository` field; removed duplicate `ProviderBundle`/`_build_providers`/non-existent `make_*` factories. Settings names fixed (`env`/`is_prod`, `redis_url`, `huggingface_token`, `providers.*`). `get_repository → container.repository` (session-per-operation; dropped `get_session`). GET route + `JobNotFound -> 404` handler. `bucket_exists()` added to port for readiness. Tests fixed (`env=`, `publish(job)`, `vector_store`, `InMemoryRepository.get` raises). §8 env names canonicalized.

**Resolved — Phase 7 (worker + pipelines)**
- ✔ Pipelines conformed to locked ports: `search(query, *, max_results=)`, dict-access `SearchResult`/`VectorMatch`, `llm.complete(prompt, *, max_new_tokens=)` (context folded into the prompt), `vector_store` tuples + `namespace`, `object_store.put_bytes(key, data, content_type=)` (no bucket), `JobType.RAG_QUERY`/`EMBED_DOCUMENT`, `payload["text"]`. `PipelineContext` dropped `bucket`.
- ✔ Worker uses the canonical `StreamConsumer(redis, keys, settings.broker, repository, processor, producer)` + `start()` + `run(stop)` (dropped the hand-rolled loop / `handler=` / `ensure_group()`); reads `container.{repository, object_store, queue, redis, providers.{embedding, vector_store, llm, search}}`.
- ✔ `JobProcessor` gained `__call__(message)` → `process(job_id: UUID, attempt)`; `get` catches `JobNotFound`. Recording fakes + 5 test files conformed (UUID ids, dict value objects, no-bucket store).
- ✔ Phase 5 `_drain` → public `drain()` (worker + integration test call it).

**Resolved — Phase 8 (containerization)**
- ✔ Compose/`.env`/§8 cred env → `AIE_OBJECT_STORE__ACCESS_KEY_ID`/`__SECRET_ACCESS_KEY`, `AIE_HUGGINGFACE_TOKEN`, `AIE_PROVIDERS__PINECONE_INDEX`. `smoke.ps1` POST body wrapped in `payload` (Phase 6 `JobSubmission` schema). Commands / `AIE_ENV` / `AIE_DATABASE_URL` / `AIE_REDIS_URL` already correct.

**Resolved — Phase 9 (CI + README)**
- ✔ CI `integration` env + §8 config table canonicalized: `AIE_ENVIRONMENT` → `AIE_ENV`, `AIE_OBJECT_STORE__ACCESS_KEY`/`__SECRET_KEY` → `__ACCESS_KEY_ID`/`__SECRET_ACCESS_KEY` (matches Phase 1 Settings + Phase 8 compose).
- ✔ README port↔adapter table: `RedisStreamProducer` → `StreamProducer` (Phase 5 lock).
- ✔ README quickstart `curl` body wrapped in `payload` (`{"payload": {"job_type": "rag_query", "query": ..., "top_k": 3}}`) — matches Phase 6 `JobSubmission` + Phase 8 `smoke.ps1`.
- ✔ poe catalog reconciled with §10.3 + quickstart + Phases 3/8: `up` = infra-only (`postgres redis minio createbuckets`); `down` = keep volumes, new `down-v` = remove volumes (Phase 8 line 589 split); `smoke` = `pwsh -File scripts/smoke.ps1` (the only smoke entry that exists; `scripts.smoke:main` demoted to an optional alternative). §11 Windows note + §13 DoD helper list updated to match.
- ✔ Exit-criteria → test-file traceability table (§10.1) verified: every referenced path (`test_concurrency.py`, `test_offload_invariant.py`, `test_retry.py`, `test_config.py`, `test_container_lifecycle.py`, `test_processor.py`, `test_pipelines.py`, `test_consumer.py`, `test_jobs.py`, `test_worker_end_to_end.py`, `test_repository_pg.py`, `test_minio_roundtrip.py`, `test_broker_redis.py`) matches a file authored in its cited phase doc.

---

**✅ ALL 9 PHASES VALIDATED — loop complete.** Cross-phase contracts are consistent end-to-end; this ledger is the canonical reference for any future implementation work.
