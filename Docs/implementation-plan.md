# Implementation Plan: Asynchronous AI Serving Engine

> Multi-phase implementation plan for the spec in [problem-statement.md](problem-statement.md).
> Status: **planned — not yet implemented.**

## Context

`Docs/problem-statement.md` specifies a production-grade, framework-agnostic **Asynchronous AI Serving Engine** built on Hexagonal (Ports & Adapters) principles — a portfolio/interview-facing project. The repo is empty (only README + the spec), so this is a greenfield build. The engine decouples ingestion (FastAPI → 202 + execution ID) from execution (background workers pulling from Redis), with PostgreSQL for state/audit, MinIO for zero-cloud object storage, and adapter-wrapped external AI SDKs (Pinecone, HuggingFace, boto3, search).

**Hard requirements from the spec:**
- All sync SDK I/O offloaded via `asyncio.to_thread` (optimized thread pool); every network boundary has exponential-backoff retries.
- No global singletons; class-based adapters implementing explicit ports; FastAPI lifespan-driven DI.
- Exit criteria: deterministic (clock-free) tests proving thread-executor wrapping via DI overrides; zero-cloud isolation (dev auto-redirects S3 → MinIO); zero resource leaks on startup/teardown.

**Decisions confirmed:**
1. **Broker**: custom asyncio worker on Redis Streams (redis-py asyncio: XADD / XREADGROUP consumer groups / XACK / XAUTOCLAIM reclaim, semaphore-bounded backpressure). No Celery/arq.
2. **AI adapters**: real SDK adapter code (Pinecone, HuggingFace, boto3, DDGS search) wrapped in `to_thread`; deterministic in-process **fake providers** are the dev/demo/test default (zero keys, zero cloud); real adapters activate when keys are configured.
3. **Toolchain**: uv + ruff + mypy (strict) + pytest/pytest-asyncio, Python 3.12+.

## Locked architectural decisions

- **Ports = `typing.Protocol`** (structural typing; adapters and fakes conform without importing the abstraction; mypy --strict checks conformance at injection sites).
- **Retry = `tenacity`** (`wait_exponential_jitter`, `stop_after_attempt`, retry only on `TransientUpstreamError`); policy params come from Settings so tests set `base_delay_s=0` and count attempts — never measure time. Composition order: **retry wraps offload** (each attempt re-offloads).
- **Composition root = plain `@dataclass AppContainer`** with `async create(settings)` / `aclose()`; shared verbatim by API (lifespan) and worker (`__main__`). FastAPI only holds it on `app.state`; core never imports FastAPI.
- **Offloader discipline**: every adapter receives a `SyncOffloader` port; default impl calls `asyncio.to_thread`; composition root installs a sized `ThreadPoolExecutor` via `loop.set_default_executor(...)` (to_thread dispatches there → spec letter + sized pool in one path). Tests inject a `RecordingOffloader` spy → deterministic proof of wrapping.
- **Stream messages are pointers** (`{job_id, job_type, attempt}`); PostgreSQL is the single source of truth for payload/status. At-least-once delivery handled by an idempotency guard (skip if job already terminal).

## Repo layout

```
src/app/
├── container.py                 # AppContainer composition root (shared API+worker)
├── core/        config.py, concurrency.py (ThreadOffloader), retry.py, logging.py
├── domain/      models.py (InferenceJob, JobStatus, JobType), chunking.py, exceptions.py
├── ports/       offloader.py, repository.py, queue.py, object_store.py, providers.py
├── adapters/
│   ├── persistence/  tables.py, repository.py, engine.py     # SQLAlchemy 2.0 async
│   ├── broker/       producer.py, consumer.py                # Redis Streams
│   ├── object_store/ s3.py                                   # boto3 via offloader
│   └── providers/    fake.py, huggingface.py, pinecone_store.py, search.py
├── services/    ingestion.py, processor.py, pipelines.py
├── api/         app.py (factory+lifespan), dependencies.py, auth.py, schemas.py, routes/{jobs,health}.py
└── worker/      __main__.py, runner.py
migrations/  (alembic async)   tests/{conftest.py, support/, unit/, integration/}
docker/Dockerfile   docker-compose.yml   .env.example   .github/workflows/ci.yml   pyproject.toml
```

**Dependencies:** fastapi, uvicorn[standard], pydantic, pydantic-settings, sqlalchemy[asyncio], asyncpg, alembic, redis>=5, boto3, tenacity, structlog, huggingface-hub, pinecone, ddgs. Dev: pytest, pytest-asyncio, pytest-cov, httpx, asgi-lifespan, fakeredis, aiosqlite, ruff, mypy, boto3-stubs[s3], poethepoet (cross-platform task runner — no Makefile/PowerShell divergence).

---

## Phase 1 — Scaffold, toolchain, settings, domain core

**Files:** `pyproject.toml`, `.gitignore`, `.gitattributes` (`* text=auto eol=lf`), `.python-version`, `src/app/core/{config,logging}.py`, `src/app/domain/{models,exceptions}.py`, `tests/conftest.py`, `tests/unit/test_{config,domain}.py`

- pyproject: `requires-python >= 3.12`, ruff (lint+format, `line-ending = "lf"`), mypy strict + pydantic plugin, pytest `asyncio_mode = "auto"` + `integration` marker, `[tool.poe.tasks]`.
- `Settings(BaseSettings)` with `env_prefix="AIE_"`, nested groups: database/redis URLs, `ObjectStoreSettings`, `BrokerSettings` (stream/group/dlq names, max_attempts=3, block_ms, reclaim_idle_ms, worker_concurrency=8), `RetrySettings`, `offload_max_workers=32`, `api_keys`, optional provider secrets (`SecretStr | None`). `Environment(StrEnum)`: dev|test|prod.
- **Zero-cloud redirect**: `@model_validator(mode="after")` — `env != prod` and no `endpoint_url` → force `http://localhost:9000` + path-style. Pure-unit-testable.
- Domain: `InferenceJob` as `@dataclass(slots=True)` (no pydantic in domain) with `mark_running()/mark_success()/mark_failed()/requeue()` enforcing `PENDING→RUNNING→SUCCESS|FAILED` (+ `RUNNING→PENDING` for retry); illegal moves raise `InvalidTransition`. `JobType`: `rag_query`, `embed_document`.

**Verify:** `uv sync; uv run poe check` (ruff+mypy+pytest); transition-matrix and redirect unit tests green.

## Phase 2 — Concurrency core, retry policy, all ports

**Files:** `src/app/ports/*.py`, `src/app/core/{concurrency,retry}.py`, `tests/support/{offloader,fakes}.py`, `tests/unit/test_{concurrency,retry}.py`

- `SyncOffloader` Protocol: `async def run[**P, R](self, fn, /, *args, **kwargs) -> R`. `ThreadOffloader` = literal `asyncio.to_thread` passthrough.
- `RecordingOffloader` spy (records `fn.__qualname__` + args, executes inline → fully deterministic). Two clock-free assertions: (1) spy proves adapter SDK calls route through the offloader boundary; (2) thread-identity test proves `ThreadOffloader` actually leaves the loop thread.
- `retrying(settings) -> AsyncRetrying`: `stop_after_attempt`, `wait_exponential_jitter`, `retry_if_exception_type(TransientUpstreamError)`, `reraise=True`. Adapters translate raw SDK errors → `TransientUpstreamError`/`PermanentUpstreamError` first.
- Ports: `JobRepository` (add/get/update), `JobQueue` (publish), `ObjectStore` (ensure_bucket/put_bytes/get_bytes), `EmbeddingProvider`, `LLMProvider`, `VectorStore`, `SearchProvider`.

**Verify:** spy pattern proven on a toy adapter; retry tests count attempts with `base_delay_s=0`.

## Phase 3 — Infra compose + persistence (SQLAlchemy, Alembic, repository)

**Files:** `docker-compose.yml` (infra only), `.env.example`, `src/app/adapters/persistence/*.py`, `alembic.ini`, `migrations/`, `tests/unit/test_repository_sqlite.py`, `tests/integration/test_repository_pg.py`

- Compose: `postgres:16-alpine`, `redis:7-alpine`, `minio/minio` (+ one-shot `minio/mc` bucket-init), healthchecks, **named volumes** (Windows-friendly).
- `JobRow`: UUID pk, type/status (indexed), `payload JSONB.with_variant(JSON, "sqlite")` (enables aiosqlite unit tests), result_ref, error, attempts, timestamps, duration_ms, composite `(status, created_at)` index.
- Repository maps row ↔ domain entity (no ORM leakage); Alembic async template reading `Settings().database_url`.

**Verify:** compose infra healthy; `alembic upgrade head` works; sqlite unit tests need no Docker; PG integration round-trip via `pytest -m integration`.

## Phase 4 — Object store + provider adapters (fakes first, real SDKs second)

**Files:** `src/app/adapters/object_store/s3.py`, `src/app/adapters/providers/*.py`, `tests/unit/adapters/*`, `tests/integration/test_minio_roundtrip.py`

- `S3ObjectStore(client, bucket, offloader, retry)` — every boto3 call goes `retrying → offloader.run`; botocore errors classified transient (5xx/conn) vs permanent (403/validation); returns `s3://bucket/key` refs. `ensure_bucket()` called by composition root in dev only.
- Fakes (deterministic, dev/test default): `FakeEmbedding` (seeded hash vector), `FakeVectorStore` (in-memory cosine), `FakeLLM` (templated answer echoing context), `FakeSearch` (canned corpus).
- Real adapters: HF sync `InferenceClient`, `pinecone` SDK, `ddgs` — constructed with `(sdk_client, offloader, retry_settings)`, never touching SDKs except through `offloader.run`.
- **Headline test:** parametrized module asserting the offloading invariant for every adapter method via `RecordingOffloader` + stub SDK objects.

**Verify:** unit suite green with zero network; MinIO put/get integration test through real `ThreadOffloader`.

## Phase 5 — Redis Streams broker (producer + consumer)

**Files:** `src/app/adapters/broker/{producer,consumer}.py`, `tests/unit/broker/*`, `tests/integration/test_broker_redis.py`

- Keys: stream `aie:jobs`, group `aie-workers`, DLQ `aie:jobs:dlq`; consumer name `{host}-{pid}-{rand}`. Idempotent `XGROUP CREATE ... MKSTREAM` (swallow BUSYGROUP). Producer XADD with approximate `maxlen` trim.
- Consume loop: reclaim orphans (`XAUTOCLAIM` idle > threshold) → compute free capacity (concurrency − in-flight = **backpressure**) → `XREADGROUP count=budget block=block_ms` → spawn tracked tasks. `consume_once()` is the deterministic test seam.
- Semantics: success → XACK; transient failure with attempts left → XACK + re-XADD attempt+1 + job back to PENDING; permanent/exhausted → DLQ + XACK + FAILED in PG. Reclaimed messages over max delivery → DLQ. Idempotency guard: processor re-reads row, ack-and-skip if terminal.
- Graceful shutdown: `asyncio.Event` stop → loop exits ≤ block_ms → `gather` in-flight drain → close redis.

**Verify:** unit tests on `fakeredis.aioredis` (publish → consume_once → assert XACK/processor/retry/DLQ); drain test gated by `asyncio.Event` (no sleeps). Known risk: if fakeredis lacks XAUTOCLAIM fidelity, reclaim test moves to integration tier.

## Phase 6 — Composition root + FastAPI ingestion API

**Files:** `src/app/container.py`, `src/app/services/ingestion.py`, `src/app/api/**`, `tests/unit/api/*`, `tests/unit/test_container_lifecycle.py`

- `AppContainer` dataclass (settings, engine, session_factory, redis, offloader, sized executor, object_store, queue, provider bundle — fake vs real chosen here by configured keys). `aclose()` tears down in reverse; lifespan stores it on `app.state`.
- `create_app()` factory (`uvicorn --factory`); `Depends(get_container)` reads `request.app.state.container` — no module globals.
- Auth: `APIKeyHeader("X-API-Key")` + `secrets.compare_digest` → 401.
- Schemas: pydantic v2 discriminated union on `job_type` (`RagQueryPayload | EmbedDocumentPayload`); `JobAccepted(job_id, status, status_url)` 202; full `JobStatusResponse`.
- `POST /v1/jobs`: insert PENDING row → `queue.publish` (retried) → 202 + id. `GET /v1/jobs/{id}`; `GET /health` (live) + `/health/ready` (SELECT 1, PING, head_bucket).

**Verify:** httpx `ASGITransport` + `asgi-lifespan` + `dependency_overrides` with all-fakes container (202 path, 401, 422). **Leak test (exit criterion 3):** stub clients with `closed` flags → `aclose()` → all closed, executor shut down; integration variant asserts `engine.pool.checkedout() == 0` after lifespan exit.

## Phase 7 — Worker process + pipelines

**Files:** `src/app/worker/{__main__,runner}.py`, `src/app/services/{processor,pipelines}.py`, `src/app/domain/chunking.py`, `tests/unit/test_{processor,pipelines}.py`, `tests/integration/test_worker_end_to_end.py`

- `runner.py` reuses `AppContainer.create` (composition-root reuse is the architectural point). **Windows signal fallback:** `loop.add_signal_handler` raises `NotImplementedError` on Proactor → fall back to `signal.signal` + `call_soon_threadsafe(stop.set)` + `KeyboardInterrupt` drain path; Linux containers use the primary path (compose `stop_grace_period: 30s`).
- `JobProcessor.process(job_id, attempt)`: load → idempotency guard → mark_running → `PIPELINES[job_type].run(job, ctx)` → mark_success(result_ref, duration via `time.monotonic()`) or raise to broker retry/DLQ logic.
- Pipelines: `rag_query` = search → embed snippets → vector upsert → embed query → vector query → LLM complete → result JSON to object store (**exercises all five provider ports + object store sequentially**); `embed_document` = chunk (pure fn) → embed → upsert → manifest to object store.

**Verify:** processor unit tests with fakes assert full status lifecycle + artifact bytes + error path; pipeline tests assert port call order via recording fakes; manual demo: infra up + api + worker in two tabs → POST rag_query → GET shows SUCCESS → artifact in MinIO console (:9001), zero keys.

## Phase 8 — App containerization + full-stack compose

**Files:** `docker/Dockerfile`, updated `docker-compose.yml`, final `.env.example`, `scripts/smoke.ps1`

- Dockerfile: uv multi-stage (`ghcr.io/astral-sh/uv:python3.12-bookworm-slim` builder, `uv sync --frozen --no-dev`, layer-cached deps → `python:3.12-slim` runtime, non-root). Same image for api (uvicorn) and worker (`python -m app.worker`).
- Compose adds: one-shot `migrate` (alembic), `api` + `worker` gated on `migrate: service_completed_successfully` + healthy infra; in-network env overrides (`postgres`, `minio:9000` hosts).

**Verify:** `docker compose up --build -d` → all healthy; `smoke.ps1` POSTs and polls to terminal state (manual demo script — keeps the test suite clock-free); `docker compose down` leaves nothing dangling.

## Phase 9 — CI, README, polish

**Files:** `.github/workflows/ci.yml`, `README.md`, final poe tasks

- CI: **quality** job (setup-uv cached → ruff check + format-check → mypy → `pytest -m "not integration" --cov`); **integration** job (GH services: postgres/redis/minio → alembic → `pytest -m integration`).
- README: mermaid architecture diagram, port/adapter table, 3-command quickstart, "Design decisions" narrative (offloader spy, Streams vs Celery, message-as-pointer), and an **exit-criteria → test-file mapping table** (strong interview signal).
- Poe tasks: fmt, lint, typecheck, test, test-int, check, api, worker, up, down, migrate, smoke.

**Verify:** CI green on GitHub; clean-clone quickstart works.

---

## Windows gotchas (handled in plan)

1. uvloop unavailable → `uvicorn[standard]` marker skips it locally; active inside Linux containers automatically.
2. Stay on default ProactorEventLoop — asyncpg + redis-py asyncio both work on it (reason for asyncpg over psycopg-async).
3. `loop.add_signal_handler` → NotImplementedError on Windows: worker implements `signal.signal` fallback (Phase 7).
4. CRLF: `.gitattributes eol=lf` + ruff `line-ending = "lf"`.
5. Repo path contains a space (`Study supply`) — quote paths in scripts; named volumes over bind mounts.
6. fakeredis stream-command fidelity — contained risk, reclaim test can shift to integration tier.

## Exit-criteria traceability

| Spec criterion | Where proven |
|---|---|
| Deterministic concurrency gates | `RecordingOffloader` spy tests (Phase 2/4), attempt-count retry tests, Event-gated drain tests — zero sleeps/clocks |
| Zero-cloud isolation | Settings redirect validator unit test (Phase 1) + all-fake provider demo path (Phase 7) |
| Zero resource leaking | Container `aclose()` leak test + `pool.checkedout() == 0` lifespan assertion (Phase 6) |
| 202 ingestion in ms | Route test: insert + publish only, no pipeline work inline (Phase 6) |
| to_thread + backoff at every boundary | Parametrized per-adapter-method offload invariant test (Phase 4); tenacity at all adapters |
