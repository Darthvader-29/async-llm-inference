# Phase 3 — Persistence: SQLAlchemy, Alembic, Repository

> **Part of:** [Asynchronous AI Serving Engine](../implementation-plan.md) · [Problem Statement](../problem-statement.md)
> **Status:** Planned (greenfield) · **Depends on:** [Phase 1 (scaffold, settings, domain)](phase-1-scaffold-toolchain-domain.md), [Phase 2 (concurrency, retry, ports)](phase-2-concurrency-retry-ports.md) · **Unlocks:** [Phase 5 (broker)](phase-5-redis-streams-broker.md), [Phase 6 (API)](phase-6-composition-root-fastapi-api.md), [Phase 7 (worker)](phase-7-worker-pipelines.md)
> **Delivers:** A running local infrastructure stack (Postgres + Redis + MinIO) plus a fully async SQLAlchemy 2.0 persistence adapter — `engine.py`, `tables.py`, `repository.py` — with Alembic async migrations, that satisfies the `JobRepository` port without leaking any ORM type into the domain, verified by a Docker-free SQLite unit round-trip and a gated Postgres integration round-trip.
> **Primary skills applied:** database-migrations-sql-migrations, postgres-best-practices, database-design, database-architect, sql-optimization-patterns, python-pro, docs-architect, mermaid-expert

---

## Table of Contents

1. [Overview & Objectives](#1-overview--objectives)
2. [Where This Fits](#2-where-this-fits)
3. [Prerequisites & Inputs](#3-prerequisites--inputs)
4. [Deliverables](#4-deliverables)
5. [Design Decisions & Rationale](#5-design-decisions--rationale)
6. [Detailed Implementation](#6-detailed-implementation)
7. [Flow & Sequence Diagrams](#7-flow--sequence-diagrams)
8. [Configuration & Environment](#8-configuration--environment)
9. [Testing Strategy](#9-testing-strategy)
10. [Verification & Exit-Criteria Mapping](#10-verification--exit-criteria-mapping)
11. [Windows & Cross-Platform Notes](#11-windows--cross-platform-notes)
12. [Common Pitfalls & Troubleshooting](#12-common-pitfalls--troubleshooting)
13. [Definition of Done](#13-definition-of-done)
14. [References & Further Reading](#14-references--further-reading)
15. [Navigation](#15-navigation)

---

## 1. Overview & Objectives

Phase 3 turns the abstract `JobRepository` port from [Phase 2](phase-2-concurrency-retry-ports.md) into a concrete, durable, **PostgreSQL-backed** adapter, and stands up the local infrastructure the rest of the engine runs against. After this phase the engine has a *single source of truth* for every inference job's state and metadata — the property the entire decoupled-processing design in the [problem statement](../problem-statement.md) depends on ("Stream messages are pointers; PostgreSQL is the single source of truth").

Concretely, this phase delivers three persistence modules and the migration machinery that keeps the schema honest, plus the infra `docker-compose.yml` that the API (Phase 6) and worker (Phase 7) will connect to.

**Objectives — what the system can do after this phase that it could not before:**

1. **Stand up reproducible local infra.** `docker compose up -d` starts `postgres:16-alpine`, `redis:7-alpine`, and `minio/minio`, each with a healthcheck, on named volumes (no bind mounts → Windows-friendly), with a one-shot `minio/mc` job that creates the application bucket.
2. **Build an async engine + session factory from `Settings`.** `engine.py` produces a `postgresql+asyncpg://` `AsyncEngine` and an `async_sessionmaker(expire_on_commit=False)`, plus a `dispose()` for clean teardown (exit-criterion 3: *zero resource leaking*).
3. **Define the `jobs` table with SQLAlchemy 2.0 typed ORM.** `tables.py` declares `JobRow` using `Mapped[]` / `mapped_column`, a UUID primary key, indexed `type`/`status`, a `JSONB`-with-`sqlite`-variant `payload` (so the *same* model runs on aiosqlite in unit tests), nullable result/error, attempt counter, lifecycle timestamps, `duration_ms`, and a composite `(status, created_at)` index for the worker's "next pending job" access pattern.
4. **Implement `SqlAlchemyJobRepository`.** A `session-per-operation` repository that maps `JobRow ↔ InferenceJob` through private `_to_domain` / `_to_row` functions so **no ORM type ever crosses the port boundary** into `domain/` or `services/`.
5. **Provide async Alembic migrations.** `alembic.ini` + an async `migrations/env.py` (using `run_sync(do_run_migrations)`) that reads `Settings().database_url`, plus a first revision that creates the `jobs` table and all its indexes — so `alembic upgrade head` is the canonical schema deployment.
6. **Prove it deterministically, two ways.** A Docker-free `tests/unit/test_repository_sqlite.py` round-trip (create → get → update) on `aiosqlite`, and an `@pytest.mark.integration` `tests/integration/test_repository_pg.py` round-trip against real Postgres.

> [!IMPORTANT]
> Two architectural invariants are *load-bearing* in this phase and are checked by the locked decisions everywhere downstream:
> 1. **No ORM leakage.** `domain/` and `services/` see only `InferenceJob` (a `@dataclass(slots=True)` from [Phase 1](phase-1-scaffold-toolchain-domain.md)). The repository is the *only* module that knows `JobRow` exists.
> 2. **The repository is constructed with a `session_factory`, not a session.** Each public method opens its own short-lived session (`session-per-operation`). This keeps the adapter safe to share across concurrent tasks and is what lets the [Phase 6](phase-6-composition-root-fastapi-api.md) `AppContainer` hold one repository instance for the whole process lifetime.

## 2. Where This Fits

This phase fills in the **Persistent Storage Layer** of the five-layer architecture and brings up the local containers for the broker and object-store layers (whose *adapters* are built in Phases 4–5). The persistence adapter is consumed by the ingestion service (Phase 6) and the job processor (Phase 7) strictly through the `JobRepository` port.

```mermaid
flowchart TD
    subgraph API["API Ingestion Layer — Phase 6"]
        ING["IngestionService"]
    end
    subgraph WK["Worker Layer — Phase 7"]
        PROC["JobProcessor"]
    end

    subgraph PORTS["Ports — Phase 2 (typing.Protocol)"]
        REPOPORT["JobRepository<br/>add / get / update"]
    end

    subgraph THIS["Persistence Adapter — PHASE 3 (this doc)"]
        REPO["SqlAlchemyJobRepository<br/>(session_factory)"]
        MAP["_to_domain / _to_row<br/>(mapping boundary)"]
        TBL["JobRow (DeclarativeBase)"]
        ENG["engine.py<br/>AsyncEngine + async_sessionmaker"]
        REPO --> MAP --> TBL
        REPO --> ENG
    end

    subgraph INFRA["Local Infrastructure — PHASE 3 (this doc)"]
        PG[("postgres:16-alpine")]
        RDS[("redis:7-alpine")]
        MIO[("minio/minio + mc init")]
    end

    subgraph MIG["Migrations — PHASE 3 (this doc)"]
        ALEMBIC["alembic env.py (async)<br/>+ first revision"]
    end

    ING -->|depends on port| REPOPORT
    PROC -->|depends on port| REPOPORT
    REPOPORT -. implemented by .-> REPO
    ENG -->|asyncpg| PG
    ALEMBIC -->|upgrade head| PG

    classDef thisPhase fill:#1f6feb,stroke:#0b1f44,color:#fff,stroke-width:2px;
    classDef infra fill:#1a7f37,stroke:#072b14,color:#fff,stroke-width:2px;
    class REPO,MAP,TBL,ENG,ALEMBIC thisPhase;
    class PG,RDS,MIO infra;
```

**Connection to prior phases.** Phase 2 defined `JobRepository` as a `typing.Protocol` (`add`/`get`/`update`) and the domain entity `InferenceJob` with its state machine (`PENDING → RUNNING → SUCCESS|FAILED`, `RUNNING → PENDING` for retry). Phase 3 supplies the *only* production implementation of that protocol. Because the port is structural, `SqlAlchemyJobRepository` never imports the protocol — `mypy --strict` checks conformance at the injection site (the `AppContainer`).

**Connection to later phases.** Phase 5's broker consumer and Phase 7's `JobProcessor` call `repo.get(job_id)` for the idempotency guard ("ack-and-skip if terminal") and `repo.update(job)` after each state transition. Phase 6's `POST /v1/jobs` calls `repo.add(job)` to insert the initial `PENDING` row *before* publishing the pointer to Redis. The composite `(status, created_at)` index and the `attempts` column exist precisely to serve those access patterns.

## 3. Prerequisites & Inputs

This phase assumes Phases 1–2 are complete. The following artifacts must already exist:

| Input (from prior phase) | Produced by | How Phase 3 uses it |
|---|---|---|
| `Settings` with `database_url`, `Environment`, object-store settings | [Phase 1 §config](phase-1-scaffold-toolchain-domain.md) | `engine.py` and `migrations/env.py` read `Settings().database_url`; compose `.env.example` mirrors the `AIE_` keys |
| `InferenceJob` `@dataclass(slots=True)`, `JobStatus`, `JobType` (StrEnum) | [Phase 1 §domain](phase-1-scaffold-toolchain-domain.md) | The repository maps rows to/from this entity; **the only domain type that crosses the port** |
| Domain transition methods (`mark_running`/`mark_success`/`mark_failed`/`requeue`) + `InvalidTransition` | [Phase 1 §domain](phase-1-scaffold-toolchain-domain.md) | Tests drive transitions; the repository persists whatever state the entity is in (it does **not** enforce transitions — the domain does) |
| `JobRepository` Protocol (`add`/`get`/`update`) | [Phase 2 §ports](phase-2-concurrency-retry-ports.md) | The contract `SqlAlchemyJobRepository` satisfies structurally |
| `pyproject.toml` with `sqlalchemy[asyncio]`, `asyncpg`, `alembic`, `aiosqlite`, `pytest-asyncio` (`asyncio_mode="auto"`), `integration` marker, `poethepoet` tasks | [Phase 1 §toolchain](phase-1-scaffold-toolchain-domain.md) | All runtime + test deps for this phase are already declared; Phase 3 adds no new dependency |
| `tests/conftest.py`, `tests/support/` | [Phase 1/2](phase-2-concurrency-retry-ports.md) | Phase 3 adds fixtures (`sqlite_session_factory`) and the integration gate |

> [!NOTE]
> Phase 3 introduces **no new third-party dependency**. Everything (`sqlalchemy[asyncio]`, `asyncpg`, `alembic`, `aiosqlite`) was declared in the Phase 1 `pyproject.toml`. This is deliberate: the dependency surface is locked early so `uv.lock` is stable and CI caching is effective.

> [!TIP]
> You can build and *unit-test* the entire persistence adapter (`tests/unit/test_repository_sqlite.py`) **without Docker** — aiosqlite runs in-process. Docker is only required for `alembic upgrade head` against Postgres and the `@pytest.mark.integration` round-trip. This keeps the inner dev loop fast and the default `pytest -m "not integration"` run hermetic.

## 4. Deliverables

| File | Type | Purpose |
|------|------|---------|
| `docker-compose.yml` | new | Infra-only stack: `postgres:16-alpine`, `redis:7-alpine`, `minio/minio`, one-shot `minio/mc` bucket-init; healthchecks; named volumes; port maps. (App services are added in [Phase 8](phase-8-containerization-compose.md).) |
| `.env.example` | new | Documented template of every `AIE_`-prefixed setting consumed by infra + persistence; copied to `.env` for local runs. |
| `src/app/adapters/persistence/__init__.py` | new | Package marker; re-exports the public adapter surface (`SqlAlchemyJobRepository`, `Base`, `build_engine`). |
| `src/app/adapters/persistence/engine.py` | new | `build_engine(settings) -> AsyncEngine`, `build_session_factory(engine) -> async_sessionmaker[AsyncSession]` (`expire_on_commit=False`), `dispose(engine)`. |
| `src/app/adapters/persistence/tables.py` | new | `Base(AsyncAttrs, DeclarativeBase)` + `JobRow` ORM model with typed `Mapped[]` columns and indexes (incl. composite `(status, created_at)`). |
| `src/app/adapters/persistence/repository.py` | new | `SqlAlchemyJobRepository(session_factory)` implementing the `JobRepository` port; `_to_domain` / `_to_row` mappers; `add` / `get` / `update`. |
| `alembic.ini` | new | Alembic config; `script_location = migrations`; `sqlalchemy.url` intentionally left blank (resolved from `Settings` in `env.py`). |
| `migrations/env.py` | new | Async Alembic environment using `connection.run_sync(do_run_migrations)`; imports `Base.metadata`; reads `Settings().database_url`. |
| `migrations/script.py.mako` | new | Standard Alembic revision template (unchanged from `alembic init`). |
| `migrations/versions/0001_create_jobs_table.py` | new | First revision: `op.create_table("jobs", ...)` + all indexes; reversible `downgrade()`. |
| `tests/support/db.py` | new | `make_sqlite_session_factory()` helper: in-memory aiosqlite engine + `Base.metadata.create_all` via `run_sync`. |
| `tests/unit/test_repository_sqlite.py` | new | Docker-free round-trip (create → get → update status) proving the adapter + mapping on aiosqlite. |
| `tests/integration/test_repository_pg.py` | new | `@pytest.mark.integration` round-trip against real Postgres (`AIE_DATABASE_URL`). |
| `pyproject.toml` | changed | Add `[tool.poe.tasks]` entries `up`, `down`, `migrate`, `revision` (thin wrappers around compose + alembic). |

## 5. Design Decisions & Rationale

| Decision | Choice | Why | Rejected alternative |
|----------|--------|-----|----------------------|
| ORM style | SQLAlchemy **2.0 typed ORM** (`DeclarativeBase`, `Mapped[]`, `mapped_column`) | First-class `mypy --strict` support; column types inferred from `Mapped[...]`; the project standard | 1.x `declarative_base()` + `Column(...)` (untyped, legacy) |
| Async driver | **asyncpg** (`postgresql+asyncpg://`) | Fastest Postgres async driver; works on Windows' default ProactorEventLoop (a locked [Windows gotcha](#11-windows--cross-platform-notes)) | `psycopg[async]` (also fine, but asyncpg chosen in plan for Proactor compatibility) |
| Session scope | **session-per-operation** (repo holds a `session_factory`) | Sessions are *not* concurrency-safe; one short session per call lets one repo instance serve many concurrent tasks safely | One long-lived session on the container (race conditions, stale identity map) |
| `expire_on_commit` | **`False`** | After `commit()`, attributes stay populated so `_to_domain(row)` can read them *without* an extra `await`/round-trip (which would also fail outside a transaction) | Default `True` → `DetachedInstanceError` / implicit lazy reload after commit |
| Mapping boundary | Private `_to_domain` / `_to_row` functions | Keeps every `JobRow`/ORM type inside `repository.py`; `domain`/`services` see only `InferenceJob` | Returning ORM rows directly (couples services to SQLAlchemy, breaks hexagonal rule) |
| `payload` column type | `JSONB().with_variant(JSON(), "sqlite")` | One ORM model serves **both** Postgres (JSONB) and aiosqlite (JSON) → unit tests need no Docker | Separate test schema (drift risk); TEXT + manual `json.dumps` (loses typing, reinvents codec) |
| Primary key | `UUID` (app-generated in domain) | Job IDs are client-facing tracking tokens returned at ingestion; opaque, non-enumerable | `bigint identity` (sequential, enumerable, leaks volume) — see UUIDv7 callout |
| Migrations | **Alembic async** env (`run_sync`) | Schema is versioned + reviewable; `upgrade head` is the single deploy path; autogenerate diffs against `Base.metadata` | `create_all()` in app startup (no history, no rollback, dangerous in prod) |
| Volumes | **Named volumes** | Windows bind mounts to Postgres/MinIO data dirs cause permission + line-ending grief; the repo path has a space (`Study supply`) | Bind mounts (`./data:/var/lib/...`) — fragile on Windows |
| Compose scope | **Infra only** in Phase 3 | App image + `migrate`/`api`/`worker` services belong to [Phase 8](phase-8-containerization-compose.md); Phase 3 keeps the loop tight | Full stack now (premature; no app image exists yet) |

### 5.1 Session-per-operation and why the repo takes a factory

The `SqlAlchemyJobRepository` is constructed once (in the [Phase 6](phase-6-composition-root-fastapi-api.md) `AppContainer`) and shared by every request handler and worker task. SQLAlchemy `AsyncSession` objects are explicitly **not** safe to share across concurrent `asyncio` tasks — a session holds a single DBAPI connection and an identity map that two interleaved coroutines would corrupt.

The clean resolution is **session-per-operation**: the repository stores an `async_sessionmaker` (a *factory*), and each of `add`/`get`/`update` opens its own `async with self._session_factory() as session:` block. The session lives exactly as long as the unit of work and is returned to the pool at the end. One repository instance, many independent sessions, zero shared mutable state.

> [!IMPORTANT]
> **The factory is shared; the session is not.** This is the async analogue of the classic "session-per-request" web pattern, scoped to a single repository call. It is also why the container holds *one* repository for the whole process: there is no per-request state to manage.

### 5.2 `expire_on_commit=False` — mandatory for this pattern

By default SQLAlchemy *expires* all attributes on a mapped instance after `commit()`, so the next attribute access triggers a refresh `SELECT`. Under async that is doubly bad:

1. The refresh is **lazy I/O**, which async SQLAlchemy forbids implicitly — you would get a `MissingGreenlet` error.
2. Even with explicit handling, it costs a round-trip we do not need.

Our flow is: `update(job)` merges the row, commits, and we are done — we do not read the row back after commit. But `add(job)` and the round-trip tests *do* read attributes (e.g. to return the persisted entity), so we set `expire_on_commit=False` once on the `async_sessionmaker` and never think about it again. The official asyncio docs explicitly recommend this for exactly this reason. See [§6.2](#62-srcappadapterspersistenceenginepy).

### 5.3 The `JSONB`-with-`sqlite`-variant trick

We want the production `payload` column to be **`JSONB`** (binary, indexable, the right Postgres choice), but we also want to run the *same* `JobRow` model under **aiosqlite** in unit tests so they need no Docker. SQLite has no `JSONB` type.

SQLAlchemy's `TypeEngine.with_variant()` solves this in one line:

```python
payload: Mapped[dict[str, Any]] = mapped_column(
    JSONB().with_variant(JSON(), "sqlite"),  # JSONB on PG, JSON on sqlite
    nullable=False,
)
```

At DDL/compile time SQLAlchemy emits `JSONB` for the `postgresql` dialect and the generic `JSON` for the `sqlite` dialect (SQLite stores JSON as `TEXT` and SQLAlchemy handles `json.dumps`/`loads` transparently). The Python-side value is a plain `dict` either way, so `_to_domain`/`_to_row` are dialect-agnostic.

> [!TIP]
> Use the *PostgreSQL* `JSONB` import (`sqlalchemy.dialects.postgresql.JSONB`) as the base and `with_variant(JSON(), "sqlite")` for the fallback — **not** the other way around. The base type is what `autogenerate`/production sees; the variant is the per-dialect override. Getting this backwards silently ships `JSON` (not `JSONB`) to Postgres and you lose `@>`/GIN indexability later.

> [!NOTE]
> **Schema parity caveat.** SQLite is a *test convenience*, not a second production target. A handful of Postgres features (true `JSONB`, GIN indexes, `CHECK` on enums, `timestamptz` semantics) are approximated or ignored on SQLite. The **integration** test (`test_repository_pg.py`) and `alembic upgrade head` are what validate the *real* schema. Treat the SQLite path as "does my mapping/repository logic work", and the Postgres path as "does the real schema work".

## 6. Detailed Implementation

> [!IMPORTANT]
> All code below targets **SQLAlchemy 2.0 async ORM**, **asyncpg**, **aiosqlite**, **Alembic ≥ 1.13 async**, **Pydantic v2 / pydantic-settings**, and **docker compose v2**. The async-engine/sessionmaker, `AsyncAttrs`+`DeclarativeBase`, `engine.dispose()`, and the Alembic `connection.run_sync(do_run_migrations)` shapes were verified against the official SQLAlchemy asyncio and Alembic asyncio-cookbook docs (see [§14](#14-references--further-reading)).

### 6.1 `docker-compose.yml` (infra only)

**Purpose & responsibilities.** Bring up the three backing services the engine needs locally, each healthchecked, with persistent named volumes and a one-shot job that creates the MinIO bucket. This file is *infra only* — the `api`/`worker`/`migrate` services are layered on in [Phase 8](phase-8-containerization-compose.md).

```yaml
# docker-compose.yml — Phase 3: infrastructure only (Postgres, Redis, MinIO).
# App services (api/worker/migrate) are added in Phase 8.
# Windows-friendly: NAMED VOLUMES only (no bind mounts), repo path may contain spaces.
name: aie  # project name -> deterministic container/volume prefixes (compose v2)

services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: ${AIE_POSTGRES_USER:-aie}
      POSTGRES_PASSWORD: ${AIE_POSTGRES_PASSWORD:-aie}
      POSTGRES_DB: ${AIE_POSTGRES_DB:-aie}
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      # pg_isready returns 0 only when the server accepts connections.
      test: ["CMD-SHELL", "pg_isready -U ${AIE_POSTGRES_USER:-aie} -d ${AIE_POSTGRES_DB:-aie}"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 10s
    restart: unless-stopped

  redis:
    image: redis:7-alpine
    # appendonly yes => durable streams across restarts (matters for Phase 5 broker).
    command: ["redis-server", "--appendonly", "yes"]
    ports:
      - "6379:6379"
    volumes:
      - redisdata:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]   # expects "PONG"
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 5s
    restart: unless-stopped

  minio:
    image: minio/minio:latest
    command: ["server", "/data", "--console-address", ":9001"]
    environment:
      MINIO_ROOT_USER: ${AIE_OBJECT_STORE__ACCESS_KEY_ID:-minioadmin}
      MINIO_ROOT_PASSWORD: ${AIE_OBJECT_STORE__SECRET_ACCESS_KEY:-minioadmin}
    ports:
      - "9000:9000"   # S3 API   (boto3 endpoint_url -> http://localhost:9000)
      - "9001:9001"   # web console
    volumes:
      - miniodata:/data
    healthcheck:
      # MinIO ships a liveness endpoint; curl is present in the minio image.
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 5s
    restart: unless-stopped

  # One-shot job: wait for MinIO, then create the application bucket and exit.
  # Re-runnable: `mc mb -p` is idempotent (no error if the bucket already exists).
  minio-init:
    image: minio/mc:latest
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
      mc alias set local http://minio:9000 ${AIE_OBJECT_STORE__ACCESS_KEY_ID:-minioadmin} ${AIE_OBJECT_STORE__SECRET_ACCESS_KEY:-minioadmin} &&
      mc mb -p local/${AIE_OBJECT_STORE__BUCKET:-aie-artifacts} &&
      echo 'bucket ready: ${AIE_OBJECT_STORE__BUCKET:-aie-artifacts}'
      "
    restart: "no"

volumes:
  pgdata:
  redisdata:
  miniodata:
```

**Walkthrough of the non-obvious parts.**

- **`name: aie`** sets the compose project name so containers/volumes get a deterministic `aie_` prefix regardless of the directory name — important because our directory is `async-llm-inference` under a path *with a space* (`Study supply`). Compose v2 derives the project name from the directory by default, and a space there can produce surprising volume names.
- **Named volumes (`pgdata`/`redisdata`/`miniodata`)** are declared at the bottom and referenced by `service.volumes`. Docker manages them inside its own VM/storage — no host path, so **no Windows path-with-space or permission problems**. This is the locked "Windows-friendly, NOT bind mounts" decision.
- **Healthchecks** use each image's native probe: `pg_isready` (Postgres), `redis-cli ping` (Redis), and MinIO's `/minio/health/live` endpoint. `start_period` gives slow first-boot time without counting early failures against `retries`.
- **`minio-init`** uses `depends_on: { minio: { condition: service_healthy } }` so `mc` only runs after MinIO answers its healthcheck. `mc mb -p` creates the bucket and the `-p` flag makes it a no-op if the bucket already exists → the job is safe to re-run on every `up`. `restart: "no"` keeps it a true one-shot (it exits 0 and stays exited).
- **`appendonly yes`** on Redis makes streams survive a container restart, which matters once [Phase 5](phase-5-redis-streams-broker.md) puts real jobs on `aie:jobs`.

> [!WARNING]
> The MinIO endpoint inside the compose network is `http://minio:9000` (service DNS name), but from your **host** (and from boto3 running on the host in Phases 4/6) it is `http://localhost:9000`. The Phase 1 `Settings` zero-cloud validator forces `http://localhost:9000` for host-side runs; the in-network host override (`minio:9000`) is wired in [Phase 8](phase-8-containerization-compose.md) when the app itself runs in a container.

> [!TIP]
> Verify health without parsing logs:
> ```powershell
> docker compose up -d
> docker compose ps        # STATUS column shows "(healthy)" per service
> ```
> The MinIO console is at <http://localhost:9001> (login = `AIE_OBJECT_STORE__ACCESS_KEY_ID` / `AIE_OBJECT_STORE__SECRET_ACCESS_KEY`). Phase 7's demo confirms artifacts land in the `aie-artifacts` bucket there.

### 6.2 `src/app/adapters/persistence/engine.py`

**Purpose & responsibilities.** Translate a `Settings` object into the two SQLAlchemy primitives the rest of the persistence layer needs — an `AsyncEngine` and an `async_sessionmaker[AsyncSession]` — and expose a `dispose()` for clean teardown. This module is the *only* place that knows the driver URL shape and engine pool tuning.

```python
# src/app/adapters/persistence/engine.py
"""Async SQLAlchemy engine + session factory, built from Settings.

The composition root (Phase 6 AppContainer) calls build_engine() once at
startup and dispose() once at shutdown. Everything else receives the
async_sessionmaker (a factory), never a live session.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import Settings


def build_engine(settings: Settings) -> AsyncEngine:
    """Create the async asyncpg Engine from Settings.

    Builds the production/dev ``postgresql+asyncpg://`` engine. Pool params are
    sensible fixed defaults (promote them to Settings if a deployment ever needs
    to tune them). Unit tests do NOT call this — they use the in-memory SQLite
    helper in ``tests/support/db.py`` (§6.9) with an explicit StaticPool.
    """
    return create_async_engine(
        settings.database_url,          # canonical Settings field is a str DSN
        echo=False,                     # SQL logging off; flip locally to debug
        pool_pre_ping=True,             # validate a pooled conn before use ->
                                        #   transparently recovers from a server
                                        #   restart / dropped TCP connection
        pool_size=10,                   # steady-state pooled connections
        max_overflow=20,                # burst connections above pool_size
        pool_recycle=1800,              # recycle conns older than 30 min
    )


def build_session_factory(
    engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    """Create the session factory shared by the repository.

    expire_on_commit=False is REQUIRED for the async session-per-operation
    pattern: it keeps attributes populated after commit() so the mapper can
    read them WITHOUT triggering implicit lazy I/O (which async forbids).
    """
    return async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
        class_=AsyncSession,   # explicit; async_sessionmaker defaults to this
    )


async def dispose(engine: AsyncEngine) -> None:
    """Dispose the engine's connection pool (teardown / exit-criterion 3).

    Called by AppContainer.aclose(); closes every pooled connection so no
    file descriptor / asyncpg connection is left dangling.
    """
    await engine.dispose()
```

**Walkthrough & rationale.**

- **`settings.database_url`** is a plain `str` DSN in [Phase 1](phase-1-scaffold-toolchain-domain.md)'s canonical `Settings` (not a `PostgresDsn` object), so it passes straight to `create_async_engine` — no coercion needed.
- **`pool_pre_ping=True`** issues a lightweight `SELECT 1` before handing out a pooled connection. Without it, a connection that died while idle (server restart, `pool_recycle`, network blip) surfaces as a hard error on the *next* query; with it, SQLAlchemy quietly discards the dead connection and opens a fresh one. This directly supports the spec's resilience goals.
- **`pool_recycle`** caps connection age so we never hold a connection past Postgres' `idle_in_transaction_session_timeout`/server-side reaping. (`postgres-best-practices` → `conn-idle-timeout`.)
- **`expire_on_commit=False`** — see [§5.2](#52-expire_on_commitfalse--mandatory-for-this-pattern). Verified against the official SQLAlchemy asyncio docs, which state the session "is configured using `expire_on_commit` set to `False`, so that we may access attributes on an object subsequent to a call to `commit()`."
- **`dispose()`** is awaited and *the* teardown hook. The [Phase 6 leak test](phase-6-composition-root-fastapi-api.md) asserts `engine.pool.checkedout() == 0` after `AppContainer.aclose()` runs `dispose()`.

> [!CAUTION]
> **asyncpg + a transaction-mode connection pooler (PgBouncer) do not mix with prepared statements.** asyncpg uses *named* server-side prepared statements by default; in PgBouncer *transaction* mode, the next statement can land on a different backend connection that has never heard of that prepared statement → `prepared statement "__asyncpg_…" does not exist`. We do **not** put PgBouncer in front of Postgres in this project (SQLAlchemy's own async pool is the pool), so the default is correct. *If* you later add PgBouncer in transaction mode, set `connect_args={"statement_cache_size": 0}` (and `prepared_statement_cache_size=0` via SQLAlchemy's `create_async_engine(..., connect_args=...)`) or use session-mode pooling. See `postgres-best-practices` → `conn-prepared-statements`.

> [!NOTE]
> **`build_engine` targets asyncpg.** Unit tests do not call it — they build their SQLite engine via the dedicated `tests/support/db.py` helper (§6.9) with an explicit `StaticPool` (one shared in-memory connection so the schema persists). Keeping the two paths separate lets `build_engine` use Postgres-appropriate pool settings without worrying about SQLite's single-connection model.

### 6.3 `src/app/adapters/persistence/tables.py`

**Purpose & responsibilities.** Declare the ORM mapping for the `jobs` table using SQLAlchemy 2.0 typed syntax, with the indexes that serve the engine's real query patterns. This is the **only** module (besides `repository.py`, which imports it) that mentions the ORM. `Base.metadata` here is also Alembic's `target_metadata`.

```python
# src/app/adapters/persistence/tables.py
"""SQLAlchemy 2.0 declarative model for the `jobs` table.

This is the single source of truth for the schema. Alembic autogenerate
diffs migrations against Base.metadata; tests create_all() from it; and the
repository maps it to/from the domain InferenceJob entity.

NOTHING in this module is exposed past the repository — domain/services never
import JobRow.
"""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    Index,
    String,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import JSON, Integer, Uuid


class Base(AsyncAttrs, DeclarativeBase):
    """Declarative base for all ORM models.

    AsyncAttrs adds the `awaitable_attrs` accessor (await obj.awaitable_attrs.x)
    for safe attribute access under async; we don't use relationships here, but
    including it now is free and future-proofs the base.
    """


class JobRow(Base):
    """Row model for one inference job. Mirrors the domain InferenceJob."""

    __tablename__ = "jobs"

    # --- identity ---------------------------------------------------------
    # UUID is generated in the DOMAIN (Phase 1) and passed in; the column does
    # not default — the app owns id creation so the value is known before the
    # INSERT (needed to return the tracking token at ingestion, Phase 6).
    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),  # native UUID on PG; CHAR(32) on sqlite (handled by SA)
        primary_key=True,
    )

    # --- classification (both indexed: hot filter columns) ----------------
    type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True)

    # --- payload: JSONB on Postgres, JSON on sqlite (variant trick) -------
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB().with_variant(JSON(), "sqlite"),
        nullable=False,
    )

    # --- result / error (nullable until terminal) -------------------------
    # result_ref is an s3://bucket/key pointer produced by the object store
    # (Phase 4/7); the payload bytes themselves never live in Postgres.
    result_ref: Mapped[str | None] = mapped_column(String(512), nullable=True)
    error: Mapped[str | None] = mapped_column(String(2048), nullable=True)

    # --- retry bookkeeping ------------------------------------------------
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # --- lifecycle timestamps (timestamptz on PG) ------------------------
    created_at: Mapped[dt.datetime] = mapped_column(
        nullable=False,
        server_default=func.now(),  # DB sets it if the app omits it
    )
    # The domain bumps updated_at on every guarded transition (Phase 1); the
    # server_default is just an INSERT safety net for out-of-band writes.
    updated_at: Mapped[dt.datetime] = mapped_column(
        nullable=False,
        server_default=func.now(),
    )

    # --- execution metric -------------------------------------------------
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)

    __table_args__ = (
        # Composite index for the worker's hot path:
        #   "oldest PENDING jobs first" -> WHERE status = :s ORDER BY created_at
        # Equality column (status) FIRST, range/sort column (created_at) LAST
        # (leftmost-prefix rule; postgres-best-practices: query-composite-indexes).
        Index("ix_jobs_status_created_at", "status", "created_at"),
        # Defense-in-depth: the domain enforces the enum, but a CHECK keeps the
        # table honest against any out-of-band writes (migrations, psql).
        CheckConstraint(
            # JobStatus StrEnum VALUES are lowercase (PENDING="pending", ...).
            "status in ('pending','running','success','failed')",
            name="ck_jobs_status",
        ),
        CheckConstraint(
            "type in ('rag_query','embed_document')",
            name="ck_jobs_type",
        ),
    )

    def __repr__(self) -> str:  # debugging aid; never used for logic
        return f"JobRow(id={self.id!r}, type={self.type!r}, status={self.status!r})"
```

**Walkthrough & rationale.**

- **`Uuid(as_uuid=True)`** is SQLAlchemy 2.0's portable UUID type: it renders to Postgres' native `UUID` and to `CHAR(32)` on SQLite, round-tripping a Python `uuid.UUID` on both. The **id is supplied by the domain**, not defaulted in the DB, because [Phase 6](phase-6-composition-root-fastapi-api.md) must know the job id *before* the INSERT to return it as the `202` tracking token. (Contrast with the `postgres-best-practices` default of `gen_random_uuid()` — that pattern fits DB-generated ids; ours are app-generated.)
- **`type`/`status` as indexed `String` columns** rather than a DB `ENUM`. The domain (`JobType`/`JobStatus` `StrEnum` from Phase 1) is the authority; storing the string keeps migrations trivial (no `ALTER TYPE ... ADD VALUE` dance) and the `CHECK` constraint gives us the integrity backstop without the ENUM's migration cost. `index=True` on each is for single-column filters; the composite index below covers the common two-column path.
- **`payload` variant** — the JSONB-on-PG / JSON-on-SQLite trick from [§5.3](#53-the-jsonb-with-sqlite-variant-trick). The Python value is always `dict[str, Any]`.
- **`result_ref` is a *pointer*, not a blob.** Outputs go to MinIO/S3 (Phases 4/7); Postgres stores only the `s3://bucket/key` reference. This keeps rows small and the DB fast — the spec's "PostgreSQL logs state transitions and metadata", not payloads.
- **`created_at`/`updated_at` server default `func.now()`** means even a hand-written `INSERT` gets timestamps; the app normally sets them from the domain (`InferenceJob` owns `created_at` and bumps `updated_at` on every guarded transition), but the DB defaults are the safety net. There are deliberately **no** separate `started_at`/`finished_at` columns — the domain entity tracks `created_at` + `updated_at` + the `duration_ms` execution metric, and the row maps **1:1** to it.
- **`__table_args__` composite index** `(status, created_at)` — equality column first, sort/range column last. This is the textbook column order for `WHERE status = 'PENDING' ORDER BY created_at` (the worker's "what should I run next" and any admin "recent failures" queries). `sql-optimization-patterns` / `query-composite-indexes`: a single composite index beats two single-column indexes + a bitmap-AND.
- **`CHECK` constraints** are *defense in depth*. The state machine lives in the domain; the constraints stop a bad migration or a stray `psql` `UPDATE` from persisting an impossible `status`/`type`.

> [!TIP]
> Because `type`/`status` are stored as the `StrEnum`'s **value** (`StrEnum` *is* a `str`), the mapper can pass `job.status` straight into the `String` column and read it straight back into `JobStatus(row.status)`. No custom `TypeDecorator` needed. See `_to_row`/`_to_domain` in [§6.4](#64-srcappadapterspersistencerepositorypy).

> [!NOTE]
> **Why UUIDv4 (random) is acceptable here despite the fragmentation note.** `postgres-best-practices` (`schema-primary-keys`) warns that random UUIDv4 PKs scatter B-tree inserts and fragment the index on *large, high-write* tables. For a job table whose lifetime row count is bounded by throughput × retention and which is dominated by *point lookups by id* and *status-scan via the composite index* (not range scans on the PK), the fragmentation cost is negligible. If write volume ever justifies it, switch the domain id generator to **UUIDv7** (time-ordered → near-sequential inserts) with **zero schema change** — the column type stays `Uuid`. That swap is a Phase-1 domain change, not a migration.

### 6.4 `src/app/adapters/persistence/repository.py`

**Purpose & responsibilities.** Implement the `JobRepository` port (`add`/`get`/`update`) against SQLAlchemy, and **own the row↔domain mapping** so no ORM type escapes. This is the hexagonal seam: services depend on the *port*; this class is the production adapter.

```python
# src/app/adapters/persistence/repository.py
"""SQLAlchemy implementation of the JobRepository port.

Hexagonal boundary: this is the ONLY module that knows JobRow exists.
Public methods accept/return domain InferenceJob; private mappers convert.
Session-per-operation: each method opens its own short-lived AsyncSession
from the injected factory.
"""

from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.adapters.persistence.tables import JobRow
from app.domain.exceptions import JobNotFound
from app.domain.models import InferenceJob, JobStatus, JobType


class SqlAlchemyJobRepository:
    """Durable job store backed by SQLAlchemy 2.0 async.

    Conforms structurally to the JobRepository Protocol (Phase 2) — it does
    NOT import the protocol; mypy --strict checks conformance where the
    AppContainer injects it.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        # A FACTORY, not a session. Shared safely across concurrent tasks;
        # each call below opens its own session.
        self._session_factory = session_factory

    # ------------------------------------------------------------------ #
    # Port methods (domain in, domain out — never a JobRow)
    # ------------------------------------------------------------------ #
    async def add(self, job: InferenceJob) -> None:
        """Insert a new job row (called by ingestion before publishing)."""
        async with self._session_factory() as session:
            async with session.begin():           # BEGIN ... COMMIT (or ROLLBACK)
                session.add(_to_row(job))
            # commit happened on exiting `session.begin()`; expire_on_commit=False
            # means no implicit refresh is triggered here.

    async def get(self, job_id: uuid.UUID) -> InferenceJob:
        """Load a job by id. Raises JobNotFound if absent.

        Used by the worker's idempotency guard (Phase 7) and GET /v1/jobs/{id}.
        """
        async with self._session_factory() as session:
            row = await session.get(JobRow, job_id)   # PK lookup via identity map/SELECT
            if row is None:
                raise JobNotFound(job_id)
            return _to_domain(row)

    async def update(self, job: InferenceJob) -> None:
        """Persist the current state of an existing job (after a transition).

        Uses session.merge() so the detached domain->row mapping is reconciled
        with the persistent row in one UPDATE. Raises JobNotFound if the id
        does not exist (merge would otherwise INSERT — we forbid that here).
        """
        async with self._session_factory() as session:
            async with session.begin():
                exists = await session.get(JobRow, job.id)
                if exists is None:
                    raise JobNotFound(job.id)
                await session.merge(_to_row(job))     # reconcile -> UPDATE
            # commit on block exit


# ---------------------------------------------------------------------- #
# Mapping boundary — the ONLY place row<->domain conversion happens.
# Keeping these module-private functions (not methods) makes the boundary
# explicit and trivially unit-testable.
# ---------------------------------------------------------------------- #
def _to_row(job: InferenceJob) -> JobRow:
    """domain InferenceJob -> ORM JobRow."""
    return JobRow(
        id=job.id,
        type=job.job_type.value,      # StrEnum -> str
        status=job.status.value,      # StrEnum -> str
        payload=job.payload,          # plain dict -> JSONB/JSON
        result_ref=job.result_ref,
        error=job.error,
        attempts=job.attempts,
        duration_ms=job.duration_ms,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )


def _to_domain(row: JobRow) -> InferenceJob:
    """ORM JobRow -> domain InferenceJob. No ORM type leaks past this return."""
    return InferenceJob(
        id=row.id,
        job_type=JobType(row.type),   # str -> StrEnum (validates membership)
        status=JobStatus(row.status), # str -> StrEnum
        payload=row.payload,
        result_ref=row.result_ref,
        error=row.error,
        attempts=row.attempts,
        duration_ms=row.duration_ms,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )
```

**Walkthrough & rationale.**

- **`__init__(self, session_factory)`** — the repository holds the *factory* (`async_sessionmaker`), never a session. This is the session-per-operation pattern from [§5.1](#51-session-per-operation-and-why-the-repo-takes-a-factory). One instance, injected by the [Phase 6 container](phase-6-composition-root-fastapi-api.md), serves the whole process.
- **`add`** opens a session, uses `async with session.begin()` (which BEGINs and COMMITs the transaction on clean exit, ROLLBACKs on exception), and `session.add(_to_row(job))`. Because the domain owns the id, this is a pure INSERT.
- **`get`** uses `session.get(JobRow, job_id)` — SQLAlchemy's by-primary-key load, which checks the identity map first and emits a `SELECT` otherwise. `None` → domain `JobNotFound` (a domain exception from Phase 1; **not** a SQLAlchemy `NoResultFound` — we translate so callers never catch ORM exceptions).
- **`update`** is the subtle one. We explicitly `get()` first to enforce "update only an existing row" (raising `JobNotFound` rather than silently inserting), then `session.merge(_to_row(job))`. `merge` takes our *detached* `JobRow` (built fresh from the domain entity) and reconciles it onto the persistent row already in the session, producing an `UPDATE`. This is the safe way to persist a domain object that was loaded, mutated in memory by the domain transition methods, and handed back to us — we never carry a live ORM instance across the service boundary, so `merge` (not "mutate the attached row") is the right tool.
- **`_to_row` / `_to_domain`** are *module-private functions*, deliberately not methods. They are the entire hexagonal mapping boundary and are independently unit-testable (`test_repository_sqlite.py` exercises them through the public API). `StrEnum.value` going out, `JobType(...)`/`JobStatus(...)` coming back in — the round-trip *re-validates* enum membership, so a corrupted `status` string in the DB raises `ValueError` at the boundary instead of poisoning the domain.

> [!IMPORTANT]
> **No ORM type appears in any public signature.** `add`/`get`/`update` speak `InferenceJob` and `uuid.UUID`. `JobRow`, `AsyncSession`, `select`, `merge` are all confined to this file. That is the literal definition of "no ORM leakage into domain/services" and is what the [mapping-boundary flowchart](#72-rowdomain-mapping-boundary-no-orm-leakage) asserts visually.

> [!WARNING]
> Do **not** return the `JobRow` from `get` "for convenience". The moment a service holds a `JobRow`, two things break: (1) the domain's state-machine methods (`mark_running()` etc.) live on `InferenceJob`, not `JobRow`, so callers would have to re-implement transitions; (2) accessing an attribute on a row whose session has closed raises `DetachedInstanceError`. The mapper exists precisely to sidestep both.

> [!NOTE]
> **Why `merge` and not `UPDATE … SET` Core statements?** A hand-written `update(JobRow).where(...).values(...)` would be marginally faster but would duplicate the column list (drift risk) and bypass the single mapping function. For a low-frequency write path (one update per state transition) the clarity of "map once, merge once" wins. If profiling ever shows the extra `SELECT` in `update` matters, replace the `get`+`merge` with a guarded Core `UPDATE ... RETURNING id` — but keep `_to_row` as the column source of truth.

### 6.5 `src/app/adapters/persistence/__init__.py`

**Purpose.** A thin public surface so the rest of the app imports from the package, not deep modules.

```python
# src/app/adapters/persistence/__init__.py
"""Persistence adapter package (SQLAlchemy 2.0 async)."""

from app.adapters.persistence.engine import (
    build_engine,
    build_session_factory,
    dispose,
)
from app.adapters.persistence.repository import SqlAlchemyJobRepository
from app.adapters.persistence.tables import Base, JobRow

__all__ = [
    "Base",
    "JobRow",
    "SqlAlchemyJobRepository",
    "build_engine",
    "build_session_factory",
    "dispose",
]
```

> [!NOTE]
> `JobRow` is re-exported only so `migrations/env.py` and tests can reach `Base.metadata` cleanly. Services still import nothing from here except via the container, which injects the *constructed* `SqlAlchemyJobRepository`. The export list is the package's intentional API.

### 6.6 `alembic.ini`

**Purpose & responsibilities.** Minimal Alembic config. Crucially, `sqlalchemy.url` is left **blank** — the real URL is resolved from `Settings` inside `env.py`, so secrets never live in a committed `.ini`, and dev/test/prod all use one config.

```ini
# alembic.ini — Alembic configuration.
# sqlalchemy.url is intentionally EMPTY: env.py loads it from Settings()
# (so the URL/secret lives only in the environment, never in this file).
[alembic]
script_location = migrations
prepend_sys_path = .
# Use OS-native path separators for version files (Windows-friendly).
version_path_separator = os
# file template: zero-padded sequence + slug -> sortable, readable filenames.
# e.g. 0001_create_jobs_table.py
file_template = %%(rev)s_%%(slug)s

sqlalchemy.url =

[loggers]
keys = root,sqlalchemy,alembic

[handlers]
keys = console

[formatters]
keys = generic

[logger_root]
level = WARNING
handlers = console
qualname =

[logger_sqlalchemy]
level = WARNING
handlers =
qualname = sqlalchemy.engine

[logger_alembic]
level = INFO
handlers =
qualname = alembic

[handler_console]
class = StreamHandler
args = (sys.stderr,)
level = NOTSET
formatter = generic

[formatter_generic]
format = %(levelname)-5.5s [%(name)s] %(message)s
datefmt = %H:%M:%S
```

**Walkthrough.**

- **`sqlalchemy.url =`** (empty) → resolved in `env.py` from `Settings().database_url`. This is the locked "Alembic reads `Settings().database_url`" decision.
- **`prepend_sys_path = .`** lets `env.py` `import app...` when Alembic runs from the repo root.
- **`version_path_separator = os`** and **`file_template`** keep revision filenames stable and Windows-friendly (zero-padded, slugged).

> [!IMPORTANT]
> We do **not** hard-code a `revision_environment` or a `[post_write_hooks]` ruff step here to keep the config minimal; if you want autogenerated revisions auto-formatted, add a `[post_write_hooks]` running `ruff format` — but pin it so CI and local agree.

### 6.7 `migrations/env.py` (async)

**Purpose & responsibilities.** The async Alembic environment. It wires `Settings().database_url` into Alembic at runtime, points `target_metadata` at `Base.metadata` (so `--autogenerate` diffs against our ORM), and runs migrations through `connection.run_sync(do_run_migrations)` — the canonical async pattern from the Alembic cookbook.

```python
# migrations/env.py — async Alembic environment.
"""Runs migrations against Settings().database_url using an async engine.

Pattern (verified against the Alembic asyncio cookbook):
  - build an async engine
  - open an async connection
  - connection.run_sync(do_run_migrations) -> Alembic's sync migration API
    runs INSIDE the greenlet-adapted sync function.
"""

from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from app.adapters.persistence.tables import Base
from app.core.config import Settings

# Alembic Config object (values from alembic.ini).
config = context.config

# Inject the real DB URL from Settings (alembic.ini leaves it blank).
# Settings() reads AIE_-prefixed env vars (and .env), so dev/test/prod all work.
config.set_main_option("sqlalchemy.url", str(Settings().database_url))

# Configure Python logging from the ini [loggers] section (if present).
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Autogenerate + online migrations diff against this metadata.
target_metadata = Base.metadata


def do_run_migrations(connection: Connection) -> None:
    """Synchronous migration body, run inside run_sync()."""
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,          # detect column TYPE changes in autogenerate
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Create an async engine, open a connection, run migrations via run_sync."""
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,    # one-shot migration run: no pooling needed
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_offline() -> None:
    """Offline ('--sql') mode: emit SQL without a live DB connection."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Online mode entry point: drive the async runner from sync Alembic."""
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
```

**Walkthrough & rationale.**

- **`config.set_main_option("sqlalchemy.url", str(Settings().database_url))`** is the single line that makes Alembic read our settings. Because `alembic.ini` left the URL blank, this is authoritative; `async_engine_from_config(...)` then picks it up via the `sqlalchemy.` prefix.
- **`do_run_migrations(connection)`** is *synchronous* and receives a *sync-facing* `Connection`. Alembic's migration API (`context.run_migrations()`) is sync; `run_sync` bridges it onto the async connection inside SQLAlchemy's greenlet adapter. This is the exact shape the official asyncio cookbook prescribes.
- **`run_async_migrations`** builds the engine with `pool.NullPool` (a migration is a one-shot, no reason to pool), runs the body, and `dispose()`s the engine — clean teardown even here.
- **`compare_type=True` / `compare_server_default=True`** make `alembic revision --autogenerate` notice column *type* and *default* changes, not just added/dropped columns — important so a future `String(16)→String(32)` on `status` is caught.
- **Offline mode** (`alembic upgrade head --sql`) is kept so you can emit a reviewable SQL script for a DBA/prod change-control flow without touching a live DB.

> [!WARNING]
> `Settings()` must be importable at Alembic runtime, which means `prepend_sys_path = .` (set in `alembic.ini`) and the `AIE_DATABASE_URL` (or the compose-provided env) must be present. In CI's integration job and in the [Phase 8](phase-8-containerization-compose.md) one-shot `migrate` service, that env is supplied; locally, your `.env` covers it. Running `alembic` with no DB env will raise a Pydantic `ValidationError` from `Settings()` — that is the *correct* fail-fast behavior, not a bug.

### 6.8 `migrations/versions/0001_create_jobs_table.py`

**Purpose & responsibilities.** The first (and, for Phase 3, only) revision: create the `jobs` table and every index/constraint declared on `JobRow`, with a real `downgrade()`. Whether you hand-write this or `--autogenerate` it, it must match `tables.py` exactly — `alembic upgrade head` is the canonical schema deploy.

```python
# migrations/versions/0001_create_jobs_table.py
"""create jobs table

Revision ID: 0001
Revises:
Create Date: 2026-01-01 00:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# --- Alembic identifiers ---------------------------------------------------
revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "jobs",
        sa.Column("id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("type", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        # JSONB on Postgres; this migration targets Postgres (the prod DB).
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("result_ref", sa.String(length=512), nullable=True),
        sa.Column("error", sa.String(length=2048), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),       # timestamptz
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_jobs"),
        sa.CheckConstraint(
            # JobStatus StrEnum values are lowercase.
            "status in ('pending','running','success','failed')",
            name="ck_jobs_status",
        ),
        sa.CheckConstraint(
            "type in ('rag_query','embed_document')",
            name="ck_jobs_type",
        ),
    )
    # Single-column indexes for hot filter columns.
    op.create_index("ix_jobs_type", "jobs", ["type"])
    op.create_index("ix_jobs_status", "jobs", ["status"])
    # Composite index for the worker's "oldest PENDING first" access pattern.
    op.create_index(
        "ix_jobs_status_created_at",
        "jobs",
        ["status", "created_at"],
    )


def downgrade() -> None:
    # Drop in reverse creation order (indexes before the table is implicit on
    # DROP TABLE, but explicit drops keep the downgrade auditable/portable).
    op.drop_index("ix_jobs_status_created_at", table_name="jobs")
    op.drop_index("ix_jobs_status", table_name="jobs")
    op.drop_index("ix_jobs_type", table_name="jobs")
    op.drop_table("jobs")
```

**Walkthrough & rationale.**

- **`down_revision = None`** marks this as the base revision. `alembic upgrade head` runs it; `alembic downgrade base` reverses it.
- **`server_default="0"` on `attempts`** and **`server_default=sa.func.now()` on `created_at`** match the ORM `server_default`/`default`, so a row inserted by raw SQL (or a future backfill) still gets sane values.
- **Index names are explicit and stable** (`ix_jobs_status_created_at`, etc.), matching `tables.py`. Stable names are what let `--autogenerate` recognize them as unchanged on later revisions (no spurious drop/create churn).
- **`downgrade()` is real**, not `pass`. The expand/contract migration discipline (`database-migrations-sql-migrations`) requires every forward step to be reversible so a bad deploy rolls back cleanly. Dropping indexes explicitly before the table makes the intent auditable and keeps the migration portable to engines where `DROP TABLE` does not cascade indexes the same way.

> [!CAUTION]
> This revision targets **Postgres** (the production DB), hence `postgresql.JSONB`. Do **not** run this migration against SQLite — the unit tests do not use Alembic at all; they `Base.metadata.create_all()` directly (§6.9), which honors the `with_variant(JSON(), "sqlite")` automatically. Mixing the two (running PG-targeted migrations on SQLite) is the classic way to get a `JSONB`-doesn't-exist error.

> [!TIP]
> **Autogenerate, then read every line.** In practice you would scaffold this with `alembic revision --autogenerate -m "create jobs table"` against an empty Postgres, then *review and tidy* the output (rename indexes to the stable names above, confirm the `CHECK`s came through). Never ship an autogenerated revision unread — autogenerate misses some constraint kinds and can reorder things. `database-migrations-sql-migrations`: the migration script is a reviewed artifact, not a generated black box.

### 6.9 `tests/support/db.py`

**Purpose & responsibilities.** A test helper that builds an **in-memory aiosqlite** engine, creates the schema from `Base.metadata` (no Alembic), and returns a session factory — the Docker-free seam the unit test uses.

```python
# tests/support/db.py
"""Test-only async SQLite session factory (in-memory, no Docker)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from app.adapters.persistence.tables import Base


@asynccontextmanager
async def sqlite_session_factory() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """Yield a session factory over a fresh in-memory SQLite DB.

    StaticPool + a single shared in-memory connection keeps the schema alive
    for the lifetime of the engine (a brand-new ':memory:' per connection would
    otherwise be empty). Tables are created from Base.metadata via run_sync.
    """
    engine: AsyncEngine = create_async_engine(
        "sqlite+aiosqlite://",          # in-memory
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,           # one connection, shared -> persistent :memory:
    )
    # create_all is sync DDL -> run it through the async connection via run_sync.
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    try:
        yield async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    finally:
        await engine.dispose()          # teardown: no leaked connection
```

**Walkthrough & rationale.**

- **`sqlite+aiosqlite://` (in-memory) + `StaticPool`** — without `StaticPool`, each new pooled connection to `:memory:` gets its *own* empty database, so the table created on connection A is invisible to connection B. `StaticPool` shares one connection, keeping the schema (and data) alive for the engine's lifetime. This is the standard SQLAlchemy recipe for in-memory async tests.
- **`Base.metadata.create_all` via `conn.run_sync`** mirrors the Alembic `run_sync` bridge: DDL is sync, the connection is async, `run_sync` adapts. The `with_variant(JSON(), "sqlite")` on `payload` means SQLite gets a `JSON`/`TEXT` column automatically — the **same** `JobRow` model, no test-only schema.
- **`finally: engine.dispose()`** keeps even the test helper leak-free, reinforcing exit-criterion 3 at the unit level.

> [!NOTE]
> Using `Base.metadata.create_all` (not Alembic) for SQLite unit tests is deliberate: it keeps unit tests fast and decoupled from the migration files, while the **integration** test exercises the *real* schema produced by `alembic upgrade head`. The two paths converge because both derive from the same `tables.py`.

### 6.10 `pyproject.toml` — poe task additions

**Purpose.** Cross-platform task shortcuts so Windows/macOS/Linux contributors run the same commands (no Makefile/PowerShell divergence — the locked `poethepoet` decision).

```toml
# pyproject.toml — append to [tool.poe.tasks] (existing tasks from Phase 1 stay).
[tool.poe.tasks]
# ... fmt, lint, typecheck, test, check (Phase 1) ...
up       = { cmd = "docker compose up -d", help = "Start infra (pg/redis/minio)" }
down     = { cmd = "docker compose down",  help = "Stop infra (keep volumes)" }
down-v   = { cmd = "docker compose down -v", help = "Stop infra + DELETE volumes" }
migrate  = { cmd = "alembic upgrade head", help = "Apply DB migrations" }
downgrade = { cmd = "alembic downgrade -1", help = "Revert last migration" }
revision = { cmd = "alembic revision --autogenerate -m", help = "New autogen revision (quote the message)" }
test-int = { cmd = "pytest -m integration", help = "Run integration tests (needs infra up)" }
```

> [!TIP]
> Local first-run sequence becomes a tidy three commands: `uv run poe up` → wait for `(healthy)` → `uv run poe migrate` → `uv run poe test-int`. `poe down-v` is the only command that destroys data (drops the named volumes); plain `poe down` keeps them so your next `up` is warm.

## 7. Flow & Sequence Diagrams

### 7.1 `jobs` table — erDiagram

The single table introduced this phase. It is intentionally normalized to *one* table: jobs have no child relationships in this design (payloads/results live in the object store as pointers, not rows).

```mermaid
erDiagram
    JOBS {
        uuid        id PK "app-generated UUID; client tracking token"
        varchar_32  type "rag_query | embed_document (indexed, CHECK)"
        varchar_16  status "PENDING|RUNNING|SUCCESS|FAILED (indexed, CHECK)"
        jsonb       payload "request payload (JSONB on PG, JSON on sqlite)"
        varchar_512 result_ref "s3://bucket/key pointer (nullable)"
        varchar_2048 error "failure message (nullable)"
        integer     attempts "retry counter, default 0"
        timestamptz created_at "row creation (server_default now())"
        timestamptz updated_at "last transition (domain-set; server_default now())"
        integer     duration_ms "execution time in ms (nullable)"
    }
```

**Index summary (created in `0001`):**

| Index | Columns | Serves |
|---|---|---|
| `pk_jobs` | `id` | Point lookup (`get(job_id)`, idempotency guard) |
| `ix_jobs_type` | `type` | Filter/aggregate by job type |
| `ix_jobs_status` | `status` | Filter by status (e.g. count RUNNING) |
| `ix_jobs_status_created_at` | `status, created_at` | "Oldest PENDING first" / "recent FAILED" — equality + sort in one scan |

### 7.2 Row↔domain mapping boundary (no ORM leakage)

This flowchart is the visual contract for the hexagonal rule: ORM types are sealed inside `repository.py`; only `InferenceJob`/`uuid.UUID` cross the dashed boundary.

```mermaid
flowchart LR
    subgraph DOMAIN["domain / services (NO ORM import)"]
        JOB["InferenceJob<br/>(@dataclass slots)"]
        JID["uuid.UUID"]
    end

    subgraph REPO["repository.py — the SEAM"]
        ADD["add(job)"]
        GET["get(job_id)"]
        UPD["update(job)"]
        TOROW["_to_row(job) -> JobRow"]
        TODOM["_to_domain(row) -> InferenceJob"]
    end

    subgraph ORM["ORM / DB (sealed)"]
        ROW["JobRow"]
        SESS["AsyncSession<br/>(per operation)"]
        PG[("Postgres / sqlite")]
    end

    JOB -->|"in"| ADD --> TOROW --> ROW
    JOB -->|"in"| UPD --> TOROW
    JID -->|"in"| GET --> SESS
    ROW --> SESS --> PG
    SESS --> ROW --> TODOM -->|"out"| JOB

    BOUNDARY{{"hexagonal boundary<br/>ORM types never cross"}}
    REPO -.seals.- BOUNDARY

    classDef seam fill:#1f6feb,stroke:#0b1f44,color:#fff;
    classDef sealed fill:#6e7681,stroke:#161b22,color:#fff;
    class ADD,GET,UPD,TOROW,TODOM seam;
    class ROW,SESS,PG sealed;
```

### 7.3 `add` (ingestion insert) — sequence

The path [Phase 6](phase-6-composition-root-fastapi-api.md)'s `POST /v1/jobs` takes to durably record a job *before* it publishes the Redis pointer.

```mermaid
sequenceDiagram
    autonumber
    participant SVC as IngestionService (Phase 6)
    participant REPO as SqlAlchemyJobRepository
    participant SF as async_sessionmaker
    participant S as AsyncSession
    participant DB as Postgres

    SVC->>REPO: add(job)  %% job is PENDING, id already set
    REPO->>SF: self._session_factory()
    SF-->>REPO: session (new)
    REPO->>S: async with session.begin()
    S->>DB: BEGIN
    REPO->>REPO: _to_row(job)  %% map domain -> JobRow
    REPO->>S: session.add(JobRow)
    S->>DB: INSERT INTO jobs (...)
    Note over S,DB: commit on clean block exit;<br/>expire_on_commit=False -> no refresh
    S->>DB: COMMIT
    S-->>REPO: session closed (returned to pool)
    REPO-->>SVC: None  %% then SVC publishes pointer to Redis
```

### 7.4 `get` → mutate → `update` (worker transition) — sequence

The path [Phase 7](phase-7-worker-pipelines.md)'s `JobProcessor` takes for each state transition, including the idempotency guard.

```mermaid
sequenceDiagram
    autonumber
    participant PROC as JobProcessor (Phase 7)
    participant REPO as SqlAlchemyJobRepository
    participant DOM as InferenceJob (domain)
    participant DB as Postgres

    PROC->>REPO: get(job_id)
    REPO->>DB: SELECT ... WHERE id = :id
    DB-->>REPO: row
    REPO->>REPO: _to_domain(row)
    REPO-->>PROC: InferenceJob

    alt status already terminal (SUCCESS/FAILED)
        PROC->>PROC: idempotency guard -> ack & skip
    else PENDING
        PROC->>DOM: job.mark_running()  %% domain enforces transition
        PROC->>REPO: update(job)
        REPO->>DB: SELECT (exists?) then UPDATE (merge)
        DB-->>REPO: ok
        Note over PROC,DB: ... pipeline runs ...
        PROC->>DOM: job.mark_success(result_ref, duration_ms)
        PROC->>REPO: update(job)
        REPO->>DB: UPDATE jobs SET status='SUCCESS', ...
    end
```

> [!NOTE]
> The **domain** (`mark_running`/`mark_success`) enforces legal transitions and raises `InvalidTransition` on illegal moves; the **repository** simply persists whatever state the entity holds. This separation — *behavior in the domain, durability in the adapter* — is the whole point of the mapping boundary and keeps the repository free of business rules.

## 8. Configuration & Environment

All settings are read by the [Phase 1](phase-1-scaffold-toolchain-domain.md) `Settings` (`env_prefix="AIE_"`); Phase 3 *consumes* them in `engine.py` and `migrations/env.py` and mirrors them in `.env.example` + `docker-compose.yml`.

| Env var | Default | Used by | Notes |
|---------|---------|---------|-------|
| `AIE_DATABASE_URL` | `postgresql+asyncpg://aie:aie@localhost:5432/aie` | `engine.py`, `migrations/env.py` | The async DSN (asyncpg). Unit tests use `tests/support/db.py` (sqlite). Engine pool params (`pool_size`/`max_overflow`/`pool_recycle`) are **fixed defaults in `engine.py`**, not env-tunable. |
| `AIE_POSTGRES_USER` | `aie` | compose | Postgres superuser for the container. |
| `AIE_POSTGRES_PASSWORD` | `aie` | compose | Dev-only password; never a prod default. |
| `AIE_POSTGRES_DB` | `aie` | compose | Database name; matches `AIE_DATABASE_URL`. |
| `AIE_OBJECT_STORE__ENDPOINT_URL` | `http://localhost:9000` (forced in dev) | `Settings.object_store`, compose, Phase 4/6 | Zero-cloud redirect → MinIO. Empty + non-prod → validator forces this. |
| `AIE_OBJECT_STORE__ACCESS_KEY_ID` | `minioadmin` | `Settings.object_store`, compose (MinIO root user) | Dev MinIO credential — one var feeds **both** the app boto3 client and the container. |
| `AIE_OBJECT_STORE__SECRET_ACCESS_KEY` | `minioadmin` | `Settings.object_store`, compose (MinIO root pw) | Dev MinIO credential. |
| `AIE_OBJECT_STORE__BUCKET` | `aie-artifacts` | `Settings.object_store`, compose (`minio-init`), Phase 4 | Bucket created by the one-shot `mc` job. |
| `AIE_REDIS_URL` | `redis://localhost:6379/0` | Phase 5 (declared here for `.env`) | Broker URL; not used by persistence but lives in the same `.env`. |
| `AIE_ENV` | `dev` | Phase 1 validator | `dev`/`test`/`prod`; drives the zero-cloud S3 redirect. |

> [!NOTE]
> The pool defaults are sized for the demo, not a benchmark. Rule of thumb (`postgres-best-practices` → `conn-pooling`): keep total app connections (API pool + worker pool) comfortably under Postgres `max_connections` (100 by default on `postgres:16-alpine`). With `pool_size=10 + max_overflow=5` per process and two processes (api + worker), peak is ~30 — well within budget.

**`.env.example` (Phase 3 form):**

```dotenv
# .env.example — copy to .env for local runs. All keys are AIE_-prefixed.
# --- environment ---
AIE_ENV=dev

# --- database (asyncpg). Engine pool params are fixed defaults in engine.py. ---
AIE_DATABASE_URL=postgresql+asyncpg://aie:aie@localhost:5432/aie

# --- postgres container ---
AIE_POSTGRES_USER=aie
AIE_POSTGRES_PASSWORD=aie
AIE_POSTGRES_DB=aie

# --- redis (broker; consumed in Phase 5) ---
AIE_REDIS_URL=redis://localhost:6379/0

# --- object store (MinIO; consumed in Phase 4). Nested AIE_OBJECT_STORE__* keys
#     feed BOTH the app Settings.object_store group AND the MinIO container/init. ---
# Leave endpoint empty in non-prod to let the Settings validator force MinIO,
# or set it explicitly as below.
AIE_OBJECT_STORE__ENDPOINT_URL=http://localhost:9000
AIE_OBJECT_STORE__ACCESS_KEY_ID=minioadmin
AIE_OBJECT_STORE__SECRET_ACCESS_KEY=minioadmin
AIE_OBJECT_STORE__BUCKET=aie-artifacts

# --- provider secrets (optional; fakes used when unset — Phase 4) ---
# AIE_HUGGINGFACE_TOKEN=
# AIE_PINECONE_API_KEY=
```

> [!WARNING]
> `.env` must be **git-ignored** (added to `.gitignore` in [Phase 1](phase-1-scaffold-toolchain-domain.md)); only `.env.example` is committed. The defaults here are *local dev* credentials (`aie`/`minioadmin`) and must never appear in a prod environment — prod supplies real secrets via the deployment platform, and the zero-cloud validator is *bypassed* only when `AIE_ENV=prod`.

## 9. Testing Strategy

Phase 3 adds two test files plus one support helper. The discipline (locked across the project) is **deterministic, clock-free** testing: no `sleep`, no wall-clock assertions. Persistence tests assert *state*, not timing.

| Test | Tier | Infra? | Proves |
|---|---|---|---|
| `tests/unit/test_repository_sqlite.py` | unit | none (aiosqlite in-memory) | `add`/`get`/`update` round-trip + mapping + `JobNotFound`; runs in default `pytest -m "not integration"` |
| `tests/integration/test_repository_pg.py` | integration | Postgres up + `alembic upgrade head` | Same round-trip against the *real* JSONB schema; gated by `@pytest.mark.integration` |

### 9.1 `tests/unit/test_repository_sqlite.py` (Docker-free)

```python
# tests/unit/test_repository_sqlite.py
"""Repository round-trip on in-memory SQLite — NO Docker, NO clock."""

from __future__ import annotations

import uuid

import pytest

from app.adapters.persistence.repository import SqlAlchemyJobRepository
from app.domain.exceptions import JobNotFound
from app.domain.models import InferenceJob, JobStatus, JobType
from tests.support.db import sqlite_session_factory

# asyncio_mode="auto" (Phase 1 pyproject) -> no @pytest.mark.asyncio needed.


async def _make_pending_job() -> InferenceJob:
    """A fresh PENDING job (domain factory mirrors Phase 1 constructor)."""
    return InferenceJob.new(
        job_type=JobType.RAG_QUERY,
        payload={"query": "what is hexagonal architecture?", "top_k": 3},
    )


async def test_add_then_get_round_trip() -> None:
    async with sqlite_session_factory() as factory:
        repo = SqlAlchemyJobRepository(factory)
        job = await _make_pending_job()

        await repo.add(job)
        loaded = await repo.get(job.id)

        # Identity + every mapped field survives the round-trip unchanged.
        assert loaded.id == job.id
        assert loaded.job_type is JobType.RAG_QUERY      # str -> StrEnum re-validated
        assert loaded.status is JobStatus.PENDING
        assert loaded.payload == {"query": "what is hexagonal architecture?", "top_k": 3}
        assert loaded.attempts == 0
        assert loaded.result_ref is None
        assert loaded.error is None
        assert loaded.created_at is not None             # server_default now()


async def test_update_persists_status_transition() -> None:
    async with sqlite_session_factory() as factory:
        repo = SqlAlchemyJobRepository(factory)
        job = await _make_pending_job()
        await repo.add(job)

        # Drive the DOMAIN state machine, then persist.
        job.mark_running()                               # PENDING -> RUNNING
        await repo.update(job)
        running = await repo.get(job.id)
        assert running.status is JobStatus.RUNNING
        assert running.attempts == 1                     # mark_running bumped attempts

        job.mark_success(result_ref="s3://aie-artifacts/x.json", duration_ms=42)
        await repo.update(job)
        done = await repo.get(job.id)
        assert done.status is JobStatus.SUCCESS
        assert done.result_ref == "s3://aie-artifacts/x.json"
        assert done.duration_ms == 42
        assert done.updated_at >= done.created_at        # transition bumped updated_at


async def test_get_missing_raises_job_not_found() -> None:
    async with sqlite_session_factory() as factory:
        repo = SqlAlchemyJobRepository(factory)
        with pytest.raises(JobNotFound):
            await repo.get(uuid.uuid4())                 # never inserted


async def test_update_missing_raises_job_not_found() -> None:
    async with sqlite_session_factory() as factory:
        repo = SqlAlchemyJobRepository(factory)
        ghost = await _make_pending_job()                # never added
        with pytest.raises(JobNotFound):
            await repo.update(ghost)
```

**Why these assertions are deterministic.**

- **No timing.** We assert `started_at is not None` (a *fact* — the transition set it), never *how long* anything took. `duration_ms=42` is a value the **domain** computed and we passed in; the test does not measure it.
- **Enum re-validation is the boundary proof.** `assert loaded.job_type is JobType.RAG_QUERY` confirms `_to_domain` turned the stored string back into the *same* enum member — i.e. the mapping boundary is intact in both directions.
- **`JobNotFound` (not a SQLAlchemy error)** confirms the adapter translates DB absence into a *domain* exception, so services never import ORM exceptions.

> [!TIP]
> These four tests run in milliseconds with zero Docker and are part of the default CI **quality** job (`pytest -m "not integration"`). They give fast feedback on the mapping logic — the part most likely to drift when columns are added.

### 9.2 `tests/integration/test_repository_pg.py` (gated)

```python
# tests/integration/test_repository_pg.py
"""Repository round-trip on REAL Postgres. Requires infra up + migrations.

Run with:  uv run poe test-int      (i.e. pytest -m integration)
Skipped by the default `pytest -m "not integration"` quality run.
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.adapters.persistence.engine import build_engine, dispose
from app.adapters.persistence.repository import SqlAlchemyJobRepository
from app.core.config import Settings
from app.domain.models import InferenceJob, JobStatus, JobType

pytestmark = pytest.mark.integration  # whole module is integration-tier


@pytest.fixture()
async def pg_session_factory() -> async_sessionmaker[AsyncSession]:
    """Session factory bound to the real Postgres from Settings.

    Assumes `alembic upgrade head` has already created the schema (the CI
    integration job and `poe migrate` do this before invoking pytest).
    """
    settings = Settings()
    # Guard: make the requirement explicit if someone runs this without PG.
    if "asyncpg" not in str(settings.database_url):
        pytest.skip("integration test requires a postgresql+asyncpg DATABASE_URL")

    engine = build_engine(settings)
    factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    try:
        yield factory
    finally:
        await dispose(engine)  # exit-criterion 3: clean teardown


async def test_pg_round_trip(pg_session_factory: async_sessionmaker[AsyncSession]) -> None:
    repo = SqlAlchemyJobRepository(pg_session_factory)
    job = InferenceJob.new(
        job_type=JobType.EMBED_DOCUMENT,
        payload={"document_uri": "s3://aie-artifacts/in.txt", "chunk_size": 512},
    )

    await repo.add(job)
    loaded = await repo.get(job.id)
    assert loaded.status is JobStatus.PENDING
    assert loaded.payload["chunk_size"] == 512   # JSONB round-trip on real PG

    job.mark_running()
    await repo.update(job)
    job.mark_failed(error="upstream 503")        # RUNNING -> FAILED
    await repo.update(job)

    final = await repo.get(job.id)
    assert final.status is JobStatus.FAILED
    assert final.error == "upstream 503"
    assert final.updated_at >= final.created_at
```

**Walkthrough & rationale.**

- **`pytestmark = pytest.mark.integration`** marks the *whole module* so the default `pytest -m "not integration"` skips it and it only runs under `poe test-int`. The `integration` marker is registered in the Phase 1 `pyproject.toml`.
- **The fixture builds a real engine from `Settings()`** and `dispose()`s it in `finally` — so even the integration test leaves no dangling asyncpg connection (exit-criterion 3 at the integration tier).
- **It asserts on `JSONB` round-trip** (`payload["chunk_size"] == 512`) which the SQLite test *cannot* truly validate (SQLite stores JSON as TEXT) — this is the test that proves the *production* type works.
- **It walks the full failure path** (`PENDING → RUNNING → FAILED`) including `error` and `finished_at`, complementing the SQLite test's success path.

> [!IMPORTANT]
> The integration test depends on the schema existing. In CI's **integration** job ([Phase 9](phase-9-ci-readme-polish.md)) the pipeline is: spin up service containers → `alembic upgrade head` → `pytest -m integration`. Locally it is `poe up` → `poe migrate` → `poe test-int`. If you forget `migrate`, the first query raises `UndefinedTable` — a clear signal, not a flake.

### 9.3 `conftest.py` touch-points

Phase 3 does not require new global fixtures beyond what [Phase 1/2](phase-2-concurrency-retry-ports.md) provide; the SQLite factory is a context-manager helper in `tests/support/db.py` so each test gets a *fresh* in-memory DB (perfect isolation, no cross-test bleed). If you prefer a fixture form, wrap it:

```python
# tests/conftest.py — optional convenience fixture (additive)
import pytest
from tests.support.db import sqlite_session_factory


@pytest.fixture()
async def sqlite_factory():
    async with sqlite_session_factory() as factory:
        yield factory
```

> [!NOTE]
> Per-test fresh DBs (rather than one shared DB with rollbacks) is the simplest correct choice for in-memory SQLite and keeps tests order-independent. For the *Postgres* integration tier, the fixture is function-scoped too; if the suite grows, switch to a transaction-rollback fixture (`session.begin_nested()` + outer rollback) to avoid re-creating state — but for Phase 3's handful of tests, simplicity wins.

## 10. Verification & Exit-Criteria Mapping

| Spec exit criterion | How this phase proves it | Command / test file |
|---------------------|--------------------------|---------------------|
| **Zero resource leaking** (pools/clients/fds open & close cleanly) | `engine.py` `dispose()` closes the pool; the SQLite helper and PG fixture both `dispose()` in `finally`; consumed by Phase 6 leak test | `await dispose(engine)`; `tests/support/db.py`; `tests/integration/test_repository_pg.py` fixture |
| **Zero-cloud isolation** (dev redirects to MinIO) | Compose runs MinIO + creates the bucket; `.env.example` leaves S3 endpoint at `localhost:9000`; the Phase 1 validator forces it in non-prod | `docker compose up -d`; MinIO console `:9001` shows `aie-artifacts` |
| **Deterministic concurrency gates** (clock-free tests) | Persistence tests assert *state* (status/timestamps-not-null/values), never elapsed time; in-memory SQLite makes them instant + hermetic | `pytest -m "not integration"` (incl. `test_repository_sqlite.py`) |
| **Single source of truth** (PG holds status/payload; streams are pointers) | `JobRow` stores `payload` (JSONB) + full lifecycle; `result_ref` is a pointer, not a blob — the table *is* the SoT | `tables.py`; `migrations/versions/0001_create_jobs_table.py` |
| **Schema is versioned & deployable** | Async Alembic env reads `Settings().database_url`; first revision is reversible | `alembic upgrade head` / `alembic downgrade base` |
| **Real-schema correctness** (JSONB, timestamptz) | Integration round-trip exercises the migrated Postgres schema, incl. JSONB sub-key read and the FAILED path | `pytest -m integration` (`test_repository_pg.py`) |

**Canonical verify sequence for this phase:**

```powershell
# 1. Unit tests (no Docker) — fast, hermetic
uv run poe test            # or: uv run pytest -m "not integration"

# 2. Infra up + healthy
uv run poe up
docker compose ps          # all services show (healthy); minio-init exited 0

# 3. Migrate the real DB
uv run poe migrate         # alembic upgrade head

# 4. Integration round-trip against real Postgres
uv run poe test-int        # pytest -m integration

# 5. Static gates (unchanged from Phase 1, must stay green)
uv run poe check           # ruff + mypy --strict + pytest
```

> [!TIP]
> A quick manual sanity check on the live schema:
> ```powershell
> docker compose exec postgres psql -U aie -d aie -c "\d+ jobs"
> ```
> You should see the four indexes (`pk_jobs`, `ix_jobs_type`, `ix_jobs_status`, `ix_jobs_status_created_at`), the two `CHECK` constraints, and `payload` typed `jsonb`.

## 11. Windows & Cross-Platform Notes

This phase is where infra meets the host, so Windows behavior matters concretely.

> [!IMPORTANT]
> **asyncpg on the default ProactorEventLoop.** The plan deliberately chose **asyncpg** (over `psycopg[async]`) because asyncpg works on Windows' default `ProactorEventLoop`, which we *do not* override (overriding it would break Proactor-only subprocess behavior). No event-loop policy change is needed for persistence on Windows — `create_async_engine("postgresql+asyncpg://...")` just works in the dev shell.

> [!WARNING]
> **Named volumes, not bind mounts — and a path with a space.** The repo lives under `D:\Study supply\projects\...` (note the space in `Study supply`). Bind-mounting host directories into Postgres/MinIO from a spaced path is a reliable source of grief on Windows (quoting, permissions, line-endings). The compose file uses **named volumes** (`pgdata`/`redisdata`/`miniodata`) exclusively, sidestepping all of it. Docker stores them inside its own backend.

> [!NOTE]
> **CRLF / line endings.** `.gitattributes` (`* text=auto eol=lf`, set in [Phase 1](phase-1-scaffold-toolchain-domain.md)) plus ruff's `line-ending = "lf"` keep `env.py`, the migration, and `docker-compose.yml` LF-normalized. This matters for the `minio-init` `entrypoint` shell script and the `pg_isready`/`redis-cli` healthcheck strings, which a stray CRLF could corrupt inside the Linux containers.

> [!TIP]
> **`aiosqlite` is fully cross-platform** and needs nothing special on Windows — the unit suite (`test_repository_sqlite.py`) runs identically on Windows/macOS/Linux and in CI. This is the main reason the SQLite path exists: a Windows contributor with no Docker Desktop can still develop and test the entire persistence adapter.

| Concern | Windows behavior | Handling in this phase |
|---|---|---|
| Event loop | ProactorEventLoop (default) | asyncpg compatible — no override |
| Volume mounts | Bind mounts to data dirs are fragile | Named volumes only |
| Repo path | Contains a space | Named volumes + quoted compose `name: aie` |
| Line endings | CRLF default | `.gitattributes eol=lf` + ruff `lf` |
| SQLite tests | Native, no deps | Run as-is, no Docker |
| `uv run poe up/migrate` | Same commands as POSIX | `poethepoet` is cross-platform |

## 12. Common Pitfalls & Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `MissingGreenlet: greenlet_spawn has not been called` | Implicit lazy I/O after commit (attribute expired) | Ensure `expire_on_commit=False` on the session factory ([§6.2](#62-srcappadapterspersistenceenginepy)); don't access un-loaded relationships under async |
| `sqlalchemy.exc.ArgumentError: ... requires an async driver` | URL is `postgresql://` not `postgresql+asyncpg://` | Use the async dialect in `AIE_DATABASE_URL` |
| `(no such table: jobs)` in a unit test | SQLite helper didn't `create_all`, or per-connection `:memory:` | Use `tests/support/db.py` (StaticPool + `create_all` via `run_sync`) |
| `asyncpg ... UndefinedTableError: relation "jobs" does not exist` (integration) | Forgot `alembic upgrade head` before `pytest -m integration` | `uv run poe migrate` first |
| `type "jsonb" does not exist` when running migration | Ran the PG-targeted migration against SQLite | Don't run Alembic on SQLite; unit tests use `create_all` (variant handles it) |
| `prepared statement "__asyncpg_..." does not exist` | PgBouncer transaction mode in front of Postgres | Not used by default; if added, set `connect_args={"statement_cache_size": 0}` or use session mode ([§6.2 caution](#62-srcappadapterspersistenceenginepy)) |
| `pydantic ValidationError` when running `alembic` | `Settings()` can't find required env (e.g. `AIE_DATABASE_URL`) | Provide `.env` / env vars; this is correct fail-fast, not a bug |
| `minio-init` exits non-zero on first `up` | MinIO not healthy yet, or wrong creds | It waits on `service_healthy`; check `AIE_S3_*` creds match MinIO root env |
| `docker compose ps` shows postgres `(unhealthy)` | First-boot slower than `start_period` | Increase `start_period`; re-check — initdb on first run takes longer |
| `DetachedInstanceError` in a service | A `JobRow` escaped the repository | Never return `JobRow`; map to `InferenceJob` (the whole point of the seam) |
| Autogenerated revision wants to drop/recreate an index every time | Index name in DB ≠ name in `tables.py` | Use the explicit stable names (`ix_jobs_status_created_at`, etc.) in both |
| `port is already allocated` on `up` | Local Postgres/Redis already on 5432/6379 | Stop the local service or remap host ports in compose |

> [!CAUTION]
> **`poe down-v` (or `docker compose down -v`) deletes the named volumes** — all Postgres rows, Redis streams, and MinIO objects vanish. Use plain `poe down` to stop containers while keeping data. This is the only data-loss command in the Phase 3 workflow; treat it accordingly.

> [!WARNING]
> **Do not call `Base.metadata.create_all` against Postgres in app code as a shortcut.** It produces an *unversioned* schema that drifts from the migrations, and it skips the `CHECK` constraints/defaults nuances that the reviewed `0001` revision encodes. `create_all` is for *SQLite unit tests only*; Postgres is provisioned exclusively by `alembic upgrade head`.

## 13. Definition of Done

- [ ] `docker compose up -d` brings up `postgres`, `redis`, `minio` all `(healthy)`, and `minio-init` exits `0` having created the `aie-artifacts` bucket (visible in the `:9001` console).
- [ ] `docker compose down` stops containers without deleting volumes; a subsequent `up` retains data.
- [ ] All compose volumes are **named** (no host bind mounts); compose `name: aie` set.
- [ ] `.env.example` documents every `AIE_` key used by infra + persistence; `.env` is git-ignored.
- [ ] `engine.py` exposes `build_engine` (asyncpg; `database_url` from `Settings`, fixed pool defaults), `build_session_factory` (`expire_on_commit=False`), and `dispose`.
- [ ] `tables.py` declares `JobRow` with: UUID PK, indexed `type`/`status`, `payload` `JSONB().with_variant(JSON(),"sqlite")`, nullable `result_ref`/`error`, `attempts` int default 0, `created_at`/`updated_at` (timestamptz), `duration_ms`, composite `(status, created_at)` index, status (lowercase values)/type `CHECK`s — columns map **1:1** to the `InferenceJob` entity.
- [ ] `repository.py` implements `add`/`get`/`update` via session-per-operation; `_to_domain`/`_to_row` confine all ORM types; `get`/`update` raise `JobNotFound`; **no ORM type in any public signature**.
- [ ] `alembic.ini` leaves `sqlalchemy.url` blank; `migrations/env.py` reads `Settings().database_url`, uses `connection.run_sync(do_run_migrations)`, targets `Base.metadata`.
- [ ] `migrations/versions/0001_create_jobs_table.py` creates the table + all four indexes + both CHECKs, with a real reversible `downgrade()`.
- [ ] `alembic upgrade head` succeeds against the compose Postgres; `\d+ jobs` shows the expected indexes/constraints/`jsonb`.
- [ ] `tests/unit/test_repository_sqlite.py` passes with **no Docker** (round-trip + transitions + `JobNotFound`), inside the default `pytest -m "not integration"` run.
- [ ] `tests/integration/test_repository_pg.py` passes under `pytest -m integration` against the migrated Postgres (incl. JSONB sub-key read + FAILED path), and its fixture `dispose()`s the engine.
- [ ] `uv run poe check` (ruff + mypy --strict + unit tests) is green; the repository conforms to the `JobRepository` Protocol at its injection site.
- [ ] No `sleep`/wall-clock assertions anywhere in the new tests.
- [ ] Poe tasks `up`, `down`, `down-v`, `migrate`, `downgrade`, `revision`, `test-int` work cross-platform.

## 14. References & Further Reading

**SQLAlchemy 2.0 (async ORM)**
- [Asyncio support — SQLAlchemy 2.0](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html) — `create_async_engine`, `async_sessionmaker(expire_on_commit=False)`, `AsyncAttrs`+`DeclarativeBase`, `engine.dispose()`, `run_sync`.
- [ORM Quick Start — Declarative & `Mapped`/`mapped_column`](https://docs.sqlalchemy.org/en/20/orm/quickstart.html)
- [`TypeEngine.with_variant()`](https://docs.sqlalchemy.org/en/20/core/type_api.html#sqlalchemy.types.TypeEngine.with_variant) — the JSONB/JSON per-dialect trick.
- [PostgreSQL `JSONB` type](https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#sqlalchemy.dialects.postgresql.JSONB)
- [`Uuid` type](https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.Uuid)
- [`Session.merge()`](https://docs.sqlalchemy.org/en/20/orm/session_state_management.html#merging) and [`Session.get()`](https://docs.sqlalchemy.org/en/20/orm/queryguide/select.html#getting-by-primary-key)
- [Connection pooling / `pool_pre_ping`](https://docs.sqlalchemy.org/en/20/core/pooling.html#disconnect-handling-pessimistic)

**Alembic (async)**
- [Using Asyncio with Alembic (cookbook)](https://alembic.sqlalchemy.org/en/latest/cookbook.html#using-asyncio-with-alembic) — the `run_sync(do_run_migrations)` env template.
- [Auto Generating Migrations](https://alembic.sqlalchemy.org/en/latest/autogenerate.html) — `compare_type`, `compare_server_default`.
- [`alembic.ini` configuration](https://alembic.sqlalchemy.org/en/latest/tutorial.html#editing-the-ini-file)

**Drivers**
- [asyncpg documentation](https://magicstack.github.io/asyncpg/current/) — and the [prepared-statement / PgBouncer note](https://magicstack.github.io/asyncpg/current/faq.html#why-am-i-getting-prepared-statement-errors).
- [aiosqlite](https://aiosqlite.omnilib.dev/en/stable/)

**PostgreSQL**
- [Multicolumn Indexes](https://www.postgresql.org/docs/current/indexes-multicolumn.html) — leftmost-prefix rule for `(status, created_at)`.
- [JSON Types & Indexing (`JSONB`, GIN)](https://www.postgresql.org/docs/current/datatype-json.html#JSON-INDEXING)
- [Data Types](https://www.postgresql.org/docs/current/datatype.html) — `timestamptz` vs `timestamp`.
- [UUID functions / `gen_random_uuid`](https://www.postgresql.org/docs/current/functions-uuid.html); UUIDv7 background: [RFC 9562](https://www.rfc-editor.org/rfc/rfc9562).

**Containers**
- [Docker Compose specification](https://docs.docker.com/compose/compose-file/) — services, healthcheck, volumes, `depends_on` conditions.
- [`postgres` image](https://hub.docker.com/_/postgres) · [`redis` image](https://hub.docker.com/_/redis) · [MinIO docs](https://min.io/docs/minio/container/index.html) · [MinIO Client `mc`](https://min.io/docs/minio/linux/reference/minio-mc.html).

## 15. Navigation

| ← Previous | Index | Next → |
|-----------|-------|--------|
| [Phase 2 — Concurrency, Retry, Ports](phase-2-concurrency-retry-ports.md) | [All phases](README.md) | [Phase 4 — Object Store & Provider Adapters](phase-4-object-store-providers.md) |
