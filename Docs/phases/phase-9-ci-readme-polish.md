# Phase 9 — CI, README, Polish

> **Part of:** [Asynchronous AI Serving Engine](../implementation-plan.md) · [Problem Statement](../problem-statement.md)
> **Status:** Planned (greenfield) · **Depends on:** [Phases 1–8](#3-prerequisites--inputs) (every prior phase contributes a test or artifact that CI exercises) · **Unlocks:** project complete — the engine is now reproducible from a clean clone and continuously verified
> **Delivers:** A two-job GitHub Actions pipeline (`quality` with zero infrastructure, `integration` with service containers), a complete `README.md` that tells the architecture-and-decisions story, a finalized `poethepoet` task catalog, and a fully-populated exit-criteria → test-file traceability matrix.
> **Primary skills applied:** github-actions-templates, cicd-automation-workflow-automate, readme, documentation, technical-change-tracker, docs-architect, mermaid-expert

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

Phase 9 is the **capstone** of the build. By the end of [Phase 8](phase-8-containerization-compose.md) the engine is fully runnable — infrastructure, API, and worker all start from `docker compose up --build`, and a `smoke.ps1` script exercises the happy path end-to-end. What is *not* yet true is that any of it is **continuously verified** or **legible to a first-time reader**. Phase 9 closes both gaps.

There is no new production code in this phase. Instead we add three artifacts that turn a working prototype into a credible, interview-ready portfolio project:

1. **`.github/workflows/ci.yml`** — a Continuous Integration pipeline with two jobs:
   - **`quality`** — checkout → install [`uv`](https://docs.astral.sh/uv/) (cached) → `uv sync` → `ruff check` + `ruff format --check` → `mypy` → `pytest -m "not integration" --cov`. **This job needs zero infrastructure** because Phases 1–7 deliberately made the entire fast suite runnable with in-process fakes, `fakeredis`, and `aiosqlite`. It is the gate that runs in seconds on every push.
   - **`integration`** — spins up PostgreSQL, Redis, and MinIO via GitHub Actions [service containers](https://docs.github.com/en/actions/using-containerized-services/about-service-containers) (with health-check options), runs `alembic upgrade head`, then `pytest -m integration`. This proves the real adapters against real infrastructure.

2. **`README.md`** — the front door. A Mermaid architecture diagram of the five-layer system, a port ↔ adapter table, a three-command quickstart, a "Design decisions" narrative (the offloader spy, Redis Streams vs Celery, message-as-pointer), and the **exit-criteria → test-file mapping table** that the [implementation plan](../implementation-plan.md) flags as a strong evaluation signal.

3. **The finalized `poethepoet` task catalog** — `fmt`, `lint`, `typecheck`, `test`, `test-int`, `check`, `api`, `worker`, `up`, `down`, `migrate`, `smoke`. These are the *single source of truth* for "how do I run X", invoked identically by a developer locally (`uv run poe check`) and by CI (`uv run poe ...`), eliminating drift between "what CI does" and "what I can reproduce".

> [!IMPORTANT]
> The defining property of this phase — and the reason the fast suite can run with **zero infrastructure** — is a decision made all the way back in [Phase 2](phase-2-concurrency-retry-ports.md) and [Phase 4](phase-4-object-store-providers.md): every external boundary is behind a `Protocol` port with a deterministic fake, and offloading is proven with a `RecordingOffloader` **spy** rather than a stopwatch. CI inherits that determinism for free. If your tests needed Docker to assert "the SDK call was offloaded", the `quality` job could not exist.

### Concrete objectives

| # | Objective | Done when |
|---|-----------|-----------|
| O1 | Fast suite runs with no infra in CI | `quality` job green using only `uv sync` + fakes/fakeredis/aiosqlite |
| O2 | Real adapters verified against real infra | `integration` job green with postgres/redis/minio service containers + `alembic upgrade head` |
| O3 | `uv` install is cached for fast CI | `astral-sh/setup-uv` with `enable-cache: true` keyed on `uv.lock` |
| O4 | README tells the full story | Architecture diagram, port↔adapter table, 3-command quickstart, design narrative, exit-criteria table all present |
| O5 | One task catalog, used everywhere | All 12 poe tasks defined in `pyproject.toml`; CI calls `uv run poe ...`, never re-spells commands |
| O6 | Exit criteria are traceable to tests | Every spec exit criterion maps to a concrete, named test file authored in Phases 1–8 |

> [!NOTE]
> This document grounds all CI YAML in the **current** GitHub Actions and `astral-sh/setup-uv` APIs (verified against the official docs — see [References](#14-references--further-reading)). Action inputs that do not exist are never invented. Where an image (MinIO) lacks a built-in health probe, the doc uses an explicit readiness step rather than a fragile in-image `--health-cmd`, and explains exactly why.

---

## 2. Where This Fits

Phase 9 does not add a runtime layer — it *wraps* the entire five-layer system in an automated verification harness and a documentation surface. The diagram below shows the full architecture (unchanged since Phase 8) with the **CI/Docs surface** highlighted as the meta-layer that observes everything.

```mermaid
flowchart TD
    subgraph CI["🟩 Phase 9 — CI & Docs meta-layer"]
        direction LR
        Q["quality job<br/>ruff · mypy · pytest -m 'not integration' --cov"]
        I["integration job<br/>services + alembic + pytest -m integration"]
        RM["README.md<br/>arch diagram · port↔adapter · quickstart · exit-criteria map"]
        POE["poe task catalog<br/>fmt lint typecheck test test-int check api worker up down migrate smoke"]
    end

    subgraph APP["Application (Phases 1, 6, 7)"]
        API["FastAPI ingestion API<br/>POST /v1/jobs → 202"]
        WORK["Async worker<br/>consume_once loop"]
        SVC["Services: ingestion / processor / pipelines"]
        DOM["Domain: InferenceJob state machine"]
    end

    subgraph PORTS["Ports & core (Phases 1, 2)"]
        OFF["SyncOffloader · ThreadOffloader"]
        RET["tenacity retry policy"]
        P["Protocol ports:<br/>JobRepository · JobQueue · ObjectStore · providers"]
    end

    subgraph ADAPTERS["Adapters (Phases 3, 4, 5)"]
        REPO["SQLAlchemy repository"]
        BRK["Redis Streams producer/consumer"]
        S3["S3ObjectStore (boto3)"]
        PROV["Providers: fake* / HF / Pinecone / DDGS"]
    end

    subgraph INFRA["Infrastructure (Phases 3, 8 compose)"]
        PG[("PostgreSQL 16")]
        RD[("Redis 7 Streams")]
        MIN[("MinIO (S3-compatible)")]
    end

    API --> SVC --> DOM
    WORK --> SVC
    SVC --> PORTS
    PORTS --> ADAPTERS
    REPO --> PG
    BRK --> RD
    S3 --> MIN

    Q -.runs.-> APP
    Q -.runs.-> PORTS
    Q -.runs against fakes.-> ADAPTERS
    I -.runs against real infra.-> INFRA
    POE -.invokes.-> Q
    POE -.invokes.-> I
    RM -.documents.-> APP
    RM -.documents.-> ADAPTERS

    classDef ci fill:#dcffe4,stroke:#22863a,stroke-width:2px,color:#000;
    class Q,I,RM,POE ci;
```

**Connection to prior phases.** CI is the *consumer* of everything built before it:

- The **`quality`** job runs the unit/contract suites authored in Phases 1, 2, 4, 5, 6, 7 — all of which are infra-free by construction.
- The **`integration`** job runs the `-m integration` tests authored in Phases 3 (`test_repository_pg.py`), 4 (`test_minio_roundtrip.py`), 5 (`test_broker_redis.py`), 6 (lifespan pool assertion), and 7 (`test_worker_end_to_end.py`).
- The **`poe` catalog** finalizes the task stubs introduced in [Phase 1](phase-1-scaffold-toolchain-domain.md)'s `pyproject.toml` and used informally through Phases 3–8.
- The **README** synthesizes the design decisions locked in the [implementation plan](../implementation-plan.md) and elaborated across all nine phase docs.

**Connection forward.** There is no Phase 10 — Phase 9 *is* the terminus. Its definition of done is the project's definition of done.

---

## 3. Prerequisites & Inputs

Everything CI runs must already exist. This phase assumes the following are complete and merged.

| Prerequisite | Produced in | Why Phase 9 needs it |
|--------------|-------------|----------------------|
| `pyproject.toml` with `[tool.ruff]`, `[tool.mypy]`, `[tool.pytest.ini_options]` (`asyncio_mode="auto"`, `integration` marker), `[tool.poe.tasks]` stubs | [Phase 1](phase-1-scaffold-toolchain-domain.md) | CI calls `ruff`, `mypy`, `pytest`, and `poe` tasks defined here |
| `uv.lock` committed | [Phase 1](phase-1-scaffold-toolchain-domain.md) | `uv sync --frozen` and cache key both depend on it |
| `RecordingOffloader` spy + attempt-counting retry tests | [Phase 2](phase-2-concurrency-retry-ports.md) | Core of the deterministic, infra-free `quality` suite |
| Async SQLAlchemy repository + Alembic async migrations | [Phase 3](phase-3-persistence-sqlalchemy-alembic.md) | `integration` job runs `alembic upgrade head` then `test_repository_pg.py` |
| `S3ObjectStore`, fakes, real provider adapters, offload-invariant test | [Phase 4](phase-4-object-store-providers.md) | Unit suite (fakes) in `quality`; `test_minio_roundtrip.py` in `integration` |
| Redis Streams producer/consumer with `consume_once()` seam | [Phase 5](phase-5-redis-streams-broker.md) | `fakeredis` unit tests in `quality`; `test_broker_redis.py` in `integration` |
| `AppContainer` + FastAPI app factory + leak test | [Phase 6](phase-6-composition-root-fastapi-api.md) | API tests (ASGITransport + fakes) in `quality`; pool-leak assertion in `integration` |
| Worker runner + pipelines + end-to-end test | [Phase 7](phase-7-worker-pipelines.md) | `test_worker_end_to_end.py` in `integration` |
| `docker/Dockerfile`, full `docker-compose.yml`, `scripts/smoke.ps1` | [Phase 8](phase-8-containerization-compose.md) | Referenced by README quickstart and the `smoke` poe task |

> [!TIP]
> A fast way to confirm the prerequisites locally before touching CI: run `uv run poe check` (the aggregate of lint + typecheck + fast tests). If it is green on your machine with **no Docker running**, the `quality` job will be green on GitHub. That equivalence is the entire point of routing CI through `poe`.

> [!WARNING]
> `uv sync --frozen` (used in CI) will **fail** if `uv.lock` is out of date relative to `pyproject.toml`. If you change dependencies, regenerate the lock with `uv lock` and commit it in the same PR. CI must never silently re-resolve dependencies — that would make the build non-reproducible.

---

## 4. Deliverables

| File | Type | Purpose |
|------|------|---------|
| `.github/workflows/ci.yml` | new | Two-job CI pipeline: `quality` (no infra) + `integration` (service containers) |
| `README.md` | changed | Full project front door: architecture diagram, port↔adapter table, quickstart, design narrative, exit-criteria map |
| `pyproject.toml` | changed | Finalize `[tool.poe.tasks]` with the complete 12-task catalog |
| `.github/workflows/` | new dir | Conventional location GitHub scans for workflows |

> [!NOTE]
> `pyproject.toml` is touched only to finalize the `[tool.poe.tasks]` table — its `[tool.ruff]`, `[tool.mypy]`, and `[tool.pytest.ini_options]` sections were authored in [Phase 1](phase-1-scaffold-toolchain-domain.md) and are reproduced here only for context where CI depends on them.

---

## 5. Design Decisions & Rationale

| Decision | Choice | Why | Rejected alternative |
|----------|--------|-----|----------------------|
| Test framework split | Two jobs: `quality` (no infra) + `integration` (services) | The fast gate gives sub-minute feedback on every push and never flakes on container startup; integration is isolated so its slower, infra-dependent nature does not block the fast loop | One monolithic job that always boots Docker — slow, flaky, and couples fast feedback to infra health |
| `uv` install action | `astral-sh/setup-uv` with `enable-cache: true` | First-party Astral action; native uv cache keyed on lockfile; far faster than `pip` + manual cache plumbing | `actions/setup-python` + `pip install uv` + hand-rolled `actions/cache` (more YAML, more drift, no uv-aware pruning) |
| CI command surface | CI calls `uv run poe <task>` | Single source of truth: the exact command a developer runs locally is the command CI runs. Zero "works on my machine" drift | Re-spelling `ruff check . && mypy ...` inline in YAML — duplicated logic that silently diverges from local tooling |
| Infra in `integration` | GitHub Actions `services:` containers with health `options:` | Native, declarative, lifecycle-managed by the runner; the runner waits for health before steps run (for images that ship a health probe) | `docker compose up` inside the job (works, but you re-implement health-waiting and teardown that `services:` gives free) |
| MinIO readiness | Explicit shell wait step polling `/minio/health/live`, **not** an in-image `--health-cmd` | The official `minio/minio` image ships **no `curl`/`wget`**, so a curl-based `--health-cmd` inside the service container fails immediately. An explicit step on the runner (which *has* curl) is robust and honest | A `--health-cmd "curl .../minio/health/live"` on the service — fails because the binary is absent in the image |
| Dependency reproducibility | `uv sync --frozen` | Forces CI to use the committed `uv.lock` exactly; a stale lock fails loudly instead of silently re-resolving | `uv sync` (allows re-resolution → non-reproducible CI) |
| Coverage | `pytest --cov=app` in `quality`, artifact uploaded | Coverage is measured on the deterministic suite where it is meaningful and stable | Measuring coverage across the integration job too (slower, and coverage of glue/IO code is noisy) |
| Trigger | `push` to `main` + `pull_request` | Standard PR gating; both jobs are required status checks | `on: [push]` only — misses PR-time gating, the primary use case for a portfolio repo |
| Concurrency | `concurrency:` group cancels superseded runs per ref | Saves runner minutes; only the latest commit on a branch matters | No concurrency control — stale runs waste minutes and clutter status |

### Why the `quality` job can run with zero infrastructure

This is the single most important architectural payoff to articulate in an interview, so it gets its own section.

```mermaid
flowchart LR
    subgraph prod["Production wiring (real)"]
        A1["adapter"] -->|"retrying → offloader.run"| SDK["sync SDK / boto3 / redis"]
    end
    subgraph test["quality job wiring (deterministic)"]
        A2["adapter under test"] -->|"retrying → offloader.run"| SPY["RecordingOffloader spy<br/>(records qualname+args, runs inline)"]
        A3["service under test"] --> FK["Fake providers / fakeredis / aiosqlite"]
    end
    SPY -.->|"assert call was offloaded<br/>WITHOUT measuring time"| OK1["✅ deterministic"]
    FK -.->|"no network, no Docker"| OK2["✅ deterministic"]
```

- **Offloading is proven by a spy, not a clock.** ([Phase 2](phase-2-concurrency-retry-ports.md), [Phase 4](phase-4-object-store-providers.md).) The `RecordingOffloader` records `fn.__qualname__` and arguments and executes the function inline. A test asserts "this adapter method routed its SDK call through the offloader" by inspecting the recording — never by timing two coroutines and hoping the scheduler cooperates.
- **Retries are counted, not timed.** ([Phase 2](phase-2-concurrency-retry-ports.md).) Tests set `base_delay_s=0` in `RetrySettings` and assert the *number* of attempts and the terminal exception type. No `wait_exponential_jitter` wall-clock sleep ever runs in the fast suite.
- **Brokers are faked in-process.** ([Phase 5](phase-5-redis-streams-broker.md).) `fakeredis.aioredis` implements the stream commands; `consume_once()` is a deterministic seam the test drives exactly once.
- **Persistence uses SQLite.** ([Phase 3](phase-3-persistence-sqlalchemy-alembic.md).) `JobRow.payload` uses `JSONB.with_variant(JSON, "sqlite")` so the same repository code runs on `aiosqlite` with no PostgreSQL.
- **Drains are Event-gated, not slept.** ([Phase 5](phase-5-redis-streams-broker.md), [Phase 6](phase-6-composition-root-fastapi-api.md).) Shutdown is driven by `asyncio.Event` and asserted by observing the loop exit, not `await asyncio.sleep(...)`.

> [!IMPORTANT]
> CI does not *make* the suite deterministic — the suite is deterministic by design, and CI simply runs it. The lesson worth stating plainly: **investing in dependency-injected test seams (spies, fakes, deterministic clocks) is what makes a fast, reliable CI gate possible.** A test suite that needs a live Redis to assert "we offloaded the call" cannot be a 30-second PR gate.

---

## 6. Detailed Implementation

This section provides the complete, runnable files. Every action version, input name, and shell command is grounded against current official documentation (see [References](#14-references--further-reading)); nothing is invented.

### 6.1 `.github/workflows/ci.yml`

**Purpose & responsibilities.** Define the two-job pipeline. `quality` gates code health with no external dependencies. `integration` validates the real adapters against real services. Both call into the `poe` task catalog so the commands match local development exactly.

```yaml
# .github/workflows/ci.yml
# Continuous Integration for the Asynchronous AI Serving Engine.
#
# Two jobs:
#   quality      — lint + typecheck + fast tests (NO infrastructure).
#   integration  — real adapters against postgres/redis/minio service containers.
#
# Both jobs install uv via the first-party astral-sh/setup-uv action (cached on
# uv.lock) and route every command through `uv run poe <task>` so CI executes the
# exact commands a developer runs locally — no inline command drift.
name: CI

# Run on every push to main and on every pull request targeting main.
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

# Cancel an in-flight run when a newer commit is pushed to the same ref.
# Saves runner minutes; only the latest commit on a branch needs verifying.
concurrency:
  group: ci-${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

# Principle of least privilege: the workflow only needs to read the repo.
permissions:
  contents: read

# Pin the Python version once; both jobs reference it. Matches requires-python >= 3.12.
env:
  PYTHON_VERSION: "3.12"

jobs:
  # ---------------------------------------------------------------------------
  # JOB 1 — quality: zero-infrastructure gate. Runs in seconds on every push.
  # ---------------------------------------------------------------------------
  quality:
    name: Quality (lint · types · fast tests)
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      # Check out the repository at the triggering commit.
      - name: Checkout
        uses: actions/checkout@v6

      # Install uv and enable its built-in dependency cache.
      # enable-cache: true caches the uv cache dir; the key is derived from the
      # files matched by cache-dependency-glob (we pin it to the committed lock).
      - name: Install uv (cached)
        uses: astral-sh/setup-uv@v8
        with:
          enable-cache: true
          cache-dependency-glob: "uv.lock"
          python-version: ${{ env.PYTHON_VERSION }}

      # Create the project virtualenv from the EXACT committed lockfile.
      # --frozen fails loudly if uv.lock is stale vs pyproject.toml.
      - name: Install dependencies
        run: uv sync --frozen --all-extras --dev

      # Lint without modifying files. `poe lint` == `ruff check .`
      - name: Ruff lint
        run: uv run poe lint

      # Verify formatting without rewriting. `poe fmt-check` == `ruff format --check .`
      - name: Ruff format check
        run: uv run poe fmt-check

      # Strict static typing. `poe typecheck` == `mypy src tests`
      - name: Mypy (strict)
        run: uv run poe typecheck

      # Fast test suite only — everything NOT marked `integration`.
      # `poe test` == `pytest -m "not integration" --cov=app --cov-report=xml --cov-report=term-missing`
      # Needs no Docker: fakes + fakeredis + aiosqlite (see Phases 2/3/4/5).
      - name: Pytest (fast, with coverage)
        run: uv run poe test

      # Publish the coverage report as a build artifact for inspection.
      - name: Upload coverage report
        if: always() # upload even if tests failed, so failures are diagnosable
        uses: actions/upload-artifact@v4
        with:
          name: coverage-xml
          path: coverage.xml
          if-no-files-found: warn

  # ---------------------------------------------------------------------------
  # JOB 2 — integration: real adapters against real infrastructure.
  # The job runs DIRECTLY ON THE RUNNER (not in a container), so service
  # containers are reached via localhost on their mapped host ports.
  # ---------------------------------------------------------------------------
  integration:
    name: Integration (postgres · redis · minio)
    runs-on: ubuntu-latest
    timeout-minutes: 20

    # Service containers managed by the runner. Postgres and Redis ship health
    # probes (pg_isready / redis-cli), so the runner waits for `healthy` before
    # running steps. MinIO does NOT ship curl/wget, so we give it no in-image
    # health-cmd and instead add an explicit readiness step below.
    services:
      postgres:
        image: postgres:16-alpine
        env:
          POSTGRES_USER: aie
          POSTGRES_PASSWORD: aie
          POSTGRES_DB: aie
        ports:
          - 5432:5432 # HOST:CONTAINER — reachable at localhost:5432 on the runner
        options: >-
          --health-cmd "pg_isready -U aie -d aie"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5

      redis:
        image: redis:7-alpine
        ports:
          - 6379:6379
        options: >-
          --health-cmd "redis-cli ping"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5

      minio:
        image: minio/minio:latest
        env:
          MINIO_ROOT_USER: minioadmin
          MINIO_ROOT_PASSWORD: minioadmin
        ports:
          - 9000:9000 # S3 API
          - 9001:9001 # web console
        # NOTE: `command`/`args` cannot be set on the `services:` short form the
        # way they can in compose; `minio/minio` requires a `server` subcommand.
        # We override it via the documented `options` entrypoint hack OR — more
        # robustly — start MinIO from a step. See the readiness step below and
        # the troubleshooting table for the rationale. For the service form we
        # rely on the default entrypoint plus an explicit `mc`/curl readiness wait.

    # Environment for ALL steps in this job. Adapters read AIE_*-prefixed settings.
    # Hosts are localhost because the job runs on the runner, not in a container.
    env:
      AIE_ENV: test
      AIE_DATABASE_URL: postgresql+asyncpg://aie:aie@localhost:5432/aie
      AIE_REDIS_URL: redis://localhost:6379/0
      AIE_OBJECT_STORE__ENDPOINT_URL: http://localhost:9000
      AIE_OBJECT_STORE__ACCESS_KEY_ID: minioadmin
      AIE_OBJECT_STORE__SECRET_ACCESS_KEY: minioadmin
      AIE_OBJECT_STORE__BUCKET: aie-artifacts
      AIE_OBJECT_STORE__FORCE_PATH_STYLE: "true"

    steps:
      - name: Checkout
        uses: actions/checkout@v6

      - name: Install uv (cached)
        uses: astral-sh/setup-uv@v8
        with:
          enable-cache: true
          cache-dependency-glob: "uv.lock"
          python-version: ${{ env.PYTHON_VERSION }}

      - name: Install dependencies
        run: uv sync --frozen --all-extras --dev

      # MinIO ships no health probe binary, so the runner cannot wait on it.
      # The runner host DOES have curl — poll the documented liveness endpoint
      # until it returns 200, then fail after a bounded number of attempts.
      - name: Wait for MinIO to be ready
        run: |
          echo "Polling MinIO liveness endpoint http://localhost:9000/minio/health/live"
          for attempt in $(seq 1 30); do
            if curl --silent --fail http://localhost:9000/minio/health/live; then
              echo "MinIO is live (attempt ${attempt})."
              exit 0
            fi
            echo "MinIO not ready yet (attempt ${attempt}); sleeping 2s..."
            sleep 2
          done
          echo "::error::MinIO did not become ready within 60s"
          exit 1

      # Create the artifacts bucket using the MinIO client. In production the
      # composition root calls ObjectStore.ensure_bucket() in dev; in CI we
      # provision it explicitly so the integration tests have a target bucket.
      - name: Create MinIO bucket
        run: |
          docker run --rm --network host \
            --entrypoint /bin/sh minio/mc:latest -c '
              mc alias set local http://localhost:9000 minioadmin minioadmin &&
              mc mb --ignore-existing local/aie-artifacts &&
              mc ls local
            '

      # Apply all Alembic migrations to the fresh Postgres service.
      # `poe migrate` == `alembic upgrade head` (reads AIE_DATABASE_URL).
      - name: Run database migrations
        run: uv run poe migrate

      # Integration suite ONLY — everything marked `integration`.
      # `poe test-int` == `pytest -m integration`
      - name: Pytest (integration)
        run: uv run poe test-int
```

#### Walkthrough of the non-obvious parts

> [!IMPORTANT]
> **`localhost`, not the service name.** Because both jobs use `runs-on: ubuntu-latest` (the steps execute *directly on the runner VM*, not inside a job container), service containers are reached at `localhost:<mapped-host-port>`. The `ports:` mapping (`5432:5432`) is what makes them reachable — without it, the container's ports are not published to the runner. This is the [documented behavior](https://docs.github.com/en/actions/using-containerized-services/about-service-containers) for runner-hosted jobs. (If the job itself ran *inside* a container via `container:`, you would instead address services by their label, e.g. `postgres:5432`, on a user-defined Docker network — but we do not do that here.)

> [!WARNING]
> **MinIO has no in-image health probe.** The official `minio/minio` image is a minimal distroless-style image that ships **neither `curl` nor `wget`**. A `--health-cmd "curl -f http://localhost:9000/minio/health/live"` placed on the *service container* therefore fails instantly (binary not found), and the runner would either hang waiting for `healthy` or treat the service as unhealthy. The robust pattern, used above, is:
> 1. Give the MinIO service **no** `--health-cmd`.
> 2. Add an explicit **"Wait for MinIO"** step that runs on the runner host (which *does* have `curl`) and polls `http://localhost:9000/minio/health/live` until HTTP 200.
> 3. Provision the bucket with a throwaway `minio/mc` container over `--network host`.
> This keeps the readiness logic honest and visible in the logs.

> [!NOTE]
> **The MinIO `server` subcommand caveat.** `minio/minio`'s default entrypoint expects a `server <DATA_DIR>` subcommand (e.g. `server /data --console-address ":9001"`). GitHub Actions `services:` short syntax exposes `image`, `ports`, `env`, `options`, plus `credentials`, `volumes`, and (per the [service container reference](https://docs.github.com/en/actions/using-workflows/workflow-syntax-for-github-actions#jobsjob_idservices)) `options` lets you pass raw `docker create` flags. If your chosen MinIO image tag requires the explicit `server` argument and it cannot be supplied cleanly via the service form, the **fully portable fallback** is to drop MinIO from `services:` and instead `docker run -d` it as the first step of the job (where you control the full command line), then run the same readiness poll. Both approaches are shown in the [troubleshooting table](#12-common-pitfalls--troubleshooting); pick whichever your runner image version accepts. The readiness step and tests are identical either way.

> [!TIP]
> **Why `uv sync --frozen --all-extras --dev`.** `--frozen` enforces the lockfile (reproducibility). `--all-extras` installs optional extras such as `boto3-stubs[s3]` if exposed as an extra. `--dev` pulls the dev dependency group (`pytest`, `mypy`, `ruff`, `poethepoet`, `fakeredis`, `aiosqlite`, etc.). Without `--dev`, `poe` itself would not be installed and `uv run poe ...` would fail.

> [!CAUTION]
> **Do not put real secrets in `env:`.** The credentials above (`minioadmin`, `aie`) are ephemeral, used only by throwaway service containers inside the job, and are safe to hardcode. **Never** hardcode real provider keys (HuggingFace, Pinecone) — the engine's fakes are the test default precisely so CI needs **zero** real keys. If you ever add an integration test that hits a real provider, inject the key via `secrets.*`, never a literal.

### 6.2 `pyproject.toml` — finalized `[tool.poe.tasks]`

**Purpose & responsibilities.** [`poethepoet`](https://poethepoet.natn.io/) is a task runner that lives inside `pyproject.toml` and runs through `uv run poe <task>`. It is the single source of truth for "how to do X", invoked identically by humans and CI, and it is **cross-platform** — no Makefile (which needs `make` on Windows) and no PowerShell/bash divergence.

```toml
# pyproject.toml  (excerpt — the finalized task catalog for Phase 9)
#
# Run any task with:  uv run poe <task>
# List all tasks with: uv run poe --help
[tool.poe.tasks]

# --- Code quality -----------------------------------------------------------
fmt        = { cmd = "ruff format .",            help = "Auto-format all code (rewrites files)" }
fmt-check  = { cmd = "ruff format --check .",    help = "Verify formatting without rewriting (CI gate)" }
lint       = { cmd = "ruff check .",             help = "Lint with ruff (rules from [tool.ruff])" }
lint-fix   = { cmd = "ruff check --fix .",       help = "Lint and auto-apply safe fixes" }
typecheck  = { cmd = "mypy src tests",           help = "Strict static type checking (mypy --strict)" }

# --- Tests ------------------------------------------------------------------
test       = { cmd = "pytest -m 'not integration' --cov=app --cov-report=xml --cov-report=term-missing", help = "Fast suite (no infra): fakes + fakeredis + aiosqlite" }
test-int   = { cmd = "pytest -m integration",    help = "Integration suite (needs postgres/redis/minio)" }
test-all   = { cmd = "pytest --cov=app",         help = "Entire suite (fast + integration)" }

# --- Aggregate gate ---------------------------------------------------------
# `check` is the local mirror of the CI `quality` job: lint, format, types, fast tests.
check      = { sequence = ["lint", "fmt-check", "typecheck", "test"], help = "Run the full quality gate locally (mirrors CI `quality` job)" }

# --- Run the application ----------------------------------------------------
api        = { cmd = "uvicorn app.api.app:create_app --factory --host 0.0.0.0 --port 8000 --reload", help = "Run the FastAPI ingestion API (dev, with reload)" }
worker     = { cmd = "python -m app.worker",     help = "Run the async background worker" }

# --- Infrastructure & ops ---------------------------------------------------
up         = { cmd = "docker compose up -d postgres redis minio createbuckets", help = "Start infrastructure only (Postgres, Redis, MinIO) + provision the bucket" }
down       = { cmd = "docker compose down",       help = "Stop all containers (keeps named volumes / data)" }
down-v     = { cmd = "docker compose down -v",    help = "Stop containers AND delete named volumes (wipes data)" }
migrate    = { cmd = "alembic upgrade head",     help = "Apply all database migrations to the latest revision" }
smoke      = { cmd = "pwsh -File scripts/smoke.ps1", help = "End-to-end happy-path smoke test (POST a job, poll to terminal)" }
```

> [!NOTE]
> **`check` is the contract between local and CI.** A developer runs `uv run poe check`; the CI `quality` job runs the same four tasks as discrete steps (so each shows up as its own annotated check on the PR). Defining `check` as a `sequence` of the individual tasks — rather than re-spelling the commands — guarantees the local gate and CI gate can never diverge.

> [!TIP]
> **`smoke` runs the PowerShell script via `pwsh`.** Phase 8 authored `scripts/smoke.ps1` as the single end-to-end smoke check (POST a job, then poll to a terminal status). The poe task invokes it with `pwsh -File scripts/smoke.ps1` rather than re-implementing the flow: `pwsh` (PowerShell **Core**) is installed on the Windows dev box and is **pre-installed on the `ubuntu-latest` runner**, so one script serves every platform. If you would prefer a pure-Python entry, add `scripts/smoke.py` with a `main()` and switch this task to `script = "scripts.smoke:main"` — just never point the task at an entry that does not exist.

> [!WARNING]
> **Quote test markers carefully.** In `pytest -m 'not integration'` the single quotes are required so `not integration` is passed as one argument. `poethepoet`'s `cmd` form uses shell-like splitting; the quotes survive. If you switch to the `shell = "..."` task form, the same quoting applies — but `shell` tasks are not cross-platform (they invoke the OS shell), so prefer `cmd` for portability.

### 6.3 `README.md` — content blueprint

**Purpose & responsibilities.** The README is the project's front door — for a portfolio repo it is the *most-read file*. It must let a reviewer understand the architecture, run the project in three commands, grasp the headline design decisions, and verify the engineering claims against concrete tests, all without opening the source. Below is the complete content blueprint with example snippets; assemble these into `README.md`.

#### 6.3.1 Title, badges, and one-paragraph pitch

```markdown
# Asynchronous AI Serving Engine

[![CI](https://github.com/<owner>/<repo>/actions/workflows/ci.yml/badge.svg)](https://github.com/<owner>/<repo>/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.12%2B-blue)
![Typed](https://img.shields.io/badge/typed-mypy--strict-success)
![Lint](https://img.shields.io/badge/lint-ruff-orange)

A production-grade, **framework-agnostic** engine that decouples the *ingestion*
of AI inference jobs (FastAPI → `202 Accepted` in milliseconds) from their
*execution* (async workers pulling from Redis Streams). Built on **Hexagonal
(Ports & Adapters)** principles: the core never imports a web framework, every
blocking SDK call is offloaded off the event loop, every network boundary
retries with backoff, and the entire system runs **with zero cloud keys** thanks
to in-process fakes and a local MinIO/Postgres/Redis stack.
```

> [!TIP]
> Replace `<owner>/<repo>` with the real GitHub path. The CI badge URL is the canonical `actions/workflows/<file>.yml/badge.svg` form — it renders green/red automatically once the workflow has run on the default branch.

#### 6.3.2 Architecture diagram (five layers)

````markdown
## Architecture

```mermaid
flowchart TD
    Client([Client])

    subgraph L1["① API Ingestion Layer — FastAPI"]
        API["POST /v1/jobs → 202 + job_id<br/>GET /v1/jobs/{id}<br/>GET /health · /health/ready"]
    end

    subgraph L2["② Async Broker Layer — Redis Streams"]
        STREAM[("aie:jobs stream<br/>group: aie-workers<br/>DLQ: aie:jobs:dlq")]
        WORKER["Async worker<br/>XREADGROUP · semaphore backpressure<br/>XACK / XAUTOCLAIM reclaim"]
    end

    subgraph L3["③ Persistent Storage Layer — PostgreSQL"]
        DB[("jobs table<br/>status state machine<br/>source of truth")]
    end

    subgraph L4["④ Object Storage Layer — MinIO (S3)"]
        OBJ[("artifacts bucket<br/>s3://aie-artifacts/...")]
    end

    subgraph L5["⑤ Adapter Gateway Layer"]
        ADP["Providers via SyncOffloader + tenacity:<br/>Fake* (default) · HuggingFace · Pinecone · DDGS"]
    end

    Client -->|"submit job"| API
    API -->|"INSERT PENDING"| DB
    API -->|"XADD pointer {job_id, job_type, attempt}"| STREAM
    API -.->|"202 + status_url"| Client

    STREAM --> WORKER
    WORKER -->|"re-read row (idempotency)"| DB
    WORKER --> ADP
    WORKER -->|"put result bytes"| OBJ
    WORKER -->|"UPDATE SUCCESS/FAILED + result_ref"| DB

    Client -->|"poll status"| API
    API -->|"SELECT"| DB
```

**The decoupling in one sentence:** the API does the absolute minimum on the
request path — insert a `PENDING` row and publish a *pointer* — then returns
`202` immediately; all the slow, blocking work happens later in the worker.
````

#### 6.3.3 Port ↔ Adapter table

```markdown
## Ports & Adapters

The core defines **ports** as `typing.Protocol` (structural typing — adapters
conform without importing the abstraction). Each port has a deterministic
**fake** (the dev/test/demo default, zero keys) and a **real** adapter that
activates only when credentials are configured.

| Port (Protocol)   | Real adapter                          | Fake / test double        | Backing tech        |
|-------------------|---------------------------------------|---------------------------|---------------------|
| `SyncOffloader`   | `ThreadOffloader` (`asyncio.to_thread`)| `RecordingOffloader` spy  | thread pool executor|
| `JobRepository`   | `SqlAlchemyJobRepository`             | (SQLite via aiosqlite)    | PostgreSQL / asyncpg|
| `JobQueue`        | `StreamProducer`                      | `fakeredis.aioredis`      | Redis Streams       |
| `ObjectStore`     | `S3ObjectStore` (boto3)               | (real MinIO in int. tests)| MinIO / S3          |
| `EmbeddingProvider`| `HuggingFaceEmbedding`               | `FakeEmbedding`           | HF Inference API    |
| `LLMProvider`     | `HuggingFaceLLM`                      | `FakeLLM`                 | HF Inference API    |
| `VectorStore`     | `PineconeVectorStore`                 | `FakeVectorStore`         | Pinecone            |
| `SearchProvider`  | `DdgsSearch`                          | `FakeSearch`              | DuckDuckGo (ddgs)   |

> Adapters never touch their SDK except through `offloader.run(...)`, and every
> call is wrapped by `tenacity` retry on `TransientUpstreamError`. See
> [Phase 2](Docs/phases/phase-2-concurrency-retry-ports.md) and
> [Phase 4](Docs/phases/phase-4-object-store-providers.md).
```

#### 6.3.4 Three-command quickstart

````markdown
## Quickstart

Requires [`uv`](https://docs.astral.sh/uv/getting-started/installation/),
Docker, and Python 3.12+. **No cloud keys needed** — fakes are the default.

```bash
# 1. Install dependencies into a project virtualenv from the lockfile
uv sync

# 2. Start infrastructure (Postgres + Redis + MinIO) and run migrations
uv run poe up        # starts Postgres + Redis + MinIO + bucket (infra only)
uv run poe migrate   # alembic upgrade head

# 3. Run the API and the worker (two terminals)
uv run poe api       # FastAPI on http://localhost:8000  (docs at /docs)
uv run poe worker    # async worker consuming the Redis stream
```

Submit a job and watch it complete — all with **zero** external API keys:

```bash
# Submit a RAG query (returns 202 + a job_id immediately)
curl -s -X POST http://localhost:8000/v1/jobs \
  -H "X-API-Key: dev-key" \
  -H "Content-Type: application/json" \
  -d '{"payload": {"job_type": "rag_query", "query": "What is hexagonal architecture?", "top_k": 3}}'

# Poll the status URL until it reads SUCCESS; the result_ref points into MinIO.
curl -s http://localhost:8000/v1/jobs/<job_id> -H "X-API-Key: dev-key"
```

Inspect stored artifacts in the MinIO console at **http://localhost:9001**
(default dev credentials `minioadmin`/`minioadmin`). Stop the stack with
`uv run poe down` (add `uv run poe down-v` to also delete the data volumes).
````

> [!NOTE]
> The README references `dev-key` as the example API key. This must match the `AIE_API_KEYS` value in `.env.example` (authored in [Phase 6](phase-6-composition-root-fastapi-api.md)). Keep them in sync, or the quickstart `curl` returns `401`.

#### 6.3.5 Design decisions narrative

This is the section a senior reviewer reads most closely. Include all three of the following, in prose, in the README:

```markdown
## Design Decisions

### 1. Offloading is proven by a spy, not a stopwatch
Every adapter receives a `SyncOffloader` port. The production implementation,
`ThreadOffloader`, is a thin pass-through to `asyncio.to_thread`; the composition
root installs a sized `ThreadPoolExecutor` as the loop's default executor, so a
single code path satisfies both the spec's letter ("offload via to_thread") and
the need for a bounded pool. In tests we inject a `RecordingOffloader` that
records each call's `fn.__qualname__` and arguments and runs it inline. This lets
us assert *"the boto3 call was routed through the offloader"* **deterministically,
without measuring time** — no flaky "did coroutine A interleave with B?" timing
tests. (See `tests/support/offloader.py`, `tests/unit/adapters/test_offload_invariant.py`.)

### 2. Redis Streams (custom worker), not Celery/arq
We implement a small async worker directly on Redis Streams
(`XADD`/`XREADGROUP`/`XACK`/`XAUTOCLAIM`) with semaphore-bounded backpressure,
rather than adopting Celery or arq. Why: (a) it keeps the dependency surface and
operational model small and fully inspectable; (b) consumer groups give us
at-least-once delivery, orphan reclaim, and a natural dead-letter path; (c) a
single `consume_once()` method is a clean, deterministic test seam we can drive
exactly once against `fakeredis` — no broker daemon, no beat scheduler, no
opaque framework lifecycle to mock. Celery would have hidden the very mechanics
(ack, reclaim, backpressure) this project exists to demonstrate.

### 3. Stream messages are pointers; Postgres is the source of truth
A stream entry carries only `{job_id, job_type, attempt}` — never the payload.
The full request, status, result reference, and error live in PostgreSQL. This
keeps the stream tiny and makes delivery semantics simple: because delivery is
at-least-once, the processor re-reads the row and an **idempotency guard** acks
and skips any job already in a terminal state. The database, not the queue, is
the arbiter of truth — so a redelivered or reclaimed message can never corrupt
state or double-charge an upstream provider.
```

#### 6.3.6 Exit-criteria → test mapping (in the README)

Embed a compact version of the [traceability table](#10-verification--exit-criteria-mapping) in the README so reviewers see the engineering rigor immediately. The authoritative, fully-expanded version lives in §10 of this document.

```markdown
## Exit Criteria → Tests

Every claim the engine makes is backed by a concrete, deterministic test:

| Spec exit criterion | Proven by (test file) | Phase |
|---------------------|------------------------|-------|
| Deterministic concurrency gates (no clock-dependent async tests) | `tests/unit/test_concurrency.py`, `tests/unit/adapters/test_offload_invariant.py`, `tests/unit/test_retry.py` | 2, 4 |
| Zero-cloud isolation (dev auto-redirects S3 → MinIO) | `tests/unit/test_config.py` (redirect validator), worker demo path | 1, 7 |
| Zero resource leaking (clean startup/teardown) | `tests/unit/test_container_lifecycle.py`, `tests/integration/test_worker_end_to_end.py` (`pool.checkedout() == 0`) | 6 |
| 202 ingestion in ms (no inline pipeline work) | `tests/unit/api/test_jobs.py` | 6 |
| to_thread + backoff at every boundary | `tests/unit/adapters/test_offload_invariant.py` (parametrized per method) | 4 |

Run the fast suite (no infra): `uv run poe test`.
Run integration (needs Docker): `uv run poe up && uv run poe migrate && uv run poe test-int`.
```

> [!IMPORTANT]
> The README's exit-criteria table is the single highest-signal element for an interview reviewer. Keep the **test file paths exact** so a reviewer can click straight to the proof. If a test file is renamed during implementation, update both this table and §10 of this doc.

---

## 7. Flow & Sequence Diagrams

### 7.1 CI pipeline flowchart (PR → jobs → gates)

```mermaid
flowchart TD
    PR["Pull request opened / commit pushed to main"] --> TRIG{"on: push(main) /<br/>pull_request(main)"}
    TRIG --> CONC["concurrency: cancel superseded runs for this ref"]
    CONC --> FORK(["Run both jobs in parallel"])

    FORK --> Q0
    FORK --> I0

    subgraph QUALITY["quality job — ubuntu-latest, NO infra"]
        direction TB
        Q0["checkout@v6"] --> Q1["setup-uv@v8 (enable-cache, key=uv.lock)"]
        Q1 --> Q2["uv sync --frozen --all-extras --dev"]
        Q2 --> Q3["poe lint  (ruff check)"]
        Q3 --> Q4["poe fmt-check  (ruff format --check)"]
        Q4 --> Q5["poe typecheck  (mypy --strict)"]
        Q5 --> Q6["poe test  (pytest -m 'not integration' --cov)"]
        Q6 --> Q7["upload coverage.xml artifact"]
    end

    subgraph INTEG["integration job — ubuntu-latest, service containers"]
        direction TB
        I0["checkout@v6"] --> I1["setup-uv@v8 (cached)"]
        I1 --> I2["uv sync --frozen --all-extras --dev"]
        I2 --> I3["wait for MinIO /minio/health/live (runner curl)"]
        I3 --> I4["mc mb aie-artifacts"]
        I4 --> I5["poe migrate  (alembic upgrade head)"]
        I5 --> I6["poe test-int  (pytest -m integration)"]
    end

    subgraph SERVICES["services: (health-gated by runner)"]
        SP[("postgres:16-alpine<br/>--health-cmd pg_isready")]
        SR[("redis:7-alpine<br/>--health-cmd redis-cli ping")]
        SM[("minio/minio<br/>no health-cmd → explicit step")]
    end
    SP -. localhost:5432 .-> INTEG
    SR -. localhost:6379 .-> INTEG
    SM -. localhost:9000 .-> INTEG

    Q7 --> GATE{"both jobs<br/>succeeded?"}
    I6 --> GATE
    GATE -->|yes| PASS([✅ Required checks pass → mergeable])
    GATE -->|no| FAIL([❌ Status red → merge blocked])

    classDef pass fill:#dcffe4,stroke:#22863a,color:#000;
    classDef fail fill:#ffeef0,stroke:#cb2431,color:#000;
    class PASS pass;
    class FAIL fail;
```

> [!NOTE]
> The two jobs run **in parallel** by default (no `needs:` dependency between them) — there is no reason to serialize a zero-infra lint/type gate behind a Docker-heavy integration job. Both must be green for the PR's required checks to pass; configure them as required status checks in the repository's branch-protection settings.

### 7.2 `setup-uv` cache lifecycle

```mermaid
sequenceDiagram
    autonumber
    participant Runner as GitHub Runner
    participant Action as astral-sh/setup-uv@v8
    participant Cache as Actions Cache
    participant uv as uv CLI

    Runner->>Action: with enable-cache: true, cache-dependency-glob: uv.lock
    Action->>Action: hash uv.lock → cache key
    Action->>Cache: restore key (uv cache dir)
    alt cache hit
        Cache-->>Action: restored cached wheels/metadata
        Note over Action: subsequent uv sync resolves from cache (fast)
    else cache miss
        Cache-->>Action: (nothing)
        Note over Action: uv sync downloads + populates cache
    end
    Runner->>uv: uv sync --frozen --all-extras --dev
    uv-->>Runner: virtualenv ready (.venv)
    Note over Action,Cache: on job end, action saves the (possibly pruned) cache<br/>under the lockfile-derived key for the next run
```

### 7.3 Integration job service-container readiness

```mermaid
sequenceDiagram
    autonumber
    participant Runner
    participant PG as postgres svc
    participant RD as redis svc
    participant MIN as minio svc
    participant Step as job steps

    Note over Runner,MIN: Runner starts service containers BEFORE the job's steps
    Runner->>PG: docker run + --health-cmd pg_isready (every 10s)
    Runner->>RD: docker run + --health-cmd redis-cli ping (every 10s)
    Runner->>MIN: docker run (NO health-cmd)
    PG-->>Runner: healthy
    RD-->>Runner: healthy
    Note over Runner,MIN: Runner waits only for services that HAVE a health probe
    Runner->>Step: begin steps (PG & RD already healthy)
    Step->>MIN: curl http://localhost:9000/minio/health/live (retry loop)
    MIN-->>Step: 200 OK
    Step->>MIN: mc mb aie-artifacts
    Step->>PG: alembic upgrade head
    Step->>Step: pytest -m integration
```

---

## 8. Configuration & Environment

Phase 9 introduces no new application settings — it *consumes* the `AIE_`-prefixed settings defined in [Phase 1](phase-1-scaffold-toolchain-domain.md) and threads them into the CI environment. The table below lists the environment the `integration` job sets so the real adapters bind to the service containers.

| Env var (CI `integration` job) | Value | Used by | Notes |
|--------------------------------|-------|---------|-------|
| `AIE_ENV` | `test` | `Settings` | Non-`prod` → triggers the zero-cloud MinIO redirect validator ([Phase 1](phase-1-scaffold-toolchain-domain.md)) |
| `AIE_DATABASE_URL` | `postgresql+asyncpg://aie:aie@localhost:5432/aie` | repository, Alembic | `localhost` because the job runs on the runner; port from `ports: 5432:5432` |
| `AIE_REDIS_URL` | `redis://localhost:6379/0` | broker producer/consumer | DB index 0; matches `ports: 6379:6379` |
| `AIE_OBJECT_STORE__ENDPOINT_URL` | `http://localhost:9000` | `S3ObjectStore` | Nested setting (`__` delimiter); points boto3 at the MinIO service |
| `AIE_OBJECT_STORE__ACCESS_KEY_ID` | `minioadmin` | `S3ObjectStore` | Matches the MinIO service `MINIO_ROOT_USER` |
| `AIE_OBJECT_STORE__SECRET_ACCESS_KEY` | `minioadmin` | `S3ObjectStore` | Matches `MINIO_ROOT_PASSWORD` |
| `AIE_OBJECT_STORE__BUCKET` | `aie-artifacts` | `S3ObjectStore` | Created by the `mc mb` step before tests |
| `AIE_OBJECT_STORE__FORCE_PATH_STYLE` | `true` | `S3ObjectStore` | MinIO requires path-style addressing (no virtual-host buckets) |
| `PYTHON_VERSION` (workflow `env`) | `3.12` | `setup-uv` | Sets `UV_PYTHON`; both jobs pin the same interpreter |

> [!IMPORTANT]
> **Nested settings use the `__` (double-underscore) delimiter.** `AIE_OBJECT_STORE__ENDPOINT_URL` maps to `Settings.object_store.endpoint_url` via `pydantic-settings`' `env_nested_delimiter="__"` (configured in [Phase 1](phase-1-scaffold-toolchain-domain.md)). Using a single underscore (`AIE_OBJECT_STORE_ENDPOINT_URL`) would **not** bind into the nested `ObjectStoreSettings` group. Confirm the exact field names against the `Settings` model you implemented; the names above follow the plan's `ObjectStoreSettings`.

> [!NOTE]
> The `integration` job sets credentials inline because they belong to throwaway containers torn down with the runner. The `quality` job sets **no** `AIE_*` vars at all — the fakes need none, and Settings falls back to its dev defaults (including the auto-redirect to `http://localhost:9000`, harmlessly unused because the fast suite never touches a real S3 client).

---

## 9. Testing Strategy

Phase 9 authors **no new application tests** — its job is to *run* the suite built across Phases 1–8 in CI. The "tests" of Phase 9 itself are (a) the workflow being syntactically valid and green on GitHub, and (b) the `poe` tasks resolving and executing. This section documents how the existing suite is partitioned and how CI exercises each partition, with emphasis on the deterministic, clock-free design that makes the `quality` job possible.

### 9.1 The marker-based partition

The entire suite is split by a single pytest marker, configured in [Phase 1](phase-1-scaffold-toolchain-domain.md):

```toml
# pyproject.toml (from Phase 1) — reproduced for context
[tool.pytest.ini_options]
asyncio_mode = "auto"               # pytest-asyncio: no @pytest.mark.asyncio needed
markers = [
    "integration: tests that require live infra (postgres/redis/minio)",
]
testpaths = ["tests"]
```

- **Unmarked tests** → the fast suite. Selected by `pytest -m "not integration"`. Run by `poe test` in the `quality` job. **Require no Docker.**
- **`@pytest.mark.integration` tests** → the integration suite. Selected by `pytest -m integration`. Run by `poe test-int` in the `integration` job. **Require service containers.**

```mermaid
flowchart LR
    SUITE["tests/"] --> U["Unmarked (fast)"]
    SUITE --> M["@pytest.mark.integration"]
    U -->|"poe test in quality job"| FAST["pytest -m 'not integration' --cov"]
    M -->|"poe test-int in integration job"| INT["pytest -m integration"]
    FAST --> NOINFRA["✅ fakes · fakeredis · aiosqlite — no Docker"]
    INT --> INFRA["🐳 postgres · redis · minio service containers"]
```

### 9.2 What runs in the `quality` job (fast, deterministic, infra-free)

| Test file | Phase | What it proves | Determinism technique |
|-----------|-------|----------------|------------------------|
| `tests/unit/test_config.py` | [1](phase-1-scaffold-toolchain-domain.md) | Zero-cloud redirect validator; env parsing | Pure function on `Settings` — no IO |
| `tests/unit/test_domain.py` | [1](phase-1-scaffold-toolchain-domain.md) | `InferenceJob` transition matrix; illegal moves raise | In-memory dataclass; no clock |
| `tests/unit/test_concurrency.py` | [2](phase-2-concurrency-retry-ports.md) | `ThreadOffloader` leaves the loop thread; spy records calls | Thread-identity assert + spy, not timing |
| `tests/unit/test_retry.py` | [2](phase-2-concurrency-retry-ports.md) | Retries only on `TransientUpstreamError`; attempt count | `base_delay_s=0`; **count** attempts, never time them |
| `tests/unit/adapters/test_offload_invariant.py` | [4](phase-4-object-store-providers.md) | Every adapter method routes its SDK call through the offloader | `RecordingOffloader` + stub SDK objects, parametrized |
| `tests/unit/adapters/test_fakes.py` | [4](phase-4-object-store-providers.md) | Fakes return deterministic, seeded outputs | Seeded hash vectors; fixed corpus |
| `tests/unit/broker/test_consumer.py` | [5](phase-5-redis-streams-broker.md) | `consume_once()` ack/retry/DLQ paths | `fakeredis.aioredis`; drive the seam exactly once |
| `tests/unit/api/test_jobs.py` | [6](phase-6-composition-root-fastapi-api.md) | `202` path inserts + publishes only (no inline work); `401`; `422` | `ASGITransport` + `dependency_overrides` with all-fakes container |
| `tests/unit/test_container_lifecycle.py` | [6](phase-6-composition-root-fastapi-api.md) | `aclose()` closes every client and shuts the executor | Stub clients with `closed` flags — observe state, not duration |
| `tests/unit/test_processor.py` | [7](phase-7-worker-pipelines.md) | Full status lifecycle + artifact bytes + error path | Recording fakes; assert call order |
| `tests/unit/test_pipelines.py` | [7](phase-7-worker-pipelines.md) | RAG/embed pipelines call ports in the right order | Recording fakes; deterministic assertions |

Representative example — the spy-based offload invariant (authored in Phase 4, *run* by the `quality` job). Reproduced here to make the clock-free guarantee concrete:

```python
# tests/unit/adapters/test_offload_invariant.py  (authored in Phase 4; CI runs it)
import pytest
from tests.support.offloader import RecordingOffloader


@pytest.mark.parametrize(
    ("make_adapter", "call", "expected_sdk_qualname"),
    [
        # (factory(offloader) -> adapter, async call on adapter, SDK method qualname)
        # ... one row per adapter method (S3 put/get, HF embed/complete, ...)
    ],
)
async def test_every_adapter_call_is_offloaded(
    make_adapter, call, expected_sdk_qualname
) -> None:
    spy = RecordingOffloader()           # records fn.__qualname__ + args, runs inline
    adapter = make_adapter(spy)

    await call(adapter)                  # exercise the adapter method

    # DETERMINISTIC assertion: the SDK call went THROUGH the offloader boundary.
    # No timing, no thread sniffing — just inspect what the spy recorded.
    assert expected_sdk_qualname in spy.recorded_qualnames
```

> [!IMPORTANT]
> Notice what is *absent*: no `time.time()`, no `asyncio.sleep`, no "run two coroutines and assert ordering". The spy turns a concurrency property ("we offloaded") into a value assertion ("the recording contains this qualname"). That is why the `quality` job is fast and never flaky — and it is the single most important thing to point at in an interview when asked "how do you test async offloading without flakiness?".

### 9.3 What runs in the `integration` job (real infra)

| Test file | Phase | What it proves | Infra used |
|-----------|-------|----------------|------------|
| `tests/integration/test_repository_pg.py` | [3](phase-3-persistence-sqlalchemy-alembic.md) | Repository round-trips against real PostgreSQL; row↔domain mapping | postgres |
| `tests/integration/test_minio_roundtrip.py` | [4](phase-4-object-store-providers.md) | `S3ObjectStore` put/get through real `ThreadOffloader` + boto3 | minio |
| `tests/integration/test_broker_redis.py` | [5](phase-5-redis-streams-broker.md) | Producer XADD → consumer XREADGROUP/XACK/reclaim against real Redis | redis |
| `tests/integration/test_worker_end_to_end.py` | [7](phase-7-worker-pipelines.md) | Full ingest→consume→process→persist→artifact path; `pool.checkedout() == 0` after teardown | postgres + redis + minio |

> [!NOTE]
> The integration suite still avoids wall-clock assertions wherever possible — e.g. the end-to-end test drives the consumer's `consume_once()` seam and polls the *database state*, not a timer, to confirm terminal status. "Integration" means *real infra*, not *flaky timing*.

### 9.4 Validating the workflow itself

Before pushing, validate the YAML locally so you do not burn CI runs on syntax errors.

```bash
# Option A: actionlint — static linter for GitHub Actions workflows (recommended).
#   Catches invalid keys, bad expressions, shell issues in run: blocks.
actionlint .github/workflows/ci.yml

# Option B: act — run the workflow locally in Docker (heavier, optional).
#   Useful to dry-run the quality job; integration services may not all map cleanly.
act pull_request -j quality
```

> [!TIP]
> Add `actionlint` to your pre-push checklist (or a local-only poe task) — it is the cheapest way to catch a malformed `services:` block or a typo in an action input before GitHub does. It is intentionally **not** a CI step here (CI failing on its own malformed YAML is circular), but it is invaluable locally.

---

## 10. Verification & Exit-Criteria Mapping

This is the heart of the phase's interview value: a complete, expanded reproduction of the [implementation plan's exit-criteria traceability table](../implementation-plan.md), with **every criterion tied to a concrete, named test file** and the phase doc that authored it. The plan flags this mapping as a strong evaluation signal — so it is reproduced here in full and completed.

### 10.1 Full exit-criteria → test traceability

| # | Spec exit criterion ([problem-statement.md](../problem-statement.md)) | How it is proven | Concrete test file(s) | Authored in | Run by CI job |
|---|----------------------------------------------------------------------|------------------|-----------------------|-------------|---------------|
| EC1 | **Deterministic concurrency gates** — no flaky, clock-time-dependent async integration tests; verify offloading via DI overrides + unit assertions | `RecordingOffloader` spy records `fn.__qualname__`/args and runs inline; a thread-identity test proves `ThreadOffloader` leaves the loop thread; retry tests **count** attempts with `base_delay_s=0`; broker drains are `asyncio.Event`-gated | `tests/unit/test_concurrency.py`, `tests/unit/adapters/test_offload_invariant.py`, `tests/unit/test_retry.py`, `tests/unit/broker/test_consumer.py` | [Phase 2](phase-2-concurrency-retry-ports.md), [Phase 4](phase-4-object-store-providers.md), [Phase 5](phase-5-redis-streams-broker.md) | `quality` |
| EC2 | **Zero-cloud isolation** — dev environment auto-redirects all cloud-bound payloads to internal MinIO | Pure-unit test of the `@model_validator(mode="after")` redirect: with `env != prod` and no `endpoint_url`, Settings forces `http://localhost:9000` + path-style; reinforced by the all-fakes worker demo path that completes with zero keys | `tests/unit/test_config.py`; demo path in `tests/unit/test_pipelines.py` / `test_processor.py` | [Phase 1](phase-1-scaffold-toolchain-domain.md), [Phase 7](phase-7-worker-pipelines.md) | `quality` |
| EC3 | **Zero resource leaking** — DB pools, Redis clients, file descriptors open and close cleanly; zero dangling processes | `AppContainer.aclose()` leak test: stub clients expose `closed` flags → after `aclose()` all are closed and the executor is shut down; integration variant asserts `engine.pool.checkedout() == 0` after lifespan exit | `tests/unit/test_container_lifecycle.py`, `tests/integration/test_worker_end_to_end.py` | [Phase 6](phase-6-composition-root-fastapi-api.md), [Phase 7](phase-7-worker-pipelines.md) | `quality` + `integration` |
| EC4 | **`202` ingestion in milliseconds** — API logs initial record, pushes to broker, returns `202` + execution ID without doing pipeline work inline | Route test asserts `POST /v1/jobs` performs exactly one repository insert (status `PENDING`) and one `queue.publish`, returns `202` with `job_id` + `status_url`, and triggers **no** provider/object-store calls | `tests/unit/api/test_jobs.py` | [Phase 6](phase-6-composition-root-fastapi-api.md) | `quality` |
| EC5 | **`to_thread` + backoff at every network boundary** — every external call offloaded and retried with exponential backoff | Parametrized per-adapter-method test asserts each method routes through `offloader.run`; each real adapter wraps the call in `tenacity` retry on `TransientUpstreamError`; retry attempt counts asserted with zero delay | `tests/unit/adapters/test_offload_invariant.py`, `tests/unit/test_retry.py` | [Phase 4](phase-4-object-store-providers.md), [Phase 2](phase-2-concurrency-retry-ports.md) | `quality` |
| EC6 | **Decoupled processing workflow** — worker pulls from Redis, runs adapters sequentially, uploads to S3, updates status to Success/Failed | `JobProcessor` test asserts full lifecycle `PENDING→RUNNING→SUCCESS` (and the `FAILED` path), artifact bytes written to object store, idempotency guard skips terminal jobs; pipeline tests assert port call order | `tests/unit/test_processor.py`, `tests/unit/test_pipelines.py`, `tests/integration/test_worker_end_to_end.py` | [Phase 7](phase-7-worker-pipelines.md) | `quality` + `integration` |
| EC7 | **At-least-once delivery is safe** — redelivered/reclaimed messages do not corrupt state | Consumer test drives `consume_once()` over `fakeredis`, then re-delivers the same entry; idempotency guard re-reads the row and acks-and-skips because it is terminal; transient-failure path re-XADDs `attempt+1`; exhausted/permanent path lands in the DLQ | `tests/unit/broker/test_consumer.py`, `tests/integration/test_broker_redis.py` | [Phase 5](phase-5-redis-streams-broker.md) | `quality` + `integration` |

> [!IMPORTANT]
> EC1–EC3 are the three exit criteria stated verbatim in the [problem statement](../problem-statement.md) §4. EC4–EC7 are derived from the spec's functional requirements (§3) and the implementation plan's expanded traceability rows. All seven are listed so a reviewer can trace **every** engineering claim to a test. The README embeds a 5-row summary ([§6.3.6](#636-exit-criteria--test-mapping-in-the-readme)); this is the full version.

### 10.2 Phase-9-specific verification

| Spec exit criterion | How this phase proves it | Command / artifact |
|---------------------|--------------------------|--------------------|
| All of EC1–EC7 are *continuously* enforced | The CI pipeline runs the fast suite on every push and the integration suite with real infra; both are required status checks | `.github/workflows/ci.yml`; green check on the PR |
| Reproducible from a clean clone | Quickstart's three commands bring the system up with zero keys | `uv sync` → `uv run poe up && uv run poe migrate` → `uv run poe api`/`worker` |
| Local gate mirrors CI exactly | `poe check` runs the same lint/format/type/test steps as the `quality` job | `uv run poe check` (green locally ⇒ green in CI) |

### 10.3 The exact verify commands

```bash
# Reproduce the CI `quality` job locally (no Docker needed):
uv sync --frozen --all-extras --dev
uv run poe check          # lint + fmt-check + typecheck + fast tests

# Reproduce the CI `integration` job locally (needs Docker):
uv run poe up             # start postgres/redis/minio
uv run poe migrate        # alembic upgrade head
uv run poe test-int       # pytest -m integration
uv run poe down-v         # tear down + remove volumes

# Validate the workflow YAML before pushing:
actionlint .github/workflows/ci.yml
```

> [!TIP]
> The clean-clone test is worth doing for real before calling the project done: `git clone`, then run the three quickstart commands on a machine that has never seen the repo. If anything requires a step not in the README, the README is incomplete — fix it. A reviewer *will* do exactly this.

---

## 11. Windows & Cross-Platform Notes

CI runs on `ubuntu-latest`, so the workflow itself sees no Windows quirks. The cross-platform concerns of this phase are about the **developer experience**: the same `poe` tasks and quickstart must work on the Windows dev machine (this repo's primary environment) as on the Linux CI runner.

| Concern | Windows behavior | Mitigation (in this phase / referenced) |
|---------|------------------|------------------------------------------|
| Task runner portability | `make` is absent on Windows by default | **`poethepoet`** tasks run via `uv run poe ...` on every OS — no Makefile, no `.ps1`/bash split ([Phase 1](phase-1-scaffold-toolchain-domain.md)) |
| Line endings in `ci.yml` | Git could check out CRLF, which can break `run:` shell heredocs on Linux | `.gitattributes` sets `* text=auto eol=lf` ([Phase 1](phase-1-scaffold-toolchain-domain.md)); the workflow file is LF |
| `smoke` task | `scripts/smoke.ps1` is a PowerShell script | The `smoke` poe task runs it via `pwsh` (PowerShell Core) — installed on the Windows dev box and **pre-installed on `ubuntu-latest`**, so the one script is cross-platform ([Phase 8](phase-8-containerization-compose.md)) |
| Repo path contains a space (`Study supply`) | Unquoted paths break scripts | Quote paths in any local script; CI checks out to a space-free path so this is a local-only concern ([Phase 8](phase-8-containerization-compose.md)) |
| `uvloop` | Unavailable on Windows | `uvicorn[standard]`'s marker skips uvloop on Windows; active automatically inside the Linux CI/container runtime ([implementation plan, Windows gotchas](../implementation-plan.md)) |
| Docker socket in `poe up` | Requires Docker Desktop running on Windows | README quickstart lists Docker as a prerequisite; `poe up` is identical across OSes |

> [!TIP]
> Run `uv run poe check` on the Windows dev box before pushing. Because the fast suite is infra-free and OS-agnostic (no shell-outs, no path-with-space pitfalls — it is pure Python), a green local `check` on Windows is a near-certain predictor of a green `quality` job on Ubuntu. The only differences CI can surface are line-ending issues (guarded by `.gitattributes`) and a stale `uv.lock` (guarded by `--frozen`).

> [!WARNING]
> If you author any workflow `run:` step using a **multi-line shell block** (like the MinIO readiness loop), keep it as POSIX `sh`/`bash` — that block runs on the Linux runner, not on Windows. Do not reach for PowerShell syntax there. Conversely, do not assume the workflow's `run:` steps work verbatim in a local Windows shell; reproduce them through the `poe` tasks, which *are* cross-platform.

---

## 12. Common Pitfalls & Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `uv sync --frozen` fails: *"lockfile is out of date"* | `pyproject.toml` changed without regenerating `uv.lock` | Run `uv lock`, commit the updated `uv.lock` in the same PR. CI must use the committed lock. |
| `quality` job: `uv run poe ...` → *"poe: command not found"* | Dev dependency group not installed | Ensure `uv sync` includes `--dev` (and `poethepoet` is in the dev group). The workflow uses `--all-extras --dev`. |
| `integration` job hangs at "Initializing containers" then times out | A service's `--health-cmd` never reports healthy (e.g. curl-based health on the MinIO image, which lacks curl) | Give MinIO **no** `--health-cmd`; use the explicit "Wait for MinIO" runner step instead (see [§6.1](#61-githubworkflowsciyml)). |
| `integration` tests: connection refused to Postgres/Redis | Missing `ports:` mapping, or addressing the service by name instead of `localhost` | Map `5432:5432`/`6379:6379` and connect via `localhost` (the job runs on the runner, not in a container). |
| MinIO service won't start: *"ERROR Unable to validate the credentials"* / no `server` subcommand | The `minio/minio` entrypoint needs a `server <dir> --console-address` command that the `services:` short form can't supply | **Fallback:** remove MinIO from `services:` and start it as a step: `docker run -d -p 9000:9000 -p 9001:9001 -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data --console-address ":9001"`, then run the same readiness poll. Identical tests. |
| `mc mb` step fails: *"Unable to initialize new alias"* | `mc` ran before MinIO was live | Order matters: the "Wait for MinIO" step must precede the `mc mb` step (it does in [§6.1](#61-githubworkflowsciyml)). |
| `alembic upgrade head` fails: *"Can't load plugin: sqlalchemy.dialects:postgresql.asyncpg"* or driver errors | Wrong URL scheme | Use `postgresql+asyncpg://...` (async driver), matching the engine config from [Phase 3](phase-3-persistence-sqlalchemy-alembic.md). |
| `pytest -m integration` collects **0 tests** | Marker not registered, or no tests carry `@pytest.mark.integration` | Confirm the `integration` marker is declared in `[tool.pytest.ini_options].markers` ([Phase 1](phase-1-scaffold-toolchain-domain.md)) and that integration tests are decorated. |
| `ruff format --check` fails in CI but `ruff check` passed | Formatting drift — `check` (lint) and `format` are separate | Run `uv run poe fmt` locally to auto-format, commit, re-push. `fmt-check` only verifies; it never rewrites. |
| `mypy` passes locally but fails in CI | Local mypy cache masked an error, or local `mypy` version differs from the locked one | CI uses the locked version via `uv sync --frozen`; reproduce with `uv run poe typecheck`. Clear local cache: `rm -rf .mypy_cache`. |
| CI badge in README shows "no status" | Workflow has never run on the default branch, or `<owner>/<repo>` placeholder not replaced | Push to `main` once; replace the placeholder with the real repo path. |
| `setup-uv` cache never hits | `cache-dependency-glob` doesn't match a committed file, or `uv.lock` not committed | Commit `uv.lock`; keep `cache-dependency-glob: "uv.lock"`. The default glob also matches `pyproject.toml`/lock files. |
| Two CI runs queue on rapid pushes, wasting minutes | No concurrency control | The `concurrency:` block with `cancel-in-progress: true` cancels superseded runs (see [§6.1](#61-githubworkflowsciyml)). |
| Integration job intermittently fails on a fresh DB | A test assumes a table/row that a migration creates, but `alembic upgrade head` ran after the failing test, or not at all | Ensure the `Run database migrations` step precedes `Pytest (integration)` (it does in [§6.1](#61-githubworkflowsciyml)); integration tests assume `head` schema. |

> [!CAUTION]
> **Never disable a failing check to make the pipeline green.** If `mypy` or a test fails in CI, fix the code or the test — do not add `# type: ignore` blanket suppressions, `continue-on-error: true`, or `-m "not integration and not flaky"` escape hatches. The value of this project to a reviewer is that the gates are *real*. A green badge over silenced checks is worse than a red one.

---

## 13. Definition of Done

The project is complete when every box below is ticked.

**CI workflow**

- [ ] `.github/workflows/ci.yml` exists with two jobs: `quality` and `integration`.
- [ ] `quality` runs with **zero** service containers and is green using only `uv sync` + fakes/fakeredis/aiosqlite.
- [ ] `quality` steps: checkout → `setup-uv` (cached) → `uv sync --frozen` → `poe lint` → `poe fmt-check` → `poe typecheck` → `poe test` (with `--cov`) → upload coverage artifact.
- [ ] `integration` declares postgres + redis + minio `services:`; postgres/redis have health `options:`; MinIO has an explicit readiness step (no curl-based in-image health-cmd).
- [ ] `integration` runs `alembic upgrade head` **before** `pytest -m integration`.
- [ ] Both jobs use `localhost` + mapped ports to reach services (job runs on the runner).
- [ ] CI calls **only** `uv run poe <task>` for its real work — no inline re-spelling of `ruff`/`mypy`/`pytest`.
- [ ] All action versions are current and pinned (`actions/checkout@v6`, `astral-sh/setup-uv@v8`, `actions/upload-artifact@v4`); no invented inputs.
- [ ] `concurrency:` cancels superseded runs; `permissions: contents: read`.
- [ ] `actionlint .github/workflows/ci.yml` passes locally.
- [ ] Both jobs are configured as **required status checks** on the `main` branch.

**README**

- [ ] Title, CI badge, and a one-paragraph pitch.
- [ ] Mermaid architecture diagram of the five layers.
- [ ] Port ↔ adapter table (real adapter + fake for each port).
- [ ] Three-command quickstart (`uv sync`; `poe up` + `poe migrate`; `poe api`/`poe worker`) that works on a clean clone with **zero** keys.
- [ ] "Design decisions" narrative covering the offloader spy, Redis Streams vs Celery, and message-as-pointer.
- [ ] Exit-criteria → test-file mapping table (summary), with exact, clickable test paths.
- [ ] Links into the relevant phase docs.

**Poe task catalog**

- [ ] All 12 core tasks present: `fmt`, `lint`, `typecheck`, `test`, `test-int`, `check`, `api`, `worker`, `up`, `down`, `migrate`, `smoke` (plus the `fmt-check`/`lint-fix`/`test-all`/`down-v` helpers).
- [ ] `check` is the local mirror of the CI `quality` job.
- [ ] Every task has a `help` string; `uv run poe --help` lists them all.

**End-to-end**

- [ ] CI is green on a real PR to `main`.
- [ ] A genuine clean-clone run of the quickstart succeeds with no extra steps.
- [ ] `uv run poe check` is green locally on the Windows dev machine.

---

## 14. References & Further Reading

**GitHub Actions — core**
- [Workflow syntax for GitHub Actions](https://docs.github.com/en/actions/using-workflows/workflow-syntax-for-github-actions) — authoritative reference for `on`, `jobs`, `services`, `permissions`, `concurrency`, `env`.
- [About service containers](https://docs.github.com/en/actions/using-containerized-services/about-service-containers) — how `services:` work and how to reach them (`localhost:<port>`) from a runner-hosted job.
- [Creating PostgreSQL service containers](https://docs.github.com/en/actions/using-containerized-services/creating-postgresql-service-containers) — canonical `pg_isready` health-check `options:` example.
- [Creating Redis service containers](https://docs.github.com/en/actions/using-containerized-services/creating-redis-service-containers) — canonical `redis-cli ping` health-check example.
- [`actions/checkout`](https://github.com/actions/checkout) — repository checkout action (v6).
- [`actions/upload-artifact`](https://github.com/actions/upload-artifact) — build-artifact upload (v4).
- [Controlling concurrency](https://docs.github.com/en/actions/using-jobs/using-concurrency) — `concurrency` group + `cancel-in-progress`.
- [Workflow status badges](https://docs.github.com/en/actions/monitoring-and-troubleshooting-workflows/adding-a-workflow-status-badge) — the `…/badge.svg` README badge.

**uv & toolchain**
- [`astral-sh/setup-uv`](https://github.com/astral-sh/setup-uv) — the uv install action; `enable-cache`, `cache-dependency-glob`, `python-version`, `version`, `prune-cache` inputs.
- [uv documentation](https://docs.astral.sh/uv/) — `uv sync`, `uv lock`, `--frozen`, dependency groups, extras.
- [uv in GitHub Actions guide](https://docs.astral.sh/uv/guides/integration/github/) — official patterns for caching and `uv sync` in CI.
- [Ruff documentation](https://docs.astral.sh/ruff/) — `ruff check`, `ruff format`, `--check`.
- [mypy documentation](https://mypy.readthedocs.io/) — `--strict` type checking.
- [pytest](https://docs.pytest.org/) · [pytest-asyncio](https://pytest-asyncio.readthedocs.io/) (`asyncio_mode`) · [pytest-cov](https://pytest-cov.readthedocs.io/) (`--cov`).
- [poethepoet](https://poethepoet.natn.io/) — task runner in `pyproject.toml`; `cmd`/`sequence`/`script`/`shell` task forms.

**Infrastructure images**
- [MinIO — health-check probes](https://min.io/docs/minio/linux/operations/monitoring/healthcheck-probe.html) — `/minio/health/live` and `/minio/health/ready` endpoints (port 9000).
- [MinIO Client (`mc`)](https://min.io/docs/minio/linux/reference/minio-mc.html) — `mc alias set`, `mc mb` for bucket provisioning.
- [PostgreSQL Docker image](https://hub.docker.com/_/postgres) · [Redis Docker image](https://hub.docker.com/_/redis) — env vars and `pg_isready`/`redis-cli` health commands.

**Validation tooling**
- [`actionlint`](https://github.com/rhysd/actionlint) — static linter for GitHub Actions workflows.
- [`act`](https://github.com/nektos/act) — run GitHub Actions workflows locally in Docker.

> [!NOTE]
> All workflow YAML in this document was grounded against the GitHub Actions service-container docs and the `astral-sh/setup-uv` action README as of the project date (2026). Action **major** versions are pinned (`@v6`, `@v8`, `@v4`); pin to the latest patch tag at implementation time and let Dependabot bump them. No action input used here is invented — each appears in the linked official documentation.

---

## 15. Navigation

| ← Previous | Index | Next → |
|-----------|-------|--------|
| [Phase 8 — Containerization & Compose](phase-8-containerization-compose.md) | [All phases](README.md) | — (project complete) |

**Cross-links used in this phase:**
[Phase 1 — Scaffold, Toolchain, Domain](phase-1-scaffold-toolchain-domain.md) ·
[Phase 2 — Concurrency, Retry, Ports](phase-2-concurrency-retry-ports.md) ·
[Phase 3 — Persistence](phase-3-persistence-sqlalchemy-alembic.md) ·
[Phase 4 — Object Store & Providers](phase-4-object-store-providers.md) ·
[Phase 5 — Redis Streams Broker](phase-5-redis-streams-broker.md) ·
[Phase 6 — Composition Root & API](phase-6-composition-root-fastapi-api.md) ·
[Phase 7 — Worker & Pipelines](phase-7-worker-pipelines.md) ·
[Phase 8 — Containerization & Compose](phase-8-containerization-compose.md) ·
[Implementation Plan](../implementation-plan.md) ·
[Problem Statement](../problem-statement.md)
