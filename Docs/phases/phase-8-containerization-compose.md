# Phase 8 — Containerization & Full-Stack Compose

> **Part of:** [Asynchronous AI Serving Engine](../implementation-plan.md) · [Problem Statement](../problem-statement.md)
> **Status:** Planned (greenfield) · **Depends on:** [Phases 1–7](#3-prerequisites--inputs) · **Unlocks:** [Phase 9 — CI, README, Polish](phase-9-ci-readme-polish.md)
> **Delivers:** A single multi-stage container image that serves both the API and the worker, a one-command full-stack `docker compose` graph (infra → migrate → api/worker) that comes up healthy with zero cloud credentials, and a manual PowerShell smoke script that proves an end-to-end `rag_query` round-trip — with `docker compose down` leaving zero dangling resources.
> **Primary skills applied:** docker-expert, deployment-pipeline-design, deployment-procedures, secrets-management, powershell-windows, docs-architect, mermaid-expert

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

By the end of [Phase 7](phase-7-worker-pipelines.md) the engine is *functionally complete*: the FastAPI app accepts `rag_query` / `embed_document` jobs and returns `202 Accepted` in milliseconds; the custom Redis Streams worker drains jobs, runs the provider pipelines through the `SyncOffloader` boundary, and writes results to the object store; PostgreSQL is the single source of truth for status; and every external boundary is retry-wrapped. All of that runs today only when you start each piece *by hand* — `uv run poe api` in one terminal, `uv run poe worker` in another, and `docker compose up` for the infra-only compose file authored in [Phase 3](phase-3-persistence-sqlalchemy-alembic.md).

**Phase 8 turns that hand-assembled developer setup into a single reproducible artifact.** It does three things:

1. **Builds one container image** (a multi-stage `uv` build) that contains the application and its locked dependencies, runs as a non-root user, and is small and cache-friendly. Crucially, the *same image* serves the API and the worker — the process is chosen by the container's command, not by building two images.
2. **Wires the full-stack `docker compose` graph** so that `docker compose up --build -d` brings up infrastructure (Postgres, Redis, MinIO + a one-shot bucket initializer), then runs database migrations exactly once via a one-shot `migrate` service, and only then starts the long-running `api` and `worker` services. The dependency conditions (`service_healthy`, `service_completed_successfully`) make the ordering deterministic — no `sleep`-and-pray entrypoints.
3. **Provides a manual smoke demo** (`scripts/smoke.ps1`) that POSTs a `rag_query` to the running API with an `X-API-Key`, captures the returned job id, and polls `GET /v1/jobs/{id}` until the job reaches a terminal state (`SUCCESS`/`FAILED`) or a timeout fires. This is a *manual* demo — it deliberately lives outside the `pytest` suite so the automated tests stay clock-free (a hard exit criterion from the spec).

> [!IMPORTANT]
> Phase 8 changes **zero application logic**. Every Python module authored in Phases 1–7 ships unchanged. This phase is purely *packaging and orchestration*: a `Dockerfile`, a `.dockerignore`, the full `docker-compose.yml`, the final `.env.example`, and `scripts/smoke.ps1`. If you find yourself editing `src/app/**` to make the container work, stop — the architecture (composition root shared by API and worker, ports/adapters, offloader discipline) was specifically designed so that containerization is a no-code-change step.

### Concrete objectives

| # | Objective | Done when |
|---|-----------|-----------|
| O1 | A multi-stage `Dockerfile` builds dependencies in a `ghcr.io/astral-sh/uv` builder, copies the resulting venv into a slim `python:3.12-slim` runtime, and runs as a non-root user. | `docker build` produces an image; `docker run ... id -u` prints a non-zero UID. |
| O2 | The same image serves both processes; the command is overridden per compose service. | `api` runs `uvicorn --factory`, `worker` runs `python -m app.worker`, from one `image:` reference. |
| O3 | `docker compose up --build -d` reaches a state where infra is `healthy`, `migrate` has `exited (0)`, and `api`/`worker` are `running`/`healthy`. | `docker compose ps` shows the expected state for every service. |
| O4 | Zero-cloud isolation holds inside the container network: the object-store endpoint resolves to `http://minio:9000`, **not** `localhost:9000`. | `AIE_OBJECT_STORE__ENDPOINT_URL=http://minio:9000` is set for `api`/`worker`/`migrate`; a `rag_query` writes an artifact visible in the MinIO console. |
| O5 | `scripts/smoke.ps1` performs a full POST→poll→terminal round-trip on Windows PowerShell 5.1 and `pwsh`. | Running the script prints `SUCCESS` and exits `0`. |
| O6 | `docker compose down` leaves no dangling containers, networks, or (optionally) volumes — the spec's *zero resource leak* exit criterion at the orchestration layer. | `docker compose ps -a` is empty; `docker network ls` shows no project network. |

---

## 2. Where This Fits

Phase 8 is the *operational shell* that wraps the whole engine. The diagram below shows the full five-layer system; the **bold** nodes are what Phase 8 introduces or formalizes (the container image, the orchestration graph, and the migrate gate). Everything else was built in earlier phases and is merely *placed into containers* here.

```mermaid
flowchart TD
    subgraph build["Phase 8: Build artifact"]
        direction LR
        IMG["<b>App image</b><br/>multi-stage uv build<br/>(serves api AND worker)"]:::phase8
    end

    subgraph orch["Phase 8: docker compose graph"]
        direction TB
        MIG["<b>migrate</b> (one-shot)<br/>alembic upgrade head"]:::phase8
        API["api service<br/>uvicorn --factory"]:::run
        WRK["worker service<br/>python -m app.worker"]:::run
    end

    subgraph infra["Infra layer (images, not built)"]
        direction LR
        PG[("postgres:16-alpine")]:::infra
        RED[("redis:7-alpine")]:::infra
        MIN[("minio/minio")]:::infra
        MC["minio/mc<br/>bucket-init (one-shot)"]:::infra
    end

    subgraph app["Application layers (Phases 1-7, unchanged)"]
        direction TB
        CR["AppContainer<br/>composition root"]:::app
        SVC["services: ingestion,<br/>processor, pipelines"]:::app
        ADP["adapters: persistence,<br/>broker, object_store, providers"]:::app
    end

    IMG -. "becomes" .-> API
    IMG -. "becomes" .-> WRK
    IMG -. "becomes" .-> MIG

    PG -- "service_healthy" --> MIG
    MIG -- "service_completed_successfully" --> API
    MIG -- "service_completed_successfully" --> WRK
    PG -- "service_healthy" --> API
    RED -- "service_healthy" --> API
    MIN -- "service_healthy" --> API
    PG -- "service_healthy" --> WRK
    RED -- "service_healthy" --> WRK
    MIN -- "service_healthy" --> WRK
    MC -- "service_completed_successfully" --> API
    MC -- "service_completed_successfully" --> WRK

    API --> CR
    WRK --> CR
    CR --> SVC --> ADP
    ADP --> PG
    ADP --> RED
    ADP --> MIN

    classDef phase8 fill:#1f6feb,stroke:#0b3d91,color:#fff,stroke-width:2px;
    classDef run fill:#2da44e,stroke:#116329,color:#fff;
    classDef infra fill:#6e7781,stroke:#424a53,color:#fff;
    classDef app fill:#bf8700,stroke:#7d4e00,color:#fff;
```

**How it connects backward:** The `migrate` service runs the *exact* Alembic configuration authored in [Phase 3](phase-3-persistence-sqlalchemy-alembic.md). The `api` service runs the `create_app()` factory and lifespan from [Phase 6](phase-6-composition-root-fastapi-api.md). The `worker` service runs the `python -m app.worker` entrypoint from [Phase 7](phase-7-worker-pipelines.md). All three import the same `AppContainer` composition root ([Phase 6](phase-6-composition-root-fastapi-api.md)). The fake providers ([Phase 4](phase-4-object-store-providers.md)) are the default, so the stack runs with **zero API keys**.

**How it connects forward:** [Phase 9](phase-9-ci-readme-polish.md) consumes this image and compose graph: the CI *integration* job spins up service containers that mirror this compose topology, and the README's three-command quickstart is literally `cp .env.example .env` → `docker compose up --build -d` → `pwsh scripts/smoke.ps1`. Phase 8 must therefore produce an image and a compose file that a *clean clone* can run with no extra steps.

---

## 3. Prerequisites & Inputs

This phase assembles artifacts from every prior phase. Nothing new is invented in `src/`; the table below is the contract Phase 8 relies on.

| Input | Produced in | Why Phase 8 needs it |
|-------|-------------|----------------------|
| `pyproject.toml` + `uv.lock` | [Phase 1](phase-1-scaffold-toolchain-domain.md) | The Dockerfile's dependency layer is keyed on these two files; `uv.lock` **must** be committed for `uv sync --frozen` to work. |
| `Settings` with `AIE_` env prefix, nested groups, and the zero-cloud `model_validator` | [Phase 1](phase-1-scaffold-toolchain-domain.md) | Every container is configured purely through `AIE_*` env vars; the validator is what redirects S3 → MinIO. |
| `AppContainer.create(settings)` / `aclose()` | [Phase 6](phase-6-composition-root-fastapi-api.md) | Shared composition root that both `api` and `worker` instantiate; its clean `aclose()` is the in-process half of the zero-leak criterion. |
| `create_app()` factory + `/health` & `/health/ready` routes | [Phase 6](phase-6-composition-root-fastapi-api.md) | `uvicorn --factory` target and the container `HEALTHCHECK` endpoint. |
| `python -m app.worker` entrypoint with Windows/Linux signal handling | [Phase 7](phase-7-worker-pipelines.md) | The worker service command; Linux SIGTERM path is what `stop_grace_period` relies on. |
| `POST /v1/jobs` (202 + `job_id`) and `GET /v1/jobs/{id}` | [Phase 6](phase-6-composition-root-fastapi-api.md) | The two endpoints `smoke.ps1` drives. |
| Alembic async env reading `Settings().database_url` | [Phase 3](phase-3-persistence-sqlalchemy-alembic.md) | The `migrate` service runs `alembic upgrade head` against this. |
| Infra-only `docker-compose.yml` (postgres/redis/minio + mc) with healthchecks & named volumes | [Phase 3](phase-3-persistence-sqlalchemy-alembic.md) | Phase 8 *extends* this file with `migrate`, `api`, `worker`; the infra services and healthchecks are reused. |
| Fake providers (zero keys) as the default | [Phase 4](phase-4-object-store-providers.md) | Lets the whole stack run with no cloud credentials — the demo path. |

> [!NOTE]
> **Tooling assumed on the host:** Docker Engine 24+ (or Docker Desktop) with **BuildKit** enabled (default since Docker 23) and **Compose v2** (`docker compose`, the plugin — *not* the legacy `docker-compose` v1 binary). Compose v2 is required for the `depends_on` long-form conditions used below. On Windows, Docker Desktop with the WSL 2 backend is the expected environment. Verify with `docker compose version` (must print `v2.x`).

> [!IMPORTANT]
> `uv.lock` is a **required, committed** input. `uv sync --frozen` *fails* (it does not silently regenerate) if the lock file is missing or out of date relative to `pyproject.toml`. That failure-closed behavior is desirable: it guarantees the image's dependency set is exactly what was resolved and reviewed, which is the whole point of a lock file in a reproducible build. If you ever see `error: Failed to parse ... uv.lock` or a "lockfile out of date" error during `docker build`, run `uv lock` on the host and commit the result *before* rebuilding.

---

## 4. Deliverables

| File | Type | Purpose |
|------|------|---------|
| `docker/Dockerfile` | new | Multi-stage `uv` build: `ghcr.io/astral-sh/uv:python3.12-bookworm-slim` builder → `python:3.12-slim` runtime, non-root, one image for api + worker. |
| `.dockerignore` | new | Keeps the build context tiny and deterministic (no `.venv`, `.git`, caches, tests, secrets). |
| `docker-compose.yml` | changed | Extends the Phase 3 infra-only file into the full stack: adds `migrate` (one-shot), `api`, `worker`; wires `depends_on` conditions and in-network env overrides. |
| `.env.example` | changed | Final, complete `AIE_*` reference with safe local defaults and commented-out real-provider keys. |
| `scripts/smoke.ps1` | new | Manual PowerShell demo: POST a `rag_query`, poll to terminal state with a timeout. **Not** part of the `pytest` suite. |

> [!TIP]
> Put the `Dockerfile` under `docker/` (not the repo root) to keep the root clean, but **keep the build context at the repo root** so the build can see `pyproject.toml`, `uv.lock`, and `src/`. The compose `build:` block makes this explicit with `context: .` and `dockerfile: docker/Dockerfile`. When building by hand, the equivalent is `docker build -f docker/Dockerfile .` (note the trailing `.` — the context is the current directory, not `docker/`).

---

## 5. Design Decisions & Rationale

| Decision | Choice | Why | Rejected alternative |
|----------|--------|-----|----------------------|
| Image build tool | `uv` multi-stage (`ghcr.io/astral-sh/uv` builder → `python:3.12-slim` runtime) | Matches the Phase 1 toolchain (`uv` everywhere); `uv sync --frozen` reproduces the exact locked env; cache mounts make rebuilds fast. | `pip install -r requirements.txt` (no lockfile fidelity, slower); Poetry-in-Docker (extra tool, slower resolver). |
| One image vs two | **One** image, command overridden per service | The composition root is shared by API and worker by design; two images would duplicate ~250 MB of identical layers and double the build/scan/push surface. | Separate `api`/`worker` Dockerfiles (DRY violation; drift risk). |
| Runtime base | `python:3.12-slim` (Debian bookworm slim) | Small, glibc-based (asyncpg/boto3 wheels "just work" — no Alpine musl wheel-compilation pain), official, well-patched. | `alpine` (musl breaks many manylinux wheels, forces source builds); full `python:3.12` (~1 GB, unnecessary). |
| uv in the runtime image? | **No** — copy only the venv | `uv` is a *build* tool; the runtime needs only the resolved `/app/.venv`. Dropping it shrinks the image and the attack surface. | Keeping `uv` in runtime (larger image, more CVEs to track). |
| Process user | Dedicated **non-root** user (`appuser`, UID 1001) | Container security baseline; a compromised process cannot write outside its lane or escalate trivially. | Running as root (default) — fails security review. |
| Migrations | A dedicated **one-shot `migrate` service** that runs `alembic upgrade head`, gated `service_completed_successfully` | Migrations must run *exactly once*, *before* app traffic, and the app must *wait* for them. A one-shot service with a completion condition expresses this precisely and idempotently. | Running migrations in the app's entrypoint (race when scaling `api` to N replicas; couples app start to schema change). |
| Startup ordering | `depends_on` long-form **conditions** (`service_healthy`, `service_completed_successfully`) | Compose v2 gates start-up on real readiness (healthchecks) and completion, removing `sleep`/retry-in-entrypoint hacks. | `depends_on` short form (only waits for *start*, not *ready*); wait-for-it scripts (more moving parts). |
| Object-store endpoint in-network | `AIE_OBJECT_STORE__ENDPOINT_URL=http://minio:9000` env override | Inside the compose network, `localhost` is the *container itself*; the MinIO service is reachable by its **service name** `minio`. This is the single most common containerization bug for this stack — see the callout below. | Relying on the Phase 1 default `http://localhost:9000` (works on host, **breaks** inside containers). |
| Worker shutdown | `stop_grace_period: 30s` on the `worker` service | The worker drains in-flight jobs on SIGTERM (Phase 7); 30 s gives it time to finish before SIGKILL, preventing mid-job kills and orphaned stream messages. | Default 10 s grace (can SIGKILL a job mid-pipeline → relies on at-least-once redelivery, noisier). |
| Healthcheck transport | `python -c "... urllib.request ..."` for the API; native CLIs (`pg_isready`, `redis-cli`, `mc ready`) for infra | The slim image has Python but **not** `curl`; using the interpreter avoids installing extra packages just for a healthcheck. | Installing `curl`/`wget` into the runtime image solely for healthchecks (bloat, more CVEs). |
| Secrets in compose | `.env` file + `${VAR}` interpolation; **no secrets baked into the image** | Twelve-factor config; the image is environment-agnostic and safe to share; real provider keys are injected at run time, never at build time. | `ENV AIE_API_KEYS=...` in the Dockerfile (secret leaks into image layers — irreversible once pushed). |

> [!CAUTION]
> **The localhost-vs-service-name trap is the defining gotcha of this phase.** The Phase 1 `Settings` validator enforces zero-cloud isolation by redirecting the object store to `http://localhost:9000` when `env != prod` and no `endpoint_url` is set. That default is correct *on the host* (where you run `uv run poe api`). **Inside a container it is wrong:** `localhost` resolves to the container's own loopback, so the API/worker would try to reach MinIO inside themselves and fail with `Connection refused`. The fix is an **explicit `AIE_` env override** in compose that sets the endpoint to the MinIO *service name*: `AIE_OBJECT_STORE__ENDPOINT_URL=http://minio:9000`. This override is set on `api`, `worker`, **and** `migrate` (if any migration touches the bucket — it doesn't here, but the var is harmless and keeps env blocks uniform). Section 8 explains exactly how this interacts with the validator.

### Why the migrate gate is a *separate* service, in depth

There are three places one *could* run `alembic upgrade head` in a container stack, and only one is correct for a multi-replica app:

```mermaid
flowchart LR
    A["Option A:<br/>in API entrypoint<br/>before uvicorn"]:::bad
    B["Option B:<br/>in image build<br/>(RUN alembic ...)"]:::worse
    C["Option C:<br/>dedicated one-shot<br/>migrate service"]:::good

    A --- An["Race when api<br/>scales to N replicas;<br/>every replica migrates"]
    B --- Bn["Impossible: DB<br/>isn't running at<br/>build time"]
    C --- Cn["Runs once; api/worker<br/>wait via<br/>service_completed_successfully"]

    classDef bad fill:#bf8700,stroke:#7d4e00,color:#fff;
    classDef worse fill:#cf222e,stroke:#82071e,color:#fff;
    classDef good fill:#2da44e,stroke:#116329,color:#fff;
```

- **Option A (entrypoint migration)** seems convenient but breaks the moment you `docker compose up --scale api=3`: three replicas race to migrate the same database. Alembic takes a lock, so it usually *works*, but it couples every app boot to a schema operation and produces confusing startup logs.
- **Option B (build-time migration)** is simply impossible — the database does not exist during `docker build`. Migrations are a *runtime* concern.
- **Option C (one-shot service)** is the orchestration-native answer: the `migrate` service runs `alembic upgrade head` to completion and *exits 0*; `api` and `worker` declare `depends_on: { migrate: { condition: service_completed_successfully } }`, so Compose blocks their start until the migration container has succeeded. On the next `up`, Alembic is a no-op (already at head) and the gate is satisfied near-instantly. This is idempotent, replica-safe, and reads like documentation.

> [!NOTE]
> `service_completed_successfully` requires the depended-on container to exit with **code 0**. If a migration fails (non-zero exit), Compose will *not* start `api`/`worker`, and `docker compose up` reports the dependency failure. That fail-closed behavior is exactly what you want: a broken migration must never let a stale-schema app accept traffic.

---

## 6. Detailed Implementation

This section gives complete, runnable files with line-level annotations.

### 6.1 `docker/Dockerfile`

**Purpose & responsibilities.** Produce a small, reproducible, non-root image that contains the application and its *locked, production-only* dependencies, suitable for running either the API or the worker. The build is split into a **builder** stage (has `uv`, resolves and installs the venv, compiles bytecode) and a **runtime** stage (`python:3.12-slim`, gets only the finished venv and the source — no `uv`, no build tools).

```dockerfile
# syntax=docker/dockerfile:1.7
# ^ Enables BuildKit frontend features: heredocs, --mount=type=cache/bind, etc.
#   Always pin the dockerfile frontend so cache mounts behave identically everywhere.

# =============================================================================
# Stage 1 — builder: resolve + install the locked environment with uv
# =============================================================================
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder
# ^ Official Astral image: Debian bookworm slim + Python 3.12 + the `uv` binary
#   preinstalled on PATH. (uv also publishes `*-trixie-slim` (Debian 13) tags;
#   pin whichever you standardize on. bookworm is chosen here for parity with the
#   python:3.12-slim runtime, which is bookworm-based.)

# --- uv behavior knobs (build-time only) ------------------------------------
ENV UV_COMPILE_BYTECODE=1 \
    # ^ Precompile .py -> .pyc during install. Trades a little build time for
    #   faster cold-start (no first-request compilation in the runtime container).
    UV_LINK_MODE=copy \
    # ^ COPY packages into the venv instead of hardlinking from the cache mount.
    #   Required: the cache lives on a BuildKit cache mount (a different filesystem),
    #   so hardlinks would fail. Silences uv's "hardlink failed, falling back" warning.
    UV_PYTHON_DOWNLOADS=0
    # ^ Never let uv download a managed Python; use the interpreter already in the
    #   base image. Keeps the build hermetic and the Python version pinned to the base.

WORKDIR /app

# --- Layer 1: dependencies ONLY (the big, slow, rarely-changing layer) -------
# Bind-mount the lock + manifest (don't COPY them — they aren't needed after this
# step and binding avoids busting the layer on unrelated file changes). Cache the
# uv download/build cache across builds. `--no-install-project` installs deps but
# NOT our own package, so editing src/ does not invalidate this layer.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-dev --no-install-project
    # ^ --frozen : use uv.lock exactly; never re-resolve or update it (fail if absent).
    #   --no-dev  : exclude the [dependency-groups] dev deps (pytest, ruff, mypy, ...).
    #   --no-install-project : install dependencies but not `app` itself (next layer).

# --- Layer 2: the project source + install our package -----------------------
# Now copy the actual source. This invalidates only the cheap final layer.
COPY pyproject.toml uv.lock ./
COPY src ./src
COPY migrations ./migrations
COPY alembic.ini ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev
    # ^ Second sync (no --no-install-project) installs `app` into /app/.venv.
    #   Fast: deps are already present from Layer 1; only our package is added.

# =============================================================================
# Stage 2 — runtime: slim, non-root, venv-only (no uv, no build tooling)
# =============================================================================
FROM python:3.12-slim AS runtime
# ^ Debian bookworm slim, glibc — manylinux wheels (asyncpg, boto3, etc.) install
#   cleanly. No uv here: a runtime image should not carry its build tools.

# --- Runtime environment -----------------------------------------------------
ENV PYTHONUNBUFFERED=1 \
    # ^ Don't buffer stdout/stderr -> logs appear immediately in `docker logs`.
    PYTHONDONTWRITEBYTECODE=1 \
    # ^ Don't write .pyc at runtime (bytecode was already compiled in the builder).
    PATH="/app/.venv/bin:$PATH"
    # ^ Put the venv's bin first so `python`, `uvicorn`, `alembic` resolve to the
    #   venv WITHOUT needing `uv run` or an explicit activate. This is how the
    #   runtime stage "activates" the environment.

# --- Create an unprivileged user (fixed UID/GID for predictable file perms) --
RUN groupadd --system --gid 1001 appgroup \
 && useradd --system --uid 1001 --gid appgroup --no-create-home --shell /usr/sbin/nologin appuser
 # ^ --system: no aging/expiry; --no-create-home: no /home needed; nologin shell:
 #   the account can't be used for interactive login. UID/GID 1001 is conventional
 #   for the first app user and avoids colliding with host UID 1000 on bind mounts.

WORKDIR /app

# --- Copy the finished venv and the application from the builder -------------
# Chown to appuser so the running process owns its files (and could, e.g., write
# .pyc if it ever needed to — it won't, given PYTHONDONTWRITEBYTECODE).
COPY --from=builder --chown=appuser:appgroup /app/.venv /app/.venv
COPY --from=builder --chown=appuser:appgroup /app/src /app/src
COPY --from=builder --chown=appuser:appgroup /app/migrations /app/migrations
COPY --from=builder --chown=appuser:appgroup /app/alembic.ini /app/alembic.ini

# --- Drop privileges ---------------------------------------------------------
USER appuser
# ^ Everything below this line runs as UID 1001. Verifiable: `docker run --rm
#   <image> id -u` prints 1001.

# --- Document the API port (informational; publish via compose `ports:`) -----
EXPOSE 8000

# --- Default command = API. Worker overrides this in compose. ----------------
# Use exec form (JSON array) so the process is PID 1 and receives SIGTERM directly
# (no shell wrapper to swallow signals) -> graceful shutdown actually works.
CMD ["uvicorn", "app.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
# ^ --factory: call create_app() to build the ASGI app (Phase 6). --host 0.0.0.0:
#   bind all interfaces so the port is reachable from outside the container.
#   The worker service overrides this with: ["python", "-m", "app.worker"].
```

#### Walkthrough of the non-obvious parts

- **`# syntax=docker/dockerfile:1.7`** — pins the BuildKit Dockerfile frontend. Without it, older Docker daemons may not understand `--mount=type=cache`/`type=bind`. Pinning makes the cache-mount semantics reproducible across machines and CI.
- **Two-layer dependency install** — the single most impactful caching technique here. Layer 1 (`--no-install-project`) installs *only* third-party dependencies and is keyed on `uv.lock` + `pyproject.toml`. Because `src/` is not present yet, editing application code *cannot* invalidate this layer; the slow dependency resolution/installation is cached. Layer 2 copies `src/` and installs just our package — fast. This is the canonical uv pattern from the official Docker integration guide.
- **`--mount=type=bind` vs `COPY` for the lock files in Layer 1** — binding the lock/manifest into the build step (rather than `COPY`ing them into the image) means those files do not become a persisted layer at that point, and unrelated changes elsewhere don't bust the cache key. They *are* `COPY`ed in Layer 2 so the installed project metadata is consistent, but by then we're in the cheap layer.
- **`--mount=type=cache,target=/root/.cache/uv`** — a *build cache* shared across builds on the same machine/runner. It is **not** baked into the image (it lives in BuildKit's cache, separate from layers), so it shrinks rebuild time without bloating the final image. With `UV_LINK_MODE=copy`, uv copies out of this cache into the venv.
- **`--frozen --no-dev`** — `--frozen` says "trust `uv.lock` exactly; do not update it"; `--no-dev` excludes development dependency groups (`pytest`, `ruff`, `mypy`, `httpx`, etc.). The result is a *production* environment with no test/lint tooling — smaller and lower-CVE.

> [!NOTE]
> `--frozen` and `--locked` both use the lockfile without re-resolving, with one nuance: `--locked` additionally *asserts the lockfile is up to date* and errors if it isn't, whereas `--frozen` uses the lockfile as-is without that freshness check. For an image build either is fine; `--frozen` is specified by the locked architecture for this project, so the Dockerfile uses it consistently. If you prefer the stricter freshness assertion in CI, swap to `--locked` and keep it identical in both `uv sync` lines.

- **Copying `migrations/` + `alembic.ini` into runtime** — the *same image* is reused by the `migrate` service to run `alembic upgrade head`, so the runtime stage must contain the Alembic config and the migration scripts. (They're tiny.)
- **`USER appuser` placement** — privileges are dropped *after* all `COPY` operations so the copies can set ownership; the running process never starts as root. The `--chown` on each `COPY --from=builder` ensures the venv and source are owned by `appuser`, avoiding permission errors at runtime.
- **Exec-form `CMD`** — JSON-array form makes the launched process PID 1, so Docker's SIGTERM (sent on `docker stop` / compose shutdown) reaches `uvicorn`/`python -m app.worker` directly. Shell-form (`CMD uvicorn ...`) would wrap it in `/bin/sh -c`, which does **not** forward SIGTERM by default — breaking the worker's graceful drain.

> [!WARNING]
> **PID 1 and zombie reaping.** A Python process as PID 1 does not reap orphaned child processes the way `init` does, and it ignores default signal dispositions. For this app it's fine — `uvicorn` and the worker install their own signal handlers (Phase 6/7) and don't spawn unmanaged children. If you later add a process that forks (rare here), add `init: true` to that compose service (or `docker run --init`) to inject a minimal `tini`-style reaper. Do **not** prematurely add `tini` to the image; it's unnecessary given the current process model.

#### Design rationale & how it honors the locked architecture

- **One image, two commands** directly reflects the locked decision that the `AppContainer` composition root is *shared verbatim* by API and worker. The image carries the whole `app` package; which entrypoint runs is a compose concern, not a build concern.
- **No `uv` in runtime / venv-on-PATH** honors "core never imports FastAPI / no global singletons" indirectly: the runtime image is just "Python + the installed package + env vars," and *all* configuration arrives via `AIE_*` env (read by `Settings`). The image has no environment-specific state baked in.
- **Non-root + slim base** satisfies the security posture expected of a production-grade portfolio piece without compromising wheel compatibility (glibc base).

### 6.2 `.dockerignore`

**Purpose & responsibilities.** Shrink and stabilize the build context. Everything here is *excluded* from what Docker sends to the daemon. A lean context speeds uploads, prevents accidental secret leakage (`.env`), and stops local artifacts (`.venv`, `__pycache__`, test caches) from invalidating layers or bloating the image.

```dockerignore
# --- Version control & CI ---------------------------------------------------
.git
.github
.gitignore
.gitattributes

# --- Local virtual environments & Python caches -----------------------------
.venv
venv/
__pycache__/
*.py[cod]
*.egg-info/
.pytest_cache/
.mypy_cache/
.ruff_cache/
.coverage
htmlcov/

# --- Secrets & local env (NEVER ship these into an image layer) -------------
.env
.env.*
!.env.example
# ^ The leading ! re-includes .env.example so it can be referenced if needed,
#   while every other .env.* (real secrets) stays excluded.

# --- Docs, IDE, OS cruft ----------------------------------------------------
Docs/
*.md
!README.md
.vscode/
.idea/
.DS_Store
Thumbs.db

# --- Docker meta (avoid recursive/irrelevant context) -----------------------
docker-compose*.yml
.dockerignore
docker/

# --- Tests (not needed at runtime; --no-dev already drops the test deps) -----
tests/

# --- Scripts (manual demo; not part of the image) ---------------------------
scripts/
```

> [!CAUTION]
> Excluding `.env` (and all `.env.*` except the example) is a **security requirement, not an optimization**. If `.env` were copied into a layer, your local API keys or provider secrets would be embedded in the image — and image layers are immutable and often pushed to a registry. Once a secret is in a pushed layer, treat it as compromised. The `.dockerignore` is the first line of defense; the Dockerfile never `COPY . .`'ing the whole context (it copies only `src/`, `migrations/`, `pyproject.toml`, `uv.lock`, `alembic.ini`) is the second.

> [!TIP]
> Excluding `tests/`, `Docs/`, and `scripts/` is safe because the runtime never needs them: tests run on the host/CI via `uv run`, docs are for humans, and `smoke.ps1` runs on the host against the *published port*, not inside a container. Keeping them out of the context shaves seconds off every `docker build` and keeps the image focused.

### 6.3 `docker-compose.yml`

**Purpose & responsibilities.** Define the full-stack graph. This *extends* the Phase 3 infra-only file. Infra services (`postgres`, `redis`, `minio`, `createbuckets`) keep their Phase 3 definitions (healthchecks, named volumes); Phase 8 adds the one-shot `migrate` service and the long-running `api` and `worker` services, plus the `x-app-*` YAML anchors that DRY up the shared image build and environment.

```yaml
# docker-compose.yml — full-stack Asynchronous AI Serving Engine
# Compose v2 (the `docker compose` plugin). No top-level `version:` key — it is
# obsolete in v2 and emits a deprecation warning if present.

# =============================================================================
# YAML anchors — DRY up the repeated build block and env so api/worker/migrate
# all reference the SAME image and the SAME in-network configuration.
# =============================================================================
x-app-build: &app-build
  context: .                      # Build context = repo root (sees pyproject/uv.lock/src)
  dockerfile: docker/Dockerfile   # Dockerfile lives under docker/

# Shared environment for every app container. These OVERRIDE the Settings()
# host defaults so services talk to each other by compose service name.
x-app-env: &app-env
  AIE_ENV: ${AIE_ENV:-dev}
  # Connection strings point at SERVICE NAMES (postgres/redis), not localhost:
  AIE_DATABASE_URL: postgresql+asyncpg://${POSTGRES_USER:-aie}:${POSTGRES_PASSWORD:-aie}@postgres:5432/${POSTGRES_DB:-aie}
  AIE_REDIS_URL: redis://redis:6379/0
  # CRITICAL zero-cloud-in-container override: MinIO is reachable as `minio`,
  # NOT localhost. See Section 8 for how this interacts with the Phase 1 validator.
  AIE_OBJECT_STORE__ENDPOINT_URL: http://minio:9000
  AIE_OBJECT_STORE__ACCESS_KEY_ID: ${MINIO_ROOT_USER:-minioadmin}
  AIE_OBJECT_STORE__SECRET_ACCESS_KEY: ${MINIO_ROOT_PASSWORD:-minioadmin}
  AIE_OBJECT_STORE__BUCKET: ${AIE_OBJECT_STORE__BUCKET:-aie-artifacts}
  AIE_OBJECT_STORE__REGION: ${AIE_OBJECT_STORE__REGION:-us-east-1}
  AIE_API_KEYS: ${AIE_API_KEYS:-local-dev-key}
  AIE_LOG_LEVEL: ${AIE_LOG_LEVEL:-INFO}
  # Provider keys are intentionally unset by default -> fake providers activate
  # (zero cloud). Uncomment in .env to switch to real SDK adapters.
  AIE_HUGGINGFACE_TOKEN: ${AIE_HUGGINGFACE_TOKEN:-}
  AIE_PINECONE_API_KEY: ${AIE_PINECONE_API_KEY:-}

services:
  # ===========================================================================
  # Infrastructure (carried over from Phase 3, with healthchecks)
  # ===========================================================================
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: ${POSTGRES_USER:-aie}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-aie}
      POSTGRES_DB: ${POSTGRES_DB:-aie}
    volumes:
      - pgdata:/var/lib/postgresql/data    # named volume (Windows-friendly)
    healthcheck:
      # pg_isready ships in the postgres image; -U/-d match the created role/db.
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER:-aie} -d ${POSTGRES_DB:-aie}"]
      interval: 5s
      timeout: 5s
      retries: 10
      start_period: 10s     # grace before failures count (initdb takes a moment)
    restart: unless-stopped

  redis:
    image: redis:7-alpine
    command: ["redis-server", "--appendonly", "yes"]   # durable AOF persistence
    volumes:
      - redisdata:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]                # prints PONG when ready
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 5s
    restart: unless-stopped

  minio:
    image: minio/minio:latest
    command: ["server", "/data", "--console-address", ":9001"]
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER:-minioadmin}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD:-minioadmin}
    ports:
      - "9000:9000"    # S3 API (host -> container), used by smoke/host tools
      - "9001:9001"    # Web console at http://localhost:9001 (manual artifact check)
    volumes:
      - miniodata:/data
    healthcheck:
      # `mc ready local` is the modern readiness probe baked into recent images.
      # Fallback if your pinned image lacks it (see Pitfalls): a TCP/HTTP check on :9000.
      test: ["CMD", "mc", "ready", "local"]
      interval: 5s
      timeout: 5s
      retries: 10
      start_period: 10s
    restart: unless-stopped

  # One-shot: create the artifacts bucket, then EXIT 0. Runs after MinIO is healthy.
  createbuckets:
    image: minio/mc:latest
    depends_on:
      minio:
        condition: service_healthy
    # entrypoint "" clears the image's default ENTRYPOINT so our shell runs cleanly.
    entrypoint: ["/bin/sh", "-c"]
    command:
      - |
        set -e
        mc alias set local http://minio:9000 "${MINIO_ROOT_USER:-minioadmin}" "${MINIO_ROOT_PASSWORD:-minioadmin}"
        mc mb --ignore-existing "local/${AIE_OBJECT_STORE__BUCKET:-aie-artifacts}"
        echo "bucket ready: ${AIE_OBJECT_STORE__BUCKET:-aie-artifacts}"
    restart: "no"     # one-shot: never restart on exit

  # ===========================================================================
  # Schema migration: one-shot, runs `alembic upgrade head`, then EXIT 0.
  # api/worker gate on this via service_completed_successfully.
  # ===========================================================================
  migrate:
    build: *app-build               # SAME image as api/worker
    image: aie-app:latest           # name it so api/worker can reuse the built image
    environment: *app-env
    depends_on:
      postgres:
        condition: service_healthy  # DB must accept connections before migrating
    command: ["alembic", "upgrade", "head"]
    restart: "no"                   # one-shot

  # ===========================================================================
  # API: long-running uvicorn. Default image CMD already runs the API, but we
  # set it explicitly for clarity and to be independent of the image default.
  # ===========================================================================
  api:
    build: *app-build
    image: aie-app:latest
    environment: *app-env
    depends_on:
      migrate:
        condition: service_completed_successfully   # schema is current
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
      minio:
        condition: service_healthy
      createbuckets:
        condition: service_completed_successfully   # bucket exists
    command:
      ["uvicorn", "app.api.app:create_app", "--factory",
       "--host", "0.0.0.0", "--port", "8000"]
    ports:
      - "8000:8000"    # host:container — smoke.ps1 hits http://localhost:8000
    healthcheck:
      # No curl in the slim image -> use the bundled Python interpreter.
      # Exit 0 on HTTP 200 from the liveness route, non-zero otherwise.
      test: ["CMD", "python", "-c",
             "import urllib.request,sys; sys.exit(0) if urllib.request.urlopen('http://localhost:8000/health', timeout=3).status==200 else sys.exit(1)"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 15s   # give the app time to build the container + open pools
    restart: unless-stopped

  # ===========================================================================
  # Worker: long-running custom Redis Streams consumer. SAME image, different CMD.
  # ===========================================================================
  worker:
    build: *app-build
    image: aie-app:latest
    environment: *app-env
    depends_on:
      migrate:
        condition: service_completed_successfully
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
      minio:
        condition: service_healthy
      createbuckets:
        condition: service_completed_successfully
    command: ["python", "-m", "app.worker"]
    # On `docker stop`/compose down, send SIGTERM and wait up to 30s for the
    # worker to drain in-flight jobs (Phase 7) before SIGKILL. Prevents killing
    # a job mid-pipeline; complements at-least-once redelivery.
    stop_grace_period: 30s
    restart: unless-stopped

# =============================================================================
# Named volumes — managed by Docker (NOT host bind mounts). On Windows this
# avoids path-with-space issues ("Study supply") and slow cross-FS bind mounts.
# `docker compose down -v` removes these; plain `down` keeps them.
# =============================================================================
volumes:
  pgdata:
  redisdata:
  miniodata:
```

#### Walkthrough of the non-obvious parts

- **No top-level `version:`** — Compose v2 ignores it and warns if present. Omitting it is current best practice.
- **YAML anchors (`&app-build`, `&app-env`)** — `migrate`, `api`, and `worker` must build the *same* image and share the *same* in-network env. Anchors define it once (`x-app-build`, `x-app-env`) and reference it with `*app-build` / `*app-env`. The `x-` prefix marks these as Compose extension fields (ignored as services). This is the DRY mechanism that prevents env drift between the three app services.
- **`build: *app-build` + `image: aie-app:latest` on all three app services** — the first service to build (Compose builds in dependency order) produces `aie-app:latest`; the others reuse it. Tagging with `image:` means `docker compose build` produces a single named image you can also `docker run` directly, and it prevents three separate anonymous builds.
- **`${VAR:-default}` interpolation** — every tunable reads from the host environment / `.env` with a safe local default. `docker compose` automatically loads a `.env` file from the project directory, so `cp .env.example .env` is enough to configure the whole stack. Defaults mean the stack runs even with an empty `.env`.
- **Connection strings target service names** — `@postgres:5432`, `redis://redis:6379`, `http://minio:9000`. Compose puts all services on a shared default network where each service is reachable by its name via Docker's embedded DNS. This is *the* reason the env overrides exist.
- **`createbuckets` `entrypoint: ["/bin/sh","-c"]` + heredoc `command`** — the `minio/mc` image has its own `ENTRYPOINT`; clearing it to `/bin/sh -c` lets us run a small idempotent script: set an `mc` alias to the in-network MinIO, then `mc mb --ignore-existing` the artifacts bucket. `--ignore-existing` makes re-runs a no-op (idempotent across repeated `up`).
- **API healthcheck via `python -c`** — the slim runtime image has Python but not `curl`. The check opens `http://localhost:8000/health` (here `localhost` *is* correct — the check runs *inside* the api container, hitting its own port) and exits 0 only on HTTP 200. `start_period: 15s` prevents early failures while the lifespan builds the `AppContainer` and opens pools.
- **`worker` has no healthcheck** — a Streams consumer has no HTTP surface to probe; its "health" is "the process is up and the loop is running." Nothing `depends_on` the worker, so a healthcheck would add no ordering value. (If you later want one, expose a tiny liveness file the loop touches, and probe that — but it's out of scope here.)
- **`stop_grace_period: 30s` on worker only** — the API can stop fast (uvicorn closes connections and the lifespan `aclose()`s the container quickly); the worker needs time to finish whatever job is mid-pipeline. 30 s is generous for the fake-provider pipelines and a sane default for real ones.

> [!IMPORTANT]
> **Why both `migrate` *and* infra appear in `api`/`worker` `depends_on`.** `service_completed_successfully` on `migrate` only guarantees the *schema* is current — it does **not** imply Postgres is still healthy at the moment the app starts (Compose evaluates conditions independently). Listing `postgres: service_healthy` *and* `migrate: service_completed_successfully` makes both guarantees explicit. Likewise `createbuckets: service_completed_successfully` ensures the artifacts bucket exists before the worker tries to write to it. Being explicit here is cheap and removes a class of "works the first time, flakes on restart" bugs.

#### Design rationale & how it honors the locked architecture

- **In-network env overrides** are the operational expression of the locked "zero-cloud isolation" rule: in containers the object store must be `http://minio:9000`. The app code is untouched; only the injected `AIE_*` env changes.
- **Migrate gate** keeps schema changes decoupled from app boot, consistent with "PostgreSQL is the single source of truth" — the schema is established once, authoritatively, before any reader/writer connects.
- **Fakes by default** (no provider keys set) honors "zero keys, zero cloud" so a clean clone runs end-to-end with no credentials.

### 6.4 `.env.example`

**Purpose & responsibilities.** The single, authoritative reference for every `AIE_*` (and infra) variable, with **safe local defaults** that make the stack run out-of-the-box and **commented-out** real-provider keys. A clean clone runs `cp .env.example .env` (or `Copy-Item .env.example .env` on Windows) and is immediately ready for `docker compose up`.

```dotenv
# =============================================================================
# Asynchronous AI Serving Engine — environment configuration
#
# Copy to `.env` (this stack runs out of the box with these defaults):
#   POSIX : cp .env.example .env
#   PWSH  : Copy-Item .env.example .env
#
# `docker compose` auto-loads `.env` from the project directory. The same names
# also configure a host-run dev process (`uv run poe api`/`worker`) via
# pydantic-settings (env_prefix="AIE_", nested groups use a "__" delimiter).
# =============================================================================

# --- Runtime environment -----------------------------------------------------
# dev | test | prod. Anything != prod triggers the zero-cloud MinIO redirect in
# Settings WHEN no object-store endpoint is set. In compose we set the endpoint
# explicitly (below), so the redirect is effectively pre-satisfied to MinIO.
AIE_ENV=dev
AIE_LOG_LEVEL=INFO

# --- API authentication ------------------------------------------------------
# Comma-separated API keys accepted on the X-API-Key header. CHANGE for anything
# beyond local. smoke.ps1 reads the FIRST key from here.
AIE_API_KEYS=local-dev-key

# --- PostgreSQL --------------------------------------------------------------
# These three feed BOTH the postgres container (POSTGRES_*) AND the app's
# AIE_DATABASE_URL (assembled in compose). Host runs use AIE_DATABASE_URL directly.
POSTGRES_USER=aie
POSTGRES_PASSWORD=aie
POSTGRES_DB=aie
# Host-run override (compose sets its own pointing at the `postgres` service):
#   AIE_DATABASE_URL=postgresql+asyncpg://aie:aie@localhost:5432/aie

# --- Redis -------------------------------------------------------------------
# Compose sets AIE_REDIS_URL=redis://redis:6379/0. Host-run default:
#   AIE_REDIS_URL=redis://localhost:6379/0

# --- Object store (MinIO / S3-compatible) ------------------------------------
# MinIO root credentials (also become the S3 access/secret keys the app uses).
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
AIE_OBJECT_STORE__BUCKET=aie-artifacts
AIE_OBJECT_STORE__REGION=us-east-1
#
# IN-NETWORK ENDPOINT (set automatically by compose; documented here):
#   AIE_OBJECT_STORE__ENDPOINT_URL=http://minio:9000   # containers use the service name
# ON-HOST ENDPOINT (for `uv run poe api` outside Docker):
#   AIE_OBJECT_STORE__ENDPOINT_URL=http://localhost:9000
# If you LEAVE the endpoint unset on host with AIE_ENV != prod, the Settings
# validator auto-fills http://localhost:9000 (path-style). In containers we never
# leave it unset — we set http://minio:9000 — because `localhost` inside a
# container is the container itself, not MinIO. (See phase-8 doc, Section 8.)

# --- Broker / Redis Streams (defaults baked into Settings; override if needed)
# AIE_BROKER__STREAM=aie:jobs
# AIE_BROKER__GROUP=aie-workers
# AIE_BROKER__DLQ=aie:jobs:dlq
# AIE_BROKER__MAX_ATTEMPTS=3
# AIE_BROKER__BLOCK_MS=5000
# AIE_BROKER__RECLAIM_IDLE_MS=60000
# AIE_BROKER__WORKER_CONCURRENCY=8

# --- Concurrency / retry (Settings defaults; override if needed) -------------
# AIE_OFFLOAD_MAX_WORKERS=32
# AIE_RETRY__MAX_ATTEMPTS=3
# AIE_RETRY__BASE_DELAY_S=0.2
# AIE_RETRY__MAX_DELAY_S=5.0

# =============================================================================
# REAL PROVIDER KEYS — LEAVE COMMENTED to use deterministic FAKE providers
# (zero keys, zero cloud). Uncomment + fill to activate the real SDK adapters.
# NEVER commit a filled-in .env — it is git-ignored and .dockerignore'd.
# =============================================================================
# AIE_HUGGINGFACE_TOKEN=hf_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
# AIE_PINECONE_API_KEY=pcsk_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
# AIE_PROVIDERS__PINECONE_INDEX=aie-index
#
# For a REAL AWS S3 (instead of MinIO), set env=prod AND provide a real endpoint
# (or omit endpoint to hit AWS), real keys, and the correct region. The
# zero-cloud redirect ONLY engages when env != prod.
# AIE_ENV=prod
# AIE_OBJECT_STORE__ENDPOINT_URL=
# AIE_OBJECT_STORE__ACCESS_KEY_ID=AKIA....
# AIE_OBJECT_STORE__SECRET_ACCESS_KEY=....
# AIE_OBJECT_STORE__REGION=us-east-1
```

> [!WARNING]
> **`minioadmin:minioadmin` is a deliberately well-known *local* default — never use it beyond your laptop.** It exists so the demo runs with zero setup. For anything shared, generate strong credentials and inject them via the host environment / a secrets manager, never by editing this committed file. The real-provider keys stay commented precisely so the default path is fakes-only.

> [!NOTE]
> **Nested settings use a `__` delimiter.** `AIE_OBJECT_STORE__ENDPOINT_URL` maps to `Settings.object_store.endpoint_url` because pydantic-settings is configured (Phase 1) with `env_nested_delimiter="__"`. Single-underscore names like `AIE_DATABASE_URL` are top-level fields. Mixing these up (`AIE_OBJECT_STORE_ENDPOINT_URL` with one underscore) silently fails to bind — double-check delimiters when adding vars.

### 6.5 `scripts/smoke.ps1`

**Purpose & responsibilities.** A **manual** end-to-end demo for Windows. It POSTs a `rag_query` to the running API with the configured API key, captures the returned `job_id`, then polls `GET /v1/jobs/{id}` on a fixed interval until the job reaches a terminal state (`SUCCESS`/`FAILED`) or a timeout elapses. It prints a clear pass/fail and sets a process exit code. It is **not** imported by `pytest` and contains **no** assertions about timing — its polling is a human-facing convenience, keeping the automated suite clock-free (a hard exit criterion).

```powershell
#Requires -Version 5.1
<#
.SYNOPSIS
    Manual end-to-end smoke test for the Asynchronous AI Serving Engine.

.DESCRIPTION
    POSTs a rag_query job to the running API (X-API-Key auth), captures the
    job id, then polls GET /v1/jobs/{id} until SUCCESS/FAILED or a timeout.

    MANUAL DEMO ONLY. This script is intentionally excluded from the pytest
    suite so the automated tests remain deterministic and clock-free. Run it
    by hand after `docker compose up --build -d`.

.PARAMETER BaseUrl
    API base URL. Default http://localhost:8000 (the compose-published port).

.PARAMETER ApiKey
    X-API-Key value. Defaults to AIE_API_KEYS from .env (first key) or env var,
    else "local-dev-key".

.PARAMETER TimeoutSeconds
    Max seconds to poll before declaring failure. Default 60.

.PARAMETER PollSeconds
    Seconds between polls. Default 2.

.EXAMPLE
    pwsh ./scripts/smoke.ps1
.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\scripts\smoke.ps1 -TimeoutSeconds 90
#>
[CmdletBinding()]
param(
    [string] $BaseUrl        = $(if ($env:SMOKE_BASE_URL) { $env:SMOKE_BASE_URL } else { "http://localhost:8000" }),
    [string] $ApiKey         = "",
    [int]    $TimeoutSeconds = 60,
    [int]    $PollSeconds    = 2
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"   # fail fast on any unhandled error

# --- ASCII status helpers (no Unicode/emoji per PowerShell-Windows rules) ----
function Write-Info { param([string] $Message) Write-Host "[i] $Message" }
function Write-Ok   { param([string] $Message) Write-Host "[OK] $Message" -ForegroundColor Green }
function Write-Err  { param([string] $Message) Write-Host "[X] $Message" -ForegroundColor Red }

# --- Resolve the script directory (so relative .env lookup is robust) --------
$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectDir = Split-Path -Parent $ScriptDir   # scripts/ -> repo root

# --- Resolve the API key: param > env var > .env (first AIE_API_KEYS) > default
function Resolve-ApiKey {
    param([string] $Provided, [string] $ProjectRoot)

    if ($Provided) { return $Provided }
    if ($env:AIE_API_KEYS) { return ($env:AIE_API_KEYS -split ",")[0].Trim() }

    $envFile = Join-Path $ProjectRoot ".env"
    if (Test-Path $envFile) {
        # Find the AIE_API_KEYS line; take the first comma-separated key.
        $line = Get-Content $envFile |
                Where-Object { $_ -match '^\s*AIE_API_KEYS\s*=' } |
                Select-Object -First 1
        if ($line) {
            $value = ($line -split "=", 2)[1].Trim()
            if ($value) { return ($value -split ",")[0].Trim() }
        }
    }
    return "local-dev-key"   # matches the .env.example default
}

$resolvedKey = Resolve-ApiKey -Provided $ApiKey -ProjectRoot $ProjectDir

Write-Info "Base URL : $BaseUrl"
Write-Info "API key  : $($resolvedKey.Substring(0, [Math]::Min(4, $resolvedKey.Length)))*** (masked)"
Write-Info "Timeout  : ${TimeoutSeconds}s (poll every ${PollSeconds}s)"

# --- 1) Submit a rag_query job ----------------------------------------------
# Discriminated-union payload (Phase 6): job_type selects the schema variant.
# Phase 6 JobSubmission wraps the discriminated union in a `payload` field;
# `job_type` is the discriminator INSIDE that payload, not a top-level field.
$body = @{
    payload = @{
        job_type = "rag_query"
        query    = "What is event-loop starvation and how does this engine avoid it?"
        top_k    = 3
    }
} | ConvertTo-Json -Depth 10   # -Depth required for nested objects (PS rule)

$headers = @{
    "X-API-Key"    = $resolvedKey
    "Content-Type" = "application/json"
}

Write-Info "POST $BaseUrl/v1/jobs (job_type=rag_query) ..."
try {
    $submit = Invoke-RestMethod -Method Post -Uri "$BaseUrl/v1/jobs" `
                                -Headers $headers -Body $body
}
catch {
    Write-Err "Submission failed: $($_.Exception.Message)"
    # Surface the HTTP status if present (401 = bad/missing key, 422 = bad payload).
    if ($_.Exception.Response) {
        $status = [int] $_.Exception.Response.StatusCode
        Write-Err "HTTP status: $status"
        if ($status -eq 401) { Write-Err "Check AIE_API_KEYS / -ApiKey." }
        if ($status -eq 422) { Write-Err "Check the request payload schema." }
    }
    exit 1
}

# The 202 body is JobAccepted { job_id, status, status_url } (Phase 6).
$jobId = $submit.job_id
if (-not $jobId) {
    Write-Err "Response did not contain a job_id. Raw: $($submit | ConvertTo-Json -Depth 10)"
    exit 1
}
Write-Ok "Accepted. job_id = $jobId (initial status: $($submit.status))"

# --- 2) Poll until terminal or timeout ---------------------------------------
$deadline   = (Get-Date).AddSeconds($TimeoutSeconds)
$terminal   = @("SUCCESS", "FAILED")
$finalState = $null
$lastStatus = ""

while ((Get-Date) -lt $deadline) {
    try {
        $job = Invoke-RestMethod -Method Get -Uri "$BaseUrl/v1/jobs/$jobId" `
                                 -Headers @{ "X-API-Key" = $resolvedKey }
    }
    catch {
        # Transient blip (e.g., app still warming) — report and keep polling.
        Write-Info "Poll error (continuing): $($_.Exception.Message)"
        Start-Sleep -Seconds $PollSeconds
        continue
    }

    $status = $job.status
    if ($status -ne $lastStatus) {
        Write-Info "status -> $status"
        $lastStatus = $status
    }

    if ($terminal -contains $status) {
        $finalState = $status
        break
    }

    Start-Sleep -Seconds $PollSeconds   # human-facing poll interval (NOT a test clock)
}

# --- 3) Report ---------------------------------------------------------------
if ($finalState -eq "SUCCESS") {
    Write-Ok "Job $jobId reached SUCCESS."
    if ($job.PSObject.Properties.Name -contains "result_ref" -and $job.result_ref) {
        Write-Info "Artifact: $($job.result_ref)  (browse MinIO console at http://localhost:9001)"
    }
    exit 0
}
elseif ($finalState -eq "FAILED") {
    Write-Err "Job $jobId reached FAILED."
    if ($job.PSObject.Properties.Name -contains "error" -and $job.error) {
        Write-Err "error: $($job.error)"
    }
    exit 1
}
else {
    Write-Err "Timed out after ${TimeoutSeconds}s; last status: '$lastStatus'."
    Write-Err "Check: docker compose ps ; docker compose logs worker"
    exit 1
}
```

#### Walkthrough of the non-obvious parts

- **`#Requires -Version 5.1`** — declares the script runs on Windows PowerShell 5.1 *and* `pwsh` 7+ (5.1 is the floor). Everything used (`Invoke-RestMethod`, splatting, `Set-StrictMode`, `[Math]::Min`) is 5.1-compatible — no `??`/ternary/`-AsHashtable` 7-only features.
- **ASCII-only status markers (`[OK]`/`[X]`/`[i]`)** — per the PowerShell-Windows skill, no Unicode/emoji in scripts; they corrupt under the default 5.1 console encoding. Color is added via `-ForegroundColor`, which is encoding-safe.
- **`Set-StrictMode -Version Latest` + `$ErrorActionPreference = "Stop"`** — fail fast on undefined variables and turn cmdlet errors into catchable exceptions so the `try/catch` around `Invoke-RestMethod` actually fires (by default `Invoke-RestMethod` throws on non-2xx, but other cmdlets may only warn).
- **`ConvertTo-Json -Depth 10`** — PowerShell's default JSON depth is 2; nested payloads (`payload.query`, `payload.top_k`) need an explicit depth or they serialize as `System.Collections.Hashtable`. This is the single most common PowerShell JSON bug.
- **Splatting headers** — `Invoke-RestMethod -Headers @{ "X-API-Key" = ... }` sends the auth header exactly as the FastAPI `APIKeyHeader("X-API-Key")` dependency expects (Phase 6).
- **API-key resolution precedence** — explicit `-ApiKey` > `$env:AIE_API_KEYS` > first key parsed from `.env` > the `local-dev-key` default. This means the script "just works" right after `cp .env.example .env` with no extra flags, but is overridable for other environments.
- **Key masking** — only the first 4 chars are printed, so the demo is safe to screen-share or paste into an interview without leaking a real key.
- **Poll loop is deadline-based, not iteration-count-based** — `(Get-Date).AddSeconds($TimeoutSeconds)` then `while ((Get-Date) -lt $deadline)`. It logs *status transitions* (not every poll) to keep output readable, and continues past transient poll errors (the app may still be warming) rather than aborting.
- **Exit codes** — `0` on SUCCESS, `1` on FAILED/timeout/submission error. A non-zero exit lets a human (or a later wrapper) detect failure; it is *not* wired into CI's test job (which stays clock-free).

> [!IMPORTANT]
> **This script is a demo, not a test.** The pytest suite proves correctness deterministically (RecordingOffloader spies, attempt-count retry tests, Event-gated drains, the `consume_once()` seam). `smoke.ps1` exists to *show* the full stack working to a human — its `Start-Sleep` polling would be exactly the kind of clock dependency the spec forbids in the automated suite. Keep it in `scripts/`, run it by hand, and never `import` or invoke it from `tests/`.

> [!TIP]
> **Running it.** With `pwsh` (PowerShell 7, recommended): `pwsh ./scripts/smoke.ps1`. With Windows PowerShell 5.1, if script execution is blocked by policy: `powershell -ExecutionPolicy Bypass -File .\scripts\smoke.ps1`. The `-ExecutionPolicy Bypass` is per-invocation and does not change the machine policy — appropriate for a local demo. To probe a different host/port: `-BaseUrl http://localhost:8000` or set `$env:SMOKE_BASE_URL`.

---

## 7. Flow & Sequence Diagrams

### 7.1 Multi-stage build (builder → runtime)

```mermaid
flowchart LR
    subgraph S1["Stage 1: builder<br/>ghcr.io/astral-sh/uv:python3.12-bookworm-slim"]
        direction TB
        B1["bind: uv.lock + pyproject.toml<br/>cache: /root/.cache/uv"]
        B2["uv sync --frozen --no-dev<br/>--no-install-project<br/>(deps only -> cached layer)"]
        B3["COPY src/ migrations/ alembic.ini"]
        B4["uv sync --frozen --no-dev<br/>(installs app into /app/.venv)"]
        B1 --> B2 --> B3 --> B4
    end

    subgraph S2["Stage 2: runtime<br/>python:3.12-slim (no uv)"]
        direction TB
        R1["create non-root appuser (UID 1001)"]
        R2["COPY --from=builder /app/.venv<br/>(+ src, migrations, alembic.ini)"]
        R3["ENV PATH=/app/.venv/bin:$PATH<br/>USER appuser"]
        R4["CMD uvicorn --factory ...<br/>(worker overrides in compose)"]
        R1 --> R2 --> R3 --> R4
    end

    B4 -- "COPY --from=builder" --> R2

    classDef s1 fill:#bf8700,stroke:#7d4e00,color:#fff;
    classDef s2 fill:#1f6feb,stroke:#0b3d91,color:#fff;
    class B1,B2,B3,B4 s1;
    class R1,R2,R3,R4 s2;
```

The builder is heavy (has `uv`, the cache, build context); the runtime is lean (only the venv + source + a non-root user). The single `COPY --from=builder /app/.venv` edge is what discards all build-time weight — the runtime image never contains `uv` or the build cache.

### 7.2 Compose dependency graph (infra → migrate → api/worker)

```mermaid
flowchart TD
    PG[("postgres<br/>healthcheck: pg_isready")]
    RED[("redis<br/>healthcheck: redis-cli ping")]
    MIN[("minio<br/>healthcheck: mc ready local")]
    MC["createbuckets<br/>(one-shot: mc mb)"]
    MIG["migrate<br/>(one-shot: alembic upgrade head)"]
    API["api<br/>(uvicorn --factory)"]
    WRK["worker<br/>(python -m app.worker)"]

    MIN -- "service_healthy" --> MC
    PG  -- "service_healthy" --> MIG

    MIG -- "service_completed_successfully" --> API
    MIG -- "service_completed_successfully" --> WRK
    MC  -- "service_completed_successfully" --> API
    MC  -- "service_completed_successfully" --> WRK
    PG  -- "service_healthy" --> API
    RED -- "service_healthy" --> API
    MIN -- "service_healthy" --> API
    PG  -- "service_healthy" --> WRK
    RED -- "service_healthy" --> WRK
    MIN -- "service_healthy" --> WRK

    classDef infra fill:#6e7781,stroke:#424a53,color:#fff;
    classDef once fill:#8250df,stroke:#512a97,color:#fff;
    classDef run fill:#2da44e,stroke:#116329,color:#fff;
    class PG,RED,MIN infra;
    class MC,MIG once;
    class API,WRK run;
```

**Reading the graph:** Compose starts leaf dependencies first. `postgres`/`redis`/`minio` start and run their healthchecks. Once `minio` is healthy, `createbuckets` runs and exits 0. Once `postgres` is healthy, `migrate` runs `alembic upgrade head` and exits 0. Only when *both* one-shots have completed *and* all three infra services are healthy do `api` and `worker` start. This is fully declarative — there is no `wait-for-it.sh`, no retry loop in any entrypoint.

### 7.3 `docker compose up --build -d` startup timeline

```mermaid
sequenceDiagram
    autonumber
    participant U as Operator (host)
    participant C as Compose engine
    participant I as infra (pg/redis/minio)
    participant K as createbuckets
    participant M as migrate
    participant A as api
    participant W as worker

    U->>C: docker compose up --build -d
    C->>C: build image aie-app:latest (multi-stage)
    C->>I: start postgres, redis, minio
    loop until healthy
        I-->>C: healthcheck OK?
    end
    Note over C,I: all infra healthy
    C->>K: start createbuckets (minio healthy)
    K->>I: mc alias set + mc mb --ignore-existing
    K-->>C: exit 0 (completed_successfully)
    C->>M: start migrate (postgres healthy)
    M->>I: alembic upgrade head
    M-->>C: exit 0 (completed_successfully)
    Note over C: gates satisfied (migrate + createbuckets done, infra healthy)
    par
        C->>A: start api (uvicorn --factory)
        A->>A: lifespan: AppContainer.create() opens pools
        A-->>C: /health 200 -> healthy
    and
        C->>W: start worker (python -m app.worker)
        W->>W: AppContainer.create(); consume loop runs
    end
    C-->>U: all services up (detached)
    U->>A: (manual) scripts/smoke.ps1: POST rag_query -> poll -> SUCCESS
```

### 7.4 Zero-cloud endpoint resolution: host vs container

```mermaid
flowchart TD
    Start{"AIE_OBJECT_STORE__ENDPOINT_URL set?"}
    Start -- "Yes (compose sets http://minio:9000)" --> UseSet["Use the explicit endpoint<br/>-> http://minio:9000"]
    Start -- "No" --> EnvChk{"AIE_ENV == prod?"}
    EnvChk -- "No (dev/test)" --> Redirect["Validator forces<br/>http://localhost:9000 + path-style<br/>(zero-cloud redirect)"]
    EnvChk -- "Yes" --> AwsPath["Endpoint stays None<br/>-> boto3 hits real AWS S3"]

    UseSet --> InNet["IN CONTAINER: resolves to the<br/>minio service via Docker DNS = CORRECT"]
    Redirect --> OnHost["ON HOST: localhost:9000 maps to the<br/>published MinIO port = CORRECT"]
    Redirect -. "IN CONTAINER this would be WRONG:<br/>localhost = the container itself" .-> Trap["Connection refused"]

    classDef good fill:#2da44e,stroke:#116329,color:#fff;
    classDef bad fill:#cf222e,stroke:#82071e,color:#fff;
    classDef neutral fill:#6e7781,stroke:#424a53,color:#fff;
    class UseSet,InNet,OnHost good;
    class Trap bad;
    class Redirect,AwsPath,EnvChk,Start neutral;
```

This diagram is the visual statement of Section 8's core point: **set the endpoint explicitly in compose so the validator's `localhost` default never engages inside a container.**

---

## 8. Configuration & Environment

### 8.1 How the zero-cloud redirect interacts with containers (the headline)

> [!IMPORTANT]
> **The Phase 1 validator's job is to keep dev/test off the cloud; the compose env override's job is to make that work *inside containers*.** They cooperate as follows.
>
> The Phase 1 `Settings` `@model_validator(mode="after")` says: *if `env != prod` **and** no object-store `endpoint_url` is configured, force `endpoint_url = http://localhost:9000` and enable path-style addressing.* That default is correct for a process running **on the host** (e.g., `uv run poe api`), where `localhost:9000` reaches the MinIO container's *published* port (compose `ports: "9000:9000"`).
>
> **Inside a container, `localhost` means the container's own loopback.** If the `api`/`worker` container fell through to the validator default, it would try to reach MinIO *inside itself* and get `Connection refused`. Therefore the compose file sets `AIE_OBJECT_STORE__ENDPOINT_URL=http://minio:9000` for every app container. Because the endpoint is now *explicitly set*, the validator's `if no endpoint_url` branch does **not** fire — the value is taken as-is. The redirect's *intent* (stay on local MinIO, never touch AWS) is preserved; only the *hostname* changes from `localhost` to the Docker DNS service name `minio`.

The net effect, summarized:

| Where the process runs | `AIE_ENV` | `AIE_OBJECT_STORE__ENDPOINT_URL` | Effective endpoint | Correct? |
|------------------------|-----------|----------------------------------|--------------------|:--------:|
| Host (`uv run poe api`) | `dev` | *(unset)* | `http://localhost:9000` (validator default) | ✅ reaches published MinIO port |
| Container (api/worker/migrate) | `dev` | `http://minio:9000` (compose sets it) | `http://minio:9000` | ✅ reaches MinIO via Docker DNS |
| Container (mistake) | `dev` | *(unset)* | `http://localhost:9000` (validator default) | ❌ loopback = the container itself |
| Anywhere | `prod` | *(unset)* | `None` → boto3 default (real AWS S3) | ✅ intentional cloud egress |

> [!CAUTION]
> Do **not** "fix" the container case by changing the Phase 1 validator default to `minio`. The validator is a *pure, unit-tested* function whose default must remain `localhost:9000` (correct for the dominant host-dev workflow and for the Phase 1 unit test). The container case is an *operational* override and belongs in compose, not in the code. Keeping the redirect logic in `Settings` and the hostname override in compose is the clean separation the architecture intends.

### 8.2 Environment variable reference (containers)

| Env var | Default (compose) | Used by | Notes |
|---------|-------------------|---------|-------|
| `AIE_ENV` | `dev` | All app services | `!= prod` keeps zero-cloud semantics; combined with explicit endpoint below. |
| `AIE_LOG_LEVEL` | `INFO` | All app services | structlog level. |
| `AIE_API_KEYS` | `local-dev-key` | `api` | Comma-separated; `X-API-Key` must match one. `smoke.ps1` uses the first. |
| `AIE_DATABASE_URL` | `postgresql+asyncpg://aie:aie@postgres:5432/aie` | `api`, `worker`, `migrate` | Host is the **service name** `postgres`. asyncpg driver. |
| `AIE_REDIS_URL` | `redis://redis:6379/0` | `api`, `worker` | Host is the **service name** `redis`. |
| `AIE_OBJECT_STORE__ENDPOINT_URL` | `http://minio:9000` | `api`, `worker` | **The critical override.** Service name, not `localhost`. |
| `AIE_OBJECT_STORE__ACCESS_KEY_ID` | `${MINIO_ROOT_USER:-minioadmin}` | `api`, `worker` | Equals the MinIO root user. |
| `AIE_OBJECT_STORE__SECRET_ACCESS_KEY` | `${MINIO_ROOT_PASSWORD:-minioadmin}` | `api`, `worker` | Equals the MinIO root password. |
| `AIE_OBJECT_STORE__BUCKET` | `aie-artifacts` | `api`, `worker`, `createbuckets` | The bucket `createbuckets` makes and the app writes to. |
| `AIE_OBJECT_STORE__REGION` | `us-east-1` | `api`, `worker` | Region label boto3/MinIO expect. |
| `POSTGRES_USER` / `_PASSWORD` / `_DB` | `aie` / `aie` / `aie` | `postgres`, and assembled into `AIE_DATABASE_URL` | One source feeds both the server and the app URL. |
| `MINIO_ROOT_USER` / `_PASSWORD` | `minioadmin` / `minioadmin` | `minio`, `createbuckets`, and the app S3 keys | Local-only credentials. |
| `AIE_HUGGINGFACE_TOKEN` | *(empty)* | `api`, `worker` | Empty → fake LLM/embedding providers. |
| `AIE_PINECONE_API_KEY` | *(empty)* | `api`, `worker` | Empty → fake vector store. |

> [!NOTE]
> **Where Settings defaults live vs where compose overrides.** Broker, retry, and concurrency knobs (`AIE_BROKER__*`, `AIE_RETRY__*`, `AIE_OFFLOAD_MAX_WORKERS`) have sensible defaults baked into `Settings` (Phase 1) and are **not** repeated in compose — they only need to appear in `.env`/compose if you want to override them. The compose `x-app-env` block deliberately lists only what *must* change for the in-network topology (connection hosts, the MinIO endpoint, credentials) plus a few commonly-tuned values, to keep the file readable.

### 8.3 Image vs runtime configuration boundary

- **Baked into the image (build time):** the Python version, the locked dependency set, the `app` package, the non-root user, the venv-on-PATH. None of it is environment-specific.
- **Injected at run time (compose env):** every connection string, credential, the object-store endpoint, log level, API keys, provider keys. The image is the same artifact in dev, CI, and (hypothetically) prod; only the env differs. This is the twelve-factor separation that lets [Phase 9](phase-9-ci-readme-polish.md) reuse the exact image in CI.

---

## 9. Testing Strategy

Phase 8 is infrastructure-as-files, so its "tests" are primarily **build/up/down verification commands** plus a couple of cheap automated guards. Critically, **none of Phase 8's verification adds clock-dependent assertions to the pytest suite** — the smoke script (the only thing that polls) is excluded from `pytest` by living in `scripts/` and never being collected.

### 9.1 What is *not* added to the test suite (and why)

| Candidate "test" | Verdict | Reason |
|------------------|---------|--------|
| Run `smoke.ps1` in CI's unit job | ❌ excluded | It polls with `Start-Sleep` → clock-dependent; violates the deterministic-suite exit criterion. It is a *manual* demo. |
| `time.sleep()` after `docker compose up`, then assert healthy | ❌ excluded | Sleeping for readiness is exactly the flaky pattern the spec forbids. Use `--wait` (below), which blocks on *healthchecks*, not a clock. |
| Asserting end-to-end latency | ❌ excluded | Timing assertions are non-deterministic across machines/CI. Correctness is proven by the deterministic unit/integration tests from earlier phases. |

### 9.2 Cheap deterministic guards Phase 8 *can* add

These run on the host (or in CI's unit job) with no Docker daemon and no clock:

**(a) Static validation of the compose file** — `docker compose config` parses, interpolates, and validates the file; it fails on YAML errors, unknown keys, or bad interpolation. It is instantaneous and deterministic:

```bash
docker compose config --quiet && echo "compose file valid"
```

**(b) Dockerfile lint (optional, recommended)** — `hadolint` catches anti-patterns (missing `USER`, unpinned `apt` packages, shell-form `CMD`). Deterministic, no daemon:

```bash
docker run --rm -i hadolint/hadolint < docker/Dockerfile
```

**(c) A pure-unit assertion that the validator default is still `localhost`** — this already exists from [Phase 1](phase-1-scaffold-toolchain-domain.md), but Phase 8 *depends* on it, so it's worth re-running. It guards the invariant that the in-container `localhost` trap is real (and thus the compose override is necessary):

```python
# tests/unit/test_config.py (Phase 1 — referenced, not re-authored here)
def test_zero_cloud_redirect_defaults_to_localhost_minio() -> None:
    """env != prod and no endpoint -> validator fills localhost:9000, path-style.

    Phase 8 relies on this: the compose file MUST override the endpoint to
    http://minio:9000 inside the container precisely because the default is
    localhost (which is wrong inside a container). Deterministic, no clock.
    """
    settings = Settings(env="dev", object_store={"endpoint_url": None})  # type: ignore[arg-type]
    assert settings.object_store.endpoint_url == "http://localhost:9000"
    assert settings.object_store.force_path_style is True


def test_explicit_endpoint_is_not_overridden_by_redirect() -> None:
    """When an endpoint IS set (as compose does -> http://minio:9000), the
    validator leaves it untouched. This is exactly the container path."""
    settings = Settings(env="dev", object_store={"endpoint_url": "http://minio:9000"})  # type: ignore[arg-type]
    assert settings.object_store.endpoint_url == "http://minio:9000"
```

> [!TIP]
> The second test above is the *unit-level* proof of the container override's correctness: it shows the validator is a pure pass-through when the endpoint is explicit, so injecting `http://minio:9000` via compose env is guaranteed not to be "redirected away." It's clock-free, daemon-free, and directly maps the operational override back to the code's contract.

### 9.3 The manual smoke flow (documented, not automated)

The full end-to-end *demonstration* (distinct from the test suite) is:

```mermaid
stateDiagram-v2
    [*] --> Up: docker compose up --build -d --wait
    Up --> Smoke: pwsh scripts/smoke.ps1
    Smoke --> Submitted: POST /v1/jobs -> 202 + job_id
    Submitted --> Polling: GET /v1/jobs/{id}
    Polling --> Polling: status in {PENDING, RUNNING}
    Polling --> Success: status == SUCCESS
    Polling --> Failed: status == FAILED
    Polling --> TimedOut: deadline exceeded
    Success --> Down: docker compose down
    Failed --> Down
    TimedOut --> Down
    Down --> [*]: zero dangling resources
```

`--wait` (Compose v2) makes `up` block until services are healthy or a dependency fails — it waits on *healthchecks*, not a fixed sleep, so it's deterministic readiness, appropriate even for scripted/manual use.

> [!NOTE]
> **Why the smoke script polls at all if the suite must be clock-free.** The constraint is on the *automated test suite*, not on operator tooling. A human running a demo needs to *wait* for an asynchronous job to finish, and polling with a timeout is the honest way to do that. The separation is structural: deterministic correctness lives in `pytest` (no clocks); human-facing "show me it works" lives in `scripts/` (polls, never imported by tests).

---

## 10. Verification & Exit-Criteria Mapping

### 10.1 The verify procedure (run top to bottom)

```bash
# 0) Preconditions
docker compose version            # must print v2.x
docker compose config --quiet     # compose file parses & interpolates cleanly

# 1) Build + start the whole stack, block until healthy (or fail fast)
cp .env.example .env              # PWSH: Copy-Item .env.example .env
docker compose up --build -d --wait
#  ^ --build: (re)build aie-app:latest ; -d: detached ; --wait: block on healthchecks
#    Exits non-zero if any healthcheck fails or a gated dependency errors.

# 2) Confirm the expected state of every service
docker compose ps
#  Expect: postgres/redis/minio = "Up (healthy)"
#          createbuckets/migrate = "Exited (0)"
#          api = "Up (healthy)" ; worker = "Up"

# 3) Manual end-to-end demo (POST -> poll -> SUCCESS)
pwsh ./scripts/smoke.ps1
#  Windows PowerShell 5.1: powershell -ExecutionPolicy Bypass -File .\scripts\smoke.ps1
#  Expect: "[OK] Job <id> reached SUCCESS." and exit code 0.

# 4) (optional) Eyeball the artifact in the MinIO console
#  Browse http://localhost:9001  (login minioadmin/minioadmin) -> bucket aie-artifacts

# 5) Tear down — leave NOTHING dangling
docker compose down
#  Removes containers + the default network. Named volumes are KEPT.
#  To also drop data volumes: docker compose down -v

# 6) Prove zero leak at the orchestration layer
docker compose ps -a              # -> empty (no containers, running or exited)
docker network ls                 # -> no "<project>_default" network remains
docker ps -a --filter "name=aie"  # -> empty
```

> [!IMPORTANT]
> **`docker compose down` vs `down -v`.** Plain `down` removes the containers and the project network but **keeps named volumes** (`pgdata`, `redisdata`, `miniodata`) so your data survives a restart — and it satisfies the "zero *dangling* resources" criterion (no orphaned containers/networks). `down -v` additionally deletes the named volumes (a clean slate). The spec's exit criterion is about *no leaks* (nothing left running or orphaned), which plain `down` achieves; use `-v` when you explicitly want to discard state. Document both so the operator chooses deliberately.

### 10.2 Exit-criteria mapping

| Spec exit criterion | How this phase proves it | Command / file |
|---------------------|--------------------------|----------------|
| **Zero-cloud isolation** | App containers point the object store at `http://minio:9000` (never the cloud); fakes are default (no keys). A `rag_query` writes its artifact to MinIO, viewable in the console. | `AIE_OBJECT_STORE__ENDPOINT_URL=http://minio:9000` in `docker-compose.yml`; `pwsh scripts/smoke.ps1`; MinIO console `:9001`. |
| **Zero resource leaking (orchestration layer)** | `docker compose down` removes all containers + the network with nothing dangling; the in-process half (`AppContainer.aclose()`, `pool.checkedout()==0`) is proven by the Phase 6 leak test that runs inside the same image. | `docker compose down` then `docker compose ps -a` (empty), `docker network ls` (no project net). |
| **Deterministic, clock-free tests preserved** | The only polling artifact (`smoke.ps1`) is excluded from `pytest` (lives in `scripts/`, never collected); compose validation and the redirect-default unit test are deterministic. | `scripts/smoke.ps1` not under `tests/`; `docker compose config --quiet`; `tests/unit/test_config.py`. |
| **202 ingestion in ms (still holds in container)** | The smoke script's submit step returns `202` + `job_id` immediately; all pipeline work happens in the separate `worker` container. | `pwsh scripts/smoke.ps1` ("Accepted. job_id = ..." prints before any polling). |
| **No global singletons / shared composition root** | The *same image* runs `uvicorn --factory app.api.app:create_app` (api) and `python -m app.worker` (worker); both build `AppContainer.create()`. | `command:` lines for `api` vs `worker` in `docker-compose.yml`. |
| **`to_thread` + backoff at every boundary (runs unchanged in container)** | No source changes in Phase 8; the offloader/retry discipline proven by Phase 2/4 tests ships verbatim in the image. | `docker/Dockerfile` copies `src/` as-is; earlier-phase tests unchanged. |

---

## 11. Windows & Cross-Platform Notes

> [!IMPORTANT]
> The **image always runs on Linux** (Docker containers are Linux even on Windows hosts, via the WSL 2 backend). So in-container concerns are pure-Linux: `uvloop` is available and active for `uvicorn[standard]`, `loop.add_signal_handler` works, SIGTERM-driven graceful shutdown (and thus `stop_grace_period`) behaves as designed. The Windows-specific gotchas live entirely on the **host side** — building, running compose, and the smoke script.

| Concern | Host (Windows) | In container (Linux) |
|---------|----------------|----------------------|
| Event loop | `ProactorEventLoop`; `uvloop` skipped by marker (Phase 1) | `uvloop` active; standard loop semantics |
| Signal handling | `loop.add_signal_handler` raises `NotImplementedError` → Phase 7 `signal.signal` fallback | `loop.add_signal_handler` works; SIGTERM drains the worker |
| Worker shutdown | N/A (worker runs in container) | SIGTERM → drain ≤ `stop_grace_period: 30s` → exit |
| Line endings | `.gitattributes eol=lf` ensures `smoke.ps1`, Dockerfile, compose stay LF | LF expected by the shell heredocs in `createbuckets` |
| Paths with spaces | Repo path `D:\Study supply\...` — **named volumes** (not bind mounts) sidestep it | No bind mounts; volumes are Docker-managed, path-agnostic |
| Smoke script | PowerShell 5.1 *and* `pwsh` 7+ supported; `#Requires -Version 5.1` | N/A |

> [!WARNING]
> **CRLF in the `createbuckets` heredoc will break it.** The compose `command:` for `createbuckets` is a shell script that runs in a Linux `/bin/sh`. If your editor (or a misconfigured Git checkout) saves `docker-compose.yml` with CRLF line endings, the embedded script lines end in `\r`, and `sh` will fail with errors like `set: -e\r: invalid option` or `mc: not found\r`. The repo-wide `.gitattributes` rule `* text=auto eol=lf` (Phase 1) prevents this; if you hit it anyway, re-checkout the file or run `dos2unix docker-compose.yml`. This is the most likely Windows-specific failure of the whole phase.

> [!TIP]
> **`docker compose` line continuation differs from PowerShell.** In `smoke.ps1`, multiline cmdlet calls use the **backtick** (`` ` ``) continuation, not `\`. The script already uses backticks for `Invoke-RestMethod` line breaks. When you copy commands from this doc's *bash* blocks into a PowerShell terminal, drop the `\` continuations and put the command on one line (or use backticks) — `docker compose up --build -d --wait` is a single line and works as-is in both shells.

> [!NOTE]
> **Docker Desktop file sharing is irrelevant here.** Because the stack uses **named volumes** exclusively (no `- ./host/path:/container/path` bind mounts), you do not need to configure Docker Desktop's "File Sharing" for the `D:\Study supply` drive, and you avoid the slow cross-filesystem I/O bind mounts incur on Windows. The build context *is* read from the host, but that's a one-time read per build, not a live mount.

---

## 12. Common Pitfalls & Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| App logs `Connection refused` / `Could not connect to endpoint URL "http://localhost:9000"` | The container is using the validator's `localhost` default instead of the in-network override. | Ensure `AIE_OBJECT_STORE__ENDPOINT_URL=http://minio:9000` is set in the service's env (it's in `x-app-env`). `docker compose config` to confirm it interpolated. |
| `error: Failed to parse ... uv.lock` or "lockfile is out of date" during build | `uv.lock` missing or stale relative to `pyproject.toml`. | On host: `uv lock`, commit `uv.lock`, rebuild. Never expect `--frozen` to regenerate it. |
| `migrate` exits non-zero; `api`/`worker` never start | A migration failed (bad revision, DB not actually reachable, wrong `AIE_DATABASE_URL`). | `docker compose logs migrate`. Fix the migration or the URL; the `service_completed_successfully` gate is *correctly* blocking a stale-schema app. |
| `api` stuck "starting" / never "healthy" | Healthcheck failing: app not listening on `:8000`, lifespan erroring, or `start_period` too short. | `docker compose logs api`. Confirm the app binds `0.0.0.0:8000`; raise `start_period` if cold start is slow; verify `/health` returns 200. |
| `createbuckets` errors with `mc: not found\r` or `set: invalid option` | CRLF line endings in `docker-compose.yml` corrupting the heredoc. | Re-checkout with LF (`.gitattributes eol=lf`) or `dos2unix docker-compose.yml`. |
| `minio` healthcheck never passes (older pinned image) | The pinned `minio/minio` tag predates `mc ready`. | Pin a current tag, or change the healthcheck to a TCP/HTTP probe, e.g. `["CMD-SHELL","curl -f http://localhost:9000/minio/health/live || exit 1"]` (note: that needs `curl`, present in the MinIO image). |
| `smoke.ps1` → `401 Unauthorized` | API key mismatch between the script and `AIE_API_KEYS`. | Pass `-ApiKey` explicitly or align `.env` `AIE_API_KEYS`; the script masks the key it used in its `[i]` line. |
| `smoke.ps1` → `422 Unprocessable Entity` | Payload doesn't match the `rag_query` discriminated-union schema (Phase 6). | Check field names/types in the `$body` hashtable against `RagQueryPayload`; ensure `ConvertTo-Json -Depth 10`. |
| `smoke.ps1` cannot run: "running scripts is disabled" | Windows PowerShell execution policy. | `powershell -ExecutionPolicy Bypass -File .\scripts\smoke.ps1` (per-invocation; doesn't change machine policy). |
| Image is unexpectedly large (hundreds of MB extra) | `uv` or build cache leaked into runtime; or `COPY . .` pulled in the world. | Confirm runtime stage copies only `/app/.venv` + `src` + migrations; confirm `.dockerignore` excludes `.venv`, `.git`, `tests`, `Docs`. |
| `worker` killed mid-job on shutdown | `stop_grace_period` too short for a real (non-fake) pipeline. | Raise `stop_grace_period` (e.g., `60s`); confirm the worker handles SIGTERM (Phase 7) and exits *before* the grace window. At-least-once redelivery covers any hard kill. |
| `docker compose up` warns `the attribute version is obsolete` | A leftover top-level `version:` key. | Remove `version:` — Compose v2 doesn't use it. |
| Two images built instead of one (`api`, `worker` separately) | Missing shared `image:` tag on the app services. | Ensure all three app services have `image: aie-app:latest` so the build is reused. |
| `service_completed_successfully` "never satisfied" but the one-shot clearly ran | The one-shot has `restart: unless-stopped` (so it "restarts" and never registers as completed), or it exited non-zero. | Set `restart: "no"` on `migrate`/`createbuckets`; check the exit code in `docker compose ps -a`. |

> [!CAUTION]
> **Never set `restart: unless-stopped`/`always` on a one-shot service.** A restart policy that resurrects the container prevents Compose from ever recording it as "completed successfully," so anything gated on `service_completed_successfully` will hang forever waiting. One-shot services (`migrate`, `createbuckets`) must use `restart: "no"`.

---

## 13. Definition of Done

- [ ] `docker/Dockerfile` builds via `docker compose build` (or `docker build -f docker/Dockerfile .`) with no errors and produces `aie-app:latest`.
- [ ] The runtime image runs as **non-root**: `docker run --rm aie-app:latest id -u` prints `1001`.
- [ ] The runtime image contains **no `uv`** and **no test/dev deps**: `docker run --rm aie-app:latest python -c "import importlib.util,sys; sys.exit(0 if importlib.util.find_spec('pytest') is None else 1)"` exits `0`.
- [ ] The same image serves both processes: `api` `command:` is `uvicorn ... --factory ...`; `worker` `command:` is `python -m app.worker`; both reference `image: aie-app:latest`.
- [ ] `.dockerignore` excludes `.git`, `.venv`, caches, `tests/`, `Docs/`, `scripts/`, and **all** `.env*` except `.env.example`.
- [ ] `docker compose config --quiet` passes (file parses and interpolates).
- [ ] `cp .env.example .env` then `docker compose up --build -d --wait` reaches: infra `healthy`, `createbuckets`+`migrate` `Exited (0)`, `api` `healthy`, `worker` `Up`.
- [ ] Every app service sets `AIE_OBJECT_STORE__ENDPOINT_URL=http://minio:9000` (verified via `docker compose config`).
- [ ] `api`/`worker` `depends_on` includes both `migrate: service_completed_successfully` **and** the infra `service_healthy` conditions (+ `createbuckets: service_completed_successfully`).
- [ ] `worker` has `stop_grace_period: 30s`; one-shots (`migrate`, `createbuckets`) have `restart: "no"`.
- [ ] `.env.example` documents **every** `AIE_*`/infra var with safe local defaults and **commented-out** real-provider keys; default path is fakes-only (no keys).
- [ ] `pwsh scripts/smoke.ps1` (and `powershell -ExecutionPolicy Bypass -File .\scripts\smoke.ps1`) POSTs a `rag_query`, captures the id, polls to `SUCCESS`, and exits `0`.
- [ ] `scripts/smoke.ps1` is **not** collected by `pytest` (it lives in `scripts/`, not `tests/`).
- [ ] The produced artifact is visible in the MinIO console (`:9001`, bucket `aie-artifacts`) after a successful smoke run.
- [ ] `docker compose down` leaves zero dangling resources: `docker compose ps -a` empty, no project network in `docker network ls`.
- [ ] All file line endings are LF (Dockerfile, compose, `smoke.ps1`) — verified against `.gitattributes`.

---

## 14. References & Further Reading

**uv & Docker build**
- uv — Using uv in Docker (official integration guide): https://docs.astral.sh/uv/guides/integration/docker/
- uv — Available Docker images & version pinning: https://docs.astral.sh/uv/guides/integration/docker/#available-images
- uv — Docker image package (GHCR, all tags incl. `python3.12-bookworm-slim`): https://github.com/astral-sh/uv/pkgs/container/uv
- uv — `uv sync` reference (`--frozen`, `--locked`, `--no-dev`, `--no-install-project`): https://docs.astral.sh/uv/reference/cli/#uv-sync
- uv — Configuration / environment variables (`UV_COMPILE_BYTECODE`, `UV_LINK_MODE`, `UV_PYTHON_DOWNLOADS`): https://docs.astral.sh/uv/reference/environment/

**Docker / BuildKit**
- Dockerfile reference (multi-stage, `COPY --from`, `USER`, exec-form `CMD`): https://docs.docker.com/reference/dockerfile/
- BuildKit cache mounts (`--mount=type=cache`) and bind mounts (`--mount=type=bind`): https://docs.docker.com/build/cache/optimize/#use-cache-mounts
- Docker — Run containers as a non-root user (best practices): https://docs.docker.com/build/building/best-practices/#user
- `.dockerignore` reference: https://docs.docker.com/reference/dockerfile/#dockerignore-file

**Docker Compose v2**
- Compose `depends_on` long-form & conditions (`service_healthy`, `service_completed_successfully`): https://docs.docker.com/reference/compose-file/services/#depends_on
- Compose `healthcheck`: https://docs.docker.com/reference/compose-file/services/#healthcheck
- Compose environment interpolation & `.env`: https://docs.docker.com/compose/how-tos/environment-variables/variable-interpolation/
- Compose `docker compose up --wait`: https://docs.docker.com/reference/cli/docker/compose/up/
- Compose `docker compose down` (volumes/networks): https://docs.docker.com/reference/cli/docker/compose/down/
- Compose extension fields (`x-` anchors): https://docs.docker.com/reference/compose-file/extension/

**Infra images**
- PostgreSQL official image (`pg_isready`, `POSTGRES_*`): https://hub.docker.com/_/postgres
- Redis official image: https://hub.docker.com/_/redis
- MinIO container quickstart (`MINIO_ROOT_USER/PASSWORD`, `server --console-address`): https://min.io/docs/minio/container/index.html
- MinIO Client `mc` reference (`mc alias set`, `mc mb`, `mc ready`): https://min.io/docs/minio/linux/reference/minio-mc.html

**FastAPI / Uvicorn**
- Uvicorn deployment & `--factory`: https://www.uvicorn.org/deployment/
- FastAPI in containers (official guide): https://fastapi.tiangolo.com/deployment/docker/

**PowerShell**
- `Invoke-RestMethod`: https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.utility/invoke-restmethod
- `about_Execution_Policies`: https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.core/about/about_execution_policies
- `ConvertTo-Json` (`-Depth`): https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.utility/convertto-json

---

## 15. Navigation

| ← Previous | Index | Next → |
|-----------|-------|--------|
| [Phase 7 — Worker Process & Pipelines](phase-7-worker-pipelines.md) | [All phases](README.md) | [Phase 9 — CI, README, Polish](phase-9-ci-readme-polish.md) |
