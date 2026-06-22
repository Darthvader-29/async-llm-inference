# Phase 1 — Scaffold, Toolchain, Settings, Domain Core

> **Part of:** [Asynchronous AI Serving Engine](../implementation-plan.md) · [Problem Statement](../problem-statement.md)
> **Status:** Planned (greenfield) · **Depends on:** none · **Unlocks:** all later phases (2–9)
> **Delivers:** A fully configured, type-checked, lint-clean Python 3.12 project skeleton with a `uv` toolchain, a nested-and-validated `Settings` object (including the zero-cloud MinIO redirect), structured logging, and a pure, framework-free domain core — the `InferenceJob` aggregate and its lifecycle state machine — all proven by deterministic, clock-free unit tests.
> **Primary skills applied:** python-pro, python-packaging, uv-package-manager, pydantic-models-py, domain-driven-design, ddd-tactical-patterns, clean-code, docs-architect, mermaid-expert, powershell-windows

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

Phase 1 lays the **foundation every other phase stands on**. There is no source code in the repository yet — only the spec, the plan, and these phase docs. By the end of Phase 1 the repository is a *real, installable, reproducible* Python project that anyone can clone and bring to a green check with two commands (`uv sync; uv run poe check`). Crucially, none of the later phases can be authored cleanly until two things exist and are trustworthy:

1. **The toolchain contract** — `pyproject.toml` pins Python ≥ 3.12, the full dependency closure, and the configuration for `ruff` (lint + format), `mypy --strict` (with the pydantic plugin), `pytest` (asyncio auto mode + the `integration` marker), and `poethepoet` task aliases. Every subsequent phase's "Verify" step is just a `poe` task defined here.
2. **The domain core** — a pure, dependency-free model of *what a job is and how its status may legally change*. This is the single place in the codebase that encodes the business rule `PENDING → RUNNING → SUCCESS | FAILED` (plus the `RUNNING → PENDING` requeue edge). Persistence (Phase 3), the broker (Phase 5), and the worker pipelines (Phase 7) all *defer* to this state machine rather than re-implementing it.

> [!IMPORTANT]
> The domain core in this phase imports **nothing** from FastAPI, SQLAlchemy, Redis, boto3, or even pydantic. It is plain `dataclasses` and `enum`. This is not stylistic preference — it is the load-bearing rule of the Hexagonal architecture. If `core/` or `domain/` ever imports an adapter or a framework, the whole "ports & adapters" claim collapses, and the project loses its interview value.

### Concrete objectives

| # | Objective | Done when |
|---|-----------|-----------|
| O1 | Reproducible toolchain | `uv sync` resolves and locks; `uv run poe check` runs ruff + mypy + unit tests in one command |
| O2 | Strict typing baseline | `mypy --strict` passes on `src/` and `tests/` with the pydantic plugin enabled |
| O3 | Validated configuration | `Settings` loads from `AIE_`-prefixed env vars with nested groups; defaults are sane for local dev |
| O4 | Zero-cloud redirect | When `env != prod` and no S3 `endpoint_url` is set, object storage is forced to `http://localhost:9000` with path-style addressing — proven by a pure unit test |
| O5 | Pure domain state machine | `InferenceJob` enforces the legal transition graph; every illegal move raises `InvalidTransition` — proven by an exhaustive transition-matrix test |
| O6 | Structured logging | `structlog` renders JSON in prod and a human console in dev, bound to stdlib `logging` |
| O7 | Cross-platform line endings | `.gitattributes` (`* text=auto eol=lf`) + ruff `line-ending = "lf"` keep the repo LF-only on Windows |

> [!NOTE]
> Phase 1 deliberately does **not** create the `ports/` Protocols or the concurrency core — those land in [Phase 2](phase-2-concurrency-retry-ports.md). It also does not create the persistence tables, the broker, or the API. The line is drawn at "things that have zero runtime dependencies and zero infrastructure": configuration, logging, and the domain entity. This keeps Phase 1's test suite runnable with `uv run pytest` and **no Docker, no network, no event loop required**.

---

## 2. Where This Fits

This phase produces the two innermost rings of the hexagon — the **domain** (the `InferenceJob` entity and its rules) and the **application configuration/observability scaffolding** (`Settings`, `logging`). Everything to the outside (ports, adapters, API, worker) is built in later phases and *depends inward* on what we create here.

```mermaid
flowchart TD
    subgraph driving["Driving side (inbound)"]
        API["FastAPI API<br/><i>Phase 6</i>"]
        WORKER["Worker process<br/><i>Phase 7</i>"]
    end

    subgraph app["Application core"]
        CONT["AppContainer<br/>composition root<br/><i>Phase 6</i>"]
        SVC["Services:<br/>ingestion / processor / pipelines<br/><i>Phase 6–7</i>"]
        PORTS["Ports (typing.Protocol)<br/><i>Phase 2</i>"]
    end

    subgraph domain["Domain core — THIS PHASE"]
        MODELS["domain/models.py<br/>InferenceJob · JobStatus · JobType"]
        EXC["domain/exceptions.py<br/>DomainError · InvalidTransition<br/>Transient/PermanentUpstreamError"]
    end

    subgraph infra["Cross-cutting — THIS PHASE"]
        CFG["core/config.py<br/>Settings (AIE_ prefix, nested)"]
        LOG["core/logging.py<br/>structlog config"]
    end

    subgraph driven["Driven side (outbound adapters)"]
        PG["PostgreSQL repo<br/><i>Phase 3</i>"]
        REDIS["Redis Streams broker<br/><i>Phase 5</i>"]
        S3["MinIO / S3 store<br/><i>Phase 4</i>"]
        PROV["AI providers<br/>(fake + real SDKs)<br/><i>Phase 4</i>"]
    end

    API --> CONT
    WORKER --> CONT
    CONT --> SVC
    CONT -.reads.-> CFG
    CONT -.configures.-> LOG
    SVC --> PORTS
    SVC --> MODELS
    PORTS --> PG & REDIS & S3 & PROV
    PG & REDIS & S3 & PROV -.map to/from.-> MODELS
    CFG -.validates → forces MinIO in dev.-> S3

    classDef thisphase fill:#1f6feb,stroke:#0d419d,color:#fff,stroke-width:2px;
    class MODELS,EXC,CFG,LOG thisphase;
```

**Backward:** nothing — this is the root of the dependency graph.

**Forward:** Phase 2 imports `TransientUpstreamError`/`PermanentUpstreamError` from `domain/exceptions.py` to build the `tenacity` retry predicate, and imports `Settings`' `RetrySettings` group to parameterize backoff. Phase 3 maps a SQLAlchemy `JobRow` to and from the `InferenceJob` dataclass and persists `JobStatus`/`JobType` values. Phase 4 reads `ObjectStoreSettings` (including the redirected `endpoint_url`) to build the boto3 client. Phase 5 reads `BrokerSettings`. Phase 6's `AppContainer` calls `Settings()` once at startup and `configure_logging(settings)` once, then injects the resolved settings everywhere. The state machine defined here is exercised end-to-end by the worker in Phase 7.

---

## 3. Prerequisites & Inputs

| Prerequisite | Why | How to get it |
|--------------|-----|---------------|
| **Python 3.12+** available to `uv` | `requires-python >= 3.12`; we use `StrEnum` (3.11+), PEP 695 generics later, and modern typing | `uv python install 3.12` (uv manages the interpreter) |
| **uv ≥ 0.5** | Project & dependency manager; provides `uv sync`, `uv run`, lockfile | `winget install astral-sh.uv` or `pipx install uv` (see References) |
| **Git** | Repo already initialized (`main` branch exists) | Already present |
| The two design docs | Locked decisions, repo layout, dependency list | [`implementation-plan.md`](../implementation-plan.md), [`problem-statement.md`](../problem-statement.md) |

There are **no infrastructure prerequisites** for Phase 1: no Docker, no PostgreSQL, no Redis, no MinIO. The only thing the phase touches at runtime is the Python interpreter. Container infrastructure first appears in [Phase 3](phase-3-persistence-sqlalchemy-alembic.md).

> [!TIP]
> Verify your interpreter before you start so `uv sync` doesn't surprise you. On Windows PowerShell:
> ```powershell
> uv python list          # shows installed/managed interpreters
> uv python install 3.12  # idempotent; installs a managed CPython 3.12 if missing
> ```

---

## 4. Deliverables

Every file created in this phase. Paths are relative to the repository root (`D:\Study supply\projects\async-llm-inference`).

| File | Type | Purpose |
|------|------|---------|
| `pyproject.toml` | new | Project metadata, dependency closure, and tool config for ruff / mypy / pytest / poe |
| `.python-version` | new | Pins the interpreter (`3.12`) for `uv`/`pyenv` auto-selection |
| `.gitignore` | new | Ignore venvs, caches, `.env`, coverage, build artifacts |
| `.gitattributes` | new | `* text=auto eol=lf` — normalize line endings to LF on a Windows checkout |
| `src/app/__init__.py` | new | Marks `app` as the importable package root (src layout) |
| `src/app/core/__init__.py` | new | Package marker for the cross-cutting core |
| `src/app/core/config.py` | new | `Settings(BaseSettings)`, nested settings groups, `Environment` enum, zero-cloud redirect validator, `get_settings()` |
| `src/app/core/logging.py` | new | `configure_logging(settings)` — structlog JSON (prod) / console (dev) bound to stdlib logging |
| `src/app/domain/__init__.py` | new | Package marker for the domain core |
| `src/app/domain/models.py` | new | `InferenceJob` dataclass + `JobStatus` / `JobType` StrEnums + transition methods |
| `src/app/domain/exceptions.py` | new | `DomainError` hierarchy; `InvalidTransition`; `TransientUpstreamError` / `PermanentUpstreamError` |
| `tests/__init__.py` | new | Marks `tests` as a package (helps import resolution / fixture discovery) |
| `tests/conftest.py` | new | Base fixtures: a `dev` `Settings` builder, `freeze`-free time helpers, anyio/asyncio backend pin |
| `tests/unit/__init__.py` | new | Package marker |
| `tests/unit/test_config.py` | new | Unit tests for `Settings`: env parsing, nested groups, and the zero-cloud redirect matrix |
| `tests/unit/test_domain.py` | new | Exhaustive transition-matrix test + happy-path lifecycle test for `InferenceJob` |
| `.env.example` | new (stub) | Documents `AIE_`-prefixed env vars; expanded in later phases |

> [!NOTE]
> `.env.example` is intentionally a small stub here. Database/Redis/MinIO connection details and provider keys are *documented* but commented out, because Phase 1 has zero infra. The file grows in [Phase 3](phase-3-persistence-sqlalchemy-alembic.md) (DB/Redis/MinIO) and [Phase 6](phase-6-composition-root-fastapi-api.md) (API keys, provider secrets). The `Docs/phases/README.md` index already links all nine phase docs, so no index edits are needed in this phase.

---

## 5. Design Decisions & Rationale

| Decision | Choice | Why | Rejected alternative |
|----------|--------|-----|----------------------|
| Package layout | **src layout** (`src/app/...`) | Prevents accidental imports of the in-tree package before install; forces tests to import the *installed* package, catching packaging bugs early | Flat layout (`app/` at root) — easy to "work in tests, break on install" |
| Build backend | **hatchling** | Mature, PEP 517 compliant, trivial `packages = ["src/app"]` for src layout; works seamlessly with `uv` | `uv_build` (newer, fine too) — hatchling is the most widely documented and stable choice |
| Config library | **pydantic-settings v2** (`BaseSettings`) | Declarative env parsing, nested models, validation, `SecretStr`, and a `model_validator` for the zero-cloud redirect — all type-checked by the pydantic mypy plugin | `os.environ` + hand-rolled parsing (no validation, no nesting); `dynaconf` (heavier, less typed) |
| Env prefix & nesting | `env_prefix="AIE_"`, `env_nested_delimiter="__"` | Namespaces all env vars; double-underscore explodes `AIE_BROKER__WORKER_CONCURRENCY` into `broker.worker_concurrency` | Single flat namespace — collides and doesn't group cleanly |
| Domain modeling | **`@dataclass(slots=True)`**, no pydantic | The domain must be framework-free; `slots=True` cuts memory and forbids typo'd attributes; transitions are explicit methods that *guard* invariants | pydantic models in domain (couples domain to a library, blurs the port boundary); raw dicts (no invariants) |
| Status type | **`StrEnum`** (`JobStatus`, `JobType`) | `StrEnum` members *are* strings, so JSON/SQL serialization is trivial and round-trips losslessly, while still giving the type checker a closed set | bare string constants (no exhaustiveness); `IntEnum` (opaque in DB/logs) |
| State machine location | In the **entity methods** (`mark_running`, etc.) | DDD: invariants live with the aggregate that owns them; one source of truth, reused by repo/broker/worker | A separate "service" that mutates status (anemic domain model — the classic anti-pattern) |
| Error taxonomy | `DomainError` base; `Transient`/`Permanent` upstream errors live in the domain | The retry predicate (Phase 2) and the broker DLQ logic (Phase 5) must classify failures *without importing any SDK*; the classification vocabulary belongs to the domain | Defining transient/permanent inside each adapter (duplicated, inconsistent) |
| Logging | **structlog** bound to stdlib `logging`, JSON in prod / console in dev | Structured key-value logs are queryable in prod; the console renderer is readable in dev; binding to stdlib means third-party libs' logs flow through the same pipeline | bare `logging` (unstructured); `print` (no levels, no structure) |
| Task runner | **poethepoet** (`[tool.poe.tasks]`) | One cross-platform task table in `pyproject.toml`; no `Makefile`/`.ps1` divergence between Windows dev and Linux CI | `Makefile` (no native Windows `make`); separate `.ps1` + `.sh` (drift) |
| Test async mode | `asyncio_mode = "auto"` | Async test functions are auto-marked; no `@pytest.mark.asyncio` boilerplate. (Phase 1 itself has no async tests, but the mode is set now so it's consistent from the start) | `strict` mode (more boilerplate); `anyio` plugin (extra dependency we don't need) |

### Why the state machine is the heart of Phase 1

The spec's whole value proposition is *decoupled, observable, self-healing* job processing. "Observable" and "self-healing" both reduce to one question: **what state is this job in, and what is it allowed to do next?** If that rule is scattered — a little in the API, a little in the worker, a little in the repository — it will drift, and you'll get jobs marked `SUCCESS` after they `FAILED`, or requeued jobs that were never `RUNNING`. By concentrating the rule in `InferenceJob` and making every mutation go through a guarded method, we get:

- **One place to read the rule** (and one diagram — see §7).
- **Exhaustive testability**: a single parametrized test can assert every `(from_status, transition)` pair either succeeds or raises `InvalidTransition` (§9).
- **At-least-once safety**: the broker (Phase 5) delivers each message ≥ 1 time; the worker (Phase 7) re-reads the row and, if it's already terminal, skips. The "is it terminal?" check is a property on the entity defined here.

```mermaid
classDiagram
    direction LR
    class InferenceJob {
        +UUID id
        +JobType job_type
        +JobStatus status
        +int attempts
        +str|None result_ref
        +str|None error
        +int|None duration_ms
        +datetime created_at
        +datetime updated_at
        +new(job_type, payload)$ InferenceJob
        +mark_running() None
        +mark_success(result_ref, duration_ms?) None
        +mark_failed(error, duration_ms?) None
        +requeue() None
        +is_terminal: bool
    }
    class JobStatus {
        <<StrEnum>>
        PENDING
        RUNNING
        SUCCESS
        FAILED
    }
    class JobType {
        <<StrEnum>>
        RAG_QUERY
        EMBED_DOCUMENT
    }
    class DomainError {
        <<Exception>>
    }
    class InvalidTransition {
        +JobStatus current
        +JobStatus target
    }
    class JobNotFound {
        +object job_id
    }
    InferenceJob --> JobStatus : status
    InferenceJob --> JobType : job_type
    InferenceJob ..> InvalidTransition : raises
    DomainError <|-- InvalidTransition
    DomainError <|-- JobNotFound
```

> [!IMPORTANT]
> The transition methods mutate the entity **in place** and bump `updated_at` and (where appropriate) `attempts`. They do not persist anything — persistence is a port concern (Phase 3). This keeps the domain free of I/O and makes it trivially unit-testable with no fixtures beyond a constructor call.

---

## 6. Detailed Implementation

This section gives the complete, runnable contents of each Phase 1 file with walkthroughs of the non-obvious parts. Read top-to-bottom; the files are ordered so that dependencies appear before the code that uses them.

### 6.1 `pyproject.toml`

The single source of truth for the build, the dependency closure, and every tool's configuration. Each block is annotated.

```toml
# =============================================================================
# pyproject.toml — Asynchronous AI Serving Engine
# Build backend: hatchling.  Managed by: uv.  Python: 3.12+.
# =============================================================================

[project]
name = "async-ai-engine"
version = "0.1.0"
description = "Asynchronous AI Serving Engine — Hexagonal, non-blocking, zero-cloud-by-default."
readme = "README.md"
requires-python = ">=3.12"          # StrEnum, modern typing, PEP 695-friendly
license = { text = "MIT" }
authors = [{ name = "Atharva Kanawade" }]

# --- Runtime dependencies (the full closure used across all 9 phases) --------
# Versions use lower bounds; uv pins exact versions in uv.lock for reproducibility.
dependencies = [
    "fastapi>=0.115",               # API layer (Phase 6)
    "uvicorn[standard]>=0.32",      # ASGI server; [standard] pulls uvloop on Linux only
    "pydantic>=2.9",                # domain-adjacent validation (schemas, Phase 6)
    "pydantic-settings>=2.5",       # Settings(BaseSettings) — THIS phase
    "sqlalchemy[asyncio]>=2.0.36",  # async ORM core (Phase 3)
    "asyncpg>=0.30",                # async Postgres driver (works on Windows Proactor loop)
    "alembic>=1.14",                # async migrations (Phase 3)
    "redis>=5.2",                   # redis-py with asyncio + Streams (Phase 5)
    "boto3>=1.35",                  # S3/MinIO client (Phase 4)
    "tenacity>=9.0",                # retry/backoff (Phase 2)
    "structlog>=24.4",              # structured logging — THIS phase
    "huggingface-hub>=0.26",        # real LLM/embedding adapter (Phase 4)
    "pinecone>=5.3",                # real vector store adapter (Phase 4)
    "ddgs>=6.0",                    # real search adapter (Phase 4)
]

# --- Console entry points ----------------------------------------------------
# Lets `uv run aie-worker` launch the worker once Phase 7 lands.
[project.scripts]
aie-worker = "app.worker.__main__:main"

# --- Dev dependencies (PEP 735 dependency groups; installed by `uv sync`) ----
[dependency-groups]
dev = [
    "pytest>=8.3",
    "pytest-asyncio>=0.24",         # asyncio_mode = "auto"
    "pytest-cov>=6.0",
    "httpx>=0.27",                  # ASGI test client (Phase 6)
    "asgi-lifespan>=2.1",           # drive lifespan in tests (Phase 6)
    "fakeredis>=2.26",              # in-memory Redis for unit tests (Phase 5)
    "aiosqlite>=0.20",              # in-memory async SQL for repo unit tests (Phase 3)
    "ruff>=0.8",
    "mypy>=1.13",
    "boto3-stubs[s3]>=1.35",        # typing for boto3 S3 calls (Phase 4)
    "poethepoet>=0.30",             # task runner
]

# --- Build backend (src layout) ----------------------------------------------
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/app"]              # the importable package lives under src/

# =============================================================================
# Ruff — linter + formatter (replaces black/isort/flake8)
# =============================================================================
[tool.ruff]
line-length = 100
target-version = "py312"
src = ["src", "tests"]              # first-party roots for import sorting
extend-exclude = ["migrations"]    # Alembic-generated files (Phase 3)

[tool.ruff.lint]
# A pragmatic, strict-leaning selection. Codes:
#   E,W  pycodestyle   F   pyflakes      I   isort        N   pep8-naming
#   UP   pyupgrade     B   bugbear       C4  comprehensions
#   SIM  simplify      RUF ruff-specific TID banned/relative imports
#   ASYNC async-specific lints (blocking calls in async fns)
select = ["E", "W", "F", "I", "N", "UP", "B", "C4", "SIM", "RUF", "TID", "ASYNC"]
ignore = [
    "E501",   # line length is enforced by the formatter, not the linter
]

[tool.ruff.lint.per-file-ignores]
# Tests may use assert and re-import fixtures; allow it.
"tests/**" = ["S101"]
"**/__init__.py" = ["F401"]        # re-exports in package __init__ are intentional

[tool.ruff.lint.flake8-tidy-imports]
ban-relative-imports = "all"       # force absolute imports → clearer hexagon boundaries

[tool.ruff.format]
quote-style = "double"
indent-style = "space"
line-ending = "lf"                 # CRITICAL on Windows: never emit CRLF (see §11)
skip-magic-trailing-comma = false

# =============================================================================
# Mypy — strict static typing, with the pydantic plugin
# =============================================================================
[tool.mypy]
python_version = "3.12"
strict = true                      # turns on the full strict bundle (see notes below)
plugins = ["pydantic.mypy"]        # understands BaseModel/BaseSettings __init__ signatures
warn_unused_configs = true
warn_redundant_casts = true
warn_unused_ignores = true
disallow_untyped_defs = true
no_implicit_reexport = true
files = ["src", "tests"]

# Third-party libs that ship no type stubs → don't fail the build on them.
[[tool.mypy.overrides]]
module = ["ddgs.*", "pinecone.*", "fakeredis.*"]
ignore_missing_imports = true

[tool.pydantic-mypy]
init_forbid_extra = true           # error on unknown kwargs to a model constructor
init_typed = true                  # use real field types in synthesized __init__
warn_required_dynamic_aliases = true

# =============================================================================
# Pytest — asyncio auto mode + the `integration` marker
# =============================================================================
[tool.pytest.ini_options]
asyncio_mode = "auto"              # async test fns auto-marked; no decorator boilerplate
testpaths = ["tests"]
addopts = "-ra -q --strict-markers"   # strict-markers: typo'd markers are errors
markers = [
    "integration: tests that require live infra (Postgres/Redis/MinIO). Deselect with -m 'not integration'.",
]

# =============================================================================
# Poe the Poet — the project's task vocabulary (one command per phase verify)
# =============================================================================
[tool.poe.tasks]
fmt        = "ruff format ."
lint       = "ruff check ."
typecheck  = "mypy src tests"
test       = "pytest -m 'not integration'"
test-int   = "pytest -m integration"
# `check` is the main gate every PR must pass — used by CI (Phase 9) and locally.
check      = ["fmt", "lint", "typecheck", "test"]
api        = "uvicorn app.api.app:create_app --factory --reload --port 8000"
worker     = "python -m app.worker"
up         = "docker compose up -d postgres redis minio"
down       = "docker compose down"
migrate    = "alembic upgrade head"
smoke      = "pwsh -File scripts/smoke.ps1"
```

> [!NOTE]
> `strict = true` in mypy is a *bundle* that enables `disallow_untyped_defs`, `disallow_any_generics`, `check_untyped_defs`, `no_implicit_optional`, `warn_return_any`, `strict_equality`, and more. The extra `disallow_untyped_defs`/`no_implicit_reexport`/`warn_*` lines above are redundant-but-explicit so a future maintainer who flips `strict = false` doesn't silently lose them. Keep them.

> [!WARNING]
> `addopts = "--strict-markers"` means **any** marker not declared in `[tool.pytest.ini_options].markers` is a hard error. This is exactly what we want — it prevents a typo like `@pytest.mark.integraton` from silently running a test in the wrong tier — but it means later phases must register any new marker here before using it.

> [!TIP]
> The `check` task is a **sequence** (`["fmt", "lint", "typecheck", "test"]`). Poe runs the referenced tasks in order and stops on the first non-zero exit. `fmt` is listed first so the formatter normalizes the tree before the linter inspects it; in CI you'll instead want a *check-only* variant (`ruff format --check`) so the job fails rather than mutates — that variant is added in [Phase 9](phase-9-ci-readme-polish.md).

### 6.2 `.python-version`, `.gitattributes`, `.gitignore`

Three small but load-bearing files.

`.python-version` — pins the interpreter so `uv` (and `pyenv`, if present) auto-selects the right CPython:

```text
3.12
```

`.gitattributes` — the single most important Windows hygiene file in the repo. It forces Git to store text files with **LF** line endings regardless of the contributor's OS, which keeps diffs clean and prevents the `ruff format` / Git "whole file changed" thrash:

```gitattributes
# Normalize all text files to LF in the repository.
# `text=auto` lets Git decide what is text; `eol=lf` forces the stored EOL.
* text=auto eol=lf

# Explicitly mark binaries so Git never tries to normalize them.
*.png  binary
*.jpg  binary
*.gif  binary
*.ico  binary
*.pdf  binary
*.whl  binary
```

`.gitignore` — keep the repo free of environments, caches, secrets, and build output:

```gitignore
# --- Python ---
__pycache__/
*.py[cod]
*.egg-info/
build/
dist/

# --- uv / virtualenvs ---
.venv/
.uv/

# --- Tooling caches ---
.ruff_cache/
.mypy_cache/
.pytest_cache/
htmlcov/
.coverage
.coverage.*
coverage.xml

# --- Secrets / local env ---
.env
.env.*
!.env.example      # but DO track the documented example

# --- OS / editor noise ---
.DS_Store
Thumbs.db
.idea/
.vscode/
```

> [!CAUTION]
> The `.env` ignore plus the `!.env.example` negation is deliberate: real secrets in `.env` must **never** be committed, but the *shape* of the configuration (`.env.example`, all placeholders, no real values) **must** be tracked so a fresh clone knows what to set. Never invert this.

### 6.3 `src/app/__init__.py` and package markers

The package root and the two sub-package markers. They are intentionally tiny — re-exports are added by later phases as the public surface grows.

`src/app/__init__.py`:

```python
"""Asynchronous AI Serving Engine — application package root.

Hexagonal architecture: this package's ``domain`` and ``core`` layers must not
import any web framework, ORM, broker, or cloud SDK. Adapters live under
``app.adapters`` (added in later phases) and depend inward on ``app.ports``.
"""

__all__: list[str] = []
```

`src/app/core/__init__.py`:

```python
"""Cross-cutting application core: configuration and logging.

Importable from anywhere; imports nothing from the web/ORM/broker layers.
"""
```

`src/app/domain/__init__.py`:

```python
"""Pure domain core: entities, value objects, and the job state machine.

This package has ZERO third-party imports. If you find yourself importing
pydantic, sqlalchemy, fastapi, redis, or boto3 here, stop — that logic belongs
in an adapter (``app.adapters.*``) behind a port (``app.ports.*``).
"""

from app.domain.exceptions import (
    DomainError,
    InvalidTransition,
    JobNotFound,
    PermanentUpstreamError,
    TransientUpstreamError,
)
from app.domain.models import InferenceJob, JobStatus, JobType

__all__ = [
    "DomainError",
    "InferenceJob",
    "InvalidTransition",
    "JobNotFound",
    "JobStatus",
    "JobType",
    "PermanentUpstreamError",
    "TransientUpstreamError",
]
```

> [!NOTE]
> The `domain/__init__.py` re-exports the public domain vocabulary so consumers write `from app.domain import InferenceJob, JobStatus` rather than reaching into submodules. The `per-file-ignores` for `__init__.py` (`F401`) in §6.1 is what lets these "unused" re-exports pass the linter.

### 6.4 `src/app/domain/exceptions.py`

The domain error taxonomy. Defining `TransientUpstreamError` / `PermanentUpstreamError` *here* (not in an adapter) is what lets Phase 2's `tenacity` predicate and Phase 5's DLQ logic classify failures without importing any SDK.

```python
"""Domain exception hierarchy.

Two independent trees, both rooted at ``DomainError``:

1. **Invariant violations** — e.g. ``InvalidTransition`` raised by the
   ``InferenceJob`` state machine when an illegal status change is attempted.
2. **Upstream classification** — ``TransientUpstreamError`` (worth retrying)
   vs ``PermanentUpstreamError`` (do not retry; dead-letter immediately).
   Adapters (Phase 4) translate raw SDK/botocore exceptions into these so the
   retry policy (Phase 2) and broker DLQ logic (Phase 5) never import an SDK.
"""

from __future__ import annotations


class DomainError(Exception):
    """Base class for every error originating in the domain/application core."""


# ---------------------------------------------------------------------------
# Invariant violations
# ---------------------------------------------------------------------------
class InvalidTransition(DomainError):
    """Raised when an illegal job-status transition is attempted.

    Carries the offending ``current`` and ``target`` statuses so the message is
    self-explanatory in logs and test failures.
    """

    def __init__(self, current: object, target: object) -> None:
        # ``object`` typing avoids a circular import with models.py while still
        # producing a precise message (the enum members stringify to their value).
        self.current = current
        self.target = target
        super().__init__(f"illegal job transition: {current} -> {target}")


class JobNotFound(DomainError):
    """Raised when a job id cannot be found in the repository.

    The persistence adapter (Phase 3) translates a missing row into this domain
    error so services/routes never see a raw SQLAlchemy ``NoResultFound``.
    Phase 6's ``GET /v1/jobs/{id}`` maps it to an HTTP 404.
    """

    def __init__(self, job_id: object) -> None:
        self.job_id = job_id
        super().__init__(f"job not found: {job_id}")


# ---------------------------------------------------------------------------
# Upstream failure classification (used by adapters + retry + DLQ)
# ---------------------------------------------------------------------------
class UpstreamError(DomainError):
    """Base for any failure crossing an external (network) boundary.

    Carries an optional ``cause`` — the original SDK/transport exception — so
    structured logs can record it without the core importing the SDK's error
    types. Adapters raise these via ``translate_upstream_error`` (Phase 2),
    e.g. ``raise TransientUpstreamError(msg, cause=e) from e``.
    """

    def __init__(self, message: str, *, cause: BaseException | None = None) -> None:
        super().__init__(message)
        self.cause = cause
        if cause is not None:
            # Mirror ``raise ... from cause`` so ``__cause__`` is set even when an
            # adapter *returns* (rather than raises) the translated error — which
            # is exactly what ``classify_*`` helpers do (Phase 4).
            self.__cause__ = cause


class TransientUpstreamError(UpstreamError):
    """A retryable upstream failure (timeout, 5xx, connection reset, throttle).

    The retry policy (Phase 2) retries ONLY on this type. Adapters must map
    raw transient SDK errors to this before re-raising.
    """


class PermanentUpstreamError(UpstreamError):
    """A non-retryable upstream failure (auth/403, 400 validation, 404).

    Retrying cannot help; the broker (Phase 5) dead-letters immediately.
    """
```

> [!IMPORTANT]
> `InvalidTransition.__init__` types its arguments as `object` rather than `JobStatus`. That is a deliberate, surgical choice to avoid a **circular import**: `models.py` imports `InvalidTransition` from this module, so this module must not import `JobStatus` from `models.py`. Because `JobStatus` is a `StrEnum`, its members stringify cleanly inside the f-string, so the message is still precise (`illegal job transition: PENDING -> SUCCESS`). Under `mypy --strict` this passes because callers always pass `JobStatus` (a subtype of `object`).

### 6.5 `src/app/domain/models.py`

The heart of the phase: the `InferenceJob` aggregate, its two `StrEnum`s, and the guarded transition methods. This is the *single source of truth* for the lifecycle rule.

```python
"""Domain entities and the job-status state machine.

``InferenceJob`` is an aggregate root modeled as a frozen-by-discipline mutable
dataclass (``slots=True``). All status changes go through guarded methods that
enforce the legal transition graph:

    PENDING --mark_running-->  RUNNING
    RUNNING --mark_success-->  SUCCESS   (terminal)
    RUNNING --mark_failed-->   FAILED    (terminal)
    RUNNING --requeue------->  PENDING   (transient-retry edge)

Any other (current, target) pair raises ``InvalidTransition``. The methods
mutate in place, bump ``updated_at``, and adjust ``attempts`` where relevant.
No persistence, no I/O — that is a port/adapter concern (Phase 3+).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from app.domain.exceptions import InvalidTransition


class JobStatus(StrEnum):
    """Lifecycle state of an inference job. Members ARE their string value."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class JobType(StrEnum):
    """The kind of pipeline a job runs (selects the pipeline in Phase 7)."""

    RAG_QUERY = "rag_query"
    EMBED_DOCUMENT = "embed_document"


# Terminal states cannot transition further. Used by the idempotency guard
# (Phase 5/7): a re-delivered message whose job is already terminal is skipped.
_TERMINAL: frozenset[JobStatus] = frozenset({JobStatus.SUCCESS, JobStatus.FAILED})

# The legal transition graph, as an adjacency map. This is the ONE place the
# rule is encoded; the transition methods consult it via ``_require``.
_ALLOWED: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.PENDING: frozenset({JobStatus.RUNNING}),
    JobStatus.RUNNING: frozenset(
        {JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.PENDING}
    ),
    JobStatus.SUCCESS: frozenset(),  # terminal
    JobStatus.FAILED: frozenset(),   # terminal
}


def _utcnow() -> datetime:
    """Timezone-aware UTC now. Centralized so tests can monkeypatch one symbol.

    Note: production code calls this; deterministic tests assert on *ordering*
    (updated_at advances) rather than on absolute clock values, and may patch
    this symbol when an exact instant is required — never ``time.sleep``.
    """
    return datetime.now(UTC)


@dataclass(slots=True)
class InferenceJob:
    """Aggregate root for a single asynchronous inference request.

    ``slots=True`` forbids attributes that aren't declared here (catches typos
    like ``job.staus = ...`` at runtime) and trims per-instance memory.
    """

    job_type: JobType
    payload: dict[str, object]
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    status: JobStatus = JobStatus.PENDING
    attempts: int = 0
    result_ref: str | None = None
    error: str | None = None
    duration_ms: int | None = None
    created_at: datetime = field(default_factory=_utcnow)
    updated_at: datetime = field(default_factory=_utcnow)

    # -- factory -------------------------------------------------------------
    @classmethod
    def new(cls, job_type: JobType, payload: dict[str, object]) -> InferenceJob:
        """Create a brand-new PENDING job (fresh UUID, ``attempts=0``).

        The canonical creation path used by the ingestion service (Phase 6).
        Equivalent to the constructor with only the required fields, but named
        so call sites read intention-first: ``InferenceJob.new(job_type, payload)``.
        """
        return cls(job_type=job_type, payload=payload)

    # -- read-only derived properties ---------------------------------------
    @property
    def is_terminal(self) -> bool:
        """True when the job can never transition again (SUCCESS or FAILED)."""
        return self.status in _TERMINAL

    # -- internal guard ------------------------------------------------------
    def _require(self, target: JobStatus) -> None:
        """Raise ``InvalidTransition`` unless ``current -> target`` is legal."""
        if target not in _ALLOWED[self.status]:
            raise InvalidTransition(self.status, target)
        self.status = target
        self.updated_at = _utcnow()

    # -- guarded transitions -------------------------------------------------
    def mark_running(self) -> None:
        """PENDING -> RUNNING. Increments ``attempts`` (this is a new try)."""
        self._require(JobStatus.RUNNING)
        self.attempts += 1
        # A fresh attempt clears any error recorded by a previous failed run.
        self.error = None

    def mark_success(self, result_ref: str, duration_ms: int | None = None) -> None:
        """RUNNING -> SUCCESS. Records the object-store reference for the output.

        ``duration_ms`` is the wall-clock execution time measured by the worker
        (Phase 7) via ``time.monotonic()``, persisted as an execution metric.
        It is optional so the pure-domain tests can call ``mark_success(ref)``.
        """
        self._require(JobStatus.SUCCESS)
        self.result_ref = result_ref
        if duration_ms is not None:
            self.duration_ms = duration_ms

    def mark_failed(self, error: str, duration_ms: int | None = None) -> None:
        """RUNNING -> FAILED (terminal). Records the failure reason (+ duration)."""
        self._require(JobStatus.FAILED)
        self.error = error
        if duration_ms is not None:
            self.duration_ms = duration_ms

    def requeue(self) -> None:
        """RUNNING -> PENDING. The transient-retry edge.

        Used by the broker (Phase 5) when an attempt failed transiently and
        retry budget remains. ``attempts`` is NOT bumped here — it is bumped on
        the next ``mark_running``, so ``attempts`` always equals the number of
        times the job actually started executing.
        """
        self._require(JobStatus.PENDING)
```

#### Walkthrough of the non-obvious parts

- **`_ALLOWED` adjacency map.** Every transition method calls `_require(target)`, which looks up the *current* status in `_ALLOWED` and rejects any target not in the allowed set. This makes the rule data-driven: the §7 state diagram and this dict are isomorphic, and the §9 test simply iterates the full `JobStatus × JobStatus` Cartesian product against it.
- **`attempts` semantics.** `mark_running` increments `attempts`; `requeue` does not. The invariant is: *`attempts` == number of times the job has entered `RUNNING`*. This matters in Phase 5, where the broker compares `attempts` to `BrokerSettings.max_attempts` to decide retry-vs-DLQ. Bumping in `requeue` instead would double-count.
- **`error` lifecycle.** `mark_failed` records the error; a subsequent `requeue → mark_running` *clears* it, so a job that ultimately succeeds on retry doesn't carry a stale error string. (Only the requeue path can re-enter `RUNNING`, so this is safe.)
- **`is_terminal`.** Pure, side-effect-free, and the basis of the idempotency guard. `_TERMINAL` and `_ALLOWED` agree by construction (terminal states map to an empty allowed set).
- **`_utcnow()` indirection.** Time comes from one private function. Production calls it; tests that need a fixed instant patch *that one symbol*. No test ever calls `time.sleep` — they assert ordering (`updated_at` advanced) or patch the clock, per the project's clock-free testing rule.

> [!WARNING]
> Do **not** add a setter or expose `status` for direct assignment. The whole point is that `job.status = JobStatus.SUCCESS` from outside the entity is impossible-by-convention; every change must go through a guarded method. `slots=True` plus code review enforces this. (A `@dataclass(frozen=True)` would forbid *all* mutation, which is too strong here — we need in-place transitions — so we use mutable + guarded methods instead.)

> [!TIP]
> Because `JobStatus`/`JobType` are `StrEnum`, `JobStatus.PENDING == "pending"` is `True` and `json.dumps({"status": job.status})` produces `{"status": "pending"}` with no custom encoder. Phase 3 stores the `.value` in Postgres; Phase 6 returns it in API responses. One type, lossless everywhere.

### 6.6 `src/app/core/config.py`

The configuration object. This is the most API-sensitive file in the phase, so every pydantic-settings construct below was verified against the current docs (see §14). It defines: nested settings groups (`BaseModel` subclasses), the top-level `Settings(BaseSettings)`, the `Environment` enum, the **zero-cloud redirect validator**, and a cached `get_settings()` accessor.

```python
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
    max_attempts: int = 3            # retry budget before dead-lettering
    block_ms: int = 5_000            # XREADGROUP block timeout (ms)
    reclaim_idle_ms: int = 60_000    # XAUTOCLAIM idle threshold for orphans (ms)
    worker_concurrency: int = 8      # semaphore size = in-flight job ceiling
    max_delivery_count: int = 5      # reclaim bounces before DLQ (Phase 5)
    maxlen: int = 10_000             # approximate XADD MAXLEN trim cap (Phase 5)


class RetrySettings(BaseModel):
    """tenacity backoff parameters (consumed in Phase 2).

    ``base_delay_s = 0`` in tests makes retries instantaneous so the suite can
    COUNT attempts deterministically instead of measuring wall-clock time.
    """

    max_attempts: int = Field(default=3, ge=1)
    base_delay_s: float = Field(default=0.2, ge=0.0)   # initial=; ge=0 lets tests pin to 0
    max_delay_s: float = Field(default=10.0, ge=0.0)   # wait_exponential_jitter(max=)
    exp_base: float = Field(default=2.0, gt=1.0)       # wait_exponential_jitter(exp_base=)
    jitter_s: float = Field(default=1.0, ge=0.0)       # wait_exponential_jitter(jitter=) max jitter


class ProviderSettings(BaseModel):
    """Non-secret AI-provider configuration (consumed in Phase 4 by build_providers).

    Secrets live as flat top-level fields on Settings (huggingface_token,
    pinecone_api_key); this group holds only the non-secret knobs (model ids,
    embedding dimension, index name, search opt-in/region).
    """

    hf_embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"  # 384-dim
    hf_llm_model: str = "HuggingFaceH4/zephyr-7b-beta"
    embedding_dim: int = 384            # must match the real embedding model's dim
    pinecone_index: str = "aie-index"
    enable_web_search: bool = False     # ddgs needs no key → gated by an explicit flag
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
        env_nested_delimiter="__",   # AIE_BROKER__BLOCK_MS -> broker.block_ms
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",              # tolerate unrelated env vars in the process
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
```

#### Walkthrough of the non-obvious parts

- **Nested groups are `BaseModel`, not `BaseSettings`.** This is the documented requirement: sub-models inherit from `pydantic.BaseModel`, and `env_nested_delimiter="__"` is what populates them from `AIE_<GROUP>__<FIELD>`. Making them `BaseSettings` would be wrong and confusing.
- **`Field(default_factory=...)` for each group.** Without a default, the group would be *required*, forcing every consumer to set its env vars. With a `default_factory`, omitting all of a group's env vars yields its defaults — so `Settings()` with an empty environment is fully valid (essential for the test suite and a fresh clone).
- **`api_keys: Annotated[frozenset[str], NoDecode]` + a before-validator.** This is a real pydantic-settings gotcha worth understanding: by default the settings source tries to **JSON-decode** any collection-typed field, so a comma-separated `AIE_API_KEYS=a,b,c` would raise (it isn't valid JSON). `NoDecode` disables that decoding so the raw string reaches `_split_api_keys`, which splits on commas. Declaring the field `frozenset[str]` then dedups and freezes the result. Phase 6's auth dependency does a constant-time compare against this set. (Alternatively one could JSON-encode the env value, but comma-separated is what `.env.example` documents and what operators expect.)
- **`SecretStr | None` for provider secrets.** Optional by default (fakes are the default provider). When set, the real adapter activates (Phase 4/6). `SecretStr` masks the value in `repr()`/logs — important because `Settings` may be logged at debug.
- **The `model_validator(mode="after")`** receives the fully populated instance, mutates the nested `object_store` group in place, and returns `self` — exactly the verified pattern for after-validators. The condition is `env is not PROD and endpoint_url is None`: an explicitly configured endpoint (even in dev) is respected, and prod is never redirected.

> [!CAUTION]
> The redirect is a **safety rail against accidental cloud spend and data egress**, and it is one of the three named exit criteria. Two failure modes to guard against: (1) if a future refactor moves `endpoint_url` resolution into the adapter and bypasses this validator, dev/test could silently hit real AWS — keep the resolution here. (2) The check is `env is not PROD`, so `TEST` is covered. If you add a `STAGING` environment later, decide explicitly whether it redirects (it almost certainly should).

> [!NOTE]
> `get_settings()` is `@lru_cache`'d, but this is **not** a global singleton in the architectural sense the project forbids. The forbidden pattern is *module-level mutable state that components import directly*. Here, the composition root calls `get_settings()` exactly once and **injects** the result; nothing else imports a global. The cache is a micro-optimization (avoid re-reading env), and tests clear it or bypass it by constructing `Settings(...)` directly.

### 6.7 `src/app/core/logging.py`

Structured logging via `structlog`, bound to the standard library so that *all* logs — ours and third-party libraries' — flow through one pipeline and render consistently: JSON in prod (machine-queryable), a colorized console in dev (human-readable). The processor names below are verified against the structlog stdlib docs (see §14).

```python
"""Structured logging configuration.

``configure_logging(settings)`` is called exactly once by the composition root
(Phase 6, in the FastAPI lifespan) and by the worker entrypoint (Phase 7). It
wires structlog on top of the standard library so that:

* our ``structlog.get_logger()`` calls and third-party ``logging`` calls render
  through the SAME formatter (via ``ProcessorFormatter.foreign_pre_chain``);
* production emits one JSON object per line (ingestible by log shippers);
* development emits a colorized, human-readable console line.
"""

from __future__ import annotations

import logging
import sys

import structlog
from structlog.types import Processor

from app.core.config import Settings

# Processors shared by both formats. They run BEFORE the format-specific
# renderer is chosen, enriching every event dict with level, logger name,
# timestamp, and (for errors) exception/stack information.
_SHARED_PROCESSORS: list[Processor] = [
    structlog.contextvars.merge_contextvars,   # bind request/job context (Phase 6/7)
    structlog.stdlib.add_log_level,            # event_dict["level"] = "info" ...
    structlog.stdlib.add_logger_name,          # event_dict["logger"] = "app.worker"
    structlog.processors.TimeStamper(fmt="iso", utc=True),
    structlog.processors.StackInfoRenderer(),  # render stack_info=True nicely
    structlog.processors.format_exc_info,      # turn exc_info into a "exception" str
]


def configure_logging(settings: Settings) -> None:
    """Idempotently configure structlog + stdlib logging for the given env.

    Safe to call more than once (it fully resets handlers each time), which is
    convenient for tests that construct a container per case.
    """
    # 1) Choose the final renderer based on environment.
    renderer: Processor = (
        structlog.processors.JSONRenderer()
        if settings.is_prod
        else structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())
    )

    # 2) Configure structlog itself. The chain ends with
    #    ``ProcessorFormatter.wrap_for_formatter`` so the actual rendering is
    #    delegated to a stdlib formatter (so library logs render identically).
    structlog.configure(
        processors=[
            *_SHARED_PROCESSORS,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # 3) Build the stdlib formatter that renders BOTH structlog and foreign
    #    (plain ``logging``) records. ``foreign_pre_chain`` runs the shared
    #    processors on records that did NOT originate from structlog.
    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
        foreign_pre_chain=_SHARED_PROCESSORS,
    )

    # 4) Attach a single stdout handler with that formatter, replacing any
    #    pre-existing handlers (idempotency).
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.DEBUG if not settings.is_prod else logging.INFO)

    # Tame noisy third-party loggers in dev; they still flow through our handler.
    for noisy in ("uvicorn.access", "botocore", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Typed convenience wrapper around ``structlog.get_logger``.

    Usage: ``log = get_logger(__name__); log.info("job.accepted", job_id=jid)``.
    """
    return structlog.get_logger(name)
```

#### Walkthrough

- **One pipeline for everything.** By ending the structlog chain with `ProcessorFormatter.wrap_for_formatter` and giving the stdlib handler a `ProcessorFormatter` with `foreign_pre_chain`, both our events and a stray `logging.getLogger("botocore").warning(...)` render through the *same* renderer. In prod that means even third-party logs come out as JSON.
- **`merge_contextvars` first.** Phase 6/7 will `structlog.contextvars.bind_contextvars(job_id=..., request_id=...)`; this processor merges that bound context into every event automatically — so a single log statement deep in a pipeline carries the job id without threading it through call signatures.
- **Renderer selection is the only env-dependent line.** `JSONRenderer` for prod; `ConsoleRenderer` for dev (colorized only when stderr is a TTY, so piped/CI output stays plain).
- **Idempotent.** Each call clears root handlers and re-adds one. Tests can call it per case without accumulating duplicate handlers.

> [!TIP]
> Structured event names are *dotted, lowercase, verb-y*: `job.accepted`, `job.completed`, `offload.dispatched`, `retry.attempt`. Keep the message as the event *name* and put variable data in keyword fields (`job_id=...`, `attempt=...`). This makes logs trivially filterable (`level=error AND event=retry.exhausted`) in prod.

### 6.8 `.env.example`

A documented, committed stub. Real values go in an un-tracked `.env`. Later phases append their own variables.

```dotenv
# =============================================================================
# .env.example — copy to .env and adjust. NEVER commit a real .env.
# All variables are OPTIONAL; the defaults in src/app/core/config.py work for
# local dev. Nested groups use the double-underscore delimiter.
# =============================================================================

# --- Environment: dev | test | prod ---
AIE_ENV=dev

# --- Connection URLs (Phase 3/5; defaults target the compose services) ---
# AIE_DATABASE_URL=postgresql+asyncpg://aie:aie@localhost:5432/aie
# AIE_REDIS_URL=redis://localhost:6379/0

# --- Object store (Phase 4). Leave endpoint unset in dev → auto-redirects to
#     local MinIO (http://localhost:9000, path-style). Set it ONLY for real S3. ---
# AIE_OBJECT_STORE__BUCKET=aie-artifacts
# AIE_OBJECT_STORE__ENDPOINT_URL=
# AIE_OBJECT_STORE__ACCESS_KEY_ID=minioadmin
# AIE_OBJECT_STORE__SECRET_ACCESS_KEY=minioadmin

# --- Broker (Phase 5) ---
# AIE_BROKER__WORKER_CONCURRENCY=8
# AIE_BROKER__MAX_ATTEMPTS=3

# --- Retry (Phase 2). Set base delay to 0 in tests to count attempts. ---
# AIE_RETRY__MAX_ATTEMPTS=3
# AIE_RETRY__BASE_DELAY_S=0.2

# --- Concurrency ---
# AIE_OFFLOAD_MAX_WORKERS=32

# --- Auth (Phase 6): comma-separated accepted API keys ---
# AIE_API_KEYS=dev-key-1,dev-key-2

# --- Optional provider secrets. Set to activate REAL adapters (else fakes). ---
# AIE_HUGGINGFACE_TOKEN=hf_xxx
# AIE_PINECONE_API_KEY=pc_xxx
```

### 6.9 `tests/conftest.py`

Base fixtures shared by the whole suite. Phase 1 needs only configuration helpers (the domain tests need no fixtures at all — they construct entities directly). Later phases extend this file with container, db, and redis fixtures.

```python
"""Shared pytest fixtures for the whole test suite.

Phase 1 provides only configuration helpers. The domain tests need NO fixtures
(they construct ``InferenceJob`` directly). Later phases add container/db/redis
fixtures here. Everything stays deterministic and clock-free — no fixture sleeps.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from app.core.config import Environment, Settings, get_settings


@pytest.fixture
def dev_settings() -> Settings:
    """A fully-defaulted ``dev`` Settings instance.

    Because every field has a default (or a default_factory), constructing
    ``Settings()`` with an empty-ish environment is valid and exercises the
    zero-cloud redirect (dev + no endpoint → MinIO).
    """
    # _env_file=None prevents a developer's local .env from leaking into tests,
    # keeping the fixture hermetic and reproducible across machines.
    return Settings(env=Environment.DEV, _env_file=None)  # type: ignore[call-arg]


@pytest.fixture(autouse=True)
def _clear_settings_cache() -> Iterator[None]:
    """Ensure ``get_settings()``'s lru_cache never bleeds between tests."""
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
```

> [!NOTE]
> `_env_file=None` is a pydantic-settings runtime override that disables `.env` loading for that instance, so a contributor's local `.env` can never make tests pass/fail differently on their machine vs CI. The `# type: ignore[call-arg]` is because the dynamic `_env_file` kwarg isn't in the statically-synthesized `__init__` signature; it is the documented escape hatch and is the single sanctioned ignore in the test suite.

> [!IMPORTANT]
> The `_clear_settings_cache` fixture is `autouse=True` so it runs around **every** test, guaranteeing the process-wide `get_settings()` cache cannot carry a stale `Settings` from one test into another. This is the kind of hidden coupling that produces "passes alone, fails in the suite" flakes — we eliminate it structurally.

### 6.10 `tests/unit/test_domain.py`

The exhaustive transition-matrix test plus a happy-path lifecycle test. This is where the state machine's correctness is *proven*, not asserted by inspection.

```python
"""Unit tests for the domain state machine. Pure, deterministic, no fixtures.

The headline test iterates the FULL Cartesian product of (start_status,
transition) and asserts each pair either performs the legal move or raises
``InvalidTransition`` — leaving no transition unverified.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable

import pytest

from app.domain import InferenceJob, InvalidTransition, JobStatus, JobType


def _job_in(status: JobStatus) -> InferenceJob:
    """Construct a job forced into ``status`` (bypassing guards for setup only)."""
    job = InferenceJob(job_type=JobType.RAG_QUERY, payload={"q": "hi"})
    # object.__setattr__ works on slotted dataclasses; used ONLY to arrange the
    # precondition for the matrix test, never in production code.
    object.__setattr__(job, "status", status)
    return job


# Map each transition name to (the method invocation, the resulting status).
_TRANSITIONS: dict[str, tuple[Callable[[InferenceJob], None], JobStatus]] = {
    "mark_running": (lambda j: j.mark_running(), JobStatus.RUNNING),
    "mark_success": (lambda j: j.mark_success("s3://b/k"), JobStatus.SUCCESS),
    "mark_failed": (lambda j: j.mark_failed("boom"), JobStatus.FAILED),
    "requeue": (lambda j: j.requeue(), JobStatus.PENDING),
}

# The legal (start, transition) pairs — the SINGLE expected truth table. Any
# pair NOT listed here must raise InvalidTransition.
_LEGAL: set[tuple[JobStatus, str]] = {
    (JobStatus.PENDING, "mark_running"),
    (JobStatus.RUNNING, "mark_success"),
    (JobStatus.RUNNING, "mark_failed"),
    (JobStatus.RUNNING, "requeue"),
}


@pytest.mark.parametrize("start", list(JobStatus))
@pytest.mark.parametrize("name", list(_TRANSITIONS))
def test_transition_matrix(start: JobStatus, name: str) -> None:
    """Every (start, transition) pair is either legal (moves) or raises."""
    invoke, target = _TRANSITIONS[name]
    job = _job_in(start)

    if (start, name) in _LEGAL:
        invoke(job)
        assert job.status is target
    else:
        with pytest.raises(InvalidTransition) as ei:
            invoke(job)
        # Status is unchanged on an illegal attempt.
        assert job.status is start
        # The exception carries the offending pair for a readable message.
        assert ei.value.current == start


def test_happy_path_lifecycle_rag_query() -> None:
    """PENDING -> RUNNING -> SUCCESS, with attempts/result/error bookkeeping."""
    job = InferenceJob(job_type=JobType.RAG_QUERY, payload={"q": "what is X?"})
    assert job.status is JobStatus.PENDING
    assert job.attempts == 0
    assert not job.is_terminal

    job.mark_running()
    assert job.status is JobStatus.RUNNING
    assert job.attempts == 1            # incremented on entering RUNNING

    job.mark_success("s3://aie-artifacts/results/abc.json")
    assert job.status is JobStatus.SUCCESS
    assert job.is_terminal
    assert job.result_ref == "s3://aie-artifacts/results/abc.json"
    assert job.error is None


def test_retry_path_requeue_then_succeed() -> None:
    """A transient failure path: RUNNING -> PENDING -> RUNNING -> SUCCESS."""
    job = InferenceJob(job_type=JobType.EMBED_DOCUMENT, payload={"doc": "..."})
    job.mark_running()                  # attempt 1
    assert job.attempts == 1

    job.requeue()                       # transient failure → back to PENDING
    assert job.status is JobStatus.PENDING
    assert job.attempts == 1            # requeue does NOT bump attempts

    job.mark_running()                  # attempt 2
    assert job.attempts == 2
    job.mark_success("s3://aie-artifacts/embeddings/xyz.json")
    assert job.is_terminal


def test_mark_running_clears_previous_error() -> None:
    """A fresh attempt clears the error recorded by a prior failed run."""
    job = InferenceJob(job_type=JobType.RAG_QUERY, payload={})
    job.mark_running()
    # Simulate a transient failure that recorded an error, then a requeue.
    object.__setattr__(job, "error", "timeout")
    job.requeue()
    job.mark_running()                  # attempt 2
    assert job.error is None            # cleared on re-entry to RUNNING


def test_terminal_states_reject_all_transitions() -> None:
    """SUCCESS and FAILED are dead ends — every transition raises."""
    for terminal in (JobStatus.SUCCESS, JobStatus.FAILED):
        for invoke, _ in _TRANSITIONS.values():
            job = _job_in(terminal)
            with pytest.raises(InvalidTransition):
                invoke(job)


def test_updated_at_advances_monotonically_without_sleep() -> None:
    """``updated_at`` is bumped on each transition — asserted by ORDERING, not time.

    We do NOT sleep. We patch the module clock to return a strictly increasing
    sequence, proving the field is refreshed on every guarded transition.
    """
    import app.domain.models as models

    ticks = iter(
        [
            models.datetime(2024, 1, 1, 0, 0, 0, tzinfo=models.UTC),  # created_at
            models.datetime(2024, 1, 1, 0, 0, 0, tzinfo=models.UTC),  # updated_at (init)
            models.datetime(2024, 1, 1, 0, 0, 1, tzinfo=models.UTC),  # mark_running
            models.datetime(2024, 1, 1, 0, 0, 2, tzinfo=models.UTC),  # mark_success
        ]
    )
    original = models._utcnow
    models._utcnow = lambda: next(ticks)  # type: ignore[assignment]
    try:
        job = InferenceJob(job_type=JobType.RAG_QUERY, payload={})
        t0 = job.updated_at
        job.mark_running()
        t1 = job.updated_at
        job.mark_success("s3://b/k")
        t2 = job.updated_at
    finally:
        models._utcnow = original  # type: ignore[assignment]

    assert t0 < t1 < t2            # strictly increasing, no wall-clock dependency
```

#### Why this is the right test design

- **Exhaustive by construction.** The outer product is `len(JobStatus) × len(_TRANSITIONS)` = 4 × 4 = 16 cases. Each is classified by membership in `_LEGAL`. If someone widens `_ALLOWED` in `models.py` without updating `_LEGAL`, a previously-illegal case now performs a move and the matrix test fails — the test and the code can't silently drift apart.
- **Negative assertions check *no* mutation.** On an illegal transition we assert `job.status is start` — proving the guard rejects *before* mutating, not after.
- **Clock-free.** The only time-related test patches `_utcnow` with a deterministic iterator and asserts *ordering*. There is no `sleep`, no `freezegun`, no wall-clock comparison — fully reproducible and instant.

> [!TIP]
> `object.__setattr__(job, "status", ...)` is used **only in test setup** to arrange a precondition (a job already in `RUNNING`/`SUCCESS`/etc.) without driving it there through the guards. Never do this in production code — it bypasses the very invariants the entity exists to protect. Keeping it confined to a `_job_in` helper makes the intent obvious in review.

### 6.11 `tests/unit/test_config.py`

Unit tests for `Settings`: env parsing, nested-group population, the `api_keys` frozenset, and — the headline — the **zero-cloud redirect matrix**. All pure and offline.

```python
"""Unit tests for Settings — env parsing, nesting, and the zero-cloud redirect.

These are pure unit tests: no network, no Docker, no event loop. We pass env
via monkeypatch and construct Settings(_env_file=None) so a developer's local
.env cannot influence the result.
"""

from __future__ import annotations

import pytest

from app.core.config import Environment, Settings

# Tests set env via monkeypatch.setenv (auto-reverted per test) and always
# construct Settings(_env_file=None) so a developer's local .env can never
# influence the result. There is no shared builder helper — each test makes the
# exact Settings it needs, which keeps the cause of any failure obvious.


# --- Defaults & the zero-cloud redirect ------------------------------------
@pytest.mark.parametrize(
    ("env_value", "expect_endpoint", "expect_path_style"),
    [
        (Environment.DEV, "http://localhost:9000", True),   # dev → MinIO
        (Environment.TEST, "http://localhost:9000", True),  # test → MinIO
        (Environment.PROD, None, False),                    # prod → untouched
    ],
)
def test_zero_cloud_redirect_matrix(
    env_value: Environment,
    expect_endpoint: str | None,
    expect_path_style: bool,
) -> None:
    """Non-prod with no endpoint forces MinIO + path-style; prod is left alone."""
    settings = Settings(env=env_value, _env_file=None)  # type: ignore[call-arg]
    assert settings.object_store.endpoint_url == expect_endpoint
    assert settings.object_store.force_path_style is expect_path_style


def test_explicit_endpoint_is_respected_even_in_dev() -> None:
    """If an operator sets an explicit S3 endpoint, the redirect must NOT override it."""
    settings = Settings(
        env=Environment.DEV,
        object_store={"endpoint_url": "https://s3.eu-west-1.amazonaws.com"},
        _env_file=None,
    )  # type: ignore[call-arg]
    assert settings.object_store.endpoint_url == "https://s3.eu-west-1.amazonaws.com"
    # force_path_style stays at its default (False) because the branch didn't run.
    assert settings.object_store.force_path_style is False


# --- Env parsing & prefixing -----------------------------------------------
def test_env_prefix_and_flat_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    """AIE_-prefixed flat env vars populate top-level fields."""
    monkeypatch.setenv("AIE_ENV", "prod")
    monkeypatch.setenv(
        "AIE_DATABASE_URL", "postgresql+asyncpg://u:p@db:5432/x"
    )
    monkeypatch.setenv("AIE_OFFLOAD_MAX_WORKERS", "64")
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.env is Environment.PROD
    assert settings.is_prod is True
    assert settings.database_url == "postgresql+asyncpg://u:p@db:5432/x"
    assert settings.offload_max_workers == 64


def test_nested_delimiter_populates_groups(monkeypatch: pytest.MonkeyPatch) -> None:
    """AIE_<GROUP>__<FIELD> populates nested settings groups via '__'."""
    monkeypatch.setenv("AIE_BROKER__WORKER_CONCURRENCY", "16")
    monkeypatch.setenv("AIE_BROKER__MAX_ATTEMPTS", "5")
    monkeypatch.setenv("AIE_RETRY__BASE_DELAY_S", "0")
    monkeypatch.setenv("AIE_OBJECT_STORE__BUCKET", "custom-bucket")
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.broker.worker_concurrency == 16
    assert settings.broker.max_attempts == 5
    assert settings.retry.base_delay_s == 0.0      # tests set this to count attempts
    assert settings.object_store.bucket == "custom-bucket"


def test_api_keys_parsed_into_frozenset(monkeypatch: pytest.MonkeyPatch) -> None:
    """Comma-separated AIE_API_KEYS parses into an immutable, deduplicated set."""
    monkeypatch.setenv("AIE_API_KEYS", "k1,k2,k2,k3")
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.api_keys == frozenset({"k1", "k2", "k3"})
    assert isinstance(settings.api_keys, frozenset)


def test_provider_secrets_are_masked(monkeypatch: pytest.MonkeyPatch) -> None:
    """SecretStr secrets never appear in repr()/str() of the value."""
    monkeypatch.setenv("AIE_HUGGINGFACE_TOKEN", "hf_supersecret")
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.huggingface_token is not None
    # The secret value is retrievable explicitly but hidden in repr.
    assert settings.huggingface_token.get_secret_value() == "hf_supersecret"
    assert "hf_supersecret" not in repr(settings.huggingface_token)


def test_unknown_env_vars_are_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    """extra='ignore' lets unrelated process env vars coexist without error."""
    monkeypatch.setenv("AIE_TOTALLY_UNKNOWN", "whatever")
    # Should not raise despite the unknown AIE_-prefixed var.
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.env is Environment.DEV
```

> [!NOTE]
> Every settings test uses `monkeypatch.setenv` (which sets the process env that pydantic-settings reads) plus `Settings(_env_file=None)`. Using `monkeypatch` guarantees env mutations are *undone automatically* after each test, so cases can't leak environment into one another — another structural defense against suite-order flakes. There is deliberately no shared `Settings`-builder helper: each test constructs exactly the configuration it asserts on, so a failure points straight at its cause.

> [!WARNING]
> Notice `object_store={"endpoint_url": ...}` is passed as a **dict** in `test_explicit_endpoint_is_respected_even_in_dev`. pydantic coerces the dict into an `ObjectStoreSettings`. Passing a pre-built `ObjectStoreSettings(...)` works too; the dict form is terser for tests. Either way, the `init_forbid_extra = true` pydantic-mypy setting means a typo'd key (e.g. `endpont_url`) is a *type error*, caught by `poe typecheck` before the test ever runs.

---

## 7. Flow & Sequence Diagrams

### 7.1 Job lifecycle state machine

The canonical lifecycle this phase encodes. Every guarded method maps to exactly one edge; terminal states have no outgoing edges. This diagram and the `_ALLOWED` map in §6.5 are the same truth expressed two ways.

```mermaid
stateDiagram-v2
    direction LR
    [*] --> PENDING : create (attempts=0)

    PENDING --> RUNNING : mark_running()<br/>attempts += 1 · error = None

    RUNNING --> SUCCESS : mark_success(result_ref)
    RUNNING --> FAILED  : mark_failed(error)
    RUNNING --> PENDING : requeue()<br/><i>transient retry · attempts unchanged</i>

    SUCCESS --> [*]
    FAILED  --> [*]

    note right of SUCCESS
        Terminal. is_terminal == True.
        Idempotency guard (Phase 5/7)
        ack-and-skips re-deliveries.
    end note
    note right of FAILED
        Terminal. Reached on permanent
        failure OR exhausted retry budget
        (broker → DLQ, Phase 5).
    end note
```

### 7.2 How a transition method enforces the invariant

The internal control flow of any guarded transition (e.g. `mark_success`). The guard rejects *before* mutating, which is what the negative matrix assertions verify.

```mermaid
flowchart TD
    A["caller invokes<br/>job.mark_success('s3://...')"] --> B["_require(SUCCESS)"]
    B --> C{"SUCCESS in<br/>_ALLOWED[self.status]?"}
    C -- no --> D["raise InvalidTransition(<br/>current, SUCCESS)"]
    D --> E["status UNCHANGED<br/>caller handles error"]
    C -- yes --> F["self.status = SUCCESS<br/>self.updated_at = _utcnow()"]
    F --> G["self.result_ref = 's3://...'"]
    G --> H["return None"]
```

### 7.3 Settings load + zero-cloud redirect (one-time, at startup)

How `get_settings()` resolves a value and the after-validator forces MinIO outside prod. This runs once per process in the composition root (Phase 6).

```mermaid
sequenceDiagram
    autonumber
    participant Root as Composition root (Phase 6)
    participant GS as get_settings()
    participant PS as pydantic-settings
    participant Env as Process env / .env
    participant MV as _force_minio_outside_prod (after-validator)

    Root->>GS: get_settings()
    Note over GS: @lru_cache → built once per process
    GS->>PS: Settings()
    PS->>Env: read AIE_*-prefixed vars
    Env-->>PS: env=dev, object_store.endpoint_url unset, ...
    PS->>PS: parse + validate fields & nested groups
    PS->>MV: run after-validator(self)
    alt env != prod AND endpoint_url is None
        MV->>MV: endpoint_url = http://localhost:9000<br/>force_path_style = True
    else prod OR endpoint already set
        MV->>MV: leave object_store untouched
    end
    MV-->>PS: return self
    PS-->>GS: validated Settings
    GS-->>Root: cached Settings (injected everywhere)
```

> [!NOTE]
> The sequence diagram makes the **timing** explicit: the redirect happens during validation, *before* any adapter is constructed. By the time Phase 4's `S3ObjectStore` reads `settings.object_store.endpoint_url`, it is already `http://localhost:9000` in dev. There is no second place where the decision could be made (and forgotten).

---

## 8. Configuration & Environment

All settings derive from `Settings` in `core/config.py`. Environment variables are prefixed `AIE_`; nested groups use the `__` delimiter. The table lists every variable Phase 1 *defines* (later phases consume most of them).

| Env var | Maps to | Default | Used by (phase) | Notes |
|---------|---------|---------|-----------------|-------|
| `AIE_ENV` | `Settings.env` | `dev` | core/config, logging | `dev`/`test`/`prod`; drives redirect + log format |
| `AIE_DATABASE_URL` | `Settings.database_url` | `postgresql+asyncpg://aie:aie@localhost:5432/aie` | 3, 5 | Must be the **asyncpg** driver URL |
| `AIE_REDIS_URL` | `Settings.redis_url` | `redis://localhost:6379/0` | 5 | redis-py asyncio URL |
| `AIE_OBJECT_STORE__BUCKET` | `object_store.bucket` | `aie-artifacts` | 4 | Bucket for job artifacts |
| `AIE_OBJECT_STORE__ENDPOINT_URL` | `object_store.endpoint_url` | `None` → MinIO outside prod | 4 | Leave unset in dev/test; set only for real S3 |
| `AIE_OBJECT_STORE__FORCE_PATH_STYLE` | `object_store.force_path_style` | `False` → `True` outside prod | 4 | MinIO requires path-style |
| `AIE_OBJECT_STORE__REGION` | `object_store.region` | `us-east-1` | 4 | S3 region |
| `AIE_OBJECT_STORE__ACCESS_KEY_ID` | `object_store.access_key_id` | `None` | 4 | `SecretStr`; MinIO default `minioadmin` |
| `AIE_OBJECT_STORE__SECRET_ACCESS_KEY` | `object_store.secret_access_key` | `None` | 4 | `SecretStr` |
| `AIE_BROKER__STREAM` | `broker.stream` | `aie:jobs` | 5 | Redis stream key |
| `AIE_BROKER__GROUP` | `broker.group` | `aie-workers` | 5 | Consumer group |
| `AIE_BROKER__DLQ` | `broker.dlq` | `aie:jobs:dlq` | 5 | Dead-letter stream |
| `AIE_BROKER__MAX_ATTEMPTS` | `broker.max_attempts` | `3` | 5 | Retry budget before DLQ |
| `AIE_BROKER__BLOCK_MS` | `broker.block_ms` | `5000` | 5 | `XREADGROUP` block timeout |
| `AIE_BROKER__RECLAIM_IDLE_MS` | `broker.reclaim_idle_ms` | `60000` | 5 | `XAUTOCLAIM` idle threshold |
| `AIE_BROKER__WORKER_CONCURRENCY` | `broker.worker_concurrency` | `8` | 5 | Semaphore size (backpressure) |
| `AIE_BROKER__MAX_DELIVERY_COUNT` | `broker.max_delivery_count` | `5` | 5 | Reclaim bounces before DLQ |
| `AIE_BROKER__MAXLEN` | `broker.maxlen` | `10000` | 5 | Approx. `XADD MAXLEN` trim cap |
| `AIE_RETRY__MAX_ATTEMPTS` | `retry.max_attempts` | `3` | 2 | `stop_after_attempt` count |
| `AIE_RETRY__BASE_DELAY_S` | `retry.base_delay_s` | `0.2` | 2 | `wait_exponential_jitter(initial=)`; **set `0` in tests** |
| `AIE_RETRY__MAX_DELAY_S` | `retry.max_delay_s` | `10.0` | 2 | `wait_exponential_jitter(max=)` |
| `AIE_RETRY__EXP_BASE` | `retry.exp_base` | `2.0` | 2 | `wait_exponential_jitter(exp_base=)` |
| `AIE_RETRY__JITTER_S` | `retry.jitter_s` | `1.0` | 2 | `wait_exponential_jitter(jitter=)` |
| `AIE_OFFLOAD_MAX_WORKERS` | `Settings.offload_max_workers` | `32` | 2, 6 | `ThreadPoolExecutor` size (loop default executor) |
| `AIE_API_KEYS` | `Settings.api_keys` | `frozenset()` | 6 | Comma-separated; parsed to `frozenset[str]` |
| `AIE_HUGGINGFACE_TOKEN` | `Settings.huggingface_token` | `None` | 4, 6 | `SecretStr`; set → real HF adapter |
| `AIE_PINECONE_API_KEY` | `Settings.pinecone_api_key` | `None` | 4, 6 | `SecretStr`; set → real Pinecone adapter |

> [!IMPORTANT]
> **`AIE_RETRY__BASE_DELAY_S=0`** is the single most important *test-time* env value in the whole project. Setting it to zero makes `tenacity`'s backoff instantaneous, so retry tests (Phase 2/4) can assert *how many times* a function was called rather than *how long* it took. This is the operational form of the "deterministic, clock-free" exit criterion. The default (`0.2`) is for real runs.

> [!TIP]
> The driver scheme in `AIE_DATABASE_URL` (`postgresql+asyncpg://`) is non-negotiable: SQLAlchemy selects the async asyncpg driver from that scheme. A plain `postgres://` or `postgresql://` URL would pick a sync driver and break the async engine (Phase 3). The default already encodes the correct scheme so a fresh clone works.

---

## 9. Testing Strategy

Phase 1's tests are **pure unit tests** — no event loop, no Docker, no network. They run in milliseconds and are the model for the project's testing discipline. There are no `integration`-marked tests in this phase.

### What we test and how

| Target | Test file | Technique | Determinism guarantee |
|--------|-----------|-----------|------------------------|
| State machine (all 16 transition pairs) | `test_domain.py::test_transition_matrix` | Parametrized Cartesian product vs an explicit `_LEGAL` truth table | No clock, no I/O; pure method calls |
| Happy path + retry path | `test_domain.py` (lifecycle/retry tests) | Drive the entity through legal sequences; assert `attempts`/`result_ref`/`error` | Pure |
| Terminal dead-ends | `test_domain.py::test_terminal_states_reject_all_transitions` | Assert every transition from `SUCCESS`/`FAILED` raises | Pure |
| `updated_at` advances | `test_domain.py::test_updated_at_advances_monotonically_without_sleep` | Patch `_utcnow` with an increasing iterator; assert ordering | **No sleep** — ordering, not wall-clock |
| Zero-cloud redirect | `test_config.py::test_zero_cloud_redirect_matrix` | Parametrize `dev/test/prod`; assert endpoint + path-style | Pure; offline |
| Explicit endpoint respected | `test_config.py::test_explicit_endpoint_is_respected_even_in_dev` | Pass explicit endpoint in dev; assert not overridden | Pure |
| Env prefix / flat fields | `test_config.py::test_env_prefix_and_flat_fields` | `monkeypatch.setenv` + `_env_file=None` | Hermetic (monkeypatch auto-undo) |
| Nested `__` delimiter | `test_config.py::test_nested_delimiter_populates_groups` | `monkeypatch.setenv` of `AIE_GROUP__FIELD` | Hermetic |
| `api_keys` frozenset parse | `test_config.py::test_api_keys_parsed_into_frozenset` | Comma-separated env → dedup frozenset | Pure |
| Secret masking | `test_config.py::test_provider_secrets_are_masked` | Assert value hidden in `repr`, retrievable via `get_secret_value()` | Pure |
| `extra='ignore'` | `test_config.py::test_unknown_env_vars_are_ignored` | Set an unknown var; assert no raise | Hermetic |

### Deterministic, clock-free principles demonstrated here

1. **Truth-table parametrization over assertions-by-eye.** The transition matrix derives every case mechanically and classifies it against one `_LEGAL` set. Coverage is total and drift-resistant.
2. **Ordering, never duration.** The only time-aware test patches the clock and asserts `t0 < t1 < t2`. We never assert "took less than N ms" or call `sleep`.
3. **Hermetic environment.** Every settings test passes `_env_file=None` and mutates env via `monkeypatch` (auto-reverted). The `autouse` cache-clear fixture (§6.9) prevents `get_settings()` from leaking state across tests.
4. **No fixtures for the domain.** The domain is pure, so its tests construct entities inline — the strongest possible evidence that the domain has no hidden dependencies.

> [!IMPORTANT]
> These principles are not Phase-1-specific niceties; they are the *template* every later phase follows. Phase 2 proves offloading with a `RecordingOffloader` spy (not timing). Phase 5 gates drain tests on an `asyncio.Event` (not `sleep`). Phase 6 proves no leaks via `closed` flags (not process inspection). Establishing the discipline here, on the simplest code, makes it non-negotiable everywhere.

Run just this phase's tests:

```powershell
# All Phase 1 unit tests
uv run pytest tests/unit/test_domain.py tests/unit/test_config.py -v

# A single test
uv run pytest tests/unit/test_domain.py::test_transition_matrix -v

# With coverage of the domain + config modules
uv run pytest tests/unit -v --cov=app.domain --cov=app.core --cov-report=term-missing
```

---

## 10. Verification & Exit-Criteria Mapping

The canonical verification for Phase 1 is the project's main gate:

```powershell
uv sync                 # resolve + install all deps into .venv, write/refresh uv.lock
uv run poe check        # fmt → lint → typecheck → unit tests, in order; stops on first failure
```

A green `poe check` means: the tree is formatted (LF endings), lint-clean, passes `mypy --strict` (with the pydantic plugin), and all unit tests pass. That is the bar for the phase.

### Mapping to the spec's exit criteria

| Spec exit criterion | How Phase 1 contributes / proves it | Command / test file |
|---------------------|-------------------------------------|---------------------|
| **Zero-Cloud Isolation** (dev auto-redirects S3 → MinIO) | The `model_validator` forces `http://localhost:9000` + path-style outside prod; proven by a parametrized matrix and an "explicit endpoint respected" test | `tests/unit/test_config.py::test_zero_cloud_redirect_matrix` (+ `…respected_even_in_dev`) |
| **Deterministic concurrency gates** (clock-free tests) | Establishes the discipline: transition matrix and `updated_at` ordering test use *no* sleeps/clocks; retry `base_delay_s=0` knob defined for downstream | `tests/unit/test_domain.py`; `RetrySettings.base_delay_s` |
| **Zero resource leaking** (clean startup/teardown) | Not exercised here (no resources yet) — but `Settings`/`logging` are designed for *injection* (no module globals), so the Phase 6 container can own and close everything | (proven in Phase 6) |
| **Framework-agnostic core** (no global singletons; core never imports FastAPI) | `domain/` and `core/` import nothing from FastAPI/ORM/broker/SDKs; `get_settings()` is injected, not imported as a global | `mypy` import graph; code review; `ban-relative-imports` |
| **Strict typing & quality bar** | `mypy --strict` + pydantic plugin + ruff (incl. ASYNC, bugbear) all green | `uv run poe check` |

> [!NOTE]
> Phase 1 does not *complete* any of the three headline runtime exit criteria on its own — that is expected, because there is no runtime yet. What it does is **make them achievable**: the redirect validator is the entire mechanism behind zero-cloud isolation; the clock-free test patterns are the entire mechanism behind deterministic gates; and the no-global-singleton config design is what lets the Phase 6 container guarantee no leaks. The full traceability table lives at the end of [`implementation-plan.md`](../implementation-plan.md).

---

## 11. Windows & Cross-Platform Notes

This repository is developed on **Windows 11** (the checkout path even contains a space: `D:\Study supply\projects\async-llm-inference`) while production runs on Linux containers. Phase 1 handles the Windows-specific concerns that surface this early.

> [!WARNING]
> **Line endings (CRLF) are the #1 Windows footgun for a Python repo.** Without intervention, Git on Windows checks out files with `\r\n`, then `ruff format` (configured `line-ending = "lf"`) rewrites them to `\n`, and every file shows as "modified". Phase 1 fixes this at two layers: `.gitattributes` (`* text=auto eol=lf`) makes Git *store* and *check out* LF, and ruff's `line-ending = "lf"` makes the formatter *emit* LF. Both are required — `.gitattributes` governs Git, ruff governs the formatter.

| Concern | Phase-1 mitigation | Where |
|---------|--------------------|-------|
| CRLF vs LF | `.gitattributes` (`* text=auto eol=lf`) + ruff `[tool.ruff.format] line-ending = "lf"` | §6.1, §6.2 |
| Path with a space (`Study supply`) | No bind mounts in Phase 1; `poe smoke` invokes `pwsh -File scripts/smoke.ps1` (a path-safe call). Later phases use **named volumes**, not bind mounts | §6.1 |
| `uvloop` unavailable on Windows | `uvicorn[standard]`'s uvloop extra is platform-gated; it's simply absent locally and activates only in the Linux container. Nothing in Phase 1 imports uvloop | (Phase 6/8) |
| ProactorEventLoop default | asyncpg + redis-py asyncio both work on Proactor (the reason asyncpg was chosen over psycopg-async). Phase 1 has no event loop, so nothing to do yet | (Phase 3/5) |
| `loop.add_signal_handler` → `NotImplementedError` on Windows | Worker signal handling has a `signal.signal` fallback — implemented in Phase 7, *not* here | (Phase 7) |
| Shell divergence | One `poe` task table instead of `Makefile` + `.ps1`. PowerShell users run `uv run poe <task>` identically to CI's Linux runner | §6.1 |

> [!TIP]
> If you ever see a "whole file changed" diff after running `poe fmt`, your working tree was checked out before `.gitattributes` took effect. Re-normalize once:
> ```powershell
> git add --renormalize .
> git commit -m "Normalize line endings to LF"
> ```
> After that, the `.gitattributes` + ruff combination keeps it LF forever.

> [!NOTE]
> `poe smoke` calls `pwsh` (PowerShell 7+). On a machine with only Windows PowerShell 5.1, either install PowerShell 7 (`winget install Microsoft.PowerShell`) or change the task to `powershell -File ...`. The smoke script itself is authored in [Phase 8](phase-8-containerization-compose.md); the task alias is registered now so the vocabulary is stable.

---

## 12. Common Pitfalls & Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `ModuleNotFoundError: No module named 'app'` in tests | `src` layout not installed; ran `pytest` outside the uv env | Run via `uv run pytest` (uv installs the package editable into `.venv`); confirm `[tool.hatch.build.targets.wheel] packages = ["src/app"]` |
| `poe: command not found` | poethepoet not installed / not in the env | `uv sync` (installs the `dev` group), then `uv run poe ...` |
| Every file shows as modified after `poe fmt` | CRLF checkout before `.gitattributes` | `git add --renormalize .` once (see §11) |
| mypy: `Unexpected keyword argument "endpoint_url" for "ObjectStoreSettings"` | Typo'd field name; pydantic-mypy `init_forbid_extra=true` is doing its job | Fix the field name — the type checker caught a bug before runtime |
| `pydantic_core.ValidationError: extra fields not permitted` at startup | A nested group set `extra="forbid"` and got an unknown key | Top-level `Settings` uses `extra="ignore"`; check you didn't typo a nested env var name (`AIE_BROKR__...`) |
| Redirect didn't fire; app tried real S3 in dev | An explicit `AIE_OBJECT_STORE__ENDPOINT_URL` was set (even empty-but-present can parse oddly), or `AIE_ENV=prod` | Unset the endpoint env var in dev; confirm `AIE_ENV=dev`; the matrix test documents the exact rule |
| Redirect fired in prod | `AIE_ENV` not actually `prod` (e.g. typo `production`) | `Environment` is a closed `StrEnum`; an invalid value raises at load — set exactly `prod` |
| `mark_success` raised `InvalidTransition` unexpectedly | Job wasn't in `RUNNING` (e.g. tried to succeed a `PENDING` job) | Call `mark_running()` first; this is the guard working as designed |
| `attempts` is one higher than expected after a retry | Misremembering: `mark_running` bumps `attempts`, `requeue` does not | This is intentional — `attempts` counts entries into `RUNNING`; see §6.5 |
| Secret value appeared in a log line | Logged `settings` with a plain renderer that called `str()` on the field, or used `.get_secret_value()` in a log | Log `SecretStr` fields by *name only*; never call `get_secret_value()` into a log; `repr` already masks |
| Tests pass alone but fail in the suite | A test mutated process env or the settings cache without cleanup | Use `monkeypatch.setenv` (auto-undo) and rely on the `autouse` cache-clear fixture; never `os.environ[...] = ...` directly |
| `ruff check` flags `ASYNC`/`B` rules in later phases | Selected rule families are stricter than black/flake8 defaults | Address the lint (they catch real bugs); use a targeted `# noqa: CODE` with a reason only when truly necessary |

> [!CAUTION]
> Do **not** "fix" an `InvalidTransition` by widening `_ALLOWED` to make a failing test pass. If a test legitimately needs a new edge, that is a *domain rule change* — update the §7 diagram, the `_ALLOWED` map, **and** the `_LEGAL` truth table together, and get it reviewed. The matrix test exists precisely to make such changes loud and deliberate.

---

## 13. Definition of Done

Tick every box before declaring Phase 1 complete.

- [ ] `pyproject.toml` exists with `requires-python = ">=3.12"`, the full runtime dependency closure, and the `dev` dependency group.
- [ ] `[tool.ruff]` configured: `line-length`, `target-version = "py312"`, lint `select` (incl. `ASYNC`, `B`), and `[tool.ruff.format] line-ending = "lf"`.
- [ ] `[tool.mypy]` has `strict = true` and `plugins = ["pydantic.mypy"]`; `[tool.pydantic-mypy]` sets `init_forbid_extra`/`init_typed`.
- [ ] `[tool.pytest.ini_options]` has `asyncio_mode = "auto"`, `--strict-markers`, and the registered `integration` marker.
- [ ] `[tool.poe.tasks]` defines: `fmt`, `lint`, `typecheck`, `test`, `test-int`, `check`, `api`, `worker`, `up`, `down`, `migrate`, `smoke`.
- [ ] `.python-version` pins `3.12`; `.gitattributes` has `* text=auto eol=lf`; `.gitignore` ignores `.env` but tracks `.env.example`.
- [ ] `src/app/core/config.py`: `Settings(BaseSettings)` with `env_prefix="AIE_"`, `env_nested_delimiter="__"`, nested `ObjectStoreSettings`/`BrokerSettings`/`RetrySettings`, `offload_max_workers`, `api_keys: frozenset[str]`, optional `SecretStr` provider secrets, and the `Environment` StrEnum.
- [ ] The zero-cloud `@model_validator(mode="after")` forces MinIO + path-style when `env != prod` and `endpoint_url is None`, and leaves explicit endpoints / prod untouched.
- [ ] `src/app/core/logging.py`: `configure_logging(settings)` renders JSON in prod / console in dev, bound to stdlib logging; idempotent.
- [ ] `src/app/domain/models.py`: `InferenceJob` `@dataclass(slots=True)` with `JobStatus`/`JobType` StrEnums and guarded `mark_running`/`mark_success`/`mark_failed`/`requeue`; illegal moves raise `InvalidTransition`; `is_terminal` property; `attempts` bumped on `mark_running` only.
- [ ] `src/app/domain/exceptions.py`: `DomainError` → `InvalidTransition`, and `TransientUpstreamError`/`PermanentUpstreamError` under an `UpstreamError` base.
- [ ] `tests/conftest.py`: `dev_settings` fixture + autouse `get_settings` cache-clear; `_env_file=None` keeps tests hermetic.
- [ ] `tests/unit/test_domain.py`: exhaustive transition matrix (every illegal move raises, status unchanged) + happy-path/retry/terminal/`updated_at`-ordering tests.
- [ ] `tests/unit/test_config.py`: redirect matrix + explicit-endpoint + env-prefix + nested-delimiter + `api_keys` frozenset + secret-masking + extra-ignore tests.
- [ ] Domain and core import **nothing** from FastAPI/SQLAlchemy/Redis/boto3 (verified by reading imports + `mypy` graph).
- [ ] `uv sync` succeeds and produces/refreshes `uv.lock`.
- [ ] `uv run poe check` is green end-to-end (format clean, lint clean, `mypy --strict` clean, unit tests pass).

---

## 14. References & Further Reading

All code in this phase was grounded against the following authoritative sources (verified current at time of writing).

**Packaging & tooling**
- uv — Projects & init: <https://docs.astral.sh/uv/concepts/projects/init/>
- uv — Dependencies & dependency groups (PEP 735): <https://docs.astral.sh/uv/concepts/projects/dependencies/>
- Hatchling — build backend / `tool.hatch.build.targets.wheel`: <https://hatch.pypa.io/latest/config/build/>
- PEP 735 — Dependency Groups: <https://peps.python.org/pep-0735/>
- poethepoet — task runner: <https://poethepoet.natn.io/>

**Ruff**
- Configuration reference (`[tool.ruff]`, `lint`, `format`, `line-ending`): <https://docs.astral.sh/ruff/settings/>
- Rule index (E/W/F/I/N/UP/B/C4/SIM/RUF/TID/ASYNC): <https://docs.astral.sh/ruff/rules/>
- Formatter: <https://docs.astral.sh/ruff/formatter/>

**Mypy & pydantic plugin**
- mypy — config file & `strict`: <https://mypy.readthedocs.io/en/stable/config_file.html>
- Pydantic mypy plugin (`plugins = ["pydantic.mypy"]`, `[tool.pydantic-mypy]`): <https://docs.pydantic.dev/latest/integrations/mypy/>

**Pydantic & pydantic-settings v2**
- pydantic-settings (`BaseSettings`, `SettingsConfigDict`, `env_prefix`, `env_nested_delimiter`, nested `BaseModel` groups, `SecretStr`): <https://docs.pydantic.dev/latest/concepts/pydantic_settings/>
- Validators (`@model_validator(mode="after")` returns `self`): <https://docs.pydantic.dev/latest/concepts/validators/>
- `SecretStr`: <https://docs.pydantic.dev/latest/api/types/#pydantic.types.SecretStr>

**Pytest & pytest-asyncio**
- pytest — `[tool.pytest.ini_options]`, markers, `--strict-markers`: <https://docs.pytest.org/en/stable/reference/customize.html>
- pytest-asyncio — modes (`asyncio_mode = "auto"`): <https://pytest-asyncio.readthedocs.io/en/latest/concepts.html>
- `monkeypatch` fixture: <https://docs.pytest.org/en/stable/how-to/monkeypatch.html>

**Structlog & tenacity (consumed in later phases)**
- structlog — standard library integration (`ProcessorFormatter`, `LoggerFactory`, processors): <https://www.structlog.org/en/stable/standard-library.html>
- structlog — `contextvars`: <https://www.structlog.org/en/stable/contextvars.html>
- tenacity — API (`AsyncRetrying`, `wait_exponential_jitter`, `stop_after_attempt`, `retry_if_exception_type`): <https://tenacity.readthedocs.io/en/latest/api.html>

**Python language/stdlib**
- `enum.StrEnum` (3.11+): <https://docs.python.org/3/library/enum.html#enum.StrEnum>
- `dataclasses` (`slots=True`): <https://docs.python.org/3/library/dataclasses.html>
- `datetime` (`datetime.now(UTC)`): <https://docs.python.org/3/library/datetime.html>
- `functools.lru_cache`: <https://docs.python.org/3/library/functools.html#functools.lru_cache>

**Architecture background**
- Hexagonal (Ports & Adapters) — Alistair Cockburn: <https://alistair.cockburn.us/hexagonal-architecture/>
- Domain-Driven Design — aggregates & invariants (Fowler, "AnemicDomainModel"): <https://martinfowler.com/bliki/AnemicDomainModel.html>

---

## 15. Navigation

| ← Previous | Index | Next → |
|-----------|-------|--------|
| — (this is the first phase) | [All phases](README.md) | [Phase 2 — Concurrency Core, Retry Policy, Ports](phase-2-concurrency-retry-ports.md) |
