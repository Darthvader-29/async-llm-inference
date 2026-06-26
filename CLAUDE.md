# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

**Implemented (Phases 1-9 complete).** The full engine described in the design docs exists and is wired up: `pyproject.toml` (uv-managed, src layout) plus source under `src/app/` (`core/`, `domain/`, `ports/`, `adapters/`, `services/`, `api/`, `worker/`, a shared `container.py` composition root), `migrations/` (async alembic), Docker/compose, CI, and a `tests/` tree (`unit/`, `integration/`, `support/`). The **unit suite passes with zero network/Docker** (sqlite via `aiosqlite`, `fakeredis.aioredis`, in-process fakes) under `ruff` + `mypy --strict`; integration tests are gated behind the `integration` marker and require live Postgres/Redis/MinIO.

Treat the design docs as the rationale of record, and keep them in sync when behavior changes:

- `Docs/problem-statement.md` — the authoritative spec (an **Asynchronous AI Serving Engine** built on Hexagonal / Ports & Adapters principles; a portfolio/interview-facing project).
- `Docs/implementation-plan.md` — the phase-by-phase build plan. Architectural decisions, the repo layout, dependency list, and per-phase verify steps all live there; keep it and the code in sync.

The rest of this file summarizes the locked decisions so you don't have to re-derive them. When in doubt, the two `Docs/` files win.

## Core architecture (locked decisions)

The engine **decouples ingestion from execution**:
- **Ingestion** — FastAPI inserts a `PENDING` job row in PostgreSQL, publishes a pointer message to the Redis Stream, and returns `202 Accepted` + execution ID in milliseconds. No pipeline work runs inline.
- **Execution** — a separate worker process pulls from Redis, runs non-blocking adapter calls, writes artifacts to MinIO/S3, and updates the job row to `SUCCESS`/`FAILED`.

Non-negotiable structural rules (these are the project's whole point — violating them defeats the exercise):

- **Ports are `typing.Protocol`** (structural typing). Adapters and fakes conform without importing the abstraction. `core/` never imports FastAPI.
- **No global singletons.** A single composition root — a plain `@dataclass AppContainer` with `async create(settings)` / `aclose()` — builds and wires everything, and is shared verbatim by the API (via FastAPI lifespan → `app.state`) and the worker (`__main__`). Routes get it via `Depends`, never module globals.
- **Every sync SDK call is offloaded.** Adapters never touch an SDK except through an injected `SyncOffloader` port (default impl = `asyncio.to_thread`; the composition root installs a sized `ThreadPoolExecutor` as the loop's default executor). Tests inject a `RecordingOffloader` spy to prove offloading **without measuring time**.
- **Retry wraps offload.** `tenacity` (`wait_exponential_jitter`, `stop_after_attempt`, retry only on `TransientUpstreamError`); each attempt re-offloads. Retry params come from `Settings` so tests set `base_delay_s=0` and count attempts — never sleep/measure clocks.
- **Stream messages are pointers** (`{job_id, job_type, attempt}`); PostgreSQL is the single source of truth for payload/status. Delivery is at-least-once, guarded by an idempotency check (re-read the row; ack-and-skip if already terminal).
- **Fakes are the default.** Deterministic in-process fake providers (embedding, vector store, LLM, search) run with zero keys and zero cloud. Real SDK adapters (HuggingFace, Pinecone, boto3, `ddgs`) activate only when keys are configured.
- **Zero-cloud isolation.** When `Environment != prod` and no S3 `endpoint_url` is set, a Pydantic `model_validator` forces object storage to the local MinIO container (`http://localhost:9000`, path-style).

Broker is a **custom asyncio worker on Redis Streams** (XADD / XREADGROUP consumer groups / XACK / XAUTOCLAIM reclaim, semaphore-bounded backpressure) — deliberately **not** Celery/arq. The deterministic test seam is `consume_once()`.

## Toolchain & commands

Python **3.12+**, managed with **uv**, tasks via **poethepoet** (`poe`), lint/format **ruff**, types **mypy --strict**, tests **pytest + pytest-asyncio** (`asyncio_mode = "auto"`). None of this exists yet — these are the commands the plan establishes (see Phase 1 and Phase 9 of `implementation-plan.md`):

```bash
uv sync                      # install deps
uv run poe check             # ruff + mypy + unit tests (the main gate)
uv run poe fmt               # ruff format
uv run poe lint              # ruff check
uv run poe typecheck         # mypy
uv run poe test              # unit tests:  pytest -m "not integration"
uv run poe test-int          # integration tests: pytest -m integration (needs infra up)
uv run poe up / down         # docker compose infra (postgres, redis, minio)
uv run poe migrate           # alembic upgrade head
uv run poe api / worker      # run the FastAPI app / the background worker
uv run poe smoke             # scripts/smoke.ps1 — POST + poll a job to terminal state
```

- **Run a single test:** `uv run pytest tests/unit/test_domain.py::test_name`
- **Two test tiers:** unit tests are the default and must run with **zero network/Docker** (sqlite via `aiosqlite`, `fakeredis.aioredis`, in-process fakes). Integration tests require live infra and are gated behind the `integration` marker.

## Testing philosophy (exit criteria — enforce these)

Tests must be **deterministic and clock-free**. Do not write flaky time-dependent async tests. Specifically:
- Prove `to_thread` wrapping via the `RecordingOffloader` spy + DI overrides, not by timing.
- Prove retry behavior by counting attempts with `base_delay_s=0`.
- Prove no resource leaks: `aclose()` closes every client/executor; integration variant asserts `engine.pool.checkedout() == 0` after lifespan exit.
- Gate graceful-shutdown/drain tests on an `asyncio.Event`, never `sleep`.

See the "Exit-criteria traceability" table at the end of `implementation-plan.md` for which test file proves which spec criterion.

## Windows / environment notes

This repo is developed on Windows (paths contain a space: `Study supply`) but containers run Linux. Already accounted for in the plan:
- Stay on the default **ProactorEventLoop**; `uvloop` is unavailable locally (skipped via the `uvicorn[standard]` marker) and activates only inside Linux containers. asyncpg + redis-py asyncio both work on Proactor — this is why asyncpg is chosen over psycopg-async.
- `loop.add_signal_handler` raises `NotImplementedError` on Windows → the worker falls back to `signal.signal` + `call_soon_threadsafe(stop.set)`.
- Line endings are forced to LF: `.gitattributes` (`* text=auto eol=lf`) + ruff `line-ending = "lf"`.
- Prefer **named Docker volumes** over bind mounts (path-with-space friendly); quote paths in scripts.
