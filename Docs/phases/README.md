# Implementation Phase Documentation

Deep-dive build guides for each phase of the **[Asynchronous AI Serving Engine](../implementation-plan.md)**. Each document is a detailed, self-contained companion to the high-level [implementation plan](../implementation-plan.md) and the [problem statement](../problem-statement.md) — covering design rationale, full file-by-file implementation, diagrams, configuration, testing strategy, and exit-criteria verification.

> [!NOTE]
> These documents describe a **planned, greenfield** build. No source code exists in the repository yet; each phase doc is the authoritative guide for implementing that phase. The architectural decisions they reference are locked in [`implementation-plan.md`](../implementation-plan.md).

## Phases

| Phase | Document | Focus |
|:-----:|----------|-------|
| 1 | [Scaffold, Toolchain, Settings, Domain Core](phase-1-scaffold-toolchain-domain.md) | Project skeleton, `uv`/`ruff`/`mypy`/`pytest`, `Settings` with zero-cloud redirect, domain entities & state machine |
| 2 | [Concurrency Core, Retry Policy, Ports](phase-2-concurrency-retry-ports.md) | `SyncOffloader`, `ThreadOffloader`, `RecordingOffloader` spy, `tenacity` retry, all `Protocol` ports |
| 3 | [Persistence — SQLAlchemy, Alembic, Repository](phase-3-persistence-sqlalchemy-alembic.md) | Async SQLAlchemy 2.0, `JobRow` ↔ domain mapping, Alembic async migrations, infra compose |
| 4 | [Object Store & Provider Adapters](phase-4-object-store-providers.md) | `S3ObjectStore` via offloader, deterministic fakes, real HF/Pinecone/search adapters, offload-invariant test |
| 5 | [Redis Streams Broker](phase-5-redis-streams-broker.md) | Producer/consumer, consumer groups, backpressure, reclaim, retry/DLQ, `consume_once()` seam |
| 6 | [Composition Root & FastAPI Ingestion API](phase-6-composition-root-fastapi-api.md) | `AppContainer`, lifespan DI, API-key auth, discriminated-union schemas, `202` ingestion, leak tests |
| 7 | [Worker Process & Pipelines](phase-7-worker-pipelines.md) | Worker runtime, Windows signal fallback, `JobProcessor`, RAG & embed pipelines, end-to-end demo |
| 8 | [Containerization & Full-Stack Compose](phase-8-containerization-compose.md) | Multi-stage uv Dockerfile, full compose graph, migrate gating, smoke script |
| 9 | [CI, README, Polish](phase-9-ci-readme-polish.md) | GitHub Actions quality + integration jobs, README narrative, exit-criteria traceability, poe tasks |

## How to read these

- **Implementing a phase?** Read its document top-to-bottom; it contains everything needed to build that phase without re-deriving decisions.
- **Onboarding?** Start with the [problem statement](../problem-statement.md), then the [implementation plan](../implementation-plan.md), then Phase 1.
- **Diagrams** are authored as Mermaid; they render in GitHub and in IDEs with a Mermaid preview extension.

## Authoring template

New phase docs follow [`_TEMPLATE.md`](_TEMPLATE.md) for structural consistency.
