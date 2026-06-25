# syntax=docker/dockerfile:1.7
# ^ Pin the BuildKit Dockerfile frontend so --mount=type=cache/bind behave
#   identically across machines and CI. Without it, older daemons may not
#   understand the cache-mount syntax used below.

# =============================================================================
# Stage 1 — builder: resolve + install the locked environment with uv
# =============================================================================
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder
# ^ Official Astral image: Debian bookworm slim + Python 3.12 + the `uv` binary
#   on PATH. bookworm is chosen for parity with the python:3.12-slim runtime.

# --- uv behavior knobs (build-time only) ------------------------------------
ENV UV_COMPILE_BYTECODE=1 \
    # ^ Precompile .py -> .pyc during install (faster cold start; no first-request
    #   compilation in the runtime container).
    UV_LINK_MODE=copy \
    # ^ COPY packages into the venv instead of hardlinking from the cache mount.
    #   The cache lives on a BuildKit cache mount (different filesystem), so
    #   hardlinks would fail; this silences uv's fallback warning.
    UV_PYTHON_DOWNLOADS=0
    # ^ Never download a managed Python; use the interpreter in the base image.

WORKDIR /app

# --- Layer 1: dependencies ONLY (the big, slow, rarely-changing layer) -------
# Bind-mount the lock + manifest (don't COPY them — they aren't needed after
# this step, and binding avoids busting the layer on unrelated changes). Cache
# the uv download/build cache across builds. --no-install-project installs deps
# but NOT our package, so editing src/ never invalidates this layer.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-dev --no-install-project
    # ^ --frozen : use uv.lock exactly; never re-resolve (fail if absent/stale).
    #   --no-dev  : exclude the dev dependency group (pytest, ruff, mypy, ...).
    #   --no-install-project : install deps but not `app` itself (next layer).

# --- Layer 2: the project source + install our package -----------------------
# README.md is REQUIRED here: pyproject.toml declares `readme = "README.md"`, so
# the hatchling build backend reads it when uv installs the project. Omitting it
# fails the build with "Readme file does not exist".
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY migrations ./migrations
COPY alembic.ini ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev
    # ^ Second sync (no --no-install-project) installs `app` into /app/.venv.
    #   Fast: deps already present from Layer 1; only our package is added.

# =============================================================================
# Stage 2 — runtime: slim, non-root, venv-only (no uv, no build tooling)
# =============================================================================
FROM python:3.12-slim AS runtime
# ^ Debian bookworm slim, glibc — manylinux wheels (asyncpg, boto3, ...) install
#   cleanly. No uv here: a runtime image should not carry its build tools.

# --- Runtime environment -----------------------------------------------------
ENV PYTHONUNBUFFERED=1 \
    # ^ Don't buffer stdout/stderr -> logs appear immediately in `docker logs`.
    PYTHONDONTWRITEBYTECODE=1 \
    # ^ Don't write .pyc at runtime (bytecode already compiled in the builder).
    PATH="/app/.venv/bin:$PATH"
    # ^ Put the venv's bin first so python/uvicorn/alembic resolve to the venv
    #   without `uv run` or an explicit activate — this is how runtime "activates".

# --- Create an unprivileged user (fixed UID/GID for predictable file perms) --
RUN groupadd --system --gid 1001 appgroup \
 && useradd --system --uid 1001 --gid appgroup --no-create-home --shell /usr/sbin/nologin appuser
 # ^ --system: no aging/expiry; --no-create-home: no /home needed; nologin shell:
 #   the account can't be used for interactive login. UID/GID 1001 avoids host
 #   UID 1000 collisions.

WORKDIR /app

# --- Copy the finished venv and the application from the builder -------------
# Chown to appuser so the running process owns its files. src/ is copied so an
# editable install (uv's default) resolves at the same /app/src path it was
# installed against in the builder; migrations/ + alembic.ini let the SAME image
# run the one-shot `migrate` service (alembic upgrade head).
COPY --from=builder --chown=appuser:appgroup /app/.venv /app/.venv
COPY --from=builder --chown=appuser:appgroup /app/src /app/src
COPY --from=builder --chown=appuser:appgroup /app/migrations /app/migrations
COPY --from=builder --chown=appuser:appgroup /app/alembic.ini /app/alembic.ini

# --- Drop privileges ---------------------------------------------------------
USER appuser
# ^ Everything below runs as UID 1001. Verify: `docker run --rm <image> id -u`
#   prints 1001.

# --- Document the API port (informational; publish via compose `ports:`) -----
EXPOSE 8000

# --- Default command = API. The worker service overrides this in compose. ----
# Exec form (JSON array) so the process is PID 1 and receives SIGTERM directly
# (no shell wrapper to swallow signals) -> graceful shutdown works.
CMD ["uvicorn", "app.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
# ^ --factory: call create_app() to build the ASGI app (Phase 6). --host 0.0.0.0:
#   bind all interfaces so the port is reachable from outside the container.
#   The worker service overrides this with: ["python", "-m", "app.worker"].
