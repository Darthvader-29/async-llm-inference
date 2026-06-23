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
