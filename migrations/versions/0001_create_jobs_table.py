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
