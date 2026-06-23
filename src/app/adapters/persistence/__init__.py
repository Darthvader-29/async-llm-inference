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
