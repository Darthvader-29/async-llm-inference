"""Application services — thin orchestrators between the API/worker and ports.

Services depend only on ports (``JobRepository``, ``JobQueue``, providers) and
the retry policy — never on adapters or FastAPI. The ingestion write-path and
the worker-side processor/pipelines both live here.
"""

from __future__ import annotations

from app.services.pipelines import PIPELINES, Pipeline, PipelineContext
from app.services.processor import JobProcessor

__all__ = ["PIPELINES", "JobProcessor", "Pipeline", "PipelineContext"]
