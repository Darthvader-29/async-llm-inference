"""Application services — thin orchestrators between the API/worker and ports.

Services depend only on ports (``JobRepository``, ``JobQueue``, providers) and
the retry policy — never on adapters or FastAPI. The ingestion write-path lives
here; the worker-side processor/pipelines arrive in Phase 7.
"""
