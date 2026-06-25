"""FastAPI ingestion API package (Phase 6).

Only modules under this package import FastAPI/Starlette; ``app.core`` and
``app.container`` stay framework-agnostic so the worker (Phase 7) can reuse the
same composition root without dragging in the web framework.
"""
