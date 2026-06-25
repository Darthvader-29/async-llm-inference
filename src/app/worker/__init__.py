"""The worker process package.

``python -m app.worker`` runs the execution half of the engine: it reuses the
SAME ``AppContainer`` composition root as the API (Phase 6), binds the Redis
Streams consumer (Phase 5) to the ``JobProcessor`` (services), and drains jobs
through the inference pipelines. No logic lives here — see ``runner`` (the
testable run loop) and ``__main__`` (the process launcher).
"""
