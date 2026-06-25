"""Worker runner — reuses the AppContainer composition root verbatim.

This is the architectural keystone of the project: the worker boots from the
SAME ``AppContainer.create(settings)`` the FastAPI lifespan uses (Phase 6).
There is exactly one DI graph; the API and the worker share it. Nothing here
constructs an engine, a Redis client, an object store, or a provider — those
all come pre-wired from the container.
"""

from __future__ import annotations

import asyncio
import signal
import sys
from typing import cast

import structlog

from app.adapters.broker.consumer import StreamConsumer
from app.adapters.broker.keys import BrokerKeys
from app.adapters.broker.producer import StreamProducer
from app.container import AppContainer
from app.core.config import Settings
from app.core.logging import configure_logging
from app.services.pipelines import PipelineContext
from app.services.processor import JobProcessor

log = structlog.get_logger(__name__)


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, stop: asyncio.Event) -> None:
    """Wire SIGINT/SIGTERM to set the stop Event, cross-platform.

    Linux: ``loop.add_signal_handler`` runs the callback *inside* the loop,
    so it can safely touch ``stop`` (an asyncio primitive).

    Windows: ``add_signal_handler`` raises ``NotImplementedError`` on the
    Proactor event loop (documented platform limitation). We fall back to
    ``signal.signal``, whose handler runs on the main thread *outside* the
    loop — so it must marshal back into the loop with
    ``loop.call_soon_threadsafe`` to set ``stop`` safely.
    """
    try:
        # --- Primary path (POSIX): asyncio-native signal handling -----------
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        log.info("signals.installed", mode="add_signal_handler")
    except NotImplementedError:
        # --- Fallback path (Windows Proactor loop) --------------------------
        # signal.signal handlers fire on the main thread, NOT inside the loop,
        # so we cannot call stop.set() directly (it touches loop internals).
        # call_soon_threadsafe is the documented, thread-safe bridge back in.
        def _handler(signum: int, frame: object) -> None:
            loop.call_soon_threadsafe(stop.set)

        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, _handler)
        log.info("signals.installed", mode="signal.signal", platform=sys.platform)


async def run_worker(container: AppContainer, settings: Settings, stop: asyncio.Event) -> None:
    """Run the consume loop until ``stop`` is set, then drain and stop cleanly.

    The container is injected (already created and not yet closed). ``stop`` is
    injected too, so tests can pre-set it and assert the loop exits without
    relying on signals or sleeps.
    """
    # Build the per-job processor from the container's pre-wired ports. NOTE:
    # every collaborator below comes FROM the container — nothing new is
    # constructed here. This is the composition-root reuse, made literal.
    ctx = PipelineContext(
        search=container.providers.search,
        embedding=container.providers.embedding,
        vector_store=container.providers.vector_store,
        llm=container.providers.llm,
        object_store=container.object_store,
    )
    processor = JobProcessor(repository=container.repository, ctx=ctx)

    # StreamConsumer (Phase 5) owns the loop, backpressure, reclaim, ACK/retry/DLQ,
    # and the graceful drain. We just supply collaborators and the stop Event.
    consumer = StreamConsumer(
        redis=container.redis,
        keys=BrokerKeys.from_settings(settings.broker),
        settings=settings.broker,
        repository=container.repository,
        processor=processor,  # consumer calls processor(message) -> __call__
        # The container always builds a StreamProducer (typed as the JobQueue
        # port); the consumer needs its richer republish/dead_letter surface.
        producer=cast(StreamProducer, container.queue),
    )
    await consumer.start()  # idempotent XGROUP CREATE ... MKSTREAM

    log.info("worker.started", concurrency=settings.broker.worker_concurrency)
    # run() loops consume_once() until stop is set, then drains in-flight tasks in
    # its finally — the per-iteration consume_once stays the deterministic seam.
    await consumer.run(stop)
    log.info("worker.stopped")


async def main() -> None:
    """Entry coroutine: load settings, build the shared container, run, close.

    Must run under ``asyncio.run`` because ``AppContainer.create`` installs a
    sized ThreadPoolExecutor as the running loop's default executor and opens
    async resources — both need a running loop.
    """
    settings = Settings()  # reads AIE_* env; applies the zero-cloud redirect
    configure_logging(settings)  # structlog setup (Phase 1) — before any container logs
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    _install_signal_handlers(loop, stop)

    # >>> Composition-root reuse: the SAME call the FastAPI lifespan makes. <<<
    container = await AppContainer.create(settings)
    try:
        await run_worker(container, settings, stop)
    except KeyboardInterrupt:
        # Windows: a Ctrl+C between iterations can surface as KeyboardInterrupt
        # despite our handler. consumer.run()'s finally has already drained
        # in-flight work; nothing to do here but fall through to aclose().
        log.info("worker.keyboard_interrupt")
    finally:
        # Symmetric, reverse-order teardown — the SAME aclose the API uses.
        await container.aclose()
        log.info("worker.closed")
