"""Concurrency core: the production offloader and the shared thread pool.

``ThreadOffloader`` is the runtime implementation of the ``SyncOffloader``
port — a literal, typed passthrough to :func:`asyncio.to_thread`. Because
``to_thread`` dispatches to the *running loop's default executor*, installing a
sized pool there (via ``install_default_executor``) bounds the engine's total
off-thread concurrency in one place.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor


class ThreadOffloader:
    """Run blocking callables in the event loop's default thread pool.

    Structurally implements :class:`app.ports.offloader.SyncOffloader`. The
    body is a one-liner on purpose: all sizing/policy lives in the executor the
    composition root installs as the loop default (see
    :func:`install_default_executor`).
    """

    async def run[**P, R](
        self,
        fn: Callable[P, R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        # asyncio.to_thread:
        #   * runs fn(*args, **kwargs) in the running loop's default executor,
        #   * copies the current contextvars.Context into the worker thread,
        #   * returns a coroutine resolving to fn's return value.
        # (Added in Python 3.9; see the asyncio docs.)
        return await asyncio.to_thread(fn, *args, **kwargs)


def build_executor(
    max_workers: int, *, thread_name_prefix: str = "aie-offload"
) -> ThreadPoolExecutor:
    """Create the sized pool the offloader will dispatch into.

    ``thread_name_prefix`` makes worker threads identifiable in tracebacks and
    profilers (e.g. ``aie-offload_0``).
    """
    return ThreadPoolExecutor(
        max_workers=max_workers,
        thread_name_prefix=thread_name_prefix,
    )


def install_default_executor(loop: asyncio.AbstractEventLoop, executor: ThreadPoolExecutor) -> None:
    """Register ``executor`` as the loop's default executor.

    After this call, every ``asyncio.to_thread(...)`` — and therefore every
    ``ThreadOffloader.run(...)`` — dispatches into ``executor`` rather than the
    lazily-created, *unbounded-by-default* fallback pool. Must be called once,
    at composition-root startup, on the loop the app will run on.

    ``set_default_executor`` requires a ``ThreadPoolExecutor`` instance
    (enforced since Python 3.11).
    """
    loop.set_default_executor(executor)
