"""The synchronous-offload port.

Every blocking SDK call in the engine goes through a ``SyncOffloader`` so it
never runs on the event-loop thread. Production uses ``ThreadOffloader``
(``app.core.concurrency``); tests inject ``RecordingOffloader``
(``tests.support.offloader``) for deterministic, clock-free assertions.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol, runtime_checkable


@runtime_checkable
class SyncOffloader(Protocol):
    """Run a blocking callable off the event-loop thread and await its result.

    The signature is generic over the callable's parameters (``P``) and return
    type (``R``) using PEP 695 syntax (Python 3.12+), so a call such as::

        text: str = await offloader.run(client.read_text, "key")

    type-checks end-to-end: ``mypy --strict`` infers ``R`` from ``fn`` and
    verifies ``*args``/``**kwargs`` against ``fn``'s parameters via ``P``.

    Implementations MUST:
      * call ``fn(*args, **kwargs)`` exactly once;
      * propagate its return value unchanged;
      * let any exception ``fn`` raises propagate unchanged (the retry policy,
        layered *outside* the offloader, decides what to do with it).
    """

    async def run[**P, R](
        self,
        fn: Callable[P, R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        """Execute ``fn(*args, **kwargs)`` off-thread and return its result.

        ``fn`` is positional-only (the ``/``) so a keyword named ``fn`` in the
        wrapped callable cannot collide with this parameter.
        """
        ...
