"""Test doubles for the SyncOffloader port.

``RecordingOffloader`` is the workhorse: it RECORDS every offloaded call
(callable qualname + args/kwargs) and RUNS the callable INLINE on the calling
(event-loop) thread. Inline execution makes it fully deterministic — no thread
scheduling, no clock — so adapter tests assert *what crossed the offload
boundary*, never *how long it took*.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class OffloadCall:
    """One recorded offload: the callable's qualified name and its arguments."""

    qualname: str
    args: tuple[object, ...]
    kwargs: dict[str, object]


@dataclass(slots=True)
class RecordingOffloader:
    """A ``SyncOffloader`` spy that records calls and executes them inline.

    Structurally conforms to ``app.ports.offloader.SyncOffloader`` (same
    ``run`` signature). Inject it anywhere a real offloader is expected to make
    a test deterministic.
    """

    calls: list[OffloadCall] = field(default_factory=list)
    #: Thread the most recent fn actually ran on — proves "inline" (== caller).
    last_run_thread_id: int | None = None

    async def run[**P, R](
        self,
        fn: Callable[P, R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        # 1) Record BEFORE executing, so a raising fn is still recorded.
        self.calls.append(
            OffloadCall(
                qualname=getattr(fn, "__qualname__", repr(fn)),
                args=tuple(args),
                kwargs=dict(kwargs),
            )
        )
        # 2) Run INLINE on the current (event-loop) thread — deterministic.
        self.last_run_thread_id = threading.get_ident()
        return fn(*args, **kwargs)

    # --- convenience assertions used by tests -------------------------------

    @property
    def qualnames(self) -> list[str]:
        """Just the recorded callable names, in call order."""
        return [c.qualname for c in self.calls]

    def assert_offloaded(self, qualname: str) -> OffloadCall:
        """Assert a call to ``qualname`` was offloaded; return the first match.

        This is the core 'routed through the boundary' assertion. It is purely
        structural — no timing involved.
        """
        for call in self.calls:
            if call.qualname == qualname or call.qualname.endswith(f".{qualname}"):
                return call
        raise AssertionError(
            f"expected an offloaded call to {qualname!r}; recorded: {self.qualnames}"
        )


@dataclass(slots=True)
class BlockingOffloader:
    """A degenerate offloader that runs ``fn`` inline WITHOUT recording.

    Used only to demonstrate the *anti-pattern* in a teaching test: it stands in
    for "no offloading at all". Adapters must never be wired with this in
    production; it exists so a test can contrast it against ``ThreadOffloader``.
    """

    async def run[**P, R](
        self,
        fn: Callable[P, R],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        return fn(*args, **kwargs)
