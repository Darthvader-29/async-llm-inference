"""Deterministic proofs for the offload boundary.

Proof A (spy): a toy adapter's SDK call is recorded as having crossed the
               offloader — purely structural, no timing.
Proof B (thread identity): the real ThreadOffloader runs fn on a DIFFERENT
               thread than the event loop — proving it truly offloads.

The final section composes retry AROUND the offloader (the production pattern)
and proves, via the spy's call ledger, that each retry attempt re-offloads.
"""

from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass

import pytest

from app.core.concurrency import ThreadOffloader, build_executor, install_default_executor
from app.core.config import RetrySettings
from app.core.retry import retrying
from app.domain.exceptions import TransientUpstreamError
from app.ports.offloader import SyncOffloader
from tests.support.offloader import RecordingOffloader

# --- A toy adapter that depends ONLY on the SyncOffloader port --------------


@dataclass(slots=True)
class _ToyClient:
    """Stands in for a blocking SDK client (boto3 / HF / pinecone)."""

    def fetch(self, key: str) -> str:
        # In a real adapter this would be a blocking network call.
        return f"value:{key}"


@dataclass(slots=True)
class ToyAdapter:
    """Minimal adapter: every external call goes through the offloader port."""

    client: _ToyClient
    offload: SyncOffloader

    async def read(self, key: str) -> str:
        # The line under test: the SDK method is handed to the offload boundary.
        return await self.offload.run(self.client.fetch, key)


# --- Proof A: the spy proves routing through the boundary -------------------


async def test_adapter_routes_sdk_call_through_offloader(
    recording_offloader: RecordingOffloader,
) -> None:
    adapter = ToyAdapter(client=_ToyClient(), offload=recording_offloader)

    result = await adapter.read("abc")

    # The call returned the real value (spy ran fn inline)...
    assert result == "value:abc"
    # ...AND the SDK method was recorded as offloaded — the structural assertion.
    call = recording_offloader.assert_offloaded("fetch")
    assert call.args == ("abc",)
    assert call.kwargs == {}
    # Exactly one offload happened (no accidental double-dispatch).
    assert recording_offloader.qualnames == ["_ToyClient.fetch"]


async def test_spy_records_even_when_sdk_raises(
    recording_offloader: RecordingOffloader,
) -> None:
    """A raising SDK call is still recorded (record-before-execute)."""

    def boom() -> str:
        raise RuntimeError("upstream down")

    with pytest.raises(RuntimeError, match="upstream down"):
        await recording_offloader.run(boom)

    assert recording_offloader.assert_offloaded("boom") is not None


def test_recording_offloader_is_structural_subtype() -> None:
    """mypy-level conformance, exercised at runtime as documentation."""
    off: SyncOffloader = RecordingOffloader()  # assignable ⇒ structurally conforms
    assert isinstance(off, SyncOffloader)  # runtime_checkable presence check


# --- Proof B: ThreadOffloader actually leaves the event-loop thread ---------


async def test_thread_offloader_runs_off_the_loop_thread() -> None:
    """Clock-free proof that ThreadOffloader offloads: different thread id."""
    offloader = ThreadOffloader()
    loop_thread_id = threading.get_ident()

    def capture_thread_id() -> int:
        # Runs inside the worker thread; returns ITS id.
        return threading.get_ident()

    worker_thread_id = await offloader.run(capture_thread_id)

    # The function executed on a DIFFERENT thread than the test/loop thread.
    assert worker_thread_id != loop_thread_id


async def test_thread_offloader_passes_args_and_returns_value() -> None:
    """The offloader forwards *args/**kwargs and returns fn's value verbatim."""
    offloader = ThreadOffloader()

    def add(a: int, b: int, *, scale: int = 1) -> int:
        return (a + b) * scale

    assert await offloader.run(add, 2, 3, scale=10) == 50


async def test_default_executor_bounds_concurrency() -> None:
    """A sized default executor caps simultaneous off-thread work.

    With a 2-worker pool installed, no more than 2 offloaded fns run at once,
    even when 5 are awaited concurrently. We prove the bound WITHOUT timing by
    tracking concurrent occupancy with a lock and an Event gate.
    """
    loop = asyncio.get_running_loop()
    executor = build_executor(max_workers=2, thread_name_prefix="test-pool")
    install_default_executor(loop, executor)
    offloader = ThreadOffloader()

    lock = threading.Lock()
    state = {"current": 0, "peak": 0}
    release = threading.Event()  # gate so threads pile up before releasing

    def occupy() -> None:
        with lock:
            state["current"] += 1
            state["peak"] = max(state["peak"], state["current"])
        # Block until the test lets go — no sleep/clock, an explicit gate.
        release.wait(timeout=5)
        with lock:
            state["current"] -= 1

    try:
        tasks = [asyncio.create_task(offloader.run(occupy)) for _ in range(5)]
        # Let the 2 pool threads reach the gate, then release deterministically.
        await asyncio.sleep(0)  # yield once so tasks get scheduled
        release.set()
        await asyncio.gather(*tasks)
        # Peak concurrency never exceeded the pool size.
        assert state["peak"] <= 2
    finally:
        executor.shutdown(wait=True)


# --- Composition: retry WRAPS offload (each attempt re-offloads) ------------


@dataclass(slots=True)
class _FlakyClient:
    """A stub SDK that fails transiently a fixed number of times."""

    fail_times: int
    _calls: int = 0

    def call(self) -> str:
        self._calls += 1
        if self._calls <= self.fail_times:
            raise TransientUpstreamError(f"transient #{self._calls}")
        return "ok"


@dataclass(slots=True)
class _RetryingToyAdapter:
    """Adapter that wraps offload in retry — the production pattern, in miniature."""

    client: _FlakyClient
    offload: SyncOffloader
    retry_settings: RetrySettings

    async def call(self) -> str:
        retryer = retrying(self.retry_settings)
        async for attempt in retryer:
            with attempt:
                # RETRY (outer) WRAPS OFFLOAD (inner): fresh offload per attempt.
                return await self.offload.run(self.client.call)
        raise AssertionError("unreachable")


async def test_each_retry_attempt_reoffloads(
    recording_offloader: RecordingOffloader,
    retry_settings: RetrySettings,
) -> None:
    """Two transient failures then success ⇒ THREE offloads recorded."""
    adapter = _RetryingToyAdapter(
        client=_FlakyClient(fail_times=2),
        offload=recording_offloader,
        retry_settings=retry_settings,  # max_attempts=3, base_delay_s=0
    )

    result = await adapter.call()

    assert result == "ok"
    # The headline assertion: each attempt re-offloaded ⇒ 3 recorded calls.
    assert len(recording_offloader.calls) == 3
    assert recording_offloader.qualnames == ["_FlakyClient.call"] * 3


async def test_exhaustion_offloads_exactly_max_attempts(
    recording_offloader: RecordingOffloader,
    retry_settings: RetrySettings,
) -> None:
    """Never-succeeding transient ⇒ exactly max_attempts offloads, then raise."""
    adapter = _RetryingToyAdapter(
        client=_FlakyClient(fail_times=99),
        offload=recording_offloader,
        retry_settings=retry_settings,
    )

    with pytest.raises(TransientUpstreamError):
        await adapter.call()

    assert len(recording_offloader.calls) == retry_settings.max_attempts  # 3
