"""Event-gated graceful shutdown: run() is gated by ``stop`` AND drains the
in-flight task in its ``finally`` before returning — proven by asyncio.Events,
zero sleeps-for-time.

We dispatch one in-flight task via a single ``consume_once`` and then enter
``run()`` with ``stop`` already set, so the loop body is skipped and we observe
ONLY the shutdown contract (stop-gating + drain). This deliberately avoids
running the read loop against fakeredis, whose ``XREADGROUP '>' BLOCK`` does not
park like real Redis — which would busy-spin and starve the event loop. The
loop-body behaviour itself is covered by the dispatch/retry/idempotency tests;
in production the real blocking read parks the loop between passes.
"""

from __future__ import annotations

import asyncio

import pytest

from app.adapters.broker.consumer import StreamConsumer
from app.adapters.broker.messages import JobMessage
from app.adapters.broker.producer import StreamProducer
from tests.unit.broker.conftest import FakeRepository, RecordingProcessor, make_job


async def test_run_drains_in_flight_on_stop_without_sleeping(
    consumer: StreamConsumer,
    producer: StreamProducer,
    repository: FakeRepository,
    processor: RecordingProcessor,
) -> None:
    job = make_job()
    repository.seed(job)
    await producer.publish(job)

    allow_finish = asyncio.Event()

    async def gated(_msg: JobMessage) -> None:
        await allow_finish.wait()  # park until the test releases it

    processor.behavior[job.id] = gated

    # One pass dispatches the gated task; it is now in-flight (parked).
    assert await consumer.consume_once() == 1
    assert consumer.in_flight_count == 1

    # Enter run() with stop ALREADY set: the loop body is skipped, but the
    # finally must still drain the in-flight task — so run() does NOT return
    # until that task is released.
    stop = asyncio.Event()
    stop.set()
    loop_task = asyncio.create_task(consumer.run(stop))

    await asyncio.sleep(0)  # cooperative yield: let run() reach drain()
    assert not loop_task.done()  # blocked draining the in-flight task

    allow_finish.set()  # release the in-flight work
    await asyncio.wait_for(loop_task, timeout=1.0)  # run() returns after drain
    assert loop_task.done()
    assert consumer.in_flight_count == 0


async def test_run_loop_iterates_while_unset_then_exits_and_drains_on_stop(
    consumer: StreamConsumer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The run() LOOP BODY (not just the finally-drain) honors stop: it keeps
    calling consume_once while stop is unset, holds in-flight work, then exits and
    drains once stop is set. consume_once is replaced with a yielding stub so the
    loop genuinely iterates without busy-spinning against fakeredis's non-parking
    XREADGROUP BLOCK. Clock-free: asyncio.sleep(0) is a cooperative yield."""
    allow = asyncio.Event()
    calls = {"n": 0}

    async def fake_consume_once(*, block_ms: int | None = None) -> int:
        calls["n"] += 1
        if calls["n"] == 1:  # put one task in-flight on the first pass

            async def work() -> None:
                await allow.wait()

            task = asyncio.create_task(work())
            consumer._in_flight.add(task)
            task.add_done_callback(consumer._in_flight.discard)
        await asyncio.sleep(0)  # yield each pass so the loop does not busy-spin
        return 0

    monkeypatch.setattr(consumer, "consume_once", fake_consume_once)

    stop = asyncio.Event()
    loop_task = asyncio.create_task(consumer.run(stop))
    for _ in range(5):
        await asyncio.sleep(0)

    # The loop kept iterating (multiple passes) and is still alive with work pending.
    assert calls["n"] >= 2
    assert not loop_task.done()
    assert consumer.in_flight_count == 1

    stop.set()
    allow.set()
    await asyncio.wait_for(loop_task, timeout=1.0)
    assert loop_task.done()  # the loop observed stop and returned
    assert consumer.in_flight_count == 0  # ...after draining the in-flight task
