"""Consumer: the asyncio Redis-Streams worker loop and its deterministic seam.

Public surface::

    StreamConsumer(redis, keys, settings, repository, processor, producer)
    await consumer.start()            # idempotent group creation
    await consumer.consume_once()     # ONE reclaim+read+dispatch pass (test seam)
    await consumer.run(stop)          # production loop until stop.is_set()
"""

from __future__ import annotations

import asyncio
from typing import Any, Protocol

import structlog
from redis.asyncio import Redis

from app.adapters.broker.keys import BrokerKeys, make_consumer_name
from app.adapters.broker.messages import JobMessage
from app.adapters.broker.producer import StreamProducer, ensure_group
from app.core.config import BrokerSettings
from app.domain.exceptions import (
    JobNotFound,
    PermanentUpstreamError,
    TransientUpstreamError,
)
from app.ports.repository import JobRepository

log = structlog.get_logger(__name__)


class JobProcessor(Protocol):
    """The unit of work the consumer dispatches to.

    Phase 5 keeps this abstract: the consumer takes a callable matching this
    Protocol. Phase 7 supplies the real processor; tests supply a recording
    fake. Structural typing means neither has to import the other.
    """

    async def __call__(self, message: JobMessage, /) -> None: ...


# A delivered stream entry as redis-py returns it: (message_id, {field: value}).
DeliveredEntry = tuple[Any, dict[Any, Any]]


class StreamConsumer:
    def __init__(
        self,
        redis: Redis,
        keys: BrokerKeys,
        settings: BrokerSettings,
        repository: JobRepository,
        processor: JobProcessor,
        producer: StreamProducer,
    ) -> None:
        self._redis = redis
        self._keys = keys
        self._repo = repository
        self._process = processor
        self._producer = producer

        # Tuning (all from Settings — never hard-coded, so tests can zero them).
        self._concurrency = settings.worker_concurrency
        self._block_ms = settings.block_ms
        self._reclaim_idle_ms = settings.reclaim_idle_ms
        self._max_attempts = settings.max_attempts
        self._max_delivery_count = settings.max_delivery_count

        # Per-process identity (computed once; see keys.make_consumer_name).
        self._consumer_name = make_consumer_name()

        # The in-flight register. Its length IS the backpressure signal. Typed as
        # Future[None] so `add_done_callback(self._in_flight.discard)` matches the
        # Callable[[Future[None]], object] callback contract under mypy --strict.
        self._in_flight: set[asyncio.Future[None]] = set()

        # Parallel set of message IDs THIS consumer is currently processing. Used
        # to skip reclaiming our own still-in-flight entries: XAUTOCLAIM filters by
        # idle time alone and cannot exclude the caller's own PEL entries, so a job
        # that runs longer than reclaim_idle_ms would otherwise be self-reclaimed
        # and double-dispatched.
        self._in_flight_ids: set[str] = set()

        # Cursor for XAUTOCLAIM scanning; "0-0" = scan from the beginning.
        self._reclaim_cursor: str = "0-0"

    # ----------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        """Idempotently ensure the consumer group exists. Safe on every worker
        and every boot (BUSYGROUP is swallowed in ensure_group)."""
        await ensure_group(self._redis, self._keys)
        log.info(
            "broker.consumer.started",
            consumer=self._consumer_name,
            group=self._keys.group,
            concurrency=self._concurrency,
        )

    @property
    def in_flight_count(self) -> int:
        """Number of currently-running dispatched tasks (test/observability)."""
        return len(self._in_flight)

    # ------------------------------------------------------------- the test seam
    async def consume_once(self, *, block_ms: int | None = None) -> int:
        """Run exactly ONE pass: reclaim orphans -> compute capacity -> read up to
        that budget -> dispatch each. Returns the number of NEW messages read
        (excludes reclaimed) so tests/metrics can assert on it.

        ``block_ms`` defaults to the configured block; tests pass ``0`` so there
        is no waiting. ``run()`` passes the configured block.
        """
        block = self._block_ms if block_ms is None else block_ms

        # (1) Rescue messages abandoned by a crashed/slow consumer.
        await self._reclaim_orphans()

        # (2) Backpressure: only read what we have capacity to start.
        capacity = self._free_capacity()
        if capacity <= 0:
            log.debug("broker.backpressure.saturated", in_flight=self.in_flight_count)
            return 0

        # (3) Read up to `capacity` brand-new messages ('>' = never delivered).
        entries = await self._read_new(count=capacity, block=block)
        if not entries:
            return 0

        # (4) Dispatch each as a tracked task.
        for message_id, fields in entries:
            self._dispatch(message_id, fields)
        return len(entries)

    # ----------------------------------------------------------- production loop
    async def run(self, stop: asyncio.Event) -> None:
        """Loop ``consume_once`` until ``stop`` is set, then drain in-flight.

        Each read uses ``block_ms``, so an idle loop wakes at most every
        ``block_ms`` to re-check ``stop`` — shutdown latency is bounded by
        ``block_ms`` with zero busy-spinning. The ``finally`` guarantees a drain
        on every exit path (stop, exception, or cancellation).
        """
        log.info("broker.consumer.loop.start", consumer=self._consumer_name)
        try:
            while not stop.is_set():
                try:
                    await self.consume_once(block_ms=self._block_ms)
                except asyncio.CancelledError:
                    raise
                except Exception:  # one bad pass must not kill the loop
                    log.exception("broker.consume_once.error")
                    # Bounded backoff so a hard failure (e.g. Redis down) does not
                    # hot-loop. run() is not exercised by the unit tier.
                    await asyncio.sleep(min(self._block_ms / 1000, 1.0) or 0.1)
        finally:
            await self.drain()
            log.info("broker.consumer.loop.stop", consumer=self._consumer_name)

    # ---------------------------------------------------------------- step (1)
    async def _reclaim_orphans(self) -> None:
        """Reclaim PEL entries idle longer than ``reclaim_idle_ms`` to THIS
        consumer, then dispatch or DLQ them.

        ``XAUTOCLAIM`` returns ``[next_cursor, claimed, deleted_ids]`` on Redis
        7.0+ (``[next_cursor, claimed]`` on 6.2). We read only ``[0]`` and ``[1]``
        so both arities are safe.
        """
        capacity = self._free_capacity()
        if capacity <= 0:
            return  # no room to run reclaimed work; try next pass

        result = await self._redis.xautoclaim(
            name=self._keys.stream,
            groupname=self._keys.group,
            consumername=self._consumer_name,
            min_idle_time=self._reclaim_idle_ms,
            start_id=self._reclaim_cursor,
            count=capacity,
        )
        next_cursor, claimed = result[0], result[1]
        self._reclaim_cursor = _decode(next_cursor)

        for message_id, fields in claimed:
            await self._handle_reclaimed(message_id, fields)

    async def _handle_reclaimed(self, message_id: Any, fields: dict[Any, Any]) -> None:
        """A reclaimed message may be poison that has bounced between consumers
        too many times. Check delivery count via XPENDING; over
        ``max_delivery_count`` -> DLQ. Otherwise dispatch normally."""
        if _decode(message_id) in self._in_flight_ids:
            # XAUTOCLAIM re-claimed an entry THIS consumer is still processing
            # (it ran longer than reclaim_idle_ms). Skip — do not double-dispatch
            # or DLQ it; the original task still owns it and will ACK on finish.
            log.debug("broker.reclaim.skip_in_flight", message_id=_decode(message_id))
            return
        deliveries = await self._delivery_count(message_id)
        if deliveries > self._max_delivery_count:
            message = JobMessage.from_fields(fields)
            await self._route_to_dlq(
                message_id, message, reason=f"exceeded max delivery count ({deliveries})"
            )
            return
        log.info(
            "broker.reclaimed",
            message_id=_decode(message_id),
            deliveries=deliveries,
            consumer=self._consumer_name,
        )
        self._dispatch(message_id, fields)

    async def _delivery_count(self, message_id: Any) -> int:
        """How many times this entry has been delivered, via XPENDING range.
        Returns 0 if it is no longer pending."""
        pending = await self._redis.xpending_range(
            name=self._keys.stream,
            groupname=self._keys.group,
            min=_decode(message_id),
            max=_decode(message_id),
            count=1,
        )
        if not pending:
            return 0
        return int(pending[0]["times_delivered"])

    # ---------------------------------------------------------------- step (2)
    def _free_capacity(self) -> int:
        """THE backpressure computation: how many more tasks we may start. Pure
        arithmetic over the in-flight set — no semaphore, no lock. 0 means "do
        not read"; the next pass tries again after tasks complete."""
        return self._concurrency - len(self._in_flight)

    # ---------------------------------------------------------------- step (3)
    async def _read_new(self, *, count: int, block: int) -> list[DeliveredEntry]:
        """XREADGROUP with the special id '>' (only entries never delivered to
        any consumer in the group). Returns a flat list of (id, fields).

        redis-py's *typed* return models the RESP3 dict, but the default RESP2
        runtime (and fakeredis) returns the legacy list ``[[stream, entries]]``.
        We treat the response as ``Any`` and handle both shapes defensively.
        """
        response: Any = await self._redis.xreadgroup(
            groupname=self._keys.group,
            consumername=self._consumer_name,
            streams={self._keys.stream: ">"},
            count=count,
            block=block or None,  # block=0 -> None (non-blocking read)
            noack=False,  # we DO want PEL tracking -> at-least-once
        )
        if not response:
            return []
        # RESP3 unified shape is {stream_name: entries}; RESP2 legacy (the default
        # runtime + fakeredis) is [[stream_name, entries]]. Handle both.
        entries = next(iter(response.values())) if isinstance(response, dict) else response[0][1]
        return list(entries)

    # ---------------------------------------------------------------- step (4)
    def _dispatch(self, message_id: Any, fields: dict[Any, Any]) -> None:
        """Spawn a tracked task to process one message. Added to the in-flight
        set and removed on completion via a done-callback — the canonical
        'fire-and-forget without losing the reference' pattern. Synchronous (no
        await): scheduling must not yield, or the capacity math in the same pass
        could race itself."""
        self._in_flight_ids.add(_decode(message_id))
        task = asyncio.create_task(self._process_and_route(message_id, fields))
        self._in_flight.add(task)
        task.add_done_callback(self._in_flight.discard)

    async def _process_and_route(self, message_id: Any, fields: dict[Any, Any]) -> None:
        """Run the processor for one message and route the outcome:

        * success                       -> XACK
        * TransientUpstreamError + room  -> XACK + re-XADD(attempt+1) + row PENDING
        * Transient but attempts gone    -> DLQ + XACK + row FAILED
        * PermanentUpstreamError         -> DLQ + XACK + row FAILED
        * row already terminal           -> idempotent ACK (processor not run)
        """
        message = JobMessage.from_fields(fields)
        bound = log.bind(
            job_id=str(message.job_id),
            attempt=message.attempt,
            message_id=_decode(message_id),
        )
        try:
            # ---- Idempotency guard (at-least-once made safe) ---------------
            if await self._already_terminal(message):
                bound.info("broker.idempotent.skip")
                await self._ack(message_id)
                return

            try:
                await self._process(message)  # the actual pipeline work
            except TransientUpstreamError as exc:
                await self._on_transient(message_id, message, exc, bound)
            except PermanentUpstreamError as exc:
                await self._route_to_dlq(message_id, message, reason=f"permanent: {exc}")
                await self._mark_failed(message, str(exc))
                bound.warning("broker.permanent_failure")
            except Exception as exc:  # unexpected -> permanent, never silent
                await self._route_to_dlq(message_id, message, reason=f"unexpected: {exc!r}")
                await self._mark_failed(message, repr(exc))
                bound.exception("broker.unexpected_failure")
            else:
                await self._ack(message_id)  # clean success -> leave PEL
                bound.info("broker.acked")
        finally:
            # No longer in-flight on THIS consumer (whatever the outcome).
            self._in_flight_ids.discard(_decode(message_id))

    async def _on_transient(
        self,
        message_id: Any,
        message: JobMessage,
        exc: TransientUpstreamError,
        bound: Any,
    ) -> None:
        """Transient failure: retry if attempts remain, else DLQ."""
        if message.attempt >= self._max_attempts:
            await self._route_to_dlq(
                message_id,
                message,
                reason=f"transient, attempts exhausted ({message.attempt}): {exc}",
            )
            await self._mark_failed(message, f"exhausted after {message.attempt}: {exc}")
            bound.warning("broker.retries_exhausted")
            return

        # Write-ahead-then-ack: reset the row to PENDING, publish a FRESH attempt+1
        # entry, and ONLY THEN ACK the old delivery. Acking LAST means a crash
        # anywhere before the ACK leaves the original in the PEL (reclaimable)
        # rather than losing the job. Requeueing before republishing keeps the row
        # PENDING before the retry entry can be read (else a reader would call
        # mark_running on a still-RUNNING row).
        await self._requeue_row(message)
        retry_id = await self._producer.republish(message.next_attempt())
        await self._ack(message_id)
        bound.info(
            "broker.retry.scheduled",
            next_attempt=message.attempt + 1,
            retry_message_id=retry_id,
        )

    # ------------------------------------------------------------ PG helpers
    async def _already_terminal(self, message: JobMessage) -> bool:
        """Idempotency guard: re-read the row; if already SUCCESS/FAILED, a
        duplicate delivery must be ack-and-skipped (not reprocessed)."""
        try:
            job = await self._repo.get(message.job_id)
        except JobNotFound:
            # Row vanished (shouldn't happen — PG is SoT). Treat as terminal so we
            # don't reprocess a ghost; ACK and move on.
            return True
        return job.is_terminal

    async def _requeue_row(self, message: JobMessage) -> None:
        """Reset the job row to PENDING for the upcoming retry delivery."""
        try:
            job = await self._repo.get(message.job_id)
        except JobNotFound:
            return
        job.requeue()  # domain transition RUNNING -> PENDING
        await self._repo.update(job)

    async def _mark_failed(self, message: JobMessage, error: str) -> None:
        """Terminally fail the job row (DLQ path)."""
        try:
            job = await self._repo.get(message.job_id)
        except JobNotFound:
            return
        job.mark_failed(error)  # domain transition -> FAILED
        await self._repo.update(job)

    # --------------------------------------------------------- redis helpers
    async def _ack(self, message_id: Any) -> None:
        """XACK one message: remove it from the group's PEL."""
        await self._redis.xack(self._keys.stream, self._keys.group, message_id)

    async def _route_to_dlq(self, message_id: Any, message: JobMessage, *, reason: str) -> None:
        """Push to DLQ then ACK the original (so it leaves the main PEL)."""
        await self._producer.dead_letter(message, reason)
        await self._ack(message_id)

    # -------------------------------------------------------------- shutdown
    async def drain(self) -> None:
        """Await all in-flight tasks so no job is abandoned mid-flight.

        ``gather(..., return_exceptions=True)`` ensures one failing task does not
        prevent the others from being awaited; each task already routed its own
        outcome inside ``_process_and_route`` so exceptions here should not occur
        — but we never raise out of drain."""
        if not self._in_flight:
            return
        log.info("broker.drain.start", in_flight=len(self._in_flight))
        await asyncio.gather(*tuple(self._in_flight), return_exceptions=True)
        log.info("broker.drain.complete")


def _decode(value: object) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)
