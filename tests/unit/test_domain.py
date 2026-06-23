"""Unit tests for the domain state machine. Pure, deterministic, no fixtures.

The headline test iterates the FULL Cartesian product of (start_status,
transition) and asserts each pair either performs the legal move or raises
``InvalidTransition`` — leaving no transition unverified.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from app.domain import InferenceJob, InvalidTransition, JobStatus, JobType


def _job_in(status: JobStatus) -> InferenceJob:
    """Construct a job forced into ``status`` (bypassing guards for setup only)."""
    job = InferenceJob(job_type=JobType.RAG_QUERY, payload={"q": "hi"})
    # object.__setattr__ works on slotted dataclasses; used ONLY to arrange the
    # precondition for the matrix test, never in production code.
    object.__setattr__(job, "status", status)
    return job


def _status_of(job: InferenceJob) -> JobStatus:
    """Read ``job.status`` as a plain ``JobStatus``.

    mypy --strict narrows ``job.status`` to a ``Literal`` after an ``is`` check
    and does not reset that narrowing across a mutating method call, which would
    make a subsequent ``is`` check look "non-overlapping". Reading through this
    helper returns an un-narrowed ``JobStatus`` per call, so chained lifecycle
    assertions type-check without weakening them.
    """
    return job.status


# Map each transition name to (the method invocation, the resulting status).
_TRANSITIONS: dict[str, tuple[Callable[[InferenceJob], None], JobStatus]] = {
    "mark_running": (lambda j: j.mark_running(), JobStatus.RUNNING),
    "mark_success": (lambda j: j.mark_success("s3://b/k"), JobStatus.SUCCESS),
    "mark_failed": (lambda j: j.mark_failed("boom"), JobStatus.FAILED),
    "requeue": (lambda j: j.requeue(), JobStatus.PENDING),
}

# The legal (start, transition) pairs — the SINGLE expected truth table. Any
# pair NOT listed here must raise InvalidTransition.
_LEGAL: set[tuple[JobStatus, str]] = {
    (JobStatus.PENDING, "mark_running"),
    (JobStatus.RUNNING, "mark_success"),
    (JobStatus.RUNNING, "mark_failed"),
    (JobStatus.RUNNING, "requeue"),
}


@pytest.mark.parametrize("start", list(JobStatus))
@pytest.mark.parametrize("name", list(_TRANSITIONS))
def test_transition_matrix(start: JobStatus, name: str) -> None:
    """Every (start, transition) pair is either legal (moves) or raises."""
    invoke, target = _TRANSITIONS[name]
    job = _job_in(start)

    if (start, name) in _LEGAL:
        invoke(job)
        assert job.status is target
    else:
        with pytest.raises(InvalidTransition) as ei:
            invoke(job)
        # Status is unchanged on an illegal attempt.
        assert job.status is start
        # The exception carries the offending pair for a readable message.
        assert ei.value.current == start


def test_happy_path_lifecycle_rag_query() -> None:
    """PENDING -> RUNNING -> SUCCESS, with attempts/result/error bookkeeping."""
    job = InferenceJob(job_type=JobType.RAG_QUERY, payload={"q": "what is X?"})
    assert _status_of(job) is JobStatus.PENDING
    assert job.attempts == 0
    assert not job.is_terminal

    job.mark_running()
    assert _status_of(job) is JobStatus.RUNNING
    assert job.attempts == 1  # incremented on entering RUNNING

    job.mark_success("s3://aie-artifacts/results/abc.json")
    assert _status_of(job) is JobStatus.SUCCESS
    assert job.is_terminal
    assert job.result_ref == "s3://aie-artifacts/results/abc.json"
    assert job.error is None


def test_retry_path_requeue_then_succeed() -> None:
    """A transient failure path: RUNNING -> PENDING -> RUNNING -> SUCCESS."""
    job = InferenceJob(job_type=JobType.EMBED_DOCUMENT, payload={"doc": "..."})
    job.mark_running()  # attempt 1
    assert job.attempts == 1

    job.requeue()  # transient failure → back to PENDING
    assert job.status is JobStatus.PENDING
    assert job.attempts == 1  # requeue does NOT bump attempts

    job.mark_running()  # attempt 2
    assert job.attempts == 2
    job.mark_success("s3://aie-artifacts/embeddings/xyz.json")
    assert job.is_terminal


def test_mark_running_clears_previous_error() -> None:
    """A fresh attempt clears the error recorded by a prior failed run."""
    job = InferenceJob(job_type=JobType.RAG_QUERY, payload={})
    job.mark_running()
    # Simulate a transient failure that recorded an error, then a requeue.
    object.__setattr__(job, "error", "timeout")
    job.requeue()
    job.mark_running()  # attempt 2
    assert job.error is None  # cleared on re-entry to RUNNING


def test_terminal_states_reject_all_transitions() -> None:
    """SUCCESS and FAILED are dead ends — every transition raises."""
    for terminal in (JobStatus.SUCCESS, JobStatus.FAILED):
        for invoke, _ in _TRANSITIONS.values():
            job = _job_in(terminal)
            with pytest.raises(InvalidTransition):
                invoke(job)


def test_updated_at_advances_monotonically_without_sleep() -> None:
    """``updated_at`` is bumped on each transition — asserted by ORDERING, not time.

    We do NOT sleep. We patch the module clock to return a strictly increasing
    sequence, proving the field is refreshed on every guarded transition.
    """
    from datetime import UTC, datetime

    import app.domain.models as models

    ticks = iter(
        [
            datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),  # created_at
            datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),  # updated_at (init)
            datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC),  # mark_running
            datetime(2024, 1, 1, 0, 0, 2, tzinfo=UTC),  # mark_success
        ]
    )
    original = models._utcnow
    models._utcnow = lambda: next(ticks)
    try:
        job = InferenceJob(job_type=JobType.RAG_QUERY, payload={})
        t0 = job.updated_at
        job.mark_running()
        t1 = job.updated_at
        job.mark_success("s3://b/k")
        t2 = job.updated_at
    finally:
        models._utcnow = original

    assert t0 < t1 < t2  # strictly increasing, no wall-clock dependency
