"""Deterministic proofs for the retry policy.

All tests use base_delay_s=0 (the ``retry_settings`` fixture) so the retry loop
spins instantly — we count attempts, we never measure time.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from app.core.config import RetrySettings
from app.core.retry import retrying, translate_upstream_error
from app.domain.exceptions import (
    PermanentUpstreamError,
    TransientUpstreamError,
)


async def _run_with_policy(settings: RetrySettings, fn: Callable[[], object]) -> object:
    """Drive a callable through the policy using the documented async-for API."""
    retryer = retrying(settings)
    async for attempt in retryer:
        with attempt:
            return fn()
    raise AssertionError("unreachable")  # reraise=True returns or raises above


# --- Attempt counting -------------------------------------------------------


async def test_retries_transient_until_max_attempts(retry_settings: RetrySettings) -> None:
    """A perpetually-transient call is attempted exactly max_attempts times."""
    calls = 0

    def always_transient() -> str:
        nonlocal calls
        calls += 1
        raise TransientUpstreamError("503 from upstream")

    with pytest.raises(TransientUpstreamError):
        await _run_with_policy(retry_settings, always_transient)

    assert calls == retry_settings.max_attempts  # == 3 (fixture)


async def test_succeeds_on_second_attempt_counts_two(retry_settings: RetrySettings) -> None:
    """Transient-then-success stops as soon as it succeeds (2 calls)."""
    calls = 0

    def fail_once_then_ok() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TransientUpstreamError("first blip")
        return "ok"

    result = await _run_with_policy(retry_settings, fail_once_then_ok)

    assert result == "ok"
    assert calls == 2


async def test_permanent_error_is_not_retried(retry_settings: RetrySettings) -> None:
    """Permanent errors short-circuit: exactly one attempt, then propagate."""
    calls = 0

    def permanent() -> str:
        nonlocal calls
        calls += 1
        raise PermanentUpstreamError("403 forbidden")

    with pytest.raises(PermanentUpstreamError):
        await _run_with_policy(retry_settings, permanent)

    assert calls == 1  # predicate did NOT match ⇒ no retry


async def test_unrelated_exception_is_not_retried(retry_settings: RetrySettings) -> None:
    """Non-upstream errors (e.g. bugs) are not retried — fail fast."""
    calls = 0

    def bug() -> str:
        nonlocal calls
        calls += 1
        raise ValueError("programming error")

    with pytest.raises(ValueError):
        await _run_with_policy(retry_settings, bug)

    assert calls == 1


async def test_success_first_try_counts_one(retry_settings: RetrySettings) -> None:
    calls = 0

    def ok() -> str:
        nonlocal calls
        calls += 1
        return "done"

    assert await _run_with_policy(retry_settings, ok) == "done"
    assert calls == 1


@pytest.mark.parametrize("max_attempts", [1, 2, 5])
async def test_attempt_count_tracks_setting(max_attempts: int) -> None:
    """The attempt count equals whatever max_attempts is configured to."""
    settings = RetrySettings(
        max_attempts=max_attempts,
        base_delay_s=0.0,
        max_delay_s=0.0,
        exp_base=2.0,
        jitter_s=0.0,
    )
    calls = 0

    def always_transient() -> str:
        nonlocal calls
        calls += 1
        raise TransientUpstreamError("boom")

    with pytest.raises(TransientUpstreamError):
        await _run_with_policy(settings, always_transient)

    assert calls == max_attempts


# --- Error translation (the adapter-side classification helper) -------------


def _is_transient_status(error: Exception) -> bool:
    """Toy classifier: treat errors whose message starts '5' as transient."""
    return str(error).startswith("5")


def test_translate_maps_transient() -> None:
    with pytest.raises(TransientUpstreamError) as exc:
        translate_upstream_error(
            RuntimeError("503 Service Unavailable"),
            is_transient=_is_transient_status,
            context="toy.call",
        )
    # Original cause preserved for logging.
    assert isinstance(exc.value.cause, RuntimeError)
    assert "toy.call" in str(exc.value)


def test_translate_maps_permanent() -> None:
    with pytest.raises(PermanentUpstreamError) as exc:
        translate_upstream_error(
            RuntimeError("403 Forbidden"),
            is_transient=_is_transient_status,
            context="toy.call",
        )
    assert exc.value.__cause__ is not None  # raise ... from error set __cause__
