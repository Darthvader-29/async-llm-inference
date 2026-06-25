"""API-key auth: constant-time membership, uniform 401, fail-closed on empty."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from app.api.auth import _is_known_key, require_api_key
from tests.support.container import fake_settings


def test_known_key_accepted() -> None:
    keys = frozenset({"alpha", "beta"})
    assert _is_known_key("beta", keys) is True


def test_unknown_key_rejected() -> None:
    keys = frozenset({"alpha", "beta"})
    assert _is_known_key("gamma", keys) is False


def test_non_ascii_key_rejected_without_crashing() -> None:
    # secrets.compare_digest raises TypeError on non-ASCII *str* operands; the
    # bytes-based check must treat such a (attacker-controlled) key as simply
    # unknown and return False, never raise.
    assert _is_known_key("café", frozenset({"alpha"})) is False


async def test_require_api_key_valid_returns_principal() -> None:
    settings = fake_settings(frozenset({"alpha"}))
    assert await require_api_key(settings=settings, presented="alpha") == "alpha"


async def test_require_api_key_missing_is_401() -> None:
    settings = fake_settings(frozenset({"alpha"}))
    with pytest.raises(HTTPException) as ei:
        await require_api_key(settings=settings, presented=None)
    assert ei.value.status_code == 401


async def test_require_api_key_wrong_is_401() -> None:
    settings = fake_settings(frozenset({"alpha"}))
    with pytest.raises(HTTPException) as ei:
        await require_api_key(settings=settings, presented="wrong")
    assert ei.value.status_code == 401


async def test_require_api_key_non_ascii_is_401_not_500() -> None:
    # A non-ASCII presented key (Starlette latin-1-decodes raw header bytes)
    # must yield the uniform 401, not crash into a 500.
    settings = fake_settings(frozenset({"alpha"}))
    with pytest.raises(HTTPException) as ei:
        await require_api_key(settings=settings, presented="café")
    assert ei.value.status_code == 401


async def test_require_api_key_empty_config_fails_closed() -> None:
    settings = fake_settings(frozenset())  # no keys configured
    with pytest.raises(HTTPException) as ei:
        await require_api_key(settings=settings, presented="anything")
    assert ei.value.status_code == 401  # fail-closed, not open
