"""API-key authentication for the ingestion API.

``APIKeyHeader(..., auto_error=False)`` is deliberate: with the default
``auto_error=True``, a *missing* header yields 403, while a *present-but-wrong*
key would 401 in our manual check — two codes for one failure class. We want a
single, uniform 401 for both, and we want the comparison to be constant-time.
"""

from __future__ import annotations

import secrets
from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyHeader

from app.api.dependencies import SettingsDep

_API_KEY_HEADER = "X-API-Key"

# auto_error=False -> dependency yields ``None`` when the header is absent, so we
# control the 401 (and its message) ourselves below.
api_key_scheme = APIKeyHeader(name=_API_KEY_HEADER, auto_error=False)

_UNAUTHORIZED = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Invalid or missing API key.",
    headers={"WWW-Authenticate": _API_KEY_HEADER},
)


def _is_known_key(presented: str, configured: frozenset[str]) -> bool:
    """Constant-time membership test.

    ``secrets.compare_digest`` is constant-time *per comparison*. We OR across
    all configured keys; the boolean accumulation stays branch-free w.r.t. the
    key bytes, so we don't leak which prefix matched. (The set size is tiny and
    not secret, so iterating it is fine.)

    The operands are compared as **bytes**: ``compare_digest`` raises
    ``TypeError`` on non-ASCII *str* input, and ``presented`` is fully
    attacker-controlled (Starlette latin-1-decodes the raw header, so any byte
    0x80-0xFF arrives as a non-ASCII str). Encoding both sides makes the check
    total over arbitrary input — a non-ASCII key simply fails to match and the
    caller raises the normal 401 instead of crashing with a 500 — while
    remaining constant-time.
    """
    presented_b = presented.encode("utf-8")
    ok = False
    for candidate in configured:
        # bitwise-or avoids short-circuit so every candidate is compared.
        ok |= secrets.compare_digest(presented_b, candidate.encode("utf-8"))
    return ok


async def require_api_key(
    settings: SettingsDep,
    presented: Annotated[str | None, Depends(api_key_scheme)],
) -> str:
    """FastAPI dependency: returns the validated key or raises 401.

    The returned value is the authenticated principal for this request; routes
    that need the caller's identity can ``Depends(require_api_key)``.
    """
    configured = settings.api_keys  # frozenset[str], from Settings (Phase 1)
    if not configured:
        # Misconfiguration: no keys set. Fail closed, never open.
        raise _UNAUTHORIZED
    if presented is None or not _is_known_key(presented, configured):
        raise _UNAUTHORIZED
    return presented


ApiKeyDep = Annotated[str, Depends(require_api_key)]
