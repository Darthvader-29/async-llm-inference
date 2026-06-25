"""Helpers to assemble an all-fakes ``AppContainer`` and stub clients for tests.

The stubs expose the *behaviour the API/container touch* (``ping``/``aclose`` on
redis, ``dispose`` on the engine, an awaitable ``execute`` on the session) plus
``closed``/``disposed`` flags so the leak test can prove teardown ran. They are
constructed by hand (bypassing ``AppContainer.create``) so each test owns every
field — only the three fields whose real types are concrete SDK clients need a
localized ``type: ignore[arg-type]``; the port-typed fields conform structurally.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from app.adapters.providers.bundle import build_providers
from app.container import AppContainer
from app.core.config import Environment, RetrySettings, Settings
from tests.support.fakes import FakeObjectStore, FakeQueue, InMemoryRepository
from tests.support.offloader import RecordingOffloader

# A reusable pair of valid keys for the auth-bearing tests.
DEFAULT_API_KEYS: frozenset[str] = frozenset({"test-key-123", "second-key"})


def fake_settings(api_keys: frozenset[str] = DEFAULT_API_KEYS) -> Settings:
    """A hermetic ``test`` Settings with clock-free retries and explicit keys.

    ``max_delay_s=0`` caps every ``wait_exponential_jitter`` result to 0, so the
    publish-retry loop counts attempts with zero wall-clock time. ``_env_file=None``
    keeps a developer's local ``.env`` from leaking into the suite.
    """
    return Settings(
        env=Environment.TEST,
        api_keys=api_keys,
        retry=RetrySettings(
            max_attempts=3, base_delay_s=0.0, max_delay_s=0.0, exp_base=2.0, jitter_s=0.0
        ),
        _env_file=None,
    )


@dataclass
class StubRedis:
    """Minimal redis stub exposing a ``closed`` flag and the calls we probe."""

    closed: bool = False
    pinged: bool = False
    fail_ping: bool = False

    async def ping(self) -> bool:
        self.pinged = True
        if self.fail_ping:
            raise RuntimeError("redis ping failed (stub)")
        return True

    async def aclose(self) -> None:
        self.closed = True


@dataclass
class StubEngine:
    """Stand-in for AsyncEngine; records ``dispose()``."""

    disposed: bool = False

    async def dispose(self) -> None:
        self.disposed = True


@dataclass
class _FakeSession:
    """Async-context-manager session whose ``execute`` is awaitable + harmless."""

    fail: bool = False

    async def __aenter__(self) -> _FakeSession:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def execute(self, statement: object) -> object:
        if self.fail:
            raise RuntimeError("db unavailable (stub)")
        return None


@dataclass
class _FakeSessionFactory:
    """Callable returning a fresh ``_FakeSession`` (mirrors ``async_sessionmaker``)."""

    fail: bool = False

    def __call__(self) -> _FakeSession:
        return _FakeSession(fail=self.fail)


def build_fake_container(
    settings: Settings, *, ready: bool = True, installed_executor: bool = False
) -> AppContainer:
    """Assemble an ``AppContainer`` wired entirely to fakes/stubs.

    The executor is a real (tiny) ``ThreadPoolExecutor`` so the leak test can
    prove ``aclose()`` shuts it down. With no provider keys in ``settings``,
    ``build_providers`` returns the all-fakes bundle — exercising the real
    selection logic. ``ready=False`` makes the DB + redis probes fail so the
    readiness endpoint returns 503. ``installed_executor=True`` simulates the
    real ``create()`` path so the leak test can exercise the ``aclose()``
    branch that handles the loop-default executor.
    """
    offloader = RecordingOffloader()
    return AppContainer(
        settings=settings,
        engine=StubEngine(),  # type: ignore[arg-type]
        session_factory=_FakeSessionFactory(fail=not ready),  # type: ignore[arg-type]
        repository=InMemoryRepository(),
        redis=StubRedis(fail_ping=not ready),  # type: ignore[arg-type]
        offloader=offloader,
        executor=ThreadPoolExecutor(max_workers=2, thread_name_prefix="test"),
        object_store=FakeObjectStore(),
        queue=FakeQueue(),
        providers=build_providers(settings, offloader),
        _installed_executor=installed_executor,
    )
