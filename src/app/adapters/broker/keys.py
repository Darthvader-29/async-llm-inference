"""Broker key/identifier helpers — pure, no I/O, settings-derived.

Single source of truth for the stream/group/DLQ names and the per-process
consumer name, so the producer and consumer can never disagree about which
stream they are talking to.
"""

from __future__ import annotations

import os
import secrets
import socket
from dataclasses import dataclass

from app.core.config import BrokerSettings


@dataclass(frozen=True, slots=True)
class BrokerKeys:
    """Resolved Redis key namespace for the broker, derived from settings.

    Frozen + slots so it is hashable, immutable, and cheap to pass around. Both
    producer and consumer take one of these so they cannot disagree about which
    stream/group/DLQ they operate on.
    """

    stream: str  # main work stream,  e.g. "aie:jobs"
    group: str  # consumer group,    e.g. "aie-workers"
    dlq: str  # dead-letter stream, e.g. "aie:jobs:dlq"

    @classmethod
    def from_settings(cls, settings: BrokerSettings) -> BrokerKeys:
        return cls(stream=settings.stream, group=settings.group, dlq=settings.dlq)


def make_consumer_name() -> str:
    """Stable-per-process, unique-across-processes consumer name.

    Format: ``{host}-{pid}-{rand}`` (e.g. ``box01-48211-9f3a1c``).

    - ``host`` + ``pid`` make it human-debuggable in ``XINFO CONSUMERS``.
    - ``rand`` guards against PID reuse and against two workers in containers
      momentarily sharing a host/PID view colliding — which would make
      ``XAUTOCLAIM`` reclaim a *live* consumer's own messages.
    """
    host = socket.gethostname()
    pid = os.getpid()
    rand = secrets.token_hex(3)  # 6 hex chars; plenty for collision avoidance
    return f"{host}-{pid}-{rand}"
