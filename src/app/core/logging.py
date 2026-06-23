"""Structured logging configuration.

``configure_logging(settings)`` is called exactly once by the composition root
(Phase 6, in the FastAPI lifespan) and by the worker entrypoint (Phase 7). It
wires structlog on top of the standard library so that:

* our ``structlog.get_logger()`` calls and third-party ``logging`` calls render
  through the SAME formatter (via ``ProcessorFormatter.foreign_pre_chain``);
* production emits one JSON object per line (ingestible by log shippers);
* development emits a colorized, human-readable console line.
"""

from __future__ import annotations

import logging
import sys

import structlog
from structlog.types import Processor

from app.core.config import Settings

# Processors shared by both formats. They run BEFORE the format-specific
# renderer is chosen, enriching every event dict with level, logger name,
# timestamp, and (for errors) exception/stack information.
_SHARED_PROCESSORS: list[Processor] = [
    structlog.contextvars.merge_contextvars,  # bind request/job context (Phase 6/7)
    structlog.stdlib.add_log_level,  # event_dict["level"] = "info" ...
    structlog.stdlib.add_logger_name,  # event_dict["logger"] = "app.worker"
    structlog.processors.TimeStamper(fmt="iso", utc=True),
    structlog.processors.StackInfoRenderer(),  # render stack_info=True nicely
    structlog.processors.format_exc_info,  # turn exc_info into a "exception" str
]


def configure_logging(settings: Settings) -> None:
    """Idempotently configure structlog + stdlib logging for the given env.

    Safe to call more than once (it fully resets handlers each time), which is
    convenient for tests that construct a container per case.
    """
    # 1) Choose the final renderer based on environment.
    renderer: Processor = (
        structlog.processors.JSONRenderer()
        if settings.is_prod
        else structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())
    )

    # 2) Configure structlog itself. The chain ends with
    #    ``ProcessorFormatter.wrap_for_formatter`` so the actual rendering is
    #    delegated to a stdlib formatter (so library logs render identically).
    structlog.configure(
        processors=[
            *_SHARED_PROCESSORS,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # 3) Build the stdlib formatter that renders BOTH structlog and foreign
    #    (plain ``logging``) records. ``foreign_pre_chain`` runs the shared
    #    processors on records that did NOT originate from structlog.
    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
        foreign_pre_chain=_SHARED_PROCESSORS,
    )

    # 4) Attach a single stdout handler with that formatter, replacing any
    #    pre-existing handlers (idempotency).
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.DEBUG if not settings.is_prod else logging.INFO)

    # Tame noisy third-party loggers in dev; they still flow through our handler.
    for noisy in ("uvicorn.access", "botocore", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Typed convenience wrapper around ``structlog.get_logger``.

    Usage: ``log = get_logger(__name__); log.info("job.accepted", job_id=jid)``.
    """
    # ``structlog.get_logger`` is typed as returning ``Any``; bind it to the
    # declared type so mypy --strict's ``warn_return_any`` is satisfied.
    logger: structlog.stdlib.BoundLogger = structlog.get_logger(name)
    return logger
