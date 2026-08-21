"""
Structured logging configuration.

Emits JSON in production and human-readable colour locally. Context bound via
:func:`structlog.contextvars.bind_contextvars` (request_id, job_id, task_id)
travels automatically with every subsequent log line on the same request or
task, so a single job's lifecycle is greppable by UUID across API and worker.

Both structlog events and plain stdlib records (uvicorn, sqlalchemy, celery)
are funnelled through one :class:`structlog.stdlib.ProcessorFormatter` so
everything lands on stdout in a single consistent format.
"""

from __future__ import annotations

import logging
import sys

import structlog

from app.config import settings

_configured = False


def configure_logging(*, force: bool = False) -> None:
    """Configure structlog and route stdlib logging through it.

    Idempotent — called from both the FastAPI module scope and the Celery
    ``setup_logging`` signal, and harmless if that happens twice in one process.
    """
    global _configured
    if _configured and not force:
        return

    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)

    # Applied to structlog events *and*, via foreign_pre_chain, to records that
    # originate from stdlib loggers we do not control.
    pre_chain: list[structlog.typing.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        timestamper,
        structlog.processors.StackInfoRenderer(),
    ]

    if settings.LOG_FORMAT == "console":
        # ConsoleRenderer formats exceptions itself, so no format_exc_info here.
        render: list[structlog.typing.Processor] = [
            structlog.dev.ConsoleRenderer(colors=True)
        ]
    else:
        render = [
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(),
        ]

    # wrap_for_formatter hands the event dict to the stdlib handler instead of
    # rendering it here. Rendering in both places is what produces log lines
    # nested inside other log lines.
    structlog.configure(
        processors=[*pre_chain, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=pre_chain,
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                *render,
            ],
        )
    )

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(settings.LOG_LEVEL.upper())

    # These libraries install their own handlers, which would emit a second
    # copy of every record in their own format. Clear them and let the records
    # propagate to the root handler configured above.
    for name in (
        "uvicorn",
        "uvicorn.error",
        "uvicorn.access",
        "sqlalchemy.engine",
        "celery",
        "celery.task",
    ):
        noisy = logging.getLogger(name)
        noisy.handlers = []
        noisy.propagate = True

    _configured = True


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a bound logger for *name*."""
    return structlog.stdlib.get_logger(name)
