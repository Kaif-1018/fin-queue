"""
Celery application instance for the Async Job Processing Platform.
"""

import structlog
from celery import Celery
from celery.signals import setup_logging, task_postrun, task_prerun

from app.config import settings
from app.logging_config import configure_logging

celery_app = Celery(
    "jobplatform",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
)

# ── Celery configuration ─────────────────────────────────────────
celery_app.conf.update(
    # Serialization
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",

    # Timezone
    timezone="UTC",
    enable_utc=True,

    # Task behaviour
    task_track_started=True,
    task_acks_late=True,              # Acknowledge only after task completes
    worker_prefetch_multiplier=1,     # Fetch one task at a time per worker

    # Auto-discover tasks in app.tasks
    imports=["app.tasks"],
)


# ── Logging ───────────────────────────────────────────────────────

@setup_logging.connect
def _configure_celery_logging(**_kwargs):
    """Stop Celery installing its own handlers so structlog owns the stream."""
    configure_logging()


@task_prerun.connect
def _bind_task_context(task_id=None, task=None, args=None, **_kwargs):
    """Bind task identity to the logging context for the duration of the task.

    ``args[0]`` is the job UUID by convention for every task in TASK_REGISTRY,
    which is what makes a job's whole lifecycle greppable by UUID across both
    the API and worker logs.
    """
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(
        task_id=task_id,
        task_name=getattr(task, "name", None),
        job_id=args[0] if args else None,
    )


@task_postrun.connect
def _unbind_task_context(**_kwargs):
    """Clear the context so a pooled worker process does not leak it forward."""
    structlog.contextvars.clear_contextvars()
