"""
Celery application instance for the Async Job Processing Platform.
"""

from celery import Celery

from app.config import settings

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
