"""
Celery background tasks for the Async Job Processing Platform.

Celery workers are synchronous, so we use psycopg2 (sync driver) here
instead of asyncpg (async driver used by FastAPI).
"""

import time
import traceback

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.celery_app import celery_app
from app.config import settings
from app.models import Job, JobStatus
from app.pubsub import publish_job_update_sync

# ── Sync database engine for Celery workers ───────────────────────
# Convert async URL (postgresql+asyncpg://...) to sync (postgresql+psycopg2://...)
SYNC_DATABASE_URL = settings.DATABASE_URL.replace(
    "postgresql+asyncpg", "postgresql+psycopg2"
)

sync_engine = create_engine(SYNC_DATABASE_URL, pool_pre_ping=True)
SyncSession = sessionmaker(bind=sync_engine)


# ── Celery task ───────────────────────────────────────────────────

@celery_app.task(name="process_document", bind=True, max_retries=3)
def process_document_task(self, job_id: str) -> dict:
    """
    Simulate a document-processing job (~10 seconds).

    Workflow:
        1. Fetch the job from PostgreSQL.
        2. Set status → PROCESSING.
        3. Simulate work with a 10-second sleep.
        4. On success → status = COMPLETED, store mock result data.
        5. On failure → status = FAILED, store error details.
    """
    with SyncSession() as session:
        job: Job | None = session.get(Job, job_id)

        if job is None:
            return {"error": f"Job {job_id} not found"}

        # ── Mark as PROCESSING ────────────────────────────────
        job.status = JobStatus.PROCESSING
        session.commit()
        publish_job_update_sync(job_id, JobStatus.PROCESSING.value)

        try:
            # ── Simulate a long-running document job (~10 s) ──
            time.sleep(10)

            # ── Mock result data ──────────────────────────────
            result = {
                "document_id": job_id,
                "pages_processed": 42,
                "extracted_entities": 17,
                "summary": "Document successfully processed and indexed.",
                "processing_time_seconds": 10,
            }

            # ── Mark as COMPLETED ─────────────────────────────
            job.status = JobStatus.COMPLETED
            job.result = result
            session.commit()
            publish_job_update_sync(job_id, JobStatus.COMPLETED.value, result)

            return {"job_id": job_id, "status": "COMPLETED", "result": result}

        except Exception as exc:
            # ── Mark as FAILED ────────────────────────────────
            session.rollback()
            job.status = JobStatus.FAILED
            error_result = {
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }
            job.result = error_result
            session.commit()
            publish_job_update_sync(job_id, JobStatus.FAILED.value, error_result)

            return {"job_id": job_id, "status": "FAILED", "error": str(exc)}
