"""
Celery background tasks for the Async Job Processing Platform.

Celery workers are synchronous, so we use psycopg2 (sync driver) here
instead of asyncpg (async driver used by FastAPI).
"""

import time
import random
import traceback

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.celery_app import celery_app
from app.config import settings
from app.models import Job, JobStatus

# ── Sync database engine for Celery workers ───────────────────────
# Convert async URL (postgresql+asyncpg://...) to sync (postgresql+psycopg2://...)
SYNC_DATABASE_URL = settings.DATABASE_URL.replace(
    "postgresql+asyncpg", "postgresql+psycopg2"
)

sync_engine = create_engine(SYNC_DATABASE_URL, pool_pre_ping=True)
SyncSession = sessionmaker(bind=sync_engine)


# ── Job handler registry ─────────────────────────────────────────
# Maps job_type strings to handler functions.
# Each handler receives the job payload dict and returns a result dict.

def handle_document_extraction(payload: dict) -> dict:
    """Simulate document extraction processing."""
    file_name = payload.get("file_name", "unknown.pdf")
    pages = payload.get("pages", [1])

    # Simulate work (2–5 seconds)
    processing_time = random.uniform(2, 5)
    time.sleep(processing_time)

    return {
        "file_name": file_name,
        "pages_processed": len(pages),
        "extracted_text_length": random.randint(500, 5000),
        "processing_time_seconds": round(processing_time, 2),
    }


def handle_data_export(payload: dict) -> dict:
    """Simulate data export processing."""
    format_ = payload.get("format", "csv")
    row_count = payload.get("row_count", 1000)

    processing_time = random.uniform(1, 4)
    time.sleep(processing_time)

    return {
        "format": format_,
        "rows_exported": row_count,
        "file_size_kb": random.randint(50, 2000),
        "processing_time_seconds": round(processing_time, 2),
    }


def handle_image_processing(payload: dict) -> dict:
    """Simulate image processing."""
    image_url = payload.get("image_url", "https://example.com/image.png")
    operation = payload.get("operation", "resize")

    processing_time = random.uniform(3, 8)
    time.sleep(processing_time)

    return {
        "image_url": image_url,
        "operation": operation,
        "output_resolution": "1920x1080",
        "processing_time_seconds": round(processing_time, 2),
    }


def handle_generic(payload: dict) -> dict:
    """Fallback handler for unknown job types."""
    processing_time = random.uniform(1, 3)
    time.sleep(processing_time)

    return {
        "message": "Generic job completed",
        "processing_time_seconds": round(processing_time, 2),
    }


JOB_HANDLERS = {
    "document_extraction": handle_document_extraction,
    "data_export": handle_data_export,
    "image_processing": handle_image_processing,
}


# ── Celery task ───────────────────────────────────────────────────

@celery_app.task(name="process_job", bind=True, max_retries=3)
def process_job_task(self, job_id: str) -> dict:
    """
    Process a job by ID.

    Workflow:
        1. Fetch the job from PostgreSQL
        2. Set status to PROCESSING
        3. Run the appropriate handler based on job_type
        4. On success → status = COMPLETED, store result
        5. On failure → status = FAILED, store error details
    """
    with SyncSession() as session:
        job: Job | None = session.get(Job, job_id)

        if job is None:
            return {"error": f"Job {job_id} not found"}

        # ── Mark as PROCESSING ────────────────────────────────
        job.status = JobStatus.PROCESSING
        session.commit()

        try:
            # ── Dispatch to handler ───────────────────────────
            handler = JOB_HANDLERS.get(job.job_type, handle_generic)
            result = handler(job.payload or {})

            # ── Mark as COMPLETED ─────────────────────────────
            job.status = JobStatus.COMPLETED
            job.result = result
            session.commit()

            return {"job_id": job_id, "status": "COMPLETED", "result": result}

        except Exception as exc:
            # ── Mark as FAILED ────────────────────────────────
            session.rollback()
            job.status = JobStatus.FAILED
            job.result = {
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }
            session.commit()

            return {"job_id": job_id, "status": "FAILED", "error": str(exc)}
