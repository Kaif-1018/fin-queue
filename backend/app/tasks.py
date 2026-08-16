"""
Celery background tasks for the Async Job Processing Platform.

Celery workers are synchronous, so we use psycopg2 (sync driver) here
instead of asyncpg (async driver used by FastAPI).
"""

import csv
import os
import traceback
from datetime import datetime
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.celery_app import celery_app
from app.config import settings
from app.models import Job, JobStatus, Transaction
from app.pubsub import publish_job_update_sync

# ── Sync database engine for Celery workers ───────────────────────
# Convert async URL (postgresql+asyncpg://...) to sync (postgresql+psycopg2://...)
SYNC_DATABASE_URL = settings.DATABASE_URL.replace(
    "postgresql+asyncpg", "postgresql+psycopg2"
)

sync_engine = create_engine(SYNC_DATABASE_URL, pool_pre_ping=True)
SyncSession = sessionmaker(bind=sync_engine)

# ── Exports directory ─────────────────────────────────────────────
EXPORTS_DIR = Path("exports")
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

# ── CSV column headers ───────────────────────────────────────────
CSV_HEADERS = ["id", "user_id", "amount", "status", "created_at"]

# ── Chunk size for yield_per streaming ────────────────────────────
CHUNK_SIZE = 5_000


# ── Celery task ───────────────────────────────────────────────────

@celery_app.task(name="generate_bulk_csv_report", bind=True, max_retries=3)
def generate_bulk_csv_report(self, job_id: str) -> dict:
    """
    Generate a CSV report of transactions for a specific user and date range.

    Workflow:
        1. Fetch the job from PostgreSQL; extract user_id, start_date, end_date.
        2. Set status → PROCESSING.
        3. Query the Transactions table with chunked streaming (yield_per).
        4. Stream chunks into a CSV file in the exports/ directory.
        5. On success → status = COMPLETED, store file_path + total_rows.
        6. On failure → status = FAILED, store error details.
        7. Fire Redis Pub/Sub event for every status transition.
    """
    with SyncSession() as session:
        job: Job | None = session.get(Job, job_id)

        if job is None:
            return {"error": f"Job {job_id} not found"}

        # ── Extract payload parameters ────────────────────────
        payload = job.payload or {}
        user_id = payload.get("user_id")
        start_date = payload.get("start_date")
        end_date = payload.get("end_date")

        if not all([user_id, start_date, end_date]):
            job.status = JobStatus.FAILED
            error_result = {
                "error": "Missing required payload fields: user_id, start_date, end_date",
            }
            job.result = error_result
            session.commit()
            publish_job_update_sync(job_id, JobStatus.FAILED.value, error_result)
            return {"job_id": job_id, "status": "FAILED", "error": error_result["error"]}

        # Parse date strings to datetime for comparison
        try:
            start_dt = datetime.fromisoformat(str(start_date))
            end_dt = datetime.fromisoformat(str(end_date))
        except (ValueError, TypeError) as e:
            job.status = JobStatus.FAILED
            error_result = {"error": f"Invalid date format: {e}"}
            job.result = error_result
            session.commit()
            publish_job_update_sync(job_id, JobStatus.FAILED.value, error_result)
            return {"job_id": job_id, "status": "FAILED", "error": error_result["error"]}

        # ── Mark as PROCESSING ────────────────────────────────
        job.status = JobStatus.PROCESSING
        session.commit()
        publish_job_update_sync(job_id, JobStatus.PROCESSING.value)

        try:
            # ── Build the query ───────────────────────────────
            query = (
                select(Transaction)
                .where(Transaction.user_id == user_id)
                .where(Transaction.created_at >= start_dt)
                .where(Transaction.created_at <= end_dt)
                .order_by(Transaction.created_at)
            )

            # ── Stream results in chunks via yield_per ────────
            file_name = f"report_{job_id}.csv"
            file_path = EXPORTS_DIR / file_name
            total_rows = 0

            result = session.execute(query.execution_options(yield_per=CHUNK_SIZE))

            with open(file_path, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(CSV_HEADERS)

                for partition in result.partitions():
                    for row in partition:
                        txn = row[0]  # extract the Transaction ORM object
                        writer.writerow([
                            txn.id,
                            str(txn.user_id),
                            f"{txn.amount:.2f}",
                            txn.status,
                            txn.created_at.isoformat(),
                        ])
                        total_rows += 1

            # ── Mark as COMPLETED ─────────────────────────────
            completed_result = {
                "file_path": str(file_path),
                "file_name": file_name,
                "total_rows": total_rows,
                "user_id": user_id,
                "start_date": start_date,
                "end_date": end_date,
            }

            job.status = JobStatus.COMPLETED
            job.result = completed_result
            session.commit()
            publish_job_update_sync(job_id, JobStatus.COMPLETED.value, completed_result)

            return {"job_id": job_id, "status": "COMPLETED", "result": completed_result}

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
