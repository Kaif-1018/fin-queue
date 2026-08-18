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


# ── Celery task: CSV ingestion with DB persistence & analytics ────

@celery_app.task(name="ingest_csv", bind=True, max_retries=3)
def ingest_csv(self, job_id: str, file_path: str) -> dict:
    """
    Ingest a CSV file into PostgreSQL and publish live progress updates.

    Workflow:
        1. Fetch the job from PostgreSQL.
        2. Set status → PROCESSING.
        3. Count total rows in the CSV.
        4. Parse and batch-insert transactions into PostgreSQL.
        5. Calculate financial metrics (total volume, status breakdown, date ranges).
        6. On success → status = COMPLETED, store rich analytics & sample rows.
        7. On failure → status = FAILED, store error details.
    """
    import time
    from collections import defaultdict
    import uuid as uuid_pkg

    BATCH_SIZE = 500

    with SyncSession() as session:
        job: Job | None = session.get(Job, job_id)

        if job is None:
            return {"error": f"Job {job_id} not found"}

        # ── Mark as PROCESSING ────────────────────────────────
        job.status = JobStatus.PROCESSING
        session.commit()
        publish_job_update_sync(job_id, JobStatus.PROCESSING.value, {"processed": 0, "total": 0})

        try:
            # ── Count total rows (excluding header) ───────────
            with open(file_path, "r", encoding="utf-8") as f:
                total_rows = sum(1 for _ in f) - 1
                total_rows = max(total_rows, 0)

            progress_interval = max(1, total_rows // 25) if total_rows > 0 else 1

            publish_job_update_sync(
                job_id,
                JobStatus.PROCESSING.value,
                {"processed": 0, "total": total_rows},
            )

            # ── Process & Insert Transactions in Batches ──────
            processed = 0
            total_amount = 0.0
            status_counts = defaultdict(int)
            status_amounts = defaultdict(float)
            user_ids = set()
            min_date = None
            max_date = None
            sample_rows = []

            txn_batch = []

            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Extract fields with safe defaults
                    raw_user_id = row.get("user_id", "").strip()
                    raw_amount = row.get("amount", "0").strip()
                    raw_status = row.get("status", "completed").strip().lower()
                    raw_date = row.get("created_at", "").strip()

                    try:
                        parsed_user_id = uuid_pkg.UUID(raw_user_id) if raw_user_id else uuid_pkg.uuid4()
                    except ValueError:
                        parsed_user_id = uuid_pkg.uuid4()

                    try:
                        parsed_amount = float(raw_amount)
                    except ValueError:
                        parsed_amount = 0.0

                    try:
                        parsed_date = datetime.fromisoformat(raw_date) if raw_date else datetime.now(timezone.utc)
                    except ValueError:
                        parsed_date = datetime.now(timezone.utc)

                    # Update running stats
                    total_amount += parsed_amount
                    status_counts[raw_status] += 1
                    status_amounts[raw_status] += parsed_amount
                    user_ids.add(str(parsed_user_id))

                    if min_date is None or parsed_date < min_date:
                        min_date = parsed_date
                    if max_date is None or parsed_date > max_date:
                        max_date = parsed_date

                    # Collect first 10 rows for UI preview table
                    if len(sample_rows) < 10:
                        sample_rows.append({
                            "id": row.get("id", processed + 1),
                            "user_id": str(parsed_user_id),
                            "amount": round(parsed_amount, 2),
                            "status": raw_status,
                            "created_at": parsed_date.isoformat(),
                        })

                    # Queue transaction object
                    txn_batch.append(
                        Transaction(
                            user_id=parsed_user_id,
                            amount=parsed_amount,
                            status=raw_status,
                            created_at=parsed_date,
                        )
                    )

                    processed += 1

                    # Batch insert to DB
                    if len(txn_batch) >= BATCH_SIZE:
                        session.add_all(txn_batch)
                        session.commit()
                        txn_batch.clear()

                    # Throttle slightly to keep WebSocket smooth
                    if processed % 20 == 0:
                        time.sleep(0.005)

                    # Publish live progress update
                    if processed % progress_interval == 0 or processed == total_rows:
                        publish_job_update_sync(
                            job_id,
                            JobStatus.PROCESSING.value,
                            {
                                "processed": processed,
                                "total": total_rows,
                                "total_amount": round(total_amount, 2),
                            },
                        )

            # Flush remaining batch
            if txn_batch:
                session.add_all(txn_batch)
                session.commit()
                txn_batch.clear()

            # ── Mark as COMPLETED with rich analytics ─────────
            avg_amount = round(total_amount / processed, 2) if processed > 0 else 0.0
            primary_user_id = next(iter(user_ids)) if user_ids else None

            completed_result = {
                "status": "COMPLETED",
                "processed": processed,
                "total": total_rows,
                "file_name": Path(file_path).name,
                "message": f"Successfully ingested {processed:,} transactions into database",
                "summary": {
                    "total_amount": round(total_amount, 2),
                    "avg_amount": avg_amount,
                    "primary_user_id": primary_user_id,
                    "unique_users_count": len(user_ids),
                    "start_date": min_date.isoformat() if min_date else None,
                    "end_date": max_date.isoformat() if max_date else None,
                    "status_counts": dict(status_counts),
                    "status_amounts": {k: round(v, 2) for k, v in status_amounts.items()},
                },
                "sample_rows": sample_rows,
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
                "status": "FAILED",
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }
            job.result = error_result
            session.commit()
            publish_job_update_sync(job_id, JobStatus.FAILED.value, error_result)

            return {"job_id": job_id, "status": "FAILED", "error": str(exc)}
