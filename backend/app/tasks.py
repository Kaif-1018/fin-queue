"""
Celery background tasks for the Async Job Processing Platform.

Celery workers are synchronous, so this module uses psycopg2 (sync driver)
rather than the asyncpg engine that FastAPI uses. Never import
``app.database.engine`` here, and never ``await`` inside a task.
"""

from __future__ import annotations

import csv
import traceback
import uuid as uuid_pkg
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path

from sqlalchemy import Engine, create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.celery_app import celery_app
from app.config import settings
from app.logging_config import get_logger
from app.models import Job, JobStatus, Transaction
from app.pubsub import publish_job_update_sync

log = get_logger(__name__)


# ── Sync database engine for Celery workers ───────────────────────
# Built lazily rather than at import time so tests can point
# ``settings.DATABASE_URL`` at a throwaway database before the first task runs.

_sync_engine: Engine | None = None
_SyncSession: sessionmaker[Session] | None = None


def _session_factory() -> sessionmaker[Session]:
    """Return the sync session factory, creating the engine on first use."""
    global _sync_engine, _SyncSession
    if _SyncSession is None:
        sync_url = settings.DATABASE_URL.replace(
            "postgresql+asyncpg", "postgresql+psycopg2"
        )
        _sync_engine = create_engine(sync_url, pool_pre_ping=True)
        _SyncSession = sessionmaker(bind=_sync_engine)
    return _SyncSession


@contextmanager
def sync_session() -> Iterator[Session]:
    """Yield a synchronous SQLAlchemy session for use inside a Celery task."""
    with _session_factory()() as session:
        yield session


def reset_sync_engine() -> None:
    """Dispose the sync engine so the next call rebuilds it.

    Used by the test suite after rebinding ``DATABASE_URL``.
    """
    global _sync_engine, _SyncSession
    if _sync_engine is not None:
        _sync_engine.dispose()
    _sync_engine = None
    _SyncSession = None


# ── Directories & constants ───────────────────────────────────────
EXPORTS_DIR = Path("exports")
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

CSV_HEADERS = ["id", "user_id", "amount", "status", "created_at"]

# Rows fetched per round-trip when streaming the transactions table out.
CHUNK_SIZE = 5_000

# Rows buffered before flushing an INSERT batch during ingestion.
BATCH_SIZE = 500

# Number of progress messages published over the life of an ingest job.
PROGRESS_UPDATES = 25

CENTS = Decimal("0.01")


# ── Money helpers ─────────────────────────────────────────────────
# Money is Decimal end to end and only becomes a string at the JSON boundary.
# Binary floats lose cents, and the loss compounds across tens of thousands of
# rows. JSON has no decimal type, so strings preserve exactness on the wire.


def _money(value: Decimal) -> str:
    """Quantise *value* to cents and render it as an exact decimal string."""
    return str(value.quantize(CENTS, rounding=ROUND_HALF_UP))


def _parse_amount(raw: str) -> Decimal:
    """Parse a CSV amount cell into a Decimal, falling back to zero."""
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(0)


def _parse_uuid(raw: str) -> uuid_pkg.UUID:
    """Parse a CSV user_id cell, minting a fresh UUID if it is unusable."""
    try:
        return uuid_pkg.UUID(raw) if raw else uuid_pkg.uuid4()
    except ValueError:
        return uuid_pkg.uuid4()


def _parse_timestamp(raw: str) -> datetime:
    """Parse a CSV timestamp cell, falling back to now()."""
    try:
        return datetime.fromisoformat(raw) if raw else datetime.now(timezone.utc)
    except (ValueError, TypeError):
        return datetime.now(timezone.utc)


def _count_csv_rows(file_path: str) -> int:
    """Count data rows in a CSV, excluding the header.

    Uses ``csv.reader`` rather than counting newlines so that quoted fields
    containing newlines are not miscounted.
    """
    with open(file_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)  # discard header
        return sum(1 for _ in reader)


# ── Celery task: stream transactions out to a CSV report ──────────


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
    with sync_session() as session:
        job: Job | None = session.get(Job, job_id)

        if job is None:
            log.warning("job.not_found", job_id=job_id, task="generate_bulk_csv_report")
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
            log.error("report.invalid_payload", job_id=job_id)
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
            log.error("report.invalid_dates", job_id=job_id, error=str(e))
            return {"job_id": job_id, "status": "FAILED", "error": error_result["error"]}

        # ── Mark as PROCESSING ────────────────────────────────
        job.status = JobStatus.PROCESSING
        session.commit()
        publish_job_update_sync(job_id, JobStatus.PROCESSING.value)
        log.info("report.started", job_id=job_id, user_id=user_id)

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
                            _money(txn.amount),
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
            log.info("report.completed", job_id=job_id, total_rows=total_rows)

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
            log.exception("report.failed", job_id=job_id)

            return {"job_id": job_id, "status": "FAILED", "error": str(exc)}


# ── Celery task: CSV ingestion with DB persistence & analytics ────


@celery_app.task(name="ingest_csv", bind=True, max_retries=3)
def ingest_csv(self, job_id: str, file_path: str | None = None) -> dict:
    """
    Ingest a CSV file into PostgreSQL and publish live progress updates.

    *file_path* may be passed explicitly or omitted, in which case it is read
    from ``job.payload["saved_path"]``. Keeping it optional lets every task in
    ``TASK_REGISTRY`` share the same ``(job_id)`` dispatch signature.

    Workflow:
        1. Fetch the job from PostgreSQL.
        2. Set status → PROCESSING.
        3. Count total rows in the CSV.
        4. Parse and batch-insert transactions into PostgreSQL.
        5. Calculate financial metrics (total volume, status breakdown, date ranges).
        6. On success → status = COMPLETED, store rich analytics & sample rows.
        7. On failure → status = FAILED, store error details.
    """
    with sync_session() as session:
        job: Job | None = session.get(Job, job_id)

        if job is None:
            log.warning("job.not_found", job_id=job_id, task="ingest_csv")
            return {"error": f"Job {job_id} not found"}

        # ── Resolve the source file ───────────────────────────
        file_path = file_path or (job.payload or {}).get("saved_path")
        if not file_path:
            job.status = JobStatus.FAILED
            error_result = {"error": "No source file: payload is missing 'saved_path'"}
            job.result = error_result
            session.commit()
            publish_job_update_sync(job_id, JobStatus.FAILED.value, error_result)
            log.error("ingest.no_source_file", job_id=job_id)
            return {"job_id": job_id, "status": "FAILED", "error": error_result["error"]}

        # ── Mark as PROCESSING ────────────────────────────────
        job.status = JobStatus.PROCESSING
        session.commit()
        publish_job_update_sync(
            job_id, JobStatus.PROCESSING.value, {"processed": 0, "total": 0}
        )

        try:
            # ── Count total rows (excluding header) ───────────
            total_rows = _count_csv_rows(file_path)

            progress_interval = (
                max(1, total_rows // PROGRESS_UPDATES) if total_rows > 0 else 1
            )

            log.info(
                "ingest.started", job_id=job_id, file=file_path, total_rows=total_rows
            )

            publish_job_update_sync(
                job_id,
                JobStatus.PROCESSING.value,
                {"processed": 0, "total": total_rows},
            )

            # ── Process & Insert Transactions in Batches ──────
            processed = 0
            total_amount = Decimal(0)
            status_counts: defaultdict[str, int] = defaultdict(int)
            status_amounts: defaultdict[str, Decimal] = defaultdict(lambda: Decimal(0))
            user_ids: set[str] = set()
            min_date: datetime | None = None
            max_date: datetime | None = None
            sample_rows: list[dict] = []

            txn_batch: list[Transaction] = []

            with open(file_path, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Extract fields with safe defaults
                    parsed_user_id = _parse_uuid(row.get("user_id", "").strip())
                    parsed_amount = _parse_amount(row.get("amount", "0").strip())
                    raw_status = row.get("status", "completed").strip().lower()
                    parsed_date = _parse_timestamp(row.get("created_at", "").strip())

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
                            "amount": _money(parsed_amount),
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

                    # Publish live progress update
                    if processed % progress_interval == 0 or processed == total_rows:
                        publish_job_update_sync(
                            job_id,
                            JobStatus.PROCESSING.value,
                            {
                                "processed": processed,
                                "total": total_rows,
                                "total_amount": _money(total_amount),
                            },
                        )

            # Flush remaining batch
            if txn_batch:
                session.add_all(txn_batch)
                session.commit()
                txn_batch.clear()

            # ── Mark as COMPLETED with rich analytics ─────────
            avg_amount = total_amount / processed if processed > 0 else Decimal(0)
            primary_user_id = next(iter(user_ids)) if user_ids else None

            completed_result = {
                "status": "COMPLETED",
                "processed": processed,
                "total": total_rows,
                "file_name": Path(file_path).name,
                "message": f"Successfully ingested {processed:,} transactions into database",
                "summary": {
                    "total_amount": _money(total_amount),
                    "avg_amount": _money(avg_amount),
                    "primary_user_id": primary_user_id,
                    "unique_users_count": len(user_ids),
                    "start_date": min_date.isoformat() if min_date else None,
                    "end_date": max_date.isoformat() if max_date else None,
                    "status_counts": dict(status_counts),
                    "status_amounts": {k: _money(v) for k, v in status_amounts.items()},
                },
                "sample_rows": sample_rows,
            }

            job.status = JobStatus.COMPLETED
            job.result = completed_result
            session.commit()
            publish_job_update_sync(job_id, JobStatus.COMPLETED.value, completed_result)
            log.info("ingest.completed", job_id=job_id, processed=processed)

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
            log.exception("ingest.failed", job_id=job_id)

            return {"job_id": job_id, "status": "FAILED", "error": str(exc)}


# ── Job type → task registry ──────────────────────────────────────
# ``POST /api/v1/jobs`` dispatches through this map. A job_type that is not
# registered is rejected with 400 rather than silently running the wrong task.

TASK_REGISTRY = {
    "bulk_csv_report": generate_bulk_csv_report,
    "csv_ingestion": ingest_csv,
}
