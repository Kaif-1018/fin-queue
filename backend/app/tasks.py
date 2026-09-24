"""
Celery background tasks for the Async Job Processing Platform.

Celery workers are synchronous, so this module uses psycopg2 (sync driver)
rather than the asyncpg engine that FastAPI uses. Never import
``app.database.engine`` here, and never ``await`` inside a task.
"""

from __future__ import annotations

import csv
import traceback
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.celery_app import celery_app
from app.config import settings
from app.logging_config import get_logger
from app.models import Job, JobStatus, Transaction
from app.pubsub import publish_job_update_sync
from app.state import as_uuid, claim_sync, transition_sync

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
UPLOADS_DIR = Path("uploads")
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)

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
        1. Fetch the job from PostgreSQL and claim it (idempotency guard).
        2. Read the owner off the job row and the date range from the payload.
        3. Query the Transactions table with chunked streaming (yield_per).
        4. Stream chunks into a CSV file in the exports/ directory.
        5. On success → status = COMPLETED, store file_path + total_rows.
        6. On failure → status = FAILED, store error details.

    The user scoped is ``job.user_id`` — set from the authenticated caller when
    the job was created. It is deliberately *not* read from the payload: a
    caller-supplied user id is how this task used to export anybody's history.

    Every status change goes through ``transition_sync``, which locks the row,
    validates the move, commits, and publishes to Redis as one call.
    """
    with sync_session() as session:
        job: Job | None = session.get(Job, as_uuid(job_id))

        if job is None:
            log.warning("job.not_found", job_id=job_id, task="generate_bulk_csv_report")
            return {"error": f"Job {job_id} not found"}

        # ── Claim the job ─────────────────────────────────────
        # A locked compare-and-set to PROCESSING. task_acks_late=True means the
        # broker redelivers anything whose ack was lost, so this is what stops a
        # finished report from being regenerated — and a cancelled one from
        # running at all.
        if not claim_sync(session, job_id):
            log.info("report.claim_rejected", job_id=job_id, status=job.status.value)
            return {"job_id": job_id, "status": job.status.value, "skipped": True}

        log.info("report.started", job_id=job_id)

        # ── Resolve the owner ─────────────────────────────────
        # A NULL owner is a job that predates auth. Fail it rather than run the
        # query unscoped: an unfiltered export would dump every user's
        # transactions into a file the requester can download.
        user_id = job.user_id
        if user_id is None:
            error_result = {
                "error": (
                    "This job has no owner (created before authentication "
                    "existed). Resubmit it as a logged-in user."
                ),
            }
            transition_sync(session, job_id, JobStatus.FAILED, error_result)
            log.error("report.no_owner", job_id=job_id)
            return {"job_id": job_id, "status": "FAILED", "error": error_result["error"]}

        # ── Extract payload parameters ────────────────────────
        payload = job.payload or {}
        start_date = payload.get("start_date")
        end_date = payload.get("end_date")

        if not all([start_date, end_date]):
            error_result = {
                "error": "Missing required payload fields: start_date, end_date",
            }
            transition_sync(session, job_id, JobStatus.FAILED, error_result)
            log.error("report.invalid_payload", job_id=job_id)
            return {"job_id": job_id, "status": "FAILED", "error": error_result["error"]}

        # Parse date strings to datetime for comparison
        try:
            start_dt = datetime.fromisoformat(str(start_date))
            end_dt = datetime.fromisoformat(str(end_date))
        except (ValueError, TypeError) as e:
            error_result = {"error": f"Invalid date format: {e}"}
            transition_sync(session, job_id, JobStatus.FAILED, error_result)
            log.error("report.invalid_dates", job_id=job_id, error=str(e))
            return {"job_id": job_id, "status": "FAILED", "error": error_result["error"]}

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
                # str(): user_id is a uuid.UUID off the ORM row now, and this
                # dict is written to a JSONB column.
                "user_id": str(user_id),
                "start_date": start_date,
                "end_date": end_date,
            }

            transition_sync(session, job_id, JobStatus.COMPLETED, completed_result)
            log.info("report.completed", job_id=job_id, total_rows=total_rows)

            return {"job_id": job_id, "status": "COMPLETED", "result": completed_result}

        except Exception as exc:
            # ── Mark as FAILED ────────────────────────────────
            # Roll back first: the failed statement may have left the session
            # unusable, and transition_sync needs to issue a SELECT ... FOR UPDATE.
            session.rollback()
            error_result = {
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }
            # strict=False: if the status has moved on under us, log it rather
            # than raising a second exception that would mask this one.
            transition_sync(
                session, job_id, JobStatus.FAILED, error_result, strict=False
            )
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
        1. Fetch the job from PostgreSQL and claim it (idempotency guard).
        2. Count total rows in the CSV.
        3. Parse and batch-insert transactions into PostgreSQL, each attributed
           to the job's owner.
        4. Calculate financial metrics (total volume, status breakdown, date ranges).
        5. On success → status = COMPLETED, store rich analytics & sample rows.
        6. On failure → status = FAILED, store error details.

    **Every row is attributed to ``job.user_id``, and the CSV's own ``user_id``
    column is ignored.** Trusting that column meant an upload could write rows
    against any user id it named — and unparseable values were assigned a fresh
    random UUID, seeding the table with owners who did not exist.

    Status changes go through ``transition_sync``. Progress updates do not — see
    the comment on the first ``publish_job_update_sync`` call below.
    """
    with sync_session() as session:
        job: Job | None = session.get(Job, as_uuid(job_id))

        if job is None:
            log.warning("job.not_found", job_id=job_id, task="ingest_csv")
            return {"error": f"Job {job_id} not found"}

        # ── Claim the job ─────────────────────────────────────
        # This is the guard that matters most here: re-running a finished ingest
        # would insert every row a second time. The row lock makes the check and
        # the write to PROCESSING atomic, so two redelivered copies cannot both
        # get through.
        if not claim_sync(session, job_id):
            log.info("ingest.claim_rejected", job_id=job_id, status=job.status.value)
            return {"job_id": job_id, "status": job.status.value, "skipped": True}

        # ── Resolve the owner ─────────────────────────────────
        # Checked before any parsing: every inserted row needs an owner, and
        # there is no sensible value to invent for a job that predates auth.
        owner_id = job.user_id
        if owner_id is None:
            error_result = {
                "status": "FAILED",
                "error": (
                    "This job has no owner (created before authentication "
                    "existed). Re-upload the file as a logged-in user."
                ),
            }
            transition_sync(session, job_id, JobStatus.FAILED, error_result)
            log.error("ingest.no_owner", job_id=job_id)
            return {"job_id": job_id, "status": "FAILED", "error": error_result["error"]}

        # ── Resolve the source file ───────────────────────────
        file_path = file_path or (job.payload or {}).get("saved_path")
        if not file_path:
            error_result = {"error": "No source file: payload is missing 'saved_path'"}
            transition_sync(session, job_id, JobStatus.FAILED, error_result)
            log.error("ingest.no_source_file", job_id=job_id)
            return {"job_id": job_id, "status": "FAILED", "error": error_result["error"]}

        # Progress updates re-publish PROCESSING with a row count, so they are
        # *not* status changes and must not go through transition_sync —
        # PROCESSING → PROCESSING is illegal by design (that rule is what makes
        # the claim above work). Publish these directly. This first one gives the
        # UI something to show while the row count below is still running.
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
            min_date: datetime | None = None
            max_date: datetime | None = None
            sample_rows: list[dict] = []

            txn_batch: list[Transaction] = []

            with open(file_path, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Extract fields with safe defaults. Note what is *not* read:
                    # row["user_id"]. Ownership comes from the job, not the file.
                    parsed_amount = _parse_amount(row.get("amount", "0").strip())
                    raw_status = row.get("status", "completed").strip().lower()
                    parsed_date = _parse_timestamp(row.get("created_at", "").strip())

                    # Update running stats
                    total_amount += parsed_amount
                    status_counts[raw_status] += 1
                    status_amounts[raw_status] += parsed_amount

                    if min_date is None or parsed_date < min_date:
                        min_date = parsed_date
                    if max_date is None or parsed_date > max_date:
                        max_date = parsed_date

                    # Collect first 10 rows for UI preview table
                    if len(sample_rows) < 10:
                        sample_rows.append({
                            "id": row.get("id", processed + 1),
                            "user_id": str(owner_id),
                            "amount": _money(parsed_amount),
                            "status": raw_status,
                            "created_at": parsed_date.isoformat(),
                        })

                    # Queue transaction object
                    txn_batch.append(
                        Transaction(
                            user_id=owner_id,
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

            completed_result = {
                "status": "COMPLETED",
                "processed": processed,
                "total": total_rows,
                "file_name": Path(file_path).name,
                "message": f"Successfully ingested {processed:,} transactions into database",
                "summary": {
                    "total_amount": _money(total_amount),
                    "avg_amount": _money(avg_amount),
                    # Every row belongs to the uploader now, so the old
                    # unique_users_count / primary_user_id pair reduced to a
                    # constant 1 and the owner. Report the owner plainly, plus
                    # the distinct-status count, which still varies by file.
                    "owner_id": str(owner_id),
                    "distinct_statuses": len(status_counts),
                    "start_date": min_date.isoformat() if min_date else None,
                    "end_date": max_date.isoformat() if max_date else None,
                    "status_counts": dict(status_counts),
                    "status_amounts": {k: _money(v) for k, v in status_amounts.items()},
                },
                "sample_rows": sample_rows,
            }

            transition_sync(session, job_id, JobStatus.COMPLETED, completed_result)
            log.info("ingest.completed", job_id=job_id, processed=processed)

            return {"job_id": job_id, "status": "COMPLETED", "result": completed_result}

        except Exception as exc:
            # ── Mark as FAILED ────────────────────────────────
            # Roll back first: the failed statement may have left the session
            # unusable, and transition_sync needs to issue a SELECT ... FOR UPDATE.
            session.rollback()
            error_result = {
                "status": "FAILED",
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }
            # strict=False: never raise out of the error path and mask *this* error.
            transition_sync(
                session, job_id, JobStatus.FAILED, error_result, strict=False
            )
            log.exception("ingest.failed", job_id=job_id)

            return {"job_id": job_id, "status": "FAILED", "error": str(exc)}


# ── Job type → task registry ──────────────────────────────────────
# ``POST /api/v1/jobs`` dispatches through this map. A job_type that is not
# registered is rejected with 400 rather than silently running the wrong task.

TASK_REGISTRY = {
    "bulk_csv_report": generate_bulk_csv_report,
    "csv_ingestion": ingest_csv,
}


# ── Periodic maintenance tasks (Day 13) ───────────────────────────


@celery_app.task(bind=True, name="app.tasks.prune_stale_files")
def prune_stale_files(
    self,
    exports_retention_hours: int | None = None,
    uploads_retention_hours: int | None = None,
) -> dict[str, int]:
    """Prune exported reports and ingested upload files older than their retention threshold.

    Periodic maintenance run by Celery Beat to prevent Docker volumes (exports_data,
    uploads_data) from unbounded disk growth.
    """
    exp_hours = (
        exports_retention_hours
        if exports_retention_hours is not None
        else settings.EXPORTS_RETENTION_HOURS
    )
    up_hours = (
        uploads_retention_hours
        if uploads_retention_hours is not None
        else settings.UPLOADS_RETENTION_HOURS
    )

    now_ts = datetime.now(timezone.utc).timestamp()
    exp_cutoff_ts = now_ts - (exp_hours * 3600)
    up_cutoff_ts = now_ts - (up_hours * 3600)

    pruned_exports = 0
    pruned_uploads = 0
    bytes_freed = 0

    # Prune exports directory
    if EXPORTS_DIR.exists() and EXPORTS_DIR.is_dir():
        for path in EXPORTS_DIR.glob("*.csv"):
            try:
                stat = path.stat()
                if stat.st_mtime < exp_cutoff_ts:
                    size = stat.st_size
                    path.unlink(missing_ok=True)
                    pruned_exports += 1
                    bytes_freed += size
                    log.info("maintenance.pruned_export", file=path.name, size=size)
            except OSError as exc:
                log.warning("maintenance.prune_export_failed", file=path.name, error=str(exc))

    # Prune uploads directory
    if UPLOADS_DIR.exists() and UPLOADS_DIR.is_dir():
        for path in UPLOADS_DIR.glob("*.csv"):
            try:
                stat = path.stat()
                if stat.st_mtime < up_cutoff_ts:
                    size = stat.st_size
                    path.unlink(missing_ok=True)
                    pruned_uploads += 1
                    bytes_freed += size
                    log.info("maintenance.pruned_upload", file=path.name, size=size)
            except OSError as exc:
                log.warning("maintenance.prune_upload_failed", file=path.name, error=str(exc))

    summary = {
        "pruned_exports": pruned_exports,
        "pruned_uploads": pruned_uploads,
        "bytes_freed": bytes_freed,
    }
    log.info(
        "maintenance.prune_completed",
        exports=pruned_exports,
        uploads=pruned_uploads,
        bytes_freed=bytes_freed,
    )
    return summary


@celery_app.task(bind=True, name="app.tasks.reap_stale_jobs")
def reap_stale_jobs(
    self,
    stale_threshold_minutes: int | None = None,
) -> dict[str, Any]:
    """Find and fail zombie jobs stuck in PROCESSING status.

    When a worker process crashes, is OOM-killed, or the container restarts mid-job,
    the job remains in PROCESSING forever because claim_sync() rejects duplicate
    deliveries.

    This periodic task finds jobs whose status is PROCESSING and whose updated_at
    is older than `stale_threshold_minutes`, safely transitioning them to FAILED
    via `transition_sync()` and notifying connected WebSocket clients.
    """
    threshold = (
        stale_threshold_minutes
        if stale_threshold_minutes is not None
        else settings.STALE_JOB_THRESHOLD_MINUTES
    )
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=threshold)

    reaped_jobs: list[str] = []

    with sync_session() as session:
        stuck_jobs = (
            session.execute(
                select(Job.id).where(
                    Job.status == JobStatus.PROCESSING,
                    Job.updated_at < cutoff,
                )
            )
            .scalars()
            .all()
        )

        for job_uuid in stuck_jobs:
            job_id_str = str(job_uuid)
            error_result = {
                "status": "FAILED",
                "error": (
                    f"Job timed out: no worker activity detected for over "
                    f"{threshold} minutes (stale job reaper)"
                ),
                "reaped_at": datetime.now(timezone.utc).isoformat(),
            }
            try:
                transitioned = transition_sync(
                    session,
                    job_uuid,
                    JobStatus.FAILED,
                    error_result,
                    strict=False,
                )
                if transitioned:
                    reaped_jobs.append(job_id_str)
                    log.warning("maintenance.reaped_stale_job", job_id=job_id_str)
            except Exception as exc:
                log.exception("maintenance.reap_failed", job_id=job_id_str, error=str(exc))

    summary = {
        "reaped_count": len(reaped_jobs),
        "job_ids": reaped_jobs,
    }
    log.info("maintenance.reap_completed", count=len(reaped_jobs))
    return summary
