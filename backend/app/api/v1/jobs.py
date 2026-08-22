"""
Job management API endpoints.

Provides CRUD operations for submitting, querying, listing,
and cancelling asynchronous jobs, plus dedicated endpoints
for triggering bulk CSV transaction reports and CSV ingestion.
"""

import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, status
from fastapi.responses import FileResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.celery_app import celery_app
from app.database import get_db
from app.logging_config import get_logger
from app.models import Job, JobStatus
from app.schemas import JobCreate, JobListResponse, JobResponse, ReportRequest
from app.state import transition
from app.tasks import EXPORTS_DIR, TASK_REGISTRY, generate_bulk_csv_report, ingest_csv

log = get_logger(__name__)

router = APIRouter(prefix="/jobs", tags=["jobs"])

# ── Uploads directory ─────────────────────────────────────────────
UPLOADS_DIR = Path("uploads")
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)


# ── POST /api/v1/jobs — Submit a new job ──────────────────────────

@router.post(
    "",
    response_model=JobResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Submit a new job",
)
async def create_job(
    body: JobCreate,
    db: AsyncSession = Depends(get_db),
):
    """
    Create a new job and enqueue it for background processing.

    1. Resolve `job_type` against `TASK_REGISTRY`; reject unknown types with 400
       rather than silently dispatching the wrong task.
    2. Insert a new row in the `jobs` table with status `PENDING`, carrying the
       Celery task id we are about to dispatch under.
    3. Commit the transaction so the UUID is generated and the row is
       visible to Celery workers (avoids a race condition).
    4. Dispatch the registered Celery task asynchronously.
    5. Return the created job immediately (client polls or opens a WebSocket).
    """
    task = TASK_REGISTRY.get(body.job_type)
    if task is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Unknown job_type '{body.job_type}'. "
                f"Supported types: {sorted(TASK_REGISTRY)}"
            ),
        )

    # Mint the Celery task id here rather than reading it back off .delay(), so
    # it is committed *before* the task can run. Otherwise a job cancelled in
    # that window has no task id to revoke.
    task_id = str(uuid.uuid4())

    job = Job(
        job_type=body.job_type,
        payload=body.payload,
        status=JobStatus.PENDING,
        celery_task_id=task_id,
    )
    db.add(job)

    # Commit *before* dispatching to Celery so the row is visible to
    # the worker when it queries PostgreSQL.  The get_db dependency's
    # auto-commit at exit becomes a harmless no-op.
    await db.commit()
    await db.refresh(job)     # Populate server-generated defaults (id, timestamps)

    # Enqueue the Celery background task
    task.apply_async(args=[str(job.id)], task_id=task_id)
    log.info(
        "job.created",
        job_id=str(job.id),
        job_type=body.job_type,
        task_id=task_id,
    )

    return job


# ── POST /api/v1/jobs/reports — Generate a bulk CSV report ───────

@router.post(
    "/reports",
    response_model=JobResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Generate a bulk CSV transaction report",
)
async def create_report(
    body: ReportRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Submit a new bulk CSV report generation job.

    1. Create a `Job` row with type `bulk_csv_report` and the user/date
       range stored in `payload`.
    2. Dispatch the `generate_bulk_csv_report` Celery task.
    3. Return the job immediately — clients can poll or use WebSocket
       for live status updates.
    """
    task_id = str(uuid.uuid4())

    job = Job(
        job_type="bulk_csv_report",
        payload={
            "user_id": str(body.user_id),
            "start_date": body.start_date.isoformat(),
            "end_date": body.end_date.isoformat(),
        },
        status=JobStatus.PENDING,
        celery_task_id=task_id,
    )
    db.add(job)

    await db.commit()
    await db.refresh(job)

    # Enqueue the report generation task
    generate_bulk_csv_report.apply_async(args=[str(job.id)], task_id=task_id)
    log.info("report.requested", job_id=str(job.id), task_id=task_id)

    return job


# ── GET /api/v1/jobs/{job_id} — Get job details ──────────────────

@router.get(
    "/{job_id}",
    response_model=JobResponse,
    summary="Get job details",
)
async def get_job(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    """Retrieve a single job by its UUID."""
    job = await db.get(Job, job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )
    return job


# ── GET /api/v1/jobs — List all jobs (paginated) ─────────────────

@router.get(
    "",
    response_model=JobListResponse,
    summary="List all jobs",
)
async def list_jobs(
    status_filter: JobStatus | None = Query(
        default=None, alias="status", description="Filter by job status"
    ),
    job_type: str | None = Query(
        default=None, description="Filter by job type"
    ),
    limit: int = Query(default=20, ge=1, le=100, description="Page size"),
    offset: int = Query(default=0, ge=0, description="Offset"),
    db: AsyncSession = Depends(get_db),
):
    """
    List jobs with optional filtering by status and/or job_type.
    Supports pagination via `limit` and `offset`.
    """
    # Build base query
    query = select(Job)
    count_query = select(func.count(Job.id))

    if status_filter is not None:
        query = query.where(Job.status == status_filter)
        count_query = count_query.where(Job.status == status_filter)

    if job_type is not None:
        query = query.where(Job.job_type == job_type)
        count_query = count_query.where(Job.job_type == job_type)

    # Get total count
    total = (await db.execute(count_query)).scalar() or 0

    # Fetch paginated results, newest first
    query = query.order_by(Job.created_at.desc()).limit(limit).offset(offset)
    result = await db.execute(query)
    jobs = result.scalars().all()

    return JobListResponse(
        items=jobs,
        total=total,
        limit=limit,
        offset=offset,
    )


# ── POST /api/v1/jobs/{job_id}/cancel — Cancel a queued job ──────

@router.post(
    "/{job_id}/cancel",
    response_model=JobResponse,
    summary="Cancel a queued job",
)
async def cancel_job(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    """
    Cancel a job if it is still in PENDING or QUEUED status.

    Jobs that are already PROCESSING, COMPLETED, FAILED, or CANCELLED cannot be
    cancelled — the tasks have no cooperative abort, so accepting a mid-flight
    cancel would report something untrue.

    Two things stop a cancelled job from running. Revoking the Celery task keeps
    a worker from dequeuing it, but revocation is in-memory per worker and is
    lost if the worker restarts. The durable guarantee is the state machine: the
    task's claim of CANCELLED → PROCESSING is illegal, so even a worker that
    never saw the revoke refuses the work and leaves the row cancelled.
    """
    job = await db.get(Job, job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )

    if job.status not in (JobStatus.PENDING, JobStatus.QUEUED):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Cannot cancel job with status '{job.status.value}'. "
                   f"Only PENDING or QUEUED jobs can be cancelled.",
        )

    task_id = job.celery_task_id

    cancelled = await transition(
        db,
        job_id,
        JobStatus.CANCELLED,
        {"message": "Job cancelled by user"},
        strict=False,
    )
    if not cancelled:
        # A worker claimed the job between the check above and the row lock.
        await db.refresh(job)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Job started processing before it could be cancelled "
                   f"(now '{job.status.value}').",
        )

    # Best effort: the row is already authoritative, so a broker hiccup here
    # must not fail the request.
    if task_id:
        try:
            celery_app.control.revoke(task_id)
        except Exception:
            log.exception("job.revoke_failed", job_id=str(job_id), task_id=task_id)

    await db.refresh(job)
    log.info("job.cancelled", job_id=str(job_id), task_id=task_id)

    return job


# ── GET /api/v1/jobs/{job_id}/download — Download a generated report ──

@router.get(
    "/{job_id}/download",
    summary="Download a completed job's output file",
    response_class=FileResponse,
)
async def download_job_result(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    """
    Stream the CSV produced by a completed job back to the client.

    Without this the report task writes to a Docker volume that no client can
    reach, so a `bulk_csv_report` job could complete successfully and still be
    useless.
    """
    job = await db.get(Job, job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )

    if job.status is not JobStatus.COMPLETED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Job is '{job.status.value}'; only COMPLETED jobs have output.",
        )

    result = job.result or {}
    raw_path = result.get("file_path")
    if not raw_path:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="This job did not produce a downloadable file.",
        )

    # Resolve under EXPORTS_DIR and confirm containment: file_path comes out of
    # a JSONB column, so treat it as untrusted and refuse anything that escapes
    # the exports directory.
    exports_root = EXPORTS_DIR.resolve()
    file_path = Path(raw_path).resolve()
    if not file_path.is_relative_to(exports_root) or not file_path.is_file():
        log.warning("download.missing_or_escaped", job_id=str(job_id), path=raw_path)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Output file is no longer available.",
        )

    return FileResponse(
        path=file_path,
        media_type="text/csv",
        filename=result.get("file_name") or file_path.name,
    )


# ── POST /api/v1/jobs/ingest — Ingest a CSV file ─────────────────

@router.post(
    "/ingest",
    response_model=JobResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Ingest a CSV file for bulk processing",
)
async def ingest_csv_file(
    file: UploadFile,
    db: AsyncSession = Depends(get_db),
):
    """
    Upload a CSV file for asynchronous bulk ingestion.

    1. Validate the file is a .csv.
    2. Save the uploaded file to the uploads/ directory.
    3. Create a Job row with type ``csv_ingestion`` and status ``PENDING``.
    4. Dispatch the ``ingest_csv`` Celery task.
    5. Return the job immediately — the client opens a WebSocket on
       ``/ws/jobs/{job_id}`` to receive live progress (processed / total rows).
    """
    # ── Validate file type ────────────────────────────────────
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only .csv files are accepted.",
        )

    # ── Save the uploaded file ────────────────────────────────
    file_id = uuid.uuid4()
    saved_name = f"{file_id}_{file.filename}"
    file_path = UPLOADS_DIR / saved_name

    contents = await file.read()
    file_path.write_bytes(contents)

    # ── Create the job ────────────────────────────────────────
    task_id = str(uuid.uuid4())

    job = Job(
        job_type="csv_ingestion",
        payload={
            "file_name": file.filename,
            "saved_path": str(file_path),
        },
        status=JobStatus.PENDING,
        celery_task_id=task_id,
    )
    db.add(job)

    await db.commit()
    await db.refresh(job)

    # ── Dispatch the Celery task ──────────────────────────────
    ingest_csv.apply_async(args=[str(job.id), str(file_path)], task_id=task_id)
    log.info(
        "ingest.requested",
        job_id=str(job.id),
        task_id=task_id,
        file_name=file.filename,
    )

    return job
