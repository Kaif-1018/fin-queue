"""
Job management API endpoints.

Provides CRUD operations for submitting, querying, listing,
and cancelling asynchronous jobs.
"""

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import Job, JobStatus
from app.schemas import JobCreate, JobListResponse, JobResponse
from app.tasks import process_document_task

router = APIRouter(prefix="/jobs", tags=["jobs"])


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

    1. Insert a new row in the `jobs` table with status `QUEUED`.
    2. Dispatch a Celery task to process the job asynchronously.
    3. Return the created job immediately (client polls for status).
    """
    job = Job(
        job_type=body.job_type,
        payload=body.payload,
        status=JobStatus.QUEUED,
    )
    db.add(job)
    await db.flush()          # Flush to generate the UUID without committing
    await db.refresh(job)     # Refresh to get server-generated defaults

    # Enqueue the Celery background task
    process_document_task.delay(str(job.id))

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
    Cancel a job if it is still in QUEUED status.
    Jobs that are already PROCESSING, COMPLETED, or FAILED cannot be cancelled.
    """
    job = await db.get(Job, job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )

    if job.status != JobStatus.QUEUED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Cannot cancel job with status '{job.status.value}'. "
                   f"Only QUEUED jobs can be cancelled.",
        )

    job.status = JobStatus.FAILED
    job.result = {"error": "Job cancelled by user"}
    await db.flush()
    await db.refresh(job)

    return job
