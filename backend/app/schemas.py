"""
Pydantic schemas for Job API request/response validation.
"""

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from app.models import JobStatus


# ── Request Schemas ───────────────────────────────────────────────

class JobCreate(BaseModel):
    """Schema for creating a new job."""
    job_type: str = Field(
        ...,
        min_length=1,
        max_length=128,
        examples=["document_extraction"],
        description="The type of job to execute.",
    )
    payload: dict[str, Any] | None = Field(
        default=None,
        examples=[{"file_name": "report.pdf", "pages": [1, 2, 3]}],
        description="Optional JSON payload with job parameters.",
    )


# ── Response Schemas ──────────────────────────────────────────────

class JobResponse(BaseModel):
    """Schema for returning a single job."""
    id: uuid.UUID
    job_type: str
    status: JobStatus
    payload: dict[str, Any] | None
    result: dict[str, Any] | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class JobListResponse(BaseModel):
    """Paginated list of jobs."""
    items: list[JobResponse]
    total: int
    limit: int
    offset: int
