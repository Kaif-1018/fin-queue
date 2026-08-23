import uuid
from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, EmailStr, Field

from app.models import JobStatus
from app.security import BCRYPT_MAX_BYTES


# ── Auth Schemas ──────────────────────────────────────────────────

class UserCreate(BaseModel):
    """Registration payload."""
    email: EmailStr = Field(
        ...,
        examples=["analyst@example.com"],
        description="Login identifier. Must be unique.",
    )
    # Capped at bcrypt's input limit rather than truncated: bcrypt hashes only
    # the first 72 bytes, so a longer password would let any suffix verify.
    password: str = Field(
        ...,
        min_length=8,
        max_length=BCRYPT_MAX_BYTES,
        description=(
            f"8–{BCRYPT_MAX_BYTES} characters. Non-ASCII characters cost more "
            f"than one byte against the upper bound."
        ),
    )


class UserResponse(BaseModel):
    """A user, as returned by the API.

    ``hashed_password`` is absent by design — response_model filtering is what
    keeps it from being serialised out of an endpoint by accident.
    """
    id: uuid.UUID
    email: EmailStr
    is_active: bool
    created_at: datetime

    model_config = {"from_attributes": True}


class Token(BaseModel):
    """OAuth2 bearer token response."""
    access_token: str
    token_type: str = "bearer"


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


class ReportRequest(BaseModel):
    """Schema for requesting a bulk CSV transaction report.

    There is deliberately no ``user_id`` field. The report is always scoped to
    the authenticated caller, derived from the bearer token in the handler. A
    field here would be a field an attacker could set, which is exactly how this
    endpoint used to let anyone export anyone else's transactions.

    ``extra="forbid"`` makes that explicit. Pydantic's default is to *ignore*
    unknown keys, so a request still carrying ``user_id`` would quietly succeed
    and return the caller's own data — safe, but silent. Rejecting it with a 422
    says plainly that the parameter is gone, rather than letting a caller believe
    a scoping request was honoured.
    """
    model_config = {"extra": "forbid"}

    start_date: date = Field(
        ...,
        examples=["2024-01-01"],
        description="Start of the date range (inclusive).",
    )
    end_date: date = Field(
        ...,
        examples=["2026-12-31"],
        description="End of the date range (inclusive).",
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
