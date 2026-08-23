"""
SQLAlchemy ORM models for the Async Job Processing Platform.
"""

import enum
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class User(Base):
    """An authenticated account. Owns jobs, and the transactions they ingest."""

    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    # 320 = 64-char local part + "@" + 255-char domain, the RFC 5321 maximum.
    email: Mapped[str] = mapped_column(
        String(320),
        nullable=False,
        unique=True,
        index=True,
    )
    # The bcrypt hash, never the password. Excluded from UserResponse so it
    # cannot be serialised out of an endpoint by accident.
    hashed_password: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default=text("true"),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        server_default=text("now()"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        server_default=text("now()"),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    def __repr__(self) -> str:
        return f"<User id={self.id} email={self.email}>"


class JobStatus(str, enum.Enum):
    """Possible states of a job.

    The permitted moves between these live in :data:`app.state.ALLOWED_TRANSITIONS`,
    and every write goes through ``app.state.transition``. CANCELLED is distinct
    from FAILED on purpose: a job the user stopped is not a job that broke.
    """
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class Job(Base):
    """Represents an asynchronous job submitted to the platform."""

    __tablename__ = "jobs"
    __table_args__ = (
        # Every job listing filters by owner and orders by created_at DESC, so
        # this pair is the hot path. Mirrors ix_transactions_user_date below.
        Index("ix_jobs_user_created", "user_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    # Nullable because jobs created before auth existed have no owner. Those
    # rows are unreachable by design: every query filters on an authenticated
    # user's id, and NULL matches nobody. Do not treat a NULL owner as
    # "belongs to everyone" — in the report task that would mean a full-table
    # export. See generate_bulk_csv_report in tasks.py.
    user_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=True,
        default=None,
    )
    job_type: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        index=True,
    )
    status: Mapped[JobStatus] = mapped_column(
        # create_constraint=False: migration 001 creates a native Postgres enum
        # with no CHECK constraint. Asking for one here makes --autogenerate
        # propose adding a constraint the real schema does not have.
        Enum(JobStatus, name="job_status", create_constraint=False),
        nullable=False,
        default=JobStatus.PENDING,
        server_default=text("'PENDING'"),
        index=True,
    )
    payload: Mapped[dict | None] = mapped_column(
        JSONB,
        nullable=True,
        default=None,
    )
    result: Mapped[dict | None] = mapped_column(
        JSONB,
        nullable=True,
        default=None,
    )
    # Set once the broker accepts the dispatch, so cancelling a job can revoke
    # the task instead of letting a worker dequeue work nobody wants. Nullable:
    # a row exists briefly before .delay() returns, and rows predating this
    # column have none.
    celery_task_id: Mapped[str | None] = mapped_column(
        String(155),
        nullable=True,
        default=None,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        server_default=text("now()"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        server_default=text("now()"),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    def __repr__(self) -> str:
        return f"<Job id={self.id} type={self.job_type} status={self.status}>"


class Transaction(Base):
    """Financial transaction record — seeded with 50K rows for report generation."""

    __tablename__ = "transactions"
    __table_args__ = (
        Index("ix_transactions_user_date", "user_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )
    # Deliberately *not* a ForeignKey to users.id. Rows seeded before auth
    # existed hold UUIDs that were never real users, so the constraint could
    # not validate. Rows written by ingest_csv now carry the uploading job's
    # owner (the CSV's own user_id column is ignored), so new data is
    # consistent; a real FK becomes possible after a full reseed, or via
    # ADD CONSTRAINT ... NOT VALID to enforce on new rows only.
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
        index=True,
    )
    # Money: Numeric(12, 2) maps to Decimal, never float. See CLAUDE.md.
    amount: Mapped[Decimal] = mapped_column(
        Numeric(12, 2),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        server_default=text("now()"),
    )

    def __repr__(self) -> str:
        return f"<Transaction id={self.id} user={self.user_id} amount={self.amount}>"
