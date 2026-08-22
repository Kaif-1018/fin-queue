"""
Job status state machine — the single owner of every ``jobs.status`` write.

Nothing outside this module may assign ``job.status``. A status change is three
things that must happen together: a validated write, a commit, and a Redis
publish. When those are spelled out at each call site, one of them eventually
gets forgotten, and the job ends up ``COMPLETED`` in Postgres while the
dashboard still shows ``PROCESSING``. Routing every change through
:func:`transition` / :func:`transition_sync` makes that omission impossible.

Two flavours are provided, matching the naming in :mod:`app.pubsub`:

  • :func:`transition`      — async, for FastAPI handlers
  • :func:`transition_sync` — sync, for Celery tasks

The transition also doubles as an **atomic claim**. Because the row is locked
with ``SELECT ... FOR UPDATE`` before the check, ``PENDING → PROCESSING`` is a
race-free compare-and-set, which is what makes a redelivered task safe to drop:
``task_acks_late=True`` means the broker resends any task whose ack was lost, and
a second ingest of the same file would double-insert. See :data:`ALLOWED_TRANSITIONS`.
"""

from __future__ import annotations

import uuid as uuid_pkg
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from app.logging_config import get_logger
from app.models import Job, JobStatus
from app.pubsub import publish_job_update, publish_job_update_sync

log = get_logger(__name__)


# ── Exceptions ────────────────────────────────────────────────────


class TransitionError(Exception):
    """Base class for state-machine refusals."""


class JobNotFound(TransitionError):
    """The job row does not exist."""


class IllegalTransition(TransitionError):
    """The requested status change is not permitted from the current status."""

    def __init__(self, job_id: str, current: JobStatus, target: JobStatus) -> None:
        self.job_id = job_id
        self.current = current
        self.target = target
        super().__init__(
            f"Job {job_id}: cannot move {current.value} → {target.value}. "
            f"Allowed from {current.value}: "
            f"{sorted(s.value for s in ALLOWED_TRANSITIONS[current]) or ['(terminal)']}"
        )


# ── The state machine ─────────────────────────────────────────────

ALLOWED_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.PENDING: frozenset(
        {
            JobStatus.QUEUED,
            JobStatus.PROCESSING,
            JobStatus.CANCELLED,
            JobStatus.FAILED,
        }
    ),
    JobStatus.QUEUED: frozenset(
        {JobStatus.PROCESSING, JobStatus.CANCELLED, JobStatus.FAILED}
    ),
    # No PROCESSING → CANCELLED: the tasks have no cooperative abort, so
    # accepting a mid-flight cancel would report a lie to the user. The cancel
    # endpoint restricts itself to PENDING / QUEUED for the same reason.
    JobStatus.PROCESSING: frozenset({JobStatus.COMPLETED, JobStatus.FAILED}),
    # Terminal. Notably PROCESSING → PROCESSING is absent, which is what makes a
    # duplicate delivery bail instead of re-running.
    JobStatus.COMPLETED: frozenset(),
    JobStatus.FAILED: frozenset(),
    JobStatus.CANCELLED: frozenset(),
}

TERMINAL_STATUSES: frozenset[JobStatus] = frozenset(
    {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED}
)


def is_terminal(status: JobStatus) -> bool:
    """True if *status* admits no further transitions."""
    return status in TERMINAL_STATUSES


def as_uuid(job_id: str | uuid_pkg.UUID) -> uuid_pkg.UUID:
    """Coerce a job id to UUID — Celery hands them over as strings."""
    return job_id if isinstance(job_id, uuid_pkg.UUID) else uuid_pkg.UUID(str(job_id))


def _check(
    job: Job | None,
    job_id: str,
    target: JobStatus,
    *,
    strict: bool,
) -> bool:
    """Validate a pending transition, raising or logging according to *strict*.

    Returns True when the caller should proceed with the write.
    """
    if job is None:
        log.warning("job.transition_no_such_job", job_id=job_id, target=target.value)
        if strict:
            raise JobNotFound(f"Job {job_id} not found")
        return False

    if target not in ALLOWED_TRANSITIONS[job.status]:
        log.warning(
            "job.transition_rejected",
            job_id=job_id,
            current=job.status.value,
            target=target.value,
        )
        if strict:
            raise IllegalTransition(job_id, job.status, target)
        return False

    return True


# ── Async flavour (FastAPI handlers) ──────────────────────────────


async def transition(
    session: AsyncSession,
    job_id: str | uuid_pkg.UUID,
    target: JobStatus,
    result: dict[str, Any] | None = None,
    *,
    strict: bool = True,
) -> bool:
    """Move *job_id* to *target*, then commit and publish — as one call.

    Locks the job row, validates ``current → target`` against
    :data:`ALLOWED_TRANSITIONS`, writes, commits, and publishes to Redis.

    With ``strict=True`` (the default) an illegal transition raises
    :class:`IllegalTransition` and a missing job raises :class:`JobNotFound`;
    the write is a bug worth surfacing. With ``strict=False`` the refusal is
    logged and ``False`` returned, which is what a claim site wants.

    Do not leave uncommitted work in *session* across this call — it commits.
    """
    uid = as_uuid(job_id)
    job = (
        await session.execute(select(Job).where(Job.id == uid).with_for_update())
    ).scalar_one_or_none()

    if not _check(job, str(job_id), target, strict=strict):
        await session.rollback()  # release the row lock
        return False

    assert job is not None  # narrowed by _check
    previous = job.status
    job.status = target
    if result is not None:
        job.result = result

    await session.commit()

    try:
        await publish_job_update(str(job_id), target.value, result)
    except Exception:
        # A publish failure must not undo a committed status. The WebSocket
        # handler sends current DB state on connect, so the next client to
        # attach recovers the truth. See app/api/v1/ws.py.
        log.exception("job.publish_failed", job_id=str(job_id), status=target.value)

    log.info(
        "job.transitioned",
        job_id=str(job_id),
        **{"from": previous.value, "to": target.value},
    )
    return True


# ── Sync flavour (Celery tasks) ───────────────────────────────────


def transition_sync(
    session: Session,
    job_id: str | uuid_pkg.UUID,
    target: JobStatus,
    result: dict[str, Any] | None = None,
    *,
    strict: bool = True,
) -> bool:
    """Synchronous :func:`transition`, for use inside Celery tasks.

    Reuses the caller's session rather than opening its own: the worker's sync
    pool and the API's async pool already compete for ``max_connections``, and a
    second connection per status change would make that worse.

    Do not leave uncommitted work in *session* across this call — it commits.
    """
    uid = as_uuid(job_id)
    job = session.execute(
        select(Job).where(Job.id == uid).with_for_update()
    ).scalar_one_or_none()

    if not _check(job, str(job_id), target, strict=strict):
        session.rollback()  # release the row lock
        return False

    assert job is not None  # narrowed by _check
    previous = job.status
    job.status = target
    if result is not None:
        job.result = result

    session.commit()

    try:
        publish_job_update_sync(str(job_id), target.value, result)
    except Exception:
        log.exception("job.publish_failed", job_id=str(job_id), status=target.value)

    log.info(
        "job.transitioned",
        job_id=str(job_id),
        **{"from": previous.value, "to": target.value},
    )
    return True


# ── Claim helper ──────────────────────────────────────────────────


def claim_sync(session: Session, job_id: str | uuid_pkg.UUID) -> bool:
    """Attempt to claim a job for processing. True if this worker owns it.

    This is the idempotency guard every task must open with. Because the row is
    locked before the check, the claim is a race-free compare-and-set:

    ==========================  ================  =========================
    Redelivery scenario         Current status    Result
    ==========================  ================  =========================
    ack lost after success      COMPLETED         rejected, no double-insert
    two duplicates race         PROCESSING        loser rejected, one runs
    cancelled before dequeue    CANCELLED         rejected, stays cancelled
    ==========================  ================  =========================
    """
    return transition_sync(session, job_id, JobStatus.PROCESSING, strict=False)
