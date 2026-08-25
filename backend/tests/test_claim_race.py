"""
The atomic claim, exercised with genuinely concurrent connections.

``claim_sync`` is the entire idempotency guarantee of this platform.
``task_acks_late=True`` means the broker redelivers any task whose ack was lost,
and a second ingest of the same file would insert every row twice — so the claim
has to be a real compare-and-set, not a check followed by a write.

These tests use threads and separate psycopg2 sessions on purpose. The race lives
in Postgres' row lock, so a single-connection test cannot reach it: two coroutines
on one connection would serialise for reasons that have nothing to do with
``SELECT ... FOR UPDATE``.
"""

from __future__ import annotations

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from sqlalchemy.orm import Session

from app.models import Job, JobStatus
from app.state import (
    IllegalTransition,
    JobNotFound,
    claim_sync,
    transition_sync,
)
from app.tasks import sync_session

# Generous: these only ever wait on a row lock held for microseconds. A timeout
# here means the claim deadlocked, which is a failure, not slowness.
LOCK_TIMEOUT = 30


@pytest.fixture
def job_id_at(make_sync_job: Any) -> Any:
    """A committed job in *status*, reduced to its id.

    The claim is owner-agnostic — it only reads and writes ``status`` — so these
    tests care about nothing else on the row.
    """

    def _make(status: JobStatus = JobStatus.PENDING) -> uuid.UUID:
        return make_sync_job(status=status).id

    return _make


def _claim_in_new_session(job_id: uuid.UUID, barrier: threading.Barrier) -> bool:
    """Claim *job_id* on a fresh connection, synchronised with its rivals.

    The barrier makes every thread arrive at the claim together; without it the
    first thread routinely finishes before the second starts and the test proves
    nothing.
    """
    with sync_session() as session:
        barrier.wait(timeout=LOCK_TIMEOUT)
        return claim_sync(session, job_id)


# ── The race ──────────────────────────────────────────────────────


def test_two_concurrent_claims_and_exactly_one_wins(sync_db: Session, job_id_at: Any):
    """The redelivered-duplicate case: both copies of a task start at once."""
    job_id = job_id_at(JobStatus.PENDING)
    barrier = threading.Barrier(2)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(_claim_in_new_session, job_id, barrier) for _ in range(2)
        ]
        results = [f.result(timeout=LOCK_TIMEOUT) for f in futures]

    assert sorted(results) == [False, True], results

    sync_db.expire_all()
    assert sync_db.get(Job, job_id).status is JobStatus.PROCESSING


def test_a_crowd_of_concurrent_claims_still_yields_one_winner(
    sync_db: Session, job_id_at: Any
):
    """Eight simultaneous claims. The lock has to serialise all of them."""
    job_id = job_id_at(JobStatus.PENDING)
    workers = 8
    barrier = threading.Barrier(workers)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(_claim_in_new_session, job_id, barrier) for _ in range(workers)
        ]
        results = [f.result(timeout=LOCK_TIMEOUT) for f in futures]

    assert sum(results) == 1, f"{sum(results)} workers claimed the same job"

    sync_db.expire_all()
    assert sync_db.get(Job, job_id).status is JobStatus.PROCESSING


# ── Sequential rejections ─────────────────────────────────────────


def test_first_claim_succeeds_and_moves_the_job_to_processing(
    sync_db: Session, job_id_at: Any
):
    job_id = job_id_at(JobStatus.PENDING)

    with sync_session() as session:
        assert claim_sync(session, job_id) is True

    sync_db.expire_all()
    assert sync_db.get(Job, job_id).status is JobStatus.PROCESSING


def test_a_second_claim_on_a_running_job_is_refused(sync_db: Session, job_id_at: Any):
    job_id = job_id_at(JobStatus.PROCESSING)

    with sync_session() as session:
        assert claim_sync(session, job_id) is False


@pytest.mark.parametrize(
    ("status", "scenario"),
    [
        (JobStatus.COMPLETED, "ack lost after the work finished"),
        (JobStatus.FAILED, "redelivered after a failure was recorded"),
        (JobStatus.CANCELLED, "cancelled before a worker dequeued it"),
    ],
)
def test_a_finished_job_is_never_reclaimed(
    sync_db: Session, job_id_at: Any, status: JobStatus, scenario: str
):
    """Each of these is a real redelivery scenario, not a hypothetical.

    The CANCELLED case is the durable half of cancellation: Celery's revoke is
    in-memory per worker and lost on restart, so a worker that never saw the
    revoke still refuses the work because of this rule.
    """
    job_id = job_id_at(status)

    with sync_session() as session:
        assert claim_sync(session, job_id) is False, scenario

    sync_db.expire_all()
    assert sync_db.get(Job, job_id).status is status


def test_claiming_a_missing_job_is_refused_not_raised(sync_db: Session):
    """Tasks call ``claim_sync`` with ``strict=False`` semantics baked in — a
    vanished row must return False rather than raise into the task body."""
    with sync_session() as session:
        assert claim_sync(session, uuid.uuid4()) is False


def test_a_rejected_claim_leaves_the_session_usable(sync_db: Session, job_id_at: Any):
    """The refusal path rolls back, which is what releases the row lock.

    Without that rollback the caller holds a lock on a row it did not claim, and
    the next worker to try blocks until the session is garbage collected.
    """
    job_id = job_id_at(JobStatus.COMPLETED)

    with sync_session() as session:
        assert claim_sync(session, job_id) is False
        # Same session, immediately afterwards: still readable, no lingering
        # transaction state.
        job = session.get(Job, job_id)
        assert job is not None and job.status is JobStatus.COMPLETED

    # And another connection can still lock the row.
    with sync_session() as other:
        assert claim_sync(other, job_id) is False


def test_a_rejected_claim_does_not_block_a_later_legitimate_one(
    sync_db: Session, job_id_at: Any
):
    job_id = job_id_at(JobStatus.PENDING)

    with sync_session() as first:
        assert claim_sync(first, job_id) is True

    # Finish it, then confirm a redelivery is refused rather than hanging.
    with sync_session() as second:
        assert transition_sync(second, job_id, JobStatus.COMPLETED, {"rows": 1}) is True

    with sync_session() as third:
        assert claim_sync(third, job_id) is False


# ── transition_sync's two modes ───────────────────────────────────


def test_strict_mode_raises_on_an_illegal_move(sync_db: Session, job_id_at: Any):
    """A direct status write that the table forbids is a bug worth surfacing."""
    job_id = job_id_at(JobStatus.COMPLETED)

    with sync_session() as session:
        with pytest.raises(IllegalTransition) as caught:
            transition_sync(session, job_id, JobStatus.PROCESSING)

    assert caught.value.current is JobStatus.COMPLETED
    assert caught.value.target is JobStatus.PROCESSING


def test_lenient_mode_returns_false_on_an_illegal_move(
    sync_db: Session, job_id_at: Any
):
    """``strict=False`` is what an ``except`` block wants: reporting a failure
    must not raise a second exception that masks the first."""
    job_id = job_id_at(JobStatus.COMPLETED)

    with sync_session() as session:
        assert transition_sync(session, job_id, JobStatus.FAILED, strict=False) is False

    sync_db.expire_all()
    assert sync_db.get(Job, job_id).status is JobStatus.COMPLETED


def test_strict_mode_raises_for_a_missing_job(sync_db: Session):
    with sync_session() as session:
        with pytest.raises(JobNotFound):
            transition_sync(session, uuid.uuid4(), JobStatus.PROCESSING)


def test_lenient_mode_returns_false_for_a_missing_job(sync_db: Session):
    with sync_session() as session:
        assert (
            transition_sync(session, uuid.uuid4(), JobStatus.PROCESSING, strict=False)
            is False
        )


def test_a_transition_commits_its_result(sync_db: Session, job_id_at: Any):
    """The write, the commit, and the publish are one call — a result visible to
    this session but uncommitted would show COMPLETED in the UI and PROCESSING
    in the database."""
    job_id = job_id_at(JobStatus.PROCESSING)
    result = {"total_rows": 42, "file_name": "report.csv"}

    with sync_session() as session:
        assert transition_sync(session, job_id, JobStatus.COMPLETED, result) is True

    # A different connection — proves it was committed, not just flushed.
    with sync_session() as other:
        job = other.get(Job, job_id)
        assert job is not None
        assert job.status is JobStatus.COMPLETED
        assert job.result == result


def test_a_transition_without_a_result_preserves_the_existing_one(
    sync_db: Session, job_id_at: Any
):
    """``claim_sync`` passes no result, and must not wipe what is already there."""
    job_id = job_id_at(JobStatus.PENDING)

    with sync_session() as session:
        transition_sync(session, job_id, JobStatus.PROCESSING, {"processed": 0})
    with sync_session() as session:
        transition_sync(session, job_id, JobStatus.COMPLETED)

    with sync_session() as other:
        assert other.get(Job, job_id).result == {"processed": 0}


def test_a_transition_accepts_a_string_job_id(sync_db: Session, job_id_at: Any):
    """Celery serialises arguments to JSON, so tasks receive UUIDs as strings."""
    job_id = job_id_at(JobStatus.PENDING)

    with sync_session() as session:
        assert claim_sync(session, str(job_id)) is True
