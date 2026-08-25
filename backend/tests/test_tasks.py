"""
The two Celery tasks, run in-process against the test database.

Called through ``task.run(...)`` rather than ``.delay()`` — the task body is the
subject, and the broker round trip is not. ``bind=True`` means ``run`` is the
decorated function with ``self`` already bound.

The assertions to read first are the ones about *ownership*, because both tasks
used to get it wrong in ways that leaked data:

  • ``ingest_csv`` trusted the CSV's own ``user_id`` column, so an upload could
    attribute rows to anybody — and an unparseable value got a fresh ``uuid4()``,
    seeding the table with owners who never existed.
  • ``generate_bulk_csv_report`` read the user id out of the request payload.
    An unscoped query here writes every user's transactions into a file the
    requester can download, so "no owner" must never widen to "all owners".
"""

from __future__ import annotations

import csv
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Job, JobStatus, Transaction
from app.tasks import generate_bulk_csv_report, ingest_csv
from tests.helpers import txn_row, write_csv

NOW = datetime.now(timezone.utc)
IN_RANGE = NOW - timedelta(days=10)
BEFORE_RANGE = NOW - timedelta(days=400)

REPORT_PAYLOAD = {
    "start_date": (NOW - timedelta(days=30)).date().isoformat(),
    "end_date": (NOW + timedelta(days=1)).date().isoformat(),
}


# ── Helpers ───────────────────────────────────────────────────────


def read_export(job_result: dict) -> list[dict[str, str]]:
    """Parse the CSV a report task wrote, and delete it."""
    path = Path(job_result["file_path"])
    try:
        with open(path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    finally:
        path.unlink(missing_ok=True)


def count_transactions(session: Session, owner: Any = None) -> int:
    query = select(func.count(Transaction.id))
    if owner is not None:
        query = query.where(Transaction.user_id == getattr(owner, "id", owner))
    return session.execute(query).scalar() or 0


# ── generate_bulk_csv_report: scoping ─────────────────────────────


def test_report_contains_only_the_owners_transactions(
    sync_db: Session,
    make_sync_user: Any,
    make_sync_job: Any,
    make_sync_transactions: Any,
):
    """The vulnerability, at the task level.

    Two users with transactions in the same date range. The report must carry
    exactly one of them, chosen by ``job.user_id`` and nothing else.
    """
    alice = make_sync_user("report-alice@example.com")
    bob = make_sync_user("report-bob@example.com")

    make_sync_transactions(alice, ["10.00", "20.00", "30.00"], created_at=IN_RANGE)
    make_sync_transactions(bob, ["999.00", "888.00"], created_at=IN_RANGE)

    job = make_sync_job(owner=alice, job_type="bulk_csv_report", payload=REPORT_PAYLOAD)

    outcome = generate_bulk_csv_report.run(str(job.id))

    assert outcome["status"] == "COMPLETED"
    rows = read_export(outcome["result"])

    assert len(rows) == 3
    assert {r["user_id"] for r in rows} == {str(alice.id)}
    assert sorted(r["amount"] for r in rows) == ["10.00", "20.00", "30.00"]
    assert "999.00" not in {r["amount"] for r in rows}


def test_report_for_an_unowned_job_fails_and_exports_nothing(
    sync_db: Session,
    make_sync_user: Any,
    make_sync_job: Any,
    make_sync_transactions: Any,
):
    """A NULL owner must fail the job, not run the query unscoped.

    This is the difference between a broken legacy row and a full-table export
    of every user's financial history, delivered through an endpoint the
    requester is allowed to download from.
    """
    somebody = make_sync_user("bystander@example.com")
    make_sync_transactions(somebody, ["1.00", "2.00"], created_at=IN_RANGE)

    job = make_sync_job(owner=None, job_type="bulk_csv_report", payload=REPORT_PAYLOAD)

    outcome = generate_bulk_csv_report.run(str(job.id))

    assert outcome["status"] == "FAILED"
    assert "no owner" in outcome["error"].lower()

    sync_db.expire_all()
    refreshed = sync_db.get(Job, job.id)
    assert refreshed is not None
    assert refreshed.status is JobStatus.FAILED
    # Nothing downloadable was produced.
    assert "file_path" not in (refreshed.result or {})


def test_report_respects_the_date_range(
    sync_db: Session,
    sync_user: Any,
    make_sync_job: Any,
    make_sync_transactions: Any,
):
    make_sync_transactions(sync_user, ["5.00"], created_at=IN_RANGE)
    make_sync_transactions(sync_user, ["7.00"], created_at=BEFORE_RANGE)

    job = make_sync_job(
        owner=sync_user, job_type="bulk_csv_report", payload=REPORT_PAYLOAD
    )
    outcome = generate_bulk_csv_report.run(str(job.id))

    rows = read_export(outcome["result"])
    assert [r["amount"] for r in rows] == ["5.00"]


def test_report_of_an_empty_range_still_produces_a_header(
    sync_db: Session, sync_user: Any, make_sync_job: Any
):
    """A user with no transactions gets an empty report, not a failure."""
    job = make_sync_job(
        owner=sync_user, job_type="bulk_csv_report", payload=REPORT_PAYLOAD
    )
    outcome = generate_bulk_csv_report.run(str(job.id))

    assert outcome["status"] == "COMPLETED"
    assert outcome["result"]["total_rows"] == 0

    path = Path(outcome["result"]["file_path"])
    try:
        assert (
            path.read_text(encoding="utf-8").strip()
            == "id,user_id,amount,status,created_at"
        )
    finally:
        path.unlink(missing_ok=True)


def test_report_writes_money_as_exact_decimal_strings(
    sync_db: Session,
    sync_user: Any,
    make_sync_job: Any,
    make_sync_transactions: Any,
):
    """Two decimal places, no float artefacts, no scientific notation."""
    make_sync_transactions(
        sync_user, ["0.01", "1234567.89", "100.00", "0.10"], created_at=IN_RANGE
    )

    job = make_sync_job(
        owner=sync_user, job_type="bulk_csv_report", payload=REPORT_PAYLOAD
    )
    rows = read_export(generate_bulk_csv_report.run(str(job.id))["result"])

    amounts = sorted(Decimal(r["amount"]) for r in rows)
    assert amounts == [
        Decimal("0.01"),
        Decimal("0.10"),
        Decimal("100.00"),
        Decimal("1234567.89"),
    ]
    for row in rows:
        assert "E" not in row["amount"].upper()
        assert row["amount"].split(".")[1] == row["amount"].split(".")[1][:2]


@pytest.mark.parametrize(
    "payload",
    [None, {}, {"start_date": "2024-01-01"}, {"end_date": "2026-01-01"}],
)
def test_report_without_a_full_date_range_fails(
    sync_db: Session, sync_user: Any, make_sync_job: Any, payload: dict | None
):
    job = make_sync_job(owner=sync_user, job_type="bulk_csv_report", payload=payload)
    outcome = generate_bulk_csv_report.run(str(job.id))

    assert outcome["status"] == "FAILED"
    assert "start_date" in outcome["error"] or "end_date" in outcome["error"]


def test_report_with_unparseable_dates_fails(
    sync_db: Session, sync_user: Any, make_sync_job: Any
):
    job = make_sync_job(
        owner=sync_user,
        job_type="bulk_csv_report",
        payload={"start_date": "last tuesday", "end_date": "soon"},
    )
    outcome = generate_bulk_csv_report.run(str(job.id))
    assert outcome["status"] == "FAILED"


def test_report_for_a_missing_job_returns_an_error(sync_db: Session):
    outcome = generate_bulk_csv_report.run(str(uuid.uuid4()))
    assert "error" in outcome
    assert "not found" in outcome["error"].lower()


# ── ingest_csv: attribution ───────────────────────────────────────


def test_ingested_rows_belong_to_the_uploader_not_the_file(
    sync_db: Session,
    tmp_path: Path,
    make_sync_user: Any,
    make_sync_job: Any,
):
    """Every row is stamped with ``job.user_id``, whatever the CSV claims.

    The file here names a *real other user* in its ``user_id`` column, which is
    the strongest form of the old bug: it used to be a working write primitive
    against another account's data.
    """
    uploader = make_sync_user("uploader@example.com")
    victim = make_sync_user("victim@example.com")

    path = write_csv(
        tmp_path / "spoofed.csv",
        [txn_row(row_id=i, user_id=victim.id, amount="5.00") for i in range(4)],
    )

    job = make_sync_job(owner=uploader, payload={"saved_path": str(path)})
    outcome = ingest_csv.run(str(job.id), str(path))

    assert outcome["status"] == "COMPLETED"
    assert count_transactions(sync_db, uploader) == 4
    assert count_transactions(sync_db, victim) == 0
    assert outcome["result"]["summary"]["owner_id"] == str(uploader.id)


def test_unparseable_user_ids_in_the_file_are_simply_ignored(
    sync_db: Session, tmp_path: Path, sync_user: Any, make_sync_job: Any
):
    """The column is not read at all, so garbage in it changes nothing.

    Previously an unparseable value produced a fresh ``uuid4()`` per row, filling
    the table with owners that matched no account and no report.
    """
    path = write_csv(
        tmp_path / "garbage-owners.csv",
        [
            {
                "id": 1,
                "user_id": "",
                "amount": "1.00",
                "status": "ok",
                "created_at": "",
            },
            {
                "id": 2,
                "user_id": "not-a-uuid",
                "amount": "2.00",
                "status": "ok",
                "created_at": "",
            },
            {
                "id": 3,
                "user_id": "../../etc/passwd",
                "amount": "3.00",
                "status": "ok",
                "created_at": "",
            },
        ],
    )

    job = make_sync_job(owner=sync_user, payload={"saved_path": str(path)})
    assert ingest_csv.run(str(job.id), str(path))["status"] == "COMPLETED"

    owners = set(sync_db.execute(select(Transaction.user_id)).scalars().all())
    assert owners == {sync_user.id}


def test_ingest_of_an_unowned_job_fails_before_inserting_anything(
    sync_db: Session, tmp_path: Path, make_sync_job: Any
):
    """There is no sensible owner to invent, so the job fails outright."""
    path = write_csv(tmp_path / "orphan.csv", [txn_row(row_id=1)])

    job = make_sync_job(owner=None, payload={"saved_path": str(path)})
    outcome = ingest_csv.run(str(job.id), str(path))

    assert outcome["status"] == "FAILED"
    assert "no owner" in outcome["error"].lower()
    assert count_transactions(sync_db) == 0


# ── ingest_csv: idempotency ───────────────────────────────────────


def test_a_redelivered_ingest_does_not_double_insert(
    sync_db: Session, tmp_path: Path, sync_user: Any, make_sync_job: Any
):
    """The scenario ``task_acks_late=True`` creates.

    The worker finishes, its ack is lost, and the broker hands the same task to
    another worker. Without the claim this doubles every row — and it is money.
    """
    path = write_csv(
        tmp_path / "twice.csv",
        [txn_row(row_id=i, amount="3.00") for i in range(5)],
    )
    job = make_sync_job(owner=sync_user, payload={"saved_path": str(path)})

    first = ingest_csv.run(str(job.id), str(path))
    assert first["status"] == "COMPLETED"
    assert count_transactions(sync_db, sync_user) == 5

    second = ingest_csv.run(str(job.id), str(path))
    assert second["skipped"] is True
    assert second["status"] == JobStatus.COMPLETED.value
    assert count_transactions(sync_db, sync_user) == 5, "rows were inserted twice"


def test_a_cancelled_job_is_never_ingested(
    sync_db: Session, tmp_path: Path, sync_user: Any, make_sync_job: Any
):
    """The durable half of cancellation.

    Celery's revoke is in-memory per worker and lost on restart, so a worker that
    never saw it still has to refuse — and does, because CANCELLED → PROCESSING
    is illegal.
    """
    path = write_csv(tmp_path / "cancelled.csv", [txn_row(row_id=1)])
    job = make_sync_job(
        owner=sync_user, status=JobStatus.CANCELLED, payload={"saved_path": str(path)}
    )

    outcome = ingest_csv.run(str(job.id), str(path))

    assert outcome["skipped"] is True
    assert count_transactions(sync_db) == 0

    sync_db.expire_all()
    assert sync_db.get(Job, job.id).status is JobStatus.CANCELLED


def test_an_already_running_job_is_not_ingested_again(
    sync_db: Session, tmp_path: Path, sync_user: Any, make_sync_job: Any
):
    path = write_csv(tmp_path / "running.csv", [txn_row(row_id=1)])
    job = make_sync_job(
        owner=sync_user, status=JobStatus.PROCESSING, payload={"saved_path": str(path)}
    )

    assert ingest_csv.run(str(job.id), str(path))["skipped"] is True
    assert count_transactions(sync_db) == 0


def test_ingest_of_a_missing_job_returns_an_error(sync_db: Session, tmp_path: Path):
    path = write_csv(tmp_path / "nojob.csv", [txn_row(row_id=1)])
    outcome = ingest_csv.run(str(uuid.uuid4()), str(path))

    assert "error" in outcome
    assert count_transactions(sync_db) == 0


# ── ingest_csv: parsing and analytics ─────────────────────────────


def test_ingest_reads_the_path_from_the_payload_when_not_passed(
    sync_db: Session, tmp_path: Path, sync_user: Any, make_sync_job: Any
):
    """Keeps every task in TASK_REGISTRY dispatchable as ``(job_id)``."""
    path = write_csv(tmp_path / "from-payload.csv", [txn_row(row_id=1)])
    job = make_sync_job(owner=sync_user, payload={"saved_path": str(path)})

    assert ingest_csv.run(str(job.id))["status"] == "COMPLETED"
    assert count_transactions(sync_db, sync_user) == 1


def test_ingest_without_a_source_file_fails(
    sync_db: Session, sync_user: Any, make_sync_job: Any
):
    job = make_sync_job(owner=sync_user, payload={})
    outcome = ingest_csv.run(str(job.id))

    assert outcome["status"] == "FAILED"
    assert "saved_path" in outcome["error"]


def test_ingest_of_a_nonexistent_file_fails_cleanly(
    sync_db: Session, tmp_path: Path, sync_user: Any, make_sync_job: Any
):
    """The job must land in FAILED with a message, not raise out of the worker."""
    missing = tmp_path / "not-here.csv"
    job = make_sync_job(owner=sync_user, payload={"saved_path": str(missing)})

    outcome = ingest_csv.run(str(job.id), str(missing))

    assert outcome["status"] == "FAILED"
    sync_db.expire_all()
    assert sync_db.get(Job, job.id).status is JobStatus.FAILED


def test_ingest_totals_are_exact_to_the_cent(
    sync_db: Session, tmp_path: Path, sync_user: Any, make_sync_job: Any
):
    """300 rows of 0.01 must total exactly 3.00.

    A float accumulator drifts here, and the drift is invisible per row — which
    is why ``Transaction.amount`` is ``Numeric(12, 2)`` and the running total is
    a ``Decimal``.
    """
    path = write_csv(
        tmp_path / "cents.csv",
        [txn_row(row_id=i, amount="0.01") for i in range(300)],
    )
    job = make_sync_job(owner=sync_user, payload={"saved_path": str(path)})

    outcome = ingest_csv.run(str(job.id), str(path))

    assert outcome["result"]["summary"]["total_amount"] == "3.00"

    stored = sync_db.execute(
        select(func.sum(Transaction.amount)).where(Transaction.user_id == sync_user.id)
    ).scalar()
    assert stored == Decimal("3.00")


def test_ingest_batches_beyond_one_flush(
    sync_db: Session, tmp_path: Path, sync_user: Any, make_sync_job: Any
):
    """BATCH_SIZE is 500, so 1,200 rows exercise the flush loop and the final
    partial batch — the place an off-by-one silently drops rows."""
    path = write_csv(
        tmp_path / "batched.csv",
        [txn_row(row_id=i, amount="1.00") for i in range(1200)],
    )
    job = make_sync_job(owner=sync_user, payload={"saved_path": str(path)})

    outcome = ingest_csv.run(str(job.id), str(path))

    assert outcome["result"]["processed"] == 1200
    assert outcome["result"]["total"] == 1200
    assert count_transactions(sync_db, sync_user) == 1200


def test_ingest_counts_rows_with_embedded_newlines_correctly(
    sync_db: Session, tmp_path: Path, sync_user: Any, make_sync_job: Any
):
    """``total`` comes from ``_count_csv_rows``, ``processed`` from the reader.

    If they disagree the progress bar never reaches 100% even though the job
    completes — the exact symptom of counting newlines instead of CSV records.
    """
    path = write_csv(
        tmp_path / "multiline.csv",
        [
            txn_row(row_id=1, amount="1.00", status="pending\nstill pending"),
            txn_row(row_id=2, amount="2.00"),
            txn_row(row_id=3, amount="3.00", status="a\nb\nc"),
        ],
    )
    job = make_sync_job(owner=sync_user, payload={"saved_path": str(path)})

    outcome = ingest_csv.run(str(job.id), str(path))

    assert outcome["result"]["total"] == 3
    assert outcome["result"]["processed"] == 3
    assert count_transactions(sync_db, sync_user) == 3


def test_ingest_summarises_statuses(
    sync_db: Session, tmp_path: Path, sync_user: Any, make_sync_job: Any
):
    path = write_csv(
        tmp_path / "statuses.csv",
        [
            txn_row(row_id=1, amount="10.00", status="completed"),
            txn_row(row_id=2, amount="20.00", status="COMPLETED"),
            txn_row(row_id=3, amount="5.00", status="pending"),
        ],
    )
    job = make_sync_job(owner=sync_user, payload={"saved_path": str(path)})

    summary = ingest_csv.run(str(job.id), str(path))["result"]["summary"]

    # Status is lower-cased on the way in, so the first two rows merge.
    assert summary["status_counts"] == {"completed": 2, "pending": 1}
    assert summary["status_amounts"] == {"completed": "30.00", "pending": "5.00"}
    assert summary["distinct_statuses"] == 2
    assert summary["total_amount"] == "35.00"
    assert summary["avg_amount"] == "11.67"


def test_ingest_of_an_empty_file_completes_without_rows(
    sync_db: Session, tmp_path: Path, sync_user: Any, make_sync_job: Any
):
    """A header-only upload is valid, and must not divide by zero computing the
    average."""
    path = write_csv(tmp_path / "header-only.csv", [])
    job = make_sync_job(owner=sync_user, payload={"saved_path": str(path)})

    outcome = ingest_csv.run(str(job.id), str(path))

    assert outcome["status"] == "COMPLETED"
    assert outcome["result"]["processed"] == 0
    assert outcome["result"]["summary"]["avg_amount"] == "0.00"
    assert count_transactions(sync_db) == 0


def test_ingest_sample_rows_are_capped_and_show_the_real_owner(
    sync_db: Session, tmp_path: Path, sync_user: Any, make_sync_job: Any
):
    """The UI preview table. It must show who the rows actually belong to, not
    the id the file asked for."""
    path = write_csv(
        tmp_path / "sampled.csv",
        [txn_row(row_id=i, amount="1.00") for i in range(25)],
    )
    job = make_sync_job(owner=sync_user, payload={"saved_path": str(path)})

    samples = ingest_csv.run(str(job.id), str(path))["result"]["sample_rows"]

    assert len(samples) == 10
    assert {s["user_id"] for s in samples} == {str(sync_user.id)}


def test_ingest_stores_a_transition_to_completed(
    sync_db: Session, tmp_path: Path, sync_user: Any, make_sync_job: Any
):
    """The row must end COMPLETED with its analytics committed — a task that
    returns a result dict but leaves the row PROCESSING is the exact failure the
    state machine exists to prevent."""
    path = write_csv(tmp_path / "final.csv", [txn_row(row_id=1, amount="9.99")])
    job = make_sync_job(owner=sync_user, payload={"saved_path": str(path)})

    ingest_csv.run(str(job.id), str(path))

    sync_db.expire_all()
    refreshed = sync_db.get(Job, job.id)
    assert refreshed is not None
    assert refreshed.status is JobStatus.COMPLETED
    assert refreshed.result is not None
    assert refreshed.result["summary"]["total_amount"] == "9.99"
