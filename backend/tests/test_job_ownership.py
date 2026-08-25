"""
Ownership scoping — the security properties Day 10 added.

Every test here corresponds to something that used to be exploitable:

  • ``POST /jobs/reports`` read ``body.user_id`` verbatim, so any caller could
    export anybody's transaction history.
  • ``GET /jobs/{id}/download`` served any job's CSV to anyone who guessed a UUID.
  • ``GET /jobs`` listed every job in the system.

The recurring assertion is **404, not 403**. A 403 confirms the UUID exists,
which turns each endpoint into a membership oracle over the whole jobs table —
so "someone else's job" and "no such job" have to be byte-identical responses.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Job, JobStatus
from app.tasks import EXPORTS_DIR
from tests.conftest import Account

REPORT_RANGE = {"start_date": "2024-01-01", "end_date": "2026-12-31"}


# ── Creation attributes the job to the caller ─────────────────────


async def test_created_job_is_owned_by_the_caller(
    client: AsyncClient, db: AsyncSession, alice: Account
):
    response = await client.post(
        "/api/v1/jobs",
        json={"job_type": "csv_ingestion", "payload": {"file_name": "x.csv"}},
        headers=alice.headers,
    )

    assert response.status_code == 201
    job = await db.get(Job, uuid.UUID(response.json()["id"]))
    assert job is not None
    assert job.user_id == alice.id


async def test_job_is_enqueued_under_the_committed_task_id(
    client: AsyncClient, alice: Account, dispatched: list
):
    """The task id is minted before the commit so a job cancelled in the gap
    still has something to revoke."""
    response = await client.post(
        "/api/v1/jobs",
        json={"job_type": "csv_ingestion", "payload": None},
        headers=alice.headers,
    )

    job_id = response.json()["id"]
    assert len(dispatched) == 1
    assert dispatched[0].task_name == "csv_ingestion"
    assert dispatched[0].args == [job_id]
    assert dispatched[0].task_id is not None


async def test_unknown_job_type_is_refused(client: AsyncClient, alice: Account):
    """``TASK_REGISTRY`` is the gate. An unregistered type must 400 rather than
    create a row nothing will ever process."""
    response = await client.post(
        "/api/v1/jobs",
        json={"job_type": "no_such_task", "payload": None},
        headers=alice.headers,
    )
    assert response.status_code == 400
    assert "no_such_task" in response.json()["detail"]


async def test_unknown_job_type_creates_no_row(
    client: AsyncClient, db: AsyncSession, alice: Account, dispatched: list
):
    await client.post(
        "/api/v1/jobs",
        json={"job_type": "no_such_task", "payload": None},
        headers=alice.headers,
    )
    assert (await db.execute(select(Job))).scalars().all() == []
    assert dispatched == []


# ── The report endpoint's removed user_id ─────────────────────────


async def test_report_request_rejects_a_user_id(client: AsyncClient, alice: Account):
    """The vulnerability, closed at the schema.

    ``ReportRequest`` sets ``extra="forbid"``, so a request still carrying
    ``user_id`` gets a 422. Pydantic's default is to *ignore* unknown keys, which
    would be safe but silent — the caller would believe their scoping request was
    honoured while quietly receiving their own data.
    """
    response = await client.post(
        "/api/v1/jobs/reports",
        json={**REPORT_RANGE, "user_id": str(uuid.uuid4())},
        headers=alice.headers,
    )
    assert response.status_code == 422


async def test_report_job_stores_only_the_date_range(
    client: AsyncClient, db: AsyncSession, alice: Account
):
    """No user id on the wire and none in the payload — the task reads the owner
    off ``job.user_id``, so there is nothing to tamper with."""
    response = await client.post(
        "/api/v1/jobs/reports", json=REPORT_RANGE, headers=alice.headers
    )

    assert response.status_code == 201
    job = await db.get(Job, uuid.UUID(response.json()["id"]))
    assert job is not None
    assert job.user_id == alice.id
    assert set(job.payload or {}) == {"start_date", "end_date"}


async def test_report_requires_a_date_range(client: AsyncClient, alice: Account):
    response = await client.post("/api/v1/jobs/reports", json={}, headers=alice.headers)
    assert response.status_code == 422


# ── Listing is scoped, including the total ────────────────────────


async def test_list_returns_only_the_callers_jobs(
    client: AsyncClient, alice: Account, bob: Account
):
    for _ in range(2):
        await client.post(
            "/api/v1/jobs",
            json={"job_type": "csv_ingestion", "payload": None},
            headers=alice.headers,
        )
    for _ in range(3):
        await client.post(
            "/api/v1/jobs",
            json={"job_type": "csv_ingestion", "payload": None},
            headers=bob.headers,
        )

    for who, expected in ((alice, 2), (bob, 3)):
        body = (await client.get("/api/v1/jobs", headers=who.headers)).json()
        assert len(body["items"]) == expected
        # The count query needs the same owner filter as the page query.
        # Without it the caller's rows appear under someone else's total and the
        # UI pages forward into empty screens.
        assert body["total"] == expected


async def test_list_is_empty_for_a_user_with_no_jobs(
    client: AsyncClient, alice: Account, bob: Account
):
    await client.post(
        "/api/v1/jobs",
        json={"job_type": "csv_ingestion", "payload": None},
        headers=alice.headers,
    )

    body = (await client.get("/api/v1/jobs", headers=bob.headers)).json()
    assert body["items"] == []
    assert body["total"] == 0


async def test_list_filters_stay_scoped(
    client: AsyncClient, alice: Account, bob: Account, make_job: Any
):
    """A status or type filter must not widen the owner filter."""
    await make_job(owner=bob, status=JobStatus.COMPLETED, job_type="bulk_csv_report")
    await make_job(owner=alice, status=JobStatus.PENDING, job_type="bulk_csv_report")

    body = (
        await client.get(
            "/api/v1/jobs",
            params={"status": "COMPLETED", "job_type": "bulk_csv_report"},
            headers=alice.headers,
        )
    ).json()

    assert body["items"] == []
    assert body["total"] == 0


async def test_list_excludes_unowned_legacy_rows(
    client: AsyncClient, alice: Account, make_job: Any
):
    """Rows predating auth have a NULL owner, and NULL matches no user id."""
    await make_job(owner=None)

    body = (await client.get("/api/v1/jobs", headers=alice.headers)).json()
    assert body["items"] == []
    assert body["total"] == 0


# ── Cross-user access returns 404 ─────────────────────────────────

CROSS_USER_ROUTES = [
    ("GET", "/api/v1/jobs/{job_id}"),
    ("POST", "/api/v1/jobs/{job_id}/cancel"),
    ("GET", "/api/v1/jobs/{job_id}/download"),
]


@pytest.mark.parametrize(("method", "template"), CROSS_USER_ROUTES)
async def test_another_users_job_is_not_found(
    client: AsyncClient,
    alice: Account,
    bob: Account,
    make_job: Any,
    method: str,
    template: str,
):
    job = await make_job(owner=alice, status=JobStatus.PENDING)

    response = await client.request(
        method, template.format(job_id=job.id), headers=bob.headers
    )
    assert response.status_code == 404


@pytest.mark.parametrize(("method", "template"), CROSS_USER_ROUTES)
async def test_an_unowned_job_is_not_found(
    client: AsyncClient, alice: Account, make_job: Any, method: str, template: str
):
    """A NULL owner means nobody, not everybody."""
    job = await make_job(owner=None, status=JobStatus.PENDING)

    response = await client.request(
        method, template.format(job_id=job.id), headers=alice.headers
    )
    assert response.status_code == 404


@pytest.mark.parametrize(("method", "template"), CROSS_USER_ROUTES)
async def test_existing_and_nonexistent_jobs_are_indistinguishable(
    client: AsyncClient,
    alice: Account,
    bob: Account,
    make_job: Any,
    method: str,
    template: str,
):
    """The anti-oracle property, stated exactly.

    Same status *and* same body for "exists but is not yours" as for "does not
    exist". Any difference — a distinct message, a different detail shape — lets
    an attacker enumerate valid job UUIDs.
    """
    someone_elses = await make_job(owner=alice, status=JobStatus.PENDING)
    imaginary = uuid.uuid4()

    real = await client.request(
        method, template.format(job_id=someone_elses.id), headers=bob.headers
    )
    fake = await client.request(
        method, template.format(job_id=imaginary), headers=bob.headers
    )

    assert real.status_code == fake.status_code == 404
    assert real.json()["detail"].replace(str(someone_elses.id), "X") == fake.json()[
        "detail"
    ].replace(str(imaginary), "X")


@pytest.mark.parametrize(("method", "template"), CROSS_USER_ROUTES)
async def test_a_malformed_uuid_is_a_validation_error(
    client: AsyncClient, alice: Account, method: str, template: str
):
    response = await client.request(
        method, template.format(job_id="not-a-uuid"), headers=alice.headers
    )
    assert response.status_code == 422


# ── Everything needs a token ──────────────────────────────────────


@pytest.mark.parametrize(
    ("method", "path", "kwargs"),
    [
        ("GET", "/api/v1/jobs", {}),
        ("POST", "/api/v1/jobs", {"json": {"job_type": "csv_ingestion"}}),
        ("POST", "/api/v1/jobs/reports", {"json": REPORT_RANGE}),
        ("GET", f"/api/v1/jobs/{uuid.uuid4()}", {}),
        ("POST", f"/api/v1/jobs/{uuid.uuid4()}/cancel", {}),
        ("GET", f"/api/v1/jobs/{uuid.uuid4()}/download", {}),
        (
            "POST",
            "/api/v1/jobs/ingest",
            {"files": {"file": ("x.csv", b"a\n", "text/csv")}},
        ),
    ],
)
async def test_endpoint_requires_authentication(
    client: AsyncClient, method: str, path: str, kwargs: dict
):
    response = await client.request(method, path, **kwargs)
    assert response.status_code == 401


# ── Cancellation ──────────────────────────────────────────────────


async def test_owner_can_cancel_a_pending_job(
    client: AsyncClient, alice: Account, make_job: Any
):
    job = await make_job(owner=alice, status=JobStatus.PENDING)

    response = await client.post(f"/api/v1/jobs/{job.id}/cancel", headers=alice.headers)

    assert response.status_code == 200
    assert response.json()["status"] == JobStatus.CANCELLED.value


@pytest.mark.parametrize(
    "status",
    [JobStatus.PROCESSING, JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED],
)
async def test_cancel_is_refused_once_work_has_started_or_finished(
    client: AsyncClient, alice: Account, make_job: Any, status: JobStatus
):
    """409 for PROCESSING is deliberate, not an oversight.

    The tasks have no cooperative abort, so accepting a mid-flight cancel would
    tell the user the work stopped while the worker kept inserting rows.
    """
    job = await make_job(owner=alice, status=status)

    response = await client.post(f"/api/v1/jobs/{job.id}/cancel", headers=alice.headers)

    assert response.status_code == 409
    assert status.value in response.json()["detail"]


async def test_cancelling_another_users_job_does_not_change_it(
    client: AsyncClient, db: AsyncSession, alice: Account, bob: Account, make_job: Any
):
    """The 404 must be a genuine no-op, not a 404 after the write."""
    job = await make_job(owner=alice, status=JobStatus.PENDING)

    assert (
        await client.post(f"/api/v1/jobs/{job.id}/cancel", headers=bob.headers)
    ).status_code == 404

    await db.refresh(job)
    assert job.status is JobStatus.PENDING


# ── Download ──────────────────────────────────────────────────────


@pytest.fixture
def export_file() -> Any:
    """Write a real CSV into the exports directory, and clean it up.

    The download endpoint resolves ``result["file_path"]`` and requires it to be
    inside ``EXPORTS_DIR``, so the file has to genuinely live there.
    """
    written: list[Path] = []

    def _write(name: str, content: str = "id,amount\n1,5.00\n") -> Path:
        EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
        path = EXPORTS_DIR / name
        path.write_text(content, encoding="utf-8")
        written.append(path)
        return path

    yield _write

    for path in written:
        path.unlink(missing_ok=True)


async def test_owner_downloads_their_report(
    client: AsyncClient, alice: Account, make_job: Any, export_file: Any
):
    path = export_file(f"report_{uuid.uuid4()}.csv")
    job = await make_job(
        owner=alice,
        status=JobStatus.COMPLETED,
        result={"file_path": str(path), "file_name": "report.csv", "total_rows": 1},
    )

    response = await client.get(
        f"/api/v1/jobs/{job.id}/download", headers=alice.headers
    )

    assert response.status_code == 200
    assert response.text == "id,amount\n1,5.00\n"
    assert "report.csv" in response.headers["content-disposition"]


async def test_another_user_cannot_download_the_file(
    client: AsyncClient, alice: Account, bob: Account, make_job: Any, export_file: Any
):
    """A report CSV is a full dump of one user's transaction history."""
    path = export_file(f"report_{uuid.uuid4()}.csv")
    job = await make_job(
        owner=alice,
        status=JobStatus.COMPLETED,
        result={"file_path": str(path), "file_name": "report.csv"},
    )

    response = await client.get(f"/api/v1/jobs/{job.id}/download", headers=bob.headers)
    assert response.status_code == 404
    assert "id,amount" not in response.text


@pytest.mark.parametrize(
    "status", [JobStatus.PENDING, JobStatus.PROCESSING, JobStatus.FAILED]
)
async def test_download_requires_a_completed_job(
    client: AsyncClient, alice: Account, make_job: Any, status: JobStatus
):
    job = await make_job(owner=alice, status=status)

    response = await client.get(
        f"/api/v1/jobs/{job.id}/download", headers=alice.headers
    )
    assert response.status_code == 409


async def test_download_of_a_job_with_no_file_is_404(
    client: AsyncClient, alice: Account, make_job: Any
):
    job = await make_job(
        owner=alice, status=JobStatus.COMPLETED, result={"total_rows": 0}
    )

    response = await client.get(
        f"/api/v1/jobs/{job.id}/download", headers=alice.headers
    )
    assert response.status_code == 404


@pytest.mark.parametrize(
    "escaped",
    [
        "/etc/passwd",
        "/etc/hostname",
        "exports/../app/config.py",
        "../requirements.txt",
    ],
)
async def test_download_refuses_a_path_outside_the_exports_directory(
    client: AsyncClient, alice: Account, make_job: Any, escaped: str
):
    """``file_path`` comes out of a JSONB column, so it is untrusted input.

    Anything that resolves outside ``EXPORTS_DIR`` must 404 rather than stream a
    file off the container's filesystem.
    """
    job = await make_job(
        owner=alice,
        status=JobStatus.COMPLETED,
        result={"file_path": escaped, "file_name": "innocent.csv"},
    )

    response = await client.get(
        f"/api/v1/jobs/{job.id}/download", headers=alice.headers
    )
    assert response.status_code == 404


async def test_download_of_a_vanished_file_is_404(
    client: AsyncClient, alice: Account, make_job: Any
):
    """The result row outlives the file — exports/ is a volume that can be
    pruned, and Day 13 plans a job that does exactly that."""
    job = await make_job(
        owner=alice,
        status=JobStatus.COMPLETED,
        result={"file_path": str(EXPORTS_DIR / "deleted.csv")},
    )

    response = await client.get(
        f"/api/v1/jobs/{job.id}/download", headers=alice.headers
    )
    assert response.status_code == 404


# ── Upload ────────────────────────────────────────────────────────


async def test_upload_creates_a_job_owned_by_the_uploader(
    client: AsyncClient, db: AsyncSession, alice: Account, dispatched: list
):
    response = await client.post(
        "/api/v1/jobs/ingest",
        files={"file": ("data.csv", b"id,amount\n1,5.00\n", "text/csv")},
        headers=alice.headers,
    )

    assert response.status_code == 201
    job = await db.get(Job, uuid.UUID(response.json()["id"]))
    assert job is not None
    assert job.user_id == alice.id
    assert job.job_type == "csv_ingestion"
    assert dispatched[0].task_name == "csv_ingestion"

    Path((job.payload or {})["saved_path"]).unlink(missing_ok=True)


@pytest.mark.parametrize("filename", ["data.txt", "data.csv.exe", "data", "data.xlsx"])
async def test_upload_rejects_a_non_csv(
    client: AsyncClient, alice: Account, filename: str
):
    response = await client.post(
        "/api/v1/jobs/ingest",
        files={"file": (filename, b"id,amount\n1,5.00\n", "text/csv")},
        headers=alice.headers,
    )
    assert response.status_code == 400


async def test_upload_accepts_an_uppercase_extension(
    client: AsyncClient, db: AsyncSession, alice: Account
):
    response = await client.post(
        "/api/v1/jobs/ingest",
        files={"file": ("DATA.CSV", b"id,amount\n1,5.00\n", "text/csv")},
        headers=alice.headers,
    )

    assert response.status_code == 201
    job = await db.get(Job, uuid.UUID(response.json()["id"]))
    assert job is not None
    Path((job.payload or {})["saved_path"]).unlink(missing_ok=True)
