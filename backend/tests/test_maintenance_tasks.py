"""
Unit and integration tests for Day 13 periodic maintenance tasks.

Covers:
  - prune_stale_files: volume pruning of expired export reports and uploaded CSVs
  - reap_stale_jobs: recovery of zombie jobs stuck in PROCESSING past timeout
  - Celery Beat schedule registration
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.celery_app import celery_app
from app.models import Job, JobStatus
from app.tasks import prune_stale_files, reap_stale_jobs


# ── File Pruning Tests ────────────────────────────────────────────


def test_prune_stale_files_deletes_old_and_preserves_recent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Old files past the retention threshold are deleted; recent ones remain."""
    exports_dir = tmp_path / "exports"
    uploads_dir = tmp_path / "uploads"
    exports_dir.mkdir()
    uploads_dir.mkdir()

    monkeypatch.setattr("app.tasks.EXPORTS_DIR", exports_dir)
    monkeypatch.setattr("app.tasks.UPLOADS_DIR", uploads_dir)

    now = time.time()
    two_days_ago = now - (48 * 3600)
    ten_minutes_ago = now - (600)

    # Old files
    old_export = exports_dir / "old_report.csv"
    old_export.write_text("id,amount\n1,10.00\n", encoding="utf-8")
    os.utime(old_export, (two_days_ago, two_days_ago))

    old_upload = uploads_dir / "old_upload.csv"
    old_upload.write_text("id,amount\n2,20.00\n", encoding="utf-8")
    os.utime(old_upload, (two_days_ago, two_days_ago))

    # Fresh files
    fresh_export = exports_dir / "fresh_report.csv"
    fresh_export.write_text("id,amount\n3,30.00\n", encoding="utf-8")
    os.utime(fresh_export, (ten_minutes_ago, ten_minutes_ago))

    fresh_upload = uploads_dir / "fresh_upload.csv"
    fresh_upload.write_text("id,amount\n4,40.00\n", encoding="utf-8")
    os.utime(fresh_upload, (ten_minutes_ago, ten_minutes_ago))

    # Run pruning with 24h retention
    result = prune_stale_files.run(
        exports_retention_hours=24,
        uploads_retention_hours=24,
    )

    # Assertions
    assert result["pruned_exports"] == 1
    assert result["pruned_uploads"] == 1
    assert result["bytes_freed"] > 0

    assert not old_export.exists()
    assert not old_upload.exists()
    assert fresh_export.exists()
    assert fresh_upload.exists()


def test_prune_stale_files_handles_missing_or_empty_directories(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Non-existent or empty directories are handled cleanly without error."""
    missing_dir = tmp_path / "nonexistent"
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()

    monkeypatch.setattr("app.tasks.EXPORTS_DIR", missing_dir)
    monkeypatch.setattr("app.tasks.UPLOADS_DIR", empty_dir)

    result = prune_stale_files.run(
        exports_retention_hours=24,
        uploads_retention_hours=24,
    )

    assert result["pruned_exports"] == 0
    assert result["pruned_uploads"] == 0
    assert result["bytes_freed"] == 0


# ── Stale Job Reaper Tests ────────────────────────────────────────


def test_reap_stale_jobs_marks_abandoned_processing_jobs_as_failed(
    sync_db: Session,
    sync_user: Any,
    make_sync_job: Any,
) -> None:
    """A job stuck in PROCESSING past the threshold is transitioned to FAILED."""
    job = make_sync_job(owner=sync_user, status=JobStatus.PROCESSING)

    # Age the updated_at timestamp to 45 minutes ago
    cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=45)
    sync_db.execute(
        update(Job).where(Job.id == job.id).values(updated_at=cutoff_time)
    )
    sync_db.commit()

    outcome = reap_stale_jobs.run(stale_threshold_minutes=30)

    assert str(job.id) in outcome["job_ids"]
    assert outcome["reaped_count"] == 1

    sync_db.expire_all()
    refreshed = sync_db.get(Job, job.id)
    assert refreshed is not None
    assert refreshed.status is JobStatus.FAILED
    assert "timed out" in refreshed.result.get("error", "")


def test_reap_stale_jobs_ignores_active_and_terminal_jobs(
    sync_db: Session,
    sync_user: Any,
    make_sync_job: Any,
) -> None:
    """Recent PROCESSING jobs and jobs in terminal states are never reaped."""
    now = datetime.now(timezone.utc)
    old_time = now - timedelta(minutes=60)

    # Active processing job (just started 5 minutes ago)
    active_job = make_sync_job(owner=sync_user, status=JobStatus.PROCESSING)
    sync_db.execute(
        update(Job)
        .where(Job.id == active_job.id)
        .values(updated_at=now - timedelta(minutes=5))
    )

    # Old completed job
    completed_job = make_sync_job(
        owner=sync_user,
        status=JobStatus.COMPLETED,
        result={"file_name": "data.csv"},
    )
    sync_db.execute(
        update(Job)
        .where(Job.id == completed_job.id)
        .values(updated_at=old_time)
    )

    # Old cancelled job
    cancelled_job = make_sync_job(owner=sync_user, status=JobStatus.CANCELLED)
    sync_db.execute(
        update(Job)
        .where(Job.id == cancelled_job.id)
        .values(updated_at=old_time)
    )

    sync_db.commit()

    outcome = reap_stale_jobs.run(stale_threshold_minutes=30)

    assert outcome["reaped_count"] == 0
    assert outcome["job_ids"] == []

    sync_db.expire_all()
    assert sync_db.get(Job, active_job.id).status is JobStatus.PROCESSING
    assert sync_db.get(Job, completed_job.id).status is JobStatus.COMPLETED
    assert sync_db.get(Job, cancelled_job.id).status is JobStatus.CANCELLED


# ── Beat Schedule Configuration ───────────────────────────────────


def test_celery_beat_schedule_registered() -> None:
    """Celery Beat schedule contains both pruning and reaping tasks."""
    schedule = celery_app.conf.beat_schedule
    assert "prune-stale-files-hourly" in schedule
    assert schedule["prune-stale-files-hourly"]["task"] == "app.tasks.prune_stale_files"
    assert schedule["prune-stale-files-hourly"]["schedule"] == 3600.0

    assert "reap-stale-jobs-every-15-minutes" in schedule
    assert (
        schedule["reap-stale-jobs-every-15-minutes"]["task"]
        == "app.tasks.reap_stale_jobs"
    )
    assert schedule["reap-stale-jobs-every-15-minutes"]["schedule"] == 900.0
