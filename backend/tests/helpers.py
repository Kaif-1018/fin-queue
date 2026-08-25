"""Small builders shared across test modules.

Kept out of ``conftest.py`` so they can be imported normally — conftest is a
plugin, not a library, and importing from it works by accident rather than by
design.
"""

from __future__ import annotations

import csv
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

INGEST_HEADERS = ["id", "user_id", "amount", "status", "created_at"]


def write_csv(
    path: Path,
    rows: list[dict[str, object]],
    headers: list[str] | None = None,
) -> Path:
    """Write *rows* to *path* as a CSV with a header line, and return the path.

    Uses ``csv.writer`` rather than string formatting so that a value containing
    a comma or a newline is quoted the way a real upload would be — which is the
    whole point of the ``_count_csv_rows`` test.
    """
    headers = headers or INGEST_HEADERS
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return path


def txn_row(
    *,
    row_id: int = 1,
    user_id: str | uuid.UUID | None = None,
    amount: str = "10.00",
    status: str = "completed",
    created_at: datetime | None = None,
) -> dict[str, object]:
    """One CSV row in the shape ``ingest_csv`` expects.

    ``user_id`` defaults to a random UUID on purpose: the ingest task must
    ignore this column entirely and stamp the job's owner instead, so tests want
    a value that is *never* the right answer.
    """
    return {
        "id": row_id,
        "user_id": str(user_id or uuid.uuid4()),
        "amount": amount,
        "status": status,
        "created_at": (created_at or datetime.now(timezone.utc)).isoformat(),
    }


def money(value: str) -> Decimal:
    """``Decimal`` from a string — never from a float. See CLAUDE.md."""
    return Decimal(value)
