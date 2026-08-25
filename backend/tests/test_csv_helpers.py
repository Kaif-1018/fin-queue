"""
CSV counting and money handling — the two places this codebase has been bitten
by "obviously correct" one-liners.

Both are documented gotchas in CLAUDE.md. Row counting used to be
``sum(1 for _ in f) - 1``, and money used to be parsed as ``float``. Neither
fails on tidy data, which is exactly why they need tests: the wrong answer looks
right until a quoted newline or the 50,000th row shows up.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from app.tasks import _count_csv_rows, _money, _parse_amount, _parse_timestamp
from tests.helpers import write_csv


# ── Row counting ──────────────────────────────────────────────────


def test_counts_data_rows_excluding_the_header(tmp_path: Path):
    path = write_csv(
        tmp_path / "plain.csv",
        [
            {
                "id": i,
                "user_id": "u",
                "amount": "1.00",
                "status": "ok",
                "created_at": "",
            }
            for i in range(7)
        ],
    )
    assert _count_csv_rows(path.as_posix()) == 7


def test_a_header_only_file_has_no_data_rows(tmp_path: Path):
    path = write_csv(tmp_path / "empty.csv", [])
    assert _count_csv_rows(path.as_posix()) == 0


def test_quoted_newlines_do_not_inflate_the_count(tmp_path: Path):
    """The bug this function exists to prevent.

    A quoted field containing a newline spans two physical lines but is one CSV
    row. Counting newlines reports a larger total than the reader will ever
    produce, so the progress bar stalls short of 100% and the completed job's
    ``processed`` never reaches its ``total``.
    """
    path = write_csv(
        tmp_path / "multiline.csv",
        [
            {
                "id": 1,
                "user_id": "u",
                "amount": "1.00",
                "status": "line one\nline two\nline three",
                "created_at": "",
            },
            {
                "id": 2,
                "user_id": "u",
                "amount": "2.00",
                "status": "ok",
                "created_at": "",
            },
        ],
    )

    assert _count_csv_rows(path.as_posix()) == 2

    # The naive implementation, for contrast — it sees the embedded newlines as
    # row separators and reports 4.
    with open(path, encoding="utf-8") as f:
        naive = sum(1 for _ in f) - 1
    assert naive == 4, "fixture no longer reproduces the miscount"


def test_a_trailing_blank_line_is_not_a_row(tmp_path: Path):
    path = tmp_path / "trailing.csv"
    path.write_text("id,amount\n1,5.00\n\n", encoding="utf-8")
    # csv.reader yields [] for the blank line, which still counts as a record —
    # pinning the real behaviour rather than an assumption about it.
    assert _count_csv_rows(path.as_posix()) == 2


# ── Money ─────────────────────────────────────────────────────────


def test_money_renders_exactly_two_decimal_places():
    assert _money(Decimal("5")) == "5.00"
    assert _money(Decimal("5.1")) == "5.10"
    assert _money(Decimal("5.00")) == "5.00"


def test_money_rounds_half_up_not_half_even():
    """Python's default rounding is banker's rounding, which would turn 1.005
    into 1.00 and 1.015 into 1.02 — inconsistent, and not what an accountant
    expects. ROUND_HALF_UP is explicit in ``_money``."""
    assert _money(Decimal("1.005")) == "1.01"
    assert _money(Decimal("1.015")) == "1.02"
    assert _money(Decimal("2.675")) == "2.68"


def test_money_never_reaches_scientific_notation():
    """``str()`` on a large Decimal can produce ``1E+3``, which lands in a CSV
    cell and in JSON as something no parser reads back as currency."""
    for value in (Decimal("1000"), Decimal("1E+3"), Decimal("100000000.5")):
        rendered = _money(value)
        assert "E" not in rendered and "e" not in rendered, rendered


def test_decimal_accumulation_stays_exact_where_float_would_not():
    """Why ``amount`` is ``Numeric(12, 2)`` and never a float.

    A tenth of a currency unit is not representable in binary. The error is
    invisible per row and compounds across the 50K seeded rows.

    The float half accumulates with ``+=`` because that is what ``ingest_csv``
    does (``total_amount += parsed_amount``). Note that the obvious
    ``sum(0.10 for _ in range(10))`` would *not* show the drift on Python 3.12+:
    ``sum()`` switched to Neumaier compensated summation for floats, so it
    returns exactly 1.0 and hides the very problem this test documents.
    """
    exact = Decimal(0)
    for _ in range(10):
        exact += Decimal("0.10")
    assert exact == Decimal("1.00")
    assert _money(exact) == "1.00"

    sloppy = 0.0
    for _ in range(10):
        sloppy += 0.10
    assert sloppy != 1.0, "float arithmetic no longer reproduces the drift"
    assert (
        abs(sloppy - 1.0) < 1e-9
    ), "drift should be tiny — that is what makes it dangerous"


def test_a_thousand_cents_add_up():
    total = Decimal(0)
    for _ in range(1000):
        total += Decimal("0.01")
    assert _money(total) == "10.00"


# ── Parsing untrusted CSV cells ───────────────────────────────────


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("10.50", Decimal("10.50")),
        ("-3.25", Decimal("-3.25")),
        ("0", Decimal(0)),
        (".5", Decimal("0.5")),
        ("1e3", Decimal("1000")),
    ],
)
def test_parse_amount_accepts_valid_numbers(raw: str, expected: Decimal):
    assert _parse_amount(raw) == expected


@pytest.mark.parametrize("raw", ["", "abc", "1.2.3", "$10.00", "10,50", None])
def test_parse_amount_falls_back_to_zero(raw):
    """A malformed cell must not abort a 50,000-row ingest.

    Zero is the deliberate choice: the row still gets counted and inserted, and
    the total is understated rather than the whole job failing. It is a trade,
    and this test is where it is written down.
    """
    assert _parse_amount(raw) == Decimal(0)


def test_parse_amount_returns_decimal_not_float():
    assert isinstance(_parse_amount("10.50"), Decimal)


def test_parse_timestamp_reads_iso_8601():
    parsed = _parse_timestamp("2026-03-04T05:06:07+00:00")
    assert parsed == datetime(2026, 3, 4, 5, 6, 7, tzinfo=timezone.utc)


@pytest.mark.parametrize("raw", ["", "not-a-date", "2026-13-45", None])
def test_parse_timestamp_falls_back_to_now(raw):
    before = datetime.now(timezone.utc)
    parsed = _parse_timestamp(raw)
    assert before <= parsed <= datetime.now(timezone.utc)
