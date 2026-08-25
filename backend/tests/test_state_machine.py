"""
The state machine as a table of rules — no database, no app.

These tests are deliberately about *invariants* rather than a list of allowed
pairs. Restating the table in the assertions would only prove the table equals
itself; a rule like "terminal means no outgoing edges" keeps holding when a
seventh status is added, and fails loudly if that status is wired up wrong.

That distinction is not hypothetical here. ws.py once kept its own
``{COMPLETED, FAILED}`` literal, and when CANCELLED arrived the copy went stale
and leaked a Redis subscription per socket. ``test_terminal_set_is_derivable``
is the test that would have caught it.
"""

from __future__ import annotations

import pytest

from app.models import JobStatus
from app.state import (
    ALLOWED_TRANSITIONS,
    TERMINAL_STATUSES,
    IllegalTransition,
    is_terminal,
)


# ── Shape of the table ────────────────────────────────────────────


def test_every_status_has_an_entry():
    """A status missing from the table makes ``_check`` raise KeyError, which
    surfaces as a 500 rather than a refused transition."""
    assert set(ALLOWED_TRANSITIONS) == set(JobStatus)


def test_every_target_is_a_real_status():
    for source, targets in ALLOWED_TRANSITIONS.items():
        for target in targets:
            assert isinstance(target, JobStatus), f"{source} → {target!r}"


def test_no_status_transitions_to_itself():
    """Self-transitions are what the claim relies on being illegal."""
    for status, targets in ALLOWED_TRANSITIONS.items():
        assert status not in targets, f"{status.value} → itself is allowed"


# ── Terminality ───────────────────────────────────────────────────


def test_terminal_set_is_derivable():
    """``TERMINAL_STATUSES`` must equal "statuses with no outgoing edges".

    Anything that reads a hard-coded terminal set — ws.py did, App.jsx still
    does — goes stale the moment a status is added. Deriving it here means the
    table is the single source of truth, and a new dead-end status is terminal
    automatically.
    """
    derived = {s for s in JobStatus if not ALLOWED_TRANSITIONS[s]}
    assert TERMINAL_STATUSES == derived


@pytest.mark.parametrize("status", sorted(TERMINAL_STATUSES, key=lambda s: s.value))
def test_terminal_statuses_are_dead_ends(status: JobStatus):
    assert ALLOWED_TRANSITIONS[status] == frozenset()
    assert is_terminal(status)


@pytest.mark.parametrize(
    "status", [JobStatus.PENDING, JobStatus.QUEUED, JobStatus.PROCESSING]
)
def test_live_statuses_are_not_terminal(status: JobStatus):
    assert not is_terminal(status)
    assert ALLOWED_TRANSITIONS[status], f"{status.value} has no way out"


def test_every_status_can_reach_a_terminal_state():
    """No status may strand a job forever.

    A breadth-first walk from each status must reach a dead end; otherwise a job
    in that state can never be finished, cancelled, or failed.
    """
    for start in JobStatus:
        seen: set[JobStatus] = set()
        frontier = [start]
        while frontier:
            current = frontier.pop()
            if current in seen:
                continue
            seen.add(current)
            frontier.extend(ALLOWED_TRANSITIONS[current])
        assert seen & TERMINAL_STATUSES, f"{start.value} cannot terminate"


# ── The specific rules the platform's behaviour depends on ────────


def test_processing_cannot_be_reclaimed():
    """``PROCESSING → PROCESSING`` is what makes a duplicate delivery bail.

    ``claim_sync`` is just ``transition_sync(..., PROCESSING, strict=False)``, so
    this single absence is the entire idempotency guarantee. Adding it to make
    "retries work" would silently re-enable double-inserts.
    """
    assert JobStatus.PROCESSING not in ALLOWED_TRANSITIONS[JobStatus.PROCESSING]


def test_finished_work_cannot_be_reclaimed():
    """A redelivered task whose ack was lost must not re-run."""
    for done in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
        assert JobStatus.PROCESSING not in ALLOWED_TRANSITIONS[done]


def test_processing_cannot_be_cancelled():
    """Deliberate: the tasks have no cooperative abort.

    Accepting a mid-flight cancel would tell the user the work stopped while the
    worker kept inserting rows. The durable half of cancellation is this rule —
    Celery's revoke is in-memory per worker and lost on restart.
    """
    assert JobStatus.CANCELLED not in ALLOWED_TRANSITIONS[JobStatus.PROCESSING]


def test_cancellation_is_reachable_only_before_work_starts():
    can_cancel = {s for s in JobStatus if JobStatus.CANCELLED in ALLOWED_TRANSITIONS[s]}
    assert can_cancel == {JobStatus.PENDING, JobStatus.QUEUED}


def test_the_documented_happy_path_is_legal():
    for source, target in (
        (JobStatus.PENDING, JobStatus.QUEUED),
        (JobStatus.QUEUED, JobStatus.PROCESSING),
        (JobStatus.PROCESSING, JobStatus.COMPLETED),
    ):
        assert target in ALLOWED_TRANSITIONS[source]

    # PENDING → PROCESSING short-circuits QUEUED, which nothing currently sets.
    assert JobStatus.PROCESSING in ALLOWED_TRANSITIONS[JobStatus.PENDING]


def test_failure_is_reachable_from_every_live_status():
    """A task can blow up at any point before it finishes."""
    for status in (JobStatus.PENDING, JobStatus.QUEUED, JobStatus.PROCESSING):
        assert JobStatus.FAILED in ALLOWED_TRANSITIONS[status]


# ── The exception ─────────────────────────────────────────────────


def test_illegal_transition_names_what_was_allowed():
    """The message is the only diagnostic a caller gets; it must be actionable."""
    exc = IllegalTransition("job-1", JobStatus.COMPLETED, JobStatus.PROCESSING)

    assert exc.current is JobStatus.COMPLETED
    assert exc.target is JobStatus.PROCESSING
    message = str(exc)
    assert "job-1" in message
    assert "COMPLETED" in message and "PROCESSING" in message
    # A terminal source has nothing to list, and the message says so rather than
    # printing an empty collection.
    assert "terminal" in message
