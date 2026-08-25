"""
The live status socket: ``/ws/jobs/{job_id}?token=<jwt>``.

Two things make this endpoint worth its own file.

The first is that its refusals are *close codes*, not HTTP statuses, and the
frontend keys off them to decide whether to retry. ws.py accepts the socket
before rejecting it precisely so those codes survive — closing pre-accept makes
Starlette collapse the whole thing into a bare HTTP 403, and the browser sees only
1006, indistinguishable from a dropped network.

The second is that this file has already gone stale once. It used to keep its own
``{COMPLETED, FAILED}`` literal, and when Day 9 added CANCELLED a socket watching
a cancelled job was never closed server-side and leaked its Redis subscription for
as long as the client stayed connected. ``test_a_cancelled_job_closes_the_socket``
is that bug, pinned.

These tests are synchronous. ws.py reaches for ``app.database.async_session``
directly — it needs a session outside the request-dependency lifecycle, so
overriding ``get_db`` does not reach it — which means the module attribute has to
be patched instead. Starlette's ``TestClient`` is the only client here that speaks
WebSocket, and it is sync.
"""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any

import pytest
import redis as sync_redis
from fastapi import WebSocketDisconnect
from fastapi.testclient import TestClient
from sqlalchemy import NullPool
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import settings
from app.models import JobStatus
from app.pubsub import publish_job_update_sync
from app.security import create_access_token
from tests.conftest import TEST_URL_ASYNC

WS_UNAUTHORISED = 4401
WS_NOT_FOUND = 4404

SUBSCRIBE_TIMEOUT = 5.0
CLOSE_TIMEOUT = 5.0


@pytest.fixture
def ws_client(monkeypatch: pytest.MonkeyPatch, sync_engine: Any) -> Any:
    """A TestClient whose WebSocket handler talks to the test database.

    ``TestClient`` is deliberately not entered as a context manager: that would
    run the app's lifespan, which opens a connection on the import-time engine
    still bound to the *development* database. ``websocket_connect`` works
    without it.

    NullPool means there is nothing pooled to dispose when the fixture ends,
    which matters because disposing an async engine needs a running loop and this
    fixture is synchronous.
    """
    from app.api.v1 import ws as ws_module
    from app.main import app

    engine = create_async_engine(TEST_URL_ASYNC, poolclass=NullPool)
    monkeypatch.setattr(
        ws_module,
        "async_session",
        async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False),
    )

    return TestClient(app)


def token_for(user: Any) -> str:
    return create_access_token(str(user.id))


def url_for(job_id: Any, token: str | None = None) -> str:
    path = f"/ws/jobs/{job_id}"
    return f"{path}?token={token}" if token is not None else path


def await_subscriber(job_id: Any) -> None:
    """Block until the handler's Redis subscription is live.

    Redis pub/sub has no replay, so a message published before the handler
    subscribes is simply lost and the test would hang on a receive that never
    arrives. Asking Redis for the subscriber count is deterministic, where
    sleeping for "long enough" is not.
    """
    channel = f"job:{job_id}"
    conn = sync_redis.from_url(settings.REDIS_URL, decode_responses=True)
    try:
        deadline = time.monotonic() + SUBSCRIBE_TIMEOUT
        while time.monotonic() < deadline:
            if dict(conn.pubsub_numsub(channel)).get(channel, 0) >= 1:
                return
            time.sleep(0.02)
        raise AssertionError(f"handler never subscribed to {channel}")
    finally:
        conn.close()


def expect_close(ws: Any, timeout: float = CLOSE_TIMEOUT) -> int:
    """Assert the server closes the socket, and return the close code.

    The naive spelling — ``with pytest.raises(WebSocketDisconnect):
    ws.receive_json()`` — is a trap here. Starlette's ``receive()`` is an untimed
    ``queue.get()``, so a handler that *fails* to close leaves the test blocked
    forever: locally that looks like a frozen run, and in CI it burns the job
    timeout instead of reporting a failure. Since the terminal-status regression
    below is exactly "the socket is never closed", the assertion has to be bounded
    to be worth anything.

    The waiting thread is left parked on the queue if nothing arrives. It is a
    daemon, so it does not hold up interpreter exit.
    """
    outcome: list[Any] = []

    def _wait() -> None:
        try:
            outcome.append(("message", ws.receive_json()))
        except WebSocketDisconnect as exc:
            outcome.append(("closed", exc.code))
        except BaseException as exc:  # noqa: BLE001 - reported below, not swallowed
            outcome.append(("error", exc))

    waiter = threading.Thread(target=_wait, daemon=True)
    waiter.start()
    waiter.join(timeout)

    if waiter.is_alive():
        raise AssertionError(
            f"server did not close the socket within {timeout}s — it is still "
            f"streaming, which is how the stale terminal-status set leaked "
            f"subscriptions"
        )

    kind, value = outcome[0]
    if kind == "error":
        raise AssertionError(f"unexpected error waiting for close: {value!r}")
    if kind == "message":
        raise AssertionError(f"expected a close, got another message: {value!r}")
    return value


# ── Authentication ────────────────────────────────────────────────


def test_a_socket_without_a_token_is_closed_4401(
    ws_client: TestClient, make_sync_job: Any
):
    job = make_sync_job(status=JobStatus.PENDING)

    with ws_client.websocket_connect(url_for(job.id)) as ws:
        code = expect_close(ws)

    assert code == WS_UNAUTHORISED


@pytest.mark.parametrize("token", ["", "not-a-jwt", "a.b.c"])
def test_a_socket_with_an_unusable_token_is_closed_4401(
    ws_client: TestClient, make_sync_job: Any, token: str
):
    job = make_sync_job(status=JobStatus.PENDING)

    with ws_client.websocket_connect(url_for(job.id, token)) as ws:
        code = expect_close(ws)

    assert code == WS_UNAUTHORISED


def test_a_token_naming_no_real_user_is_closed_4401(
    ws_client: TestClient, make_sync_job: Any
):
    """Correctly signed, but the account does not exist."""
    job = make_sync_job(status=JobStatus.PENDING)
    orphan = create_access_token(str(uuid.uuid4()))

    with ws_client.websocket_connect(url_for(job.id, orphan)) as ws:
        code = expect_close(ws)

    assert code == WS_UNAUTHORISED


def test_a_deactivated_user_is_closed_4401(
    ws_client: TestClient, sync_db: Any, make_sync_user: Any, make_sync_job: Any
):
    user = make_sync_user("ws-inactive@example.com")
    job = make_sync_job(owner=user, status=JobStatus.PENDING)

    user.is_active = False
    sync_db.commit()

    with ws_client.websocket_connect(url_for(job.id, token_for(user))) as ws:
        code = expect_close(ws)

    assert code == WS_UNAUTHORISED


# ── Ownership ─────────────────────────────────────────────────────


def test_another_users_job_is_closed_4404(
    ws_client: TestClient, make_sync_user: Any, make_sync_job: Any
):
    """Authentication alone is not enough.

    Without this check any logged-in user could watch anyone else's job — and the
    forwarded messages carry the job's full ``result`` payload.
    """
    owner = make_sync_user("ws-owner@example.com")
    outsider = make_sync_user("ws-outsider@example.com")
    job = make_sync_job(owner=owner, status=JobStatus.PENDING)

    with ws_client.websocket_connect(url_for(job.id, token_for(outsider))) as ws:
        code = expect_close(ws)

    assert code == WS_NOT_FOUND


def test_a_nonexistent_job_is_closed_4404(ws_client: TestClient, sync_user: Any):
    with ws_client.websocket_connect(url_for(uuid.uuid4(), token_for(sync_user))) as ws:
        code = expect_close(ws)

    assert code == WS_NOT_FOUND


def test_an_unowned_job_is_closed_4404(
    ws_client: TestClient, sync_user: Any, make_sync_job: Any
):
    """A NULL owner matches nobody, here as everywhere else."""
    job = make_sync_job(owner=None, status=JobStatus.PENDING)

    with ws_client.websocket_connect(url_for(job.id, token_for(sync_user))) as ws:
        code = expect_close(ws)

    assert code == WS_NOT_FOUND


def test_the_two_refusals_share_one_close_code(
    ws_client: TestClient, make_sync_user: Any, make_sync_job: Any
):
    """ "Not yours" and "no such job" must be indistinguishable.

    Two codes here would let a client enumerate valid job UUIDs by watching which
    refusal it got.
    """
    owner = make_sync_user("ws-a@example.com")
    outsider = make_sync_user("ws-b@example.com")
    real = make_sync_job(owner=owner, status=JobStatus.PENDING)

    codes = []
    for job_id in (real.id, uuid.uuid4()):
        with ws_client.websocket_connect(url_for(job_id, token_for(outsider))) as ws:
            codes.append(expect_close(ws))

    assert codes == [WS_NOT_FOUND, WS_NOT_FOUND]


# ── Current state comes from the database first ───────────────────


def test_the_first_message_is_the_current_database_state(
    ws_client: TestClient, sync_user: Any, make_sync_job: Any
):
    """Read before subscribing, because pub/sub has no replay.

    A client that attaches after the interesting message was published still has
    to learn where the job actually got to.
    """
    job = make_sync_job(
        owner=sync_user, status=JobStatus.PROCESSING, result={"processed": 7}
    )

    with ws_client.websocket_connect(url_for(job.id, token_for(sync_user))) as ws:
        assert ws.receive_json() == {
            "job_id": str(job.id),
            "status": "PROCESSING",
            "result": {"processed": 7},
        }


@pytest.mark.parametrize(
    "status", [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]
)
def test_a_terminal_job_closes_the_socket(
    ws_client: TestClient, sync_user: Any, make_sync_job: Any, status: JobStatus
):
    """The regression test for the stale terminal set.

    CANCELLED is the case that was broken: ws.py's hand-written
    ``{COMPLETED, FAILED}`` did not include it, so the socket stayed open and its
    Redis subscription leaked for the lifetime of the client. Nothing looked wrong
    because the frontend's own copy of the set already listed CANCELLED.
    """
    job = make_sync_job(owner=sync_user, status=status)

    with ws_client.websocket_connect(url_for(job.id, token_for(sync_user))) as ws:
        first = ws.receive_json()
        assert first["status"] == status.value

        # Server-side close, not a client disconnect.
        expect_close(ws)


@pytest.mark.parametrize("status", [JobStatus.PENDING, JobStatus.PROCESSING])
def test_a_live_job_keeps_the_socket_open(
    ws_client: TestClient, sync_user: Any, make_sync_job: Any, status: JobStatus
):
    job = make_sync_job(owner=sync_user, status=status)

    with ws_client.websocket_connect(url_for(job.id, token_for(sync_user))) as ws:
        assert ws.receive_json()["status"] == status.value
        # Still attached: the handler reaches its Redis subscription.
        await_subscriber(job.id)


# ── Forwarding from Redis ─────────────────────────────────────────


def test_a_published_update_is_forwarded(
    ws_client: TestClient, sync_user: Any, make_sync_job: Any
):
    """The whole point of the endpoint: the worker never talks to the browser.

    It publishes to ``job:{id}`` and this handler forwards.
    """
    job = make_sync_job(owner=sync_user, status=JobStatus.PENDING)

    with ws_client.websocket_connect(url_for(job.id, token_for(sync_user))) as ws:
        assert ws.receive_json()["status"] == "PENDING"
        await_subscriber(job.id)

        publish_job_update_sync(
            str(job.id), "PROCESSING", {"processed": 250, "total": 1000}
        )

        forwarded = ws.receive_json()
        assert forwarded["status"] == "PROCESSING"
        assert forwarded["result"] == {"processed": 250, "total": 1000}


def test_progress_updates_do_not_close_the_socket(
    ws_client: TestClient, sync_user: Any, make_sync_job: Any
):
    """Progress is republished PROCESSING, over and over.

    This is why progress deliberately bypasses ``transition()``:
    PROCESSING → PROCESSING is illegal by design, and routing progress through
    the state machine would kill the live progress bar.
    """
    job = make_sync_job(owner=sync_user, status=JobStatus.PENDING)

    with ws_client.websocket_connect(url_for(job.id, token_for(sync_user))) as ws:
        assert ws.receive_json()["status"] == "PENDING"
        await_subscriber(job.id)

        for processed in (100, 200, 300):
            publish_job_update_sync(
                str(job.id), "PROCESSING", {"processed": processed, "total": 300}
            )
            assert ws.receive_json()["result"]["processed"] == processed


def test_a_published_terminal_status_closes_the_socket(
    ws_client: TestClient, sync_user: Any, make_sync_job: Any
):
    job = make_sync_job(owner=sync_user, status=JobStatus.PENDING)

    with ws_client.websocket_connect(url_for(job.id, token_for(sync_user))) as ws:
        assert ws.receive_json()["status"] == "PENDING"
        await_subscriber(job.id)

        publish_job_update_sync(str(job.id), "COMPLETED", {"total_rows": 42})

        assert ws.receive_json()["status"] == "COMPLETED"
        expect_close(ws)


def test_a_published_cancellation_closes_the_socket(
    ws_client: TestClient, sync_user: Any, make_sync_job: Any
):
    """The leak, from the other direction: a cancel arriving mid-stream."""
    job = make_sync_job(owner=sync_user, status=JobStatus.PENDING)

    with ws_client.websocket_connect(url_for(job.id, token_for(sync_user))) as ws:
        assert ws.receive_json()["status"] == "PENDING"
        await_subscriber(job.id)

        publish_job_update_sync(
            str(job.id), "CANCELLED", {"message": "Job cancelled by user"}
        )

        assert ws.receive_json()["status"] == "CANCELLED"
        expect_close(ws)
