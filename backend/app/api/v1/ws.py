"""
WebSocket endpoint for real-time job status updates.

Route: ``/ws/jobs/{job_id}?token=<jwt>``

Flow:
1. Authenticate the token and confirm the caller owns the job — before the
   handshake completes, so an unauthorised client never gets a live socket.
2. Send the job's current status from the database immediately.
3. If the job is already terminal, close.
4. Otherwise, subscribe to Redis Pub/Sub ``job:{job_id}`` and forward
   each status-change message to the client.
5. Close when a terminal status is received or the client disconnects.

**Why the token is in the query string.** Browsers cannot set an
``Authorization`` header on a WebSocket — the ``WebSocket`` constructor takes a
URL and a subprotocol list, nothing else. So the token rides in the URL, where it
will appear in access logs, proxy logs, and browser history. That exposure is
bounded by ``ACCESS_TOKEN_EXPIRE_MINUTES`` (see config.py). The upgrade path, if
this ever needs to be airtight, is to accept the socket unauthenticated and
require the token as the client's first frame, so it never touches a URL.

Close codes (4000-4999 is the application-private range):
  4401 — no token, or a token that does not resolve to an active user
  4404 — job does not exist, or is not owned by the caller (same code on
         purpose: distinguishing them would confirm which UUIDs are real)
"""

import uuid

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect

from app.database import async_session
from app.deps import get_current_user_ws
from app.logging_config import get_logger
from app.models import Job
from app.pubsub import subscribe_job_updates
from app.state import TERMINAL_STATUSES

log = get_logger(__name__)

router = APIRouter()

# Application-private close codes (the 4000-4999 range is reserved for exactly
# this). The frontend keys off these to distinguish a permanent refusal, which
# must not be retried, from a transient drop, which should be.
WS_UNAUTHORISED = 4401
WS_NOT_FOUND = 4404

# Derived from the state machine rather than restated as a literal. The previous
# hand-written {COMPLETED, FAILED} set silently went stale when Day 9 added
# CANCELLED: a socket on a cancelled job was never closed server-side and its
# Redis subscription leaked for as long as the client stayed connected. Reading
# the set from state.py means the next status added cannot desync this file.
_TERMINAL_VALUES = frozenset(s.value for s in TERMINAL_STATUSES)


@router.websocket("/ws/jobs/{job_id}")
async def job_status_ws(
    websocket: WebSocket,
    job_id: uuid.UUID,
    token: str | None = Query(default=None),
):
    """Stream real-time status updates for one of the caller's jobs."""
    # ── 1. Authenticate and authorise ─────────────────────────────
    # The socket is accepted *before* the check so the rejection can carry a
    # specific close code. Closing prior to accept() looks tidier, but Starlette
    # turns it into a bare HTTP 403 on the handshake and the 4401/4404 below
    # never reach the client — a browser sees only code 1006 (abnormal closure),
    # indistinguishable from a dropped network, so it retries a rejection that
    # will never succeed. Accepting costs nothing here: no job data is read or
    # sent until ownership is confirmed.
    await websocket.accept()

    async with async_session() as session:
        user = await get_current_user_ws(session, token)

        if user is None:
            log.info("ws.rejected", job_id=str(job_id), reason="unauthenticated")
            await websocket.close(code=WS_UNAUTHORISED)
            return

        job = await session.get(Job, job_id)

    # Authentication alone is not enough: without this ownership check any
    # logged-in user could watch anyone else's job by its UUID. Both cases share
    # one close code so the client cannot tell "not yours" from "no such job".
    if job is None or job.user_id != user.id:
        log.info(
            "ws.rejected",
            job_id=str(job_id),
            user_id=str(user.id),
            reason="not_found_or_not_owned",
        )
        await websocket.close(code=WS_NOT_FOUND)
        return

    try:
        # ── 2. Send current state from the database ───────────
        # Read before subscribing, because Redis pub/sub has no replay: any
        # message published while this client was disconnected is gone. The
        # database is the recovery path.
        current = {
            "job_id": str(job.id),
            "status": job.status.value,
            "result": job.result,
        }
        await websocket.send_json(current)

        # If already in a terminal state, nothing more to stream.
        if current["status"] in _TERMINAL_VALUES:
            await websocket.close()
            return

        # ── 3. Subscribe to Redis Pub/Sub and forward ─────────
        async for message in subscribe_job_updates(str(job_id)):
            await websocket.send_json(message)

            if message.get("status") in _TERMINAL_VALUES:
                await websocket.close()
                return

    except WebSocketDisconnect:
        # Client disconnected — nothing to clean up.
        pass
