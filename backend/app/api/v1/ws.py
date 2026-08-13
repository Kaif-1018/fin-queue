"""
WebSocket endpoint for real-time job status updates.

Route: ``/ws/jobs/{job_id}``

Flow:
1. Accept the WebSocket connection.
2. Query the DB for the job's current status and send it immediately.
3. If the job is already terminal (COMPLETED / FAILED), close.
4. Otherwise, subscribe to Redis Pub/Sub ``job:{job_id}`` and forward
   each status-change message to the client.
5. Close when a terminal status is received or the client disconnects.
"""

import uuid

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session
from app.models import Job, JobStatus
from app.pubsub import subscribe_job_updates

router = APIRouter()

_TERMINAL_STATUSES = {JobStatus.COMPLETED.value, JobStatus.FAILED.value}


@router.websocket("/ws/jobs/{job_id}")
async def job_status_ws(websocket: WebSocket, job_id: uuid.UUID):
    """Stream real-time status updates for a single job."""
    await websocket.accept()

    try:
        # ── 1. Send current state from the database ──────────
        async with async_session() as session:
            job = await session.get(Job, job_id)

        if job is None:
            await websocket.send_json({"error": f"Job {job_id} not found"})
            await websocket.close(code=4004)
            return

        current = {
            "job_id": str(job.id),
            "status": job.status.value,
            "result": job.result,
        }
        await websocket.send_json(current)

        # If already in a terminal state, nothing more to stream.
        if current["status"] in _TERMINAL_STATUSES:
            await websocket.close()
            return

        # ── 2. Subscribe to Redis Pub/Sub and forward ────────
        async for message in subscribe_job_updates(str(job_id)):
            await websocket.send_json(message)

            if message.get("status") in _TERMINAL_STATUSES:
                await websocket.close()
                return

    except WebSocketDisconnect:
        # Client disconnected — nothing to clean up.
        pass
