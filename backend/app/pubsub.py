"""
Redis Pub/Sub helpers for real-time job status updates.

Two flavours are provided:
  • Async (for FastAPI / WebSocket handlers)  — uses ``redis.asyncio``
  • Sync  (for Celery workers)                — uses ``redis.Redis``

Channel naming convention:  ``job:{job_id}``
"""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from typing import Any

import redis as sync_redis
import redis.asyncio as aio_redis

from app.config import settings

# ── Channel helper ────────────────────────────────────────────────

def _channel(job_id: str) -> str:
    """Return the Redis Pub/Sub channel name for a given job."""
    return f"job:{job_id}"


# ── Async helpers (FastAPI / WebSocket side) ──────────────────────

async def subscribe_job_updates(job_id: str) -> AsyncGenerator[dict[str, Any], None]:
    """
    Subscribe to status updates for *job_id* and yield parsed messages.

    Usage::

        async for msg in subscribe_job_updates(job_id):
            await ws.send_json(msg)

    The generator creates its own Redis connection, subscribes to the
    channel, and cleans up on ``async for`` exit (or explicit ``.aclose()``).
    """
    conn = aio_redis.from_url(settings.REDIS_URL, decode_responses=True)
    pubsub = conn.pubsub()
    try:
        await pubsub.subscribe(_channel(job_id))
        async for raw_message in pubsub.listen():
            if raw_message["type"] != "message":
                continue
            data: dict[str, Any] = json.loads(raw_message["data"])
            yield data
    finally:
        await pubsub.unsubscribe(_channel(job_id))
        await pubsub.aclose()
        await conn.aclose()


async def publish_job_update(job_id: str, status: str, result: Any = None) -> None:
    """Publish a job-status message (async version — rarely needed)."""
    conn = aio_redis.from_url(settings.REDIS_URL, decode_responses=True)
    try:
        payload = json.dumps({"job_id": job_id, "status": status, "result": result})
        await conn.publish(_channel(job_id), payload)
    finally:
        await conn.aclose()


# ── Sync helper (Celery worker side) ──────────────────────────────

def publish_job_update_sync(job_id: str, status: str, result: Any = None) -> None:
    """
    Publish a job-status message synchronously.

    Designed to be called from inside a Celery task after committing a
    status change to PostgreSQL.
    """
    conn = sync_redis.from_url(settings.REDIS_URL, decode_responses=True)
    try:
        payload = json.dumps({"job_id": job_id, "status": status, "result": result})
        conn.publish(_channel(job_id), payload)
    finally:
        conn.close()
