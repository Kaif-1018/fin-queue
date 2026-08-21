"""
Async Job Processing Platform – FastAPI entrypoint.
"""

import uuid
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from app.api.v1.jobs import router as jobs_router
from app.api.v1.ws import router as ws_router
from app.config import settings
from app.database import engine
from app.logging_config import configure_logging, get_logger

configure_logging()
log = get_logger(__name__)


# ── Lifespan (startup / shutdown) ─────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Verify database connectivity on startup; dispose engine on shutdown.

    Schema creation deliberately does *not* happen here — Alembic owns the
    schema and the container entrypoint runs ``alembic upgrade head``. Calling
    ``create_all`` as well lets the live schema drift away from the migration
    history and makes ``--autogenerate`` produce nonsense.
    """
    async with engine.begin() as conn:
        result = await conn.execute(text("SELECT 1"))
        assert result.scalar() == 1
    log.info("startup.database_connected")

    yield  # ← application runs here

    await engine.dispose()
    log.info("shutdown.database_disposed")


# ── App ───────────────────────────────────────────────────────────
app = FastAPI(
    title=settings.APP_NAME,
    version="0.1.0",
    lifespan=lifespan,
)


# ── Request ID middleware ─────────────────────────────────────────
@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    """Bind a request ID to the logging context and echo it back.

    Honours an inbound ``X-Request-ID`` so a caller (or a future load balancer)
    can correlate its own trace with ours.
    """
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())

    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(
        request_id=request_id,
        method=request.method,
        path=request.url.path,
    )

    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


# ── CORS (allow React dev server + production frontend) ──────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",   # Vite dev server
        "http://localhost:3000",   # Production (nginx)
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Request-ID"],
)


# ── Routers ───────────────────────────────────────────────────────
app.include_router(jobs_router, prefix="/api/v1")
app.include_router(ws_router)


# ── Health check ──────────────────────────────────────────────────
@app.get("/health", tags=["ops"])
async def health_check():
    """Simple liveness probe."""
    return {"status": "healthy", "service": settings.APP_NAME}


@app.get("/", tags=["ops"])
async def root():
    """Root endpoint – welcome message."""
    return {
        "message": f"Welcome to {settings.APP_NAME}",
        "docs": "/docs",
    }
