"""
Async Job Processing Platform – FastAPI entrypoint.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import text

from app.config import settings
from app.database import engine


# ── Lifespan (startup / shutdown) ─────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Verify database connectivity on startup; dispose engine on shutdown."""
    # Startup: verify the database connection
    async with engine.begin() as conn:
        result = await conn.execute(text("SELECT 1"))
        assert result.scalar() == 1
        print("✅  Database connection verified")

    yield  # ← application runs here

    # Shutdown: cleanly close all connections
    await engine.dispose()
    print("🛑  Database connections closed")


# ── App ───────────────────────────────────────────────────────────
app = FastAPI(
    title=settings.APP_NAME,
    version="0.1.0",
    lifespan=lifespan,
)


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
