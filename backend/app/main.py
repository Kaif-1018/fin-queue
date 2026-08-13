"""
Async Job Processing Platform – FastAPI entrypoint.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from app.config import settings
from app.database import Base, engine
from app.api.v1.jobs import router as jobs_router
from app.api.v1.ws import router as ws_router


# ── Lifespan (startup / shutdown) ─────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Verify database connectivity on startup; dispose engine on shutdown."""
    # Startup: verify the database connection and create tables
    async with engine.begin() as conn:
        result = await conn.execute(text("SELECT 1"))
        assert result.scalar() == 1
        print("✅  Database connection verified")

        # Create all tables if they don't exist yet
        await conn.run_sync(Base.metadata.create_all)
        print("✅  Database tables created/verified")

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

