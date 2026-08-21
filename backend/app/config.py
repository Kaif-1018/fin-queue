"""
Async Job Processing Platform - Configuration

Loads all settings from environment variables using pydantic-settings.
"""

from typing import Literal

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings populated from environment variables."""

    # ── Database ──────────────────────────────────────────────────
    DATABASE_URL: str = "postgresql+asyncpg://jobplatform:jobplatform_secret@localhost:5432/jobplatform_db"

    # ── Redis ─────────────────────────────────────────────────────
    REDIS_URL: str = "redis://localhost:6379/0"

    # ── Celery ────────────────────────────────────────────────────
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/1"

    # ── App ───────────────────────────────────────────────────────
    APP_NAME: str = "Async Job Processing Platform"
    DEBUG: bool = True

    # ── Logging ───────────────────────────────────────────────────
    # "console" for colourised local output, "json" for machine-parseable logs.
    LOG_FORMAT: Literal["console", "json"] = "console"
    LOG_LEVEL: str = "INFO"

    # Echo every SQL statement. Kept separate from DEBUG on purpose: with
    # echo=True SQLAlchemy's InstanceLogger bypasses log-level checks entirely,
    # so this cannot be quietened after the fact — and it logs bound parameters,
    # which is a data-leak risk anywhere real.
    SQL_ECHO: bool = False

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
