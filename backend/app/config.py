"""
Async Job Processing Platform - Configuration

Loads all settings from environment variables using pydantic-settings.
"""

from typing import Literal

from pydantic import model_validator
from pydantic_settings import BaseSettings

# The in-repo default for JWT_SECRET_KEY. Anyone who can read this file can forge
# a token signed with it, so booting with it outside DEBUG is refused below.
DEV_JWT_SECRET = "dev-only-insecure-secret-change-me"


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

    # ── Auth ──────────────────────────────────────────────────────
    JWT_SECRET_KEY: str = DEV_JWT_SECRET
    JWT_ALGORITHM: str = "HS256"
    # Short by design: the WebSocket carries this token in its query string, so
    # it lands in access logs and browser history. Expiry bounds that exposure.
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60

    # ── Logging ───────────────────────────────────────────────────
    # "console" for colourised local output, "json" for machine-parseable logs.
    LOG_FORMAT: Literal["console", "json"] = "console"
    LOG_LEVEL: str = "INFO"

    # Echo every SQL statement. Kept separate from DEBUG on purpose: with
    # echo=True SQLAlchemy's InstanceLogger bypasses log-level checks entirely,
    # so this cannot be quietened after the fact — and it logs bound parameters,
    # which is a data-leak risk anywhere real.
    SQL_ECHO: bool = False

    # ── Maintenance & Celery Beat (Day 13) ────────────────────────
    # Retention period in hours before files in exports/ and uploads/ are pruned.
    EXPORTS_RETENTION_HOURS: int = 24
    UPLOADS_RETENTION_HOURS: int = 24
    # Inactivity threshold in minutes before a stuck PROCESSING job is reaped.
    STALE_JOB_THRESHOLD_MINUTES: int = 30

    @model_validator(mode="after")
    def _reject_dev_secret_outside_debug(self) -> "Settings":
        """Refuse to start with the repo's placeholder signing key in production.

        Raised at import so the failure is a startup crash with a clear message
        rather than an API that quietly issues forgeable tokens.
        """
        if not self.DEBUG and self.JWT_SECRET_KEY == DEV_JWT_SECRET:
            raise ValueError(
                "JWT_SECRET_KEY is still the development placeholder while "
                "DEBUG=False. Set it to a random secret "
                "(python -c 'import secrets; print(secrets.token_urlsafe(32))')."
            )
        return self

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
