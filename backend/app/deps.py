"""
FastAPI dependencies for authentication.

Splits into two flavours because the two protocols fail differently:

  • :func:`get_current_user`    — HTTP. Raises ``HTTPException`` (401 / 403).
  • :func:`get_current_user_ws` — WebSocket. Returns ``None``; there is no HTTP
    status to answer a failed upgrade with, so the caller closes the socket.
"""

from __future__ import annotations

import uuid as uuid_pkg

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.logging_config import get_logger
from app.models import User
from app.security import decode_access_token

log = get_logger(__name__)

# tokenUrl is what Swagger's "Authorize" button posts to. It must match the
# login route registered in app.main, or /docs cannot authenticate.
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login")


CREDENTIALS_EXCEPTION = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Could not validate credentials",
    headers={"WWW-Authenticate": "Bearer"},
)


async def _load_user(session: AsyncSession, token: str) -> User | None:
    """Resolve *token* to an active user, or ``None``.

    One code path for both protocols so HTTP and WebSocket cannot drift apart on
    what counts as a valid caller.
    """
    subject = decode_access_token(token)
    if subject is None:
        return None

    try:
        user_id = uuid_pkg.UUID(subject)
    except ValueError:
        # A well-signed token whose sub is not a UUID: either a hand-crafted
        # payload or a format change that outlived its tokens.
        log.warning("auth.token_subject_malformed")
        return None

    user = await session.get(User, user_id)
    if user is None:
        # Signed correctly but the account is gone — a token outliving its user.
        log.info("auth.user_not_found", user_id=str(user_id))
        return None

    return user


# ── HTTP ──────────────────────────────────────────────────────────


async def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_db),
) -> User:
    """Resolve the bearer token to the calling user.

    401 for anything wrong with the token or a missing account; 403 for a real
    but deactivated account. The distinction matters: a deactivated user should
    be told to contact support, not to log in again.
    """
    user = await _load_user(db, token)
    if user is None:
        raise CREDENTIALS_EXCEPTION

    if not user.is_active:
        log.info("auth.inactive_user_rejected", user_id=str(user.id))
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This account is deactivated.",
        )

    return user


# ── WebSocket ─────────────────────────────────────────────────────


async def get_current_user_ws(db: AsyncSession, token: str | None) -> User | None:
    """WebSocket flavour of :func:`get_current_user`. Returns ``None`` on failure.

    Not a FastAPI dependency: the endpoint needs to close the socket with a
    specific code rather than let an exception become a 500, so it calls this
    directly and decides what to send.
    """
    if not token:
        return None

    user = await _load_user(db, token)
    if user is None or not user.is_active:
        return None

    return user
