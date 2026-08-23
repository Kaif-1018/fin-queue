"""
Authentication endpoints — register, login, and identity.

Route prefix: ``/api/v1/auth``
"""

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.deps import get_current_user
from app.logging_config import get_logger
from app.models import User
from app.schemas import Token, UserCreate, UserResponse
from app.security import (
    create_access_token,
    dummy_verify,
    hash_password,
    verify_password,
)

log = get_logger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])


def _normalise_email(raw: str) -> str:
    """Lower-case an email for storage and lookup.

    Domains are case-insensitive and no mainstream provider treats the local
    part as case-sensitive. Without this, registering ``Ana@x.com`` and logging
    in as ``ana@x.com`` are two different accounts — and the unique index would
    happily hold both.
    """
    return raw.strip().lower()


# ── POST /api/v1/auth/register ────────────────────────────────────

@router.post(
    "/register",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new account",
)
async def register(
    body: UserCreate,
    db: AsyncSession = Depends(get_db),
):
    """Create an account and return it (without the password hash).

    Uniqueness is enforced by catching the unique-violation on commit rather
    than by a SELECT-then-INSERT: two simultaneous registrations of the same
    email both pass a pre-check, and only the index can actually arbitrate.
    """
    user = User(
        email=_normalise_email(body.email),
        hashed_password=hash_password(body.password),
    )
    db.add(user)

    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        log.info("auth.register_duplicate", email=_normalise_email(body.email))
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="An account with that email already exists.",
        ) from None

    await db.refresh(user)
    log.info("auth.registered", user_id=str(user.id))

    return user


# ── POST /api/v1/auth/login ───────────────────────────────────────

@router.post(
    "/login",
    response_model=Token,
    summary="Exchange credentials for an access token",
)
async def login(
    form: OAuth2PasswordRequestForm = Depends(),
    db: AsyncSession = Depends(get_db),
):
    """Verify credentials and return a signed JWT.

    Takes form data rather than JSON because that is what the OAuth2 password
    flow specifies, which is what lets Swagger's Authorize button work. The
    email goes in the ``username`` field.

    An unknown email and a wrong password produce the identical 401, and the
    unknown-email path still runs one bcrypt verification against a dummy hash.
    Skipping that would return "no such user" roughly an order of magnitude
    faster than "wrong password", and that timing difference is enough to
    enumerate the user table.
    """
    email = _normalise_email(form.username)

    user = (
        await db.execute(select(User).where(User.email == email))
    ).scalar_one_or_none()

    if user is None:
        dummy_verify(form.password)
        log.info("auth.login_failed", reason="unknown_email")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not verify_password(form.password, user.hashed_password):
        log.info("auth.login_failed", reason="bad_password", user_id=str(user.id))
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not user.is_active:
        log.info("auth.login_failed", reason="inactive", user_id=str(user.id))
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This account is deactivated.",
        )

    log.info("auth.login_succeeded", user_id=str(user.id))

    return Token(access_token=create_access_token(str(user.id)))


# ── GET /api/v1/auth/me ───────────────────────────────────────────

@router.get(
    "/me",
    response_model=UserResponse,
    summary="Get the authenticated user",
)
async def read_me(current_user: User = Depends(get_current_user)):
    """Return the account behind the bearer token."""
    return current_user
