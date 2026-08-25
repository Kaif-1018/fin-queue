"""
Password hashing and JWT encode/decode.

Deliberately free of database and FastAPI imports: everything here is a pure
function over strings, so it can be unit-tested without a session, an app, or a
running Postgres. The parts that *do* need a session live in :mod:`app.deps`.
"""

from __future__ import annotations

import re
import uuid as uuid_pkg
from datetime import datetime, timedelta, timezone

import bcrypt
import jwt

from app.config import settings
from app.logging_config import get_logger

log = get_logger(__name__)


# ── Passwords ─────────────────────────────────────────────────────
# bcrypt hashes at most the first 72 bytes of input and silently ignores the
# rest, which would make "<72 correct bytes><anything>" verify successfully.
# Rather than truncate quietly, reject over-long passwords at the schema layer
# (see UserCreate.max_length) and assert the invariant here too.

BCRYPT_MAX_BYTES = 72

# A well-formed bcrypt hash: version tag, two-digit cost, then 53 characters of
# radix-64 salt and digest — 60 in total.
#
# This is checked before calling into bcrypt because bcrypt 4.x is a Rust
# extension, and a structurally invalid hash makes it *panic* rather than raise:
# pyo3 surfaces that as PanicException, which derives from BaseException and so
# slips past `except Exception` entirely. Letting one reach checkpw would break
# verify_password's "never raises" contract and turn a corrupt column value into
# an unhandled teardown of the login request.
_BCRYPT_HASH = re.compile(r"^\$2[abxy]\$\d{2}\$[./A-Za-z0-9]{53}$")


class PasswordTooLong(ValueError):
    """The password exceeds bcrypt's 72-byte input limit."""


def _encode(raw: str) -> bytes:
    """UTF-8 encode a password, refusing anything bcrypt would truncate."""
    encoded = raw.encode("utf-8")
    if len(encoded) > BCRYPT_MAX_BYTES:
        raise PasswordTooLong(
            f"Password is {len(encoded)} bytes; bcrypt accepts at most "
            f"{BCRYPT_MAX_BYTES}. Note that non-ASCII characters cost more than "
            f"one byte each."
        )
    return encoded


def hash_password(raw: str) -> str:
    """Hash *raw* with bcrypt, returning the encoded hash as a string."""
    return bcrypt.hashpw(_encode(raw), bcrypt.gensalt()).decode("utf-8")


def verify_password(raw: str, hashed: str) -> bool:
    """True if *raw* matches *hashed*. Never raises."""
    if not _BCRYPT_HASH.match(hashed):
        # Not a bcrypt hash at all: a truncated column, a placeholder, or a
        # scheme this build cannot verify. Failing the check is correct, and it
        # keeps a malformed value out of the Rust extension (see _BCRYPT_HASH).
        log.warning("auth.stored_hash_malformed", length=len(hashed))
        return False

    try:
        return bcrypt.checkpw(_encode(raw), hashed.encode("utf-8"))
    except (PasswordTooLong, ValueError, TypeError):
        # An over-long candidate is a failed verification, not an error the
        # caller should have to handle.
        return False


# A valid hash of a value nobody will guess. The login handler verifies against
# this when the email is unknown, so an unregistered email costs the same time
# as a registered one — otherwise response latency enumerates the user table.
_DUMMY_HASH = hash_password(uuid_pkg.uuid4().hex)


def dummy_verify(raw: str) -> None:
    """Burn one bcrypt verification to equalise timing on an unknown email."""
    verify_password(raw, _DUMMY_HASH)


# ── Tokens ────────────────────────────────────────────────────────


def create_access_token(subject: str) -> str:
    """Mint a signed JWT whose ``sub`` is *subject* (the user's UUID as a str)."""
    now = datetime.now(timezone.utc)
    payload = {
        "sub": subject,
        "iat": now,
        "exp": now + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES),
    }
    return jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


def decode_access_token(token: str) -> str | None:
    """Return the token's subject, or ``None`` if it is unusable.

    Never raises. Callers get one bit of information — a subject or nothing —
    so there is no way to accidentally treat "expired" as "valid but odd".
    ``algorithms`` is pinned to a single value: accepting a list the token gets
    to choose from is how ``alg: none`` forgeries get through.
    """
    try:
        payload = jwt.decode(
            token,
            settings.JWT_SECRET_KEY,
            algorithms=[settings.JWT_ALGORITHM],
        )
    except jwt.PyJWTError as exc:
        log.info("auth.token_rejected", reason=type(exc).__name__)
        return None

    subject = payload.get("sub")
    if not isinstance(subject, str) or not subject:
        log.info("auth.token_rejected", reason="missing_sub")
        return None

    return subject
