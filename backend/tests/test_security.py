"""
Password hashing and JWT handling — pure functions, no database.

The security-relevant assertions here are the negative ones. Anything that makes
``decode_access_token`` return a subject when it should return ``None`` is a
forged-token bug, and the whole point of that function returning ``str | None``
is that a caller cannot accidentally treat "expired" as "valid but odd".
"""

from __future__ import annotations

import base64
import json
import uuid
from datetime import datetime, timedelta, timezone

import jwt
import pytest

from app.config import settings
from app.security import (
    BCRYPT_MAX_BYTES,
    PasswordTooLong,
    create_access_token,
    decode_access_token,
    dummy_verify,
    hash_password,
    verify_password,
)


# ── Passwords ─────────────────────────────────────────────────────


def test_hash_then_verify_round_trips():
    hashed = hash_password("correct horse battery staple")
    assert verify_password("correct horse battery staple", hashed)


def test_verify_rejects_the_wrong_password():
    hashed = hash_password("correct horse battery staple")
    assert not verify_password("Correct horse battery staple", hashed)
    assert not verify_password("", hashed)


def test_the_hash_is_not_the_password():
    """A stored hash must not contain the plaintext, and must be salted."""
    raw = "correct horse battery staple"
    first = hash_password(raw)
    second = hash_password(raw)

    assert raw not in first
    assert first != second, "same password hashed twice — salt is not random"
    assert verify_password(raw, first) and verify_password(raw, second)


def test_password_at_the_bcrypt_limit_is_accepted():
    exactly_72 = "a" * BCRYPT_MAX_BYTES
    assert verify_password(exactly_72, hash_password(exactly_72))


def test_over_long_password_is_refused_rather_than_truncated():
    """bcrypt ignores everything past 72 bytes.

    Truncating quietly would make ``<72 correct bytes> + <anything>`` verify, so
    a stolen 72-byte prefix would be as good as the password. Refusing is the
    only safe option, and ``UserCreate.max_length`` refuses it earlier still.
    """
    with pytest.raises(PasswordTooLong):
        hash_password("a" * (BCRYPT_MAX_BYTES + 1))


def test_the_limit_counts_bytes_not_characters():
    """Non-ASCII costs more than one byte, so a 40-character password can be
    over the limit. Counting characters would let it through and truncate."""
    forty_chars = "é" * 40  # 80 bytes in UTF-8
    assert len(forty_chars) == 40
    assert len(forty_chars.encode("utf-8")) > BCRYPT_MAX_BYTES

    with pytest.raises(PasswordTooLong):
        hash_password(forty_chars)


@pytest.mark.parametrize(
    "stored", ["", "not-a-hash", "$2b$12$tooshort", "$argon2id$v=19$m=65536"]
)
def test_verify_returns_false_for_a_corrupt_stored_hash(stored: str):
    """A mangled column value is a failed login, not a 500."""
    assert verify_password("anything", stored) is False


def test_verify_does_not_raise_on_an_over_long_candidate():
    """The candidate comes from a request body. Even though the schema caps it,
    a direct caller must get False rather than an exception."""
    hashed = hash_password("short-enough")
    assert verify_password("a" * 200, hashed) is False


def test_dummy_verify_is_silent():
    """Used on the unknown-email login path purely to burn the same amount of
    time as a real bcrypt check. It must never raise or return anything."""
    assert dummy_verify("whatever") is None


def test_production_cost_factor_is_not_weakened(real_gensalt):
    """The suite runs at bcrypt's minimum cost for speed (see ``_cheap_hashing``).

    That patch must not be able to hide a weakened *default*, so this asserts on
    the unpatched function. bcrypt's own default is what production gets, since
    ``hash_password`` calls ``gensalt()`` with no arguments.
    """
    cost = int(real_gensalt().decode().split("$")[2])
    assert cost >= 12, f"bcrypt default cost dropped to {cost}"


# ── Tokens ────────────────────────────────────────────────────────


def test_token_round_trips_its_subject():
    subject = str(uuid.uuid4())
    assert decode_access_token(create_access_token(subject)) == subject


def test_token_carries_an_expiry_and_issued_at():
    claims = jwt.decode(
        create_access_token("subject"),
        settings.JWT_SECRET_KEY,
        algorithms=[settings.JWT_ALGORITHM],
    )
    assert claims["sub"] == "subject"
    assert claims["exp"] > claims["iat"]

    expected = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    assert abs((claims["exp"] - claims["iat"]) - expected.total_seconds()) < 2


def test_expired_token_is_rejected(monkeypatch: pytest.MonkeyPatch):
    """There is no revocation list, so expiry is the *only* thing that ends a
    leaked token's life — including one lifted out of a WebSocket URL in an
    access log."""
    monkeypatch.setattr(settings, "ACCESS_TOKEN_EXPIRE_MINUTES", -1)
    assert decode_access_token(create_access_token("subject")) is None


def test_token_signed_with_another_secret_is_rejected():
    forged = jwt.encode(
        {
            "sub": "subject",
            "exp": datetime.now(timezone.utc) + timedelta(hours=1),
        },
        "not-our-signing-key",
        algorithm=settings.JWT_ALGORITHM,
    )
    assert decode_access_token(forged) is None


def test_unsigned_token_is_rejected():
    """The ``alg: none`` forgery.

    ``decode_access_token`` pins ``algorithms=[HS256]``. Passing a list the token
    gets to choose from is how an attacker drops the signature entirely and
    picks their own subject. Hand-built rather than via ``jwt.encode`` so the
    test does not depend on PyJWT agreeing to produce one.
    """

    def segment(data: dict) -> str:
        raw = json.dumps(data, separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

    header = segment({"alg": "none", "typ": "JWT"})
    payload = segment({"sub": "administrator", "exp": 99999999999, "iat": 1})
    assert decode_access_token(f"{header}.{payload}.") is None


@pytest.mark.parametrize(
    "claims",
    [
        {},  # no sub at all
        {"sub": None},  # present but null
        {"sub": ""},  # present but empty
        {"sub": 12345},  # present but not a string
    ],
)
def test_token_without_a_usable_subject_is_rejected(claims: dict):
    """A well-signed token still has to name somebody.

    ``get_current_user`` would otherwise pass the value to ``uuid.UUID()`` or
    look up ``None`` as a primary key.
    """
    token = jwt.encode(
        {**claims, "exp": datetime.now(timezone.utc) + timedelta(hours=1)},
        settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM,
    )
    assert decode_access_token(token) is None


@pytest.mark.parametrize("garbage", ["", "not.a.token", "a.b.c", "....", "Bearer x"])
def test_malformed_tokens_are_rejected_without_raising(garbage: str):
    assert decode_access_token(garbage) is None
