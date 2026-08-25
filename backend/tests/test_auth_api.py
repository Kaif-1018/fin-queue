"""
The auth endpoints, end to end against a real database.

Registration and login are the two routes reachable without a token, and
CLAUDE.md flags that they have no rate limiting — which makes their *other*
properties load-bearing: a uniform 401, a normalised email, and a response body
that never carries the password hash.
"""

from __future__ import annotations

import uuid

from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import User
from app.security import create_access_token
from tests.conftest import DEFAULT_PASSWORD, Account


# ── Registration ──────────────────────────────────────────────────


async def test_register_creates_an_account(client: AsyncClient, db: AsyncSession):
    response = await client.post(
        "/api/v1/auth/register",
        json={"email": "new@example.com", "password": DEFAULT_PASSWORD},
    )

    assert response.status_code == 201
    body = response.json()
    assert body["email"] == "new@example.com"
    assert body["is_active"] is True
    uuid.UUID(body["id"])  # parses

    stored = (
        await db.execute(select(User).where(User.email == "new@example.com"))
    ).scalar_one()
    assert stored.hashed_password != DEFAULT_PASSWORD


async def test_register_never_returns_the_password_hash(client: AsyncClient):
    """``UserResponse`` omits the field; this is the test that keeps it omitted.

    Adding ``hashed_password`` to the model — or returning the ORM object from a
    route without a ``response_model`` — would leak a crackable hash to whoever
    registered.
    """
    response = await client.post(
        "/api/v1/auth/register",
        json={"email": "leak@example.com", "password": DEFAULT_PASSWORD},
    )

    body = response.json()
    assert "hashed_password" not in body
    assert "password" not in body
    assert DEFAULT_PASSWORD not in response.text


async def test_duplicate_email_is_rejected(client: AsyncClient, alice: Account):
    response = await client.post(
        "/api/v1/auth/register",
        json={"email": alice.email, "password": DEFAULT_PASSWORD},
    )
    assert response.status_code == 409


async def test_email_uniqueness_ignores_case(client: AsyncClient, alice: Account):
    """``ALICE@example.com`` must not become a second account.

    Domains are case-insensitive and no mainstream provider treats the local part
    as case-sensitive, so without normalisation the unique index happily holds
    both rows and the user cannot tell which one their password belongs to.
    """
    response = await client.post(
        "/api/v1/auth/register",
        json={"email": alice.email.upper(), "password": DEFAULT_PASSWORD},
    )
    assert response.status_code == 409


async def test_registration_email_is_stored_lowercased(
    client: AsyncClient, db: AsyncSession
):
    await client.post(
        "/api/v1/auth/register",
        json={"email": "MiXeD@Example.COM", "password": DEFAULT_PASSWORD},
    )

    stored = (await db.execute(select(User))).scalars().all()
    assert [u.email for u in stored] == ["mixed@example.com"]


async def test_short_password_is_rejected(client: AsyncClient):
    response = await client.post(
        "/api/v1/auth/register",
        json={"email": "short@example.com", "password": "1234567"},
    )
    assert response.status_code == 422


async def test_password_beyond_bcrypts_limit_is_rejected(client: AsyncClient):
    """Refused by the schema, before ``hash_password`` can raise on it."""
    response = await client.post(
        "/api/v1/auth/register",
        json={"email": "long@example.com", "password": "a" * 73},
    )
    assert response.status_code == 422


async def test_malformed_email_is_rejected(client: AsyncClient):
    response = await client.post(
        "/api/v1/auth/register",
        json={"email": "not-an-email", "password": DEFAULT_PASSWORD},
    )
    assert response.status_code == 422


# ── Login ─────────────────────────────────────────────────────────


async def test_login_returns_a_bearer_token(client: AsyncClient, alice: Account):
    response = await client.post(
        "/api/v1/auth/login",
        data={"username": alice.email, "password": DEFAULT_PASSWORD},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["token_type"] == "bearer"
    assert body["access_token"]


async def test_login_accepts_a_differently_cased_email(
    client: AsyncClient, alice: Account
):
    response = await client.post(
        "/api/v1/auth/login",
        data={"username": alice.email.upper(), "password": DEFAULT_PASSWORD},
    )
    assert response.status_code == 200


async def test_login_ignores_surrounding_whitespace(
    client: AsyncClient, alice: Account
):
    response = await client.post(
        "/api/v1/auth/login",
        data={"username": f"  {alice.email}  ", "password": DEFAULT_PASSWORD},
    )
    assert response.status_code == 200


async def test_wrong_password_and_unknown_email_are_indistinguishable(
    client: AsyncClient, alice: Account
):
    """The anti-enumeration property.

    Both paths must produce the same status *and* the same body. If "no such
    user" said so, an unauthenticated caller could walk a list of addresses and
    learn which ones hold accounts — and ``/auth/login`` has no rate limiting to
    slow that down.
    """
    wrong_password = await client.post(
        "/api/v1/auth/login",
        data={"username": alice.email, "password": "not-the-password"},
    )
    unknown_email = await client.post(
        "/api/v1/auth/login",
        data={"username": "nobody@example.com", "password": DEFAULT_PASSWORD},
    )

    assert wrong_password.status_code == unknown_email.status_code == 401
    assert wrong_password.json() == unknown_email.json()
    assert wrong_password.headers["www-authenticate"] == "Bearer"


async def test_deactivated_account_cannot_log_in(
    client: AsyncClient, db: AsyncSession, alice: Account
):
    """403, not 401: "contact support" is different advice from "try again"."""
    user = await db.get(User, alice.id)
    assert user is not None
    user.is_active = False
    await db.commit()

    response = await client.post(
        "/api/v1/auth/login",
        data={"username": alice.email, "password": DEFAULT_PASSWORD},
    )
    assert response.status_code == 403


# ── Identity ──────────────────────────────────────────────────────


async def test_me_returns_the_token_holder(client: AsyncClient, alice: Account):
    response = await client.get("/api/v1/auth/me", headers=alice.headers)

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == str(alice.id)
    assert body["email"] == alice.email


async def test_me_requires_a_token(client: AsyncClient):
    response = await client.get("/api/v1/auth/me")
    assert response.status_code == 401


async def test_me_rejects_a_garbage_token(client: AsyncClient):
    response = await client.get(
        "/api/v1/auth/me", headers={"Authorization": "Bearer not-a-jwt"}
    )
    assert response.status_code == 401


async def test_token_for_a_deleted_user_is_rejected(client: AsyncClient):
    """A correctly signed token naming a user who no longer exists.

    There is no revocation list, so this is the only thing standing between a
    deleted account's outstanding token and a valid session.
    """
    orphan = create_access_token(str(uuid.uuid4()))
    response = await client.get(
        "/api/v1/auth/me", headers={"Authorization": f"Bearer {orphan}"}
    )
    assert response.status_code == 401


async def test_token_whose_subject_is_not_a_uuid_is_rejected(client: AsyncClient):
    response = await client.get(
        "/api/v1/auth/me",
        headers={"Authorization": f"Bearer {create_access_token('not-a-uuid')}"},
    )
    assert response.status_code == 401


async def test_deactivated_user_with_a_live_token_is_refused(
    client: AsyncClient, db: AsyncSession, alice: Account
):
    """Deactivation must take effect on the *next request*, not at token expiry.

    Checking ``is_active`` only at login would leave a disabled account working
    for up to ACCESS_TOKEN_EXPIRE_MINUTES.
    """
    user = await db.get(User, alice.id)
    assert user is not None
    user.is_active = False
    await db.commit()

    response = await client.get("/api/v1/auth/me", headers=alice.headers)
    assert response.status_code == 403


# ── Swagger's Authorize button ────────────────────────────────────


async def test_login_route_matches_the_documented_token_url(client: AsyncClient):
    """``OAuth2PasswordBearer(tokenUrl=...)`` in deps.py must keep pointing at
    the real login route, or /docs' Authorize button silently stops working —
    a failure with no error message anywhere.
    """
    schema = (await client.get("/openapi.json")).json()
    flow = schema["components"]["securitySchemes"]["OAuth2PasswordBearer"]["flows"]
    token_url = flow["password"]["tokenUrl"]

    assert token_url.lstrip("/") == "api/v1/auth/login"
    assert token_url.lstrip("/") in {p.lstrip("/") for p in schema["paths"]} or (
        f"/{token_url.lstrip('/')}" in schema["paths"]
    )
