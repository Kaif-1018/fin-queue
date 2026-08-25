"""
Shared fixtures for the test suite.

**The suite needs a real Postgres.** The code depends on JSONB, a native enum,
``gen_random_uuid()``, and ``SELECT ... FOR UPDATE`` — SQLite has none of them,
and the row lock is the whole subject of ``test_claim_race.py``. A throwaway
database (``<dev-db>_test``) is created once per session and migrated with
Alembic rather than ``Base.metadata.create_all``: Alembic owns the schema
(CLAUDE.md), so running the real migrations here also proves the chain applies
cleanly to an empty database.

**Isolation is by TRUNCATE, not by an open transaction.** The usual trick of
wrapping each test in a transaction that is rolled back afterwards cannot work
here: ``claim_sync`` and both Celery tasks open their *own* psycopg2 connections,
and one connection cannot see another's uncommitted rows. Everything below
commits for real, and each test starts from empty tables.

Nothing here talks to a broker. ``apply_async`` and ``control.revoke`` are
stubbed for every test by the autouse ``dispatched`` fixture, so endpoint tests
assert on the dispatch that *would* have happened. Task tests call the task
function directly instead.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, NamedTuple

import bcrypt
import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from sqlalchemy import NullPool, create_engine, make_url, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import settings

BACKEND_DIR = Path(__file__).resolve().parents[1]

# ── Test database URLs ────────────────────────────────────────────
# Derived from DATABASE_URL so the suite follows the environment it is run in:
# inside the backend container that is host `postgres`, in CI it is `localhost`.
# render_as_string(hide_password=False) is required — plain str(URL) masks the
# password as "***" and every connection then fails to authenticate.

_DEV_URL = make_url(settings.DATABASE_URL)
TEST_DB_NAME = f"{_DEV_URL.database or 'jobplatform_db'}_test"

TEST_URL_ASYNC = _DEV_URL.set(database=TEST_DB_NAME).render_as_string(
    hide_password=False
)
TEST_URL_SYNC = _DEV_URL.set(
    database=TEST_DB_NAME, drivername="postgresql+psycopg2"
).render_as_string(hide_password=False)
# "postgres" always exists and is never the database being dropped.
ADMIN_URL = _DEV_URL.set(
    database="postgres", drivername="postgresql+psycopg2"
).render_as_string(hide_password=False)

TABLES = ("users", "jobs", "transactions")


# ── Hashing cost ──────────────────────────────────────────────────
# Captured before the fixture below rebinds it, so a test can still assert on
# what production actually uses.
_REAL_GENSALT = bcrypt.gensalt

# bcrypt's whole purpose is to be slow, and at the default cost of 12 a single
# hash takes roughly a quarter of a second. Most tests here need an account, and
# every account costs one hash plus one verify — which had the suite spending the
# large majority of its runtime proving that bcrypt is expensive.
MIN_PRODUCTION_COST = 12
TEST_COST = 4


@pytest.fixture(scope="session", autouse=True)
def _cheap_hashing() -> Any:
    """Run the suite at bcrypt's minimum cost factor.

    The cost is not what any of these tests verify — hashing, verification,
    salting, and the byte limit all behave identically at cost 4, and
    ``test_production_cost_factor_is_not_weakened`` guards the real default
    separately.
    """
    patch = pytest.MonkeyPatch()
    patch.setattr(
        bcrypt,
        "gensalt",
        lambda rounds=TEST_COST, prefix=b"2b": _REAL_GENSALT(
            rounds=TEST_COST, prefix=prefix
        ),
    )
    yield
    patch.undo()


@pytest.fixture(scope="session")
def real_gensalt() -> Any:
    """bcrypt's unpatched ``gensalt``, for asserting the production cost."""
    return _REAL_GENSALT


# ── Session setup: build and migrate the throwaway database ───────


@pytest.fixture(scope="session", autouse=True)
def _test_database() -> Any:
    """Create ``<dev-db>_test``, migrate it, and point the app at it.

    Synchronous on purpose. Alembic's ``env.py`` calls ``asyncio.run()``
    internally, which raises if a loop is already running — so the migration has
    to happen outside any event loop, which rules out an async fixture.
    """
    admin = create_engine(ADMIN_URL, isolation_level="AUTOCOMMIT")
    try:
        with admin.connect() as conn:
            # WITH (FORCE) disconnects anything still attached from a previous
            # aborted run; without it a leaked connection fails the DROP.
            conn.execute(text(f'DROP DATABASE IF EXISTS "{TEST_DB_NAME}" WITH (FORCE)'))
            conn.execute(text(f'CREATE DATABASE "{TEST_DB_NAME}"'))
    finally:
        admin.dispose()

    # Rebind before migrating: alembic/env.py reads settings.DATABASE_URL and
    # overwrites whatever alembic.ini says, so setting sqlalchemy.url on the
    # Config below would be discarded.
    settings.DATABASE_URL = TEST_URL_ASYNC

    # The sync engine in tasks.py derives its URL from the same setting and
    # caches it on first use. This is exactly what reset_sync_engine() is for.
    from app.tasks import reset_sync_engine

    reset_sync_engine()

    config = Config(str(BACKEND_DIR / "alembic.ini"))
    config.set_main_option("script_location", str(BACKEND_DIR / "alembic"))
    command.upgrade(config, "head")

    yield

    reset_sync_engine()
    admin = create_engine(ADMIN_URL, isolation_level="AUTOCOMMIT")
    try:
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{TEST_DB_NAME}" WITH (FORCE)'))
    finally:
        admin.dispose()


@pytest.fixture(scope="session")
def sync_engine(_test_database: None):
    """A psycopg2 engine on the test database, for truncation and for the
    thread-based claim tests (which need genuinely concurrent connections)."""
    engine = create_engine(TEST_URL_SYNC, poolclass=NullPool)
    yield engine
    engine.dispose()


@pytest.fixture(autouse=True)
def _clean_tables(sync_engine) -> Any:
    """Empty every table *before* each test.

    Before rather than after, so a failing test leaves its rows in place to be
    inspected. CASCADE covers the jobs → users foreign key.
    """
    with sync_engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {', '.join(TABLES)} RESTART IDENTITY CASCADE"))
    yield


# ── Async engine and sessions ─────────────────────────────────────
# Function-scoped, with NullPool: a pooled connection outliving its test would
# hold the database open and make the session-teardown DROP fail.


@pytest_asyncio.fixture
async def engine(_test_database: None):
    eng = create_async_engine(TEST_URL_ASYNC, poolclass=NullPool)
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def session_factory(engine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


@pytest_asyncio.fixture
async def db(session_factory) -> Any:
    """An AsyncSession for the test body's own reads and writes.

    Separate from the sessions the app uses, so a test cannot accidentally
    observe a row through the same identity map that created it.
    """
    async with session_factory() as session:
        yield session


# ── Celery stubs ──────────────────────────────────────────────────


class Dispatch(NamedTuple):
    """One recorded ``apply_async`` call."""

    task_name: str
    args: list[Any]
    task_id: str | None


@pytest.fixture(autouse=True)
def dispatched(monkeypatch: pytest.MonkeyPatch) -> list[Dispatch]:
    """Record task dispatches instead of sending them to the broker.

    Autouse: an endpoint test that reached a real broker would either hang or
    hand the job to a live worker mid-test. Tests that want to assert an enqueue
    happened take this fixture and read the list.
    """
    from app.celery_app import celery_app
    from app.tasks import TASK_REGISTRY

    calls: list[Dispatch] = []

    def record_factory(name: str):
        def record(
            *_args: Any,
            args: list[Any] | None = None,
            task_id: str | None = None,
            **_kw: Any,
        ):
            calls.append(
                Dispatch(task_name=name, args=list(args or []), task_id=task_id)
            )

            class _Result:
                id = task_id

            return _Result()

        return record

    for name, task in TASK_REGISTRY.items():
        monkeypatch.setattr(task, "apply_async", record_factory(name))

    # cancel_job revokes the task after a successful cancel. Harmless in
    # production, but a broadcast to a broker nobody is listening on adds a
    # timeout to every cancel test.
    monkeypatch.setattr(
        celery_app.control, "revoke", lambda *a, **k: None, raising=False
    )

    return calls


# ── HTTP client ───────────────────────────────────────────────────


@pytest_asyncio.fixture
async def client(session_factory) -> Any:
    """An httpx client wired to the ASGI app in-process.

    ``get_db`` is overridden rather than reconfigured because ``app.database``
    built its engine at import time, pointing at the development database. The
    override mirrors the real dependency's commit-on-exit behaviour.

    Lifespan never runs under ASGITransport, which is what we want: the startup
    hook opens a connection on that development engine.
    """
    from app.database import get_db
    from app.main import app

    async def _get_db() -> Any:
        async with session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    app.dependency_overrides[get_db] = _get_db
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            yield c
    finally:
        app.dependency_overrides.clear()


# ── Accounts ──────────────────────────────────────────────────────

DEFAULT_PASSWORD = "test-password-123"


class Account(NamedTuple):
    id: uuid.UUID
    email: str
    token: str

    @property
    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}


@pytest_asyncio.fixture
async def register(client: AsyncClient) -> Any:
    """Factory: register an account through the API and log it in.

    Goes through the real endpoints rather than inserting a row, so every test
    that needs a user also exercises the hashing and token paths.
    """

    async def _register(email: str, password: str = DEFAULT_PASSWORD) -> Account:
        created = await client.post(
            "/api/v1/auth/register", json={"email": email, "password": password}
        )
        assert created.status_code == 201, created.text

        logged_in = await client.post(
            "/api/v1/auth/login", data={"username": email, "password": password}
        )
        assert logged_in.status_code == 200, logged_in.text

        return Account(
            id=uuid.UUID(created.json()["id"]),
            email=email,
            token=logged_in.json()["access_token"],
        )

    return _register


@pytest_asyncio.fixture
async def alice(register) -> Account:
    return await register("alice@example.com")


@pytest_asyncio.fixture
async def bob(register) -> Account:
    """A second account. Exists so every ownership test has an outsider."""
    return await register("bob@example.com")


# ── Row builders ──────────────────────────────────────────────────


@pytest_asyncio.fixture
async def make_job(db: AsyncSession) -> Any:
    """Factory: insert a Job row directly, bypassing the API.

    Needed for states the API will not produce on demand — a COMPLETED job with
    a result, or the NULL-owner rows that predate authentication.
    """
    from app.models import Job, JobStatus

    async def _make_job(
        *,
        owner: Account | uuid.UUID | None = None,
        job_type: str = "bulk_csv_report",
        status: JobStatus = JobStatus.PENDING,
        payload: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
        celery_task_id: str | None = None,
    ) -> Job:
        owner_id = owner.id if isinstance(owner, Account) else owner
        job = Job(
            job_type=job_type,
            status=status,
            payload=payload,
            result=result,
            user_id=owner_id,
            celery_task_id=celery_task_id or str(uuid.uuid4()),
        )
        db.add(job)
        await db.commit()
        await db.refresh(job)
        return job

    return _make_job


@pytest_asyncio.fixture
async def make_transactions(db: AsyncSession) -> Any:
    """Factory: insert Transaction rows for a given owner."""
    from datetime import datetime

    from app.models import Transaction

    async def _make(
        owner: Account | uuid.UUID,
        amounts: list[str],
        *,
        status: str = "completed",
        created_at: datetime | None = None,
    ) -> list[Transaction]:
        from decimal import Decimal

        owner_id = owner.id if isinstance(owner, Account) else owner
        rows = [
            Transaction(
                user_id=owner_id,
                amount=Decimal(a),
                status=status,
                **({"created_at": created_at} if created_at else {}),
            )
            for a in amounts
        ]
        db.add_all(rows)
        await db.commit()
        return rows

    return _make


# ── Synchronous row builders ──────────────────────────────────────
# The Celery tasks and claim_sync are synchronous and open their own psycopg2
# connections, so the tests covering them arrange their rows the same way rather
# than reaching across from an async session.


@pytest.fixture
def sync_db(sync_engine) -> Any:
    """A psycopg2 session for arranging rows, mirroring what a task uses."""
    from sqlalchemy.orm import Session

    with Session(sync_engine) as session:
        yield session


@pytest.fixture
def make_sync_user(sync_db) -> Any:
    from app.models import User
    from app.security import hash_password

    def _make(email: str | None = None) -> Any:
        user = User(
            email=email or f"sync-{uuid.uuid4().hex[:8]}@example.com",
            hashed_password=hash_password("irrelevant-here"),
        )
        sync_db.add(user)
        sync_db.commit()
        sync_db.refresh(user)
        return user

    return _make


@pytest.fixture
def sync_user(make_sync_user) -> Any:
    """One owner, for the common case."""
    return make_sync_user()


@pytest.fixture
def make_sync_job(sync_db, sync_user) -> Any:
    """Factory: insert a Job row over a psycopg2 connection.

    ``owner`` defaults to ``sync_user``; pass ``owner=None`` explicitly for the
    unowned rows that predate authentication.
    """
    from app.models import Job, JobStatus

    _unset = object()

    def _make(
        *,
        status: JobStatus = JobStatus.PENDING,
        owner: Any = _unset,
        job_type: str = "csv_ingestion",
        payload: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
    ) -> Any:
        resolved = sync_user if owner is _unset else owner
        owner_id = getattr(resolved, "id", resolved)

        job = Job(
            job_type=job_type,
            status=status,
            payload=payload,
            result=result,
            user_id=owner_id,
            celery_task_id=str(uuid.uuid4()),
        )
        sync_db.add(job)
        sync_db.commit()
        sync_db.refresh(job)
        return job

    return _make


@pytest.fixture
def make_sync_transactions(sync_db) -> Any:
    """Factory: insert Transaction rows over a psycopg2 connection."""
    from datetime import datetime
    from decimal import Decimal

    from app.models import Transaction

    def _make(
        owner: Any,
        amounts: list[str],
        *,
        status: str = "completed",
        created_at: datetime | None = None,
    ) -> list[Any]:
        owner_id = getattr(owner, "id", owner)
        rows = [
            Transaction(
                user_id=owner_id,
                amount=Decimal(a),
                status=status,
                **({"created_at": created_at} if created_at else {}),
            )
            for a in amounts
        ]
        sync_db.add_all(rows)
        sync_db.commit()
        return rows

    return _make
