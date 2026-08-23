"""
Database seeder — create demo users and populate `transactions` with mock records.

Usage (inside the backend container):
    python seed_db.py

Every transaction is attributed to a real row in `users`, so a seeded account can
log in and immediately generate a non-empty report. Seeding against random UUIDs
(as this script used to) leaves every logged-in user owning nothing, and every
report comes back with a header row and no data.

Uses Faker for realistic data and inserts in batches of 5,000 for performance.
"""

import os
import random
import uuid
from datetime import timezone

import bcrypt
from faker import Faker
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# ── Configuration ─────────────────────────────────────────────────
# Re-use the same env-var approach as the rest of the app.
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://jobplatform:jobplatform_secret@localhost:5432/jobplatform_db",
)
# Convert async URL → sync (psycopg2)
SYNC_DATABASE_URL = DATABASE_URL.replace("postgresql+asyncpg", "postgresql+psycopg2")

engine = create_engine(SYNC_DATABASE_URL, pool_pre_ping=True)
Session = sessionmaker(bind=engine)

fake = Faker()
Faker.seed(42)
random.seed(42)

# ── Seed parameters ──────────────────────────────────────────────
TOTAL_ROWS = 50_000
BATCH_SIZE = 5_000
STATUSES = ["completed", "pending", "refunded", "failed"]

# Demo accounts. The password is shared and printed on purpose — this is a
# development seeder, and a credential you have to go read the source to find is
# a credential that wastes your time. Never run this against anything real.
DEMO_PASSWORD = "demo-password-123"
DEMO_EMAILS = [
    "analyst@example.com",
    "auditor@example.com",
    "viewer@example.com",
]


def upsert_users(session) -> list[uuid.UUID]:
    """Ensure each demo account exists; return their ids in DEMO_EMAILS order.

    Idempotent on email so re-running the seeder does not collide with the unique
    index. Existing accounts keep their current password hash — re-seeding
    transactions should not silently reset a password you may have changed.
    """
    hashed = bcrypt.hashpw(
        DEMO_PASSWORD.encode("utf-8"), bcrypt.gensalt()
    ).decode("utf-8")

    user_ids: list[uuid.UUID] = []
    for email in DEMO_EMAILS:
        existing = session.execute(
            text("SELECT id FROM users WHERE email = :email"),
            {"email": email},
        ).scalar()

        if existing:
            user_ids.append(existing)
            print(f"    ↻  {email} already exists")
            continue

        new_id = session.execute(
            text(
                "INSERT INTO users (email, hashed_password) "
                "VALUES (:email, :hashed) RETURNING id"
            ),
            {"email": email, "hashed": hashed},
        ).scalar()
        session.commit()
        user_ids.append(new_id)
        print(f"    ✅  created {email}")

    return user_ids


def seed() -> None:
    """Create the demo accounts, then insert 50,000 mock transaction rows."""
    with Session() as session:
        print("👤  Ensuring demo accounts …")
        user_ids = upsert_users(session)

        print(f"\n🌱  Seeding {TOTAL_ROWS:,} transactions …")

        # Check if table already has data
        count = session.execute(text("SELECT COUNT(*) FROM transactions")).scalar()
        if count and count > 0:
            print(f"⚠️   Table already has {count:,} rows. Truncating …")
            session.execute(text("TRUNCATE TABLE transactions RESTART IDENTITY"))
            session.commit()

        rows_inserted = 0
        for _ in range(0, TOTAL_ROWS, BATCH_SIZE):
            batch = []
            for _ in range(BATCH_SIZE):
                batch.append({
                    "user_id": random.choice(user_ids),
                    "amount": round(random.uniform(1.00, 9999.99), 2),
                    "status": random.choice(STATUSES),
                    "created_at": fake.date_time_between(
                        start_date="-2y",
                        end_date="now",
                        tzinfo=timezone.utc,
                    ),
                })

            session.execute(
                text(
                    "INSERT INTO transactions (user_id, amount, status, created_at) "
                    "VALUES (:user_id, :amount, :status, :created_at)"
                ),
                batch,
            )
            session.commit()
            rows_inserted += len(batch)
            print(f"    ✅  {rows_inserted:>6,} / {TOTAL_ROWS:,} rows inserted")

    print(f"\n🎉  Done! {TOTAL_ROWS:,} transactions seeded across {len(user_ids)} users.")
    print(f"\n📋  Log in at http://localhost:3000 — password: {DEMO_PASSWORD}")
    for email, uid in zip(DEMO_EMAILS, user_ids):
        print(f"    {email:<24} {uid}")


if __name__ == "__main__":
    seed()
