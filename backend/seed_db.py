"""
Database seeder — populate the `transactions` table with 50,000 mock records.

Usage (inside the backend container):
    python seed_db.py

Uses Faker for realistic data and inserts in batches of 5,000 for performance.
"""

import random
import uuid
from datetime import datetime, timezone

from faker import Faker
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# ── Configuration ─────────────────────────────────────────────────
# Re-use the same env-var approach as the rest of the app.
import os

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

# Use a pool of 10 fixed user IDs so reports have meaningful groupings
USER_IDS = [uuid.uuid4() for _ in range(10)]


def seed() -> None:
    """Insert 50,000 mock transaction rows."""
    print(f"🌱  Seeding {TOTAL_ROWS:,} transactions …")
    print(f"    User IDs pool: {[str(u)[:8] + '…' for u in USER_IDS]}")

    with Session() as session:
        # Check if table already has data
        count = session.execute(text("SELECT COUNT(*) FROM transactions")).scalar()
        if count and count > 0:
            print(f"⚠️   Table already has {count:,} rows. Truncating …")
            session.execute(text("TRUNCATE TABLE transactions RESTART IDENTITY"))
            session.commit()

        rows_inserted = 0
        for batch_start in range(0, TOTAL_ROWS, BATCH_SIZE):
            batch = []
            for _ in range(BATCH_SIZE):
                batch.append({
                    "user_id": random.choice(USER_IDS),
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

    print(f"\n🎉  Done! {TOTAL_ROWS:,} transactions seeded.")
    print("\n📋  User IDs for testing:")
    for i, uid in enumerate(USER_IDS):
        print(f"    [{i}] {uid}")


if __name__ == "__main__":
    seed()
