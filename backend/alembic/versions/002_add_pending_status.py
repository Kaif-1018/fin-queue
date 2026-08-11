"""add pending status to job_status enum

Revision ID: 002_add_pending_status
Revises: 001_create_jobs_table
Create Date: 2026-08-11 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = '002_add_pending_status'
down_revision: Union[str, None] = '001_create_jobs_table'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # PostgreSQL requires ALTER TYPE ... ADD VALUE to be committed before
    # the new value can be used (e.g. as a DEFAULT).  We must commit the
    # current Alembic transaction, run the ADD VALUE outside a transaction,
    # then open a new transaction for the remaining DDL.

    # 1. Commit the Alembic-managed transaction so ADD VALUE runs standalone.
    op.execute("COMMIT")

    # 2. Add 'PENDING' to the enum (runs outside a transaction).
    op.execute("ALTER TYPE job_status ADD VALUE IF NOT EXISTS 'PENDING' BEFORE 'QUEUED'")

    # 3. Start a new transaction for the remaining DDL.
    op.execute("BEGIN")

    # 4. Now it's safe to reference 'PENDING' as a default.
    op.execute("ALTER TABLE jobs ALTER COLUMN status SET DEFAULT 'PENDING'")


def downgrade() -> None:
    # Revert the server default back to 'QUEUED'
    op.execute("ALTER TABLE jobs ALTER COLUMN status SET DEFAULT 'QUEUED'")
    # Note: PostgreSQL does not support removing values from an enum type.
    # The 'PENDING' value will remain in the enum after downgrade.
    # To fully remove it you would need to recreate the type.
