"""add CANCELLED status and celery_task_id

Revision ID: 004_add_cancelled_status
Revises: 003_create_transactions_table
Create Date: 2026-08-23 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '004_add_cancelled_status'
down_revision: Union[str, None] = '003_create_transactions_table'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── 1. Add CANCELLED to the job_status enum ───────────────────
    # jobs.status is a native Postgres enum (see 001). PostgreSQL will not let
    # ALTER TYPE ... ADD VALUE run inside a transaction that then uses the new
    # value, so commit the Alembic transaction, add the value standalone, and
    # open a fresh transaction for the remaining DDL. Same dance as 002.
    op.execute("COMMIT")
    op.execute("ALTER TYPE job_status ADD VALUE IF NOT EXISTS 'CANCELLED'")
    op.execute("BEGIN")

    # ── 2. Track the Celery task so cancellation can revoke it ────
    # Nullable: existing rows have no task id, and a new row exists for a moment
    # before .delay() returns one. String(155) matches Celery's own task-id column.
    op.add_column(
        'jobs',
        sa.Column('celery_task_id', sa.String(length=155), nullable=True),
    )
    op.create_index(
        op.f('ix_jobs_celery_task_id'), 'jobs', ['celery_task_id'], unique=False
    )


def downgrade() -> None:
    op.drop_index(op.f('ix_jobs_celery_task_id'), table_name='jobs')
    op.drop_column('jobs', 'celery_task_id')

    # Note: PostgreSQL cannot remove a value from an enum type, so 'CANCELLED'
    # survives this downgrade. Fully removing it means recreating job_status and
    # rewriting every dependent column. Any row already CANCELLED would also
    # need remapping (presumably to FAILED) before the type could be narrowed.
