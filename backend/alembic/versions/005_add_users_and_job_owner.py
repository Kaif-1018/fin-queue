"""add users table and jobs.user_id owner column

Revision ID: 005_add_users_and_job_owner
Revises: 004_add_cancelled_status
Create Date: 2026-08-24 00:00:00.000000

Plain DDL — no enum values are added here, so none of the COMMIT / ADD VALUE /
BEGIN dance from 002_add_pending_status.py is needed.

``jobs.user_id`` is nullable on purpose. Jobs created before auth existed have no
owner, and there is no defensible value to invent for them. Because every query
filters on an authenticated user's id, a NULL owner matches nobody and those
rows become unreachable rather than public.

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

# revision identifiers, used by Alembic.
revision: str = '005_add_users_and_job_owner'
down_revision: Union[str, None] = '004_add_cancelled_status'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'users',
        sa.Column(
            'id',
            UUID(as_uuid=True),
            server_default=sa.text('gen_random_uuid()'),
            nullable=False,
        ),
        # 320 = RFC 5321 maximum: 64-char local part + "@" + 255-char domain.
        sa.Column('email', sa.String(length=320), nullable=False),
        sa.Column('hashed_password', sa.String(length=128), nullable=False),
        sa.Column(
            'is_active',
            sa.Boolean(),
            server_default=sa.text('true'),
            nullable=False,
        ),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.Column(
            'updated_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    # Unique, because login resolves an account by email. The index this creates
    # also serves that lookup.
    op.create_index('ix_users_email', 'users', ['email'], unique=True)

    # ── Job ownership ─────────────────────────────────────────────
    op.add_column(
        'jobs',
        sa.Column('user_id', UUID(as_uuid=True), nullable=True),
    )
    op.create_foreign_key(
        'fk_jobs_user_id_users',
        'jobs',
        'users',
        ['user_id'],
        ['id'],
        ondelete='CASCADE',
    )
    # Composite: every listing filters by owner and orders by created_at DESC.
    op.create_index('ix_jobs_user_created', 'jobs', ['user_id', 'created_at'])


def downgrade() -> None:
    op.drop_index('ix_jobs_user_created', table_name='jobs')
    op.drop_constraint('fk_jobs_user_id_users', 'jobs', type_='foreignkey')
    op.drop_column('jobs', 'user_id')

    op.drop_index('ix_users_email', table_name='users')
    op.drop_table('users')
