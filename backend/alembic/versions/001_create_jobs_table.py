"""create jobs table

Revision ID: 001_create_jobs_table
Revises: 
Create Date: 2026-08-09 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '001_create_jobs_table'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create enum type only if it doesn't already exist
    job_status_enum = postgresql.ENUM('QUEUED', 'PROCESSING', 'COMPLETED', 'FAILED', name='job_status', create_type=False)
    job_status_enum.create(op.get_bind(), checkfirst=True)

    op.create_table(
        'jobs',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('job_type', sa.String(length=128), nullable=False),
        sa.Column('status', postgresql.ENUM('QUEUED', 'PROCESSING', 'COMPLETED', 'FAILED', name='job_status', create_type=False), server_default=sa.text("'QUEUED'"), nullable=False),
        sa.Column('payload', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('result', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_jobs_job_type'), 'jobs', ['job_type'], unique=False)
    op.create_index(op.f('ix_jobs_status'), 'jobs', ['status'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_jobs_status'), table_name='jobs')
    op.drop_index(op.f('ix_jobs_job_type'), table_name='jobs')
    op.drop_table('jobs')

    job_status_enum = postgresql.ENUM('QUEUED', 'PROCESSING', 'COMPLETED', 'FAILED', name='job_status')
    job_status_enum.drop(op.get_bind(), checkfirst=True)
