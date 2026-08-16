"""create transactions table

Revision ID: 003_create_transactions_table
Revises: 002_add_pending_status
Create Date: 2026-08-16 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

# revision identifiers, used by Alembic.
revision: str = '003_create_transactions_table'
down_revision: Union[str, None] = '002_add_pending_status'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'transactions',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('user_id', UUID(as_uuid=True), nullable=False),
        sa.Column('amount', sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    # Individual indexes
    op.create_index('ix_transactions_user_id', 'transactions', ['user_id'])
    op.create_index('ix_transactions_status', 'transactions', ['status'])
    # Composite index for the range query used by the report generator
    op.create_index('ix_transactions_user_date', 'transactions', ['user_id', 'created_at'])


def downgrade() -> None:
    op.drop_index('ix_transactions_user_date', table_name='transactions')
    op.drop_index('ix_transactions_status', table_name='transactions')
    op.drop_index('ix_transactions_user_id', table_name='transactions')
    op.drop_table('transactions')
