"""Rename snap log -> function log

Revision ID: 20210418
Revises: 3cbdd93d0687
Create Date: 2021-04-18 13:35:05.061458

"""
import snapflow
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "20210418"
down_revision = "3cbdd93d0687"
branch_labels = None
depends_on = None


def upgrade():
    op.rename_table("_snapflow_snap_log", "_snapflow_data_function_log")


def downgrade():
    op.rename_table("_snapflow_data_function_log", "_snapflow_snap_log")
