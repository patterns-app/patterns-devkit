"""Alias name

Revision ID: a94cb79f72fc
Revises: 20210418
Create Date: 2021-04-21 17:48:59.792577

"""
from alembic import op
import sqlalchemy as sa
import snapflow
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "a94cb79f72fc"
down_revision = "20210418"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("_snapflow_alias") as batch_op:
        batch_op.alter_column(
            "alias",
            existing_type=sa.String(length=128),
            nullable=False,
            new_column_name="name",
        )


def downgrade():
    pass
