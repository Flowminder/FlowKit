"""Create infrastructure schema

Revision ID: 444b907d635e
Revises:
Create Date: 2019-09-02 15:03:50.013736

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "444b907d635e"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE SCHEMA infrastructure")


def downgrade():
    op.execute("DROP SCHEMA infrastructure")
