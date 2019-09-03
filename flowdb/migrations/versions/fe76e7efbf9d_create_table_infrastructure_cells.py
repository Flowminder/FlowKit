"""Create table infrastructure.cells

Revision ID: fe76e7efbf9d
Revises: 444b907d635e
Create Date: 2019-09-02 15:46:35.167406

"""
from alembic import op
import sqlalchemy as sa
from geoalchemy2.types import Geometry


# revision identifiers, used by Alembic.
revision = "fe76e7efbf9d"
down_revision = "444b907d635e"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "cells",
        sa.Column("location_id", sa.Integer, primary_key=True),
        sa.Column("cell_id", sa.String(50), nullable=False),
        sa.Column("geom_point", Geometry("Point", srid=4326)),
        schema="infrastructure",
    )


def downgrade():
    op.drop_table("cells", schema="infrastructure")
