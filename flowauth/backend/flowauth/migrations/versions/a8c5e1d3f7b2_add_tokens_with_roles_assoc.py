# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""Add tokens_with_roles association table

Revision ID: a8c5e1d3f7b2
Revises: 976c731ff30f
Create Date: 2026-04-28 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa
import structlog


# revision identifiers, used by Alembic.
revision = "a8c5e1d3f7b2"
down_revision = "976c731ff30f"
branch_labels = None
depends_on = None

logger = structlog.get_logger("flowauth.migration")


def upgrade():
    logger.info(
        "Running upgrade.",
        migration_script=__file__,
        revision=revision,
        down_revision=down_revision,
        branch_labels=branch_labels,
        depends_on=depends_on,
    )
    op.create_table(
        "tokens_with_roles",
        sa.Column("token_id", sa.Integer(), nullable=False),
        sa.Column("role_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["role_id"],
            ["role.id"],
        ),
        sa.ForeignKeyConstraint(
            ["token_id"],
            ["token_history.id"],
        ),
        sa.PrimaryKeyConstraint("token_id", "role_id"),
    )


def downgrade():
    logger.info(
        "Running downgrade.",
        migration_script=__file__,
        revision=revision,
        down_revision=down_revision,
        branch_labels=branch_labels,
        depends_on=depends_on,
    )
    op.drop_table("tokens_with_roles")
