# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""Make latest_token_expiry nullable on server and role

A NULL ``latest_token_expiry`` means the row imposes no absolute expiry
cap on tokens; only ``longest_token_life_minutes`` then bounds the token
lifetime. Existing non-null rows are left as-is so behaviour is unchanged
until an operator nulls them out explicitly.

Revision ID: c1d4e7b9a2f3
Revises: a8c5e1d3f7b2
Create Date: 2026-04-28 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa
import structlog


# revision identifiers, used by Alembic.
revision = "c1d4e7b9a2f3"
down_revision = "a8c5e1d3f7b2"
branch_labels = None
depends_on = None

logger = structlog.get_logger("flowauth.migration")


def upgrade():
    """Make ``latest_token_expiry`` nullable on ``server`` and ``role``. A
    NULL value means the row imposes no absolute expiry cap on tokens; only
    ``longest_token_life_minutes`` then bounds the token lifetime.
    Existing rows keep their non-null values, so behaviour is unchanged
    until an operator nulls the column out explicitly."""
    logger.info(
        "Running upgrade.",
        migration_script=__file__,
        revision=revision,
        down_revision=down_revision,
        branch_labels=branch_labels,
        depends_on=depends_on,
    )
    with op.batch_alter_table("server", schema=None) as batch_op:
        batch_op.alter_column(
            "latest_token_expiry", existing_type=sa.DateTime(), nullable=True
        )
    with op.batch_alter_table("role", schema=None) as batch_op:
        batch_op.alter_column(
            "latest_token_expiry", existing_type=sa.DateTime(), nullable=True
        )


def downgrade():
    """Restore the NOT NULL constraint on ``latest_token_expiry``. Will fail
    if any rows currently have NULL in those columns; operators must
    populate them with a sentinel datetime first."""
    logger.info(
        "Running downgrade.",
        migration_script=__file__,
        revision=revision,
        down_revision=down_revision,
        branch_labels=branch_labels,
        depends_on=depends_on,
    )
    # NOTE: downgrade will fail if any rows have NULL in these columns.
    # Operators must populate them with a sentinel datetime first.
    with op.batch_alter_table("role", schema=None) as batch_op:
        batch_op.alter_column(
            "latest_token_expiry", existing_type=sa.DateTime(), nullable=False
        )
    with op.batch_alter_table("server", schema=None) as batch_op:
        batch_op.alter_column(
            "latest_token_expiry", existing_type=sa.DateTime(), nullable=False
        )
