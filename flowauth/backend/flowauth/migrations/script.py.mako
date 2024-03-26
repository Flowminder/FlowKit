"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""
from alembic import op
import sqlalchemy as sa
import structlog
${imports if imports else ""}

# revision identifiers, used by Alembic.
revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}

logger = structlog.get_logger("flowauth.migration")

def upgrade():
    logger.info("Running upgrade.", migration_script=__file__, revision=revision, down_revision=down_revision, branch_labels=branch_labels,depends_on=depends_on)
    ${upgrades if upgrades else "pass"}


def downgrade():
    logger.info("Running downgrade.", migration_script=__file__, revision=revision, down_revision=down_revision, branch_labels=branch_labels,depends_on=depends_on)
    ${downgrades if downgrades else "pass"}
