"""version_uid not primary key for stats anymore.

Revision ID: 9e0939e3bc6d
Revises: cd3f15ae79f8
Create Date: 2020-04-15 10:03:03.071747

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9e0939e3bc6d'
down_revision = 'cd3f15ae79f8'
branch_labels = None
depends_on = None


def upgrade():
    op.rename_table('stats', 'stats_old')


def downgrade():
    pass
