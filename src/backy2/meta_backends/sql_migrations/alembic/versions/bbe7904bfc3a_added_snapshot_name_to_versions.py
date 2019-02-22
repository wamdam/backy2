"""Added snapshot_name to versions

Revision ID: bbe7904bfc3a
Revises: 
Create Date: 2017-04-11 18:11:04.786199

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'bbe7904bfc3a'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('versions', sa.Column('snapshot_name', sa.String(), server_default='', nullable=False))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('versions', 'snapshot_name')
    # ### end Alembic commands ###