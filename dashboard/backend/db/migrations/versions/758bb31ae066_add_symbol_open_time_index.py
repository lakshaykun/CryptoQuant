"""add symbol open_time index

Revision ID: 758bb31ae066
Revises: 1ea7c5eda3d2
Create Date: 2026-04-27 10:56:43.323321

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '758bb31ae066'
down_revision: Union[str, Sequence[str], None] = '1ea7c5eda3d2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.execute("CREATE INDEX idx_market_symbol_time ON market (symbol, open_time DESC);")

def downgrade():
    op.execute("DROP INDEX IF EXISTS idx_market_symbol_time;")
