"""create sentiment tables

Revision ID: 41af1b4f7e44
Revises: 0caa4195dffd
Create Date: 2026-04-28 15:08:26.465026

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '41af1b4f7e44'
down_revision: Union[str, Sequence[str], None] = '0caa4195dffd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.execute("""
        CREATE TABLE sentiment_gold (
            window_start    TIMESTAMPTZ     NOT NULL,
            symbol          TEXT            NOT NULL,
            sentiment_index DOUBLE PRECISION,
            avg_confidence  DOUBLE PRECISION,
            message_count   INTEGER,
            window_date     DATE            NOT NULL,
            PRIMARY KEY (window_start, symbol)
        )
    """)
    op.execute("SELECT create_hypertable('sentiment_gold', by_range('window_start'), if_not_exists => TRUE)")
    op.execute("CREATE INDEX idx_sentiment_gold_symbol_time ON sentiment_gold (symbol, window_start DESC)")

    op.execute("""
        CREATE TABLE sentiment_silver (
            event_time  TIMESTAMPTZ NOT NULL,
            symbol      TEXT        NOT NULL,
            source      TEXT        NOT NULL,
            engagement  INTEGER,
            PRIMARY KEY (event_time, symbol, source)
        )
    """)
    op.execute("SELECT create_hypertable('sentiment_silver', by_range('event_time'), if_not_exists => TRUE)")
    op.execute("CREATE INDEX idx_sentiment_silver_symbol_time ON sentiment_silver (symbol, event_time DESC)")

def downgrade():
    op.execute("DROP TABLE IF EXISTS sentiment_gold")
    op.execute("DROP TABLE IF EXISTS sentiment_silver")