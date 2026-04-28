import asyncpg
import pandas as pd
from data_platform.utils_global.logger import get_logger

logger = get_logger(__name__)

GOLD_INSERT_SQL = """
    INSERT INTO sentiment_gold (
        window_start, symbol, sentiment_index, avg_confidence, message_count, window_date
    ) VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (window_start, symbol) DO NOTHING
"""

SILVER_INSERT_SQL = """
    INSERT INTO sentiment_silver (
        event_time, symbol, source, engagement
    ) VALUES ($1, $2, $3, $4)
    ON CONFLICT (event_time, symbol, source) DO NOTHING
"""


async def upsert_gold_rows(df: pd.DataFrame, conn: asyncpg.Connection) -> int:
    if df.empty:
        logger.info("[sentiment/writer] No gold rows to write")
        return 0
    try:
        records = [tuple(row) for row in df.itertuples(index=False, name=None)]
        await conn.executemany(GOLD_INSERT_SQL, records)
        logger.info(f"[sentiment/writer] Upserted {len(records)} gold rows")
        return len(records)
    except Exception as e:
        logger.error(f"[sentiment/writer] Gold upsert failed → {e}")
        raise


async def upsert_silver_rows(df: pd.DataFrame, conn: asyncpg.Connection) -> int:
    if df.empty:
        logger.info("[sentiment/writer] No silver rows to write")
        return 0
    try:
        records = [tuple(row) for row in df.itertuples(index=False, name=None)]
        await conn.executemany(SILVER_INSERT_SQL, records)
        logger.info(f"[sentiment/writer] Upserted {len(records)} silver rows")
        return len(records)
    except Exception as e:
        logger.error(f"[sentiment/writer] Silver upsert failed → {e}")
        raise