import os
import datetime
import asyncpg
from dotenv import load_dotenv
from delta_sync.sentiment import reader, writer
from data_platform.utils_global.logger import get_logger

load_dotenv()

logger = get_logger(__name__)
DATABASE_URL = os.getenv("DATABASE_URL")


async def get_last_synced_time(conn: asyncpg.Connection, table: str, time_col: str) -> dict:
    rows = await conn.fetch(f"SELECT symbol, MAX({time_col}) as latest FROM {table} GROUP BY symbol")
    return {r['symbol']: r['latest'] for r in rows}


async def run():
    logger.info("[sentiment/job] Starting sync run")
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        # gold table
        last_gold_time = await get_last_synced_time(conn, "sentiment_gold", "window_start")

        if last_gold_time is None:
            df_gold = reader.read_gold_full()
        else:
            df_gold = reader.read_gold_incremental(last_gold_time)

        if not df_gold.empty:
            await writer.upsert_gold_rows(df_gold, conn)
        else:
            logger.info("[sentiment/job] No new gold rows")

        # silver table
        last_silver_time = await get_last_synced_time(conn, "sentiment_silver", "event_time")

        if last_silver_time is None:
            df_silver = reader.read_silver_full()
        else:
            df_silver = reader.read_silver_incremental(last_silver_time)

        if not df_silver.empty:
            await writer.upsert_silver_rows(df_silver, conn)
        else:
            logger.info("[sentiment/job] No new silver rows")

        logger.info("[sentiment/job] Sync complete")

    except Exception as e:
        logger.error(f"[sentiment/job] Sync run failed → {e}")
        raise

    finally:
        await conn.close()