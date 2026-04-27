import os
import datetime
import asyncpg
from dotenv import load_dotenv
from delta_sync import reader, writer
from data_platform.utils_global.logger import get_logger

load_dotenv()

logger = get_logger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")


async def get_last_synced_time(conn: asyncpg.Connection) -> datetime.datetime | None:
    """
    Gets the latest open_time already in TimescaleDB.
    """
    result = await conn.fetchval("SELECT MAX(open_time) FROM market")
    return result


async def run():
    """
    Reads new rows from Delta gold table and writes to TimescaleDB.
    """
    logger.info("[job] Starting sync run")

    conn = await asyncpg.connect(DATABASE_URL)

    try:
        last_synced_time = await get_last_synced_time(conn)

        # if syncing for the first time, do a full load; otherwise, only read new rows
        if last_synced_time is None:
            logger.info("[job] No existing data — running full load")
            df = reader.read_full()
        else:
            logger.info(f"[job] Incremental load from {last_synced_time}")
            df = reader.read_incremental(last_synced_time)

        if df.empty:
            logger.info("[job] No new rows found — skipping write")
            return

        rows_written = await writer.upsert_market_rows(df, conn)
        logger.info(f"[job] Sync complete → {rows_written} rows written")

    except Exception as e:
        logger.error(f"[job] Sync run failed → {e}")
        raise

    finally:
        await conn.close()