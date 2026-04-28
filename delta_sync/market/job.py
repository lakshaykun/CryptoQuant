import os
import datetime
import asyncpg
from dotenv import load_dotenv
from delta_sync import reader, writer
from data_platform.utils_global.logger import get_logger

load_dotenv()

logger = get_logger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")


async def get_last_synced_time(conn: asyncpg.Connection, table: str) -> datetime.datetime | None:
    result = await conn.fetchval(f"SELECT MAX(open_time) FROM {table}")
    return result

async def run():
    logger.info("[job] Starting sync run")
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        # read market data
        last_market_time = await get_last_synced_time(conn, "market")

        if last_market_time is None:
            df_market = reader.read_full()
        else:
            df_market = reader.read_incremental(last_market_time)

        # write market data
        if not df_market.empty:
            await writer.upsert_market_rows(df_market, conn)
        else:
            logger.info("[job] No new market rows")

        # read predictions data
        last_pred_time = await get_last_synced_time(conn, "predictions")

        if last_pred_time is None:
            df_pred = reader.read_predictions_full()
        else:
            df_pred = reader.read_predictions_incremental(last_pred_time)

        # write predictions data
        if not df_pred.empty:
            await writer.upsert_prediction_rows(df_pred, conn)
        else:
            logger.info("[job] No new prediction rows")

        logger.info("[job] Sync complete")

    except Exception as e:
        logger.error(f"[job] Sync run failed → {e}")
        raise

    finally:
        await conn.close()
