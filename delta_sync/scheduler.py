import asyncio
import os
from dotenv import load_dotenv
from delta_sync.job import run
from data_platform.utils_global.logger import get_logger

load_dotenv()

logger = get_logger(__name__)

SYNC_INTERVAL_SECONDS = int(os.getenv("SYNC_INTERVAL_SECONDS", 60))

async def loop():
    """
    Runs the sync job repeatedly on a fixed interval.
    """
    logger.info(f"[scheduler] Starting — interval: {SYNC_INTERVAL_SECONDS}s")

    while True:
        try:
            await run()
        except Exception as e:
            logger.error(f"[scheduler] Job failed, will retry next interval → {e}")

        await asyncio.sleep(SYNC_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(loop())