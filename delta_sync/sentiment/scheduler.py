import asyncio
import os
from dotenv import load_dotenv
from delta_sync.sentiment.job import run
from data_platform.utils_global.logger import get_logger

load_dotenv()

logger = get_logger(__name__)
SYNC_INTERVAL_SECONDS = int(os.getenv("SENTIMENT_SYNC_INTERVAL_SECONDS", 60))


async def loop():
    logger.info(f"[sentiment/scheduler] Starting — interval: {SYNC_INTERVAL_SECONDS}s")
    while True:
        try:
            await run()
        except Exception as e:
            logger.error(f"[sentiment/scheduler] Job failed, retrying next interval → {e}")
        await asyncio.sleep(SYNC_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(loop())