# pipelines/ingestion/batch/jobs/market/fetch_today.py

import time
from logging import Logger

from pandas import DataFrame

from pipelines.ingestion.batch.sources.market.binance_today import fetch_today

REQUEST_DELAY_SECONDS = 1.5


def fetch_market_today(
    symbols: list,
    interval: str,
    logger: Logger,
    base_path: str,
):
    if not symbols:
        logger.info("No symbols to fetch.")
        return DataFrame()
    if not base_path:
        logger.info("No base path provided for saving data.")
        return DataFrame()

    for index, symbol in enumerate(symbols):
        fetch_today(
            symbol,
            interval,
            base_path,
            logger,
        )

        if index < len(symbols) - 1:
            time.sleep(REQUEST_DELAY_SECONDS)

    logger.info("✅ Completed fetching today's market data for following symbols:")
    for symbol in symbols:
        logger.info(f"Symbol: {symbol}")



