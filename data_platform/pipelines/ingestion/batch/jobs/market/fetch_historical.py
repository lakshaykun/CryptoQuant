# pipelines/ingestion/batch/jobs/market/fetch_historical.py

from datetime import datetime, timedelta
from pipelines.ingestion.batch.sources.market.binance_historical import fetch_single_day
from logging import Logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from pandas import DataFrame

MAX_WORKERS = 10


def daterange(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def fetch_market_historical(
        symbols: list, 
        interval: str, 
        last_open_time_symbols: dict, 
        logger: Logger,
        base_path: str,
):
    if not symbols:
        logger.info("No symbols to fetch.")
        return DataFrame()
    if not last_open_time_symbols:
        logger.info("No last open time information available.")
        return DataFrame()
    if not base_path:
        logger.info("No base path provided for saving data.")
        return DataFrame()
    
    end_date = datetime.now()

    tasks = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for symbol in symbols:
            for single_date in daterange(last_open_time_symbols[symbol], end_date):
                tasks.append(
                    executor.submit(
                        fetch_single_day,
                        symbol,
                        interval,
                        single_date,
                        base_path,
                        logger
                    )
                )

        for future in as_completed(tasks):
            future.result()

    logger.info("✅ Completed fetching historical data for following symbols:")
    for symbol in symbols:
        last_open_time = last_open_time_symbols.get(symbol)
        if last_open_time:
            logger.info(f"Symbol: {symbol}, Last Open Time: {last_open_time}")
        else:
            logger.info(f"Symbol: {symbol}, No data fetched.")