# pipelines/ingestion/batch/jobs/market/backfill_historical.py

from datetime import datetime
from pipelines.ingestion.batch.sources.market.binance_historical import fetch_coins_data
from logging import Logger
from pandas import DataFrame


def fetch_market_historical_backfill(
        symbols: list, 
        interval: str, 
        last_open_time_symbols: dict, 
        logger: Logger
) -> DataFrame:
    if not symbols:
        logger.info("No symbols to fetch.")
        return DataFrame()
    
    end_date = datetime.now()

    pdf = fetch_coins_data(symbols, interval, last_open_time_symbols, end_date, logger)

    if pdf.empty:
        logger.warning("No data fetched.")
        return pdf

    logger.info(f"Fetched {len(pdf)} rows for {len(symbols)} symbols")
    
    return pdf