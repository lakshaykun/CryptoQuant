# pipelines/ingestion/batch/jobs/market/backfill_historical.py

from datetime import datetime
from pipelines.ingestion.batch.sources.market.binance_historical import fetch_coins_data
from pipelines.storage.delta.reader import get_last_timestamp_symbols
from pandas import DataFrame


def fetch_market_historical_backfill(
        symbols, 
        interval, 
        start_date, 
        spark, 
        logger
) -> DataFrame:
    if not symbols:
        logger.info("No symbols to fetch.")
        return DataFrame()
    
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    last_ts_symbols = get_last_timestamp_symbols(spark, "bronze_market", symbols, start_date)

    end_date = datetime.now()

    pdf = fetch_coins_data(symbols, interval, last_ts_symbols, end_date, logger)

    if pdf.empty:
        logger.warning("No data fetched.")
        return pdf

    logger.info(f"Fetched {len(pdf)} rows for {len(symbols)} symbols")
    
    return pdf