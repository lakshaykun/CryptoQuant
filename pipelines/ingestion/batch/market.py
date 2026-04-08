# pipelines/ingestion/batch/market.py

from datetime import datetime
from pipelines.ingestion.batch.fetch_coins import fetch_coins_data
from pipelines.bronze.market import write_to_bronze
from pipelines.ingestion.batch.utils import get_last_ingested_timestamp


def run_market_batch_ingestion_pipeline(symbols, interval, bronze_market_path, start_date, spark, logger):
    if not symbols:
        logger.info("No symbols to fetch.")
        return
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    last_ts_symbols = get_last_ingested_timestamp(spark, bronze_market_path, logger, symbols, start_date)

    end_date = datetime.now()

    pdf = fetch_coins_data(symbols, interval, last_ts_symbols, end_date, logger)

    if pdf.empty:
        logger.warning("No data fetched.")
        return

    logger.info(f"Fetched {len(pdf)} rows for {len(symbols)} symbols")

    write_to_bronze(pdf, spark, bronze_market_path, logger, source="batch")
