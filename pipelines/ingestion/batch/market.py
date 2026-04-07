# pipelines/ingestion/batch/market.py

from datetime import datetime
from pipelines.ingestion.batch.fetch_coins import fetch_coins_data
from configs.data import INTERVAL, START_DATE, BRONZE_PATH, MARKET_PATH
from pipelines.bronze.market import write_to_bronze
from pipelines.ingestion.batch.utils import get_last_ingested_timestamp, interval_to_timedelta


def run_market_batch_ingestion_pipeline(symbols, spark, logger):
    if not symbols:
        logger.info("No symbols to fetch.")
        return

    bronze_path = f"{BRONZE_PATH}/{MARKET_PATH}"

    last_ts = get_last_ingested_timestamp(spark, bronze_path, logger)

    if last_ts:
        start_date = last_ts - interval_to_timedelta(INTERVAL)  # depends on INTERVAL
        logger.info(f"Resuming from {start_date}")
    else:
        start_date = datetime.strptime(START_DATE, "%Y-%m-%d")
        logger.info(f"No previous data found. Starting from {start_date}")

    end_date = datetime.now()

    pdf = fetch_coins_data(symbols, INTERVAL, start_date, end_date, logger)

    if pdf.empty:
        logger.warning("No data fetched.")
        return

    logger.info(f"Fetched {len(pdf)} rows for {len(symbols)} symbols")

    write_to_bronze(pdf, spark, bronze_path, logger, source="batch")
