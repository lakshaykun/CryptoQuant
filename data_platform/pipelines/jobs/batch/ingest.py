# pipelines/orchestration/batch/ingest.py

from datetime import datetime
from pyspark.sql import SparkSession
from pipelines.schema.state.market import STATE_MARKET_SCHEMA
from pipelines.storage.delta.reader import get_last_open_time_symbols
from utils.logger import get_logger
from utils.config_loader import load_config
from pipelines.ingestion.batch.jobs.market.fetch_historical import fetch_market_historical
from pipelines.storage.delta.writer import write_batch


def main():
    logger = get_logger("ingest_job")

    spark = SparkSession.builder.appName("ingest-job").getOrCreate()

    try:
        config = load_config("configs/data.yaml")

        symbols = config.get("symbols", ["BTCUSDT"])
        start_date = datetime.fromisoformat(
            config.get("start_date", "2026-01-01")
        )
        interval = config.get("interval", "5m")

        last_open_time_symbols = get_last_open_time_symbols(
            spark, "bronze_market", symbols, start_date
        )

        base_path = config.get("raw_data_path", {}).get("market", "/opt/app/raw_data/market/")

        fetch_market_historical(
            symbols,
            interval,
            last_open_time_symbols,
            logger,
            base_path=base_path
        )

        logger.info("Ingestion complete")

        state_df = spark.createDataFrame(
            [(symbol, last_time) for symbol, last_time in last_open_time_symbols.items()],
            ["symbol", "last_processed_time"]
        )

        # Update state table with new last processed times
        write_batch(
            state_df,
            "market_state",
            STATE_MARKET_SCHEMA,
            mode = "overwrite",
            upsert = False
        )

        logger.info("State table updated")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()