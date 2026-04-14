# pipelines/orchestration/batch/ingest.py

from datetime import datetime
from pyspark.sql import SparkSession
from pipelines.schema.state.market import STATE_MARKET_SCHEMA
from pipelines.storage.delta.reader import get_last_open_time_symbols
from pipelines.utils.logger import get_logger
from pipelines.utils.config_loader import load_config
from pipelines.ingestion.batch.jobs.market.backfill_historical import fetch_market_historical_backfill
from pipelines.transformers.bronze.market import BronzeMarketTransformer
from pipelines.storage.delta.writer import write_batch
from pipelines.schema.bronze.market import BRONZE_MARKET_SCHEMA


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

        pdf = fetch_market_historical_backfill(
            symbols,
            interval,
            last_open_time_symbols,
            logger
        )

        if pdf.empty:
            logger.warning("No data fetched")
            return

        df = BronzeMarketTransformer.transform(pdf, source="batch", spark=spark)

        write_batch(df, "bronze_market", BRONZE_MARKET_SCHEMA)

        logger.info("Ingestion complete")

        state_df = spark.createDataFrame(
            [(symbol, last_time) for symbol, last_time in last_open_time_symbols.items()],
            ["symbol", "last_processed_time"]
        )

        # overwrite per symbol (merge-like behavior)
        write_batch(
            state_df,
            "market_state",
            STATE_MARKET_SCHEMA,
            mode = "append",
            upsert = True

        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()