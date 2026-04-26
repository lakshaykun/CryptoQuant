# pipelines/jobs/batch/silver.py

from datetime import datetime

from pyspark.sql import SparkSession

from pipelines.storage.delta.reader import get_last_processed_time_symbols, read_incremental_symbols
from pipelines.transformers.silver.market import SilverMarketTransformer
from pipelines.storage.delta.writer import write_batch
from pipelines.schema.silver.market import SILVER_MARKET_SCHEMA
from utils_global.config_loader import load_config
from utils_global.logger import get_logger


def main():
    logger = get_logger("silver_job")

    spark = SparkSession.builder.appName("silver-job").getOrCreate()

    try:
        config = load_config("configs/data.yaml")
        symbols = config.get("symbols") or []
        state_date_value = config.get("state_date") or config.get("start_date")

        if not symbols:
            raise ValueError("data.yaml must define 'symbols' for silver ingestion")
        if not state_date_value:
            raise ValueError("data.yaml must define either 'state_date' or 'start_date'")

        state_date = datetime.fromisoformat(state_date_value)

        last_open_times_symbols = get_last_processed_time_symbols(
            spark,
            "market_batch_state",
            symbols,
            state_date,
        )

        df = read_incremental_symbols(
            spark,
            "bronze_market",
            last_values=last_open_times_symbols,
        )

        df = SilverMarketTransformer.transform(df)

        if df is None or df.rdd.isEmpty():
            logger.info("No new data to process, skipping write")
            return

        write_batch(df, "silver_market", SILVER_MARKET_SCHEMA)

        logger.info("Silver complete")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()