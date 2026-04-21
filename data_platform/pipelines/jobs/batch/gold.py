# pipelines/jobs/batch/gold.py

from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pipelines.storage.delta.reader import read_incremental_symbols, read_table, check_table_exists
from pipelines.transformers.gold.market import GoldMarketTransformer
from pipelines.storage.delta.writer import write_batch
from pipelines.schema.gold.market import GOLD_MARKET_SCHEMA
from utils_global.logger import get_logger


def main():
    logger = get_logger("gold_job")

    spark = SparkSession.builder.appName("gold-job").getOrCreate()

    try:
        if not check_table_exists(spark, "market_state"):
            logger.warning("No state found, skipping")
            return
        
        state_df = read_table(spark, "market_state")

        last_open_times_symbols = {
            row["symbol"]: max(
                (row["last_processed_time"] - timedelta(minutes=30)),
                datetime(1970, 1, 1)
            )
            for row in state_df.collect()
        }

        df = read_incremental_symbols(
            spark,
            "silver_market",
            last_values=last_open_times_symbols,
        )

        df = GoldMarketTransformer.transform(df)

        if df is None or df.rdd.isEmpty():
            logger.info("No new data to process, skipping write")
            return

        write_batch(df, "gold_market", GOLD_MARKET_SCHEMA)

        logger.info("Gold complete")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()