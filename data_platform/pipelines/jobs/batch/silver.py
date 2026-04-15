# pipelines/jobs/batch/silver.py

from pyspark.sql import SparkSession
from pipelines.storage.delta.reader import read_incremental_symbols, read_table, check_table_exists
from pipelines.transformers.silver.market import SilverMarketTransformer
from pipelines.storage.delta.writer import write_batch
from pipelines.schema.silver.market import SILVER_MARKET_SCHEMA
from utils.logger import get_logger


def main():
    logger = get_logger("silver_job")

    spark = SparkSession.builder.appName("silver-job").getOrCreate()

    try:
        if not check_table_exists(spark, "market_state"):
            logger.warning("No state found, skipping")
            return
        
        state_df = read_table(spark, "market_state")

        last_open_times_symbols = {
            row["symbol"]: row["last_processed_time"]
            for row in state_df.collect()
        }

        df = read_incremental_symbols(
            spark,
            "bronze_market",
            last_values=last_open_times_symbols,
        )

        df = SilverMarketTransformer.transform(df)

        write_batch(df, "silver_market", SILVER_MARKET_SCHEMA)

        logger.info("Silver complete")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()