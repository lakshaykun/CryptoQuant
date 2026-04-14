# pipelines/jobs/batch/bronze.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipelines.schema.raw.market import RAW_MARKET_SCHEMA
from pipelines.storage.delta.writer import write_batch
from pipelines.transformers.bronze.market import BronzeMarketTransformer
from pipelines.schema.bronze.market import BRONZE_MARKET_SCHEMA
from pipelines.utils.logger import get_logger
from pipelines.utils.config_loader import load_config
from pipelines.storage.local.csv import read_csv


def main():
    logger = get_logger("bronze_job")

    spark = SparkSession.builder.appName("bronze-job").getOrCreate()

    try:
        config = load_config("configs/data.yaml")
        raw_path = config.get("raw_data_path", {}).get("market", "/opt/app/raw_data/market/")

        logger.info(f"Reading raw data from: {raw_path}")

        # -----------------------------
        # Read RAW CSV files
        # -----------------------------
        raw_csv_path = f"{raw_path}/*.csv"
        df = read_csv(spark, raw_csv_path, RAW_MARKET_SCHEMA)

        if df.rdd.isEmpty():
            logger.warning("No raw data found, skipping")
            return

        # -----------------------------
        # Transform → Bronze
        # -----------------------------
        df = BronzeMarketTransformer.transform(df, "batch")

        logger.info(f"Bronze count: {df.count()}")

        # -----------------------------
        # Write to Delta
        # -----------------------------
        write_batch(
            df,
            "bronze_market",
            BRONZE_MARKET_SCHEMA
        )

        logger.info("Bronze ingestion complete")

    finally:
        spark.stop()
        logger.info("Spark stopped")


if __name__ == "__main__":
    main()