# pipelines/jobs/batch/bronze.py

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipelines.schema.raw.market import RAW_MARKET_SCHEMA
from pipelines.schema.state.market import STATE_MARKET_SCHEMA
from pipelines.storage.delta.reader import check_table_exists, get_last_processed_time_symbols
from pipelines.storage.delta.writer import write_batch
from pipelines.transformers.bronze.market import BronzeMarketTransformer
from pipelines.schema.bronze.market import BRONZE_MARKET_SCHEMA
from utils_global.logger import get_logger
from utils_global.config_loader import load_config
from pipelines.storage.local.csv import read_csv


def main():
    logger = get_logger("bronze_job")

    spark = SparkSession.builder.appName("bronze-job").getOrCreate()

    try:
        config = load_config("configs/data.yaml")
        raw_path = config.get("raw_data_path", {}).get("market", "/opt/app/raw_data/market/")
        symbols = config.get("symbols") or []
        state_date_value = config.get("state_date") or config.get("start_date")

        if not state_date_value:
            raise ValueError("data.yaml must define either 'state_date' or 'start_date'")

        state_date = datetime.fromisoformat(state_date_value)

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

        if df is None or df.rdd.isEmpty():
            logger.warning("No data after transformation, skipping write")
            return

        logger.info(f"Bronze count: {df.count()}")

        current_state_df = df.groupBy("symbol").agg(
            F.max("open_time").alias("last_processed_time")
        )

        # -----------------------------
        # Write to Delta
        # -----------------------------
        write_batch(
            df,
            "bronze_market",
            BRONZE_MARKET_SCHEMA
        )

        if not symbols:
            raise ValueError("data.yaml must define 'symbols' for market state updates")

        if check_table_exists(spark, "market_state"):
            checkpoint = get_last_processed_time_symbols(
                spark,
                "market_state",
                symbols,
                state_date,
            )
        else:
            checkpoint = {symbol: state_date for symbol in symbols}

        checkpoint_df = spark.createDataFrame(
            [(symbol, last_time) for symbol, last_time in checkpoint.items()],
            ["symbol", "last_processed_time"],
        )

        state_df = (
            checkpoint_df.select("symbol", "last_processed_time")
            .unionByName(current_state_df.select("symbol", "last_processed_time"))
            .groupBy("symbol")
            .agg(F.max("last_processed_time").alias("last_processed_time"))
        )

        write_batch(
            state_df,
            "market_state",
            STATE_MARKET_SCHEMA,
            mode="overwrite",
            upsert=False,
        )

        logger.info("Bronze ingestion complete")

    finally:
        spark.stop()
        logger.info("Spark stopped")


if __name__ == "__main__":
    main()