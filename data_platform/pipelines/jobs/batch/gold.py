# pipelines/jobs/batch/gold.py

from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipelines.schema.state.market import STATE_MARKET_SCHEMA
from pipelines.storage.delta.reader import get_last_processed_time_symbols, read_incremental_symbols
from pipelines.transformers.gold.market import GoldMarketTransformer
from pipelines.storage.delta.writer import write_batch
from pipelines.schema.gold.market import GOLD_MARKET_SCHEMA
from utils_global.config_loader import load_config
from utils_global.logger import get_logger


def main():
    logger = get_logger("gold_job")

    spark = SparkSession.builder.appName("gold-job").getOrCreate()

    try:
        config = load_config("configs/data.yaml")
        symbols = config.get("symbols") or []
        state_date_value = config.get("state_date") or config.get("start_date")

        if not symbols:
            raise ValueError("data.yaml must define 'symbols' for gold ingestion")
        if not state_date_value:
            raise ValueError("data.yaml must define either 'state_date' or 'start_date'")

        state_date = datetime.fromisoformat(state_date_value)

        state_checkpoint_symbols = get_last_processed_time_symbols(
            spark,
            "market_state",
            symbols,
            state_date,
        )

        read_checkpoint_symbols = {
            symbol: max(
                (last_processed_time - timedelta(minutes=30)),
                datetime(1970, 1, 1)
            )
            for symbol, last_processed_time in state_checkpoint_symbols.items()
        }

        df = read_incremental_symbols(
            spark,
            "silver_market",
            last_values=read_checkpoint_symbols,
        )

        df = GoldMarketTransformer.transform(df)

        if df is None or df.rdd.isEmpty():
            logger.info("No new data to process, skipping write")
            return

        write_batch(df, "gold_market", GOLD_MARKET_SCHEMA)

        previous_state_df = spark.createDataFrame(
            [(symbol, last_time) for symbol, last_time in state_checkpoint_symbols.items()],
            ["symbol", "last_processed_time"],
        )

        current_state_df = df.groupBy("symbol").agg(
            F.max("open_time").alias("last_processed_time")
        )

        state_df = (
            previous_state_df
            .unionByName(current_state_df)
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

        logger.info("Gold complete")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()