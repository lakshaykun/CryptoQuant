# pipelines/orchestration/batch/ingest.py

from datetime import datetime
from pyspark.sql import SparkSession
from pipelines.storage.delta.reader import (
    get_last_processed_time_symbols,
)
from utils_global.logger import get_logger
from utils_global.config_loader import load_config
from pipelines.ingestion.batch.jobs.market.fetch_historical import fetch_market_historical


def main():
    logger = get_logger("ingest_job")

    spark = SparkSession.builder.appName("ingest-job").getOrCreate()

    try:
        config = load_config("configs/data.yaml")

        symbols = config.get("symbols")
        state_date_value = config.get("state_date") or config.get("start_date")
        if not state_date_value:
            raise ValueError("data.yaml must define either 'state_date' or 'start_date'")

        state_date = datetime.fromisoformat(state_date_value)
        interval = config.get("interval")

        last_open_time_symbols = get_last_processed_time_symbols(
            spark,
            "market_state",
            symbols,
            state_date,
        )

        logger.info("Loaded ingestion checkpoint for raw fetch")

        base_path = config.get("raw_data_path").get("market")

        fetch_market_historical(
            symbols,
            interval,
            last_open_time_symbols,
            logger,
            base_path=base_path
        )

        logger.info("Ingestion complete")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()