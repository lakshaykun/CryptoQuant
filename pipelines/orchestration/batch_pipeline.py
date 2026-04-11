# pipelines/orchestration/batch_pipeline.py

from pipelines.schema.gold.market import GOLD_MARKET_SCHEMA
from pipelines.schema.silver.market import SILVER_MARKET_SCHEMA
from pipelines.storage.delta.writer import write_batch
from pipelines.transformers.bronze.market import BronzeMarketTransformer
from pipelines.transformers.gold.market import GoldMarketTransformer
from pipelines.transformers.silver.market import SilverMarketTransformer
from pipelines.utils.spark import get_spark
from pipelines.utils.logger import get_logger
from pipelines.ingestion.batch.jobs.market.backfill_historical import fetch_market_historical_backfill
from pipelines.utils.config_loader import load_config
from pipelines.schema.bronze.market import BRONZE_MARKET_SCHEMA


def run_batch_pipeline(data_config_path="configs/data.yaml"):
    logger = get_logger("batch_pipeline")

    logger.info("Starting batch pipeline...")

    spark = get_spark(logger)
    dataConfig = load_config(data_config_path)

    try:
        # Step 1: Ingestion - Backfill historical market data
        pdf = fetch_market_historical_backfill(
            dataConfig.get("symbols", ["BTCUSDT"]), 
            dataConfig.get("interval", "5m"), 
            dataConfig.get("start_date", "2026-01-01"), 
            spark, 
            logger
        )

        if pdf.empty:
            logger.warning("No data fetched. Skipping pipeline.")
            return
    

        # Step 2: Transform and Write to Bronze
        df = BronzeMarketTransformer.transform(pdf, source="batch", spark=spark)

        # log few samples of head and tail after transformation for sanity check
        logger.info("Bronze data:")
        df.show(6, False)
        df.orderBy("open_time", ascending=False).show(6, False)

        write_batch(
            df,
            "bronze_market",
            BRONZE_MARKET_SCHEMA,
        )

        # Step 3: Transform and Write to Silver
        df = SilverMarketTransformer.transform(df)

        # log few samples of head and tail after transformation for sanity check
        logger.info("Silver data:")
        df.show(6, False)
        df.orderBy("open_time", ascending=False).show(6, False)

        write_batch(
            df,
            "silver_market",
            SILVER_MARKET_SCHEMA,
        )

        # Step 4: Transform and Write to Gold
        df = GoldMarketTransformer.transform(df)

        # log few samples of head and tail after transformation for sanity check
        logger.info("Gold data:")
        df.show(6, False)
        df.orderBy("open_time", ascending=False).show(6, False)

        write_batch(
            df,
            "gold_market",
            GOLD_MARKET_SCHEMA,
        )

        logger.info("Batch pipeline completed successfully")

    except Exception as e:
        logger.error(f"Batch pipeline failed: {e}", exc_info=True)
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")