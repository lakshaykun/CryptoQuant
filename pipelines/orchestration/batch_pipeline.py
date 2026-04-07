# pipelines/orchestration/batch_pipeline.py

from pipelines.utils.spark import get_spark
from pipelines.utils.logger import get_logger
from pipelines.ingestion.batch.market import run_market_batch_ingestion_pipeline


def run_batch_pipeline(symbols):
    logger = get_logger("batch_pipeline")

    logger.info("Starting batch pipeline...")

    spark = get_spark(logger)

    try:
        # Step 1: Ingestion → Bronze
        run_market_batch_ingestion_pipeline(symbols, spark, logger)

        # (future)
        # Step 2: Bronze → Silver
        # run_market_silver_pipeline(spark)

        # Step 3: Silver → Gold
        # run_market_gold_pipeline(spark)

        logger.info("Batch pipeline completed successfully")

    except Exception as e:
        logger.error(f"Batch pipeline failed: {e}", exc_info=True)
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")