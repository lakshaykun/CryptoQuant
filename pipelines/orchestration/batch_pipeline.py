# pipelines/orchestration/batch_pipeline.py

from pipelines.utils.spark import get_spark
from pipelines.utils.logger import get_logger
from pipelines.ingestion.batch.market import run_market_batch_ingestion_pipeline
from pipelines.utils.config_loader import load_config
from pipelines.silver.market import bronze_to_silver
from pipelines.gold.market import silver_to_gold


def run_batch_pipeline(data_config_path="configs/data.yaml"):
    logger = get_logger("batch_pipeline")

    logger.info("Starting batch pipeline...")

    spark = get_spark(logger)
    dataConfig = load_config(data_config_path)

    try:
        # Step 1: Ingestion → Bronze
        run_market_batch_ingestion_pipeline(
            dataConfig.get("symbols", ["BTCUSDT"]), 
            dataConfig.get("interval", "5m"), 
            dataConfig.get("bronze_market_path"), 
            dataConfig.get("start_date", "2026-01-01"), 
            spark, 
            logger
        )

        # Step 2: Bronze → Silver
        bronze_to_silver(
            spark, 
            dataConfig.get("bronze_market_path"), 
            dataConfig.get("silver_market_path"), 
            logger
        )

        # (future)
        # Step 3: Silver → Gold
        silver_to_gold(
            spark,
            dataConfig.get("silver_market_path"),
            dataConfig.get("gold_market_path"),
            logger
        )


        logger.info("Batch pipeline completed successfully")

    except Exception as e:
        logger.error(f"Batch pipeline failed: {e}", exc_info=True)
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")