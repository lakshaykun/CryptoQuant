from __future__ import annotations

import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipelines.schema.bronze.sentiment import BRONZE_SENTIMENT_SCHEMA
from pipelines.storage.delta.utils import get_table_config
from utils.config_loader import load_config
from utils.logger import get_logger
from utils.spark_utils import create_delta_spark_session


logger = get_logger("sentiment_delta_sink")
_SPARK: SparkSession | None = None
_DATA_CONFIG = load_config("configs/data.yaml")


def _spark() -> SparkSession:
    global _SPARK
    if _SPARK is None:
        _SPARK = create_delta_spark_session("sentiment-source-delta-sink")
    return _SPARK


def write_events_to_bronze_delta(events: list[dict]) -> int:
    if not events:
        return 0

    spark = _spark()
    bronze_path = get_table_config("bronze_sentiment", _DATA_CONFIG)["path"]

    df = spark.createDataFrame(events)
    if "timestamp" not in df.columns:
        return 0

    prepared = (
        df
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withColumn("event_date", F.to_date("event_time"))
        .where(
            F.col("id").isNotNull()
            & F.col("source").isNotNull()
            & F.col("symbol").isNotNull()
            & F.col("event_time").isNotNull()
            & F.col("event_date").isNotNull()
        )
        .dropDuplicates(["id", "source", "symbol", "event_time"])
        .select([field.name for field in BRONZE_SENTIMENT_SCHEMA.fields if field.name != "ingestion_time"])
    )

    if prepared.rdd.isEmpty():
        return 0

    count = prepared.count()
    for attempt in range(1, 4):
        try:
            (
                prepared.write
                .format("delta")
                .mode("append")
                .partitionBy("source", "symbol", "event_date")
                .save(bronze_path)
            )
            logger.info("Delta bronze_sentiment append: rows=%d path=%s", count, bronze_path)
            return int(count)
        except Exception as exc:
            if attempt == 3:
                logger.warning("Delta append failed after retries: %s", exc)
                return 0
            time.sleep(0.5 * attempt)
    return 0
