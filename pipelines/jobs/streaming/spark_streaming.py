# pipelines/jobs/streaming/spark_streaming.py

import time
from datetime import datetime, timezone

from prometheus_client import Gauge
from pyspark.sql import functions as F
from pipelines.ingestion.streaming.utils.helpers import parse_kafka_message
from pipelines.schema.bronze.market import BRONZE_MARKET_SCHEMA
from pipelines.schema.gold.market import GOLD_MARKET_SCHEMA
from pipelines.schema.silver.market import SILVER_MARKET_SCHEMA
from pipelines.transformers.bronze.market import BronzeMarketTransformer
from pipelines.transformers.gold.market import GoldMarketTransformer
from pipelines.transformers.silver.market import SilverMarketTransformer
from utils_global.logger import get_logger
from utils_global.config_loader import load_config
from utils_global.prometheus import build_registry, metric_name, push_registry
from pipelines.schema.raw.market import RAW_MARKET_SCHEMA
from pipelines.utils.spark import get_spark
from pipelines.storage.delta.writer import write_batch


kafkaConfig = load_config("configs/kafka.yaml")
dataConfig = load_config("configs/data.yaml")
logger = get_logger("spark_streaming")

spark = get_spark(logger, need_kafka=True)


def _normalize_open_time(value):
    if value is None:
        return None

    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)

    if isinstance(value, (int, float)):
        seconds = float(value)
        if seconds > 1e12:
            seconds /= 1000.0
        return datetime.fromtimestamp(seconds, tz=timezone.utc)

    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def _estimate_streaming_lag_seconds(df):
    max_open_time = df.select(F.max("open_time").alias("max_open_time")).collect()[0]["max_open_time"]
    max_open_time = _normalize_open_time(max_open_time)
    if max_open_time is None:
        return 0.0

    lag = (datetime.now(timezone.utc) - max_open_time).total_seconds()
    return float(max(lag, 0.0))


def _push_streaming_metrics(records_processed, batch_processing_time, streaming_lag):
    registry = build_registry()

    records_metric = Gauge(
        metric_name("records_processed"),
        "Number of records processed in the latest Spark micro-batch.",
        ["pipeline_job"],
        registry=registry,
    )
    batch_time_metric = Gauge(
        metric_name("batch_processing_time_seconds"),
        "Processing time in seconds for the latest Spark micro-batch.",
        ["pipeline_job"],
        registry=registry,
    )
    lag_metric = Gauge(
        metric_name("streaming_lag_seconds"),
        "Lag in seconds between now and latest processed market open_time.",
        ["pipeline_job"],
        registry=registry,
    )

    records_metric.labels(pipeline_job="spark_streaming").set(float(records_processed))
    batch_time_metric.labels(pipeline_job="spark_streaming").set(float(batch_processing_time))
    lag_metric.labels(pipeline_job="spark_streaming").set(float(streaming_lag))

    push_registry(registry, job_name="spark_streaming", grouping_key={"component": "spark_streaming"})


def _process_pipeline(df, epoch_id):
    if df is None:
        return

    records_processed = df.count()
    if records_processed == 0:
        return

    start_time = time.perf_counter()

    # -------------------
    # Bronze
    # -------------------
    bronze = BronzeMarketTransformer().transform(df, "stream")
    
    write_batch(
        bronze,
        "bronze_market",
        expected_schema=BRONZE_MARKET_SCHEMA
    )

    # -------------------
    # Silver
    # -------------------
    silver = SilverMarketTransformer().transform(bronze)

    write_batch(
        silver,
        "silver_market",
        expected_schema=SILVER_MARKET_SCHEMA
    )

    # -------------------
    # Gold
    # -------------------
    gold = GoldMarketTransformer.process_gold_stream_batch(silver, epoch_id)


    # Write to delta
    write_batch(
        gold,
        "gold_market",
        expected_schema=GOLD_MARKET_SCHEMA,
    )

    duration = time.perf_counter() - start_time
    lag_seconds = _estimate_streaming_lag_seconds(df)
    _push_streaming_metrics(records_processed, duration, lag_seconds)
    logger.info(
        "[spark_streaming] epoch=%s records=%s duration=%.3fs lag=%.3fs",
        epoch_id,
        records_processed,
        duration,
        lag_seconds,
    )

# -------------------------------
# 1. READ FROM KAFKA
# -------------------------------
brokers = kafkaConfig["brokers"]

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", brokers) \
    .option("subscribe", list(kafkaConfig["topics"].keys())[0]) \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = parse_kafka_message(df, RAW_MARKET_SCHEMA)

parsed_df.writeStream \
    .foreachBatch(_process_pipeline) \
    .option("checkpointLocation", dataConfig["checkpoints"]["market"]) \
    .start()

spark.streams.awaitAnyTermination()