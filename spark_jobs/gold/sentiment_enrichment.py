from spark_jobs.common.runtime_env import configure_pyspark_python

configure_pyspark_python()

from delta.tables import DeltaTable
from pyspark.sql.functions import avg, col, count, lit, sum as spark_sum, window
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

from spark_jobs.common.job_runtime import await_query, configure_job_logging
from utils.spark_utils import create_delta_spark_session
from utils.spark_config import (
    get_checkpoint_path,
    get_delta_path,
    get_gold_cron_minutes,
    get_gold_watermark_seconds,
    get_gold_window_seconds,
    get_spark_app_name,
    get_spark_master,
)

SILVER_DELTA_PATH = get_delta_path("silver", "delta/silver")
GOLD_DELTA_PATH = get_delta_path("gold", "delta/gold")
GOLD_CHECKPOINT_PATH = get_checkpoint_path("gold", "checkpoints/gold")
GOLD_WINDOW_SECONDS = get_gold_window_seconds(300)
GOLD_WINDOW_DURATION = f"{GOLD_WINDOW_SECONDS} seconds"
GOLD_WATERMARK_SECONDS = get_gold_watermark_seconds(600)
GOLD_WATERMARK_DURATION = f"{GOLD_WATERMARK_SECONDS} seconds"
GOLD_CRON_MINUTES = get_gold_cron_minutes(5)
APP_NAME = f"{get_spark_app_name()}-gold"

silver_schema = StructType([
    StructField("id", StringType()),
    StructField("timestamp", StringType()),
    StructField("source", StringType()),
    StructField("text", StringType()),
    StructField("engagement", IntegerType()),
    StructField("symbol", StringType()),
    StructField("event_time", TimestampType()),
    StructField("label", StringType()),
    StructField("confidence", DoubleType()),
    StructField("weighted_sentiment", DoubleType()),
])


def ensure_delta_table(spark: SparkSession, path: str, schema: StructType) -> None:
    if DeltaTable.isDeltaTable(spark, path):
        return

    spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(path)


def build_silver_stream():
    spark = create_delta_spark_session(APP_NAME, master=get_spark_master())
    ensure_delta_table(spark, SILVER_DELTA_PATH, silver_schema)
    return (
        spark.readStream
        .format("delta")
        .load(SILVER_DELTA_PATH)
    )


def build_gold_aggregation(silver_stream):
    weighted_col = col("weighted_sentiment") if "weighted_sentiment" in silver_stream.columns else lit(0.0)
    confidence_col = col("confidence") if "confidence" in silver_stream.columns else lit(0.0)

    compact = silver_stream.select(
        "event_time",
        "symbol",
        weighted_col.alias("weighted_sentiment"),
        "engagement",
        confidence_col.alias("confidence"),
    )

    return (
        compact
        .withWatermark("event_time", GOLD_WATERMARK_DURATION)
        .groupBy(
            window(col("event_time"), GOLD_WINDOW_DURATION),
            col("symbol"),
        )
        .agg(
            avg("weighted_sentiment").alias("sentiment_index"),
            spark_sum("engagement").alias("total_engagement"),
            avg("confidence").alias("avg_confidence"),
            count("*").alias("message_count"),
        )
    )


def run() -> None:
    configure_job_logging()
    silver_stream = build_silver_stream()
    gold_stream = build_gold_aggregation(silver_stream)

    query: StreamingQuery = (
        gold_stream.writeStream
        .format("delta")
        .queryName(f"{APP_NAME}-write")
        .outputMode("append")
        .option("checkpointLocation", GOLD_CHECKPOINT_PATH)
        .trigger(processingTime=f"{GOLD_CRON_MINUTES} minutes")
        .start(GOLD_DELTA_PATH)
    )
    await_query(query)


if __name__ == "__main__":
    run()