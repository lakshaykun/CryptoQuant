from pipelines.jobs.streaming.common.runtime_env import configure_pyspark_python

configure_pyspark_python()

import concurrent.futures
from collections.abc import Iterator

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, from_json
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.window import Window
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipelines.jobs.streaming.common.job_runtime import (
    await_query,
    configure_job_logging,
    log_stream_schema,
)
from pipelines.jobs.streaming.common.sentiment_client import CryptoBertClient
from pipelines.transformers.silver.sentiment import SilverSentimentTransformer
from utils.spark_config import (
    get_checkpoint_path,
    get_delta_path,
    get_gold_sentiment_endpoint,
    get_gold_sentiment_timeout_seconds,
    get_spark_app_name,
    get_spark_master,
    get_silver_trigger_seconds,
)
from utils.spark_utils import create_delta_spark_session

BRONZE_DELTA_PATH = get_delta_path("bronze", "delta/bronze")
SILVER_DELTA_PATH = get_delta_path("silver", "delta/silver")
SILVER_CHECKPOINT_ROOT = get_checkpoint_path("silver", "checkpoints/silver")
SILVER_CHECKPOINT_PATH = f"{SILVER_CHECKPOINT_ROOT.rstrip('/')}/clean_merge_stream"
SILVER_SENTIMENT_ENDPOINT = get_gold_sentiment_endpoint("http://127.0.0.1:8000/predict")
SILVER_SENTIMENT_TIMEOUT_SECONDS = get_gold_sentiment_timeout_seconds(10)
APP_NAME = f"{get_spark_app_name()}-silver"
SILVER_TRIGGER_SECONDS = get_silver_trigger_seconds(5)
MAX_CONCURRENT_REQUESTS = 20

scored_schema = StructType([
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

bronze_schema = StructType([
    StructField("raw_json", StringType()),
    StructField("topic", StringType()),
    StructField("partition", IntegerType()),
    StructField("offset", LongType()),
    StructField("kafka_timestamp", TimestampType()),
])


_SENTIMENT_CLIENT: CryptoBertClient | None = None


def _get_sentiment_client() -> CryptoBertClient:
    global _SENTIMENT_CLIENT
    if _SENTIMENT_CLIENT is None:
        _SENTIMENT_CLIENT = CryptoBertClient(
            endpoint=SILVER_SENTIMENT_ENDPOINT,
            timeout_seconds=SILVER_SENTIMENT_TIMEOUT_SECONDS,
        )
    return _SENTIMENT_CLIENT


def ensure_delta_table(spark: SparkSession, path: str, schema: StructType) -> None:
    if DeltaTable.isDeltaTable(spark, path):
        return
    spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(path)


def build_parsed_stream():
    spark = create_delta_spark_session(APP_NAME, master=get_spark_master())
    ensure_delta_table(spark, BRONZE_DELTA_PATH, bronze_schema)

    return (
        spark.readStream
        .format("delta")
        .load(BRONZE_DELTA_PATH)
        .select(
            from_json(col("raw_json"), scored_schema).alias("data"),
            col("kafka_timestamp"),
        )
        .select("data.*", "kafka_timestamp")
    )


def _apply_duplicate_engagement_delta(batch_df: DataFrame) -> DataFrame:
    if batch_df is None or not batch_df.take(1):
        return batch_df

    if not DeltaTable.isDeltaTable(batch_df.sparkSession, SILVER_DELTA_PATH):
        return batch_df

    current_ids = batch_df.select("id").where(F.col("id").isNotNull()).dropDuplicates()
    if not current_ids.take(1):
        return batch_df

    silver_history = batch_df.sparkSession.read.format("delta").load(SILVER_DELTA_PATH)
    if "id" not in silver_history.columns or "engagement" not in silver_history.columns:
        return batch_df

    order_cols = [F.col("event_time").desc_nulls_last()] if "event_time" in silver_history.columns else []
    if "timestamp" in silver_history.columns:
        order_cols.append(F.col("timestamp").desc_nulls_last())
    order_cols.append(F.col("engagement").cast("long").desc_nulls_last())

    latest_per_id = (
        silver_history
        .select("id", "engagement", *(["event_time"] if "event_time" in silver_history.columns else []), *(["timestamp"] if "timestamp" in silver_history.columns else []))
        .where(F.col("id").isNotNull())
        .join(current_ids, on="id", how="inner")
        .withColumn("_rn", F.row_number().over(Window.partitionBy("id").orderBy(*order_cols)))
        .where(F.col("_rn") == 1)
        .select(
            F.col("id"),
            F.col("engagement").cast("long").alias("_prev_engagement"),
        )
    )

    return (
        batch_df
        .join(latest_per_id, on="id", how="left")
        .withColumn("_current_engagement", F.coalesce(F.col("engagement").cast("long"), F.lit(0)))
        .withColumn(
            "engagement",
            F.when(F.col("_prev_engagement").isNull(), F.col("_current_engagement"))
            .otherwise(F.col("_current_engagement") - F.col("_prev_engagement"))
            .cast("int"),
        )
        .drop("_prev_engagement", "_current_engagement")
    )


def _score_row_batch(rows: list, client: CryptoBertClient) -> list[tuple]:
    if not rows:
        return []

    texts = [str(getattr(row, "text", "") or "") for row in rows]

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS) as executor:
        results = list(executor.map(client.infer_sentiment, texts))

    scored_rows: list[tuple] = []
    for row, (label, confidence, base_score) in zip(rows, results):
        engagement = float(getattr(row, "engagement", 0) or 0)
        weighted_sentiment = float(base_score) * float(confidence) * engagement
        scored_rows.append(
            (
                getattr(row, "id", None),
                getattr(row, "timestamp", None),
                getattr(row, "source", None),
                getattr(row, "text", None),
                getattr(row, "engagement", None),
                getattr(row, "symbol", None),
                getattr(row, "event_time", None),
                label,
                float(confidence),
                weighted_sentiment,
            )
        )

    return scored_rows


def _score_partition(rows_iter: Iterator) -> Iterator[tuple]:
    client = _get_sentiment_client()
    batch: list = []
    chunk_size = 200

    for row in rows_iter:
        batch.append(row)
        if len(batch) >= chunk_size:
            for scored in _score_row_batch(batch, client):
                yield scored
            batch.clear()

    if batch:
        for scored in _score_row_batch(batch, client):
            yield scored


def build_scored_stream(cleaned_stream: DataFrame) -> DataFrame:
    scored_rdd = cleaned_stream.rdd.mapPartitions(_score_partition)
    return cleaned_stream.sparkSession.createDataFrame(scored_rdd, schema=scored_schema)


def process_silver_batch(batch_df: DataFrame, batch_id: int) -> None:
    if not batch_df.take(1):
        return

    adjusted_batch = _apply_duplicate_engagement_delta(batch_df)
    scored_batch = build_scored_stream(adjusted_batch)
    if not scored_batch.take(1):
        return

    # Insert-only write with Delta transactional dedupe for foreachBatch retries.
    (
        scored_batch.write
        .format("delta")
        .mode("append")
        .option("txnAppId", APP_NAME)
        .option("txnVersion", int(batch_id))
        .save(SILVER_DELTA_PATH)
    )


def start_silver_query(validated_stream: DataFrame) -> StreamingQuery:
    return (
        validated_stream.writeStream
        .queryName(f"{APP_NAME}-write")
        .option("checkpointLocation", SILVER_CHECKPOINT_PATH)
        .trigger(processingTime=f"{SILVER_TRIGGER_SECONDS} seconds")
        .foreachBatch(process_silver_batch)
        .start()
    )


def run() -> None:
    configure_job_logging()

    parsed_stream = build_parsed_stream()
    validated_stream = SilverSentimentTransformer.transform(parsed_stream)
    log_stream_schema(validated_stream, "silver_validated_stream")

    query = start_silver_query(validated_stream)
    await_query(query)


if __name__ == "__main__":
    run()
