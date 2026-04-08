from spark_jobs.common.runtime_env import configure_pyspark_python

configure_pyspark_python()

from pyspark.sql.functions import avg, col, count, sum as spark_sum, udf, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from spark_jobs.common.job_runtime import await_query, configure_job_logging, start_delta_query
from spark_jobs.common.sentiment_client import CryptoBertClient
from utils.spark_utils import create_delta_spark_session
from utils.spark_config import (
    get_checkpoint_path,
    get_delta_path,
    get_gold_sentiment_endpoint,
    get_gold_sentiment_timeout_seconds,
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
GOLD_SENTIMENT_ENDPOINT = get_gold_sentiment_endpoint("http://127.0.0.1:8000/predict")
GOLD_SENTIMENT_TIMEOUT_SECONDS = get_gold_sentiment_timeout_seconds(10)
APP_NAME = f"{get_spark_app_name()}-gold"


_SENTIMENT_CLIENT: CryptoBertClient | None = None


def _get_sentiment_client() -> CryptoBertClient:
    global _SENTIMENT_CLIENT
    if _SENTIMENT_CLIENT is None:
        _SENTIMENT_CLIENT = CryptoBertClient(
            endpoint=GOLD_SENTIMENT_ENDPOINT,
            timeout_seconds=GOLD_SENTIMENT_TIMEOUT_SECONDS,
        )
    return _SENTIMENT_CLIENT


def infer_sentiment(text: str):
    return _get_sentiment_client().infer_sentiment(text)

schema = StructType([
    StructField("label", StringType()),
    StructField("confidence", DoubleType()),
    StructField("base_score", DoubleType())
])

sentiment_udf = udf(infer_sentiment, schema)


def build_silver_stream():
    spark = create_delta_spark_session(APP_NAME, master=get_spark_master())
    return (
        spark.readStream
        .format("delta")
        .load(SILVER_DELTA_PATH)
    )


def build_scored_stream(silver_stream):
    return (
        silver_stream
        .withColumn("sentiment_results", sentiment_udf(col("text")))
        .select(
            "*",
            col("sentiment_results.label").alias("label"),
            col("sentiment_results.confidence").alias("confidence"),
            (
                col("sentiment_results.base_score")
                * col("sentiment_results.confidence")
                * col("engagement")
            ).alias("weighted_sentiment"),
        )
        .drop("sentiment_results")
    )


def build_gold_aggregation(scored_stream):
    return (
        scored_stream
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
    scored_stream = build_scored_stream(silver_stream)
    gold_stream = build_gold_aggregation(scored_stream)

    query = start_delta_query(
        gold_stream,
        output_path=GOLD_DELTA_PATH,
        checkpoint_path=GOLD_CHECKPOINT_PATH,
        query_name=f"{APP_NAME}-write",
    )
    await_query(query)


if __name__ == "__main__":
    run()