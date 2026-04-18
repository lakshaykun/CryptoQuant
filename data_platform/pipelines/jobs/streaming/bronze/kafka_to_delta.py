from pipelines.jobs.streaming.common.runtime_env import configure_pyspark_python

configure_pyspark_python()

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, expr

from pipelines.jobs.streaming.common.job_runtime import (
    await_query,
    configure_job_logging,
    start_delta_query,
)
from utils.spark_config import (
    get_checkpoint_path,
    get_delta_path,
    get_kafka_option,
    get_spark_app_name,
    get_spark_master,
)
from utils.spark_utils import create_delta_spark_session

BRONZE_DELTA_PATH = get_delta_path("bronze", "delta/bronze")
BRONZE_CHECKPOINT_PATH = get_checkpoint_path("bronze", "checkpoints/bronze")
KAFKA_BOOTSTRAP_SERVERS = get_kafka_option("bootstrap_servers", "localhost:9092")
KAFKA_SUBSCRIBE_TOPICS = get_kafka_option("subscribe", "btc_reddit,btc_yt,btc_news")
KAFKA_STARTING_OFFSETS = get_kafka_option("starting_offsets", "latest")
KAFKA_FAIL_ON_DATA_LOSS = get_kafka_option("fail_on_data_loss", "false")
APP_NAME = f"{get_spark_app_name()}-bronze"
BRONZE_LOOKBACK_HOURS = 24


def build_kafka_stream() -> DataFrame:
    spark = create_delta_spark_session(
        APP_NAME,
        include_kafka=True,
        master=get_spark_master(),
    )

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_SUBSCRIBE_TOPICS)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", KAFKA_FAIL_ON_DATA_LOSS)
        .load()
    )


def build_raw_events_stream(kafka_df: DataFrame) -> DataFrame:
    return (
        kafka_df
        .selectExpr(
            "CAST(value AS STRING) as raw_json",
            "topic",
            "partition",
            "offset",
            "timestamp as kafka_timestamp",
        )
        .filter(
            col("kafka_timestamp") >= current_timestamp() - expr(f"INTERVAL {BRONZE_LOOKBACK_HOURS} HOURS")
        )
    )


def run() -> None:
    configure_job_logging()
    kafka_df = build_kafka_stream()
    raw_events = build_raw_events_stream(kafka_df)

    query = start_delta_query(
        raw_events,
        output_path=BRONZE_DELTA_PATH,
        checkpoint_path=BRONZE_CHECKPOINT_PATH,
        query_name=f"{APP_NAME}-write",
    )
    await_query(query)


if __name__ == "__main__":
    run()
