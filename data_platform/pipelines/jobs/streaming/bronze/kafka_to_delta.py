from pipelines.jobs.streaming.common.runtime_env import configure_pyspark_python

configure_pyspark_python()

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp, expr, from_json, to_date, to_timestamp
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pipelines.jobs.streaming.common.job_runtime import (
    await_query,
    configure_job_logging,
)
from pipelines.storage.delta.utils import get_table_config
from utils.config_loader import load_config
from utils.spark_config import (
    get_kafka_option,
    get_spark_app_name,
    get_spark_master,
)
from utils.spark_utils import create_delta_spark_session

# =========================
# CONFIG
# =========================
DATA_CONFIG = load_config("configs/data.yaml")

BRONZE_TABLE = get_table_config("bronze_sentiment", DATA_CONFIG)
BRONZE_DELTA_PATH = BRONZE_TABLE["path"]
BRONZE_CHECKPOINT_PATH = BRONZE_TABLE.get(
    "checkpoint", "delta/checkpoint/bronze/sentiment"
)

KAFKA_BOOTSTRAP_SERVERS = get_kafka_option("bootstrap_servers", "localhost:9092")
KAFKA_SUBSCRIBE_TOPICS = get_kafka_option(
    "subscribe", "btc_reddit,btc_yt,btc_news,btc_telegram"
)
KAFKA_STARTING_OFFSETS = get_kafka_option("starting_offsets", "latest")
KAFKA_FAIL_ON_DATA_LOSS = get_kafka_option("fail_on_data_loss", "false")

APP_NAME = f"{get_spark_app_name()}-bronze"
BRONZE_LOOKBACK_HOURS = 6

# =========================
# SCHEMA
# =========================
event_schema = StructType([
    StructField("id", StringType()),
    StructField("timestamp", StringType()),
    StructField("source", StringType()),
    StructField("text", StringType()),
    StructField("engagement", IntegerType()),
    StructField("symbol", StringType()),
])

# =========================
# KAFKA STREAM
# =========================
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

# =========================
# TRANSFORMATION
# =========================
def build_bronze_events_stream(kafka_df: DataFrame) -> DataFrame:
    parsed_ts = to_timestamp(col("timestamp"))

    return (
        kafka_df
        .select(
            from_json(col("value").cast("string"), event_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select("data.*", "kafka_timestamp")

        # Parse timestamp once
        .withColumn("parsed_event_time", parsed_ts)

        # Fallback to current_timestamp if null
        .withColumn(
            "event_time",
            F.when(col("parsed_event_time").isNotNull(), col("parsed_event_time"))
             .otherwise(current_timestamp())
        )

        .withColumn("event_date", to_date(col("event_time")))

        # Filter recent Kafka messages
        .where(
            col("kafka_timestamp") >= current_timestamp() - expr(f"INTERVAL {BRONZE_LOOKBACK_HOURS} HOURS")
        )

        # Required fields validation
        .where(
            col("id").isNotNull()
            & col("source").isNotNull()
            & col("symbol").isNotNull()
        )

        # Deduplication
        .dropDuplicates(["id", "source", "symbol", "event_time"])

        # Final selection
        .select(
            "id",
            "timestamp",
            "source",
            "text",
            "engagement",
            "symbol",
            "event_time",
            "event_date",
        )
    )

# =========================
# MAIN
# =========================
def run() -> None:
    configure_job_logging()

    kafka_df = build_kafka_stream()
    bronze_events = build_bronze_events_stream(kafka_df)

    query = (
        bronze_events.writeStream
        .format("delta")
        .queryName(f"{APP_NAME}-write")
        .outputMode("append")
        .option("checkpointLocation", BRONZE_CHECKPOINT_PATH)
        .partitionBy("source", "symbol", "event_date")
        .start(BRONZE_DELTA_PATH)
    )

    await_query(query)

# =========================
# ENTRYPOINT
# =========================
if __name__ == "__main__":
    run()