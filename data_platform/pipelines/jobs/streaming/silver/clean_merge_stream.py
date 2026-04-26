from pipelines.jobs.streaming.common.runtime_env import configure_pyspark_python

configure_pyspark_python()

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

from pipelines.jobs.streaming.common.job_runtime import (
    await_query,
    configure_job_logging,
    log_stream_schema,
)
from pipelines.jobs.streaming.common.sentiment_state import upsert_sentiment_state
from pipelines.storage.delta.utils import get_table_config
from pipelines.schema.bronze.sentiment import BRONZE_SENTIMENT_SCHEMA
from pipelines.schema.silver.sentiment import SILVER_SENTIMENT_SCHEMA
from pipelines.transformers.silver.sentiment import SilverSentimentTransformer
from utils.config_loader import load_config
from utils.spark_config import (
    get_spark_app_name,
    get_spark_master,
    get_silver_trigger_seconds,
)
from utils.spark_utils import create_delta_spark_session

DATA_CONFIG = load_config("configs/data.yaml")
BRONZE_DELTA_PATH = get_table_config("bronze_sentiment", DATA_CONFIG)["path"]
SILVER_DELTA_PATH = get_table_config("silver_sentiment", DATA_CONFIG)["path"]
SILVER_CHECKPOINT_PATH = get_table_config("silver_sentiment", DATA_CONFIG).get(
    "checkpoint",
    "delta/checkpoint/silver/sentiment",
)
APP_NAME = f"{get_spark_app_name()}-silver"
SILVER_TRIGGER_SECONDS = get_silver_trigger_seconds(5)

def ensure_delta_table(spark: SparkSession, path: str, schema: StructType) -> None:
    if DeltaTable.isDeltaTable(spark, path):
        return
    spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(path)


def build_parsed_stream():
    spark = create_delta_spark_session(APP_NAME, master=get_spark_master())
    ensure_delta_table(spark, BRONZE_DELTA_PATH, BRONZE_SENTIMENT_SCHEMA)

    return (
        spark.readStream
        .format("delta")
        .load(BRONZE_DELTA_PATH)
        .select("id", "timestamp", "source", "text", "engagement", "symbol", "event_time")
    )


def process_silver_batch(batch_df: DataFrame, batch_id: int) -> None:
    if not batch_df.take(1):
        return

    silver_batch = (
        batch_df
        .withColumn("event_date", F.to_date("event_time"))
        .where(F.col("event_time").isNotNull() & F.col("event_date").isNotNull())
        .dropDuplicates(["id", "source", "symbol", "event_time"])
        .select([field.name for field in SILVER_SENTIMENT_SCHEMA.fields if field.name != "ingestion_time"])
    )
    if not silver_batch.take(1):
        return

    # Insert-only write with Delta transactional dedupe for foreachBatch retries.
    (
        silver_batch.write
        .format("delta")
        .mode("append")
        .option("txnAppId", APP_NAME)
        .option("txnVersion", int(batch_id))
        .partitionBy("source", "symbol", "event_date")
        .save(SILVER_DELTA_PATH)
    )

    upsert_sentiment_state(silver_batch, layer="silver")


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
