import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from utils.spark_utils import create_delta_spark_session
from utils.spark_config import (
    get_checkpoint_path,
    get_delta_path,
    get_kafka_option,
    get_spark_app_name,
    get_spark_master,
)

BRONZE_DELTA_PATH = get_delta_path("bronze", "delta/bronze")
BRONZE_CHECKPOINT_PATH = get_checkpoint_path("bronze", "checkpoints/bronze")
KAFKA_BOOTSTRAP_SERVERS = get_kafka_option("bootstrap_servers", "localhost:9092")
KAFKA_SUBSCRIBE_TOPICS = get_kafka_option("subscribe", "btc_reddit,btc_yt,btc_news")
KAFKA_STARTING_OFFSETS = get_kafka_option("starting_offsets", "latest")
KAFKA_FAIL_ON_DATA_LOSS = get_kafka_option("fail_on_data_loss", "false")
APP_NAME = f"{get_spark_app_name()}-bronze"

spark = create_delta_spark_session(
    APP_NAME,
    include_kafka=True,
    master=get_spark_master(),
)

bronze_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_SUBSCRIBE_TOPICS)
    .option("startingOffsets", KAFKA_STARTING_OFFSETS)
    .option("failOnDataLoss", KAFKA_FAIL_ON_DATA_LOSS)
    .load()
)

raw_events = bronze_df.selectExpr(
    "CAST(value AS STRING) as raw_json",
    "topic",
    "partition",
    "offset",
    "timestamp as kafka_timestamp"
)

query = (
    raw_events.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", BRONZE_CHECKPOINT_PATH)
    .start(BRONZE_DELTA_PATH)
)
  


query.awaitTermination()