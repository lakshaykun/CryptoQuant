import time

from pyspark.sql import functions as F

from models.inference.api_client import predict_with_api
from pipelines.jobs.streaming.kafka_batch import get_kafka_batch
from pipelines.schema.predictions.log_return_lead1 import PREDICTIONS_LOG_RETURN_LEAD1_SCHEMA
from pipelines.storage.delta.market_stream_state import load_last_offsets
from pipelines.storage.delta.writer import write_batch
from pipelines.utils.spark import get_spark
from utils_global.config_loader import load_config
from utils_global.logger import get_logger
from pipelines.transformers.predictions.log_return_lead1 import Log_Return_Lead1_Transformer


logger = get_logger("spark_predictions")
data_config = load_config("configs/data.yaml")
kafkaConfig = load_config("configs/kafka.yaml")
spark = get_spark(logger)

def run():
    start = time.perf_counter()

    # -----------------------------
    # 1. LOAD STATE
    # -----------------------------
    offsets = load_last_offsets(spark)

    # -----------------------------
    # 2. READ KAFKA
    # -----------------------------
    df, topic = get_kafka_batch(spark, kafkaConfig["topics"]["market_stream"], kafkaConfig["brokers"], offsets)

    if df.rdd.isEmpty():
        logger.info("[batch] No new data")
        return

    parsed = parse_kafka_message(df, RAW_MARKET_SCHEMA)

    gold_path = data_config["tables"]["gold_market"]["path"]
    checkpoint_path = data_config["checkpoints"]["predictions_log_return_lead1"]

    brokers = kafkaConfig["brokers"]

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", brokers) \
        .option("subscribe", list(kafkaConfig["topics"].keys())[0]) \
        .option("startingOffsets", "latest") \
        .load()

    gold_stream = (
        spark.readStream
        .format("delta")
        .option("ignoreChanges", "true")
        .load(gold_path)
    )

    query = (
        gold_stream.writeStream
        .foreachBatch(_process_predictions)
        .option("checkpointLocation", checkpoint_path)
        .start()
    )

    result_df = result_df.dropDuplicates(["symbol", "open_time"])

    write_batch(
        result_df,
        "predictions_log_return_lead1",
        expected_schema=PREDICTIONS_LOG_RETURN_LEAD1_SCHEMA,
    )

    duration = time.perf_counter() - start

    logger.info("[batch] done in %.3fs", duration)


if __name__ == "__main__":
    run()