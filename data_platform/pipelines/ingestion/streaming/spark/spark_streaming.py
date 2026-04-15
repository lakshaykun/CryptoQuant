# pipelines/ingestion/streaming/spark/spark_streaming.py

from pipelines.ingestion.streaming.utils.helpers import parse_kafka_message
from pipelines.schema.bronze.market import BRONZE_MARKET_SCHEMA
from pipelines.schema.gold.market import GOLD_MARKET_SCHEMA
from pipelines.schema.silver.market import SILVER_MARKET_SCHEMA
from pipelines.transformers.bronze.market import BronzeMarketTransformer
from pipelines.transformers.gold.market import GoldMarketTransformer
from pipelines.transformers.silver.market import SilverMarketTransformer
from utils.logger import get_logger
from utils.config_loader import load_config
from pipelines.schema.raw.market import RAW_MARKET_SCHEMA
from pipelines.utils.spark import get_spark
from pipelines.storage.delta.writer import write_batch
import os

env = os.getenv("ENV", "host")


kafkaConfig = load_config("configs/kafka.yaml")
dataConfig = load_config("configs/data.yaml")
logger = get_logger("spark_streaming")

spark = get_spark(logger, need_kafka=True)


def _process_pipeline(df, epoch_id):
    if df is None or df.rdd.isEmpty():
        return

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

# -------------------------------
# 1. READ FROM KAFKA
# -------------------------------
brokers = kafkaConfig["brokers"][env]

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", brokers) \
    .option("subscribe", list(kafkaConfig["topics"].keys())[0]) \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = parse_kafka_message(df, RAW_MARKET_SCHEMA)

parsed_df.writeStream \
    .foreachBatch(_process_pipeline) \
    .option("checkpointLocation", "medallion/checkpoints/pipeline") \
    .start()

spark.streams.awaitAnyTermination()