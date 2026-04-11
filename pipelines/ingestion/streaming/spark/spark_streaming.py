# pipelines/ingestion/streaming/spark/spark_streaming.py

from pipelines.ingestion.streaming.utils.helpers import parse_kafka_message
from pipelines.storage.delta.writer import write_stream
from pipelines.transformers.bronze.market import BronzeMarketTransformer
from pipelines.utils.logger import get_logger
from pipelines.utils.config_loader import load_config
from pipelines.schema.raw.market import RAW_MARKET_SCHEMA
from pipelines.utils.spark import get_spark

kafkaConfig = load_config("configs/kafka.yaml")
dataConfig = load_config("configs/data.yaml")
logger = get_logger("spark_streaming")

spark = get_spark(logger)

# -------------------------------
# 1. READ FROM KAFKA
# -------------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaConfig.get("brokers", "localhost:9092")) \
    .option("subscribe", kafkaConfig.get("crypto_topic", "crypto_prices")) \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = parse_kafka_message(df, RAW_MARKET_SCHEMA)

# -------------------------------
# 2. BRONZE
# -------------------------------
bronze_transformer = BronzeMarketTransformer()
bronze = bronze_transformer.transform(parsed_df, "stream")


# -------------------------------
# 5. WRITE ALL STREAMS
# -------------------------------
bronze_query = write_stream(
        bronze, 
        "bronze_market", 
        "append"
    )

# bronze_query = bronze.writeStream \
#     .format("delta") \
#     .outputMode("append") \
#     .option("checkpointLocation", dataConfig.get("checkpoint_path", {}).get("bronze_market", "medallion/checkpoints/bronze/market")) \
#     .partitionBy("symbol", "date") \
#     .trigger(processingTime="1 second") \
#     .start(dataConfig.get("bronze_market_path", "medallion/bronze/market/"))


# -------------------------------
# 6. KEEP STREAM ALIVE
# -------------------------------
spark.streams.awaitAnyTermination()