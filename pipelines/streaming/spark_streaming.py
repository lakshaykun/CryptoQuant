from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pipelines.utils.helpers import parse_kafka_message
from pipelines.utils.logger import get_logger

logger = get_logger("spark_streaming")

spark = SparkSession.builder \
    .appName("CryptoStreaming") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto_prices") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = parse_kafka_message(df)

query = parsed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "medallion/checkpoints/bronze") \
    .start("medallion/bronze/market/")

query.awaitTermination()