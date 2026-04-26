from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType, TimestampType


BRONZE_SENTIMENT_SCHEMA = StructType([
    StructField("id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("source", StringType(), False),
    StructField("text", StringType(), False),
    StructField("engagement", IntegerType(), True),
    StructField("symbol", StringType(), False),
    StructField("event_time", TimestampType(), True),
    StructField("event_date", DateType(), True),
    StructField("ingestion_time", TimestampType(), True),
])