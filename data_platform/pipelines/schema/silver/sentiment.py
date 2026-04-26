from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


SILVER_SENTIMENT_SCHEMA = StructType([
    StructField("id", StringType(), False),
    StructField("timestamp", StringType(), True),
    StructField("source", StringType(), False),
    StructField("text", StringType(), True),
    StructField("engagement", IntegerType(), True),
    StructField("symbol", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("event_date", DateType(), False),
    StructField("ingestion_time", TimestampType(), True),
])
