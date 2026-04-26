from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType


STATE_SENTIMENT_SCHEMA = StructType([
    StructField("layer", StringType(), False),
    StructField("source", StringType(), False),
    StructField("symbol", StringType(), False),
    StructField("last_processed_time", TimestampType(), False),
    StructField("state_date", DateType(), False),
    StructField("ingestion_time", TimestampType(), True),
])
