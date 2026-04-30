from pyspark.sql.types import BooleanType, DateType, DoubleType, StringType, StructField, StructType, TimestampType


GOLD_PROCESSED_SENTIMENT_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("window_start_time", TimestampType(), False),
    StructField("aggregated_sentiment", DoubleType(), True),
    StructField("ema_filled_flag", BooleanType(), False),
    StructField("date", DateType(), False),
    StructField("ingestion_time", TimestampType(), True),
])
