from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType


GOLD_SENTIMENT_SCHEMA = StructType([
    StructField("window_start", TimestampType(), False),
    StructField("window_end", TimestampType(), False),
    StructField("symbol", StringType(), False),
    StructField("sentiment_index", DoubleType(), True),
    StructField("avg_confidence", DoubleType(), True),
    StructField("message_count", IntegerType(), True),
    StructField("window_date", DateType(), False),
    StructField("ingestion_time", TimestampType(), True),
])
