from pyspark.sql.types import *


PREDICTIONS_STATE_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("last_processed_time", TimestampType()),
    StructField("ingestion_time", TimestampType())
])