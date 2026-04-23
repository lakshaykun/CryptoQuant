# pipelines/schema/state/market.py

from pyspark.sql.types import *

STATE_MARKET_SCHEMA = StructType([
    StructField("symbol", StringType()),

    StructField("last_processed_time", TimestampType()),

    StructField("ingestion_time", TimestampType())
])