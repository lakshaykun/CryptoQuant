# pipelines/schema/state/market_stream.py

from pyspark.sql.types import *

STATE_MARKET_SCHEMA = StructType([
    StructField("topic", StringType(), False),
    StructField("partition", IntegerType(), False),
    StructField("offset", LongType(), False),
    StructField("updated_at", TimestampType(), False),
])